#include "condition_builder.hh"
#include <sstream>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

using namespace std;

// 固定列引用（避免随机选择），输出 rel.ident().col
struct fixed_column_ref : value_expr {
  named_relation* rel;
  column col;
  fixed_column_ref(prod* p, named_relation* r, const column& c)
    : value_expr(p), rel(r), col(c) { type = c.type; }
  virtual void out(std::ostream& out) { out << rel->ident() << "." << col.name; }
};

// 采样常量表达式：直接打印预构造的字面量
struct sample_const_expr : value_expr {
  string literal;
  sample_const_expr(prod* p, sqltype* t, const string& lit) : value_expr(p), literal(lit) { type = t; }
  virtual void out(std::ostream& out) { out << literal; }
};

// 通用比较表达式（lhs op rhs）
struct cmp_expr_ast : bool_expr {
  shared_ptr<value_expr> lhs, rhs;
  string op;
  cmp_expr_ast(prod* p, const shared_ptr<value_expr>& l, const string& oper, const shared_ptr<value_expr>& r)
    : bool_expr(p), lhs(l), rhs(r), op(oper) {}
  virtual void out(std::ostream& out) {
    out << *lhs << " " << op << " " << *rhs;
  }
  virtual void accept(prod_visitor* v) {
    v->visit(this); lhs->accept(v); rhs->accept(v);
  }
};

// IN/NOT IN 谓词
struct in_expr_ast : bool_expr {
  shared_ptr<value_expr> lhs;
  vector<shared_ptr<value_expr>> rhs_list;
  bool negate;
  in_expr_ast(prod* p, const shared_ptr<value_expr>& l, const vector<shared_ptr<value_expr>>& set, bool not_in)
    : bool_expr(p), lhs(l), rhs_list(set), negate(not_in) {}
  virtual void out(std::ostream& out) {
    out << *lhs << (negate ? " NOT IN (" : " IN (");
    for (size_t i=0;i<rhs_list.size();++i) { out << *rhs_list[i]; if (i+1 != rhs_list.size()) out << ", "; }
    out << ")";
  }
  virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); for (auto& r : rhs_list) r->accept(v); }
};

// LIKE/NOT LIKE 谓词
struct like_expr_ast : bool_expr {
  shared_ptr<value_expr> lhs;
  shared_ptr<value_expr> pattern;
  bool negate;
  like_expr_ast(prod* p, const shared_ptr<value_expr>& l, const shared_ptr<value_expr>& pat, bool not_like)
    : bool_expr(p), lhs(l), pattern(pat), negate(not_like) {}
  virtual void out(std::ostream& out) {
    out << *lhs << (negate ? " NOT LIKE " : " LIKE ") << *pattern;
  }
  virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); pattern->accept(v); }
};

// JSON 谓词：JSON_EXTRACT(col,'$.path') 与 =/!=/IS [NOT] NULL（仅标量比较）
struct json_pred_expr : bool_expr {
  shared_ptr<value_expr> colref;
  string path;
  string oper; // "=", "!=", "IS NULL", "IS NOT NULL"
  shared_ptr<value_expr> rhs; // 可为空（IS NULL/IS NOT NULL）
  json_pred_expr(prod* p, const shared_ptr<value_expr>& c, const string& jp, const string& op, const shared_ptr<value_expr>& r)
    : bool_expr(p), colref(c), path(jp), oper(op), rhs(r) {}
  virtual void out(std::ostream& out) {
    out << "JSON_EXTRACT(" << *colref << ", '" << path << "') ";
    if (oper == "IS NULL" || oper == "IS NOT NULL") { out << oper; return; }
    out << oper << " " << *rhs;
  }
  virtual void accept(prod_visitor* v) { v->visit(this); colref->accept(v); if (rhs) rhs->accept(v); }
};

// BLOB 安全比较：LENGTH(col) 与关系运算或 IS [NOT] NULL
struct blob_len_expr : bool_expr {
  shared_ptr<value_expr> colref;
  string oper; // "IS NULL"/"IS NOT NULL" or comparison op
  shared_ptr<value_expr> rhs; // 可能为空（IS [NOT] NULL）
  blob_len_expr(prod* p, const shared_ptr<value_expr>& c, const string& o, const shared_ptr<value_expr>& r)
    : bool_expr(p), colref(c), oper(o), rhs(r) {}
  virtual void out(std::ostream& out) {
    if (oper == "IS NULL" || oper == "IS NOT NULL") { out << *colref << " " << oper; return; }
    out << "LENGTH(" << *colref << ") " << oper << " " << *rhs;
  }
  virtual void accept(prod_visitor* v) { v->visit(this); colref->accept(v); if (rhs) rhs->accept(v); }
};

// 空间布尔：ST_Equals/ST_Intersects/ST_Within/ST_Disjoint
struct spatial_bool_expr : bool_expr {
  shared_ptr<value_expr> colref;
  string func;
  shared_ptr<value_expr> geom_rhs;
  spatial_bool_expr(prod* p, const shared_ptr<value_expr>& c, const string& f, const shared_ptr<value_expr>& g)
    : bool_expr(p), colref(c), func(f), geom_rhs(g) {}
  virtual void out(std::ostream& out) {
    // [SRID-4326 Guard] 谓词层统一 SRID=4326，避免 0/4326 混用
    out << func << "(ST_SRID(" << *colref << ", 4326), ST_SRID(" << *geom_rhs << ", 4326))";
  }
  virtual void accept(prod_visitor* v) { v->visit(this); colref->accept(v); geom_rhs->accept(v); }
};

// 内部：生成样本常量列表（最多 3 个）+ 查询级去重（按 alias.col 指纹）
namespace {
struct InSetRegistryCB {
  std::unordered_map<const void*, std::unordered_map<std::string, std::unordered_set<std::string>>> used;
  static std::string fingerprint(const std::vector<std::string>& lits) {
    std::vector<std::string> toks; for (auto &s : lits) { if (!s.empty()) toks.push_back(s); }
    std::sort(toks.begin(), toks.end()); std::ostringstream os; for(size_t i=0;i<toks.size();++i){ if(i) os<<"|"; os<<toks[i]; } return os.str();
  }
  bool has(prod* p, const std::string& alias_col, const std::vector<std::string>& lits) {
    if (!p || !p->scope) { return false; }
    const void* key = static_cast<const void*>(p->scope->stmt_seq.get());
    auto fp = fingerprint(lits); auto it = used.find(key); if (it==used.end()) return false; auto it2 = it->second.find(alias_col); if (it2==it->second.end()) return false; return it2->second.count(fp) > 0;
  }
  void add(prod* p, const std::string& alias_col, const std::vector<std::string>& lits) {
    if (!p || !p->scope) { return; }
    const void* key = static_cast<const void*>(p->scope->stmt_seq.get());
    auto fp = fingerprint(lits);
    auto &m = used[key];
    m[alias_col].insert(fp);
    if (used.size() > 64) { used.clear(); }
  }
};
static InSetRegistryCB g_inset_registry_cb;
}
// ---- 语句级清理（供外部调用） ----
namespace condgen_utils {
  void clear_inset_registry_for_stmt_key(const void* key) {
    if (!key) return;
    g_inset_registry_cb.used.erase(key);
  }
}

static vector<shared_ptr<value_expr>> build_diverse_sample_set(prod* p, const ColumnRef& cr, const SampleList* sl, sqltype* t) {
  vector<shared_ptr<value_expr>> result;
  std::string alias_col = cr.rel->ident() + "." + cr.col.name;
  auto push = [&](const std::string &lit){ result.push_back(make_shared<sample_const_expr>(p, t, lit)); };
  std::vector<std::string> picked;
  int maxn = 1 + (d6()%3);
  auto shuffle_vals = [&](std::vector<Value> v){ for(size_t i=0;i<v.size();++i){ size_t j=(size_t)(d100()%v.size()); std::swap(v[i], v[j]); } return v; };
  std::vector<Value> vals = (sl?sl->values:std::vector<Value>());
  // 统一族过滤：仅保留与目标列类型族一致的样本（防止跨族值如 'a' 进入时间/数值 IN 集合）
  const std::string target_tname = safe_type_name(t);
  TypeFamily target_family = family_of(target_tname);
  std::vector<Value> filtered;
  for (auto &v : vals) { if (!v.is_null && v.family == target_family) filtered.push_back(v); }
  vals = shuffle_vals(filtered);
  auto ensure_diverse = [&](){
    int tries=8;
    while(tries-->0){
      // pick up to maxn non-null literals
      picked.clear(); result.clear();
      for(size_t i=0;i<vals.size() && picked.size()< (size_t)maxn; ++i){ const Value &v = vals[i]; if(v.is_null) continue; picked.push_back(v.literal); push(v.literal); }
      if (!g_inset_registry_cb.has(p, alias_col, picked)) { break; }
      // mix fallback when samples insufficient
      if (vals.size() < (size_t)maxn) {
        const std::string tname = safe_type_name(t);
        if (tname=="varchar") { vals.push_back(Value{sql_quote("ab"), false, TypeFamily::String, SourceTag::Synthetic}); }
        else if (tname=="date") { vals.push_back(Value{"CAST('1999-09-09' AS DATE)", false, TypeFamily::Time, SourceTag::Synthetic}); }
        else if (tname=="datetime"||tname=="timestamp") { vals.push_back(Value{"CAST('1999-09-09 00:00:00' AS DATETIME)", false, TypeFamily::Time, SourceTag::Synthetic}); }
        else if (tname=="time") { vals.push_back(Value{"CAST('00:00:00' AS TIME)", false, TypeFamily::Time, SourceTag::Synthetic}); }
        else if (tname=="year") { vals.push_back(Value{"1970", false, TypeFamily::Time, SourceTag::Synthetic}); }
        else { vals.push_back(Value{"1", false, TypeFamily::Numeric, SourceTag::Synthetic}); vals.push_back(Value{"0", false, TypeFamily::Numeric, SourceTag::Synthetic}); }
      }
      vals = shuffle_vals(vals);
    }
  };
  ensure_diverse();
  if (picked.empty()) {
    // fallback: use one const_expr
    result.clear(); result.push_back(make_shared<const_expr>(p, t));
  }
  g_inset_registry_cb.add(p, alias_col, picked);
  return result;
}

// 简单工具：生成 LIKE 模式常量（在已有样本上添加前后缀/通配）
static shared_ptr<value_expr> make_like_pattern(prod* p, sqltype* t, const ValueCatalog& vc, const string& rel_ident, const string& colname) {
  (void)t; // [噪声抑制] 未使用参数抑制 -Wextra
  const SampleList* sl = vc.get(rel_ident, colname);
  if (sl && !sl->values.empty()) {
    const Value& v = sl->values[(d100()-1) % sl->values.size()];
    string lit = v.literal;
    // 若样本已带引号，插入通配；否则使用 const_expr
    if (!lit.empty() && lit.front()=='\'' && lit.back()=='\'') {
      // 前缀或后缀通配
      if (d6() > 3) lit.insert(1, "%"); else lit.insert(lit.size()-1, "%");
      return make_shared<sample_const_expr>(p, sqltype::get("varchar"), lit);
    }
  }
  return make_shared<const_expr>(p, sqltype::get("varchar"));
}

shared_ptr<bool_expr> ConditionBuilder::build_predicate_for_column(prod* pctx, const ColumnRef& ref, CondContext ctx, const ValueCatalog& vc) {
  (void)ctx; // [噪声抑制] 未使用参数抑制 -Wextra
  // 固定列引用
  auto colref = make_shared<fixed_column_ref>(pctx, ref.rel, ref.col);
  string rel_ident = ref.rel->ident();
  string colname = ref.col.name;
  string tname = safe_type_name(ref.col.type);
  const SampleList* sl = vc.get(rel_ident, colname);
  // 根据类型族选择合法谓词
  if (tname=="geometry") {
    // 几何安全谓词白名单：IS [NOT] NULL、ST_IsEmpty(col)=1、ST_SRID(col)=4326，均在 SRID=4326 约束下优先生成
    int pick = d6();
    if (pick==1) {
      int np = d6();
      return make_shared<blob_len_expr>(pctx, colref, (np<=3?"IS NULL":"IS NOT NULL"), nullptr);
    } else if (pick==2) {
      auto lhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), string("ST_IsEmpty(") + rel_ident + "." + colname + ")");
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), "1");
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    } else if (pick==3) {
      auto lhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), string("ST_SRID(") + rel_ident + "." + colname + ")");
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), "4326");
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    } else {
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("geometry"), "ST_GeomFromText('POINT(0 0)', 4326)");
      string f; switch (d6()) { case 1: f="ST_Equals"; break; case 2: f="ST_Intersects"; break; case 3: f="ST_Within"; break; default: f="ST_Disjoint"; }
      return make_shared<spatial_bool_expr>(pctx, colref, f, rhs);
    }
  }
  if (tname=="json") {
    // JSON 安全谓词白名单：JSON_TYPE(col) IS NOT NULL、JSON_CONTAINS(col, CAST('{}' AS JSON))=1；否则回退到 JSON_EXTRACT 路径
    int pick = d6();
    if (pick==1) {
      auto jt = make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), string("JSON_TYPE(") + rel_ident + "." + colname + ")");
      return make_shared<blob_len_expr>(pctx, jt, "IS NOT NULL", nullptr);
    } else if (pick==2) {
      auto lhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), string("JSON_CONTAINS(") + rel_ident + "." + colname + ", CAST('{}' AS JSON))");
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), "1");
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    } else {
      // JSON_EXTRACT 路径生成 + 标量比较或 NULL 判断
      string path = (d6()>3) ? "$.a[0].b" : "$.k";
      int pick2 = d6();
      if (pick2 <= 2) {
        return make_shared<json_pred_expr>(pctx, colref, path, (pick2==1?"IS NULL":"IS NOT NULL"), nullptr);
      } else {
        // 随机选择数值或字符串常量
        if (d6()>3) {
          auto rhs = make_shared<const_expr>(pctx, sqltype::get("int"));
          return make_shared<json_pred_expr>(pctx, colref, path, (d6()>3?"=":"!="), rhs);
        } else {
          auto rhs = make_shared<const_expr>(pctx, sqltype::get("varchar"));
          return make_shared<json_pred_expr>(pctx, colref, path, (d6()>3?"=":"!="), rhs);
        }
      }
    }
  }
  if (tname=="blob" || tname=="binary" || tname=="varbinary" || tname.find("blob")!=string::npos) {
    int pick = d6();
    if (pick<=2) return make_shared<blob_len_expr>(pctx, colref, (pick==1?"IS NULL":"IS NOT NULL"), nullptr);
    // LENGTH(col) 比较与数值常量
    auto rhs = make_shared<const_expr>(pctx, sqltype::get("int"));
    string ops[4] = {"=","!=",">","<"};
    return make_shared<blob_len_expr>(pctx, colref, ops[(d100()-1)%4], rhs);
  }
  if (tname=="bit") {
    // CAST(col AS UNSIGNED) 比较或 IN(0,1)
    int pick = d6();
    if (pick<=2) {
      auto rhs = make_shared<const_expr>(pctx, sqltype::get("int"));
      string op = (pick==1?"=":"!=");
      // 用 cmp_expr_ast，但 lhs 包裹 CAST(.. AS UNSIGNED)
      auto casted = make_shared<sample_const_expr>(pctx, sqltype::get("int"), string("CAST(") + rel_ident + "." + colname + " AS UNSIGNED)");
      return make_shared<cmp_expr_ast>(pctx, casted, op, rhs);
    } else {
      vector<shared_ptr<value_expr>> set = { make_shared<sample_const_expr>(pctx, sqltype::get("int"), "0"), make_shared<sample_const_expr>(pctx, sqltype::get("int"), "1") };
      return make_shared<in_expr_ast>(pctx, colref, set, false);
    }
  }
  // 时间类型：=、!=、<、<=、>、>=、BETWEEN、IN/NOT IN、IS [NOT] NULL（右值同族）
  if (tname=="date" || tname=="datetime" || tname=="timestamp" || tname=="time" || tname=="year") {
    int pick = d6();
    if (pick<=2) {
      return make_shared<blob_len_expr>(pctx, colref, (pick==1?"IS NULL":"IS NOT NULL"), nullptr);
    }
    // 选择操作符类：RANGE/IN/NIN/REL/EQ/NEQ
    int oc = d6();
    if (oc == 3) {
      // BETWEEN：两端同族常量
      struct between_expr_ast : bool_expr {
        shared_ptr<value_expr> lhs, lo, hi;
        between_expr_ast(prod* p, const shared_ptr<value_expr>& l, const shared_ptr<value_expr>& a, const shared_ptr<value_expr>& b)
          : bool_expr(p), lhs(l), lo(a), hi(b) {}
        virtual void out(std::ostream& out) { out << *lhs << " BETWEEN " << *lo << " AND " << *hi; }
        virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); lo->accept(v); hi->accept(v); }
      };
      auto lo = make_shared<const_expr>(pctx, sqltype::get(tname));
      auto hi = make_shared<const_expr>(pctx, sqltype::get(tname));
      return make_shared<between_expr_ast>(pctx, colref, lo, hi);
    }
    if (oc == 4 || oc == 5) {
      // IN/NOT IN：集合来自同族样本或安全常量
      auto set = build_diverse_sample_set(pctx, ColumnRef(ref.rel, ref.col), sl, sqltype::get(tname));
      return make_shared<in_expr_ast>(pctx, colref, set, (oc==5));
    }
    auto rhs = make_shared<const_expr>(pctx, sqltype::get(tname));
    const char* ops[6] = {"=","!=","<","<=",">",">="};
    return make_shared<cmp_expr_ast>(pctx, colref, ops[(d100())%6], rhs);
  }
  // 字符串/枚举：=, !=, LIKE/NOT LIKE, IN/NOT IN
  if (tname=="varchar" || tname=="char" || tname=="text" || tname=="enum") {
    int pick = d6();
    if (pick==1) {
      // [类型统一] ?: 操作数上行转换为 shared_ptr<value_expr>，避免 const_expr 与 sample_const_expr 类型不一致
      shared_ptr<value_expr> rhs = (!sl || sl->values.empty())
        ? static_pointer_cast<value_expr>(make_shared<const_expr>(pctx, sqltype::get("varchar")))
        : static_pointer_cast<value_expr>(make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), sl->values[(d100()-1)%sl->values.size()].literal));
      return make_shared<cmp_expr_ast>(pctx, colref, "=", rhs);
    }
    if (pick==2) {
      // [类型统一] ?: 操作数上行转换为 shared_ptr<value_expr>，避免 const_expr 与 sample_const_expr 类型不一致
      shared_ptr<value_expr> rhs = (!sl || sl->values.empty())
        ? static_pointer_cast<value_expr>(make_shared<const_expr>(pctx, sqltype::get("varchar")))
        : static_pointer_cast<value_expr>(make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), sl->values[(d100()-1)%sl->values.size()].literal));
      return make_shared<cmp_expr_ast>(pctx, colref, "!=", rhs);
    }
    if (pick==3 || pick==4) {
      auto pat = make_like_pattern(pctx, sqltype::get("varchar"), vc, rel_ident, colname);
      return make_shared<like_expr_ast>(pctx, colref, pat, (pick==4));
    }
    // IN/NOT IN：来自样本集合（查询级去重与随机化）
    auto set = build_diverse_sample_set(pctx, ColumnRef(ref.rel, ref.col), sl, sqltype::get("varchar"));
    return make_shared<in_expr_ast>(pctx, colref, set, (d6()>3));
  }
  // 数值族：=、!=、<、<=、>、>=、BETWEEN、IN/NOT IN
  if (tname=="int" || tname=="tinyint" || tname=="smallint" || tname=="mediumint" || tname=="bigint" || tname=="float" || tname=="double" || tname=="decimal") {
    int pick = d6();
    // 优先选择 BETWEEN/IN/NIN；否则走关系/等值
    if (pick == 3) {
      // BETWEEN（同族常量）
      struct between_expr_ast2 : bool_expr {
        shared_ptr<value_expr> lhs, lo, hi;
        between_expr_ast2(prod* p, const shared_ptr<value_expr>& l, const shared_ptr<value_expr>& a, const shared_ptr<value_expr>& b)
          : bool_expr(p), lhs(l), lo(a), hi(b) {}
        virtual void out(std::ostream& out) { out << *lhs << " BETWEEN " << *lo << " AND " << *hi; }
        virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); lo->accept(v); hi->accept(v); }
      };
      auto lo = make_shared<const_expr>(pctx, ref.col.type);
      auto hi = make_shared<const_expr>(pctx, ref.col.type);
      return make_shared<between_expr_ast2>(pctx, colref, lo, hi);
    }
    if (pick == 5 || pick == 6) {
      auto set = build_diverse_sample_set(pctx, ColumnRef(ref.rel, ref.col), sl, ref.col.type);
      return make_shared<in_expr_ast>(pctx, colref, set, (pick==6));
    }
    // [类型统一] 操作数上行转换为 shared_ptr<value_expr>
    shared_ptr<value_expr> rhs = (!sl || sl->values.empty())
      ? static_pointer_cast<value_expr>(make_shared<const_expr>(pctx, ref.col.type))
      : static_pointer_cast<value_expr>(make_shared<sample_const_expr>(pctx, ref.col.type, sl->values[(d100()-1)%sl->values.size()].literal));
    const char* ops[6] = {"=","!=","<","<=",">",">="};
    return make_shared<cmp_expr_ast>(pctx, colref, ops[(d100())%6], rhs);
  }
  // 兜底：IS NOT NULL
  return make_shared<blob_len_expr>(pctx, colref, "IS NOT NULL", nullptr);
}

shared_ptr<bool_expr> ConditionBuilder::compose_predicates(prod* pctx, const vector<shared_ptr<bool_expr>>& preds) {
  if (preds.empty()) return make_shared<truth_value>(nullptr);
  if (preds.size() == 1) return preds[0];
  // 自定义二元布尔项：仅作为组合容器，不产生额外子树
  struct bool_term2 : bool_expr {
    shared_ptr<bool_expr> lhs, rhs; string op;
    bool_term2(prod* p, const shared_ptr<bool_expr>& l, const shared_ptr<bool_expr>& r)
      : bool_expr(p), lhs(l), rhs(r) { op = ((d6()<4)?"or":"and"); }
    virtual void out(std::ostream& out) {
      out << "(" << *lhs << ") "; indent(out); out << op << " (" << *rhs << ")";
    }
    virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); rhs->accept(v); }
  };
  shared_ptr<bool_expr> acc = preds[0];
  for (size_t i = 1; i < preds.size(); ++i) {
    acc = make_shared<bool_term2>(pctx, acc, preds[i]);
  }
  return acc;
}

ClauseExtras ConditionBuilder::build_extras(const vector<shared_ptr<named_relation>>& refs, bool avoid_limit_duplicate) {
  (void)refs; // [噪声抑制] 未使用参数抑制 -Wextra
  ClauseExtras ce; ce.avoid_limit_duplicate = avoid_limit_duplicate; return ce;
}

// ===== 新增：作用域选列与权重控制 =====
std::vector<ColumnRef> ConditionBuilder::pick_scope_columns(const std::vector<std::shared_ptr<named_relation>>& scope_refs, bool only_physical) {
  std::vector<ColumnRef> cols;
  auto is_supported = [](const std::string &tname){
    if (tname=="geometry" || tname=="json" || tname=="bit") return true;
    if (tname=="date" || tname=="datetime" || tname=="timestamp" || tname=="time" || tname=="year") return true;
    if (tname=="varchar" || tname=="char" || tname=="text" || tname=="enum") return true;
    if (tname=="int" || tname=="tinyint" || tname=="smallint" || tname=="mediumint" || tname=="bigint" || tname=="float" || tname=="double" || tname=="decimal") return true;
    if (tname=="binary" || tname=="varbinary" || tname.find("blob")!=std::string::npos) return true;
    return false; };
  for (auto &nrsp : scope_refs) {
    auto *nr = nrsp.get(); if (!nr) continue;
    auto cols_list = nr->columns();
    for (auto &c : cols_list) {
      bool is_physical = false;
      // 基表或其别名
      if (dynamic_cast<table*>(nr)) {
        is_physical = true;
      } else if (auto ar = dynamic_cast<aliased_relation*>(nr)) {
        if (dynamic_cast<table*>(ar->rel)) {
          is_physical = true;
        } else {
          // 子查询/JSON_TABLE 的别名：subq_* / jt_* 在类型守卫通过时视为“安全物理列”
          std::string alias = nr->ident();
          bool alias_physical = (alias.rfind("subq_", 0) == 0) || (alias.rfind("jt_", 0) == 0);
          std::string tname = safe_type_name(c.type);
          if (alias_physical && is_supported(tname)) {
            is_physical = true;
          }
        }
      }
      if (only_physical && !is_physical) continue;
      cols.emplace_back(nr, c);
    }
  }
  return cols;
}

std::shared_ptr<bool_expr> ConditionBuilder::build_predicates_for_scope(prod* pctx, const std::vector<std::shared_ptr<named_relation>>& scope_refs,
                                                       CondContext ctx, const ValueCatalog& vc, int count, double base_table_weight) {
  // [比例硬约束] 先按硬约束配额填充“表列条件”，再补其他条件（附加项）；避免跨作用域引用。
  int k = count;
  if (k <= 0) k = 1 + (d6() % 3);
  // 计算配额：ceil(total_conditions * base_table_weight)
  int quota = (int)std::ceil(k * base_table_weight);
  auto phys_cols = pick_scope_columns(scope_refs, true);
  auto all_cols  = pick_scope_columns(scope_refs, false);
  bool log_on = (pctx && pctx->scope && pctx->scope->schema && (pctx->scope->schema->condgen_debug || pctx->scope->schema->verbose));
  // 统计参数快照（用于后续 [EXTRA][scope=..] 汇总）：白名单命中视为物理列数；类型守卫粗略按“已知族”判定。
  auto is_supported = [](const std::string &tname){
    if (tname=="geometry" || tname=="json" || tname=="bit") return true;
    if (tname=="date" || tname=="datetime" || tname=="timestamp" || tname=="time" || tname=="year") return true;
    if (tname=="varchar" || tname=="char" || tname=="text" || tname=="enum") return true;
    if (tname=="int" || tname=="tinyint" || tname=="smallint" || tname=="mediumint" || tname=="bigint" || tname=="float" || tname=="double" || tname=="decimal") return true;
    if (tname=="binary" || tname=="varbinary" || tname.find("blob")!=std::string::npos) return true;
    return false; };
  last_stats_.k = k;
  last_stats_.quota = quota;
  last_stats_.candidates = (int)all_cols.size();
  last_stats_.physical = (int)phys_cols.size();
  last_stats_.whitelist_hits = (int)phys_cols.size();
  last_stats_.type_guard_ok = 0;
  for (auto &nrsp : scope_refs) {
    auto *nr = nrsp.get(); if (!nr) continue;
    for (auto &c : nr->columns()) {
      std::string tname = safe_type_name(c.type);
      if (is_supported(tname)) last_stats_.type_guard_ok++;
    }
  }
  last_stats_.base_weight = base_table_weight;
  last_stats_.density = (pctx && pctx->scope && pctx->scope->schema) ? pctx->scope->schema->extra_conds_density : -1.0;
  last_stats_.gate = d100();
  if (log_on) {
    const char* sctx = (ctx==CondContext::WHERE?"WHERE":(ctx==CondContext::JOIN_ON?"JOIN_ON":"HAVING"));
    std::cerr << "[EXTRA][builder] scope=" << sctx
              << " whitelist_hits=" << last_stats_.whitelist_hits
              << " type_guard_ok=" << last_stats_.type_guard_ok
              << " gate=d100(" << last_stats_.gate << ")"
              << " density=" << last_stats_.density
              << " base_weight=" << base_table_weight
              << " count=" << k
              << " candidates=" << (int)all_cols.size()
              << " physical=" << (int)phys_cols.size()
              << std::endl;
    // 候选列列表与逐列原因：非物理列在基础配额中视为 BLACKLISTED；样本缺失视为 SAMPLE_MISS；未知类型视为 TYPE_MISMATCH（仅日志）。
    std::ostringstream oss; oss << "[EXTRA][builder] candidates=[";
    for (size_t i=0;i<all_cols.size();++i) { oss << all_cols[i].rel->ident() << "." << all_cols[i].col.name; if (i+1<all_cols.size()) oss << ","; }
    oss << "]"; std::cerr << oss.str() << std::endl;
    for (auto &cr : all_cols) {
      bool is_physical = false;
      // 基表或其别名
      if (dynamic_cast<table*>(cr.rel)) is_physical = true;
      else if (auto ar = dynamic_cast<aliased_relation*>(cr.rel)) {
        if (dynamic_cast<table*>(ar->rel)) is_physical = true;
        else {
          std::string alias = cr.rel->ident();
          bool alias_physical = (alias.rfind("subq_", 0) == 0) || (alias.rfind("jt_", 0) == 0);
          std::string tname2 = safe_type_name(cr.col.type);
          if (alias_physical && is_supported(tname2)) {
            is_physical = true;
            if (log_on) std::cerr << "[EXTRA][alias] alias as physical candidate: " << alias << "." << cr.col.name << std::endl;
          }
        }
      }
      if (!is_physical) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=BLACKLISTED" << std::endl;
      }
          std::string tname = safe_type_name(cr.col.type);
      if (!is_supported(tname)) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=TYPE_MISMATCH" << std::endl;
      }
      const SampleList* sl = vc.get(cr.rel->ident(), cr.col.name);
      if (!sl || sl->values.empty()) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=SAMPLE_MISS" << std::endl;
      }
    }
  }
  std::vector<std::shared_ptr<bool_expr>> preds;
  // [约束计数器] 每个作用域：常量 true/false 至多一个；IS NULL/IS NOT NULL 至多一个；至少一个列条件
  int const_bool_count = 0;
  int is_null_count = 0;
  bool has_column_predicate = false;
  last_stats_.const_bool_count = 0;
  last_stats_.is_null_count = 0;
  last_stats_.has_column_predicate = 0;
  auto is_nullness_expr = [](const std::shared_ptr<bool_expr>& b)->bool{
    if (!b) return false;
    if (auto bl = dynamic_cast<blob_len_expr*>(b.get())) {
      return (bl->oper == "IS NULL" || bl->oper == "IS NOT NULL");
    }
    if (auto je = dynamic_cast<json_pred_expr*>(b.get())) {
      return (je->oper == "IS NULL" || je->oper == "IS NOT NULL");
    }
    return false;
  };
  auto is_json_type_is_not_null = [](const std::shared_ptr<bool_expr>& b)->bool{
    if (!b) return false;
    if (auto bl = dynamic_cast<blob_len_expr*>(b.get())) {
      if (bl->oper == "IS NOT NULL") {
        if (auto sc = dynamic_cast<sample_const_expr*>(bl->colref.get())) {
          return sc->literal.rfind("JSON_TYPE(", 0) == 0;
        }
      }
    }
    return false;
  };
  auto build_self_compare_for = [&](const ColumnRef& cr)->std::shared_ptr<bool_expr> {
    // 根据类型生成安全“列自比较”
    std::string tname2 = safe_type_name(cr.col.type);
    auto colref2 = make_shared<fixed_column_ref>(pctx, cr.rel, cr.col);
    if (tname2=="int" || tname2=="tinyint" || tname2=="smallint" || tname2=="mediumint" || tname2=="bigint" || tname2=="float" || tname2=="double" || tname2=="decimal" || tname2=="date" || tname2=="datetime" || tname2=="timestamp" || tname2=="time" || tname2=="year") {
      std::string op = (d6()>3?"=":">=");
      auto lhs = colref2; auto rhs = make_shared<fixed_column_ref>(pctx, cr.rel, cr.col);
      return make_shared<cmp_expr_ast>(pctx, lhs, op, rhs);
    }
    if (tname2=="varchar" || tname2=="char" || tname2=="text" || tname2=="enum" || tname2=="binary" || tname2=="varbinary" || tname2.find("blob")!=std::string::npos) {
      auto lhs = colref2; auto rhs = make_shared<fixed_column_ref>(pctx, cr.rel, cr.col);
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    }
    if (tname2=="json") {
      auto jt = make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), std::string("JSON_TYPE(") + cr.rel->ident() + "." + cr.col.name + ")");
      return make_shared<blob_len_expr>(pctx, jt, "IS NOT NULL", nullptr); // 计为列条件，不计入 is_null 配额
    }
    if (tname2=="geometry") {
      auto lhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), std::string("ST_SRID(") + cr.rel->ident() + "." + cr.col.name + ")");
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), "4326");
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    }
    // 兜底
    auto lhs = colref2; auto rhs = make_shared<fixed_column_ref>(pctx, cr.rel, cr.col);
    return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
  };
  // 先填满配额：仅从物理表列生成谓词；若物理列不足，允许重复使用同列以满足配额（避免跨作用域）
  for (int i = 0; i < quota; ++i) {
    if (phys_cols.empty()) break; // 无物理列时无法满足配额，后续回退策略处理
    const ColumnRef& cr = phys_cols[(d100()-1) % phys_cols.size()];
    auto p = build_predicate_for_column(pctx, cr, ctx, vc);
    if (p) {
      // 配额约束：nullness 谓词至多一个；若超过则改为列自比较
      if (is_nullness_expr(p) && !is_json_type_is_not_null(p)) {
        if (is_null_count >= 1) {
          if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl;
          p = build_self_compare_for(cr);
        } else {
          is_null_count++;
        }
      }
      preds.push_back(p);
      has_column_predicate = true;
    }
  }
  // 回退策略：若未达到配额且存在非空作用域列，则继续使用物理列（可重复）或用“列自比较/IS [NOT] NULL”安全条件补齐
  while ((int)preds.size() < quota) {
    if (!phys_cols.empty()) {
      const ColumnRef& cr = phys_cols[(d100()-1) % phys_cols.size()];
      auto p = build_predicate_for_column(pctx, cr, ctx, vc);
      if (p) {
        if (is_nullness_expr(p) && !is_json_type_is_not_null(p)) {
          if (is_null_count >= 1) {
            if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl;
            p = build_self_compare_for(cr);
          } else {
            is_null_count++;
          }
        }
        preds.push_back(p);
        has_column_predicate = true;
        continue;
      }
    }
    // 使用作用域内任意列的“列自比较”为优先；若不支持则用 IS [NOT] NULL（受配额约束）
    if (!all_cols.empty()) {
      const ColumnRef& cr2 = all_cols[(d100()-1) % all_cols.size()];
      auto p2 = build_self_compare_for(cr2);
      if (p2) { preds.push_back(p2); has_column_predicate = true; }
      else {
        auto colref = make_shared<fixed_column_ref>(pctx, cr2.rel, cr2.col);
        bool notnull = (d6()>3);
        if (is_null_count >= 1) {
          if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl;
        } else {
          preds.push_back(make_shared<blob_len_expr>(pctx, colref, (notnull?"IS NOT NULL":"IS NULL"), nullptr));
          is_null_count++;
          has_column_predicate = true;
        }
      }
    } else {
      break; // 无列可用，退出
    }
  }
  // 再补其他条件：严格按 (k - preds.size()) 生成作为附加项（常量/函数/空间布尔等），不替代表列条件
  int extras = k - (int)preds.size();
  last_stats_.extras = extras;
  last_stats_.extras_tagged = 0;
  for (int i = 0; i < extras; ++i) {
    int extra_pick = d6();
    std::shared_ptr<bool_expr> extra;
    if (extra_pick <= 2) {
      // 常量 true/false 受配额约束
      if (const_bool_count >= 1) {
        if (log_on) std::cerr << "[EXTRA][enforce] skip const_bool reason=quota_reached" << std::endl;
        extra = nullptr;
      } else {
        extra = std::make_shared<truth_value>(pctx);
        const_bool_count++;
      }
    } else if (extra_pick <= 4) {
      // 统一门控：禁用不确定系统函数（VERSION()/SCHEMA()）时，使用确定常量占位
      bool nd_gate = (pctx && pctx->scope && pctx->scope->schema) ? pctx->scope->schema->feature.disable_nondeterministic_funcs : false;
      auto f = nd_gate ? std::make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), "'v'")
                       : std::make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), (d6()>3?"VERSION()":"SCHEMA()"));
      // 空性受配额约束
      if (is_null_count >= 1) {
        if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl;
        extra = nullptr;
      } else {
        extra = std::make_shared<blob_len_expr>(pctx, f, (d6()>3?"IS NULL":"IS NOT NULL"), nullptr);
        is_null_count++;
      }
    } else {
      auto g1 = std::make_shared<sample_const_expr>(pctx, sqltype::get("geometry"), "ST_GeomFromText('POINT(0 0)', 4326)");
      auto g2 = std::make_shared<sample_const_expr>(pctx, sqltype::get("geometry"), "ST_GeomFromText('POINT(1 1)', 4326)");
      extra = std::make_shared<spatial_bool_expr>(pctx, g1, (d6()>3?"ST_Disjoint":"ST_Equals"), g2);
    }
    if (extra) {
      preds.push_back(extra);
      if (log_on) {
        // 输出额外谓词的文本，便于肉眼识别（仅日志，不改变 SQL 语义）
        std::ostringstream es; extra->out(es);
        const char* sctx = (ctx==CondContext::WHERE?"where_pred":(ctx==CondContext::JOIN_ON?"on_pred":"having_pred"));
        static int extra_sql_id = 0; extra_sql_id++;
        std::cerr << "[EXTRA_SQL] " << sctx << " #id=" << extra_sql_id << " text=\"" << es.str() << "\"" << std::endl;
        last_stats_.extras_tagged++;
      }
    }
  }
  // 结束后，若未出现任何“列条件”，强制补齐一个“列自比较”谓词
  if (!has_column_predicate) {
    if (!all_cols.empty()) {
      const ColumnRef& crx = all_cols[0];
      auto selfp = build_self_compare_for(crx);
      if (selfp) {
        preds.push_back(selfp);
        has_column_predicate = true;
        if (log_on) {
          std::string tnamex = safe_type_name(crx.col.type);
          std::string method = (tnamex=="int"||tnamex=="tinyint"||tnamex=="smallint"||tnamex=="mediumint"||tnamex=="bigint"||tnamex=="float"||tnamex=="double"||tnamex=="decimal"||tnamex=="date"||tnamex=="datetime"||tnamex=="timestamp"||tnamex=="time"||tnamex=="year") ? ("eq_or_ge") : ("eq");
          std::cerr << "[EXTRA][enforce] add column_self_pred key=" << crx.rel->ident() << "." << crx.col.name << " type=" << tnamex << " method=" << method << std::endl;
        }
      }
    }
  }
  // 汇总计数（仅日志）
  last_stats_.const_bool_count = const_bool_count;
  last_stats_.is_null_count = is_null_count;
  last_stats_.has_column_predicate = has_column_predicate ? 1 : 0;
  if (log_on) {
    if (!all_cols.empty()) {
      std::cerr << "[EXTRA][builder] base_predicates=" << quota
                << " total_predicates=" << k
                << " extras_injected=" << extras
                << std::endl;
    } else {
      std::cerr << "[EXTRA][builder] skip reason=NO_COLUMNS" << std::endl;
    }
  }
  return compose_predicates(pctx, preds);
}

// 重载：JOIN joinscope.refs 为原始指针数组
std::shared_ptr<bool_expr> ConditionBuilder::build_predicates_for_scope(prod* pctx, const std::vector<named_relation*>& raw_refs,
                                                       CondContext ctx, const ValueCatalog& vc, int count, double base_table_weight) {
  // [比例硬约束/JOIN] 先按硬约束配额填充“表列条件”，再补其他条件；避免跨作用域引用。
  std::vector<ColumnRef> phys_cols;
  std::vector<ColumnRef> all_cols;
  for (auto *nr : raw_refs) {
    if (!nr) continue;
    bool is_physical = false;
    if (dynamic_cast<table*>(nr)) {
      is_physical = true;
    } else if (auto ar = dynamic_cast<aliased_relation*>(nr)) {
      if (dynamic_cast<table*>(ar->rel)) is_physical = true;
    }
    auto cols_list = nr->columns();
    for (auto &c : cols_list) {
      ColumnRef cr(nr, c);
      all_cols.push_back(cr);
      if (is_physical) phys_cols.push_back(cr);
    }
  }
  int k = count; if (k <= 0) k = 1 + (d6() % 3);
  int quota = (int)std::ceil(k * base_table_weight);
  bool log_on = (pctx && pctx->scope && pctx->scope->schema && (pctx->scope->schema->condgen_debug || pctx->scope->schema->verbose));
  auto is_supported = [](const std::string &tname){
    if (tname=="geometry" || tname=="json" || tname=="bit") return true;
    if (tname=="date" || tname=="datetime" || tname=="timestamp" || tname=="time" || tname=="year") return true;
    if (tname=="varchar" || tname=="char" || tname=="text" || tname=="enum") return true;
    if (tname=="int" || tname=="tinyint" || tname=="smallint" || tname=="mediumint" || tname=="bigint" || tname=="float" || tname=="double" || tname=="decimal") return true;
    if (tname=="binary" || tname=="varbinary" || tname.find("blob")!=std::string::npos) return true;
    return false; };
  last_stats_.k = k; last_stats_.quota = quota; last_stats_.candidates = (int)all_cols.size(); last_stats_.physical = (int)phys_cols.size();
  last_stats_.whitelist_hits = (int)phys_cols.size(); last_stats_.type_guard_ok = 0;
  for (auto &cr : all_cols) { std::string tname = safe_type_name(cr.col.type); if (is_supported(tname)) last_stats_.type_guard_ok++; }
  last_stats_.base_weight = base_table_weight; last_stats_.density = (pctx && pctx->scope && pctx->scope->schema) ? pctx->scope->schema->extra_conds_density : -1.0; last_stats_.gate = d100();
  if (log_on) {
    const char* sctx = (ctx==CondContext::WHERE?"WHERE":(ctx==CondContext::JOIN_ON?"JOIN_ON":"HAVING"));
    std::cerr << "[EXTRA][builder] scope=" << sctx
              << " whitelist_hits=" << last_stats_.whitelist_hits
              << " type_guard_ok=" << last_stats_.type_guard_ok
              << " gate=d100(" << last_stats_.gate << ")"
              << " density=" << last_stats_.density
              << " base_weight=" << base_table_weight
              << " count=" << k
              << " candidates=" << (int)all_cols.size()
              << " physical=" << (int)phys_cols.size()
              << std::endl;
    std::ostringstream oss; oss << "[EXTRA][builder] candidates=[";
    for (size_t i=0;i<all_cols.size();++i) { oss << all_cols[i].rel->ident() << "." << all_cols[i].col.name; if (i+1<all_cols.size()) oss << ","; }
    oss << "]"; std::cerr << oss.str() << std::endl;
    for (auto &cr : all_cols) {
      bool is_physical = false;
      // 基表或其别名
      if (dynamic_cast<table*>(cr.rel)) is_physical = true;
      else if (auto ar = dynamic_cast<aliased_relation*>(cr.rel)) {
        if (dynamic_cast<table*>(ar->rel)) is_physical = true;
        else {
          std::string alias = cr.rel->ident();
          bool alias_physical = (alias.rfind("subq_", 0) == 0) || (alias.rfind("jt_", 0) == 0);
          std::string tname2 = safe_type_name(cr.col.type);
          if (alias_physical && is_supported(tname2)) {
            is_physical = true;
            if (log_on) std::cerr << "[EXTRA][alias] alias as physical candidate: " << alias << "." << cr.col.name << std::endl;
          }
        }
      }
      if (!is_physical) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=BLACKLISTED" << std::endl;
      }
      std::string tname = safe_type_name(cr.col.type);
      if (!is_supported(tname)) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=TYPE_MISMATCH" << std::endl;
      }
      const SampleList* sl = vc.get(cr.rel->ident(), cr.col.name);
      if (!sl || sl->values.empty()) {
        std::cerr << "[EXTRA][candidate] key=" << cr.rel->ident() << "." << cr.col.name << " skip reason=SAMPLE_MISS" << std::endl;
      }
    }
  }
  std::vector<std::shared_ptr<bool_expr>> preds;
  // [约束计数器] 每个作用域：常量 true/false 至多一个；IS NULL/IS NOT NULL 至多一个；至少一个列条件
  int const_bool_count = 0; int is_null_count = 0; bool has_column_predicate = false;
  last_stats_.const_bool_count = 0; last_stats_.is_null_count = 0; last_stats_.has_column_predicate = 0;
  auto is_nullness_expr = [](const std::shared_ptr<bool_expr>& b)->bool{
    if (!b) return false;
    if (auto bl = dynamic_cast<blob_len_expr*>(b.get())) return (bl->oper == "IS NULL" || bl->oper == "IS NOT NULL");
    if (auto je = dynamic_cast<json_pred_expr*>(b.get())) return (je->oper == "IS NULL" || je->oper == "IS NOT NULL");
    return false;
  };
  auto is_json_type_is_not_null = [](const std::shared_ptr<bool_expr>& b)->bool{
    if (!b) return false;
    if (auto bl = dynamic_cast<blob_len_expr*>(b.get())) {
      if (bl->oper == "IS NOT NULL") {
        if (auto sc = dynamic_cast<sample_const_expr*>(bl->colref.get())) {
          return sc->literal.rfind("JSON_TYPE(", 0) == 0;
        }
      }
    }
    return false;
  };
  auto build_self_compare_for = [&](const ColumnRef& cr)->std::shared_ptr<bool_expr> {
    std::string tname2 = safe_type_name(cr.col.type);
    auto colref2 = make_shared<fixed_column_ref>(pctx, cr.rel, cr.col);
    if (tname2=="int" || tname2=="tinyint" || tname2=="smallint" || tname2=="mediumint" || tname2=="bigint" || tname2=="float" || tname2=="double" || tname2=="decimal" || tname2=="date" || tname2=="datetime" || tname2=="timestamp" || tname2=="time" || tname2=="year") {
      std::string op = (d6()>3?"=":">=");
      return make_shared<cmp_expr_ast>(pctx, colref2, op, make_shared<fixed_column_ref>(pctx, cr.rel, cr.col));
    }
    if (tname2=="varchar" || tname2=="char" || tname2=="text" || tname2=="enum" || tname2=="binary" || tname2=="varbinary" || tname2.find("blob")!=std::string::npos) {
      return make_shared<cmp_expr_ast>(pctx, colref2, "=", make_shared<fixed_column_ref>(pctx, cr.rel, cr.col));
    }
    if (tname2=="json") {
      auto jt = make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), std::string("JSON_TYPE(") + cr.rel->ident() + "." + cr.col.name + ")");
      return make_shared<blob_len_expr>(pctx, jt, "IS NOT NULL", nullptr);
    }
    if (tname2=="geometry") {
      auto lhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), std::string("ST_SRID(") + cr.rel->ident() + "." + cr.col.name + ")");
      auto rhs = make_shared<sample_const_expr>(pctx, sqltype::get("int"), "4326");
      return make_shared<cmp_expr_ast>(pctx, lhs, "=", rhs);
    }
    return make_shared<cmp_expr_ast>(pctx, colref2, "=", make_shared<fixed_column_ref>(pctx, cr.rel, cr.col));
  };
  // 先填满配额：仅从物理表列生成谓词；若物理列不足，允许重复使用
  for (int i=0; i<quota; ++i) {
    if (phys_cols.empty()) break;
    const ColumnRef& cr = phys_cols[(d100()-1) % phys_cols.size()];
    auto p = build_predicate_for_column(pctx, cr, ctx, vc);
    if (p) {
      if (is_nullness_expr(p) && !is_json_type_is_not_null(p)) {
        if (is_null_count >= 1) { if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl; p = build_self_compare_for(cr); }
        else { is_null_count++; }
      }
      preds.push_back(p); has_column_predicate = true;
    }
  }
  while ((int)preds.size() < quota) {
    if (!phys_cols.empty()) {
      const ColumnRef& cr = phys_cols[(d100()-1) % phys_cols.size()];
      auto p = build_predicate_for_column(pctx, cr, ctx, vc);
      if (p) {
        if (is_nullness_expr(p) && !is_json_type_is_not_null(p)) {
          if (is_null_count >= 1) { if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl; p = build_self_compare_for(cr); }
          else { is_null_count++; }
        }
        preds.push_back(p); has_column_predicate = true; continue;
      }
    }
    if (!all_cols.empty()) {
      const ColumnRef& cr2 = all_cols[(d100()-1) % all_cols.size()];
      auto p2 = build_self_compare_for(cr2);
      if (p2) { preds.push_back(p2); has_column_predicate = true; }
      else {
        auto colref = make_shared<fixed_column_ref>(pctx, cr2.rel, cr2.col);
        bool notnull = (d6()>3);
        if (is_null_count >= 1) { if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl; }
        else { preds.push_back(make_shared<blob_len_expr>(pctx, colref, (notnull?"IS NOT NULL":"IS NULL"), nullptr)); is_null_count++; has_column_predicate = true; }
      }
    } else { break; }
  }
  int extras = k - (int)preds.size();
  last_stats_.extras = extras; last_stats_.extras_tagged = 0;
  for (int i=0; i<extras; ++i) {
    int extra_pick = d6();
    std::shared_ptr<bool_expr> extra;
    if (extra_pick <= 2) { if (const_bool_count >= 1) { if (log_on) std::cerr << "[EXTRA][enforce] skip const_bool reason=quota_reached" << std::endl; extra = nullptr; } else { extra = std::make_shared<truth_value>(pctx); const_bool_count++; } }
    else if (extra_pick <= 4) {
      // 统一门控：禁用不确定系统函数（VERSION()/SCHEMA()）时，使用确定常量占位
      bool nd_gate = (pctx && pctx->scope && pctx->scope->schema) ? pctx->scope->schema->feature.disable_nondeterministic_funcs : false;
      auto f = nd_gate ? std::make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), "'v'")
                       : std::make_shared<sample_const_expr>(pctx, sqltype::get("varchar"), (d6()>3?"VERSION()":"SCHEMA()"));
      if (is_null_count >= 1) { if (log_on) std::cerr << "[EXTRA][enforce] skip nullness reason=quota_reached" << std::endl; extra = nullptr; }
      else { extra = std::make_shared<blob_len_expr>(pctx, f, (d6()>3?"IS NULL":"IS NOT NULL"), nullptr); is_null_count++; }
    } else {
      auto g1 = std::make_shared<sample_const_expr>(pctx, sqltype::get("geometry"), "ST_GeomFromText('POINT(0 0)', 4326)");
      auto g2 = std::make_shared<sample_const_expr>(pctx, sqltype::get("geometry"), "ST_GeomFromText('POINT(1 1)', 4326)");
      extra = std::make_shared<spatial_bool_expr>(pctx, g1, (d6()>3?"ST_Disjoint":"ST_Equals"), g2);
    }
    if (extra) {
      preds.push_back(extra);
      if (log_on) {
        std::ostringstream es; extra->out(es);
        const char* sctx = (ctx==CondContext::WHERE?"where_pred":(ctx==CondContext::JOIN_ON?"on_pred":"having_pred"));
        static int extra_sql_id = 0; extra_sql_id++;
        std::cerr << "[EXTRA_SQL] " << sctx << " #id=" << extra_sql_id << " text=\"" << es.str() << "\"" << std::endl;
        last_stats_.extras_tagged++;
      }
    }
  }
  // 结束后，若未出现任何“列条件”，强制补齐一个“列自比较”谓词
  if (!has_column_predicate) {
    if (!all_cols.empty()) {
      const ColumnRef& crx = all_cols[0]; auto selfp = build_self_compare_for(crx);
      if (selfp) { preds.push_back(selfp); has_column_predicate = true; if (log_on) { std::string tnamex = safe_type_name(crx.col.type); std::string method = (tnamex=="int"||tnamex=="tinyint"||tnamex=="smallint"||tnamex=="mediumint"||tnamex=="bigint"||tnamex=="float"||tnamex=="double"||tnamex=="decimal"||tnamex=="date"||tnamex=="datetime"||tnamex=="timestamp"||tnamex=="time"||tnamex=="year") ? ("eq_or_ge") : ("eq"); std::cerr << "[EXTRA][enforce] add column_self_pred key=" << crx.rel->ident() << "." << crx.col.name << " type=" << tnamex << " method=" << method << std::endl; } }
    }
  }
  last_stats_.const_bool_count = const_bool_count; last_stats_.is_null_count = is_null_count; last_stats_.has_column_predicate = has_column_predicate ? 1 : 0;
  if (log_on) {
    if (!all_cols.empty()) {
      std::cerr << "[EXTRA][builder] base_predicates=" << quota
                << " total_predicates=" << k
                << " extras_injected=" << extras
                << std::endl;
    } else {
      std::cerr << "[EXTRA][builder] skip reason=NO_COLUMNS" << std::endl;
    }
  }
  return compose_predicates(pctx, preds);
}
