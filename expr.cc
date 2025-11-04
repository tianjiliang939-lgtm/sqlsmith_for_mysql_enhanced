#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <memory>
#include <cassert>
#include <string>
// 需要 std::unordered_set 定义窗口白/黑名单
#include <unordered_set> 
#include <unordered_map>
// IN 集合指纹记录（排序去重）
#include <set> 

// 统一实现 WGS84 坐标范围校验（经度[-180,180]、纬度[-90,90]，由于易出现问题，都取[-90,90])
inline double clamp_lon(double x) { return std::max(-90.0, std::min(90.0, x)); }
inline double clamp_lat(double y) { return std::max(-90.0,  std::min(90.0,  y)); }
// 提供统一二元裁剪工具，明确顺序为（lon, lat）
inline void clamp_lonlat(double &lon, double &lat) { lon = clamp_lon(lon); lat = clamp_lat(lat); }


#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"
#include "expr.hh"
#include "json_expr.hh"
#include "spatial_expr.hh"
#include "value_catalog.hh"
#include "mysql.hh"

// ---- 安全类型工具实现：稳定类型族枚举 / 指针身份映射 + 按值拷贝 ----
static inline sqltype* T(const char* n){ return sqltype::get(std::string(n)); }
SafeTypeFamily safe_type_family(sqltype* t){
  if (!t) return SafeTypeFamily::Unknown;
  // Prefer pointer identity to avoid touching potentially invalid string objects
  auto* TINT = sqltype::get("int");
  auto* TTINY = sqltype::get("tinyint");
  auto* TSMALL = sqltype::get("smallint");
  auto* TMED = sqltype::get("mediumint");
  auto* TBIG = sqltype::get("bigint");
  auto* TFLOAT = sqltype::get("float");
  auto* TDOUBLE = sqltype::get("double");
  auto* TDEC = sqltype::get("decimal");
  auto* TVARCHAR = sqltype::get("varchar");
  auto* TCHAR = sqltype::get("char");
  auto* TTEXT = sqltype::get("text");
  auto* TENUM = sqltype::get("enum");
  auto* TDATE = sqltype::get("date");
  auto* TTIME = sqltype::get("time");
  auto* TYEAR = sqltype::get("year");
  auto* TDATETIME = sqltype::get("datetime");
  auto* TSTAMP = sqltype::get("timestamp");
  auto* TJSON = sqltype::get("json");
  auto* TGEOM = sqltype::get("geometry");
  auto* TBIN = sqltype::get("binary");
  auto* TVARBIN = sqltype::get("varbinary");
  auto* TBLOB = sqltype::get("blob");
  auto is_any = [&](sqltype* x, std::initializer_list<sqltype*> lst){ for (auto* y: lst){ if (x==y) return true; } return false; };
  if (is_any(t,{TINT,TTINY,TSMALL,TMED,TBIG,TFLOAT,TDOUBLE,TDEC})) return SafeTypeFamily::Numeric;
  if (is_any(t,{TVARCHAR,TCHAR,TTEXT,TENUM})) return SafeTypeFamily::String;
  if (t==TDATE) return SafeTypeFamily::Date;
  if (is_any(t,{TTIME,TYEAR})) return SafeTypeFamily::Time;
  if (t==TDATETIME) return SafeTypeFamily::Datetime;
  if (t==TSTAMP) return SafeTypeFamily::Timestamp;
  if (t==TJSON) return SafeTypeFamily::Json;
  if (t==TGEOM) return SafeTypeFamily::Geometry;
  // Binary/blob treated as string family for predicate generation decisions
  if (is_any(t,{TBIN,TVARBIN,TBLOB})) return SafeTypeFamily::String;
  return SafeTypeFamily::Unknown;
}
std::string safe_type_name(sqltype* t){
  if (!t) return std::string("unknown");
  // Map by pointer identity to canonical stable strings; do not touch t->name
  auto* TINT = sqltype::get("int");
  auto* TTINY = sqltype::get("tinyint");
  auto* TSMALL = sqltype::get("smallint");
  auto* TMED = sqltype::get("mediumint");
  auto* TBIG = sqltype::get("bigint");
  auto* TFLOAT = sqltype::get("float");
  auto* TDOUBLE = sqltype::get("double");
  auto* TDEC = sqltype::get("decimal");
  auto* TVARCHAR = sqltype::get("varchar");
  auto* TCHAR = sqltype::get("char");
  auto* TTEXT = sqltype::get("text");
  auto* TENUM = sqltype::get("enum");
  auto* TDATE = sqltype::get("date");
  auto* TTIME = sqltype::get("time");
  auto* TYEAR = sqltype::get("year");
  auto* TDATETIME = sqltype::get("datetime");
  auto* TSTAMP = sqltype::get("timestamp");
  auto* TJSON = sqltype::get("json");
  auto* TGEOM = sqltype::get("geometry");
  auto* TBIN = sqltype::get("binary");
  auto* TVARBIN = sqltype::get("varbinary");
  auto* TBLOB = sqltype::get("blob");
  if (t==TINT) return "int";
  if (t==TTINY) return "tinyint";
  if (t==TSMALL) return "smallint";
  if (t==TMED) return "mediumint";
  if (t==TBIG) return "bigint";
  if (t==TFLOAT) return "float";
  if (t==TDOUBLE) return "double";
  if (t==TDEC) return "decimal";
  if (t==TVARCHAR || t==TCHAR || t==TTEXT || t==TENUM) return "varchar";
  if (t==TDATE) return "date";
  if (t==TTIME || t==TYEAR) return "time";
  if (t==TDATETIME) return "datetime";
  if (t==TSTAMP) return "timestamp";
  if (t==TJSON) return "json";
  if (t==TGEOM) return "geometry";
  if (t==TBIN) return "binary";
  if (t==TVARBIN) return "varbinary";
  if (t==TBLOB) return "blob";
  return std::string("unknown");
}

// ===== Exists 作用域最小补齐工具：生成类型安全的列条件文本 =====
// 查询级（语句级）IN 集合指纹注册表：避免同一列在同一查询内重复完全相同的 IN 常量集
namespace {
struct InSetRegistry {
  // 按语句唯一指针（scope->stmt_seq 的地址）区分作用域；键：alias.col → 已用集合指纹
  std::unordered_map<const void*, std::unordered_map<std::string, std::unordered_set<std::string>>> used;
  static std::string fingerprint(const std::vector<std::string>& literals) {
    std::vector<std::string> toks;
    for (auto &s : literals) { if (!s.empty()) toks.push_back(s); }
    std::sort(toks.begin(), toks.end());
    std::ostringstream os; for (size_t i=0;i<toks.size();++i){ if(i) os<<"|"; os<<toks[i]; } return os.str();
  }
  bool has(prod* p, const std::string& alias_col, const std::vector<std::string>& lits) {
    if (!p || !p->scope) { return false; }
    const void* key = static_cast<const void*>(p->scope->stmt_seq.get());
    auto fp = fingerprint(lits);
    auto it = used.find(key); if (it == used.end()) return false;
    auto it2 = it->second.find(alias_col); if (it2 == it->second.end()) return false;
    return it2->second.count(fp) > 0;
  }
  void add(prod* p, const std::string& alias_col, const std::vector<std::string>& lits) {
    if (!p || !p->scope) { return; }
    const void* key = static_cast<const void*>(p->scope->stmt_seq.get());
    auto fp = fingerprint(lits);
    auto &m = used[key]; auto &s = m[alias_col]; s.insert(fp);
    // 简单内存上限：超过 64 条语句时清空（避免泄漏），不影响当前语义
    if (used.size() > 64) used.clear();
  }
};
static InSetRegistry g_inset_registry;
}
// ---- 语句级清理（供外部调用） ----
namespace expr_utils {
  void clear_inset_registry_for_stmt_key(const void* key) {
    if (!key) return;
    g_inset_registry.used.erase(key);
  }
}
static inline bool t_is_text(sqltype* t){ return safe_type_family(t) == SafeTypeFamily::String; }
static inline bool t_is_time(sqltype* t){ SafeTypeFamily f = safe_type_family(t); return (f == SafeTypeFamily::Date || f == SafeTypeFamily::Time || f == SafeTypeFamily::Datetime || f == SafeTypeFamily::Timestamp); }
static inline bool t_is_bin(sqltype* t){ std::string n = safe_type_name(t); return (n=="binary" || n=="varbinary" || n.find("blob")!=std::string::npos); }
static inline bool t_is_num(sqltype* t){ return safe_type_family(t) == SafeTypeFamily::Numeric; }

std::string window_function::make_column_predicate_for_scope(prod* p, int used_nullness) {
  if (!p || !p->scope) return std::string("1=1");
  // Nearest alias: last pushed ref in current scope
  named_relation* near_rel = nullptr; const column* picked_col = nullptr; sqltype* picked_type = nullptr;
  if (!p->scope->refs.empty()) {
    near_rel = p->scope->refs.back();
    if (near_rel) {
      // Prefer non-JSON/spatial columns
      for (auto &c : near_rel->columns()) {
        sqltype* t = c.type; if (!t) continue; std::string n = safe_type_name(t);
        bool ext = (n=="json" || n=="geometry"); if (ext) continue;
        bool ok = t_is_num(t) || t_is_text(t) || t_is_time(t) || t_is_bin(t);
        if (!ok) { continue; }
        picked_col = &c;
        picked_type = t;
        break;
      }
      // If none found, allow JSON/spatial
      if (!picked_col) {
        for (auto &c : near_rel->columns()) {
          sqltype* t = c.type; if (!t) continue; std::string n = safe_type_name(t);
          bool ext = (n=="json" || n=="geometry"); if (!ext) continue;
          picked_col = &c; picked_type = t; break;
        }
      }
    }
  }
  if (!near_rel || !picked_type || !picked_col) return std::string("1=1");
  std::string alias_col = near_rel->ident() + "." + picked_col->name;

  // Try to resolve real table ident for alias to access base samples
  std::string real_ident;
  if (auto ar = dynamic_cast<aliased_relation*>(near_rel)) {
    if (auto t = dynamic_cast<table*>(ar->rel)) real_ident = t->ident();
  } else if (auto t = dynamic_cast<table*>(near_rel)) {
    real_ident = t->ident();
  }

  const SampleList* sl = nullptr;
#ifdef HAVE_LIBMYSQLCLIENT
  if (p->scope->schema) {
    if (auto sm = dynamic_cast<schema_mysql*>(p->scope->schema)) {
      if (!real_ident.empty()) sl = sm->base_samples_cache.get(real_ident, picked_col->name);
    }
  }
#endif

  std::ostringstream oss;
  std::string tn = safe_type_name(picked_type);
  auto build_diverse_in = [&](std::vector<Value> vals, size_t maxn){
      // 打乱顺序并构造至多 maxn 个值；若与已用集合指纹相同，则尝试混入边界值或不同样本
      auto shuffle_vals = [&](std::vector<Value> v){ for(size_t i=0;i<v.size();++i){ size_t j = (size_t)(d100() % v.size()); std::swap(v[i], v[j]); } return v; };
      vals = shuffle_vals(vals);
      std::vector<std::string> picked;
      for (size_t i=0;i<vals.size() && picked.size()<maxn; ++i) {
        const Value &v = vals[i]; if (v.is_null) continue; picked.push_back(v.literal);
      }
      auto ensure_diverse = [&](){
        int tries = 8;
        while (tries-- > 0 && g_inset_registry.has(p, alias_col, picked)) {
          vals = shuffle_vals(vals);
          // 若样本不足，混入类型安全边界/保守值
          if (vals.size() < maxn) {
            if (tn=="varchar") { vals.push_back(Value{sql_quote("ab"), false, TypeFamily::String, SourceTag::Synthetic}); }
            else if (tn=="date") { vals.push_back(Value{sql_quote("1970-01-01"), false, TypeFamily::Time, SourceTag::Synthetic}); }
            else if (tn=="datetime" || tn=="timestamp") { vals.push_back(Value{sql_quote("1970-01-01 00:00:00"), false, TypeFamily::Time, SourceTag::Synthetic}); }
            else if (tn=="time") { vals.push_back(Value{sql_quote("00:00:00"), false, TypeFamily::Time, SourceTag::Synthetic}); }
            else if (tn=="year") { vals.push_back(Value{std::string("1970"), false, TypeFamily::Time, SourceTag::Synthetic}); }
            else { vals.push_back(Value{"1", false, TypeFamily::Numeric, SourceTag::Synthetic}); vals.push_back(Value{"0", false, TypeFamily::Numeric, SourceTag::Synthetic}); }
          }
          picked.clear();
          for (size_t i=0;i<vals.size() && picked.size()<maxn; ++i) { const Value &v = vals[i]; if (v.is_null) continue; picked.push_back(v.literal); }
        }
      };
      ensure_diverse();
      g_inset_registry.add(p, alias_col, picked);
      oss << alias_col << " IN (";
      for (size_t i=0;i<picked.size(); ++i) { if (i) oss << ","; oss << picked[i]; }
      if (picked.empty()) {
        if (tn=="varchar") oss << "''";
        else if (tn=="date") oss << "'1970-01-01'";
        else if (tn=="datetime" || tn=="timestamp") oss << "'1970-01-01 00:00:00'";
        else if (tn=="time") oss << "'00:00:00'";
        else if (tn=="year") oss << "1970";
        else oss << "0";
      }
      oss << ")";
  };

  // JSON/spatial: prefer safe whitelist checks; avoid a second NULL predicate when budget exhausted
  if (tn=="json"){
     if (p->scope->schema && p->scope->schema->mysql_mode) {
        if (used_nullness >= 1) oss << "JSON_VALID(" << alias_col << ") = 1";
        else oss << "JSON_TYPE(" << alias_col << ") IS NOT NULL";
     } else {
        oss << alias_col << " IS NOT NULL";
     }
     return oss.str();
  }
  if (tn=="geometry"){
     if (p->scope->schema && p->scope->schema->mysql_mode) {
        if (used_nullness >= 1) oss << "ST_IsValid(" << alias_col << ") = 1";
        else oss << "ST_SRID(" << alias_col << ") = 4326";
     } else {
        oss << alias_col << " IS NOT NULL";
     }
     return oss.str();
  }

  // For numeric/string/time/blob: use dataset-driven constants when available
  if (sl && !sl->values.empty()) {
      TypeFamily fam = sl->family;
      size_t maxn = 3;
      bool use_in = sl->values.size() > 1;
      if (fam == TypeFamily::String) {
         // Occasionally use LIKE for strings when appropriate (use first sample)
         for (const auto &v: sl->values) {
           if (!v.is_null && !v.literal.empty() && v.literal.front()=='\'' ) {
             // Build a simple LIKE pattern using the sample literal
             oss << alias_col << " LIKE " << v.literal; // match whole; minimal footprint
             return oss.str();
           }
         }
      }
      if (use_in) {
         build_diverse_in(sl->values, maxn);
      } else {
         const Value &v = sl->values[0];
         oss << alias_col << " = " << v.literal;
      }
      return oss.str();
  } else {
      // Fallback when no samples: construct a safe predicate based on type
      if (t_is_num(picked_type)) {
          oss << alias_col << " = 0";
      } else if (t_is_text(picked_type)) {
          oss << alias_col << " = ''";
      } else if (t_is_time(picked_type)) {
          // use a generic non-null check to avoid malformed literals across engines
          oss << alias_col << " IS NOT NULL";
      } else if (t_is_bin(picked_type)) {
          oss << "LENGTH(" << alias_col << ") >= 0";
      } else {
          oss << alias_col << " IS NOT NULL";
      }
      return oss.str();
  }
}

using namespace std;
using impedance::matched;

// ===================== 窗口函数与聚合限制 =====================
// MySQL 8.0 窗口函数白名单（允许携带 OVER）
static const std::unordered_set<std::string> kWindowWhitelist = {
  // 既有窗口函数
  "ROW_NUMBER","RANK","DENSE_RANK","LEAD","LAG",
  // 新增：聚合窗口
  "SUM","AVG","COUNT","MIN","MAX","STDDEV","VARIANCE",
  // 新增：排名/分布
  "CUME_DIST","PERCENT_RANK","NTILE",
  // 新增：值函数
  "FIRST_VALUE","LAST_VALUE","NTH_VALUE"
};
// MySQL 8.0 聚合函数黑名单（禁止携带 OVER；同时禁止 JSON 聚合与文本聚合窗口化）
static const std::unordered_set<std::string> kWindowAggBlacklist = {
  // 仍禁止窗口化的聚合（保持与 MySQL 8 兼容）
  "STDDEV_POP","STDDEV_SAMP",
  "VAR_POP","VAR_SAMP",
  // JSON/GROUP 聚合禁止窗口化
  "JSON_ARRAYAGG","JSON_OBJECTAGG","GROUP_CONCAT"
};
// 窗口专用函数（不得作为普通聚合使用）：排名/值类 + 统计窗口（STDDEV/VARIANCE）
static const std::unordered_set<std::string> kWindowOnly = {
  "CUME_DIST","PERCENT_RANK","NTILE",
  "FIRST_VALUE","LAST_VALUE","NTH_VALUE",
  "STDDEV","VARIANCE"
};

// SRID=4326 下禁止生成不支持的空间函数（如 ST_Envelope）
static const std::unordered_set<std::string> kGeoSRSBlacklist = {
  "ST_Envelope",
  "ST_Buffer"
};
static inline bool is_window_whitelisted(const std::string& n) { return kWindowWhitelist.count(n)>0; }
static inline bool is_agg_name(const std::string& n) { return kWindowAggBlacklist.count(n)>0; }
// 仅在 SELECT 列项上下文允许窗口函数携带 OVER（保守策略）
static inline bool context_allows_window(prod *p) {
  while (p) {
    if (auto sl = dynamic_cast<select_list *>(p)) {
      return dynamic_cast<query_spec *>(sl->pprod) != nullptr;
    }
    p = p->pprod;
  }
  return false;
}
// 仅在 SELECT 列项允许直接聚合（funcall agg=true）；在其它上下文重试/降级
static inline bool context_allows_agg(prod *p) {
  while (p) {
    if (auto sl = dynamic_cast<select_list *>(p)) {
      return dynamic_cast<query_spec *>(sl->pprod) != nullptr;
    }
    // HAVING 扩展：在 having_guard 上下文允许聚合
    if (dynamic_cast<having_guard *>(p)) {
      return true;
    }
    p = p->pprod;
  }
  return false;
}
// =============================================================================
// 聚合入参类型限制：统一工具函数
static inline bool type_is_numeric(sqltype *t) {
  if (!t) return false;
  auto* TINT = sqltype::get("int");
  auto* TTINY = sqltype::get("tinyint");
  auto* TBIG = sqltype::get("bigint");
  auto* TFLOAT = sqltype::get("float");
  auto* TDOUBLE = sqltype::get("double");
  auto* TDEC = sqltype::get("decimal");
  auto is_any = [&](sqltype* x, std::initializer_list<sqltype*> lst){ for (auto* y: lst){ if (x==y) return true; } return false; };
  return is_any(t,{TINT,TTINY,TBIG,TFLOAT,TDOUBLE,TDEC});
}
// 判断表达式是否为 geometry 类型或来源于 ST_* 函数
static inline bool is_spatial_expr(const value_expr *e) {
  if (!e) return false;
  // Guard: avoid dereferencing potentially invalid string objects; prefer stable pointer identity
  sqltype* t = e->type;
  if (!t) return false;
  // Pointer identity to the canonical geometry type avoids touching t->name
  if (t == sqltype::get("geometry")) return true;
  // Fallback: detect ST_* function origin without touching type strings
  if (auto f = dynamic_cast<const funcall *>(e)) {
    if (f->proc && f->proc->name.size()>=3 && f->proc->name.compare(0,3,"ST_")==0) return true;
  }
  return false;
}
// JSON/GROUP 聚合名判定
static inline bool is_json_or_group_agg(const std::string& n) {
  return n=="JSON_ARRAYAGG" || n=="JSON_OBJECTAGG" || n=="GROUP_CONCAT";
}
// 参数子树聚合/窗口检测：禁止聚合参数中出现任何聚合或窗口函数
static bool contains_agg_or_window(const value_expr* e) {
  if (!e) return false;
  // 允许子查询（atomic_subselect）作为标量参数，它不属于当前查询的聚合上下文
  if (dynamic_cast<const atomic_subselect*>(e)) return false;
  // 直接窗口函数
  if (dynamic_cast<const window_function*>(e)) return true;
  // 直接函数调用
  if (auto f = dynamic_cast<const funcall*>(e)) {
    if (f->is_aggregate) return true;
    for (auto &p : f->parms) {
      if (contains_agg_or_window(p.get())) return true;
    }
    return false;
  }
  // coalesce/nullif
  if (auto c = dynamic_cast<const coalesce*>(e)) {
    for (auto &p : c->value_exprs) {
      if (contains_agg_or_window(p.get())) return true;
    }
    return false;
  }
  // case 表达式
  if (auto cse = dynamic_cast<const case_expr*>(e)) {
    if (contains_agg_or_window(cse->true_expr.get())) return true;
    if (contains_agg_or_window(cse->false_expr.get())) return true;
    // 条件可能是 bool_expr，其子树同样不允许聚合
    if (auto be = dynamic_cast<const bool_expr*>(cse->condition.get())) {
      // bool_binop
      if (auto bb = dynamic_cast<const bool_binop*>(be)) {
        if (contains_agg_or_window(bb->lhs.get())) return true;
        if (contains_agg_or_window(bb->rhs.get())) return true;
      } else if (auto np = dynamic_cast<const null_predicate*>(be)) {
        if (contains_agg_or_window(np->expr.get())) return true;
      } // exists_predicate 持有子查询，允许
    }
    return false;
  }
  // 其它（常量、列引用、json_expr、spatial_expr 等）默认视为非聚合标量
  return false;
}

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint)
{
  // Recursion depth guard: prevent deep retries causing stack overflow or invalid pointer deref; RAII to auto-decrement
  static thread_local int g_expr_depth = 0;
  struct DepthGuard { int* d; DepthGuard(int* dp): d(dp){ ++(*d);} ~DepthGuard(){ --(*d);} } guard(&g_expr_depth);
  try {
    // Depth-based safe fallback
    const int kMaxExprDepth = 6;
    if (g_expr_depth > kMaxExprDepth) {
      sqltype *fallback = type_constraint ? type_constraint : p->scope->schema->inttype;
      return make_shared<const_expr>(p, fallback);
    }

    /*
    cerr << p->level <<","<<type_constraint<<endl;
    if (1 == d20())
        return make_shared<column_reference>(p, type_constraint);
    return make_shared<funcall>(p, type_constraint);
    */

    // 优先遵循类型约束：仅当明确请求 json/geometry 或无约束且密度开关命中时才生成对应扩展表达式（类型守卫）
    if (type_constraint == sqltype::get("json"))
      return make_shared<json_expr>(p, type_constraint);
    if (!type_constraint && p->scope->schema->enable_json && d100() < (int)(p->scope->schema->json_density*100))
      return make_shared<json_expr>(p, type_constraint);
    if (type_constraint == sqltype::get("geometry"))
      return make_shared<spatial_expr>(p, type_constraint);
    if (!type_constraint && p->scope->schema->enable_spatial && d100() < (int)(p->scope->schema->spatial_density*100))
      return make_shared<spatial_expr>(p, type_constraint);

    if (1 == d6() && p->level < d6() && window_function::allowed(p) && p->scope && p->scope->schema && p->scope->schema->feature.window_enabled)
      return make_shared<window_function>(p, type_constraint);
    else if (1 == d42() && p->level < d6())
      return make_shared<coalesce>(p, type_constraint);
    else if (1 == d42() && p->level < d6())
      return make_shared<nullif>(p, type_constraint);
    else if (p->level < d6() && d6() > 2) {
      // Guard: avoid selecting funcall when routines catalogs are empty (e.g., dummy schema fallback)
      bool has_routines = !(p->scope->schema->routines_returning_type.empty()) || !(p->scope->schema->parameterless_routines_returning_type.empty());
      if (has_routines) {
        return make_shared<funcall>(p, type_constraint);
      } else {
        sqltype *fallback = type_constraint ? type_constraint : p->scope->schema->inttype;
        return make_shared<const_expr>(p, fallback);
      }
    }
    else if (d12()==1)
      return make_shared<atomic_subselect>(p, type_constraint);
    else if (p->level< d6() && d9()==1)
      return make_shared<case_expr>(p, type_constraint);
    else if (p->scope->refs.size() && d20() > 1) {
      // Robustness: if a type constraint is given but no matching columns exist, fallback to a literal
      if (type_constraint) {
        auto pairs_check = p->scope->refs_of_type(type_constraint);
        if (pairs_check.empty()) {
          return make_shared<const_expr>(p, type_constraint);
        }
      }
      return make_shared<column_reference>(p, type_constraint);
    }
    else
      return make_shared<const_expr>(p, type_constraint);
    
  } catch (runtime_error &e) {
  }
  p->retry();
  return factory(p, type_constraint);
}

case_expr::case_expr(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  condition = bool_expr::factory(this);
  // 构造分支（防御约束）：true/false 支持空/未知类型的回退
  true_expr = value_expr::factory(this, type_constraint);
  false_expr = value_expr::factory(this, true_expr ? true_expr->type : type_constraint);

  // 一致性安全包装与公共类型推导（最小改动，架构不变）
  auto family = [&](sqltype* t){ return safe_type_family(t); };
  auto consistent_safe = [&](sqltype* lhs, sqltype* rhs)->bool {
      if (!lhs || !rhs) return false;
      if (family(lhs)==SafeTypeFamily::Unknown || family(rhs)==SafeTypeFamily::Unknown) return false;
      return lhs->consistent(rhs);
  };
  auto pick_default = [&]()->sqltype* {
      if (type_constraint) return type_constraint;
      return scope && scope->schema ? scope->schema->inttype : sqltype::get("int");
  };
  auto common_type = [&](sqltype* a, sqltype* b)->sqltype* {
      if (!a || !b) return nullptr;
      if (a==b) return a;
      if (consistent_safe(a,b)) return b;
      if (consistent_safe(b,a)) return a;
      // 同族时选择规范类型（按值映射），避免引用易失字符串
      SafeTypeFamily fa = family(a), fb = family(b);
      if (fa != SafeTypeFamily::Unknown && fa == fb) {
          switch (fa) {
              case SafeTypeFamily::Numeric: return sqltype::get("int");
              case SafeTypeFamily::String: return sqltype::get("varchar");
              case SafeTypeFamily::Datetime: return sqltype::get("datetime");
              case SafeTypeFamily::Timestamp: return sqltype::get("timestamp");
              case SafeTypeFamily::Date: return sqltype::get("date");
              case SafeTypeFamily::Time: return sqltype::get("time");
              case SafeTypeFamily::Json: return sqltype::get("json");
              case SafeTypeFamily::Geometry: return sqltype::get("geometry");
              default: break;
          }
      }
      return nullptr;
  };

  sqltype* t_true = true_expr ? true_expr->type : nullptr;
  sqltype* t_false = false_expr ? false_expr->type : nullptr;

  sqltype* res = common_type(t_true, t_false);
  if (!res) res = pick_default();

  // 严格类型检查开关：强制分支统一到公共类型（仅日志标记）
  bool strict = (scope && scope->schema) ? scope->schema->feature.strict_case_type_check : false;
  if (strict) {
      if (!true_expr || true_expr->type != res) true_expr = value_expr::factory(this, res);
      if (!false_expr || false_expr->type != res) false_expr = value_expr::factory(this, res);
      if (scope && scope->schema && (scope->schema->verbose || scope->schema->condgen_debug)) {
        std::cerr << "[case_expr] strict_type_check unify to " << safe_type_name(res) << std::endl;
      }
  } else {
      // 非严格：最小必要调整
      if (t_true && t_false && t_true != t_false) {
          if (consistent_safe(t_true,t_false)) {
              // 采用 false 分支类型
              true_expr = value_expr::factory(this, t_false);
          } else if (consistent_safe(t_false,t_true)) {
              false_expr = value_expr::factory(this, t_true);
          } else {
              // 均不一致：统一回退到公共类型/默认类型
              true_expr = value_expr::factory(this, res);
              false_expr = value_expr::factory(this, res);
          }
      } else {
          // 任一为空或未知：统一回退
          if (!t_true || family(t_true)==SafeTypeFamily::Unknown) true_expr = value_expr::factory(this, res);
          if (!t_false || family(t_false)==SafeTypeFamily::Unknown) false_expr = value_expr::factory(this, res);
      }
  }

  type = (true_expr && true_expr->type) ? true_expr->type : res;
}

void case_expr::out(std::ostream &out)
{
  out << "case when " << *condition;
  out << " then " << *true_expr;
  out << " else " << *false_expr;
  out << " end";
  indent(out);
}

void case_expr::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
  true_expr->accept(v);
  false_expr->accept(v);
}

column_reference::column_reference(prod *p, sqltype *type_constraint) : value_expr(p)
{
  if (type_constraint) {
    auto pairs = scope->refs_of_type(type_constraint);
    if (!pairs.empty()) {
      auto picked = random_pick(pairs);
      reference += picked.first->ident()
        + "." + picked.second.name;
      type = picked.second.type;
      assert(type_constraint->consistent(type));
    } else {
      // Fallback: pick any available column reference to avoid empty set crash; downstream may adjust types via consistency
      if (!scope->refs.empty()) {
        named_relation *r = random_pick(scope->refs);
        if (r && !r->columns().empty()) {
          reference += r->ident() + ".";
          column &c = random_pick(r->columns());
          type = c.type;
          reference += c.name;
        } else {
          // As last resort, synthesize a safe const_expr via COALESCE/NULLIF path by delegating to factory
          auto fallback = make_shared<const_expr>(p, type_constraint);
          // Represent constant via reference text to keep printing valid; use CAST for minimal intrusion
          reference = fallback->expr; // constant literal
          type = type_constraint ? type_constraint : scope->schema->inttype;
        }
      } else {
        auto fallback = make_shared<const_expr>(p, type_constraint);
        reference = fallback->expr;
        type = type_constraint ? type_constraint : scope->schema->inttype;
      }
    }
  } else {
    named_relation *r = random_pick(scope->refs);

    reference += r->ident() + ".";
    column &c = random_pick(r->columns());
    type = c.type;
    reference += c.name;
  }
}

shared_ptr<bool_expr> bool_expr::factory(prod *p)
{
  try {
       if (p->level > d100())
            return make_shared<truth_value>(p);
       if(d6() < 4)
            return make_shared<comparison_op>(p);
       else if (d6() < 4)
            return make_shared<bool_term>(p);
       else if (d6() < 4)
            return make_shared<null_predicate>(p);
       else if (d6() < 4)
            return make_shared<truth_value>(p);
       else 
            return make_shared<exists_predicate>(p);
       //return make_shared<distinct_pred>(p);
  } catch (runtime_error &e) {
  }
  p->retry();
  return factory(p);
    
}

exists_predicate::exists_predicate(prod *p) : bool_expr(p)
{
  subquery = make_shared<query_spec>(this, scope);
}

void exists_predicate::accept(prod_visitor *v)
{
  v->visit(this);
  subquery->accept(v);
}

void exists_predicate::out(std::ostream &out)
{
  out << "EXISTS (";
  indent(out);
  out << *subquery << ")";
}

distinct_pred::distinct_pred(prod *p) : bool_binop(p)
{
  lhs = make_shared<column_reference>(p);
  rhs = make_shared<column_reference>(p, lhs->type);
}

comparison_op::comparison_op(prod *p) : bool_binop(p)
{
  auto &idx = p->scope->schema->operators_returning_type;

  auto iters = idx.equal_range(scope->schema->booltype);
  oper = random_pick(random_pick(iters)->second);

  lhs = value_expr::factory(this, oper->left);
  rhs = value_expr::factory(this, oper->right);

  if (oper->left == oper->right
         && lhs->type != rhs->type) {

    if (lhs->type->consistent(rhs->type))
      lhs = value_expr::factory(this, rhs->type);
    else
      rhs = value_expr::factory(this, lhs->type);
  }
}

coalesce::coalesce(prod *p, sqltype *type_constraint, const char *abbrev)
     : value_expr(p), abbrev_(abbrev)
{
  // 构造入参，优先采用类型约束；若首参类型未知，则第二参以约束类型回退
  auto first_expr = value_expr::factory(this, type_constraint);
  sqltype* t1 = first_expr ? first_expr->type : nullptr;
  auto second_expr = value_expr::factory(this, t1 ? t1 : type_constraint);
  sqltype* t2 = second_expr ? second_expr->type : nullptr;

  // 一致性安全判定与公共类型挑选（最小修复，不触及类型系统重构）
  auto family = [&](sqltype* t){ return safe_type_family(t); };
  auto consistent_safe = [&](sqltype* a, sqltype* b)->bool {
    if (!a || !b) return false;
    if (family(a)==SafeTypeFamily::Unknown || family(b)==SafeTypeFamily::Unknown) return false;
    return a->consistent(b);
  };
  auto pick_default = [&]()->sqltype* {
    if (type_constraint) return type_constraint;
    // 为更通用的 COALESCE 输出，默认偏向 varchar；若不可用则回退到整型
    sqltype* v = sqltype::get("varchar");
    if (v) return v;
    return (scope && scope->schema && scope->schema->inttype) ? scope->schema->inttype : sqltype::get("int");
  };
  auto common_type = [&](sqltype* a, sqltype* b)->sqltype* {
    if (!a && !b) return nullptr;
    if (!a) return b;
    if (!b) return a;
    if (a == b) return a;
    if (consistent_safe(a,b)) return b;
    if (consistent_safe(b,a)) return a;
    SafeTypeFamily fa = family(a), fb = family(b);
    if (fa != SafeTypeFamily::Unknown && fa == fb) {
      switch (fa) {
        case SafeTypeFamily::Numeric: return sqltype::get("int");
        case SafeTypeFamily::String: return sqltype::get("varchar");
        case SafeTypeFamily::Datetime: return sqltype::get("datetime");
        case SafeTypeFamily::Timestamp: return sqltype::get("timestamp");
        case SafeTypeFamily::Date: return sqltype::get("date");
        case SafeTypeFamily::Time: return sqltype::get("time");
        case SafeTypeFamily::Json: return sqltype::get("json");
        case SafeTypeFamily::Geometry: return sqltype::get("geometry");
        default: break;
      }
    }
    return nullptr;
  };

  // 统一重试逻辑：在可一致时拉齐，否则统一降级到公共/默认类型
  retry_limit = 20;
  int guard = retry_limit;
  while (guard-- > 0) {
    if (t1 == t2 && t1) break; // 已一致且非空
    if (t1 && t2) {
      if (consistent_safe(t1, t2)) {
        retry();
        first_expr = value_expr::factory(this, t2);
      } else if (consistent_safe(t2, t1)) {
        retry();
        second_expr = value_expr::factory(this, t1);
      } else {
        sqltype* target = common_type(t1, t2);
        if (!target) target = pick_default();
        retry();
        first_expr = value_expr::factory(this, target);
        second_expr = value_expr::factory(this, target);
      }
    } else {
      // 任一为空：以另一侧或默认类型回填
      sqltype* target = t1 ? t1 : (t2 ? t2 : pick_default());
      retry();
      if (!t1) first_expr = value_expr::factory(this, target);
      if (!t2) second_expr = value_expr::factory(this, target);
    }
    t1 = first_expr ? first_expr->type : nullptr;
    t2 = second_expr ? second_expr->type : nullptr;
    if (!t1 && !t2) break; // 极端失败，退出
  }

  // 最终类型选择：优先第二参，其次公共/默认类型；并记录调试信息（不致崩溃）
  sqltype* final_type = nullptr;
  if (t1 && t2 && t1 == t2) final_type = t2;
  if (!final_type) final_type = common_type(t1, t2);
  if (!final_type) final_type = pick_default();
  type = final_type;

  if (scope && scope->schema && (scope->schema->verbose || scope->schema->condgen_debug)) {
    std::cerr << "[coalesce] t1=" << safe_type_name(t1)
              << ", t2=" << safe_type_name(t2)
              << ", final=" << safe_type_name(type) << std::endl;
  }

  value_exprs.push_back(first_expr);
  value_exprs.push_back(second_expr);
}
 
void coalesce::out(std::ostream &out)
{
  out << abbrev_ << "(";
  for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
    out << **expr;
    if (expr+1 != value_exprs.end())
      out << ",", indent(out);
  }
  out << ")";
}

const_expr::const_expr(prod *p, sqltype *type_constraint)
    : value_expr(p), expr("")
{
  // Prefer canonical types; if constraint is null or suspicious, fall back to inttype
  type = type_constraint ? type_constraint : scope->schema->inttype;
  auto* TINT = sqltype::get("int");
  auto* TTINY = sqltype::get("tinyint");
  auto* TSMALL = sqltype::get("smallint");
  auto* TMED = sqltype::get("mediumint");
  auto* TBIG = sqltype::get("bigint");
  auto* TFLOAT = sqltype::get("float");
  auto* TDOUBLE = sqltype::get("double");
  auto* TDEC = sqltype::get("decimal");
  auto* TVARCHAR = sqltype::get("varchar");
  auto* TDATE = sqltype::get("date");
  auto* TTIME = sqltype::get("time");
  auto* TDATETIME = sqltype::get("datetime");
  auto* TSTAMP = sqltype::get("timestamp");
  auto* TJSON = sqltype::get("json");
  auto* TGEOM = sqltype::get("geometry");
  auto is_any = [&](sqltype* t, std::initializer_list<sqltype*> lst){ for (auto* x: lst){ if (t==x) return true; } return false; };

  // Numeric (integer family)
  if (is_any(type,{TINT,TTINY,TSMALL,TMED,TBIG})) {
    expr = std::to_string(d100());
    return;
  }
  // Boolean
  if (type == scope->schema->booltype) {
    bool allow_false = (scope && scope->schema) ? scope->schema->feature.allow_false_cond : true;
    expr += (allow_false ? ((d6() > 3) ? scope->schema->true_literal : scope->schema->false_literal) : scope->schema->true_literal);
    return;
  }
  // INSERT default
  if (dynamic_cast<insert_stmt*>(p) && (d6() > 3)) {
    expr += "default";
    return;
  }
  // Floating / decimal
  if (is_any(type,{TFLOAT,TDOUBLE,TDEC})) {
    expr = std::to_string(d100()) + "." + std::to_string(d6());
    return;
  }
  // String
  if (type == TVARCHAR) {
    expr = "'s" + std::to_string(d100()) + "'";
    return;
  }
  // Date
  if (type == TDATE) {
    int y = 2000 + (d100() % 25);
    int m = 1 + (d12() % 12);
    int dd = 1 + (d20() % 28);
    std::string d = std::string("'") + std::to_string(y) + "-" + (m<10?"0":"") + std::to_string(m) + "-" + (dd<10?"0":"") + std::to_string(dd) + "'";
    expr = std::string("CAST(") + d + " AS DATE)";
    return;
  }
  // Time
  if (type == TTIME) {
    int h = d20() % 24, mi = d100() % 60, s = d100() % 60;
    std::string t = std::string("'") + (h<10?"0":"") + std::to_string(h) + ":" + (mi<10?"0":"") + std::to_string(mi) + ":" + (s<10?"0":"") + std::to_string(s) + "'";
    expr = std::string("CAST(") + t + " AS TIME)";
    return;
  }
  // Datetime / Timestamp
  if (is_any(type,{TDATETIME,TSTAMP})) {
    int y = 2000 + (d100() % 25);
    int m = 1 + (d12() % 12);
    int dd = 1 + (d20() % 28);
    int h = d20() % 24, mi = d100() % 60, s = d100() % 60;
    std::string dt = std::string("'") + std::to_string(y) + "-" + (m<10?"0":"") + std::to_string(m) + "-" + (dd<10?"0":"") + std::to_string(dd) + " "
        + (h<10?"0":"") + std::to_string(h) + ":" + (mi<10?"0":"") + std::to_string(mi) + ":" + (s<10?"0":"") + std::to_string(s) + "'";
    // MySQL: prefer CAST(... AS DATETIME) for both DATETIME and TIMESTAMP (TIMESTAMP cast is not supported)
    expr = std::string("CAST(") + dt + " AS DATETIME)";
    return;
  }
  // JSON
  if (type == TJSON) {
    expr = "CAST('{}' AS JSON)";
    return;
  }
  // Geometry
  if (type == TGEOM) {
    expr = "ST_SRID(ST_GeomFromText('POINT(0 0)', 4326), 4326)";
    return;
  }
  // Fallback: unknown/suspicious type — emit NULL
  expr += "NULL";
}

funcall::funcall(prod *p, sqltype *type_constraint, bool agg)
  : value_expr(p), is_aggregate(agg)
{
  if (type_constraint == scope->schema->internaltype)
    fail("cannot call functions involving internal type");
  auto &idx = agg ? p->scope->schema->aggregates_returning_type
    : (3 < d6()) ?
    p->scope->schema->routines_returning_type
    : p->scope->schema->parameterless_routines_returning_type;
 retry:
  if (!type_constraint) {
    proc = random_pick(random_pick(idx.begin(), idx.end())->second);
  } else {
    auto iters = idx.equal_range(type_constraint);
    proc = random_pick(random_pick(iters)->second);
    if (proc && !type_constraint->consistent(proc->restype)) {
      retry();
      goto retry;
    }
  }

  if (!proc) {
    retry();
    goto retry;
  }
  // 开关一致性：在函数选择阶段统一剔除 FOUND_ROWS（不依赖注册时机）
  if (scope && scope->schema && scope->schema->feature.exclude_found_rows) {
    if (proc) {
      auto up = [](std::string s){ for (auto &c : s) c = (char)std::toupper((unsigned char)c); return s; };
      if (up(proc->name) == std::string("FOUND_ROWS")) {
        retry();
        goto retry;
      }
    }
  }
  // 统一门控：禁用不确定系统函数（默认关闭，名称大小写不敏感）
  if (scope && scope->schema && scope->schema->feature.disable_nondeterministic_funcs) {
    auto up = [](std::string s){ for (auto &c : s) c = (char)std::toupper((unsigned char)c); return s; };
    static const std::unordered_set<std::string> kNonDeterministicFuncs = {
      // MySQL/ANSI 常见
      "CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","LOCALTIME","LOCALTIMESTAMP","NOW","SYSDATE","CURDATE","CURTIME",
      // UTC 系列与 UNIX 时间戳
      "UTC_DATE","UTC_TIME","UTC_TIMESTAMP","UNIX_TIMESTAMP",
      // 随机/唯一值
      "RAND","RANDOM","UUID","UUID_SHORT","UUID_TO_BIN","BIN_TO_UUID",
      // 连接/会话/环境
      "CONNECTION_ID","FOUND_ROWS","LAST_INSERT_ID","ROW_COUNT","VERSION","DATABASE","SCHEMA","USER","CURRENT_USER","SESSION_USER","SYSTEM_USER",
      // 锁/状态/延时
      "GET_LOCK","RELEASE_LOCK","IS_FREE_LOCK","IS_USED_LOCK","SLEEP","BENCHMARK",
      // 其他可能不确定/环境依赖
      "MASTER_POS_WAIT","LOAD_FILE","CONNECTION_PROPERTY","PS_CURRENT_THREAD_ID",
      // PostgreSQL 常见（兼容门控）
      "CLOCK_TIMESTAMP","TRANSACTION_TIMESTAMP","TIMEOFDAY"
    };
    if (proc) {
      std::string pname = up(proc->name);
      if (kNonDeterministicFuncs.count(pname)>0) {
        retry();
        goto retry;
      }
    }
  }

  // 在窗口上下文禁止黑名单聚合（JSON_ARRAYAGG/JSON_OBJECTAGG/GROUP_CONCAT）
  if (is_aggregate && dynamic_cast<window_function*>(p) != nullptr && proc) {
    if (kWindowAggBlacklist.count(proc->name)) {
      retry();
	  // 重新选择其他合法窗口函数
      goto retry; 
    }
  }

  // 非窗口上下文中禁止选择窗口专用函数作为聚合
  if (is_aggregate && (nullptr == dynamic_cast<window_function*>(p)) && proc) {
    if (kWindowOnly.count(proc->name)) {
      retry();
	  // 重新选择非窗口专用的聚合函数
      goto retry; 
    }
  }
  // 上下文限制：除 SELECT 列项外禁止直接聚合（funcall agg=true）
  if (is_aggregate && !context_allows_agg(p)) {
    retry();
    goto retry;
  }
  // 非窗口上下文禁止选择窗口专用或窗口白名单函数作为普通例程
  if (!is_aggregate && (nullptr == dynamic_cast<window_function*>(p)) && proc) {
    const std::string nm = proc->name;
    if (kWindowOnly.count(nm) || is_window_whitelisted(nm)) {
      retry();
      goto retry;
    }
  }

  if (type_constraint)
    type = type_constraint;
  else
    type = proc->restype;

  if (type == scope->schema->internaltype) {
    retry();
    goto retry;
  }

  for (auto type : proc->argtypes)
    if (type == scope->schema->internaltype
        || type == scope->schema->arraytype) {
      retry();
      goto retry;
    }
  
  for (auto argtype : proc->argtypes) {
    assert(argtype);
    auto expr = value_expr::factory(this, argtype);
    // 聚合函数仅允许数值入参；禁止 geometry/ST_* 作为入参
    if (is_aggregate && proc) {
      const std::string fname = proc->name;
      if (!is_json_or_group_agg(fname)) {
        bool arg_is_spatial = false;
        if (expr && expr->type) { arg_is_spatial = is_spatial_expr(expr.get()); }
        if (arg_is_spatial || !type_is_numeric(argtype)) {
          sqltype *fallback = scope->schema->inttype;
          expr = make_shared<const_expr>(this, fallback);
        }
      } else {
        // JSON/GROUP 聚合同样禁止空间类型与 ST_* 来源入参
        bool arg_is_spatial = false;
        if (expr && expr->type) { arg_is_spatial = is_spatial_expr(expr.get()); }
        if (arg_is_spatial) {
          expr = make_shared<const_expr>(this, argtype ? argtype : scope->schema->inttype);
        }
      }
      // 聚合参数内禁止出现任何聚合或窗口函数（递归检查）
      if (contains_agg_or_window(expr.get())) {
        sqltype *fallback = argtype ? argtype : scope->schema->inttype;
        expr = make_shared<const_expr>(this, fallback);
      }
    }
    // 数学函数入参拦截：若入参为 geometry/ST_*，统一回退为数值常量/列
    if (proc && kWindowWhitelist.size() /* use presence of header */) {
      static const std::unordered_set<std::string> kMathWhitelist = {
        "ABS","SQRT","ROUND","FLOOR","CEIL","CEILING","EXP",
        "LOG","LOG10","LOG2","LN","POW","POWER","DEGREES","RADIANS",
        "SIN","COS","TAN","ASIN","ACOS","ATAN","SIGN"
      };
      bool arg_is_spatial = false;
      if (expr && expr->type) { arg_is_spatial = is_spatial_expr(expr.get()); }
      if (kMathWhitelist.count(proc->name) && arg_is_spatial) {
        sqltype *fallback = type_is_numeric(argtype) ? argtype : scope->schema->inttype;
        expr = make_shared<const_expr>(this, fallback);
      }
    }
    // FORMAT/GREATEST/LEAST/CONV/COT 参数类型防呆
    if (proc) {
      const std::string fname2 = proc->name;
      size_t idx = parms.size();
      if (fname2 == "FORMAT") {
        if (idx == 0) {
          bool bad = ((expr->type && is_spatial_expr(expr.get())) || !(type_is_numeric(argtype)));
          if (bad) { sqltype *fallback = scope->schema->inttype; expr = make_shared<const_expr>(this, fallback); }
        } else if (idx == 1) {
          if (!(argtype && (safe_type_name(argtype)=="int" || safe_type_name(argtype)=="tinyint" || safe_type_name(argtype)=="smallint" || safe_type_name(argtype)=="mediumint" || safe_type_name(argtype)=="bigint"))) {
            expr = make_shared<const_expr>(this, scope->schema->inttype);
          }
        } else if (idx == 2) {
          if (!(argtype && safe_type_name(argtype) == std::string("varchar"))) {
            expr = make_shared<const_expr>(this, sqltype::get("varchar"));
          }
        }
      } else if (fname2 == "GREATEST" || fname2 == "LEAST") {
        if (idx == 1 && !proc->argtypes.empty()) {
          sqltype* first = proc->argtypes[0];
          if (first != argtype) {
            expr = make_shared<const_expr>(this, first);
          }
        }
      } else if (fname2 == "COT") {
        if (!type_is_numeric(argtype) || (expr->type && is_spatial_expr(expr.get()))) {
          expr = make_shared<const_expr>(this, scope->schema->inttype);
        }
      } else if (fname2 == "CONV") {
        if (idx == 0) {
          if (!(argtype && (safe_type_name(argtype) == std::string("varchar") || type_is_numeric(argtype)))) {
            expr = make_shared<const_expr>(this, sqltype::get("varchar"));
          }
        } else if (idx == 1 || idx == 2) {
          if (!type_is_numeric(argtype)) {
            expr = make_shared<const_expr>(this, scope->schema->inttype);
          }
        }
      }
    }
    parms.push_back(expr);
  }
}

void funcall::out(std::ostream &out)
{
  const std::string pname = proc->name;
  // 特殊输出：仅作为聚合函数；按 MySQL 语法在函数内部打印 ORDER BY 与 SEPARATOR
  if (pname == "GROUP_CONCAT" && scope && scope->schema && scope->schema->mysql_mode) {
    out << proc->ident() << "(";
    if (!parms.empty()) { indent(out); out << *parms[0]; }
    // 选择一个 ORDER BY 列（优先最近别名的首列），失败回退为常量表达式
    std::string order_expr;
    try {
      named_relation* nr = (scope && !scope->refs.empty()) ? scope->refs.back() : nullptr;
      if (nr && !nr->columns().empty()) {
        const column &c = nr->columns().front();
        order_expr = nr->ident() + std::string(".") + c.name;
      }
    } catch (...) {
      // ignore
    }
    if (order_expr.empty()) order_expr = std::string("CAST(0 AS SIGNED)");
    out << " ORDER BY " << order_expr << " SEPARATOR ','";
    out << ")";
    return;
  }
  out << proc->ident() << "(";
  // 聚合参数打印前的防御性断言：不允许子树含聚合/窗口
  if (is_aggregate) {
    for (auto it = parms.begin(); it != parms.end(); ++it) {
      assert(!contains_agg_or_window(it->get()));
    }
  }
  // Special-case MySQL 8.0 JSON & spatial functions for strict signatures
  if (pname == "JSON_CONTAINS_PATH") {
    // JSON_CONTAINS_PATH(json_doc, 'one'|'all', path[, path...]) — 第二参必须为常量 'one' 或 'all'
    out << "JSON_OBJECT('k0', 1, 'k1', 2), '" << (d6() > 3 ? "all" : "one") << "', '$." << (d6() > 3 ? "k1" : "k0") << "'";
    out << ")";
    return;
  }
  if (pname == "JSON_EXTRACT") {
    // 第 1 参必须是合法 JSON 文档；路径必须是常量 '$.key' 或 '$[n]'
    if (d6() > 3) {
      out << "JSON_OBJECT('k0', 66, 'k1', 75), '$.k1'";
    } else {
      out << "JSON_ARRAY(1,2,3), '$[1]'";
    }
    out << ")";
    return;
  }
  if (pname == "JSON_SCHEMA_VALIDATION_REPORT") {
    // Arg1: schema object; Arg2: instance object（均为 JSON 对象）
    out << "JSON_OBJECT('type','object','properties',JSON_OBJECT('a',JSON_OBJECT('type','integer'))), JSON_OBJECT('x', 1)";
    out << ")";
    return;
  }
  if (pname == "ST_GeomFromGeoJSON") {
    // 统一 options=1（数值型，不加引号）；SRID 由上层逻辑通过 ST_SRID(geom, srid) 包装
    std::ostringstream doc;
    // [FIX][SRID-Range-Check] GeoJSON 坐标统一 clamp，保持 [lon,lat] 顺序
    doc << "{\"type\":\"Point\",\"coordinates\":[" << (int)clamp_lon((int)d100()) << "," << (int)clamp_lat((int)d100()) << "]}"; // [FIX][LonLat-Order]
    out << "ST_SRID(ST_GeomFromGeoJSON('" << doc.str() << "', 1), 4326)";
    return;
  }
  // 额外白名单：JSON_KEYS 仅允许 '$' 作为 path；JSON_OVERLAPS 两参必须是 JSON 文档
  if (pname == "JSON_KEYS") {
    out << "JSON_OBJECT('k0',1,'k1',2), '$'";
    out << ")";
    return;
  }
  if (pname == "JSON_OVERLAPS") {
    out << "JSON_OBJECT('a', 1), JSON_ARRAY('x','y')";
    out << ")";
    return;
  }
  if (pname == "STR_TO_DATE") {
    out << "'2004-02-08 20:18:37', '%Y-%m-%d %H:%i:%s'";
    out << ")";
    return;
  }
  // Special-case: CONV(N, from_base, to_base) — clamp bases to [2,36] and cast N to CHAR
  if (pname == "CONV" && parms.size() == 3) {
    out << "CAST(" << *parms[0] << " AS CHAR), ";
    out << "LEAST(GREATEST(CAST(" << *parms[1] << " AS SIGNED), 2), 36), ";
    out << "LEAST(GREATEST(CAST(" << *parms[2] << " AS SIGNED), 2), 36)";
    out << ")";
    return;
  }
  for (size_t i = 0; i < parms.size(); ++i) {
    indent(out);
    // 针对 NTILE 与 NTH_VALUE 的入参安全修正：确保整数常量范围
    if (pname == "NTILE" && i == 0) {
      int buckets = 2 + (d100() % 127); // 2..128
      out << buckets;
      if (i+1 != parms.size()) out << ",";
      continue;
    }
    if (pname == "NTH_VALUE" && i == 1) {
      int nth = 1 + (d100() % 128); // 1..128
      out << nth;
      if (i+1 != parms.size()) out << ",";
      continue;
    }
    auto &arg = parms[i];
    std::string target = proc->argtypes.size() > i ? safe_type_name(proc->argtypes[i]) : safe_type_name(arg->type);
    auto emit_cast = [&](const std::string &t) {
      std::string argn = safe_type_name(arg->type);
      if (t == argn) {
        out << *arg;
      } else {
        // Guard: prevent illegal casts from geometry to numeric/string/time types
        if (argn == std::string("geometry")) {
          if (t == std::string("geometry")) { out << *arg; return; }
          if (t == std::string("varchar")) { out << "ST_AsText(" << *arg << ")"; return; }
          // Do not attempt to cast geometry to other types
          out << *arg; return;
        }
        // MySQL CAST 目标类型白名单映射：
        // 整型 → SIGNED
        // 浮点 → FLOAT/DOUBLE；DECIMAL → DECIMAL
        // 字符 → CHAR
        // 日期/时间 → DATE/TIME/DATETIME（TIMESTAMP 不允许，统一用 DATETIME）
        // JSON：保持安全策略，使用 JSON_QUOTE(CAST(... AS CHAR))
        std::string cast_type = "CHAR";
        if (t == "int" || t == "tinyint" || t == "smallint" || t == "mediumint" || t == "bigint" || t == "boolean") {
          cast_type = "SIGNED";
        } else if (t == "float") {
          cast_type = "FLOAT";
        } else if (t == "double") {
          cast_type = "DOUBLE";
        } else if (t == "decimal") {
          cast_type = "DECIMAL";
        } else if (t == "varchar") {
          cast_type = "CHAR";
        } else if (t == "date") {
          cast_type = "DATE";
        } else if (t == "time") {
          cast_type = "TIME";
        } else if (t == "datetime") {
          cast_type = "DATETIME";
        } else if (t == "timestamp") {
          // MySQL 不支持 CAST(... AS TIMESTAMP)，统一替换为 DATETIME
          cast_type = "DATETIME";
        } else if (t == "json") {
          // 为避免生成非法 JSON 文本，统一采用 JSON_QUOTE 包裹
          out << "JSON_QUOTE(CAST(" << *arg << " AS CHAR))"; return;
        } else if (t == "geometry") {
          out << "ST_GeomFromText(" << *arg << ")"; return;
        }
        out << "CAST(" << *arg << " AS " << cast_type << ")";
      }
    };
    emit_cast(target);
    if (i+1 != parms.size())
      out << ",";
  }

  // COUNT 支持 COUNT(*)；其它零参窗口函数（如 CUME_DIST/PERCENT_RANK）不追加 *
  if (is_aggregate && (parms.begin() == parms.end()) && pname == "COUNT")
    out << "*";
  out << ")";
}

atomic_subselect::atomic_subselect(prod *p, sqltype *type_constraint)
  : value_expr(p), offset((d6() == 6) ? d100() : d6())
{
  match();
  if (d6() < 3) {
    if (type_constraint) {
      auto idx = scope->schema->aggregates_returning_type;
      auto iters = idx.equal_range(type_constraint);
      agg = random_pick(random_pick(iters)->second);
    } else {
      agg = &random_pick<>(scope->schema->aggregates);
    }
    int guard = 0;
    // 子查询上下文禁止选择窗口专用函数作为聚合（必须带 OVER）
    while (agg && kWindowOnly.count(agg->name)) {
      if (++guard > 16) { agg = 0; break; }
      retry();
      if (type_constraint) {
        auto idx = scope->schema->aggregates_returning_type;
        auto iters = idx.equal_range(type_constraint);
        agg = random_pick(random_pick(iters)->second);
      } else {
        agg = &random_pick<>(scope->schema->aggregates);
      }
    }
    if (agg && agg->argtypes.size() != 1)
      agg = 0;
    else if (agg)
      type_constraint = agg->argtypes[0];
  } else {
    agg = 0;
  }

  if (type_constraint) {
    auto idx = scope->schema->tables_with_columns_of_type;
    col = 0;
    auto iters = idx.equal_range(type_constraint);
    tab = random_pick(random_pick(iters)->second);

    for (auto &cand : tab->columns()) {
      if (type_constraint->consistent(cand.type)) {
        col = &cand;
        break;
      }
    }
    assert(col);
  } else {
    tab = &random_pick<>(scope->schema->tables);
    col = &random_pick<>(tab->columns());
  }

  type = agg ? agg->restype : col->type;
}

void atomic_subselect::out(std::ostream &out)
{
  out << "(select ";

  if (agg)
    out << agg->ident() << "(" << col->name << ")";
  else
    out << col->name;
  
  out 
  << " from " << tab->ident();

  bool force_ord = (scope && scope->schema) ? scope->schema->feature.force_order_by : false;
  if (force_ord) {
    out << " order by 1";
  }

  if (!agg) {
    out << " limit 1 offset " << offset;
  }

  out << ")";
  indent(out);
}

void ensure_non_empty_window_spec(window_function* wf){
  if (!wf) return;
  size_t pc = wf->partition_by.size();
  size_t oc = wf->order_by.size();
  if (pc == 0 && oc == 0) {
    try {
      // 优先从作用域选取一个可用列；若作用域为空，column_reference 内部将安全回退为常量文本
      auto cref = std::make_shared<column_reference>(wf);
      wf->order_by.push_back(cref);
    } catch (...) {
      // 保持空，交由 out() 阶段打印常量兜底
    }
  }
}

void window_function::out(std::ostream &out)
{
  indent(out);
  out << *aggregate;
  if (suppress_over) {
    // 统一降级：不打印 OVER 子句
    return;
  }
  // 空子句防护与最小合法表示：根据 partition_by/order_by 是否为空打印
  // 统一保障：至少包含一个 ORDER BY 或 PARTITION BY，不受查询级 ORDER BY 开关影响
  ensure_non_empty_window_spec(this);
  size_t pc = partition_by.size();
  size_t oc = order_by.size();

  out << " over (";
  bool printed_any = false;

  if (pc > 0) {
    out << "partition by ";
    for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++) {
      out << **ref;
      if (ref+1 != partition_by.end())
        out << ",";
    }
    printed_any = true;
  }

  if (oc > 0) {
    if (printed_any) out << " order by ";
    else out << "order by ";
    for (auto ref = order_by.begin(); ref != order_by.end(); ref++) {
      out << **ref;
      if (ref+1 != order_by.end())
        out << ",";
    }
    printed_any = true;
  }

  // 若 partition/order 仍为空（极端兜底失败），直接打印常量表达式，保持语法合法
  if (!printed_any) {
	// 安全常量表达式（跨方言可接受）
    out << "order by CAST(0 AS SIGNED)"; 
    printed_any = true;
  }

  // 值函数安全帧：仅当窗口规范非空时追加帧（避免在 OVER () 中出现帧导致语义异常）
  if (printed_any && aggregate && aggregate->proc) {
    const std::string n = aggregate->proc->name;
    if (n == "FIRST_VALUE" || n == "LAST_VALUE" || n == "NTH_VALUE") {
      out << " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING";
    }
  }
  out << ")";
}

// 旧的窗口兼容判定逻辑（允许 SUM/AVG/COUNT 等窗口化）已移除

window_function::window_function(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  match();
  aggregate = make_shared<funcall>(this, type_constraint, true);
  // 严格窗口策略：
  // 仅白名单窗口函数可携带 OVER；
  // 所有聚合（COUNT/SUM/AVG/MIN/MAX/STDDEV_*/VAR_*）及 JSON/GROUP_CONCAT 聚合禁止窗口化；
  // 在不合法上下文（非 SELECT 列项）或嵌套窗口场合统一降级（不打印 OVER）。
  // 简化窗口开关：在 SELECT 列项上下文允许打印 OVER，否则抑制
  bool allow_over = context_allows_window(p);
  suppress_over = !allow_over;
}

bool window_function::allowed(prod *pprod) {
  // 只允许在 SELECT 列项上下文生成窗口函数；禁止 WHERE/HAVING/ON/GROUP BY 等过滤/分组上下文
  // 通过父链判定：select_list 下且属于 query_spec 的列项
  return context_allows_window(pprod);
}

// ---- Diversified predicate generator: operator rotation and sample shuffle ----
std::string expr_utils::make_diversified_predicate_for_scope(prod* p, int used_nullness) {
  // Diversify by choosing a column from nearest alias and rotating operator classes
  if (!p || !p->scope || p->scope->refs.empty()) return window_function::make_column_predicate_for_scope(p, used_nullness);
  named_relation* near_rel = p->scope->refs.back();
  if (!near_rel) return window_function::make_column_predicate_for_scope(p, used_nullness);
  // pick a column; prefer non-JSON/spatial
  const column* picked_col = nullptr; sqltype* picked_type = nullptr;
  for (auto &c : near_rel->columns()) { sqltype* t=c.type; if(!t) continue; std::string n = safe_type_name(t); if (n=="json"||n=="geometry") continue; picked_col=&c; picked_type=t; break; }
  if (!picked_col) { for (auto &c : near_rel->columns()) { sqltype* t=c.type; if(!t) continue; picked_col=&c; picked_type=t; break; } }
  if (!picked_col || !picked_type) return window_function::make_column_predicate_for_scope(p, used_nullness);
  return expr_utils::make_diversified_for_alias_column(p, near_rel->ident(), picked_col->name, picked_type, used_nullness, std::string(), std::string());
}

std::string expr_utils::make_diversified_for_alias_column(prod* p, const std::string& alias, const std::string& col_name, sqltype* col_type, int used_nullness, const std::string& avoid_opclass, const std::string &avoid_values_key) {
  // 保留接口兼容，当前实现未使用
  (void)avoid_values_key; 
  if (!p || !p->scope) return window_function::make_column_predicate_for_scope(p, used_nullness);
  // resolve real table ident for alias
  named_relation* rel = nullptr; for (auto r : p->scope->refs) { if (r && r->ident() == alias) { rel = r; break; } }
  if (!rel) return window_function::make_column_predicate_for_scope(p, used_nullness);
  std::string real_ident;
  if (auto ar = dynamic_cast<aliased_relation*>(rel)) { if (auto t = dynamic_cast<table*>(ar->rel)) real_ident = t->ident(); }
  else if (auto tt = dynamic_cast<table*>(rel)) { real_ident = tt->ident(); }
  // sample list
  const SampleList* sl = nullptr;
#ifdef HAVE_LIBMYSQLCLIENT
  if (p->scope->schema) {
    if (auto sm = dynamic_cast<schema_mysql*>(p->scope->schema)) {
      sl = sm->base_samples_cache.get(real_ident.empty()?alias:real_ident, col_name);
    }
  }
#endif
  std::ostringstream oss; std::string alias_col = alias + "." + col_name; const std::string tname = col_type ? safe_type_name(col_type) : std::string("varchar");
  const bool is_str = (tname=="varchar" || tname=="char" || tname=="text" || tname=="enum");
  const bool is_time = (tname=="date" || tname=="datetime" || tname=="timestamp" || tname=="time" || tname=="year");
  auto shuffled = [&](std::vector<Value> v){
    for (size_t i=0; i<v.size(); ++i) { if (v.empty()) break; size_t j = (size_t)(d100() % v.size()); std::swap(v[i], v[j]); }
    return v;
  };
  auto emit_in_set = [&](const std::vector<Value> &vals, bool neg){
    oss << alias_col << (neg?" NOT IN (":" IN ("); size_t cnt=0; auto vv = shuffled(vals);
    for (size_t i=0;i<vv.size() && cnt<3; ++i){ if (vv[i].is_null) continue; if (cnt) oss<<","; oss<<vv[i].literal; ++cnt; }
    if (cnt == 0) {
        if (tname == "varchar") oss << "''";
        else if (tname == "date") oss << "CAST('1970-01-01' AS DATE)";
        else if (tname == "datetime" || tname == "timestamp") oss << "CAST('1970-01-01 00:00:00' AS DATETIME)";
        else if (tname == "time") oss << "CAST('00:00:00' AS TIME)";
        else if (tname == "year") oss << "1970";
        else oss << "0";
    }
    oss << ")";
  };
  auto emit_like = [&](const std::string &lit){
	// assume quoted
    std::string core = lit; 
    std::string raw = (core.size()>=2 && core.front()=='\'' && core.back()=='\'') ? core.substr(1, core.size()-2) : core;
    int mode = 1 + (d6()%3);
    if (mode==1) oss << alias_col << " LIKE '" << raw << "%'"; else if (mode==2) oss << alias_col << " LIKE '%" << raw << "'"; else oss << alias_col << " LIKE '%" << raw << "%'";
  };
  auto emit_rel = [&](const Value &v){ const char* ops[] = {"=","!=","<",">","<=",">="}; const char* pick = ops[d100()%6]; oss << alias_col << " " << pick << " " << v.literal; };
  auto emit_between = [&](const std::vector<Value> &vals){ Value a,b; auto vv=shuffled(vals); if(vv.size()>=2){ a=vv[0]; b=vv[1]; } else { if (is_str) { a.literal = "'a'"; b.literal = "'b'"; } else if (is_time) { if (tname=="date") { a.literal = "CAST('1970-01-01' AS DATE)"; b.literal = "CAST('1970-01-02' AS DATE)"; } else if (tname=="time") { a.literal = "CAST('00:00:00' AS TIME)"; b.literal = "CAST('23:59:59' AS TIME)"; } else if (tname=="year") { a.literal = "1970"; b.literal = "1971"; } else { a.literal = "CAST('1970-01-01 00:00:00' AS DATETIME)"; b.literal = "CAST('1970-01-02 00:00:00' AS DATETIME)"; } } else { a.literal = "0"; b.literal = "1"; } } oss << alias_col << " BETWEEN " << a.literal << " AND " << b.literal; };

  // JSON/spatial rotation
  if (tname=="json") {
    int pick = 1 + (d6()%2);
    if (p->scope->schema && p->scope->schema->mysql_mode) {
      if (used_nullness >= 1) { oss << (pick==1? (std::string("JSON_VALID(")+alias_col+") = 1") : (std::string("JSON_VALID(")+alias_col+") = 1")); }
      else { oss << (pick==1? (std::string("JSON_TYPE(")+alias_col+") IS NOT NULL") : (std::string("JSON_VALID(")+alias_col+") = 1")); }
    } else {
      oss << alias_col << " IS NOT NULL";
    }
    return oss.str();
  }
  if (tname=="geometry") {
    int pick = 1 + (d6()%2);
    if (p->scope->schema && p->scope->schema->mysql_mode) {
      if (used_nullness >= 1) { oss << (pick==1? (std::string("ST_IsValid(")+alias_col+") = 1") : (std::string("ST_SRID(")+alias_col+") = 4326")); }
      else { oss << (pick==1? (std::string("ST_SRID(")+alias_col+") = 4326") : (std::string("ST_IsValid(")+alias_col+") = 1")); }
    } else {
      oss << alias_col << " IS NOT NULL";
    }
    return oss.str();
  }

  // Select operator class different from avoid_opclass
  std::vector<std::string> allowed;
  /* is_str computed above */
  /* is_time computed above */
  if (is_str) { allowed = {"LIKE","EQ","IN","NIN"}; }
  else if (is_time) { allowed = {"EQ","RANGE","REL","IN"}; }
  else { allowed = {"EQ","REL","RANGE","IN","NEQ"}; }
  allowed.erase(std::remove(allowed.begin(), allowed.end(), avoid_opclass), allowed.end());
  std::string pick_oc = allowed.empty()?std::string("EQ"):allowed[d100()%allowed.size()];

  // Use samples if available
  std::vector<Value> vals = sl?sl->values:std::vector<Value>();
  // 统一族过滤：只保留与列类型族一致的样本，避免跨族值进入 IN/RANGE/REL
  TypeFamily tf = family_of(tname);
  std::vector<Value> filtered;
  for (const auto &v : vals) { if (!v.is_null && v.family == tf) filtered.push_back(v); }
  vals = shuffled(filtered);

  if (pick_oc=="IN") { emit_in_set(vals, false); }
  else if (pick_oc=="NIN") { emit_in_set(vals, true); }
  else if (pick_oc=="LIKE") {
    for (const auto &v : vals) { if (!v.is_null && v.family==TypeFamily::String) { emit_like(v.literal); return oss.str(); } }
    emit_like("'a'");
  }
  else if (pick_oc=="RANGE") { emit_between(vals); }
  else if (pick_oc=="REL" || pick_oc=="EQ" || pick_oc=="NEQ") {
    if (!vals.empty()) emit_rel(vals[0]); else { Value v; if (is_str) { v.literal = std::string("'a'"); v.is_null=false; v.family=TypeFamily::String; v.src=SourceTag::Synthetic; }
    else if (is_time) { if (tname=="date") { v.literal = std::string("'1970-01-01'"); }
      else if (tname=="time") { v.literal = std::string("'00:00:00'"); }
      else if (tname=="year") { v.literal = std::string("1970"); }
      else { v.literal = std::string("CAST('1970-01-01 00:00:00' AS DATETIME)"); }
      v.is_null=false; v.family=TypeFamily::Time; v.src=SourceTag::Synthetic; }
    else { v.literal = std::string("0"); v.is_null=false; v.family=TypeFamily::Numeric; v.src=SourceTag::Synthetic; } emit_rel(v);} 
  } else {
    if (!vals.empty()) emit_rel(vals[0]); else { Value v; if (is_str) { v.literal = std::string("'a'"); v.is_null=false; v.family=TypeFamily::String; v.src=SourceTag::Synthetic; }
    else if (is_time) { if (tname=="date") { v.literal = std::string("'1970-01-01'"); }
      else if (tname=="time") { v.literal = std::string("'00:00:00'"); }
      else if (tname=="year") { v.literal = std::string("1970"); }
      else { v.literal = std::string("CAST('1970-01-01 00:00:00' AS DATETIME)"); }
      v.is_null=false; v.family=TypeFamily::Time; v.src=SourceTag::Synthetic; }
    else { v.literal = std::string("0"); v.is_null=false; v.family=TypeFamily::Numeric; v.src=SourceTag::Synthetic; } emit_rel(v);} 
  }

  return oss.str();
}

// (removed)
