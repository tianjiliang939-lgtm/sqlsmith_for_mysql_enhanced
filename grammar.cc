#include <typeinfo>
#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <cassert>
#include <unordered_set>
#include <unordered_map>
#include <functional>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"
#include "json_table_ref.hh"
#include "temp_dataset.hh"
#include "postgres.hh"
#include "mysql.hh"
#include <cstring>
// Use <cstring> and std::strlen to resolve undeclared strlen; C++-safe length retrieval.

using namespace std;

static inline bool supports_lateral(const schema* sch){
  if (!sch) return false;
  // 支持方言：PostgreSQL 与 MySQL 8.0（不依赖连接版本检查）
  if (dynamic_cast<const schema_pqxx*>(sch) != nullptr) return true;
  if (sch->mysql_mode) return true;
  return false;
}

// 检测表达式子树中是否包含窗口函数（window_function）
static bool expr_contains_window(const value_expr* e) {
  if (!e) return false;
  if (dynamic_cast<const window_function*>(e)) return true;
  if (auto f = dynamic_cast<const funcall*>(e)) {
    for (auto &p : f->parms) { if (expr_contains_window(p.get())) return true; }
    return false;
  }
  if (auto c = dynamic_cast<const coalesce*>(e)) {
    for (auto &p : c->value_exprs) { if (expr_contains_window(p.get())) return true; }
    return false;
  }
  if (auto k = dynamic_cast<const case_expr*>(e)) {
    if (expr_contains_window(k->true_expr.get())) return true;
    if (expr_contains_window(k->false_expr.get())) return true;
    return false; // 条件是 bool_expr，不在 value_expr 检测范围
  }
  // 列引用/常量/子查询默认视为非窗口
  return false;
}


// --- Batch Probability Controller externs ---
extern "C" double batch_get_col_p_win();
extern "C" double batch_get_col_p_agg();
extern "C" double batch_get_p_next_win();
extern "C" double batch_get_p_next_agg();
extern "C" double batch_get_p_cond();
extern "C" void batch_prepare_next();
extern "C" void batch_on_stmt_generated(int has_win, int has_agg);


// Minimal helpers defined early to ensure visibility before use
struct ClauseBudget {
  int bool_literal_count = 0;
  int null_pred_count = 0;
  bool has_column_cond = false;
  bool has_nearest_column_cond = false;
  // Per-clause dedup signatures: alias:column:opclass:values-hash
  std::unordered_set<std::string> seen_signatures;
};
struct ClauseContext {
  std::string nearest_alias;
};
static inline ClauseContext make_clause_ctx(prod* owner){ ClauseContext c; if(owner && owner->scope && !owner->scope->refs.empty()){ auto *nr = owner->scope->refs.back(); if(nr) c.nearest_alias = nr->ident(); } return c; }
static void out_bool_expr_with_budget(std::ostream& out, const bool_expr* b, ClauseBudget& budget, const ClauseContext& ctx, prod* owner);

shared_ptr<table_ref> table_ref::factory(prod *p) {
  try {
    if (p->level < 3 + d6()) {
      if (d6() > 3 && p->level < d6())
        return make_shared<table_subquery>(p);
      if (d6() > 3)
        return make_shared<joined_table>(p);
    }
    
    // 在 JSON 启用且 MySQL 模式下，部分概率生成 JSON_TABLE 引用（受临时数据集总开关控制）
    if (p->scope->schema->enable_json && p->scope->schema->mysql_mode && p->scope->schema->feature.temp_dataset_enabled && d6() > 4)
      return make_shared<json_table_ref>(p);

    // [MySQL 8.0] 临时数据集 provider：与原始数据集（表）等概率选择（受 CLI 开关控制）
    if (p->scope && p->scope->schema && p->scope->schema->mysql_mode && d6() > 3) {
      if (p->scope->schema->feature.temp_dataset_enabled) {
		// 约 50% 选择临时数据集
        if (d6() > 3) return make_shared<temp_dataset_ref>(p); 
        else return make_shared<table_or_query_name>(p);
      } else {
        // 关闭临时数据集时，回退为普通表选择
        return make_shared<table_or_query_name>(p);
      }
    }
    if (d6() > 3)
      return make_shared<table_or_query_name>(p);
    /*
    else
      return make_shared<table_sample>(p);
    */
    return make_shared<table_or_query_name>(p);
  } catch (runtime_error &e) {
    p->retry();
  }
  return factory(p);
}

table_or_query_name::table_or_query_name(prod *p) : table_ref(p) {
  t = random_pick(scope->tables);
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("ref"), t));
}

void table_or_query_name::out(std::ostream &out) {
  out << t->ident() << " as " << refs[0]->ident();
}

target_table::target_table(prod *p, table *victim) : table_ref(p)
{
  while (! victim
         || victim->schema == "pg_catalog"
         || !victim->is_base_table
         || !victim->columns().size()) {
    struct named_relation *pick = random_pick(scope->tables);
    victim = dynamic_cast<table *>(pick);
    retry();
  }
  victim_ = victim;
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("target"), victim));
}

void target_table::out(std::ostream &out) {
  out << victim_->ident() << " as " << refs[0]->ident();
}

table_sample::table_sample(prod *p) : table_ref(p) {
  match();
  retry_limit = 1000; /* retries are cheap here */
  do {
    auto pick = random_pick(scope->schema->base_tables);
    t = dynamic_cast<struct table*>(pick);
    retry();
  } while (!t || !t->is_base_table);
  
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("sample"), t));
  percent = 0.1 * d100();
  method = (d6() > 2) ? "system" : "bernoulli";
}

void table_sample::out(std::ostream &out) {
  out << t->ident() <<
    " as " << refs[0]->ident() <<
    " tablesample " << method <<
    " (" << percent << ") ";
}

table_subquery::table_subquery(prod *p, bool lateral)
  : table_ref(p), is_lateral(lateral) {
  query = make_shared<query_spec>(this, scope, lateral);
  string alias = scope->stmt_uid("subq");
  relation *aliased_rel = &query->select_list->derived_table;
  refs.push_back(make_shared<aliased_relation>(alias, aliased_rel));
}

table_subquery::~table_subquery() { }

void table_subquery::accept(prod_visitor *v) {
  query->accept(v);
  v->visit(this);
}

shared_ptr<join_cond> join_cond::factory(prod *p, table_ref &lhs, table_ref &rhs)
{
     try {
          if (d6() < 6)
               return make_shared<expr_join_cond>(p, lhs, rhs);
          else
               return make_shared<simple_join_cond>(p, lhs, rhs);
     } catch (runtime_error &e) {
          p->retry();
     }
     return factory(p, lhs, rhs);
}

simple_join_cond::simple_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs)
{
retry:
  named_relation *left_rel = &*random_pick(lhs.refs);
  
  if (!left_rel->columns().size())
    { retry(); goto retry; }

  named_relation *right_rel = &*random_pick(rhs.refs);

  column &c1 = random_pick(left_rel->columns());

  for (auto c2 : right_rel->columns()) {
    if (c1.type == c2.type) {
      condition +=
        left_rel->ident() + "." + c1.name + " = " + right_rel->ident() + "." + c2.name + " ";
      break;
    }
  }
  if (condition == "") {
    retry(); goto retry;
  }
}

void simple_join_cond::out(std::ostream &out) {
     out << condition;
}

expr_join_cond::expr_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs), joinscope(p->scope)
{
     scope = &joinscope;
     for (auto ref: lhs.refs)
          joinscope.refs.push_back(&*ref);
     for (auto ref: rhs.refs)
          joinscope.refs.push_back(&*ref);
     search = bool_expr::factory(this);
}

void expr_join_cond::out(std::ostream &out) {
     ClauseBudget budget;
     ClauseContext ctx;
     if (!joinscope.refs.empty() && joinscope.refs.back()) ctx.nearest_alias = joinscope.refs.back()->ident();
     std::ostringstream oss;
     out_bool_expr_with_budget(oss, search.get(), budget, ctx, this);
     out << oss.str();
     // Append nearest-alias column predicate if missing
     if (!budget.has_column_cond || (!ctx.nearest_alias.empty() && !budget.has_nearest_column_cond)) {
       indent(out);
       out << " and " << window_function::make_column_predicate_for_scope(this, budget.null_pred_count);
     }
}

joined_table::joined_table(prod *p) : table_ref(p) {
  lhs = table_ref::factory(this);
  rhs = table_ref::factory(this);

  condition = join_cond::factory(this, *lhs, *rhs);

  if (d6()<4) {
    type = "inner";
  } else if (d6()<4) {
    type = "left";
  } else {
    type = "right";
  }

  for (auto ref: lhs->refs)
    refs.push_back(ref);
  for (auto ref: rhs->refs)
    refs.push_back(ref);
}

// 轻量工具：统计谓词类型计数（列参与/常量真值/空性判断）
static void count_predicates(const bool_expr* b, int &col, int &const_bool, int &isnull) {
  if (!b) return;
  if (auto tv = dynamic_cast<const truth_value*>(b)) { (void)tv; const_bool++; return; }
  if (auto np = dynamic_cast<const null_predicate*>(b)) {
    isnull++;
    if (np->expr && dynamic_cast<column_reference*>(np->expr.get())) col++;
    return;
  }
  if (auto dp = dynamic_cast<const distinct_pred*>(b)) { (void)dp; col++; return; }
  if (auto cmp = dynamic_cast<const comparison_op*>(b)) {
    if (cmp->lhs && dynamic_cast<column_reference*>(cmp->lhs.get())) col++;
    if (cmp->rhs && dynamic_cast<column_reference*>(cmp->rhs.get())) col++;
    return;
  }
  if (auto bt = dynamic_cast<const bool_term*>(b)) {
    count_predicates(dynamic_cast<const bool_expr*>(bt->lhs.get()), col, const_bool, isnull);
    count_predicates(dynamic_cast<const bool_expr*>(bt->rhs.get()), col, const_bool, isnull);
    return;
  }
  if (auto ex = dynamic_cast<const exists_predicate*>(b)) { (void)ex; return; }
}

// ----- Clause budget & nearest-alias enforcement helpers (file-local) -----
static inline bool parse_alias_col(const std::string& s, std::string& alias, std::string& col) {
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '.') {
      size_t j = i; while (j > 0) { char c = s[j-1]; if (isalnum(c) || c=='_') --j; else break; }
      size_t k = i+1; while (k < s.size()) { char c = s[k]; if (isalnum(c) || c=='_') ++k; else break; }
      if (j < i && k > i+1) { alias = s.substr(j, i-j); col = s.substr(i+1, k-(i+1)); return true; }
    }
  }
  return false;
}
static inline std::string normalize_values_key(const std::string& text) {
  std::string t = text;
  auto pos_in = t.find(" IN ("); if (pos_in != std::string::npos) {
    size_t l = pos_in + 5; size_t r = t.find(")", l); if (r == std::string::npos) r = t.size();
    std::string inside = t.substr(l, r-l);
    std::vector<std::string> toks; std::string cur; bool in_str=false;
    for (size_t i=0; i < inside.size(); ++i) {
      char c = inside[i]; if (c=='\'') in_str=!in_str; if (!in_str && c==',') { if (!cur.empty()) {
          size_t a=0; while (a < cur.size() && isspace(cur[a])) ++a; size_t b=cur.size(); while (b> a && isspace(cur[b-1])) --b; toks.push_back(cur.substr(a,b-a)); cur.clear();
        } } else cur.push_back(c);
    }
    if (!cur.empty()) { size_t a=0; while (a < cur.size() && isspace(cur[a])) ++a; size_t b=cur.size(); while (b> a && isspace(cur[b-1])) --b; toks.push_back(cur.substr(a,b-a)); }
    sort(toks.begin(), toks.end()); std::ostringstream os; for (size_t i=0;i<toks.size();++i){ if(i) os<<"|"; os<<toks[i]; } return std::string("IN:")+os.str();
  }
  auto pos_like = t.find(" LIKE "); if (pos_like != std::string::npos) {
    std::string pat = t.substr(pos_like+6);
    return std::string("LIKE:")+pat;
  }
  auto pos_between = t.find(" BETWEEN "); if (pos_between != std::string::npos) {
    size_t l = pos_between + 9; size_t andp = t.find(" AND ", l); if (andp == std::string::npos) andp = t.size();
    std::string lower = t.substr(l, andp-l);
    std::string upper = (andp < t.size()) ? t.substr(andp+5) : std::string();
    return std::string("BETWEEN:")+lower+"|"+upper;
  }
  static const char* ops[] = {" = ", " != ", " <> ", " <= ", " >= ", " < ", " > "};
  for (auto op: ops) {
    auto p = t.find(op); if (p != std::string::npos) { return std::string("REL:")+t.substr(p+std::strlen(op)); }
  }
  if (t.find(" IS NULL") != std::string::npos) return std::string("ISNULL");
  if (t.find(" IS NOT NULL") != std::string::npos) return std::string("ISNOTNULL");
  return std::string("OTHER:")+t;
}
static inline std::string opclass_from(const std::string& text) {
  if (text.find(" NOT IN ") != std::string::npos || text.find(" NOT IN(") != std::string::npos) return "NIN";
  if (text.find(" IN (") != std::string::npos) return "IN";
  if (text.find(" LIKE ") != std::string::npos) return "LIKE";
  if (text.find(" BETWEEN ") != std::string::npos) return "RANGE";
  if (text.find(" IS NOT NULL") != std::string::npos) return "ISNOTNULL";
  if (text.find(" IS NULL") != std::string::npos) return "ISNULL";
  if (text.find(" != ") != std::string::npos || text.find(" <>") != std::string::npos) return "NEQ";
  if (text.find(" = ") != std::string::npos) return "EQ";
  if (text.find(" <= ") != std::string::npos || text.find(" >= ") != std::string::npos || text.find(" < ") != std::string::npos || text.find(" > ") != std::string::npos) return "REL";
  return "OTHER";
}
static inline std::string make_signature_key(const std::string& pred_text) {
  std::string alias, col; if (!parse_alias_col(pred_text, alias, col)) return std::string();
  std::string op = opclass_from(pred_text);
  std::string valk = normalize_values_key(pred_text);
  std::ostringstream os; os << alias << ":" << col << ":" << op << ":" << valk; return os.str();
}

static void out_bool_expr_with_budget(std::ostream &out, const bool_expr* b, ClauseBudget &budget, const ClauseContext &ctx, prod* owner){
  if(!b){ out << "true"; return; }
  if(auto tv = dynamic_cast<const truth_value*>(b)){
    if(budget.bool_literal_count == 0){ out << tv->op; budget.bool_literal_count++; }
    else {
      // skip extra true/false; replace with a nearest-alias column predicate to keep syntax valid
      std::string pred = window_function::make_column_predicate_for_scope(owner, budget.null_pred_count);
      std::string sig = make_signature_key(pred);
      if (!sig.empty() && budget.seen_signatures.count(sig)) {
        // rotate to a diversified predicate to avoid same signature
        pred = expr_utils::make_diversified_predicate_for_scope(owner, budget.null_pred_count);
      }
      out << pred;
      if(!ctx.nearest_alias.empty() && pred.find(ctx.nearest_alias + ".") != std::string::npos) budget.has_nearest_column_cond = true;
      budget.has_column_cond = true; if (!sig.empty()) budget.seen_signatures.insert(make_signature_key(pred));
    }
    return;
  }
  if(auto np = dynamic_cast<const null_predicate*>(b)){
    if(budget.null_pred_count == 0){
      std::ostringstream exprs; exprs << *np->expr;
      std::string exprstr = exprs.str();
      std::string pred = exprstr + std::string(" is ") + np->negate + "NULL";
      std::string sig = make_signature_key(pred);
      if (!sig.empty() && budget.seen_signatures.count(sig)) {
        pred = window_function::make_column_predicate_for_scope(owner, budget.null_pred_count);
      }
      out << pred;
      budget.null_pred_count++;
      if(dynamic_cast<column_reference*>(np->expr.get())){
        budget.has_column_cond = true;
        if(!ctx.nearest_alias.empty() && pred.find(ctx.nearest_alias + ".") != std::string::npos) budget.has_nearest_column_cond = true;
      }
      if (!sig.empty()) budget.seen_signatures.insert(make_signature_key(pred));
    } else {
      // skip extra IS [NOT] NULL; replace with data-driven predicate without adding a second nullness
      std::string pred = window_function::make_column_predicate_for_scope(owner, budget.null_pred_count);
      out << pred;
      if(!ctx.nearest_alias.empty() && pred.find(ctx.nearest_alias + ".") != std::string::npos) budget.has_nearest_column_cond = true;
      budget.has_column_cond = true;
    }
    return;
  }
  if(auto cmp = dynamic_cast<const comparison_op*>(b)){
    std::ostringstream ls, rs; ls << *cmp->lhs; rs << *cmp->rhs;
    std::string lss = ls.str(), rss = rs.str();
    bool lhs_is_col = dynamic_cast<column_reference*>(cmp->lhs.get()) != nullptr;
    bool rhs_is_col = dynamic_cast<column_reference*>(cmp->rhs.get()) != nullptr;
    if(lhs_is_col || rhs_is_col){
      budget.has_column_cond = true;
      if(!ctx.nearest_alias.empty()){
        if(lhs_is_col && lss.compare(0, ctx.nearest_alias.size()+1, ctx.nearest_alias + ".") == 0) budget.has_nearest_column_cond = true;
        if(rhs_is_col && rss.compare(0, ctx.nearest_alias.size()+1, ctx.nearest_alias + ".") == 0) budget.has_nearest_column_cond = true;
      }
    }
    // Replace meaningless self-equality (col = col) with dataset-driven predicate
    const std::string opname = cmp->oper ? cmp->oper->name : std::string("");
    if(lhs_is_col && rhs_is_col && lss == rss && (opname == "=" || opname == "<=>")){
      std::string pred = window_function::make_column_predicate_for_scope(owner, budget.null_pred_count);
      out << pred;
      if(!ctx.nearest_alias.empty() && pred.find(ctx.nearest_alias + ".") != std::string::npos) budget.has_nearest_column_cond = true;
      budget.has_column_cond = true;
    } else {
      std::string pred = lss + std::string(" ") + (cmp->oper ? cmp->oper->name : std::string("=")) + std::string(" ") + rss;
      std::string sig = make_signature_key(pred);
      if (!sig.empty() && budget.seen_signatures.count(sig)) {
        std::string alias, col; sqltype* coltype = nullptr;
        if (lhs_is_col) { parse_alias_col(lss, alias, col); coltype = cmp->lhs->type; }
        else if (rhs_is_col) { parse_alias_col(rss, alias, col); coltype = cmp->rhs->type; }
        if (!alias.empty() && !col.empty()) {
          // 当比较操作两侧类型不可靠（未知或不匹配）时，回退到作用域内的真实列类型，避免将时间/数值列误判为字符串
          sqltype* looked_up_type = coltype;
          auto lookup_type_in_scope = [&]() -> sqltype* {
            if (!owner || !owner->scope) return nullptr;
            for (auto r : owner->scope->refs) {
              if (!r) continue;
              if (r->ident() == alias) {
                for (auto &c2 : r->columns()) { if (c2.name == col) return c2.type; }
              }
            }
            return nullptr;
          };
          // 若传入 coltype 为空或未知族，则尝试通过作用域查找真实类型
          if (!looked_up_type || safe_type_family(looked_up_type) == SafeTypeFamily::Unknown) {
            sqltype* t2 = lookup_type_in_scope(); if (t2) looked_up_type = t2;
          }
          pred = expr_utils::make_diversified_for_alias_column(owner, alias, col, looked_up_type, budget.null_pred_count, opclass_from(pred), normalize_values_key(pred));
        } else {
          pred = expr_utils::make_diversified_predicate_for_scope(owner, budget.null_pred_count);
        }
      }
      out << pred;
      if (!sig.empty()) budget.seen_signatures.insert(make_signature_key(pred));
    }
    return;
  }
  if(auto bt = dynamic_cast<const bool_term*>(b)){
    out << "(";
    out_bool_expr_with_budget(out, dynamic_cast<const bool_expr*>(bt->lhs.get()), budget, ctx, owner);
    out << ") "; if (owner) owner->indent(out);
    out << bt->op << " (";
    out_bool_expr_with_budget(out, dynamic_cast<const bool_expr*>(bt->rhs.get()), budget, ctx, owner);
    out << ")";
    return;
  }
  if(auto dp = dynamic_cast<const distinct_pred*>(b)){
    std::ostringstream ls, rs; ls << *dp->lhs; rs << *dp->rhs;
    std::string lss = ls.str(), rss = rs.str();
    bool lhs_is_col = dynamic_cast<column_reference*>(dp->lhs.get()) != nullptr;
    bool rhs_is_col = dynamic_cast<column_reference*>(dp->rhs.get()) != nullptr;
    if(lhs_is_col || rhs_is_col){
      budget.has_column_cond = true;
      if(!ctx.nearest_alias.empty()){
        if(lhs_is_col && lss.compare(0, ctx.nearest_alias.size()+1, ctx.nearest_alias + ".") == 0) budget.has_nearest_column_cond = true;
        if(rhs_is_col && rss.compare(0, ctx.nearest_alias.size()+1, ctx.nearest_alias + ".") == 0) budget.has_nearest_column_cond = true;
      }
    }
    std::string pred = lss + std::string(" is distinct from ") + rss;
    std::string sig = make_signature_key(pred);
    if (!sig.empty() && budget.seen_signatures.count(sig)) {
      pred = expr_utils::make_diversified_predicate_for_scope(owner, budget.null_pred_count);
    }
    out << pred;
    if (!sig.empty()) budget.seen_signatures.insert(make_signature_key(pred));
    return;
  }
  if(auto ex = dynamic_cast<const exists_predicate*>(b)){
    out << "EXISTS ("; if (owner) owner->indent(out); ex->subquery->out(out); out << ")"; return;
  }
  // Fallback to default printing
  const_cast<bool_expr*>(b)->out(out);
}

void joined_table::out(std::ostream &out) {
  out << *lhs;
  indent(out);
  out << type << " join " << *rhs;
  indent(out);
  // 在 EXISTS 子查询作用域内，对 JOIN ON 条件进行最小补齐：确保至少一个列条件，避免纯常量
  bool in_exists_ctx = false; prod *p = this; while (p) { if (dynamic_cast<exists_predicate*>(p)) { in_exists_ctx = true; break; } p = p->pprod; }
  if (in_exists_ctx) {
    // 打印原始条件，并在需要时追加一个安全列条件
    out << "on (";
    if (auto ej = dynamic_cast<expr_join_cond*>(condition.get())) {
      int colc=0, cbc=0, inc=0; count_predicates(ej->search.get(), colc, cbc, inc);
      out << *ej;
      if (colc == 0) {
        out << ") "; indent(out); out << "and " << window_function::make_column_predicate_for_scope(ej, inc);
        return;
      } else {
        out << ")"; return;
      }
    } else {
      // simple_join_cond 已是列等值，直接打印
      out << *condition << ")"; return;
    }
  }
  out << "on (" << *condition << ")";
}

void table_subquery::out(std::ostream &out) {
  if (is_lateral && supports_lateral(scope->schema))
    out << "LATERAL "; // Enforce uppercase for LATERAL at printer; global final pass also uppercases keywords
  out << "(" << *query << ") as " << refs[0]->ident();
}

void from_clause::out(std::ostream &out) {
  if (!reflist.size())
    return;
  if (!printed) {
    out << "from ";
    printed = true;
  }
  for (auto r = reflist.begin(); r < reflist.end(); r++) {
    // 别名守卫：若派生表/表函数缺少别名，自动补齐，避免 MySQL 3667
    auto tref = *r;
    if (tref && !tref->refs.empty()) {
      for (auto &nr : tref->refs) {
        if (nr && nr->ident().size() == 0) {
          const char *prefix = "ref";
          if (dynamic_cast<json_table_ref*>(tref.get())) prefix = "jt";
          else if (dynamic_cast<temp_dataset_ref*>(tref.get())) prefix = "t";
          nr->name = scope->stmt_uid(prefix);
        }
      }
    }
    indent(out);
    out << **r;
    if (r + 1 != reflist.end())
      out << ",";
  }
}

from_clause::from_clause(prod *p) : prod(p) {
  reflist.push_back(table_ref::factory(this));
  for (auto r : reflist.back()->refs)
    scope->refs.push_back(&*r);

  while (d6() > 5) {
    // add a lateral subquery（方言感知；仅 PostgreSQL 支持；且需开启开关）
    if (!scope || !scope->schema) break;
    if (!scope->schema->feature.lateral_enabled) break;
    if (!supports_lateral(scope->schema)) break;
    if (!impedance::matched(typeid(lateral_subquery)))
      break;
    reflist.push_back(make_shared<lateral_subquery>(this));
    for (auto r : reflist.back()->refs)
      scope->refs.push_back(&*r);
  }
}

select_list::select_list(prod *p) : prod(p)
{
  do {
    shared_ptr<value_expr> e;
    try {
      // 在 SELECT 列项中按“跨语句概率控制”的列级概率进行函数选择；失败回退为标量
      double pcol_agg = batch_get_col_p_agg();
      double pcol_win = batch_get_col_p_win();
      bool allow_window = window_function::allowed(this) && scope && scope->schema && scope->schema->feature.window_enabled;
      int gate = d100();
      if (gate < (int)(pcol_agg * 100.0)) {
        e = make_shared<funcall>(this, nullptr, true);
      } else if (allow_window && d100() < (int)(pcol_win * 100.0)) {
        e = make_shared<window_function>(this, nullptr);
      } else {
        e = value_expr::factory(this);
      }
    } catch (runtime_error &ex) {
      // 最小回退：生成常量表达式
      sqltype* fb = (scope && scope->schema && scope->schema->inttype) ? scope->schema->inttype : sqltype::get("int");
      e = make_shared<const_expr>(this, fb);
    }
    value_exprs.push_back(e);
    ostringstream name;
    name << "c" << columns++;
    sqltype *t = (e ? e->type : nullptr);
    // [Stability Guard] 避免硬断言；当类型为空或未知时，回退到安全默认类型
    if (!t || safe_type_family(t) == SafeTypeFamily::Unknown) {
      sqltype* fallback = (scope && scope->schema && scope->schema->inttype) ? scope->schema->inttype : sqltype::get("int");
      auto ce = value_expr::factory(this, fallback);
      value_exprs.back() = ce;
      t = fallback;
    }
    derived_table.columns().push_back(column(name.str(), t));
  } while (d6() > 1);

  // 语句级统计标记：由 query_spec 构造阶段进行 has_win/has_agg 统计与计数，移除逐语句 1:1 校准以遵循“跨语句概率控制”。

}

void select_list::out(std::ostream &out)
{
  int i = 0;
  for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
    indent(out);
    out << **expr << " as " << derived_table.columns()[i].name;
    i++;
    if (expr+1 != value_exprs.end())
      out << ", ";
  }
}

void query_spec::out(std::ostream &out) {
  // 按原有管线输出 SELECT，不在此处进行 CTE 包装（职责收敛到 common_table_expression::out）
  std::ostringstream __buf;
  __buf << "select";
  // 仅在主查询（最外层查询）注入用户自定义 hint：pprod==nullptr 或父为顶层 CTE
  bool is_outermost = (pprod == nullptr) || (dynamic_cast<common_table_expression*>(pprod) && (!pprod->pprod));
  if (is_outermost && scope && scope->schema && !scope->schema->customer_hint.empty()) {
    __buf << " " << scope->schema->customer_hint;
  }
  __buf << " " << set_quantifier << " "
        << *select_list;
  indent(__buf);
  // 存在 table_ref/provider 时，强制输出 FROM，再统一打印 table_ref
  if (from_clause && from_clause->reflist.size()) {
    __buf << "from ";
    from_clause->printed = true;
  }
  __buf << *from_clause;
  indent(__buf);
  // WHERE 子句统一预算与最近别名约束（适用于普通与 EXISTS 作用域）
  __buf << "where ";
  {
    ClauseBudget budget; ClauseContext ctx = make_clause_ctx(this);
    std::ostringstream oss; out_bool_expr_with_budget(oss, search.get(), budget, ctx, this);
    __buf << "(" << oss.str() << ")";
    if (!budget.has_column_cond || (!ctx.nearest_alias.empty() && !budget.has_nearest_column_cond)) {
      indent(__buf);
      __buf << "and " << window_function::make_column_predicate_for_scope(this, budget.null_pred_count);
    }
  }

  // 输出 GROUP BY 与 HAVING（当存在聚合时）
  if (!group_by_exprs.empty()) {
    indent(__buf);
    __buf << "group by ";
    for (auto it = group_by_exprs.begin(); it != group_by_exprs.end(); ++it) {
      __buf << **it;
      if (it + 1 != group_by_exprs.end()) __buf << ", ";
    }
    if (having) {
      indent(__buf);
      __buf << "having (" << *having << ")";
    }
  }

  // 统一 ORDER BY 输出策略（全局开关）：
  bool force_ord = (scope && scope->schema) ? scope->schema->feature.force_order_by : false;
  bool has_limit = (limit_clause.length() != 0);
  if (force_ord) {
    int ncols = 0;
    if (select_list) {
      ncols = (int)select_list->derived_table.columns().size();
    }
    if (ncols <= 0) ncols = 1;
    indent(__buf);
    __buf << "order by ";
    for (int i = 1; i <= ncols; ++i) {
      __buf << i;
      if (i < ncols) __buf << ",";
    }
    if (has_limit) {
      indent(__buf);
      __buf << limit_clause;
    }
  } else {
    if (has_limit) {
      indent(__buf);
      __buf << limit_clause;
    }
  }

  // 直接输出原始 SELECT（CTE 扩展职责在 common_table_expression::out 内实现）
  out << __buf.str();
}

struct for_update_verify : prod_visitor {
  virtual void visit(prod *p) {
    if (dynamic_cast<window_function*>(p))
      throw("window function");
    joined_table* join = dynamic_cast<joined_table*>(p);
    if (join && join->type != "inner")
      throw("outer join");
    query_spec* subquery = dynamic_cast<query_spec*>(p);
    if (subquery)
      subquery->set_quantifier = "";
    table_or_query_name* tab = dynamic_cast<table_or_query_name*>(p);
    if (tab) {
      table *actual_table = dynamic_cast<table*>(tab->t);
      if (actual_table && !actual_table->is_insertable)
        throw("read only");
      if (actual_table->name.find("pg_"))
        throw("catalog");
    }
    table_sample* sample = dynamic_cast<table_sample*>(p);
    if (sample) {
      table *actual_table = dynamic_cast<table*>(sample->t);
      if (actual_table && !actual_table->is_insertable)
        throw("read only");
      if (actual_table->name.find("pg_"))
        throw("catalog");
    }
  } ;
};


select_for_update::select_for_update(prod *p, struct scope *s, bool lateral)
  : query_spec(p,s,lateral)
{
  static const char *modes[] = {
    "update",
    "share",
  };

  try {
    for_update_verify v1;
    this->accept(&v1);

  } catch (const char* reason) {
    lockmode = 0;
    return;
  }
  if (d6() > 5) {
    lockmode = modes[d6()%2];
  } else {
    lockmode = 0;
  }
  set_quantifier = ""; // disallow distinct
}

void select_for_update::out(std::ostream &out) {
  query_spec::out(out);
  if (lockmode) {
    indent(out);
    out << " for " << lockmode;
  }
}

query_spec::query_spec(prod *p, struct scope *s, bool lateral) :
  prod(p), myscope(s)
{
  scope = &myscope;
  scope->tables = s->tables;

  if (lateral)
    scope->refs = s->refs;
  
  from_clause = make_shared<struct from_clause>(this);
  select_list = make_shared<struct select_list>(this);
  
  // 在构造 SELECT 列项后，检测聚合（不含窗口函数）；若存在，则自动生成 GROUP BY 与 HAVING
  {
    auto is_agg_only = [](value_expr* e)->bool {
      if (!e) return false;
      if (auto f = dynamic_cast<funcall*>(e)) return f->is_aggregate;
      return false;
    };
    auto is_window_expr = [](value_expr* e)->bool { return expr_contains_window(e); };
    bool has_agg = false;
    bool has_win = false;
    std::vector<shared_ptr<value_expr>> nonagg_exprs;
    // 先统计聚合与非聚合表达式
    for (auto &ve : select_list->value_exprs) {
      if (is_agg_only(ve.get())) has_agg = true;
      if (is_window_expr(ve.get())) has_win = true;
      if (!is_agg_only(ve.get()) && !is_window_expr(ve.get())) nonagg_exprs.push_back(ve);
    }
    if (has_agg) {
      // 使用“聚合函数的参数列”作为 GROUP BY 条件列（仅加入物理列 column_reference，去重）
      std::unordered_set<std::string> seen_cols;
      auto add_group_col = [&](const std::shared_ptr<value_expr>& v){
        if (!v) return;
        if (auto cref = dynamic_cast<column_reference*>(v.get())) {
          std::string alias, col; if (parse_alias_col(cref->reference, alias, col)) {
            std::string key = alias + "." + col; if (!seen_cols.count(key)) { group_by_exprs.push_back(v); seen_cols.insert(key); }
          }
        }
      };
      for (auto &ve : select_list->value_exprs) {
        if (auto f = dynamic_cast<funcall*>(ve.get())) {
          if (f->is_aggregate) {
            for (auto &arg : f->parms) add_group_col(arg);
          }
        }
      }
      // 兼容旧逻辑：若非聚合表达式存在，也加入 GROUP BY（不做去重，表达式可能非列）
      if (!nonagg_exprs.empty()) {
        for (auto &e : nonagg_exprs) group_by_exprs.push_back(e);
      } else if (group_by_exprs.empty()) {
        // 若既无聚合参数列可用又无非聚合表达式，仍加入一个安全列
        try {
          group_by_exprs.push_back(make_shared<column_reference>(this));
        } catch (...) {
          group_by_exprs.push_back(make_shared<column_reference>(this));
        }
      }
      // 先行移除包含窗口函数的 GROUP BY 表达式，避免 MySQL 3593
      {
        std::vector<shared_ptr<value_expr>> filtered2;
        for (auto &e : group_by_exprs) {
          if (!expr_contains_window(e.get())) filtered2.push_back(e);
          else {
            if (scope && scope->schema && (scope->schema->verbose || scope->schema->condgen_debug)) {
              std::cerr << "[GBY][FILTER] drop window expr in GROUP BY" << std::endl;
            }
          }
        }
        group_by_exprs.swap(filtered2);
        if (group_by_exprs.empty()) {
          // 兜底选择一个安全物理列或常量
          try {
            auto cref = make_shared<column_reference>(this);
            group_by_exprs.push_back(cref);
          } catch (...) {
            sqltype* fb = scope->schema->inttype ? scope->schema->inttype : sqltype::get("int");
            auto ce = make_shared<const_expr>(this, fb);
            group_by_exprs.push_back(ce);
          }
        }
      }
      
      auto is_pure_constant = [&](const std::shared_ptr<value_expr>& v)->bool {
        if (!v) return false;
        // 仅当表达式子树不包含列引用/子查询等，视为纯常量表达式
        if (dynamic_cast<column_reference*>(v.get())) return false;
        if (dynamic_cast<window_function*>(v.get())) return false;
        // 检查 funcall/coalesce/nullif/case 的子参是否均为“非列引用”
        std::function<bool(const value_expr*)> no_col_in_tree;
        no_col_in_tree = [&](const value_expr* e)->bool {
          if (!e) return true;
          if (dynamic_cast<const column_reference*>(e)) return false;
          if (dynamic_cast<const window_function*>(e)) return false;
          if (auto f = dynamic_cast<const funcall*>(e)) {
            for (auto &p : f->parms) { if (!no_col_in_tree(p.get())) return false; }
            return true;
          }
          if (auto c = dynamic_cast<const coalesce*>(e)) {
            for (auto &p : c->value_exprs) { if (!no_col_in_tree(p.get())) return false; }
            return true;
          }
          if (auto k = dynamic_cast<const case_expr*>(e)) {
            // 仅判断 true/false 分支（条件属于 bool_expr，不影响 GROUP BY）
            return no_col_in_tree(k->true_expr.get()) && no_col_in_tree(k->false_expr.get());
          }
          // const_expr / atomic_subselect 等其它：atomic_subselect 属列来源，视为非常量
          if (dynamic_cast<const atomic_subselect*>(e)) return false;
          if (dynamic_cast<const const_expr*>(e)) return true;
          // 其它未识别表达式，按保守：视为非常量（避免误删）
          return false;
        };
        return no_col_in_tree(v.get());
      };
      auto is_numeric_token = [&](const std::shared_ptr<value_expr>& v)->bool {
        // 通过文本判断是否为纯数值 token（避免 MySQL 位次解释）
        std::ostringstream oss; oss << *v; std::string s = oss.str();
        auto trim = [&](std::string &x){ size_t a=0; while(a<x.size() && isspace((unsigned char)x[a])) ++a; size_t b=x.size(); while(b> a && isspace((unsigned char)x[b-1])) --b; x = x.substr(a,b-a); };
        trim(s);
        if (s.empty()) return false;
        // 数值匹配：可选正负，整数/小数，且不含别名点号
        bool has_dot_alias = (s.find('.') != std::string::npos && s.find(".") != std::string::npos && s.find(" ")==std::string::npos);
        if (has_dot_alias) return false;
        // 简单数值正则：^[-+]?\d+(\.\d+)?$
        bool all_digits = true; bool has_decimal=false; size_t i=0; if(s[0]=='-'||s[0]=='+') i=1; for(; i<s.size(); ++i){ char c=s[i]; if (c=='.'){ if (has_decimal) { all_digits=false; break; } has_decimal=true; continue; } if (!isdigit((unsigned char)c)) { all_digits=false; break; } }
        return all_digits;
      };
      if (scope && scope->schema && scope->schema->mysql_mode) {
        std::vector<shared_ptr<value_expr>> filtered;
        for (auto &e : group_by_exprs) {
          if (is_numeric_token(e)) {
            if (scope->schema->verbose || scope->schema->condgen_debug) {
              std::ostringstream os; os << *e; std::cerr << "[GBY][FILTER] drop numeric token '" << os.str() << "'" << std::endl;
            }
            continue;
          }
          if (is_pure_constant(e)) {
            if (scope->schema->verbose || scope->schema->condgen_debug) {
              std::cerr << "[GBY][FILTER] drop pure constant expr" << std::endl;
            }
            continue;
          }
          filtered.push_back(e);
        }
        group_by_exprs.swap(filtered);
        if (group_by_exprs.empty()) {
          // 选择一个安全物理列兜底
          try {
            auto cref = make_shared<column_reference>(this);
            group_by_exprs.push_back(cref);
            if (scope->schema->verbose || scope->schema->condgen_debug) {
              std::ostringstream os; os << *cref; std::cerr << "[GBY][FALLBACK] select safe column " << os.str() << std::endl;
            }
          } catch (...) {
            // 极端失败：使用 CAST(0 AS SIGNED) 兜底（不会被 MySQL 解释为位次）
            sqltype* fb = scope->schema->inttype ? scope->schema->inttype : sqltype::get("int");
            auto ce = make_shared<const_expr>(this, fb);
            group_by_exprs.push_back(ce);
            if (scope->schema->verbose || scope->schema->condgen_debug) {
              std::cerr << "[GBY][FALLBACK] select safe const expr" << std::endl;
            }
          }
        }
      }
      // HAVING（占位取消）：遵循“非必选”，此处不再强制创建初始谓词；条件生成器按概率注入
      // having_guard hctx(this);
      // having = bool_expr::factory(&hctx);
    }
    // 语句级统计回传：跨语句概率控制器计数与动态校准
    batch_on_stmt_generated(has_win ? 1 : 0, has_agg ? 1 : 0);
  }
  
  set_quantifier = (d100() == 1) ? "distinct" : "";

  search = bool_expr::factory(this);

  if (d6() > 2) {
    ostringstream cons;
    cons << "limit " << d100() + d100();
    limit_clause = cons.str();
  }
}

long prepare_stmt::seq;

void modifying_stmt::pick_victim()
{
  do {
      struct named_relation *pick = random_pick(scope->tables);
      victim = dynamic_cast<struct table*>(pick);
      retry();
    } while (! victim
           || victim->schema == "pg_catalog"
           || !victim->is_base_table
           || !victim->columns().size());
}

modifying_stmt::modifying_stmt(prod *p, struct scope *s, table *victim)
  : prod(p), myscope(s)
{
  scope = &myscope;
  scope->tables = s->tables;

  if (!victim)
    pick_victim();
}


delete_stmt::delete_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p,s,v) {
  scope->refs.push_back(victim);
  search = bool_expr::factory(this);
}

delete_returning::delete_returning(prod *p, struct scope *s, table *victim)
  : delete_stmt(p, s, victim) {
  match();
  select_list = make_shared<struct select_list>(this);
}

insert_stmt::insert_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v)
{
  match();

  for (auto col : victim->columns()) {
    auto expr = value_expr::factory(this, col.type);
    assert(expr->type == col.type);
    value_exprs.push_back(expr);
  }
}

void insert_stmt::out(std::ostream &out)
{
  out << "insert into " << victim->ident() << " ";

  if (!value_exprs.size()) {
    out << "default values";
    return;
  }

  out << "values (";
  
  for (auto expr = value_exprs.begin();
       expr != value_exprs.end();
       expr++) {
    indent(out);
    out << **expr;
    if (expr+1 != value_exprs.end())
      out << ", ";
  }
  out << ")";
}

set_list::set_list(prod *p, table *target) : prod(p)
{
  do {
    for (auto col : target->columns()) {
      if (d6() < 4)
        continue;
      auto expr = value_expr::factory(this, col.type);
      value_exprs.push_back(expr);
      names.push_back(col.name);
    }
  } while (!names.size());
}

void set_list::out(std::ostream &out)
{
  assert(names.size());
  out << " set ";
  for (size_t i = 0; i < names.size(); i++) {
    indent(out);
    out << names[i] << " = " << *value_exprs[i];
    if (i+1 != names.size())
      out << ", ";
  }
}

update_stmt::update_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v) {
  scope->refs.push_back(victim);
  search = bool_expr::factory(this);
  set_list = make_shared<struct set_list>(this, victim);
}

void update_stmt::out(std::ostream &out)
{
  out << "update " << victim->ident() << *set_list;
}

update_returning::update_returning(prod *p, struct scope *s, table *v)
  : update_stmt(p, s, v) {
  match();

  select_list = make_shared<struct select_list>(this);
}

upsert_stmt::upsert_stmt(prod *p, struct scope *s, table *v)
  : insert_stmt(p,s,v)
{
  match();

  if (!victim->constraints.size())
    fail("need table w/ constraint for upsert");
    
  set_list = std::make_shared<struct set_list>(this, victim);
  search = bool_expr::factory(this);
  constraint = random_pick(victim->constraints);
}

insert_returning::insert_returning(prod *p, struct scope *s, table *victim)
  : insert_stmt(p, s, victim) {
  match();
  select_list = make_shared<struct select_list>(this);
}

upsert_returning::upsert_returning(prod *p, struct scope *s, table *v)
  : upsert_stmt(p, s, v) {
  match();
  select_list = make_shared<struct select_list>(this);
}

merge_returning::merge_returning(prod *p, struct scope *s, table *v)
  : merge_stmt(p, s, v) {
  match();
  select_list = make_shared<struct select_list>(this);
}

shared_ptr<prod> statement_factory(struct scope *s)
{
  try {
    s->new_stmt();
    batch_prepare_next();
    // 仅在启用时才可能生成对应语句类型（默认 select-only）
    if (s && s->schema) {
      const auto &feat = s->schema->feature;
      if (feat.stmt_enable_merge && d42() == 1) {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_merge);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: merge" << std::endl;
          return make_shared<merge_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks MERGE RETURNING; generating non-returning MERGE" << std::endl;
        }
        return make_shared<merge_stmt>((struct prod *)0, s);
      }
      if (feat.stmt_enable_insert && d42() == 1) {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_insert);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: insert" << std::endl;
          return make_shared<insert_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks INSERT RETURNING; generating non-returning INSERT" << std::endl;
        }
        return make_shared<insert_stmt>((struct prod *)0, s);
      }
      else if (feat.stmt_enable_delete && d42() == 1)
        return make_shared<delete_returning>((struct prod *)0, s);
      else if (feat.stmt_enable_upsert && d42() == 1) {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_upsert);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: upsert" << std::endl;
          return make_shared<upsert_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks UPSERT RETURNING; generating non-returning UPSERT" << std::endl;
        }
        return make_shared<upsert_stmt>((struct prod *)0, s);
      } else if (feat.stmt_enable_update && d42() == 1)
        return make_shared<update_returning>((struct prod *)0, s);
    }
    if (s && s->schema && s->schema->feature.stmt_enable_select && d6() > 4)
      return make_shared<select_for_update>((struct prod *)0, s);
    else if (s && s->schema && s->schema->feature.stmt_enable_select && d6() > 5 && s->schema->enable_cte)
      return make_shared<common_table_expression>((struct prod *)0, s);
    // 若 select 关闭而前述分支均未命中，按启用的非 select 类型兜底输出（保持产量）
    if (s && s->schema && !s->schema->feature.stmt_enable_select) {
      const auto &feat = s->schema->feature;
      if (feat.stmt_enable_insert) {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_insert);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: insert (fallback path)" << std::endl;
          return make_shared<insert_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks INSERT RETURNING; generating non-returning INSERT (fallback path)" << std::endl;
        }
        return make_shared<insert_stmt>((struct prod *)0, s);
      }
      if (feat.stmt_enable_delete) return make_shared<delete_returning>((struct prod *)0, s);
      if (feat.stmt_enable_upsert) {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_upsert);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: upsert (fallback path)" << std::endl;
          return make_shared<upsert_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks UPSERT RETURNING; generating non-returning UPSERT (fallback path)" << std::endl;
        }
        return make_shared<upsert_stmt>((struct prod *)0, s);
      }
      if (feat.stmt_enable_update) return make_shared<update_returning>((struct prod *)0, s);
      if (feat.stmt_enable_merge)  {
        bool want_ret = (feat.ret_enable_global || feat.ret_enable_merge);
        bool mysql = (s && s->schema && s->schema->mysql_mode);
        if (want_ret && !mysql) {
          std::cerr << "[RET] enabled: merge (fallback path)" << std::endl;
          return make_shared<merge_returning>((struct prod *)0, s);
        } else if (want_ret && mysql) {
          std::cerr << "[RET] fallback: dialect lacks MERGE RETURNING; generating non-returning MERGE (fallback path)" << std::endl;
        }
        return make_shared<merge_stmt>((struct prod *)0, s);
      }
    }
    return make_shared<query_spec>((struct prod *)0, s);
  } catch (runtime_error &e) {
    return statement_factory(s);
  }
}

void common_table_expression::accept(prod_visitor *v)
{
  v->visit(this);
  for(auto q : with_queries)
    q->accept(v);
  query->accept(v);
}

common_table_expression::common_table_expression(prod *parent, struct scope *s)
  : prod(parent), myscope(s)
{
  scope = &myscope;
  do {
    shared_ptr<query_spec> query = make_shared<query_spec>(this, s);
    with_queries.push_back(query);
    string alias = scope->stmt_uid("jennifer");
    relation *relation = &query->select_list->derived_table;
    auto aliased_rel = make_shared<aliased_relation>(alias, relation);
    refs.push_back(aliased_rel);
    scope->tables.push_back(&*aliased_rel);

  } while (d6() > 2);

 retry:
  do {
    auto pick = random_pick(s->tables);
    scope->tables.push_back(pick);
  } while (d6() > 3);
  try {
    query = make_shared<query_spec>(this, scope);
  } catch (runtime_error &e) {
    retry();
    goto retry;
  }

}

void common_table_expression::out(std::ostream &out)
{
  // 全局开关：关闭时不输出 WITH/RECURSIVE，直接走原有 SELECT 路径
  if (!scope || !scope->schema || !scope->schema->enable_cte) { out << *query; return; }

  // 仅在 MySQL 模式下按批次概率触发 CTE 扩展；命中时 50% 选择 WITH 或 WITH RECURSIVE
  bool mysql = (scope && scope->schema) ? scope->schema->mysql_mode : false;
  double p = batch_get_p_cond();
  auto hit = [&](){ try { std::uniform_real_distribution<double> U(0.0,1.0); return U(smith::rng) < p; } catch(...) { return false; } };
  auto flip = [&](){ try { std::uniform_int_distribution<int> C(0,1); return C(smith::rng)==1; } catch(...) { return false; } };

  // 不存在 CTE 子查询或非 SELECT（理论上不会发生）→ 回退为普通 WITH
  bool allow_extend = mysql && !with_queries.empty();
  if (!allow_extend || !hit()) {
    // 普通 WITH：照常输出现有 CTE 与最终查询
    out << "WITH ";
    for (size_t i = 0; i < with_queries.size(); i++) {
      indent(out);
      out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
      if (i+1 != with_queries.size()) out << ", ";
      indent(out);
    }
    out << *query; indent(out);
    return;
  }

  // 在 50% 概率下选择 WITH 或 WITH RECURSIVE
  bool use_recursive = flip();
  if (!use_recursive) {
    out << "WITH ";
    for (size_t i = 0; i < with_queries.size(); i++) {
      indent(out);
      out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
      if (i+1 != with_queries.size()) out << ", ";
      indent(out);
    }
    out << *query; indent(out);
    return;
  }

  // WITH RECURSIVE：基于首个 CTE 子查询构造 anchor + UNION ALL + recursive term（WHERE 0 终止）
  // 安全回退：任何异常则退回普通 WITH
  try {
    const std::string cte_name = refs[0]->ident();
    std::ostringstream anchor; anchor << *with_queries[0];
    // 输出递归 CTE 首项
    out << "WITH RECURSIVE " << cte_name << " AS (";
    indent(out);
    // 保持列数与类型一致（SELECT * FROM (anchor) AS base）
    out << "SELECT * FROM (" << anchor.str() << ") AS base";
    indent(out);
    out << "UNION ALL";
    indent(out);
    // 引用当前 cte_name，并以 WHERE 0 作为零行终止守卫，避免默认改变结果集与死循环
    out << "SELECT * FROM " << cte_name << " WHERE 0";
    out << ")"; // 结束递归 CTE 定义
    // 若存在额外 CTE，按普通 WITH 方式继续声明（非递归）
    for (size_t i = 1; i < with_queries.size(); i++) {
      out << ", "; indent(out);
      out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
    }
    indent(out);
    // 最终 SELECT 紧随 CTE 输出；ORDER BY/LIMIT 保持在最外层，由 *query 负责
    out << *query; indent(out);
    return;
  } catch (...) {
    // 回退普通 WITH
    out << "WITH ";
    for (size_t i = 0; i < with_queries.size(); i++) {
      indent(out);
      out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
      if (i+1 != with_queries.size()) out << ", ";
      indent(out);
    }
    out << *query; indent(out);
    return;
  }
}

merge_stmt::merge_stmt(prod *p, struct scope *s, table *v)
     : modifying_stmt(p,s,v) {
  match();
  target_table_ = make_shared<target_table>(this, victim);
  data_source = table_ref::factory(this);
  // join_condition = join_cond::factory(this, *target_table_, *data_source);
  join_condition = make_shared<simple_join_cond>(this, *target_table_, *data_source);


  /* Put data_source into scope but not target_table.  Visibility of
     the latter varies depending on kind of when clause. */
  // for (auto r : data_source->refs)
  // scope->refs.push_back(&*r);

  clauselist.push_back(when_clause::factory(this));
  while (d6()>4)
    clauselist.push_back(when_clause::factory(this));
}

void merge_stmt::out(std::ostream &out)
{
     out << "MERGE INTO " << *target_table_;
     indent(out);
     out << "USING " << *data_source;
     indent(out);
     out << "ON " << *join_condition;
     indent(out);
     for (auto p : clauselist) {
       out << *p;
       indent(out);
     }
}

void merge_stmt::accept(prod_visitor *v)
{
  v->visit(this);
  target_table_->accept(v);
  data_source->accept(v);
  join_condition->accept(v);
  for (auto p : clauselist)
    p->accept(v);
    
}

when_clause::when_clause(merge_stmt *p)
  : prod(p)
{
  condition = bool_expr::factory(this);
  matched = d6() > 3;
}

void when_clause::out(std::ostream &out)
{
  out << (matched ? "WHEN MATCHED " : "WHEN NOT MATCHED");
  indent(out);
  out << "AND " << *condition;
  indent(out);
  out << " THEN ";
  out << (matched ? "DELETE" : "DO NOTHING");
}

void when_clause::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
}

when_clause_update::when_clause_update(merge_stmt *p)
  : when_clause(p), myscope(p->scope)
{
  myscope.tables = scope->tables;
  myscope.refs = scope->refs;
  scope = &myscope;
  scope->refs.push_back(&*(p->target_table_->refs[0]));
  
  set_list = std::make_shared<struct set_list>(this, p->victim);
}

void when_clause_update::out(std::ostream &out) {
  out << "WHEN MATCHED AND " << *condition;
  indent(out);
  out << " THEN UPDATE " << *set_list;
}

void when_clause_update::accept(prod_visitor *v)
{
  v->visit(this);
  set_list->accept(v);
}


when_clause_insert::when_clause_insert(struct merge_stmt *p)
  : when_clause(p)
{
  for (auto col : p->victim->columns()) {
    auto expr = value_expr::factory(this, col.type);
    assert(expr->type == col.type);
    exprs.push_back(expr);
  }
}

void when_clause_insert::out(std::ostream &out) {
  out << "WHEN NOT MATCHED AND " << *condition;
  indent(out);
  out << " THEN INSERT VALUES ( ";

  for (auto expr = exprs.begin();
       expr != exprs.end();
       expr++) {
    out << **expr;
    if (expr+1 != exprs.end())
      out << ", ";
  }
  out << ")";

}

void when_clause_insert::accept(prod_visitor *v)
{
  v->visit(this);
  for (auto p : exprs)
    p->accept(v);
}

shared_ptr<when_clause> when_clause::factory(struct merge_stmt *p)
{
  try {
    switch(d6()) {
    case 1:
    case 2:
      return make_shared<when_clause_insert>(p);
    case 3:
    case 4:
      return make_shared<when_clause_update>(p);
    default:
      return make_shared<when_clause>(p);
    }
  } catch (runtime_error &e) {
    p->retry();
  }
  return factory(p);
}
