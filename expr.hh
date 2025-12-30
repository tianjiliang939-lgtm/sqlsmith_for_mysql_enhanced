/// @file
/// @brief grammar: Value expression productions

#ifndef EXPR_HH
#define EXPR_HH


#include "prod.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "random.hh"
#include <string>
#include <memory>
#include <vector>
#include <ostream>
#include <utility>

using std::shared_ptr;
using std::vector;
using std::string;

// 稳定类型族枚举：避免在输出阶段依赖易失字符串
enum class SafeTypeFamily {
  Unknown = 0,
  Numeric,
  String,
  Date,
  Time,
  Datetime,
  Timestamp,
  Json,
  Geometry
};

// 统一在输出阶段使用“稳定类型枚举/指针身份映射 + 按值拷贝”
SafeTypeFamily safe_type_family(sqltype* t);
std::string safe_type_name(sqltype* t);

struct value_expr: prod {
  sqltype *type;
  virtual void out(std::ostream &out) = 0;
  virtual ~value_expr() { }
  value_expr(prod *p) : prod(p) { }
  static shared_ptr<value_expr> factory(prod *p, sqltype *type_constraint = 0);
};

struct case_expr : value_expr {
  shared_ptr<value_expr> condition;
  shared_ptr<value_expr> true_expr;
  shared_ptr<value_expr> false_expr;
  case_expr(prod *p, sqltype *type_constraint = 0);
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct funcall : value_expr {
  routine *proc;
  bool is_aggregate;
  vector<shared_ptr<value_expr> > parms;
  virtual void out(std::ostream &out);
  virtual ~funcall() { }
  funcall(prod *p, sqltype *type_constraint = 0, bool agg = 0);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    for (auto p : parms)
      p->accept(v);
  }
};

struct atomic_subselect : value_expr {
  table *tab;
  column *col;
  int offset;
  routine *agg;
  atomic_subselect(prod *p, sqltype *type_constraint = 0);
  virtual void out(std::ostream &out);
};

struct const_expr: value_expr {
  std::string expr;
  const_expr(prod *p, sqltype *type_constraint = 0);
  virtual void out(std::ostream &out) { out << expr; }
  virtual ~const_expr() { }
};

struct column_reference: value_expr {
  column_reference(prod *p, sqltype *type_constraint = 0);
  virtual void out(std::ostream &out) { out << reference; }
  std::string reference;
  virtual ~column_reference() { }
};

struct coalesce : value_expr {
  const char *abbrev_;
  vector<shared_ptr<value_expr> > value_exprs;
  virtual ~coalesce() { };
  coalesce(prod *p, sqltype *type_constraint = 0, const char *abbrev = "coalesce");
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    for (auto p : value_exprs)
      p->accept(v);
  }
};

struct nullif : coalesce {
 virtual ~nullif() { };
     nullif(prod *p, sqltype *type_constraint = 0)
          : coalesce(p, type_constraint, "nullif")
          { };
};

struct bool_expr : value_expr {
  virtual ~bool_expr() { }
  bool_expr(prod *p) : value_expr(p) { type = scope->schema->booltype; }
  static shared_ptr<bool_expr> factory(prod *p);
};

struct truth_value : bool_expr {
  virtual ~truth_value() { }
  const char *op;
  virtual void out(std::ostream &out) { out << op; }
  truth_value(prod *p) : bool_expr(p) {
    bool allow_false = (scope && scope->schema) ? scope->schema->feature.allow_false_cond : true;
    op = (allow_false ? ((d6() < 4) ? scope->schema->true_literal : scope->schema->false_literal) : scope->schema->true_literal);
  }
};

struct null_predicate : bool_expr {
  virtual ~null_predicate() { }
  const char *negate;
  shared_ptr<value_expr> expr;
  null_predicate(prod *p) : bool_expr(p) {
    negate = ((d6()<4) ? "not " : "");
    expr = value_expr::factory(this);
  }
  virtual void out(std::ostream &out) {
    out << *expr << " is " << negate << "NULL";
  }
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    expr->accept(v);
  }
};

struct exists_predicate : bool_expr {
  shared_ptr<struct query_spec> subquery;
  virtual ~exists_predicate() { }
  exists_predicate(prod *p);
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct bool_binop : bool_expr {
  shared_ptr<value_expr> lhs, rhs;
  bool_binop(prod *p) : bool_expr(p) { }
  virtual void out(std::ostream &out) = 0;
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    lhs->accept(v);
    rhs->accept(v);
  }
};

struct bool_term : bool_binop {
  virtual ~bool_term() { }
  const char *op;
  virtual void out(std::ostream &out) {
    out << "(" << *lhs << ") ";
    indent(out);
    out << op << " (" << *rhs << ")";
  }
  bool_term(prod *p) : bool_binop(p)
  {
    lhs = bool_expr::factory(this);
    rhs = bool_expr::factory(this);
    // 常量布尔与 AND/OR 组合策略：
    // - 任一为 false → OR（避免 AND false 恒假）
    // - 否则任一为 true → AND（避免 OR true 恒真）
    // - 否则沿用原先随机选择
    auto classify = [&](const shared_ptr<value_expr>& e)->int {
      if (!e) return 0;
      auto tv = dynamic_cast<truth_value*>(e.get());
      if (!tv) return 0;
      if (!scope || !scope->schema) return 0;
      const char* tlit = scope->schema->true_literal;
      const char* flit = scope->schema->false_literal;
      const char* val  = tv->op;
      if (!val) return 0;
      std::string sval(val);
      if (flit && sval == flit) return -1; // false
      if (tlit && sval == tlit) return 1;  // true
      return 0;
    };
    int lk = classify(lhs);
    int rk = classify(rhs);
    if (lk == -1 || rk == -1) {
      op = "or";
    } else if (lk == 1 || rk == 1) {
      op = "and";
    } else {
      op = ((d6()<4) ? "or" : "and");
    }
  }
};

struct distinct_pred : bool_binop {
  distinct_pred(prod *p);
  virtual ~distinct_pred() { };
  virtual void out(std::ostream &o) {
    o << *lhs << " is distinct from " << *rhs;
  }
};

struct comparison_op : bool_binop {
  op *oper;
  comparison_op(prod *p);
  virtual ~comparison_op() { };
  virtual void out(std::ostream &o) {
    o << *lhs << " " << oper->name << " " << *rhs;
  }
};

struct window_function : value_expr {
  // 最小工具：在当前作用域生成一个类型安全的“列条件”文本（用于 EXISTS 子查询作用域补齐）
  // 根据首个安全列类型族选择谓词：
  // - 默认：col = col（数值/字符/时间等）
  // - JSON/几何：col IS NOT NULL（安全且类型兼容）
  static std::string make_column_predicate_for_scope(prod* p, int used_nullness = 0);
  // 多样化生成：轮换操作符类、打乱样本集合；避免重复签名
  static std::string make_diversified_predicate_for_scope(prod* p, int used_nullness = 0);
  static std::string make_diversified_for_alias_column(prod* p, const std::string& alias, const std::string& col_name,
                                                       sqltype* col_type, int used_nullness,
                                                       const std::string& avoid_opclass, const std::string& avoid_values_key);

  virtual void out(std::ostream &out);
  virtual ~window_function() { }
  window_function(prod *p, sqltype *type_constraint);
  vector<shared_ptr<column_reference> > partition_by;
  vector<shared_ptr<column_reference> > order_by;
  shared_ptr<funcall> aggregate;
  // 对不兼容的窗口聚合进行降级（不打印 OVER）
  bool suppress_over = false; 
  static bool allowed(prod *pprod);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    aggregate->accept(v);
    for (auto p : partition_by)
      p->accept(v);
    for (auto p : order_by)
      p->accept(v);
  }
};

// 轻量 helper：保障窗口 OVER 子句非空（不受查询级 ORDER BY 开关影响），最小改动
void ensure_non_empty_window_spec(window_function* wf);

#endif

// ---- 辅助自由函数声明（供 grammar.cc 调用） ----
namespace expr_utils {
  std::string make_diversified_predicate_for_scope(prod* p, int used_nullness);
  std::string make_diversified_for_alias_column(prod* p, const std::string& alias, const std::string& col_name,
                                                sqltype* col_type, int used_nullness,
                                                const std::string& avoid_opclass, const std::string& avoid_values_key);
  // 语句级清理：移除当前语句键下的 IN 集合指纹，避免跨语句复用导致的潜在问题（最小改动）
  void clear_inset_registry_for_stmt_key(const void* key);
}
