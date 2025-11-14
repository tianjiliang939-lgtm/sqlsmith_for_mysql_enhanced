#ifndef JSON_EXPR_HH
#define JSON_EXPR_HH

#include "expr.hh"
#include <string>
#include <vector>
#include <ostream>

// JSON 表达式生成：构造、访问、修改、谓词与负例
struct json_expr : value_expr {
  // 最近一次生成的函数名（用于日志统计）
  std::string last_func;
  // 控制负例的比例（由 schema 配置提供）
  double neg_rate;
  // 构造函数
  json_expr(prod *p, sqltype *type_constraint = 0);
  // 输出 SQL
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
  }
private:
  // 随机生成一种 JSON 常量或函数调用
  void build_random_json(std::ostream &out);
  // 生成 JSON 常量
  void emit_json_constant(std::ostream &out, bool nested);
  // 生成负例 JSON（无效 JSON）
  void emit_invalid_json(std::ostream &out);
  // 构造型
  void emit_constructor(std::ostream &out);
  // 访问器/谓词/修改器类函数
  void emit_accessor_or_modifier(std::ostream &out);
  // 组合“recipes”
  void emit_combos(std::ostream &out);
};

#endif
