#ifndef SPATIAL_EXPR_HH
#define SPATIAL_EXPR_HH

#include "expr.hh"
#include <string>
#include <vector>
#include <iosfwd>

// Spatial 表达式生成：构造（WKT/WKB/GeoJSON）、输出、测度、关系、操作与负例
struct spatial_expr : value_expr {
  std::string last_func;
  double neg_rate;
  int srid; // 选用的 SRID
  spatial_expr(prod *p, sqltype *type_constraint = 0);
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
  }
private:
  int pick_srid();
  void emit_constructor(std::ostream &out);
  void emit_measure_or_relation(std::ostream &out);
  void emit_operation(std::ostream &out);
  void emit_info(std::ostream &out);
  void emit_invalid_geometry(std::ostream &out);
  // 组合“recipes”
  void emit_combos(std::ostream &out);
};

#endif
