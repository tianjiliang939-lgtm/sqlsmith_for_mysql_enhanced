#ifndef JSON_TABLE_REF_HH
#define JSON_TABLE_REF_HH

#include "grammar.hh"
#include <string>

// 在 FROM 子句中生成 MySQL 8 JSON_TABLE(... ) 引用
struct json_table_ref : table_ref {
  std::string alias;
  // 维护派生列定义以供列引用
  relation derived;
  json_table_ref(prod *p);
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v) {
    v->visit(this);
  }
};

#endif
