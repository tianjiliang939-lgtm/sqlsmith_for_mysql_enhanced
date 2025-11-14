#include "json_expr.hh"
#include "random.hh"
#include "schema.hh"
#include <sstream>

using namespace std;

json_expr::json_expr(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  (void)type_constraint;
  type = sqltype::get("json");
  neg_rate = 0.0; // 默认负例比例（MySQL 模式下禁用非法 JSON 负例）
}

static void emit_simple_value(std::ostream &out)
{
  int which = d6();
  if (which <= 2) out << d100();
  else if (which <= 4) out << "'" << "s" << d100() << "'";
  else if (which == 5) out << (d6() > 3 ? "true" : "false");
  else out << "null";
}

void json_expr::emit_json_constant(std::ostream &out, bool nested)
{
  if (d6() > 3) {
    last_func = "JSON_OBJECT";
    int pairs = 1 + d6();
    out << "JSON_OBJECT(";
    for (int i = 0; i < pairs; ++i) {
      out << "'k" << i << "', ";
      emit_simple_value(out);
      if (i + 1 < pairs) out << ", ";
    }
    if (nested && d6() > 4) {
      out << ", 'nested', JSON_ARRAY('a', 1, 'b', 2)";
    }
    out << ")";
  } else {
    last_func = "JSON_ARRAY";
    int elems = 1 + d6();
    out << "JSON_ARRAY(";
    for (int i = 0; i < elems; ++i) {
      emit_simple_value(out);
      if (i + 1 < elems) out << ", ";
    }
    out << ")";
  }
}

void json_expr::emit_invalid_json(std::ostream &out)
{
  last_func = "INVALID_JSON";
  switch(d6()) {
    case 1: out << "'{'"; break;                  // 不闭合
    case 2: out << "'{]'"; break;                 // 括号错配
    case 3: out << "'\\x00'"; break;            // 非法字符
    case 4: out << "'{\"k\": unquoted}'"; break; // 未加引号的值
    case 5: out << "'{\"k\": Infinity}'"; break; // 非标准数字
    default: out << "'{'"; break;
  }
}

void json_expr::emit_constructor(std::ostream &out)
{
  int which = d6();
  if (which <= 2) { emit_json_constant(out, true); return; }
  if (which <= 4) {
    last_func = "JSON_MERGE_PATCH";
    out << "JSON_MERGE_PATCH("; emit_json_constant(out, false); out << ", "; emit_json_constant(out, false); out << ")"; return;
  }
  if (which == 5) {
    last_func = "JSON_MERGE_PRESERVE";
    out << "JSON_MERGE_PRESERVE("; emit_json_constant(out, false); out << ", "; emit_json_constant(out, false); out << ")"; return;
  }
  last_func = "JSON_QUOTE";
  out << "JSON_QUOTE('x')";
}

void json_expr::emit_accessor_or_modifier(std::ostream &out)
{
  int which = d12();
  if (which <= 2) {
    last_func = "JSON_EXTRACT";
    out << "JSON_EXTRACT("; emit_json_constant(out, false); out << ", '$." << (d6()>3 ? "a" : "b[0]") << "')"; return;
  }
  if (which <= 4) {
    last_func = "JSON_UNQUOTE";
    out << "JSON_UNQUOTE(JSON_QUOTE('x'))"; return;
  }
  if (which <= 6) {
    last_func = "JSON_SET";
    out << "JSON_SET("; emit_json_constant(out, false); out << ", '$.k', 'v')"; return;
  }
  if (which <= 8) {
    last_func = "JSON_INSERT";
    out << "JSON_INSERT("; emit_json_constant(out, false); out << ", '$.k', 1)"; return;
  }
  if (which <= 10) {
    last_func = "JSON_REPLACE";
    out << "JSON_REPLACE("; emit_json_constant(out, false); out << ", '$.k', 'nv')"; return;
  }
  last_func = "JSON_REMOVE";
  out << "JSON_REMOVE("; emit_json_constant(out, false); out << ", '$.k')";
}

void json_expr::emit_combos(std::ostream &out)
{
  // 轻量概率控制：依赖 json_density，外层函数名用于统计
  int which = d6();
  switch (which) {
    case 1: {
      // 嵌套修改器：JSON_SET(JSON_REPLACE(JSON_OBJECT(...), path, JSON_ARRAY(...)), path2, val2)
      last_func = "JSON_SET";
      out << "JSON_SET(";
      out << "JSON_REPLACE("; emit_json_constant(out, true); out << ", '$.k1', JSON_ARRAY('a', 1))";
      out << ", '$.k2', 'v2')";
      break;
    }
    case 2: {
      // 访问器 + 谓词：JSON_CONTAINS(JSON_EXTRACT(doc, '$.k'), JSON_ARRAY(...))
      last_func = "JSON_CONTAINS";
      out << "JSON_CONTAINS(";
      out << "JSON_EXTRACT("; emit_json_constant(out, false); out << ", '$.k')";
      out << ", JSON_ARRAY('x', 'y'))";
      break;
    }
    case 3: {
      // 合并 + 比较：JSON_OVERLAPS(JSON_MERGE_PATCH(a, b), b)
      last_func = "JSON_OVERLAPS";
      out << "JSON_OVERLAPS(JSON_MERGE_PATCH("; emit_json_constant(out, false); out << ", "; emit_json_constant(out, false); out << "), "; emit_json_constant(out, false); out << ")";
      break;
    }
    case 4: {
      // Schema 校验：VALID 或 VALIDATION_REPORT
      if (d6() > 3) {
        last_func = "JSON_SCHEMA_VALID";
        out << "JSON_SCHEMA_VALID(";
      } else {
        last_func = "JSON_SCHEMA_VALIDATION_REPORT";
        out << "JSON_SCHEMA_VALIDATION_REPORT(";
      }
      // schema: {"type":"object","properties":{"a":{"type":"integer"}}}
      out << "JSON_OBJECT('type','object','properties',JSON_OBJECT('a',JSON_OBJECT('type','integer')))";
      out << ", "; emit_json_constant(out, false); out << ")";
      break;
    }
    default: {
      // 默认：JSON_TABLE 由 grammar 负责；这里回退到常规组合
      last_func = "JSON_OVERLAPS";
      out << "JSON_OVERLAPS("; emit_json_constant(out, false); out << ", "; emit_json_constant(out, false); out << ")";
      break;
    }
  }
}

void json_expr::build_random_json(std::ostream &out)
{
  // 组合 recipes：按密度触发
  if (scope->schema->enable_json && scope->schema->mysql_mode && d100() < (int)(scope->schema->json_density * 50 + 10)) {
    emit_combos(out);
    return;
  }
  int choice = d20();
  if (choice <= 5) { emit_constructor(out); return; }
  if (choice <= 10) { emit_accessor_or_modifier(out); return; }
  if (choice <= 13) {
    last_func = "JSON_PRETTY";
    out << "JSON_PRETTY("; emit_json_constant(out, true); out << ")"; return;
  }
  if (choice <= 15) {
    last_func = "JSON_SEARCH";
    out << "JSON_SEARCH("; emit_json_constant(out, false); out << ", 'one', '" << (d6()>3?"v":"k") << d6() << "')"; return;
  }
  if (choice <= 17) {
    last_func = "JSON_LENGTH";
    out << "JSON_LENGTH("; emit_json_constant(out, false); out << ")"; return;
  }
  if (choice <= 18) {
    last_func = "JSON_DEPTH";
    out << "JSON_DEPTH("; emit_json_constant(out, false); out << ")"; return;
  }
  if (choice <= 19) {
    last_func = "JSON_OVERLAPS";
    out << "JSON_OVERLAPS("; emit_json_constant(out, false); out << ", "; emit_json_constant(out, false); out << ")"; return;
  }
  // Schema validation
  last_func = "JSON_SCHEMA_VALIDATION_REPORT";
  out << "JSON_SCHEMA_VALIDATION_REPORT(";
  out << "JSON_OBJECT('type','object','properties',JSON_OBJECT('a',JSON_OBJECT('type','integer')))";
  out << ", "; emit_json_constant(out, false); out << ")";
}

void json_expr::out(std::ostream &out)
{
  match();
  if (d100() < (int)(neg_rate*100)) { emit_invalid_json(out); return; }
  build_random_json(out);
}
