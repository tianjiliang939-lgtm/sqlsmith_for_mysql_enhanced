#include "temp_dataset.hh"
#include <sstream>
#include <unordered_set>

using namespace std;

// 可选类型覆盖（若 schema 不支持某类型会回退到 varchar/int）
static const unordered_set<string> kAllowedTypes = {
  // 数值
  "tinyint","smallint","mediumint","int","bigint","decimal","float","double",
  // 日期时间
  "date","time","datetime","timestamp",
  // 字符与文本
  "char","varchar","text",
  // JSON
  "json",
  // 位与二进制
  "bit","binary","varbinary","blob",
  // 枚举/集合（工程若支持）
  "enum","set",
  // 空间（统一 geometry，或具体 point/linestring/polygon 等）
  "geometry","point","linestring","polygon"
};

static inline bool type_supported(sqltype *t) {
  if (!t) return false;
  return kAllowedTypes.count(safe_type_name(t)) > 0;
}

void temp_dataset_ref::decide_mode() {
  // 在 MySQL 模式下，优先以等概率在 VALUES 与 TABLE 之间选择；若无可用表，则强制 VALUES
  bool mysql = scope && scope->schema && scope->schema->mysql_mode;
  bool have_table = scope && scope->tables.size() > 0;
  if (!mysql) {
    use_values = true; // 仅 MySQL 8.0 目标，非 MySQL 模式保守退回 VALUES 派生表
    return;
  }
  if (!have_table) { use_values = true; return; }
  // 等概率选择
  use_values = (d6() % 2 == 0);
}

static inline string colname(int i) { ostringstream oss; oss << "c" << i; return oss.str(); }

void temp_dataset_ref::build_values_schema() {
  // 随机 1..10 列；列类型从 schema->types 中挑选（受限于 kAllowedTypes），否则回退到 varchar/int
  int cols = 1 + (d6() % 10); // 1..10（近似随机）
  alias = scope->stmt_uid("v");
  col_names.clear(); col_types.clear();
  values_rel.columns().clear();
  for (int i = 1; i <= cols; ++i) {
    sqltype *pick = nullptr;
    // 按可用类型挑选
    if (scope && scope->schema) {
      auto types = scope->schema->types; // 收集于加载列阶段
      if (!types.empty()) {
        for (int tries = 0; tries < 8; ++tries) {
          sqltype *t = random_pick(types);
          if (t && type_supported(t)) { pick = t; break; }
          retry();
        }
      }
    }
    if (!pick) {
      // 兜底：数值/字符串二选一
      pick = ((d6()%2)==0) ? sqltype::get("int") : sqltype::get("varchar");
      if (!pick) pick = sqltype::get("varchar");
    }
    const string cname = colname(i);
    const string tname = safe_type_name(pick);
    col_names.push_back(cname);
    col_types.push_back(tname);
    values_rel.columns().push_back(column(cname, pick));
  }
  // 注册派生别名到 scope（供列引用）
  refs.push_back(make_shared<aliased_relation>(alias, &values_rel));
}

string temp_dataset_ref::gen_value_for_type(const string &tname) {
  // 注入 NULL/空/边界值：约 10% NULL，10% 空/极限
  int r = d100();
  if (r < 10) return "NULL";
  auto R = [](){ return d100(); };
  if (tname == "tinyint" || tname == "smallint" || tname == "mediumint" || tname == "int" || tname == "bigint") {
    if (r < 20) return to_string((R()%2) ? INT32_MAX : INT32_MIN);
    return to_string(R());
  } else if (tname == "decimal") {
    // 控制精度：DECIMAL(18,6) 近似
    ostringstream oss; oss << (R()%1000000) << "." << (R()%1000000); return oss.str();
  } else if (tname == "float" || tname == "double") {
    ostringstream oss; oss << (R()%10000) * 0.01; return oss.str();
  } else if (tname == "date") {
    if (r < 20) return q("1970-01-01");
    if (r < 30) return q("9999-12-31");
    return q("2020-01-0" + to_string(1 + (R()%9)));
  } else if (tname == "time") {
    if (r < 20) return q("00:00:00");
    return q("23:59:" + to_string(R()%60));
  } else if (tname == "datetime" || tname == "timestamp") {
    if (r < 20) return "CAST('1970-01-01 00:00:00' AS DATETIME)";
    return "CAST('2020-01-01 12:34:56' AS DATETIME)";
  } else if (tname == "char" || tname == "varchar" || tname == "text") {
    if (r < 15) return q("");
    ostringstream s; s << "s" << R(); return q(s.str());
  } else if (tname == "json") {
    if (r < 20) return "JSON_ARRAY()"; // 空 JSON
    // 混合对象与数组
    if (R()%2) return "JSON_OBJECT('k0','v' , 'k1'," + to_string(R()%100) + ")";
    else return "JSON_ARRAY('x'," + to_string(R()%10) + ",true,NULL)";
  } else if (tname == "bit") {
    // 生成 4~8 位的位串
    int n = 4 + (R()%5);
    string b; for (int i=0;i<n;i++) b += ((R()%2)?'1':'0');
    return string("b'") + b + "'";
  } else if (tname == "binary" || tname == "varbinary" || tname == "blob") {
    // 生成 4~16 字节十六进制，统一偶数字符长度，必要时前补'0'
    int n = 4 + (R()%13);
    const char hex[17] = "0123456789ABCDEF";
    string h; for (int i=0;i<n;i++){ h += hex[R()%16]; }
    if (h.size() % 2 != 0) h.insert(0, 1, '0');
    return string("X'") + h + "'";
  } else if (tname == "enum" || tname == "set") {
    // 兜底使用字符串
    return q("opt" + to_string(R()%5));
  } else if (tname == "geometry" || tname == "point") {
    int x = 10 + (R()%90), y = 10 + (R()%90);
    ostringstream oss; oss << "ST_GeomFromText('POINT(" << x << " " << y << ")', 4326)";
    return oss.str();
  } else if (tname == "linestring") {
    ostringstream wkt; wkt << "LINESTRING(10 10, 20 30, 40 50)";
    return string("ST_GeomFromText('") + wkt.str() + "', 4326)";
  } else if (tname == "polygon") {
    ostringstream wkt; wkt << "POLYGON((0 0,1 0,1 1,0 1,0 0))";
    return string("ST_GeomFromText('") + wkt.str() + "', 4326)";
  }
  // 默认兜底：字符串
  return q("s" + to_string(R()));
}

void temp_dataset_ref::fill_values_rows() {
  int nrows = 1 + (d6() % 50); // 1..50（近似随机）
  rows.clear(); rows.reserve(nrows);
  for (int r = 0; r < nrows; ++r) {
    vector<string> one;
    for (size_t i = 0; i < col_types.size(); ++i) {
      one.push_back(gen_value_for_type(col_types[i]));
    }
    rows.push_back(move(one));
  }
}

temp_dataset_ref::temp_dataset_ref(prod *p) : table_ref(p) {
  decide_mode();
  if (use_values) {
    build_values_schema();
    fill_values_rows();
  } else {
    // TABLE 模式：选择真实表；若失败则回退 VALUES
    int retries = 0;
    while (retries++ < 10) {
      auto pick = random_pick(scope->tables);
      table *t = dynamic_cast<table*>(pick);
      if (t && t->is_base_table) { chosen_table = t; break; }
      retry();
    }
    if (!chosen_table) {
      use_values = true; build_values_schema(); fill_values_rows();
    } else {
      // 引用别名与 refs：使用原表列引用，但打印时输出 (TABLE schema.table) AS alias
      alias = scope->stmt_uid("t");
      refs.push_back(make_shared<aliased_relation>(alias, chosen_table));
    }
  }
}

void temp_dataset_ref::out(std::ostream &out) {
  if (use_values) {
    // 输出形如：(VALUES ROW(...), ROW(...)) AS t(c1,...)
    out << "(";
    out << "VALUES ";
    for (size_t r = 0; r < rows.size(); ++r) {
      if (r) out << ", ";
      out << "ROW(";
      for (size_t c = 0; c < rows[r].size(); ++c) {
        if (c) out << ", ";
        out << rows[r][c];
      }
      out << ")";
    }
    out << ") AS " << alias << "(";
    for (size_t i = 0; i < col_names.size(); ++i) {
      if (i) out << ", ";
      out << col_names[i];
    }
    out << ")";
  } else {
    // 输出形如：(TABLE schema.table) AS t
    out << "(TABLE " << chosen_table->ident() << ") AS " << alias;
  }
}
