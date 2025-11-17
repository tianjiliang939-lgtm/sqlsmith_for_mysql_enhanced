#include "json_table_ref.hh"
#include "random.hh"
#include "schema.hh"
#include "expr.hh"
#include <ostream>
#include <sstream>

using namespace std;

// 将逻辑类型转换为 MySQL JSON_TABLE 可接受的列类型字符串
// 规则：
// - "int" → "INT"
// - "varchar" → "VARCHAR(N)"，N 从安全范围 {32, 64, 128, 255, 512} 随机选择（默认覆盖 255）
// - 其它数值类型原样返回；绝不输出不带长度的 VARCHAR
static std::string format_mysql_json_table_type(const sqltype *t) {
  if (!t) return std::string("VARCHAR(128)");
  sqltype* TVARCHAR = sqltype::get("varchar");
  sqltype* TINT = sqltype::get("int");
  if (t == TVARCHAR) {
    std::vector<int> lens = {32, 64, 128, 255, 512};
    auto it = random_pick(lens.begin(), lens.end());
    int len = *it;
    std::ostringstream oss; oss << "VARCHAR(" << len << ")"; return oss.str();
  }
  if (t == TINT) {
    return std::string("INT");
  }
  // Fallback to stable safe name (lowercase is acceptable for MySQL types)
  return safe_type_name(const_cast<sqltype*>(t));
}

json_table_ref::json_table_ref(prod *p) : table_ref(p)
{
  // 生成一个简单的 COLUMNS 映射，包含 2-4 列，类型为 VARCHAR/INT，便于 join
  int cols = 2 + d6()%3; // 2..4
  for (int i = 0; i < cols; ++i) {
    string cname = string("jt_c") + to_string(i);
    sqltype *t = (d6() > 3) ? sqltype::get("varchar") : sqltype::get("int");
    derived.columns().push_back(column(cname, t));
  }
  // 统一自动别名前缀：jt_<rand>，避免 MySQL 3667（缺少别名）
  alias = scope->stmt_uid("jt");
  // 注册到 scope.refs 以便列引用
  refs.push_back(make_shared<aliased_relation>(alias, &derived));
}

void json_table_ref::out(std::ostream &out)
{
  // 基础 JSON 文档使用 JSON_OBJECT 生成，路径简单为 '$' 或 '$.a/.b'
  out << "JSON_TABLE(";
  out << "JSON_OBJECT('a', 1, 'b', JSON_ARRAY('x','y')), '$' COLUMNS (";
  for (size_t i = 0; i < derived.columns().size(); ++i) {
    auto &c = derived.columns()[i];
    // 列路径选择：
    // - INT 列：优先选择 '$.b[0]'（避免直接引用数组），或回退到 '$.a'（源为标量）
    // - VARCHAR 列：使用 '$.a'，避免引用数组整体导致类型不匹配
    std::string tname = safe_type_name(c.type);
    std::string path;
    if (tname == "int" || tname == "INT") {
      path = (d6() > 3) ? "a" : "b[0]";
    } else {
      // 变长字符串统一从标量 'a' 取值；类型格式化器会输出 VARCHAR(N)
      path = "a";
    }
    out << c.name << " " << format_mysql_json_table_type(c.type) << " PATH '$." << path << "'";
    if (i + 1 < derived.columns().size()) out << ", ";
  }
  out << ") ) AS " << alias;
}
