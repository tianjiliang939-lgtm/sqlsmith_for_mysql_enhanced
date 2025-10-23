#include <string>
#include <iostream>
#include <stdexcept>
#include <regex>
#include <unordered_set> 
#include "mysql.hh"

using namespace std;
string db_name;

static regex e_syntax("You have an error in your SQL syntax.*");
void Stringsplit(string str, const char split);
// 使用字符分割
void Stringsplit(const string& str, const char split, vector<string>& res)
{
    if (str == "")        return;
    // 在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + split;
    size_t pos = strs.find(split);

    // 若找不到内容则字符串搜索函数返回 npos
    while (pos != strs.npos)
    {
        string temp = strs.substr(0, pos);
        res.push_back(temp);
        // 去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(split);
    }
}

// 文件级静态工具函数，替代重复 lambda
static inline bool type_is_numeric(const std::string& t) {
  static const std::unordered_set<std::string> kNumeric = {"int","tinyint","smallint","mediumint","bigint","float","double","decimal"};
  return kNumeric.count(t) > 0;
}

mysql_connection::mysql_connection(const std::string &conninfo)
{
  (void) conninfo;
  // host:port/dbname?username=xxxx&password=xxxx
  vector<string> strList;
  Stringsplit(conninfo, '/', strList);
  string host_port = strList[0];
  string db_other = strList[1];
  vector<string> strList1;
  Stringsplit(host_port, ':', strList1);
  string host = strList1[0];
  int port = std::stoi(strList1[1]);
  vector<string> strList2;
  Stringsplit(db_other, '?', strList2);
  db_name = strList2[0];
  string user_password = strList2[1];
  vector<string> strList3;
  Stringsplit(user_password, '&', strList3);
  string user_str = strList3[0];
  string password_str = strList3[1];
  vector<string> strList4;
  Stringsplit(user_str, '=', strList4);
  string db_user = strList4[1];
  vector<string> strList5;
  Stringsplit(password_str, '=', strList5);
  string user_pwd = strList5[1];
  cerr << "host:" << host << endl;
  cerr << "port:" << port << endl;
  cerr << "dbname:" << db_name << endl;
  cerr << "dbuser:" << db_user << endl;
  cerr << "userpassowrd:" << user_pwd << endl;
  con = mysql_init(NULL);
  if (!mysql_real_connect(con, host.c_str(), db_user.c_str(), user_pwd.c_str(), 
                          db_name.c_str()/*dbname*/, port, NULL, 0)) {
    throw runtime_error(mysql_error(con));
  }
}

void mysql_connection::q(std::string s)
{
  if (mysql_query(con, s.c_str()))
    error();
} 

void mysql_connection::error()
{
  throw runtime_error(mysql_error(con));
} 

mysql_connection::~mysql_connection()
{
  mysql_close(con);
}

// 数学函数白名单与工具函数，用于注册过滤
static const std::unordered_set<std::string> kMathWhitelistNames = {
  "ABS","SQRT","ROUND","FLOOR","CEIL","CEILING","EXP",
  "LOG","LOG10","LOG2","LN","POW","POWER","DEGREES","RADIANS",
  "SIN","COS","TAN","ASIN","ACOS","ATAN","SIGN","COT"
};
static inline bool is_numeric_type(sqltype *t) {
  if (!t) return false;
  const std::string n = t->name;
  return (n=="int" || n=="tinyint" || n=="smallint" || n=="mediumint" || n=="bigint" || n=="float" || n=="double" || n=="decimal");
}
// 附加类型族工具
static inline bool is_integer_type(sqltype *t) {
  if (!t) return false;
  const std::string n = t->name;
  return (n=="int" || n=="tinyint" || n=="smallint" || n=="mediumint" || n=="bigint");
}
static inline bool is_string_type(sqltype *t) {
  return t && (t->name == std::string("varchar"));
}
static inline bool is_date_type(sqltype *t) {
  return t && (t->name == std::string("date"));
}
static inline bool is_datetime_type(sqltype *t) {
  return t && (t->name == std::string("datetime") || t->name == std::string("timestamp"));
}
static inline bool is_json_type(sqltype *t) {
  return t && (t->name == std::string("json"));
}
static inline bool is_geometry_type(sqltype *t) {
  return t && (t->name == std::string("geometry"));
}
static inline bool is_comparable_type(sqltype *t) {
  return is_numeric_type(t) || is_string_type(t) || is_date_type(t) || is_datetime_type(t);
}

// 覆写注册：仅对白名单数学函数实施 numeric→numeric 过滤，其它函数原样注册
void schema_mysql::register_routine(routine& r) {
  const std::string name = r.name;
  auto down = [](std::string s){ for (auto &c : s) c = (char)std::tolower((unsigned char)c); return s; };
  const std::string lname = down(name);
  // 按开关剔除 FOUND_ROWS 系统函数（大小写无关）
  if (this->feature.exclude_found_rows && lname == std::string("found_rows")) {
    return;
  }
  // 注册阶段守卫：当禁用不确定函数时，不注册以下函数以避免进入选择池（大小写无关）
  if (this->feature.disable_nondeterministic_funcs) {
    static const std::unordered_set<std::string> kNonDeterministicNames = {
      "current_date","current_time","current_timestamp","localtime","localtimestamp",
      "now","sysdate","curdate","curtime",
      "utc_date","utc_time","utc_timestamp",
      "unix_timestamp",
      "rand","random","uuid","uuid_short","uuid_to_bin","bin_to_uuid",
      "connection_id","found_rows","last_insert_id","row_count","version","database","schema","user","current_user","session_user","system_user",
      "get_lock","release_lock","is_free_lock","is_used_lock","sleep","benchmark",
      "master_pos_wait","load_file","connection_property","ps_current_thread_id"
    };
    if (kNonDeterministicNames.count(lname)>0) {
      return;
    }
  }
  if (kMathWhitelistNames.count(name)) {
    // 数学函数返回值必须为数值
    if (!is_numeric_type(r.restype)) return; 
    for (auto *argt : r.argtypes) {
      // 数学函数入参必须为数值
      if (!is_numeric_type(argt)) return; 
    }
  }
  // FORMAT(X,D[,locale]): X∈NUMERIC，D∈INT，locale∈STRING；禁止 X 为 GEOMETRY/JSON
  if (name == "FORMAT") {
    if (!((r.argtypes.size() == 2) || (r.argtypes.size() == 3))) return;
    // MySQL FORMAT 返回字符串
    if (!is_string_type(r.restype)) return; 
    if (!is_numeric_type(r.argtypes[0]) || is_json_type(r.argtypes[0]) || is_geometry_type(r.argtypes[0])) return;
    if (!is_integer_type(r.argtypes[1])) return;
    if (r.argtypes.size() == 3 && !is_string_type(r.argtypes[2])) return;
  }
  // GREATEST/LEAST(args...): 参数同一类型族（NUMERIC 或 STRING），禁止混入 GEOMETRY/JSON
  if (name == "GREATEST" || name == "LEAST") {
    if (r.argtypes.size() < 2) return;
    sqltype *a0 = r.argtypes[0];
    sqltype *a1 = r.argtypes[1];
    bool both_numeric = is_numeric_type(a0) && is_numeric_type(a1);
    bool both_string = is_string_type(a0) && is_string_type(a1);
    if (!(both_numeric || both_string)) return;
    if (is_json_type(a0) || is_json_type(a1) || is_geometry_type(a0) || is_geometry_type(a1)) return;
    // restype 必须与参数族一致：数值→数值、字符串→字符串
    if (both_numeric && !is_numeric_type(r.restype)) return;
    if (both_string && !is_string_type(r.restype)) return;
  }
  // CONV(N, from_base, to_base): N∈STRING|INT，基数为 INT（范围在 out() 中防呆限制为 2–36）；返回 STRING
  if (name == "CONV") {
    if (r.argtypes.size() != 3) return;
    if (!(is_string_type(r.argtypes[0]) || is_integer_type(r.argtypes[0]))) return;
    if (!is_integer_type(r.argtypes[1]) || !is_integer_type(r.argtypes[2])) return;
    if (!is_string_type(r.restype)) return; // MySQL CONV 返回字符串
  }
  // COT(X): X∈REAL(NUMERIC)
  if (name == "COT") {
    if (r.argtypes.size() != 1 || !is_numeric_type(r.argtypes[0]) || !is_numeric_type(r.restype)) return;
  }
  schema::register_routine(r);
}

// 覆写聚合注册：数值聚合仅 numeric→numeric；JSON/GROUP 聚合禁止空间入参（geometry）
void schema_mysql::register_aggregate(routine& r) {
  const std::string name = r.name;
  static const std::unordered_set<std::string> kNumericAgg = {
    "COUNT","SUM","AVG",
    "STDDEV","STDDEV_POP","STDDEV_SAMP",
    "VARIANCE","VAR_POP","VAR_SAMP"
  };
  static const std::unordered_set<std::string> kJsonGroupAgg = {
    "JSON_ARRAYAGG","JSON_OBJECTAGG","GROUP_CONCAT"
  };
  if (kNumericAgg.count(name)) {
    // 返回值必须为数值
    if (!is_numeric_type(r.restype)) return; 
    for (auto *argt : r.argtypes) {
      // 入参必须为数值
      if (!is_numeric_type(argt)) return; 
    }
  }
  if (name == "MIN" || name == "MAX") {
    // 仅允许可比较类型；禁止 GEOMETRY/JSON
    if (!is_comparable_type(r.restype) || is_geometry_type(r.restype) || is_json_type(r.restype)) return;
    for (auto *argt : r.argtypes) {
      if (!is_comparable_type(argt) || is_geometry_type(argt) || is_json_type(argt)) return;
    }
  }
  if (kJsonGroupAgg.count(name)) {
    for (auto *argt : r.argtypes) {
      if (argt && argt->name == std::string("geometry")) return; // 禁止空间入参
    }
  }
  schema::register_aggregate(r);
}

schema_mysql::schema_mysql(const std::string &conninfo)
  : mysql_connection(conninfo)
{
  // 方言初始化即刻开启 mysql_mode，确保后续 grammar/factory 能看到
  mysql_mode = true;
  cerr << "MySQL client version: " << mysql_get_client_info() << endl;

  {
    q("select version();");
    MYSQL_RES *result = mysql_store_result(con);
    if (!result)
      error();
    int num_fields = mysql_num_fields(result);
    MYSQL_ROW row = mysql_fetch_row(result);

    if (num_fields != 1 || !row)
      throw runtime_error("unexpected version() result");
    version = row[0];
    mysql_free_result(result);

    cerr << "MySQL server version: " << version << endl;

  }

  {
    cerr << "Loading tables...";
    q("select table_name, table_schema, table_type"
      " from information_schema.tables where table_schema = '"+db_name+"';");

    MYSQL_RES *result = mysql_store_result(con);
    MYSQL_ROW row;
    string basetab("BASE TABLE");
  
    while ((row = mysql_fetch_row(result))) {
      if (!row[0] || !row[1] || !row[2])
        throw std::runtime_error("broken schema");

      table tab(row[0], row[1],
                false /* insertable */,
                basetab == row[2] /* base_table */);

      tables.push_back(tab);
    }
    mysql_free_result(result);
    cerr << "done." << endl;
  }

  {
    cerr << "Loading columns...";

    for (auto t = tables.begin(); t != tables.end(); ++t) {
      string qs("select column_name, data_type, column_type"
                " from information_schema.columns where"
                " table_catalog = 'def'");
      qs += " and table_schema = '" + t->schema + "'";
      qs += " and table_name = '" + t->name + "'";
      qs += ";";

      q(qs);

      MYSQL_RES *result = mysql_store_result(con);
      MYSQL_ROW row;
  
      while ((row = mysql_fetch_row(result))) {
        if (!row[0] || !row[1])
          throw std::runtime_error("broken schema");

        column col(row[0], sqltype::get(row[1]));
        types.push_back(sqltype::get(row[1]));
        t->columns().push_back(col);
        // 从列定义中解析 SRID（只读元数据，不影响生成层 SRID=4326）
        if (row[2]) {
          std::string ctype = row[2];
          std::smatch m;
          std::regex r("SRID\\s*([0-9]+)");
          if (std::regex_search(ctype, m, r)) {
            int srid = stoi(m[1]);
            std::string key = t->schema + "." + t->name + "." + col.name;
            column_srid[key] = srid;
          }
        }
      }
      mysql_free_result(result);

    }
    cerr << "done." << endl;
  }

  for (auto t : sqltype::typemap) {
    cerr << "type found: " << t.second->name << endl;
  }

#define BINOP(n,t) do {op o(#n,sqltype::get(#t),sqltype::get(#t),sqltype::get(#t)); register_operator(o); } while(0)

  BINOP(||, varchar);
  BINOP(*, int);
  BINOP(/, int);

  BINOP(+, int);
  BINOP(-, int);

  BINOP(>>, int);
  BINOP(<<, int);

  BINOP(&, int);
  BINOP(|, int);
  BINOP(^, int);
  BINOP(%, int);

  BINOP(<, int);
  BINOP(<=, int);
  BINOP(>, int);
  BINOP(>=, int);

  BINOP(=, int);
  BINOP(!=, int);
  BINOP(<>, int);
  BINOP(IS, int);
  BINOP(IS NOT, int);
  BINOP(!, int);

  BINOP(AND, int);
  BINOP(OR, int);
  BINOP(XOR, int);
  BINOP(&&, int);

  BINOP(*, varchar);
  BINOP(/, varchar);

  BINOP(+, varchar);
  BINOP(-, varchar);

  BINOP(>>, varchar);
  BINOP(<<, varchar);

  BINOP(&, varchar);
  BINOP(|, varchar);
  BINOP(^, varchar);
  BINOP(%, varchar);

  BINOP(<, varchar);
  BINOP(<=, varchar);
  BINOP(>, varchar);
  BINOP(>=, varchar);

  BINOP(=, varchar);
  BINOP(!=, varchar);
  BINOP(<>, varchar);
  BINOP(IS, varchar);
  BINOP(IS NOT, varchar);
  BINOP(!, varchar);

  BINOP(AND, varchar);
  BINOP(OR, varchar);
  BINOP(XOR, varchar);
  BINOP(&&, varchar);

  BINOP(*, float);
  BINOP(/, float);

  BINOP(+, float);
  BINOP(-, float);

  BINOP(>>, float);
  BINOP(<<, float);

  BINOP(&, float);
  BINOP(|, float);
  BINOP(^, float);
  BINOP(%, float);

  BINOP(<, float);
  BINOP(<=, float);
  BINOP(>, float);
  BINOP(>=, float);

  BINOP(=, float);
  BINOP(!=, float);
  BINOP(<>, float);
  BINOP(IS, float);
  BINOP(IS NOT, float);
  BINOP(!, float);

  BINOP(AND, float);
  BINOP(OR, float);
  BINOP(XOR, float);
  BINOP(&&, float);

  BINOP(*, datetime);
  BINOP(/, datetime);

  BINOP(+, datetime);
  BINOP(-, datetime);

  BINOP(>>, datetime);
  BINOP(<<, datetime);

  BINOP(&, datetime);
  BINOP(|, datetime);
  BINOP(^, datetime);
  BINOP(%, datetime);

  BINOP(<, datetime);
  BINOP(<=, datetime);
  BINOP(>, datetime);
  BINOP(>=, datetime);

  BINOP(=, datetime);
  BINOP(!=, datetime);
  BINOP(<>, datetime);
  BINOP(IS, datetime);
  BINOP(IS NOT, datetime);
  BINOP(!, datetime);

  BINOP(AND, datetime);
  BINOP(OR, datetime);
  BINOP(XOR, datetime);
  BINOP(&&, datetime);

#define FUNC(n,r) do {                                                  \
    routine proc("", "", sqltype::get(#r), #n);                         \
    register_routine(proc);                                             \
  } while(0)

#define FUNC1(n,r,a) do {                                               \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    register_routine(proc);                                             \
  } while(0)

#define FUNC2(n,r,a,b) do {                                             \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    proc.argtypes.push_back(sqltype::get(#b));                          \
    register_routine(proc);                                             \
  } while(0)

#define FUNC3(n,r,a,b,c) do {                                           \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    proc.argtypes.push_back(sqltype::get(#b));                          \
    proc.argtypes.push_back(sqltype::get(#c));                          \
    register_routine(proc);                                             \
  } while(0)

#define FUNC4(n,r,a,b,c,d) do {                                           \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    proc.argtypes.push_back(sqltype::get(#b));                          \
    proc.argtypes.push_back(sqltype::get(#c));                          \
    proc.argtypes.push_back(sqltype::get(#d));                          \
    register_routine(proc);                                             \
  } while(0)

  //零元函数
  FUNC(UUID, varchar);
  FUNC(UUID_SHORT, varchar);
  FUNC(PS_CURRENT_THREAD_ID, varchar);
  FUNC(CONNECTION_ID, varchar);
  FUNC(CURRENT_ROLE, varchar);
  FUNC(USER, varchar);
  FUNC(CURRENT_USER, varchar);
  FUNC(DATABASE, varchar);
  FUNC(FOUND_ROWS, varchar);
  FUNC(ICU_VERSION, varchar);
  FUNC(LAST_INSERT_ID, varchar);
  FUNC(ROLES_GRAPHML, varchar);
  FUNC(ROW_COUNT, varchar);
  FUNC(SESSION_USER, varchar);
  FUNC(SYSTEM_USER, varchar);
  FUNC(SCHEMA, varchar);
  FUNC(VERSION, varchar);
  FUNC(RELEASE_ALL_LOCKS, varchar);
  FUNC(CURDATE, date);
  FUNC(CURRENT_DATE, date);
  FUNC(CURRENT_TIME, time);
  FUNC(CURRENT_TIMESTAMP, datetime);
  FUNC(CURTIME, time);
  FUNC(LOCALTIME, datetime);
  FUNC(NOW, datetime);
  FUNC(SYSDATE, datetime);
  FUNC(UNIX_TIMESTAMP, varchar);
  FUNC(UTC_DATE, date);
  FUNC(UTC_TIME, time);
  FUNC(UTC_TIMESTAMP, datetime);
  FUNC(PI, varchar);  
  FUNC(RAND, varchar);  

  //一元函数
  FUNC1(IS_UUID, int, int);
  FUNC1(IS_UUID, int, float);
  FUNC1(IS_UUID, int, varchar);
  FUNC1(IS_UUID, int, datetime);
  FUNC1(IS_UUID, varchar, int);
  FUNC1(IS_UUID, varchar, float);
  FUNC1(IS_UUID, varchar, varchar);
  FUNC1(IS_UUID, varchar, datetime);
  FUNC1(FORMAT_PICO_TIME, int, int);
  FUNC1(FORMAT_PICO_TIME, int, float);
  FUNC1(FORMAT_PICO_TIME, int, varchar);
  FUNC1(FORMAT_PICO_TIME, int, datetime);
  FUNC1(FORMAT_PICO_TIME, varchar, int);
  FUNC1(FORMAT_PICO_TIME, varchar, float);
  FUNC1(FORMAT_PICO_TIME, varchar, varchar);
  FUNC1(FORMAT_PICO_TIME, varchar, datetime);
  FUNC1(CHARSET, int, int);
  FUNC1(CHARSET, int, float);
  FUNC1(CHARSET, int, varchar);
  FUNC1(CHARSET, int, datetime);
  FUNC1(CHARSET, varchar, int);
  FUNC1(CHARSET, varchar, float);
  FUNC1(CHARSET, varchar, varchar);
  FUNC1(CHARSET, varchar, datetime);
  FUNC1(COERCIBILITY, int, int);
  FUNC1(COERCIBILITY, int, float);
  FUNC1(COERCIBILITY, int, varchar);
  FUNC1(COERCIBILITY, int, datetime);
  FUNC1(COERCIBILITY, varchar, int);
  FUNC1(COERCIBILITY, varchar, float);
  FUNC1(COERCIBILITY, varchar, varchar);
  FUNC1(COERCIBILITY, varchar, datetime);
  FUNC1(COLLATION, int, int);
  FUNC1(COLLATION, int, float);
  FUNC1(COLLATION, int, varchar);
  FUNC1(COLLATION, int, datetime);
  FUNC1(COLLATION, varchar, int);
  FUNC1(COLLATION, varchar, float);
  FUNC1(COLLATION, varchar, varchar);
  FUNC1(COLLATION, varchar, datetime);
  FUNC1(RELEASE_LOCK, int, int);
  FUNC1(RELEASE_LOCK, int, float);
  FUNC1(RELEASE_LOCK, int, varchar);
  FUNC1(RELEASE_LOCK, int, datetime);
  FUNC1(RELEASE_LOCK, varchar, int);
  FUNC1(RELEASE_LOCK, varchar, float);
  FUNC1(RELEASE_LOCK, varchar, varchar);
  FUNC1(RELEASE_LOCK, varchar, datetime);
  FUNC1(IS_USED_LOCK, int, int);
  FUNC1(IS_USED_LOCK, int, float);
  FUNC1(IS_USED_LOCK, int, varchar);
  FUNC1(IS_USED_LOCK, int, datetime);
  FUNC1(IS_USED_LOCK, varchar, int);
  FUNC1(IS_USED_LOCK, varchar, float);
  FUNC1(IS_USED_LOCK, varchar, varchar);
  FUNC1(IS_USED_LOCK, varchar, datetime);
  FUNC1(IS_FREE_LOCK, int, int);
  FUNC1(IS_FREE_LOCK, int, float);
  FUNC1(IS_FREE_LOCK, int, varchar);
  FUNC1(IS_FREE_LOCK, int, datetime);
  FUNC1(IS_FREE_LOCK, varchar, int);
  FUNC1(IS_FREE_LOCK, varchar, float);
  FUNC1(IS_FREE_LOCK, varchar, varchar);
  FUNC1(IS_FREE_LOCK, varchar, datetime);
  FUNC1(MD5, int, int);
  FUNC1(MD5, int, float);
  FUNC1(MD5, int, varchar);
  FUNC1(MD5, int, datetime);
  FUNC1(MD5, varchar, int);
  FUNC1(MD5, varchar, float);
  FUNC1(MD5, varchar, varchar);
  FUNC1(MD5, varchar, datetime);
  FUNC1(COMPRESS, int, int);
  FUNC1(COMPRESS, int, float);
  FUNC1(COMPRESS, int, varchar);
  FUNC1(COMPRESS, int, datetime);
  FUNC1(COMPRESS, varchar, int);
  FUNC1(COMPRESS, varchar, float);
  FUNC1(COMPRESS, varchar, varchar);
  FUNC1(COMPRESS, varchar, datetime);
  FUNC1(RANDOM_BYTES, int, int);
  FUNC1(RANDOM_BYTES, int, float);
  FUNC1(RANDOM_BYTES, int, varchar);
  FUNC1(RANDOM_BYTES, int, datetime);
  FUNC1(RANDOM_BYTES, varchar, int);
  FUNC1(RANDOM_BYTES, varchar, float);
  FUNC1(RANDOM_BYTES, varchar, varchar);
  FUNC1(RANDOM_BYTES, varchar, datetime);
  FUNC1(JSON_VALID, int, int);
  FUNC1(JSON_VALID, int, float);
  FUNC1(JSON_VALID, int, varchar);
  FUNC1(JSON_VALID, int, datetime);
  FUNC1(JSON_VALID, varchar, int);
  FUNC1(JSON_VALID, varchar, float);
  FUNC1(JSON_VALID, varchar, varchar);
  FUNC1(JSON_VALID, varchar, datetime);
  FUNC1(JSON_ARRAY, int, int);
  FUNC1(JSON_ARRAY, int, float);
  FUNC1(JSON_ARRAY, int, varchar);
  FUNC1(JSON_ARRAY, int, datetime);
  FUNC1(JSON_ARRAY, varchar, int);
  FUNC1(JSON_ARRAY, varchar, float);
  FUNC1(JSON_ARRAY, varchar, varchar);
  FUNC1(JSON_ARRAY, varchar, datetime);
  FUNC1(LAST_INSERT_ID, int, int);
  FUNC1(LAST_INSERT_ID, int, float);
  FUNC1(LAST_INSERT_ID, int, varchar);
  FUNC1(LAST_INSERT_ID, int, datetime);
  FUNC1(LAST_INSERT_ID, varchar, int);
  FUNC1(LAST_INSERT_ID, varchar, float);
  FUNC1(LAST_INSERT_ID, varchar, varchar);
  FUNC1(LAST_INSERT_ID, varchar, datetime);
  FUNC1(BIT_COUNT, int, int);
  FUNC1(BIT_COUNT, int, float);
  FUNC1(BIT_COUNT, int, varchar);
  FUNC1(BIT_COUNT, int, datetime);
  FUNC1(BIT_COUNT, varchar, int);
  FUNC1(BIT_COUNT, varchar, float);
  FUNC1(BIT_COUNT, varchar, varchar);
  FUNC1(BIT_COUNT, varchar, datetime);
  FUNC1(UCASE, int, int);
  FUNC1(UCASE, int, float);
  FUNC1(UCASE, int, varchar);
  FUNC1(UCASE, int, datetime);
  FUNC1(UCASE, varchar, int);
  FUNC1(UCASE, varchar, float);
  FUNC1(UCASE, varchar, varchar);
  FUNC1(UCASE, varchar, datetime);
  FUNC1(UNHEX, int, int);
  FUNC1(UNHEX, int, float);
  FUNC1(UNHEX, int, varchar);
  FUNC1(UNHEX, int, datetime);
  FUNC1(UNHEX, varchar, int);
  FUNC1(UNHEX, varchar, float);
  FUNC1(UNHEX, varchar, varchar);
  FUNC1(UNHEX, varchar, datetime);
  FUNC1(UPPER, int, int);
  FUNC1(UPPER, int, float);
  FUNC1(UPPER, int, varchar);
  FUNC1(UPPER, int, datetime);
  FUNC1(UPPER, varchar, int);
  FUNC1(UPPER, varchar, float);
  FUNC1(UPPER, varchar, varchar);
  FUNC1(UPPER, varchar, datetime);
  FUNC1(WEIGHT_STRING, int, int);
  FUNC1(WEIGHT_STRING, int, float);
  FUNC1(WEIGHT_STRING, int, varchar);
  FUNC1(WEIGHT_STRING, int, datetime);
  FUNC1(WEIGHT_STRING, varchar, int);
  FUNC1(WEIGHT_STRING, varchar, float);
  FUNC1(WEIGHT_STRING, varchar, varchar);
  FUNC1(WEIGHT_STRING, varchar, datetime);
  FUNC1(TO_BASE64, int, int);
  FUNC1(TO_BASE64, int, float);
  FUNC1(TO_BASE64, int, varchar);
  FUNC1(TO_BASE64, int, datetime);
  FUNC1(TO_BASE64, varchar, int);
  FUNC1(TO_BASE64, varchar, float);
  FUNC1(TO_BASE64, varchar, varchar);
  FUNC1(TO_BASE64, varchar, datetime);
  FUNC1(TRIM, int, int);
  FUNC1(TRIM, int, float);
  FUNC1(TRIM, int, varchar);
  FUNC1(TRIM, int, datetime);
  FUNC1(TRIM, varchar, int);
  FUNC1(TRIM, varchar, float);
  FUNC1(TRIM, varchar, varchar);
  FUNC1(TRIM, varchar, datetime);
  FUNC1(SPACE, int, int);
  FUNC1(SPACE, int, float);
  FUNC1(SPACE, int, varchar);
  FUNC1(SPACE, int, datetime);
  FUNC1(SPACE, varchar, int);
  FUNC1(SPACE, varchar, float);
  FUNC1(SPACE, varchar, varchar);
  FUNC1(SPACE, varchar, datetime);
  FUNC1(RTRIM, int, int);
  FUNC1(RTRIM, int, float);
  FUNC1(RTRIM, int, varchar);
  FUNC1(RTRIM, int, datetime);
  FUNC1(RTRIM, varchar, int);
  FUNC1(RTRIM, varchar, float);
  FUNC1(RTRIM, varchar, varchar);
  FUNC1(RTRIM, varchar, datetime);
  FUNC1(SOUNDEX, int, int);
  FUNC1(SOUNDEX, int, float);
  FUNC1(SOUNDEX, int, varchar);
  FUNC1(SOUNDEX, int, datetime);
  FUNC1(SOUNDEX, varchar, int);
  FUNC1(SOUNDEX, varchar, float);
  FUNC1(SOUNDEX, varchar, varchar);
  FUNC1(SOUNDEX, varchar, datetime);
  FUNC1(REVERSE, int, int);
  FUNC1(REVERSE, int, float);
  FUNC1(REVERSE, int, varchar);
  FUNC1(REVERSE, int, datetime);
  FUNC1(REVERSE, varchar, int);
  FUNC1(REVERSE, varchar, float);
  FUNC1(REVERSE, varchar, varchar);
  FUNC1(REVERSE, varchar, datetime);
  FUNC1(QUOTE, int, int);
  FUNC1(QUOTE, int, float);
  FUNC1(QUOTE, int, varchar);
  FUNC1(QUOTE, int, datetime);
  FUNC1(QUOTE, varchar, int);
  FUNC1(QUOTE, varchar, float);
  FUNC1(QUOTE, varchar, varchar);
  FUNC1(QUOTE, varchar, datetime);
  FUNC1(OCT, int, int);
  FUNC1(OCT, int, float);
  FUNC1(OCT, int, varchar);
  FUNC1(OCT, int, datetime);
  FUNC1(OCT, varchar, int);
  FUNC1(OCT, varchar, float);
  FUNC1(OCT, varchar, varchar);
  FUNC1(OCT, varchar, datetime);
  FUNC1(OCTET_LENGTH, int, int);
  FUNC1(OCTET_LENGTH, int, float);
  FUNC1(OCTET_LENGTH, int, varchar);
  FUNC1(OCTET_LENGTH, int, datetime);
  FUNC1(OCTET_LENGTH, varchar, int);
  FUNC1(OCTET_LENGTH, varchar, float);
  FUNC1(OCTET_LENGTH, varchar, varchar);
  FUNC1(OCTET_LENGTH, varchar, datetime);
  FUNC1(ORD, int, int);
  FUNC1(ORD, int, float);
  FUNC1(ORD, int, varchar);
  FUNC1(ORD, int, datetime);
  FUNC1(ORD, varchar, int);
  FUNC1(ORD, varchar, float);
  FUNC1(ORD, varchar, varchar);
  FUNC1(ORD, varchar, datetime);
  FUNC1(LOWER, int, int);
  FUNC1(LOWER, int, float);
  FUNC1(LOWER, int, varchar);
  FUNC1(LOWER, int, datetime);
  FUNC1(LOWER, varchar, int);
  FUNC1(LOWER, varchar, float);
  FUNC1(LOWER, varchar, varchar);
  FUNC1(LOWER, varchar, datetime);
  FUNC1(LTRIM, int, int);
  FUNC1(LTRIM, int, float);
  FUNC1(LTRIM, int, varchar);
  FUNC1(LTRIM, int, datetime);
  FUNC1(LTRIM, varchar, int);
  FUNC1(LTRIM, varchar, float);
  FUNC1(LTRIM, varchar, varchar);
  FUNC1(LTRIM, varchar, datetime);
  FUNC1(LCASE, int, int);
  FUNC1(LCASE, int, float);
  FUNC1(LCASE, int, varchar);
  FUNC1(LCASE, int, datetime);
  FUNC1(LCASE, varchar, int);
  FUNC1(LCASE, varchar, float);
  FUNC1(LCASE, varchar, varchar);
  FUNC1(LCASE, varchar, datetime);
  FUNC1(LENGTH, int, int);
  FUNC1(LENGTH, int, float);
  FUNC1(LENGTH, int, varchar);
  FUNC1(LENGTH, int, datetime);
  FUNC1(LENGTH, varchar, int);
  FUNC1(LENGTH, varchar, float);
  FUNC1(LENGTH, varchar, varchar);
  FUNC1(LENGTH, varchar, datetime);
  FUNC1(FROM_BASE64, int, int);
  FUNC1(FROM_BASE64, int, float);
  FUNC1(FROM_BASE64, int, varchar);
  FUNC1(FROM_BASE64, int, datetime);
  FUNC1(FROM_BASE64, varchar, int);
  FUNC1(FROM_BASE64, varchar, float);
  FUNC1(FROM_BASE64, varchar, varchar);
  FUNC1(FROM_BASE64, varchar, datetime);
  FUNC1(HEX, int, int);
  FUNC1(HEX, int, float);
  FUNC1(HEX, int, varchar);
  FUNC1(HEX, int, datetime);
  FUNC1(HEX, varchar, int);
  FUNC1(HEX, varchar, float);
  FUNC1(HEX, varchar, varchar);
  FUNC1(HEX, varchar, datetime);
  FUNC1(ASCII, int, int);
  FUNC1(ASCII, int, float);
  FUNC1(ASCII, int, varchar);
  FUNC1(ASCII, int, datetime);
  FUNC1(ASCII, varchar, int);
  FUNC1(ASCII, varchar, float);
  FUNC1(ASCII, varchar, varchar);
  FUNC1(ASCII, varchar, datetime);
  FUNC1(BIN, int, int);
  FUNC1(BIN, int, float);
  FUNC1(BIN, int, varchar);
  FUNC1(BIN, int, datetime);
  FUNC1(BIN, varchar, int);
  FUNC1(BIN, varchar, float);
  FUNC1(BIN, varchar, varchar);
  FUNC1(BIN, varchar, datetime);
  FUNC1(BIT_LENGTH, int, int);
  FUNC1(BIT_LENGTH, int, float);
  FUNC1(BIT_LENGTH, int, varchar);
  FUNC1(BIT_LENGTH, int, datetime);
  FUNC1(BIT_LENGTH, varchar, int);
  FUNC1(BIT_LENGTH, varchar, float);
  FUNC1(BIT_LENGTH, varchar, varchar);
  FUNC1(BIT_LENGTH, varchar, datetime);
  FUNC1(CHAR, int, int);
  FUNC1(CHAR, int, float);
  FUNC1(CHAR, int, varchar);
  FUNC1(CHAR, int, datetime);
  FUNC1(CHAR, varchar, int);
  FUNC1(CHAR, varchar, float);
  FUNC1(CHAR, varchar, varchar);
  FUNC1(CHAR, varchar, datetime);
  FUNC1(CHAR_LENGTH, int, int);
  FUNC1(CHAR_LENGTH, int, float);
  FUNC1(CHAR_LENGTH, int, varchar);
  FUNC1(CHAR_LENGTH, int, datetime);
  FUNC1(CHAR_LENGTH, varchar, int);
  FUNC1(CHAR_LENGTH, varchar, float);
  FUNC1(CHAR_LENGTH, varchar, varchar);
  FUNC1(CHAR_LENGTH, varchar, datetime);
  FUNC1(CHARACTER_LENGTH, int, int);
  FUNC1(CHARACTER_LENGTH, int, float);
  FUNC1(CHARACTER_LENGTH, int, varchar);
  FUNC1(CHARACTER_LENGTH, int, datetime);
  FUNC1(CHARACTER_LENGTH, varchar, int);
  FUNC1(CHARACTER_LENGTH, varchar, float);
  FUNC1(CHARACTER_LENGTH, varchar, varchar);
  FUNC1(CHARACTER_LENGTH, varchar, datetime);
  FUNC1(ASCII, int, int);
  FUNC1(ASCII, int, float);
  FUNC1(ASCII, int, varchar);
  FUNC1(ASCII, int, datetime);
  FUNC1(ASCII, varchar, int);
  FUNC1(ASCII, varchar, float);
  FUNC1(ASCII, varchar, varchar);
  FUNC1(ASCII, varchar, datetime);
  FUNC1(BIN, int, int);
  FUNC1(BIN, int, float);
  FUNC1(BIN, int, varchar);
  FUNC1(BIN, int, datetime);
  FUNC1(BIN, varchar, int);
  FUNC1(BIN, varchar, float);
  FUNC1(BIN, varchar, varchar);
  FUNC1(BIN, varchar, datetime);
  FUNC1(BIT_LENGTH, int, int);
  FUNC1(BIT_LENGTH, int, float);
  FUNC1(BIT_LENGTH, int, varchar);
  FUNC1(BIT_LENGTH, int, datetime);
  FUNC1(BIT_LENGTH, varchar, int);
  FUNC1(BIT_LENGTH, varchar, float);
  FUNC1(BIT_LENGTH, varchar, varchar);
  FUNC1(BIT_LENGTH, varchar, datetime);

  //二元函数
    FUNC2(GET_LOCK, int, int, int);
  FUNC2(GET_LOCK, int, int, float);
  FUNC2(GET_LOCK, int, int, varchar);
  FUNC2(GET_LOCK, int, int, datetime);
  FUNC2(GET_LOCK, int, float, int);
  FUNC2(GET_LOCK, int, float, float);
  FUNC2(GET_LOCK, int, float, varchar);
  FUNC2(GET_LOCK, int, float, datetime);
  FUNC2(GET_LOCK, int, varchar, int);
  FUNC2(GET_LOCK, int, varchar, float);
  FUNC2(GET_LOCK, int, varchar, varchar);
  FUNC2(GET_LOCK, int, varchar, datetime);
  FUNC2(GET_LOCK, int, datetime, int);
  FUNC2(GET_LOCK, int, datetime, float);
  FUNC2(GET_LOCK, int, datetime, varchar);
  FUNC2(GET_LOCK, int, datetime, datetime);
  FUNC2(GET_LOCK, varchar, int, int);
  FUNC2(GET_LOCK, varchar, int, float);
  FUNC2(GET_LOCK, varchar, int, varchar);
  FUNC2(GET_LOCK, varchar, int, datetime);
  FUNC2(GET_LOCK, varchar, float, int);
  FUNC2(GET_LOCK, varchar, float, float);
  FUNC2(GET_LOCK, varchar, float, varchar);
  FUNC2(GET_LOCK, varchar, float, datetime);
  FUNC2(GET_LOCK, varchar, varchar, int);
  FUNC2(GET_LOCK, varchar, varchar, float);
  FUNC2(GET_LOCK, varchar, varchar, varchar);
  FUNC2(GET_LOCK, varchar, varchar, datetime);
  FUNC2(GET_LOCK, varchar, datetime, int);
  FUNC2(GET_LOCK, varchar, datetime, float);
  FUNC2(GET_LOCK, varchar, datetime, varchar);
  FUNC2(GET_LOCK, varchar, datetime, datetime);
  FUNC2(AES_ENCRYPT, int, int, int);
  FUNC2(AES_ENCRYPT, int, int, float);
  FUNC2(AES_ENCRYPT, int, int, varchar);
  FUNC2(AES_ENCRYPT, int, int, datetime);
  FUNC2(AES_ENCRYPT, int, float, int);
  FUNC2(AES_ENCRYPT, int, float, float);
  FUNC2(AES_ENCRYPT, int, float, varchar);
  FUNC2(AES_ENCRYPT, int, float, datetime);
  FUNC2(AES_ENCRYPT, int, varchar, int);
  FUNC2(AES_ENCRYPT, int, varchar, float);
  FUNC2(AES_ENCRYPT, int, varchar, varchar);
  FUNC2(AES_ENCRYPT, int, varchar, datetime);
  FUNC2(AES_ENCRYPT, int, datetime, int);
  FUNC2(AES_ENCRYPT, int, datetime, float);
  FUNC2(AES_ENCRYPT, int, datetime, varchar);
  FUNC2(AES_ENCRYPT, int, datetime, datetime);
  FUNC2(AES_ENCRYPT, varchar, int, int);
  FUNC2(AES_ENCRYPT, varchar, int, float);
  FUNC2(AES_ENCRYPT, varchar, int, varchar);
  FUNC2(AES_ENCRYPT, varchar, int, datetime);
  FUNC2(AES_ENCRYPT, varchar, float, int);
  FUNC2(AES_ENCRYPT, varchar, float, float);
  FUNC2(AES_ENCRYPT, varchar, float, varchar);
  FUNC2(AES_ENCRYPT, varchar, float, datetime);
  FUNC2(AES_ENCRYPT, varchar, varchar, int);
  FUNC2(AES_ENCRYPT, varchar, varchar, float);
  FUNC2(AES_ENCRYPT, varchar, varchar, varchar);
  FUNC2(AES_ENCRYPT, varchar, varchar, datetime);
  FUNC2(AES_ENCRYPT, varchar, datetime, int);
  FUNC2(AES_ENCRYPT, varchar, datetime, float);
  FUNC2(AES_ENCRYPT, varchar, datetime, varchar);
  FUNC2(AES_ENCRYPT, varchar, datetime, datetime);
  FUNC2(AES_DECRYPT, int, int, int);
  FUNC2(AES_DECRYPT, int, int, float);
  FUNC2(AES_DECRYPT, int, int, varchar);
  FUNC2(AES_DECRYPT, int, int, datetime);
  FUNC2(AES_DECRYPT, int, float, int);
  FUNC2(AES_DECRYPT, int, float, float);
  FUNC2(AES_DECRYPT, int, float, varchar);
  FUNC2(AES_DECRYPT, int, float, datetime);
  FUNC2(AES_DECRYPT, int, varchar, int);
  FUNC2(AES_DECRYPT, int, varchar, float);
  FUNC2(AES_DECRYPT, int, varchar, varchar);
  FUNC2(AES_DECRYPT, int, varchar, datetime);
  FUNC2(AES_DECRYPT, int, datetime, int);
  FUNC2(AES_DECRYPT, int, datetime, float);
  FUNC2(AES_DECRYPT, int, datetime, varchar);
  FUNC2(AES_DECRYPT, int, datetime, datetime);
  FUNC2(AES_DECRYPT, varchar, int, int);
  FUNC2(AES_DECRYPT, varchar, int, float);
  FUNC2(AES_DECRYPT, varchar, int, varchar);
  FUNC2(AES_DECRYPT, varchar, int, datetime);
  FUNC2(AES_DECRYPT, varchar, float, int);
  FUNC2(AES_DECRYPT, varchar, float, float);
  FUNC2(AES_DECRYPT, varchar, float, varchar);
  FUNC2(AES_DECRYPT, varchar, float, datetime);
  FUNC2(AES_DECRYPT, varchar, varchar, int);
  FUNC2(AES_DECRYPT, varchar, varchar, float);
  FUNC2(AES_DECRYPT, varchar, varchar, varchar);
  FUNC2(AES_DECRYPT, varchar, varchar, datetime);
  FUNC2(AES_DECRYPT, varchar, datetime, int);
  FUNC2(AES_DECRYPT, varchar, datetime, float);
  FUNC2(AES_DECRYPT, varchar, datetime, varchar);
  FUNC2(AES_DECRYPT, varchar, datetime, datetime);
  FUNC2(SUBSTRING, int, int, int);
  FUNC2(SUBSTRING, int, int, float);
  FUNC2(SUBSTRING, int, int, varchar);
  FUNC2(SUBSTRING, int, int, datetime);
  FUNC2(SUBSTRING, int, float, int);
  FUNC2(SUBSTRING, int, float, float);
  FUNC2(SUBSTRING, int, float, varchar);
  FUNC2(SUBSTRING, int, float, datetime);
  FUNC2(SUBSTRING, int, varchar, int);
  FUNC2(SUBSTRING, int, varchar, float);
  FUNC2(SUBSTRING, int, varchar, varchar);
  FUNC2(SUBSTRING, int, varchar, datetime);
  FUNC2(SUBSTRING, int, datetime, int);
  FUNC2(SUBSTRING, int, datetime, float);
  FUNC2(SUBSTRING, int, datetime, varchar);
  FUNC2(SUBSTRING, int, datetime, datetime);
  FUNC2(SUBSTRING, varchar, int, int);
  FUNC2(SUBSTRING, varchar, int, float);
  FUNC2(SUBSTRING, varchar, int, varchar);
  FUNC2(SUBSTRING, varchar, int, datetime);
  FUNC2(SUBSTRING, varchar, float, int);
  FUNC2(SUBSTRING, varchar, float, float);
  FUNC2(SUBSTRING, varchar, float, varchar);
  FUNC2(SUBSTRING, varchar, float, datetime);
  FUNC2(SUBSTRING, varchar, varchar, int);
  FUNC2(SUBSTRING, varchar, varchar, float);
  FUNC2(SUBSTRING, varchar, varchar, varchar);
  FUNC2(SUBSTRING, varchar, varchar, datetime);
  FUNC2(SUBSTRING, varchar, datetime, int);
  FUNC2(SUBSTRING, varchar, datetime, float);
  FUNC2(SUBSTRING, varchar, datetime, varchar);
  FUNC2(SUBSTRING, varchar, datetime, datetime);
  FUNC2(SUBSTR, int, int, int);
  FUNC2(SUBSTR, int, int, float);
  FUNC2(SUBSTR, int, int, varchar);
  FUNC2(SUBSTR, int, int, datetime);
  FUNC2(SUBSTR, int, float, int);
  FUNC2(SUBSTR, int, float, float);
  FUNC2(SUBSTR, int, float, varchar);
  FUNC2(SUBSTR, int, float, datetime);
  FUNC2(SUBSTR, int, varchar, int);
  FUNC2(SUBSTR, int, varchar, float);
  FUNC2(SUBSTR, int, varchar, varchar);
  FUNC2(SUBSTR, int, varchar, datetime);
  FUNC2(SUBSTR, int, datetime, int);
  FUNC2(SUBSTR, int, datetime, float);
  FUNC2(SUBSTR, int, datetime, varchar);
  FUNC2(SUBSTR, int, datetime, datetime);
  FUNC2(SUBSTR, varchar, int, int);
  FUNC2(SUBSTR, varchar, int, float);
  FUNC2(SUBSTR, varchar, int, varchar);
  FUNC2(SUBSTR, varchar, int, datetime);
  FUNC2(SUBSTR, varchar, float, int);
  FUNC2(SUBSTR, varchar, float, float);
  FUNC2(SUBSTR, varchar, float, varchar);
  FUNC2(SUBSTR, varchar, float, datetime);
  FUNC2(SUBSTR, varchar, varchar, int);
  FUNC2(SUBSTR, varchar, varchar, float);
  FUNC2(SUBSTR, varchar, varchar, varchar);
  FUNC2(SUBSTR, varchar, varchar, datetime);
  FUNC2(SUBSTR, varchar, datetime, int);
  FUNC2(SUBSTR, varchar, datetime, float);
  FUNC2(SUBSTR, varchar, datetime, varchar);
  FUNC2(SUBSTR, varchar, datetime, datetime);
  FUNC2(STRCMP, int, int, int);
  FUNC2(STRCMP, int, int, float);
  FUNC2(STRCMP, int, int, varchar);
  FUNC2(STRCMP, int, int, datetime);
  FUNC2(STRCMP, int, float, int);
  FUNC2(STRCMP, int, float, float);
  FUNC2(STRCMP, int, float, varchar);
  FUNC2(STRCMP, int, float, datetime);
  FUNC2(STRCMP, int, varchar, int);
  FUNC2(STRCMP, int, varchar, float);
  FUNC2(STRCMP, int, varchar, varchar);
  FUNC2(STRCMP, int, varchar, datetime);
  FUNC2(STRCMP, int, datetime, int);
  FUNC2(STRCMP, int, datetime, float);
  FUNC2(STRCMP, int, datetime, varchar);
  FUNC2(STRCMP, int, datetime, datetime);
  FUNC2(STRCMP, varchar, int, int);
  FUNC2(STRCMP, varchar, int, float);
  FUNC2(STRCMP, varchar, int, varchar);
  FUNC2(STRCMP, varchar, int, datetime);
  FUNC2(STRCMP, varchar, float, int);
  FUNC2(STRCMP, varchar, float, float);
  FUNC2(STRCMP, varchar, float, varchar);
  FUNC2(STRCMP, varchar, float, datetime);
  FUNC2(STRCMP, varchar, varchar, int);
  FUNC2(STRCMP, varchar, varchar, float);
  FUNC2(STRCMP, varchar, varchar, varchar);
  FUNC2(STRCMP, varchar, varchar, datetime);
  FUNC2(STRCMP, varchar, datetime, int);
  FUNC2(STRCMP, varchar, datetime, float);
  FUNC2(STRCMP, varchar, datetime, varchar);
  FUNC2(STRCMP, varchar, datetime, datetime);
  FUNC2(RIGHT, int, int, int);
  FUNC2(RIGHT, int, int, float);
  FUNC2(RIGHT, int, int, varchar);
  FUNC2(RIGHT, int, int, datetime);
  FUNC2(RIGHT, int, float, int);
  FUNC2(RIGHT, int, float, float);
  FUNC2(RIGHT, int, float, varchar);
  FUNC2(RIGHT, int, float, datetime);
  FUNC2(RIGHT, int, varchar, int);
  FUNC2(RIGHT, int, varchar, float);
  FUNC2(RIGHT, int, varchar, varchar);
  FUNC2(RIGHT, int, varchar, datetime);
  FUNC2(RIGHT, int, datetime, int);
  FUNC2(RIGHT, int, datetime, float);
  FUNC2(RIGHT, int, datetime, varchar);
  FUNC2(RIGHT, int, datetime, datetime);
  FUNC2(RIGHT, varchar, int, int);
  FUNC2(RIGHT, varchar, int, float);
  FUNC2(RIGHT, varchar, int, varchar);
  FUNC2(RIGHT, varchar, int, datetime);
  FUNC2(RIGHT, varchar, float, int);
  FUNC2(RIGHT, varchar, float, float);
  FUNC2(RIGHT, varchar, float, varchar);
  FUNC2(RIGHT, varchar, float, datetime);
  FUNC2(RIGHT, varchar, varchar, int);
  FUNC2(RIGHT, varchar, varchar, float);
  FUNC2(RIGHT, varchar, varchar, varchar);
  FUNC2(RIGHT, varchar, varchar, datetime);
  FUNC2(RIGHT, varchar, datetime, int);
  FUNC2(RIGHT, varchar, datetime, float);
  FUNC2(RIGHT, varchar, datetime, varchar);
  FUNC2(RIGHT, varchar, datetime, datetime);
  FUNC2(REPEAT, int, int, int);
  FUNC2(REPEAT, int, int, float);
  FUNC2(REPEAT, int, int, varchar);
  FUNC2(REPEAT, int, int, datetime);
  FUNC2(REPEAT, int, float, int);
  FUNC2(REPEAT, int, float, float);
  FUNC2(REPEAT, int, float, varchar);
  FUNC2(REPEAT, int, float, datetime);
  FUNC2(REPEAT, int, varchar, int);
  FUNC2(REPEAT, int, varchar, float);
  FUNC2(REPEAT, int, varchar, varchar);
  FUNC2(REPEAT, int, varchar, datetime);
  FUNC2(REPEAT, int, datetime, int);
  FUNC2(REPEAT, int, datetime, float);
  FUNC2(REPEAT, int, datetime, varchar);
  FUNC2(REPEAT, int, datetime, datetime);
  FUNC2(REPEAT, varchar, int, int);
  FUNC2(REPEAT, varchar, int, float);
  FUNC2(REPEAT, varchar, int, varchar);
  FUNC2(REPEAT, varchar, int, datetime);
  FUNC2(REPEAT, varchar, float, int);
  FUNC2(REPEAT, varchar, float, float);
  FUNC2(REPEAT, varchar, float, varchar);
  FUNC2(REPEAT, varchar, float, datetime);
  FUNC2(REPEAT, varchar, varchar, int);
  FUNC2(REPEAT, varchar, varchar, float);
  FUNC2(REPEAT, varchar, varchar, varchar);
  FUNC2(REPEAT, varchar, varchar, datetime);
  FUNC2(REPEAT, varchar, datetime, int);
  FUNC2(REPEAT, varchar, datetime, float);
  FUNC2(REPEAT, varchar, datetime, varchar);
  FUNC2(REPEAT, varchar, datetime, datetime);
  FUNC2(REGEXP_INSTR, int, int, int);
  FUNC2(REGEXP_INSTR, int, int, float);
  FUNC2(REGEXP_INSTR, int, int, varchar);
  FUNC2(REGEXP_INSTR, int, int, datetime);
  FUNC2(REGEXP_INSTR, int, float, int);
  FUNC2(REGEXP_INSTR, int, float, float);
  FUNC2(REGEXP_INSTR, int, float, varchar);
  FUNC2(REGEXP_INSTR, int, float, datetime);
  FUNC2(REGEXP_INSTR, int, varchar, int);
  FUNC2(REGEXP_INSTR, int, varchar, float);
  FUNC2(REGEXP_INSTR, int, varchar, varchar);
  FUNC2(REGEXP_INSTR, int, varchar, datetime);
  FUNC2(REGEXP_INSTR, int, datetime, int);
  FUNC2(REGEXP_INSTR, int, datetime, float);
  FUNC2(REGEXP_INSTR, int, datetime, varchar);
  FUNC2(REGEXP_INSTR, int, datetime, datetime);
  FUNC2(REGEXP_INSTR, varchar, int, int);
  FUNC2(REGEXP_INSTR, varchar, int, float);
  FUNC2(REGEXP_INSTR, varchar, int, varchar);
  FUNC2(REGEXP_INSTR, varchar, int, datetime);
  FUNC2(REGEXP_INSTR, varchar, float, int);
  FUNC2(REGEXP_INSTR, varchar, float, float);
  FUNC2(REGEXP_INSTR, varchar, float, varchar);
  FUNC2(REGEXP_INSTR, varchar, float, datetime);
  FUNC2(REGEXP_INSTR, varchar, varchar, int);
  FUNC2(REGEXP_INSTR, varchar, varchar, float);
  FUNC2(REGEXP_INSTR, varchar, varchar, varchar);
  FUNC2(REGEXP_INSTR, varchar, varchar, datetime);
  FUNC2(REGEXP_INSTR, varchar, datetime, int);
  FUNC2(REGEXP_INSTR, varchar, datetime, float);
  FUNC2(REGEXP_INSTR, varchar, datetime, varchar);
  FUNC2(REGEXP_INSTR, varchar, datetime, datetime);
  FUNC2(REGEXP_LIKE, int, int, int);
  FUNC2(REGEXP_LIKE, int, int, float);
  FUNC2(REGEXP_LIKE, int, int, varchar);
  FUNC2(REGEXP_LIKE, int, int, datetime);
  FUNC2(REGEXP_LIKE, int, float, int);
  FUNC2(REGEXP_LIKE, int, float, float);
  FUNC2(REGEXP_LIKE, int, float, varchar);
  FUNC2(REGEXP_LIKE, int, float, datetime);
  FUNC2(REGEXP_LIKE, int, varchar, int);
  FUNC2(REGEXP_LIKE, int, varchar, float);
  FUNC2(REGEXP_LIKE, int, varchar, varchar);
  FUNC2(REGEXP_LIKE, int, varchar, datetime);
  FUNC2(REGEXP_LIKE, int, datetime, int);
  FUNC2(REGEXP_LIKE, int, datetime, float);
  FUNC2(REGEXP_LIKE, int, datetime, varchar);
  FUNC2(REGEXP_LIKE, int, datetime, datetime);
  FUNC2(REGEXP_LIKE, varchar, int, int);
  FUNC2(REGEXP_LIKE, varchar, int, float);
  FUNC2(REGEXP_LIKE, varchar, int, varchar);
  FUNC2(REGEXP_LIKE, varchar, int, datetime);
  FUNC2(REGEXP_LIKE, varchar, float, int);
  FUNC2(REGEXP_LIKE, varchar, float, float);
  FUNC2(REGEXP_LIKE, varchar, float, varchar);
  FUNC2(REGEXP_LIKE, varchar, float, datetime);
  FUNC2(REGEXP_LIKE, varchar, varchar, int);
  FUNC2(REGEXP_LIKE, varchar, varchar, float);
  FUNC2(REGEXP_LIKE, varchar, varchar, varchar);
  FUNC2(REGEXP_LIKE, varchar, varchar, datetime);
  FUNC2(REGEXP_LIKE, varchar, datetime, int);
  FUNC2(REGEXP_LIKE, varchar, datetime, float);
  FUNC2(REGEXP_LIKE, varchar, datetime, varchar);
  FUNC2(REGEXP_LIKE, varchar, datetime, datetime);
  FUNC3(REGEXP_REPLACE, int, int, int, varchar);
  FUNC3(REGEXP_REPLACE, int, int, float, varchar);
  FUNC3(REGEXP_REPLACE, int, int, varchar, varchar);
  FUNC3(REGEXP_REPLACE, int, int, datetime, varchar);
  FUNC3(REGEXP_REPLACE, int, float, int, varchar);
  FUNC3(REGEXP_REPLACE, int, float, float, varchar);
  FUNC3(REGEXP_REPLACE, int, float, varchar, varchar);
  FUNC3(REGEXP_REPLACE, int, float, datetime, varchar);
  FUNC3(REGEXP_REPLACE, int, varchar, int, varchar);
  FUNC3(REGEXP_REPLACE, int, varchar, float, varchar);
  FUNC3(REGEXP_REPLACE, int, varchar, varchar, varchar);
  FUNC3(REGEXP_REPLACE, int, varchar, datetime, varchar);
  FUNC3(REGEXP_REPLACE, int, datetime, int, varchar);
  FUNC3(REGEXP_REPLACE, int, datetime, float, varchar);
  FUNC3(REGEXP_REPLACE, int, datetime, varchar, varchar);
  FUNC3(REGEXP_REPLACE, int, datetime, datetime, varchar);
  FUNC3(REGEXP_REPLACE, varchar, int, int, varchar);
  FUNC3(REGEXP_REPLACE, varchar, int, float, varchar);
  FUNC3(REGEXP_REPLACE, varchar, int, varchar, varchar);
  FUNC3(REGEXP_REPLACE, varchar, int, datetime, varchar);
  FUNC3(REGEXP_REPLACE, varchar, float, int, varchar);
  FUNC3(REGEXP_REPLACE, varchar, float, float, varchar);
  FUNC3(REGEXP_REPLACE, varchar, float, varchar, varchar);
  FUNC3(REGEXP_REPLACE, varchar, float, datetime, varchar);
  FUNC3(REGEXP_REPLACE, varchar, varchar, int, varchar);
  FUNC3(REGEXP_REPLACE, varchar, varchar, float, varchar);
  FUNC3(REGEXP_REPLACE, varchar, varchar, varchar, varchar);
  FUNC3(REGEXP_REPLACE, varchar, varchar, datetime, varchar);
  FUNC3(REGEXP_REPLACE, varchar, datetime, int, varchar);
  FUNC3(REGEXP_REPLACE, varchar, datetime, float, varchar);
  FUNC3(REGEXP_REPLACE, varchar, datetime, varchar, varchar);
  FUNC3(REGEXP_REPLACE, varchar, datetime, datetime, varchar);
  FUNC2(REGEXP_SUBSTR, int, int, int);
  FUNC2(REGEXP_SUBSTR, int, int, float);
  FUNC2(REGEXP_SUBSTR, int, int, varchar);
  FUNC2(REGEXP_SUBSTR, int, int, datetime);
  FUNC2(REGEXP_SUBSTR, int, float, int);
  FUNC2(REGEXP_SUBSTR, int, float, float);
  FUNC2(REGEXP_SUBSTR, int, float, varchar);
  FUNC2(REGEXP_SUBSTR, int, float, datetime);
  FUNC2(REGEXP_SUBSTR, int, varchar, int);
  FUNC2(REGEXP_SUBSTR, int, varchar, float);
  FUNC2(REGEXP_SUBSTR, int, varchar, varchar);
  FUNC2(REGEXP_SUBSTR, int, varchar, datetime);
  FUNC2(REGEXP_SUBSTR, int, datetime, int);
  FUNC2(REGEXP_SUBSTR, int, datetime, float);
  FUNC2(REGEXP_SUBSTR, int, datetime, varchar);
  FUNC2(REGEXP_SUBSTR, int, datetime, datetime);
  FUNC2(REGEXP_SUBSTR, varchar, int, int);
  FUNC2(REGEXP_SUBSTR, varchar, int, float);
  FUNC2(REGEXP_SUBSTR, varchar, int, varchar);
  FUNC2(REGEXP_SUBSTR, varchar, int, datetime);
  FUNC2(REGEXP_SUBSTR, varchar, float, int);
  FUNC2(REGEXP_SUBSTR, varchar, float, float);
  FUNC2(REGEXP_SUBSTR, varchar, float, varchar);
  FUNC2(REGEXP_SUBSTR, varchar, float, datetime);
  FUNC2(REGEXP_SUBSTR, varchar, varchar, int);
  FUNC2(REGEXP_SUBSTR, varchar, varchar, float);
  FUNC2(REGEXP_SUBSTR, varchar, varchar, varchar);
  FUNC2(REGEXP_SUBSTR, varchar, varchar, datetime);
  FUNC2(REGEXP_SUBSTR, varchar, datetime, int);
  FUNC2(REGEXP_SUBSTR, varchar, datetime, float);
  FUNC2(REGEXP_SUBSTR, varchar, datetime, varchar);
  FUNC2(REGEXP_SUBSTR, varchar, datetime, datetime);
  FUNC2(LOCATE, int, int, int);
  FUNC2(LOCATE, int, int, float);
  FUNC2(LOCATE, int, int, varchar);
  FUNC2(LOCATE, int, int, datetime);
  FUNC2(LOCATE, int, float, int);
  FUNC2(LOCATE, int, float, float);
  FUNC2(LOCATE, int, float, varchar);
  FUNC2(LOCATE, int, float, datetime);
  FUNC2(LOCATE, int, varchar, int);
  FUNC2(LOCATE, int, varchar, float);
  FUNC2(LOCATE, int, varchar, varchar);
  FUNC2(LOCATE, int, varchar, datetime);
  FUNC2(LOCATE, int, datetime, int);
  FUNC2(LOCATE, int, datetime, float);
  FUNC2(LOCATE, int, datetime, varchar);
  FUNC2(LOCATE, int, datetime, datetime);
  FUNC2(LOCATE, varchar, int, int);
  FUNC2(LOCATE, varchar, int, float);
  FUNC2(LOCATE, varchar, int, varchar);
  FUNC2(LOCATE, varchar, int, datetime);
  FUNC2(LOCATE, varchar, float, int);
  FUNC2(LOCATE, varchar, float, float);
  FUNC2(LOCATE, varchar, float, varchar);
  FUNC2(LOCATE, varchar, float, datetime);
  FUNC2(LOCATE, varchar, varchar, int);
  FUNC2(LOCATE, varchar, varchar, float);
  FUNC2(LOCATE, varchar, varchar, varchar);
  FUNC2(LOCATE, varchar, varchar, datetime);
  FUNC2(LOCATE, varchar, datetime, int);
  FUNC2(LOCATE, varchar, datetime, float);
  FUNC2(LOCATE, varchar, datetime, varchar);
  FUNC2(LOCATE, varchar, datetime, datetime);
  FUNC2(INSTR, int, int, int);
  FUNC2(INSTR, int, int, float);
  FUNC2(INSTR, int, int, varchar);
  FUNC2(INSTR, int, int, datetime);
  FUNC2(INSTR, int, float, int);
  FUNC2(INSTR, int, float, float);
  FUNC2(INSTR, int, float, varchar);
  FUNC2(INSTR, int, float, datetime);
  FUNC2(INSTR, int, varchar, int);
  FUNC2(INSTR, int, varchar, float);
  FUNC2(INSTR, int, varchar, varchar);
  FUNC2(INSTR, int, varchar, datetime);
  FUNC2(INSTR, int, datetime, int);
  FUNC2(INSTR, int, datetime, float);
  FUNC2(INSTR, int, datetime, varchar);
  FUNC2(INSTR, int, datetime, datetime);
  FUNC2(INSTR, varchar, int, int);
  FUNC2(INSTR, varchar, int, float);
  FUNC2(INSTR, varchar, int, varchar);
  FUNC2(INSTR, varchar, int, datetime);
  FUNC2(INSTR, varchar, float, int);
  FUNC2(INSTR, varchar, float, float);
  FUNC2(INSTR, varchar, float, varchar);
  FUNC2(INSTR, varchar, float, datetime);
  FUNC2(INSTR, varchar, varchar, int);
  FUNC2(INSTR, varchar, varchar, float);
  FUNC2(INSTR, varchar, varchar, varchar);
  FUNC2(INSTR, varchar, varchar, datetime);
  FUNC2(INSTR, varchar, datetime, int);
  FUNC2(INSTR, varchar, datetime, float);
  FUNC2(INSTR, varchar, datetime, varchar);
  FUNC2(INSTR, varchar, datetime, datetime);
  FUNC2(LEFT, int, int, int);
  FUNC2(LEFT, int, int, float);
  FUNC2(LEFT, int, int, varchar);
  FUNC2(LEFT, int, int, datetime);
  FUNC2(LEFT, int, float, int);
  FUNC2(LEFT, int, float, float);
  FUNC2(LEFT, int, float, varchar);
  FUNC2(LEFT, int, float, datetime);
  FUNC2(LEFT, int, varchar, int);
  FUNC2(LEFT, int, varchar, float);
  FUNC2(LEFT, int, varchar, varchar);
  FUNC2(LEFT, int, varchar, datetime);
  FUNC2(LEFT, int, datetime, int);
  FUNC2(LEFT, int, datetime, float);
  FUNC2(LEFT, int, datetime, varchar);
  FUNC2(LEFT, int, datetime, datetime);
  FUNC2(LEFT, varchar, int, int);
  FUNC2(LEFT, varchar, int, float);
  FUNC2(LEFT, varchar, int, varchar);
  FUNC2(LEFT, varchar, int, datetime);
  FUNC2(LEFT, varchar, float, int);
  FUNC2(LEFT, varchar, float, float);
  FUNC2(LEFT, varchar, float, varchar);
  FUNC2(LEFT, varchar, float, datetime);
  FUNC2(LEFT, varchar, varchar, int);
  FUNC2(LEFT, varchar, varchar, float);
  FUNC2(LEFT, varchar, varchar, varchar);
  FUNC2(LEFT, varchar, varchar, datetime);
  FUNC2(LEFT, varchar, datetime, int);
  FUNC2(LEFT, varchar, datetime, float);
  FUNC2(LEFT, varchar, datetime, varchar);
  FUNC2(LEFT, varchar, datetime, datetime);
  FUNC2(FORMAT, int, int, int);
  FUNC2(FORMAT, int, int, float);
  FUNC2(FORMAT, int, int, varchar);
  FUNC2(FORMAT, int, int, datetime);
  FUNC2(FORMAT, int, float, int);
  FUNC2(FORMAT, int, float, float);
  FUNC2(FORMAT, int, float, varchar);
  FUNC2(FORMAT, int, float, datetime);
  FUNC2(FORMAT, int, varchar, int);
  FUNC2(FORMAT, int, varchar, float);
  FUNC2(FORMAT, int, varchar, varchar);
  FUNC2(FORMAT, int, varchar, datetime);
  FUNC2(FORMAT, int, datetime, int);
  FUNC2(FORMAT, int, datetime, float);
  FUNC2(FORMAT, int, datetime, varchar);
  FUNC2(FORMAT, int, datetime, datetime);
  FUNC2(FORMAT, varchar, int, int);
  FUNC2(FORMAT, varchar, int, float);
  FUNC2(FORMAT, varchar, int, varchar);
  FUNC2(FORMAT, varchar, int, datetime);
  FUNC2(FORMAT, varchar, float, int);
  FUNC2(FORMAT, varchar, float, float);
  FUNC2(FORMAT, varchar, float, varchar);
  FUNC2(FORMAT, varchar, float, datetime);
  FUNC2(FORMAT, varchar, varchar, int);
  FUNC2(FORMAT, varchar, varchar, float);
  FUNC2(FORMAT, varchar, varchar, varchar);
  FUNC2(FORMAT, varchar, varchar, datetime);
  FUNC2(FORMAT, varchar, datetime, int);
  FUNC2(FORMAT, varchar, datetime, float);
  FUNC2(FORMAT, varchar, datetime, varchar);
  FUNC2(FORMAT, varchar, datetime, datetime);
  FUNC2(FIND_IN_SET, int, int, int);
  FUNC2(FIND_IN_SET, int, int, float);
  FUNC2(FIND_IN_SET, int, int, varchar);
  FUNC2(FIND_IN_SET, int, int, datetime);
  FUNC2(FIND_IN_SET, int, float, int);
  FUNC2(FIND_IN_SET, int, float, float);
  FUNC2(FIND_IN_SET, int, float, varchar);
  FUNC2(FIND_IN_SET, int, float, datetime);
  FUNC2(FIND_IN_SET, int, varchar, int);
  FUNC2(FIND_IN_SET, int, varchar, float);
  FUNC2(FIND_IN_SET, int, varchar, varchar);
  FUNC2(FIND_IN_SET, int, varchar, datetime);
  FUNC2(FIND_IN_SET, int, datetime, int);
  FUNC2(FIND_IN_SET, int, datetime, float);
  FUNC2(FIND_IN_SET, int, datetime, varchar);
  FUNC2(FIND_IN_SET, int, datetime, datetime);
  FUNC2(FIND_IN_SET, varchar, int, int);
  FUNC2(FIND_IN_SET, varchar, int, float);
  FUNC2(FIND_IN_SET, varchar, int, varchar);
  FUNC2(FIND_IN_SET, varchar, int, datetime);
  FUNC2(FIND_IN_SET, varchar, float, int);
  FUNC2(FIND_IN_SET, varchar, float, float);
  FUNC2(FIND_IN_SET, varchar, float, varchar);
  FUNC2(FIND_IN_SET, varchar, float, datetime);
  FUNC2(FIND_IN_SET, varchar, varchar, int);
  FUNC2(FIND_IN_SET, varchar, varchar, float);
  FUNC2(FIND_IN_SET, varchar, varchar, varchar);
  FUNC2(FIND_IN_SET, varchar, varchar, datetime);
  FUNC2(FIND_IN_SET, varchar, datetime, int);
  FUNC2(FIND_IN_SET, varchar, datetime, float);
  FUNC2(FIND_IN_SET, varchar, datetime, varchar);
  FUNC2(FIND_IN_SET, varchar, datetime, datetime);
  FUNC2(CONCAT, int, int, int);
  FUNC2(CONCAT, int, int, float);
  FUNC2(CONCAT, int, int, varchar);
  FUNC2(CONCAT, int, int, datetime);
  FUNC2(CONCAT, int, float, int);
  FUNC2(CONCAT, int, float, float);
  FUNC2(CONCAT, int, float, varchar);
  FUNC2(CONCAT, int, float, datetime);
  FUNC2(CONCAT, int, varchar, int);
  FUNC2(CONCAT, int, varchar, float);
  FUNC2(CONCAT, int, varchar, varchar);
  FUNC2(CONCAT, int, varchar, datetime);
  FUNC2(CONCAT, int, datetime, int);
  FUNC2(CONCAT, int, datetime, float);
  FUNC2(CONCAT, int, datetime, varchar);
  FUNC2(CONCAT, int, datetime, datetime);
  FUNC2(CONCAT, varchar, int, int);
  FUNC2(CONCAT, varchar, int, float);
  FUNC2(CONCAT, varchar, int, varchar);
  FUNC2(CONCAT, varchar, int, datetime);
  FUNC2(CONCAT, varchar, float, int);
  FUNC2(CONCAT, varchar, float, float);
  FUNC2(CONCAT, varchar, float, varchar);
  FUNC2(CONCAT, varchar, float, datetime);
  FUNC2(CONCAT, varchar, varchar, int);
  FUNC2(CONCAT, varchar, varchar, float);
  FUNC2(CONCAT, varchar, varchar, varchar);
  FUNC2(CONCAT, varchar, varchar, datetime);
  FUNC2(CONCAT, varchar, datetime, int);
  FUNC2(CONCAT, varchar, datetime, float);
  FUNC2(CONCAT, varchar, datetime, varchar);
  FUNC2(CONCAT, varchar, datetime, datetime);
  FUNC2(IFNULL, int, int, int);
  FUNC2(IFNULL, int, int, float);
  FUNC2(IFNULL, int, int, varchar);
  FUNC2(IFNULL, int, int, datetime);
  FUNC2(IFNULL, int, float, int);
  FUNC2(IFNULL, int, float, float);
  FUNC2(IFNULL, int, float, varchar);
  FUNC2(IFNULL, int, float, datetime);
  FUNC2(IFNULL, int, varchar, int);
  FUNC2(IFNULL, int, varchar, float);
  FUNC2(IFNULL, int, varchar, varchar);
  FUNC2(IFNULL, int, varchar, datetime);
  FUNC2(IFNULL, int, datetime, int);
  FUNC2(IFNULL, int, datetime, float);
  FUNC2(IFNULL, int, datetime, varchar);
  FUNC2(IFNULL, int, datetime, datetime);
  FUNC2(IFNULL, varchar, int, int);
  FUNC2(IFNULL, varchar, int, float);
  FUNC2(IFNULL, varchar, int, varchar);
  FUNC2(IFNULL, varchar, int, datetime);
  FUNC2(IFNULL, varchar, float, int);
  FUNC2(IFNULL, varchar, float, float);
  FUNC2(IFNULL, varchar, float, varchar);
  FUNC2(IFNULL, varchar, float, datetime);
  FUNC2(IFNULL, varchar, varchar, int);
  FUNC2(IFNULL, varchar, varchar, float);
  FUNC2(IFNULL, varchar, varchar, varchar);
  FUNC2(IFNULL, varchar, varchar, datetime);
  FUNC2(IFNULL, varchar, datetime, int);
  FUNC2(IFNULL, varchar, datetime, float);
  FUNC2(IFNULL, varchar, datetime, varchar);
  FUNC2(IFNULL, varchar, datetime, datetime);
  FUNC2(NULLIF, int, int, int);
  FUNC2(NULLIF, int, int, float);
  FUNC2(NULLIF, int, int, varchar);
  FUNC2(NULLIF, int, int, datetime);
  FUNC2(NULLIF, int, float, int);
  FUNC2(NULLIF, int, float, float);
  FUNC2(NULLIF, int, float, varchar);
  FUNC2(NULLIF, int, float, datetime);
  FUNC2(NULLIF, int, varchar, int);
  FUNC2(NULLIF, int, varchar, float);
  FUNC2(NULLIF, int, varchar, varchar);
  FUNC2(NULLIF, int, varchar, datetime);
  FUNC2(NULLIF, int, datetime, int);
  FUNC2(NULLIF, int, datetime, float);
  FUNC2(NULLIF, int, datetime, varchar);
  FUNC2(NULLIF, int, datetime, datetime);
  FUNC2(NULLIF, varchar, int, int);
  FUNC2(NULLIF, varchar, int, float);
  FUNC2(NULLIF, varchar, int, varchar);
  FUNC2(NULLIF, varchar, int, datetime);
  FUNC2(NULLIF, varchar, float, int);
  FUNC2(NULLIF, varchar, float, float);
  FUNC2(NULLIF, varchar, float, varchar);
  FUNC2(NULLIF, varchar, float, datetime);
  FUNC2(NULLIF, varchar, varchar, int);
  FUNC2(NULLIF, varchar, varchar, float);
  FUNC2(NULLIF, varchar, varchar, varchar);
  FUNC2(NULLIF, varchar, varchar, datetime);
  FUNC2(NULLIF, varchar, datetime, int);
  FUNC2(NULLIF, varchar, datetime, float);
  FUNC2(NULLIF, varchar, datetime, varchar);
  FUNC2(NULLIF, varchar, datetime, datetime);
  FUNC2(COALESCE, int, int, int);
  FUNC2(COALESCE, int, int, float);
  FUNC2(COALESCE, int, int, varchar);
  FUNC2(COALESCE, int, int, datetime);
  FUNC2(COALESCE, int, float, int);
  FUNC2(COALESCE, int, float, float);
  FUNC2(COALESCE, int, float, varchar);
  FUNC2(COALESCE, int, float, datetime);
  FUNC2(COALESCE, int, varchar, int);
  FUNC2(COALESCE, int, varchar, float);
  FUNC2(COALESCE, int, varchar, varchar);
  FUNC2(COALESCE, int, varchar, datetime);
  FUNC2(COALESCE, int, datetime, int);
  FUNC2(COALESCE, int, datetime, float);
  FUNC2(COALESCE, int, datetime, varchar);
  FUNC2(COALESCE, int, datetime, datetime);
  FUNC2(COALESCE, varchar, int, int);
  FUNC2(COALESCE, varchar, int, float);
  FUNC2(COALESCE, varchar, int, varchar);
  FUNC2(COALESCE, varchar, int, datetime);
  FUNC2(COALESCE, varchar, float, int);
  FUNC2(COALESCE, varchar, float, float);
  FUNC2(COALESCE, varchar, float, varchar);
  FUNC2(COALESCE, varchar, float, datetime);
  FUNC2(COALESCE, varchar, varchar, int);
  FUNC2(COALESCE, varchar, varchar, float);
  FUNC2(COALESCE, varchar, varchar, varchar);
  FUNC2(COALESCE, varchar, varchar, datetime);
  FUNC2(COALESCE, varchar, datetime, int);
  FUNC2(COALESCE, varchar, datetime, float);
  FUNC2(COALESCE, varchar, datetime, varchar);
  FUNC2(COALESCE, varchar, datetime, datetime);
  FUNC2(GREATEST, int, int, int);
  FUNC2(GREATEST, int, int, float);
  FUNC2(GREATEST, int, int, varchar);
  FUNC2(GREATEST, int, int, datetime);
  FUNC2(GREATEST, int, float, int);
  FUNC2(GREATEST, int, float, float);
  FUNC2(GREATEST, int, float, varchar);
  FUNC2(GREATEST, int, float, datetime);
  FUNC2(GREATEST, int, varchar, int);
  FUNC2(GREATEST, int, varchar, float);
  FUNC2(GREATEST, int, varchar, varchar);
  FUNC2(GREATEST, int, varchar, datetime);
  FUNC2(GREATEST, int, datetime, int);
  FUNC2(GREATEST, int, datetime, float);
  FUNC2(GREATEST, int, datetime, varchar);
  FUNC2(GREATEST, int, datetime, datetime);
  FUNC2(GREATEST, varchar, int, int);
  FUNC2(GREATEST, varchar, int, float);
  FUNC2(GREATEST, varchar, int, varchar);
  FUNC2(GREATEST, varchar, int, datetime);
  FUNC2(GREATEST, varchar, float, int);
  FUNC2(GREATEST, varchar, float, float);
  FUNC2(GREATEST, varchar, float, varchar);
  FUNC2(GREATEST, varchar, float, datetime);
  FUNC2(GREATEST, varchar, varchar, int);
  FUNC2(GREATEST, varchar, varchar, float);
  FUNC2(GREATEST, varchar, varchar, varchar);
  FUNC2(GREATEST, varchar, varchar, datetime);
  FUNC2(GREATEST, varchar, datetime, int);
  FUNC2(GREATEST, varchar, datetime, float);
  FUNC2(GREATEST, varchar, datetime, varchar);
  FUNC2(GREATEST, varchar, datetime, datetime);
  FUNC2(LEAST, int, int, int);
  FUNC2(LEAST, int, int, float);
  FUNC2(LEAST, int, int, varchar);
  FUNC2(LEAST, int, int, datetime);
  FUNC2(LEAST, int, float, int);
  FUNC2(LEAST, int, float, float);
  FUNC2(LEAST, int, float, varchar);
  FUNC2(LEAST, int, float, datetime);
  FUNC2(LEAST, int, varchar, int);
  FUNC2(LEAST, int, varchar, float);
  FUNC2(LEAST, int, varchar, varchar);
  FUNC2(LEAST, int, varchar, datetime);
  FUNC2(LEAST, int, datetime, int);
  FUNC2(LEAST, int, datetime, float);
  FUNC2(LEAST, int, datetime, varchar);
  FUNC2(LEAST, int, datetime, datetime);
  FUNC2(LEAST, varchar, int, int);
  FUNC2(LEAST, varchar, int, float);
  FUNC2(LEAST, varchar, int, varchar);
  FUNC2(LEAST, varchar, int, datetime);
  FUNC2(LEAST, varchar, float, int);
  FUNC2(LEAST, varchar, float, float);
  FUNC2(LEAST, varchar, float, varchar);
  FUNC2(LEAST, varchar, float, datetime);
  FUNC2(LEAST, varchar, varchar, int);
  FUNC2(LEAST, varchar, varchar, float);
  FUNC2(LEAST, varchar, varchar, varchar);
  FUNC2(LEAST, varchar, varchar, datetime);
  FUNC2(LEAST, varchar, datetime, int);
  FUNC2(LEAST, varchar, datetime, float);
  FUNC2(LEAST, varchar, datetime, varchar);
  FUNC2(LEAST, varchar, datetime, datetime);

  //三元函数
  FUNC3(SUBSTRING_INDEX, varchar, int, int, int);
  FUNC3(SUBSTRING_INDEX, varchar, int, int, float);
  FUNC3(SUBSTRING_INDEX, varchar, int, int, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, int, float, int);
  FUNC3(SUBSTRING_INDEX, varchar, int, float, float);
  FUNC3(SUBSTRING_INDEX, varchar, int, float, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, int, varchar, int);
  FUNC3(SUBSTRING_INDEX, varchar, int, varchar, float);
  FUNC3(SUBSTRING_INDEX, varchar, int, varchar, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, float, int, int);
  FUNC3(SUBSTRING_INDEX, varchar, float, int, float);
  FUNC3(SUBSTRING_INDEX, varchar, float, int, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, float, float, int);
  FUNC3(SUBSTRING_INDEX, varchar, float, float, float);
  FUNC3(SUBSTRING_INDEX, varchar, float, float, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, float, varchar, int);
  FUNC3(SUBSTRING_INDEX, varchar, float, varchar, float);
  FUNC3(SUBSTRING_INDEX, varchar, float, varchar, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, int, int);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, int, float);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, int, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, float, int);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, float, float);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, float, varchar);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, varchar, int);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, varchar, float);
  FUNC3(SUBSTRING_INDEX, varchar, varchar, varchar, varchar);
  FUNC3(SUBSTRING, varchar, int, int, int);
  FUNC3(SUBSTRING, varchar, int, int, float);
  FUNC3(SUBSTRING, varchar, int, int, varchar);
  FUNC3(SUBSTRING, varchar, int, float, int);
  FUNC3(SUBSTRING, varchar, int, float, float);
  FUNC3(SUBSTRING, varchar, int, float, varchar);
  FUNC3(SUBSTRING, varchar, int, varchar, int);
  FUNC3(SUBSTRING, varchar, int, varchar, float);
  FUNC3(SUBSTRING, varchar, int, varchar, varchar);
  FUNC3(SUBSTRING, varchar, float, int, int);
  FUNC3(SUBSTRING, varchar, float, int, float);
  FUNC3(SUBSTRING, varchar, float, int, varchar);
  FUNC3(SUBSTRING, varchar, float, float, int);
  FUNC3(SUBSTRING, varchar, float, float, float);
  FUNC3(SUBSTRING, varchar, float, float, varchar);
  FUNC3(SUBSTRING, varchar, float, varchar, int);
  FUNC3(SUBSTRING, varchar, float, varchar, float);
  FUNC3(SUBSTRING, varchar, float, varchar, varchar);
  FUNC3(SUBSTRING, varchar, varchar, int, int);
  FUNC3(SUBSTRING, varchar, varchar, int, float);
  FUNC3(SUBSTRING, varchar, varchar, int, varchar);
  FUNC3(SUBSTRING, varchar, varchar, float, int);
  FUNC3(SUBSTRING, varchar, varchar, float, float);
  FUNC3(SUBSTRING, varchar, varchar, float, varchar);
  FUNC3(SUBSTRING, varchar, varchar, varchar, int);
  FUNC3(SUBSTRING, varchar, varchar, varchar, float);
  FUNC3(SUBSTRING, varchar, varchar, varchar, varchar);
  FUNC3(SUBSTR, varchar, int, int, int);
  FUNC3(SUBSTR, varchar, int, int, float);
  FUNC3(SUBSTR, varchar, int, int, varchar);
  FUNC3(SUBSTR, varchar, int, float, int);
  FUNC3(SUBSTR, varchar, int, float, float);
  FUNC3(SUBSTR, varchar, int, float, varchar);
  FUNC3(SUBSTR, varchar, int, varchar, int);
  FUNC3(SUBSTR, varchar, int, varchar, float);
  FUNC3(SUBSTR, varchar, int, varchar, varchar);
  FUNC3(SUBSTR, varchar, float, int, int);
  FUNC3(SUBSTR, varchar, float, int, float);
  FUNC3(SUBSTR, varchar, float, int, varchar);
  FUNC3(SUBSTR, varchar, float, float, int);
  FUNC3(SUBSTR, varchar, float, float, float);
  FUNC3(SUBSTR, varchar, float, float, varchar);
  FUNC3(SUBSTR, varchar, float, varchar, int);
  FUNC3(SUBSTR, varchar, float, varchar, float);
  FUNC3(SUBSTR, varchar, float, varchar, varchar);
  FUNC3(SUBSTR, varchar, varchar, int, int);
  FUNC3(SUBSTR, varchar, varchar, int, float);
  FUNC3(SUBSTR, varchar, varchar, int, varchar);
  FUNC3(SUBSTR, varchar, varchar, float, int);
  FUNC3(SUBSTR, varchar, varchar, float, float);
  FUNC3(SUBSTR, varchar, varchar, float, varchar);
  FUNC3(SUBSTR, varchar, varchar, varchar, int);
  FUNC3(SUBSTR, varchar, varchar, varchar, float);
  FUNC3(SUBSTR, varchar, varchar, varchar, varchar);
  FUNC3(RPAD, varchar, int, int, int);
  FUNC3(RPAD, varchar, int, int, float);
  FUNC3(RPAD, varchar, int, int, varchar);
  FUNC3(RPAD, varchar, int, float, int);
  FUNC3(RPAD, varchar, int, float, float);
  FUNC3(RPAD, varchar, int, float, varchar);
  FUNC3(RPAD, varchar, int, varchar, int);
  FUNC3(RPAD, varchar, int, varchar, float);
  FUNC3(RPAD, varchar, int, varchar, varchar);
  FUNC3(RPAD, varchar, float, int, int);
  FUNC3(RPAD, varchar, float, int, float);
  FUNC3(RPAD, varchar, float, int, varchar);
  FUNC3(RPAD, varchar, float, float, int);
  FUNC3(RPAD, varchar, float, float, float);
  FUNC3(RPAD, varchar, float, float, varchar);
  FUNC3(RPAD, varchar, float, varchar, int);
  FUNC3(RPAD, varchar, float, varchar, float);
  FUNC3(RPAD, varchar, float, varchar, varchar);
  FUNC3(RPAD, varchar, varchar, int, int);
  FUNC3(RPAD, varchar, varchar, int, float);
  FUNC3(RPAD, varchar, varchar, int, varchar);
  FUNC3(RPAD, varchar, varchar, float, int);
  FUNC3(RPAD, varchar, varchar, float, float);
  FUNC3(RPAD, varchar, varchar, float, varchar);
  FUNC3(RPAD, varchar, varchar, varchar, int);
  FUNC3(RPAD, varchar, varchar, varchar, float);
  FUNC3(RPAD, varchar, varchar, varchar, varchar);
  FUNC3(REPLACE, varchar, int, int, int);
  FUNC3(REPLACE, varchar, int, int, float);
  FUNC3(REPLACE, varchar, int, int, varchar);
  FUNC3(REPLACE, varchar, int, float, int);
  FUNC3(REPLACE, varchar, int, float, float);
  FUNC3(REPLACE, varchar, int, float, varchar);
  FUNC3(REPLACE, varchar, int, varchar, int);
  FUNC3(REPLACE, varchar, int, varchar, float);
  FUNC3(REPLACE, varchar, int, varchar, varchar);
  FUNC3(REPLACE, varchar, float, int, int);
  FUNC3(REPLACE, varchar, float, int, float);
  FUNC3(REPLACE, varchar, float, int, varchar);
  FUNC3(REPLACE, varchar, float, float, int);
  FUNC3(REPLACE, varchar, float, float, float);
  FUNC3(REPLACE, varchar, float, float, varchar);
  FUNC3(REPLACE, varchar, float, varchar, int);
  FUNC3(REPLACE, varchar, float, varchar, float);
  FUNC3(REPLACE, varchar, float, varchar, varchar);
  FUNC3(REPLACE, varchar, varchar, int, int);
  FUNC3(REPLACE, varchar, varchar, int, float);
  FUNC3(REPLACE, varchar, varchar, int, varchar);
  FUNC3(REPLACE, varchar, varchar, float, int);
  FUNC3(REPLACE, varchar, varchar, float, float);
  FUNC3(REPLACE, varchar, varchar, float, varchar);
  FUNC3(REPLACE, varchar, varchar, varchar, int);
  FUNC3(REPLACE, varchar, varchar, varchar, float);
  FUNC3(REPLACE, varchar, varchar, varchar, varchar);
  FUNC3(MAKE_SET, varchar, int, int, int);
  FUNC3(MAKE_SET, varchar, int, int, float);
  FUNC3(MAKE_SET, varchar, int, int, varchar);
  FUNC3(MAKE_SET, varchar, int, float, int);
  FUNC3(MAKE_SET, varchar, int, float, float);
  FUNC3(MAKE_SET, varchar, int, float, varchar);
  FUNC3(MAKE_SET, varchar, int, varchar, int);
  FUNC3(MAKE_SET, varchar, int, varchar, float);
  FUNC3(MAKE_SET, varchar, int, varchar, varchar);
  FUNC3(MAKE_SET, varchar, float, int, int);
  FUNC3(MAKE_SET, varchar, float, int, float);
  FUNC3(MAKE_SET, varchar, float, int, varchar);
  FUNC3(MAKE_SET, varchar, float, float, int);
  FUNC3(MAKE_SET, varchar, float, float, float);
  FUNC3(MAKE_SET, varchar, float, float, varchar);
  FUNC3(MAKE_SET, varchar, float, varchar, int);
  FUNC3(MAKE_SET, varchar, float, varchar, float);
  FUNC3(MAKE_SET, varchar, float, varchar, varchar);
  FUNC3(MAKE_SET, varchar, varchar, int, int);
  FUNC3(MAKE_SET, varchar, varchar, int, float);
  FUNC3(MAKE_SET, varchar, varchar, int, varchar);
  FUNC3(MAKE_SET, varchar, varchar, float, int);
  FUNC3(MAKE_SET, varchar, varchar, float, float);
  FUNC3(MAKE_SET, varchar, varchar, float, varchar);
  FUNC3(MAKE_SET, varchar, varchar, varchar, int);
  FUNC3(MAKE_SET, varchar, varchar, varchar, float);
  FUNC3(MAKE_SET, varchar, varchar, varchar, varchar);
  FUNC3(MID, varchar, int, int, int);
  FUNC3(MID, varchar, int, int, float);
  FUNC3(MID, varchar, int, int, varchar);
  FUNC3(MID, varchar, int, float, int);
  FUNC3(MID, varchar, int, float, float);
  FUNC3(MID, varchar, int, float, varchar);
  FUNC3(MID, varchar, int, varchar, int);
  FUNC3(MID, varchar, int, varchar, float);
  FUNC3(MID, varchar, int, varchar, varchar);
  FUNC3(MID, varchar, float, int, int);
  FUNC3(MID, varchar, float, int, float);
  FUNC3(MID, varchar, float, int, varchar);
  FUNC3(MID, varchar, float, float, int);
  FUNC3(MID, varchar, float, float, float);
  FUNC3(MID, varchar, float, float, varchar);
  FUNC3(MID, varchar, float, varchar, int);
  FUNC3(MID, varchar, float, varchar, float);
  FUNC3(MID, varchar, float, varchar, varchar);
  FUNC3(MID, varchar, varchar, int, int);
  FUNC3(MID, varchar, varchar, int, float);
  FUNC3(MID, varchar, varchar, int, varchar);
  FUNC3(MID, varchar, varchar, float, int);
  FUNC3(MID, varchar, varchar, float, float);
  FUNC3(MID, varchar, varchar, float, varchar);
  FUNC3(MID, varchar, varchar, varchar, int);
  FUNC3(MID, varchar, varchar, varchar, float);
  FUNC3(MID, varchar, varchar, varchar, varchar);
  FUNC3(LPAD, varchar, int, int, int);
  FUNC3(LPAD, varchar, int, int, float);
  FUNC3(LPAD, varchar, int, int, varchar);
  FUNC3(LPAD, varchar, int, float, int);
  FUNC3(LPAD, varchar, int, float, float);
  FUNC3(LPAD, varchar, int, float, varchar);
  FUNC3(LPAD, varchar, int, varchar, int);
  FUNC3(LPAD, varchar, int, varchar, float);
  FUNC3(LPAD, varchar, int, varchar, varchar);
  FUNC3(LPAD, varchar, float, int, int);
  FUNC3(LPAD, varchar, float, int, float);
  FUNC3(LPAD, varchar, float, int, varchar);
  FUNC3(LPAD, varchar, float, float, int);
  FUNC3(LPAD, varchar, float, float, float);
  FUNC3(LPAD, varchar, float, float, varchar);
  FUNC3(LPAD, varchar, float, varchar, int);
  FUNC3(LPAD, varchar, float, varchar, float);
  FUNC3(LPAD, varchar, float, varchar, varchar);
  FUNC3(LPAD, varchar, varchar, int, int);
  FUNC3(LPAD, varchar, varchar, int, float);
  FUNC3(LPAD, varchar, varchar, int, varchar);
  FUNC3(LPAD, varchar, varchar, float, int);
  FUNC3(LPAD, varchar, varchar, float, float);
  FUNC3(LPAD, varchar, varchar, float, varchar);
  FUNC3(LPAD, varchar, varchar, varchar, int);
  FUNC3(LPAD, varchar, varchar, varchar, float);
  FUNC3(LPAD, varchar, varchar, varchar, varchar);
  FUNC3(LOCATE, varchar, int, int, int);
  FUNC3(LOCATE, varchar, int, int, float);
  FUNC3(LOCATE, varchar, int, int, varchar);
  FUNC3(LOCATE, varchar, int, float, int);
  FUNC3(LOCATE, varchar, int, float, float);
  FUNC3(LOCATE, varchar, int, float, varchar);
  FUNC3(LOCATE, varchar, int, varchar, int);
  FUNC3(LOCATE, varchar, int, varchar, float);
  FUNC3(LOCATE, varchar, int, varchar, varchar);
  FUNC3(LOCATE, varchar, float, int, int);
  FUNC3(LOCATE, varchar, float, int, float);
  FUNC3(LOCATE, varchar, float, int, varchar);
  FUNC3(LOCATE, varchar, float, float, int);
  FUNC3(LOCATE, varchar, float, float, float);
  FUNC3(LOCATE, varchar, float, float, varchar);
  FUNC3(LOCATE, varchar, float, varchar, int);
  FUNC3(LOCATE, varchar, float, varchar, float);
  FUNC3(LOCATE, varchar, float, varchar, varchar);
  FUNC3(LOCATE, varchar, varchar, int, int);
  FUNC3(LOCATE, varchar, varchar, int, float);
  FUNC3(LOCATE, varchar, varchar, int, varchar);
  FUNC3(LOCATE, varchar, varchar, float, int);
  FUNC3(LOCATE, varchar, varchar, float, float);
  FUNC3(LOCATE, varchar, varchar, float, varchar);
  FUNC3(LOCATE, varchar, varchar, varchar, int);
  FUNC3(LOCATE, varchar, varchar, varchar, float);
  FUNC3(LOCATE, varchar, varchar, varchar, varchar);
  FUNC3(CONCAT, varchar, int, int, int);
  FUNC3(CONCAT, varchar, int, int, float);
  FUNC3(CONCAT, varchar, int, int, varchar);
  FUNC3(CONCAT, varchar, int, float, int);
  FUNC3(CONCAT, varchar, int, float, float);
  FUNC3(CONCAT, varchar, int, float, varchar);
  FUNC3(CONCAT, varchar, int, varchar, int);
  FUNC3(CONCAT, varchar, int, varchar, float);
  FUNC3(CONCAT, varchar, int, varchar, varchar);
  FUNC3(CONCAT, varchar, float, int, int);
  FUNC3(CONCAT, varchar, float, int, float);
  FUNC3(CONCAT, varchar, float, int, varchar);
  FUNC3(CONCAT, varchar, float, float, int);
  FUNC3(CONCAT, varchar, float, float, float);
  FUNC3(CONCAT, varchar, float, float, varchar);
  FUNC3(CONCAT, varchar, float, varchar, int);
  FUNC3(CONCAT, varchar, float, varchar, float);
  FUNC3(CONCAT, varchar, float, varchar, varchar);
  FUNC3(CONCAT, varchar, varchar, int, int);
  FUNC3(CONCAT, varchar, varchar, int, float);
  FUNC3(CONCAT, varchar, varchar, int, varchar);
  FUNC3(CONCAT, varchar, varchar, float, int);
  FUNC3(CONCAT, varchar, varchar, float, float);
  FUNC3(CONCAT, varchar, varchar, float, varchar);
  FUNC3(CONCAT, varchar, varchar, varchar, int);
  FUNC3(CONCAT, varchar, varchar, varchar, float);
  FUNC3(CONCAT, varchar, varchar, varchar, varchar);
  FUNC3(CONCAT_WS, varchar, int, int, int);
  FUNC3(CONCAT_WS, varchar, int, int, float);
  FUNC3(CONCAT_WS, varchar, int, int, varchar);
  FUNC3(CONCAT_WS, varchar, int, float, int);
  FUNC3(CONCAT_WS, varchar, int, float, float);
  FUNC3(CONCAT_WS, varchar, int, float, varchar);
  FUNC3(CONCAT_WS, varchar, int, varchar, int);
  FUNC3(CONCAT_WS, varchar, int, varchar, float);
  FUNC3(CONCAT_WS, varchar, int, varchar, varchar);
  FUNC3(CONCAT_WS, varchar, float, int, int);
  FUNC3(CONCAT_WS, varchar, float, int, float);
  FUNC3(CONCAT_WS, varchar, float, int, varchar);
  FUNC3(CONCAT_WS, varchar, float, float, int);
  FUNC3(CONCAT_WS, varchar, float, float, float);
  FUNC3(CONCAT_WS, varchar, float, float, varchar);
  FUNC3(CONCAT_WS, varchar, float, varchar, int);
  FUNC3(CONCAT_WS, varchar, float, varchar, float);
  FUNC3(CONCAT_WS, varchar, float, varchar, varchar);
  FUNC3(CONCAT_WS, varchar, varchar, int, int);
  FUNC3(CONCAT_WS, varchar, varchar, int, float);
  FUNC3(CONCAT_WS, varchar, varchar, int, varchar);
  FUNC3(CONCAT_WS, varchar, varchar, float, int);
  FUNC3(CONCAT_WS, varchar, varchar, float, float);
  FUNC3(CONCAT_WS, varchar, varchar, float, varchar);
  FUNC3(CONCAT_WS, varchar, varchar, varchar, int);
  FUNC3(CONCAT_WS, varchar, varchar, varchar, float);
  FUNC3(CONCAT_WS, varchar, varchar, varchar, varchar);
  FUNC3(IF, varchar, int, int, int);
  FUNC3(IF, varchar, int, int, float);
  FUNC3(IF, varchar, int, int, varchar);
  FUNC3(IF, varchar, int, float, int);
  FUNC3(IF, varchar, int, float, float);
  FUNC3(IF, varchar, int, float, varchar);
  FUNC3(IF, varchar, int, varchar, int);
  FUNC3(IF, varchar, int, varchar, float);
  FUNC3(IF, varchar, int, varchar, varchar);
  FUNC3(IF, varchar, float, int, int);
  FUNC3(IF, varchar, float, int, float);
  FUNC3(IF, varchar, float, int, varchar);
  FUNC3(IF, varchar, float, float, int);
  FUNC3(IF, varchar, float, float, float);
  FUNC3(IF, varchar, float, float, varchar);
  FUNC3(IF, varchar, float, varchar, int);
  FUNC3(IF, varchar, float, varchar, float);
  FUNC3(IF, varchar, float, varchar, varchar);
  FUNC3(IF, varchar, varchar, int, int);
  FUNC3(IF, varchar, varchar, int, float);
  FUNC3(IF, varchar, varchar, int, varchar);
  FUNC3(IF, varchar, varchar, float, int);
  FUNC3(IF, varchar, varchar, float, float);
  FUNC3(IF, varchar, varchar, float, varchar);
  FUNC3(IF, varchar, varchar, varchar, int);
  FUNC3(IF, varchar, varchar, varchar, float);
  FUNC3(IF, varchar, varchar, varchar, varchar);
  
  //四元函数
  FUNC4(INSERT, varchar, int, int, int, int);
  FUNC4(INSERT, varchar, int, int, int, float);
  FUNC4(INSERT, varchar, int, int, int, varchar);
  FUNC4(INSERT, varchar, int, int, float, int);
  FUNC4(INSERT, varchar, int, int, float, float);
  FUNC4(INSERT, varchar, int, int, float, varchar);
  FUNC4(INSERT, varchar, int, int, varchar, int);
  FUNC4(INSERT, varchar, int, int, varchar, float);
  FUNC4(INSERT, varchar, int, int, varchar, varchar);
  FUNC4(INSERT, varchar, int, float, int, int);
  FUNC4(INSERT, varchar, int, float, int, float);
  FUNC4(INSERT, varchar, int, float, int, varchar);
  FUNC4(INSERT, varchar, int, float, float, int);
  FUNC4(INSERT, varchar, int, float, float, float);
  FUNC4(INSERT, varchar, int, float, float, varchar);
  FUNC4(INSERT, varchar, int, float, varchar, int);
  FUNC4(INSERT, varchar, int, float, varchar, float);
  FUNC4(INSERT, varchar, int, float, varchar, varchar);
  FUNC4(INSERT, varchar, int, varchar, int, int);
  FUNC4(INSERT, varchar, int, varchar, int, float);
  FUNC4(INSERT, varchar, int, varchar, int, varchar);
  FUNC4(INSERT, varchar, int, varchar, float, int);
  FUNC4(INSERT, varchar, int, varchar, float, float);
  FUNC4(INSERT, varchar, int, varchar, float, varchar);
  FUNC4(INSERT, varchar, int, varchar, varchar, int);
  FUNC4(INSERT, varchar, int, varchar, varchar, float);
  FUNC4(INSERT, varchar, int, varchar, varchar, varchar);
  FUNC4(INSERT, varchar, float, int, int, int);
  FUNC4(INSERT, varchar, float, int, int, float);
  FUNC4(INSERT, varchar, float, int, int, varchar);
  FUNC4(INSERT, varchar, float, int, float, int);
  FUNC4(INSERT, varchar, float, int, float, float);
  FUNC4(INSERT, varchar, float, int, float, varchar);
  FUNC4(INSERT, varchar, float, int, varchar, int);
  FUNC4(INSERT, varchar, float, int, varchar, float);
  FUNC4(INSERT, varchar, float, int, varchar, varchar);
  FUNC4(INSERT, varchar, float, float, int, int);
  FUNC4(INSERT, varchar, float, float, int, float);
  FUNC4(INSERT, varchar, float, float, int, varchar);
  FUNC4(INSERT, varchar, float, float, float, int);
  FUNC4(INSERT, varchar, float, float, float, float);
  FUNC4(INSERT, varchar, float, float, float, varchar);
  FUNC4(INSERT, varchar, float, float, varchar, int);
  FUNC4(INSERT, varchar, float, float, varchar, float);
  FUNC4(INSERT, varchar, float, float, varchar, varchar);
  FUNC4(INSERT, varchar, float, varchar, int, int);
  FUNC4(INSERT, varchar, float, varchar, int, float);
  FUNC4(INSERT, varchar, float, varchar, int, varchar);
  FUNC4(INSERT, varchar, float, varchar, float, int);
  FUNC4(INSERT, varchar, float, varchar, float, float);
  FUNC4(INSERT, varchar, float, varchar, float, varchar);
  FUNC4(INSERT, varchar, float, varchar, varchar, int);
  FUNC4(INSERT, varchar, float, varchar, varchar, float);
  FUNC4(INSERT, varchar, float, varchar, varchar, varchar);
  FUNC4(INSERT, varchar, varchar, int, int, int);
  FUNC4(INSERT, varchar, varchar, int, int, float);
  FUNC4(INSERT, varchar, varchar, int, int, varchar);
  FUNC4(INSERT, varchar, varchar, int, float, int);
  FUNC4(INSERT, varchar, varchar, int, float, float);
  FUNC4(INSERT, varchar, varchar, int, float, varchar);
  FUNC4(INSERT, varchar, varchar, int, varchar, int);
  FUNC4(INSERT, varchar, varchar, int, varchar, float);
  FUNC4(INSERT, varchar, varchar, int, varchar, varchar);
  FUNC4(INSERT, varchar, varchar, float, int, int);
  FUNC4(INSERT, varchar, varchar, float, int, float);
  FUNC4(INSERT, varchar, varchar, float, int, varchar);
  FUNC4(INSERT, varchar, varchar, float, float, int);
  FUNC4(INSERT, varchar, varchar, float, float, float);
  FUNC4(INSERT, varchar, varchar, float, float, varchar);
  FUNC4(INSERT, varchar, varchar, float, varchar, int);
  FUNC4(INSERT, varchar, varchar, float, varchar, float);
  FUNC4(INSERT, varchar, varchar, float, varchar, varchar);
  FUNC4(INSERT, varchar, varchar, varchar, int, int);
  FUNC4(INSERT, varchar, varchar, varchar, int, float);
  FUNC4(INSERT, varchar, varchar, varchar, int, varchar);
  FUNC4(INSERT, varchar, varchar, varchar, float, int);
  FUNC4(INSERT, varchar, varchar, varchar, float, float);
  FUNC4(INSERT, varchar, varchar, varchar, float, varchar);
  FUNC4(INSERT, varchar, varchar, varchar, varchar, int);
  FUNC4(INSERT, varchar, varchar, varchar, varchar, float);
  FUNC4(INSERT, varchar, varchar, varchar, varchar, varchar);
  FUNC4(FIELD, varchar, int, int, int, int);
  FUNC4(FIELD, varchar, int, int, int, float);
  FUNC4(FIELD, varchar, int, int, int, varchar);
  FUNC4(FIELD, varchar, int, int, float, int);
  FUNC4(FIELD, varchar, int, int, float, float);
  FUNC4(FIELD, varchar, int, int, float, varchar);
  FUNC4(FIELD, varchar, int, int, varchar, int);
  FUNC4(FIELD, varchar, int, int, varchar, float);
  FUNC4(FIELD, varchar, int, int, varchar, varchar);
  FUNC4(FIELD, varchar, int, float, int, int);
  FUNC4(FIELD, varchar, int, float, int, float);
  FUNC4(FIELD, varchar, int, float, int, varchar);
  FUNC4(FIELD, varchar, int, float, float, int);
  FUNC4(FIELD, varchar, int, float, float, float);
  FUNC4(FIELD, varchar, int, float, float, varchar);
  FUNC4(FIELD, varchar, int, float, varchar, int);
  FUNC4(FIELD, varchar, int, float, varchar, float);
  FUNC4(FIELD, varchar, int, float, varchar, varchar);
  FUNC4(FIELD, varchar, int, varchar, int, int);
  FUNC4(FIELD, varchar, int, varchar, int, float);
  FUNC4(FIELD, varchar, int, varchar, int, varchar);
  FUNC4(FIELD, varchar, int, varchar, float, int);
  FUNC4(FIELD, varchar, int, varchar, float, float);
  FUNC4(FIELD, varchar, int, varchar, float, varchar);
  FUNC4(FIELD, varchar, int, varchar, varchar, int);
  FUNC4(FIELD, varchar, int, varchar, varchar, float);
  FUNC4(FIELD, varchar, int, varchar, varchar, varchar);
  FUNC4(FIELD, varchar, float, int, int, int);
  FUNC4(FIELD, varchar, float, int, int, float);
  FUNC4(FIELD, varchar, float, int, int, varchar);
  FUNC4(FIELD, varchar, float, int, float, int);
  FUNC4(FIELD, varchar, float, int, float, float);
  FUNC4(FIELD, varchar, float, int, float, varchar);
  FUNC4(FIELD, varchar, float, int, varchar, int);
  FUNC4(FIELD, varchar, float, int, varchar, float);
  FUNC4(FIELD, varchar, float, int, varchar, varchar);
  FUNC4(FIELD, varchar, float, float, int, int);
  FUNC4(FIELD, varchar, float, float, int, float);
  FUNC4(FIELD, varchar, float, float, int, varchar);
  FUNC4(FIELD, varchar, float, float, float, int);
  FUNC4(FIELD, varchar, float, float, float, float);
  FUNC4(FIELD, varchar, float, float, float, varchar);
  FUNC4(FIELD, varchar, float, float, varchar, int);
  FUNC4(FIELD, varchar, float, float, varchar, float);
  FUNC4(FIELD, varchar, float, float, varchar, varchar);
  FUNC4(FIELD, varchar, float, varchar, int, int);
  FUNC4(FIELD, varchar, float, varchar, int, float);
  FUNC4(FIELD, varchar, float, varchar, int, varchar);
  FUNC4(FIELD, varchar, float, varchar, float, int);
  FUNC4(FIELD, varchar, float, varchar, float, float);
  FUNC4(FIELD, varchar, float, varchar, float, varchar);
  FUNC4(FIELD, varchar, float, varchar, varchar, int);
  FUNC4(FIELD, varchar, float, varchar, varchar, float);
  FUNC4(FIELD, varchar, float, varchar, varchar, varchar);
  FUNC4(FIELD, varchar, varchar, int, int, int);
  FUNC4(FIELD, varchar, varchar, int, int, float);
  FUNC4(FIELD, varchar, varchar, int, int, varchar);
  FUNC4(FIELD, varchar, varchar, int, float, int);
  FUNC4(FIELD, varchar, varchar, int, float, float);
  FUNC4(FIELD, varchar, varchar, int, float, varchar);
  FUNC4(FIELD, varchar, varchar, int, varchar, int);
  FUNC4(FIELD, varchar, varchar, int, varchar, float);
  FUNC4(FIELD, varchar, varchar, int, varchar, varchar);
  FUNC4(FIELD, varchar, varchar, float, int, int);
  FUNC4(FIELD, varchar, varchar, float, int, float);
  FUNC4(FIELD, varchar, varchar, float, int, varchar);
  FUNC4(FIELD, varchar, varchar, float, float, int);
  FUNC4(FIELD, varchar, varchar, float, float, float);
  FUNC4(FIELD, varchar, varchar, float, float, varchar);
  FUNC4(FIELD, varchar, varchar, float, varchar, int);
  FUNC4(FIELD, varchar, varchar, float, varchar, float);
  FUNC4(FIELD, varchar, varchar, float, varchar, varchar);
  FUNC4(FIELD, varchar, varchar, varchar, int, int);
  FUNC4(FIELD, varchar, varchar, varchar, int, float);
  FUNC4(FIELD, varchar, varchar, varchar, int, varchar);
  FUNC4(FIELD, varchar, varchar, varchar, float, int);
  FUNC4(FIELD, varchar, varchar, varchar, float, float);
  FUNC4(FIELD, varchar, varchar, varchar, float, varchar);
  FUNC4(FIELD, varchar, varchar, varchar, varchar, int);
  FUNC4(FIELD, varchar, varchar, varchar, varchar, float);
  FUNC4(FIELD, varchar, varchar, varchar, varchar, varchar);
  FUNC4(ELT, varchar, int, int, int, int);
  FUNC4(ELT, varchar, int, int, int, float);
  FUNC4(ELT, varchar, int, int, int, varchar);
  FUNC4(ELT, varchar, int, int, float, int);
  FUNC4(ELT, varchar, int, int, float, float);
  FUNC4(ELT, varchar, int, int, float, varchar);
  FUNC4(ELT, varchar, int, int, varchar, int);
  FUNC4(ELT, varchar, int, int, varchar, float);
  FUNC4(ELT, varchar, int, int, varchar, varchar);
  FUNC4(ELT, varchar, int, float, int, int);
  FUNC4(ELT, varchar, int, float, int, float);
  FUNC4(ELT, varchar, int, float, int, varchar);
  FUNC4(ELT, varchar, int, float, float, int);
  FUNC4(ELT, varchar, int, float, float, float);
  FUNC4(ELT, varchar, int, float, float, varchar);
  FUNC4(ELT, varchar, int, float, varchar, int);
  FUNC4(ELT, varchar, int, float, varchar, float);
  FUNC4(ELT, varchar, int, float, varchar, varchar);
  FUNC4(ELT, varchar, int, varchar, int, int);
  FUNC4(ELT, varchar, int, varchar, int, float);
  FUNC4(ELT, varchar, int, varchar, int, varchar);
  FUNC4(ELT, varchar, int, varchar, float, int);
  FUNC4(ELT, varchar, int, varchar, float, float);
  FUNC4(ELT, varchar, int, varchar, float, varchar);
  FUNC4(ELT, varchar, int, varchar, varchar, int);
  FUNC4(ELT, varchar, int, varchar, varchar, float);
  FUNC4(ELT, varchar, int, varchar, varchar, varchar);
  FUNC4(ELT, varchar, float, int, int, int);
  FUNC4(ELT, varchar, float, int, int, float);
  FUNC4(ELT, varchar, float, int, int, varchar);
  FUNC4(ELT, varchar, float, int, float, int);
  FUNC4(ELT, varchar, float, int, float, float);
  FUNC4(ELT, varchar, float, int, float, varchar);
  FUNC4(ELT, varchar, float, int, varchar, int);
  FUNC4(ELT, varchar, float, int, varchar, float);
  FUNC4(ELT, varchar, float, int, varchar, varchar);
  FUNC4(ELT, varchar, float, float, int, int);
  FUNC4(ELT, varchar, float, float, int, float);
  FUNC4(ELT, varchar, float, float, int, varchar);
  FUNC4(ELT, varchar, float, float, float, int);
  FUNC4(ELT, varchar, float, float, float, float);
  FUNC4(ELT, varchar, float, float, float, varchar);
  FUNC4(ELT, varchar, float, float, varchar, int);
  FUNC4(ELT, varchar, float, float, varchar, float);
  FUNC4(ELT, varchar, float, float, varchar, varchar);
  FUNC4(ELT, varchar, float, varchar, int, int);
  FUNC4(ELT, varchar, float, varchar, int, float);
  FUNC4(ELT, varchar, float, varchar, int, varchar);
  FUNC4(ELT, varchar, float, varchar, float, int);
  FUNC4(ELT, varchar, float, varchar, float, float);
  FUNC4(ELT, varchar, float, varchar, float, varchar);
  FUNC4(ELT, varchar, float, varchar, varchar, int);
  FUNC4(ELT, varchar, float, varchar, varchar, float);
  FUNC4(ELT, varchar, float, varchar, varchar, varchar);
  FUNC4(ELT, varchar, varchar, int, int, int);
  FUNC4(ELT, varchar, varchar, int, int, float);
  FUNC4(ELT, varchar, varchar, int, int, varchar);
  FUNC4(ELT, varchar, varchar, int, float, int);
  FUNC4(ELT, varchar, varchar, int, float, float);
  FUNC4(ELT, varchar, varchar, int, float, varchar);
  FUNC4(ELT, varchar, varchar, int, varchar, int);
  FUNC4(ELT, varchar, varchar, int, varchar, float);
  FUNC4(ELT, varchar, varchar, int, varchar, varchar);
  FUNC4(ELT, varchar, varchar, float, int, int);
  FUNC4(ELT, varchar, varchar, float, int, float);
  FUNC4(ELT, varchar, varchar, float, int, varchar);
  FUNC4(ELT, varchar, varchar, float, float, int);
  FUNC4(ELT, varchar, varchar, float, float, float);
  FUNC4(ELT, varchar, varchar, float, float, varchar);
  FUNC4(ELT, varchar, varchar, float, varchar, int);
  FUNC4(ELT, varchar, varchar, float, varchar, float);
  FUNC4(ELT, varchar, varchar, float, varchar, varchar);
  FUNC4(ELT, varchar, varchar, varchar, int, int);
  FUNC4(ELT, varchar, varchar, varchar, int, float);
  FUNC4(ELT, varchar, varchar, varchar, int, varchar);
  FUNC4(ELT, varchar, varchar, varchar, float, int);
  FUNC4(ELT, varchar, varchar, varchar, float, float);
  FUNC4(ELT, varchar, varchar, varchar, float, varchar);
  FUNC4(ELT, varchar, varchar, varchar, varchar, int);
  FUNC4(ELT, varchar, varchar, varchar, varchar, float);
  FUNC4(ELT, varchar, varchar, varchar, varchar, varchar);

  //时间函数
  FUNC1(WEEK, int, date);
  FUNC1(WEEK, int, time);
  FUNC1(WEEK, int, timestamp);
  FUNC1(WEEK, int, datetime);
  FUNC1(WEEKDAY, int, date);
  FUNC1(WEEKDAY, int, time);
  FUNC1(WEEKDAY, int, timestamp);
  FUNC1(WEEKDAY, int, datetime);
  FUNC1(WEEKOFYEAR, int, date);
  FUNC1(WEEKOFYEAR, int, time);
  FUNC1(WEEKOFYEAR, int, timestamp);
  FUNC1(WEEKOFYEAR, int, datetime);
  FUNC1(YEAR, int, date);
  FUNC1(YEAR, int, time);
  FUNC1(YEAR, int, timestamp);
  FUNC1(YEAR, int, datetime);
  FUNC1(YEARWEEK, int, date);
  FUNC1(YEARWEEK, int, time);
  FUNC1(YEARWEEK, int, timestamp);
  FUNC1(YEARWEEK, int, datetime);
  FUNC1(DAY, int, date);
  FUNC1(DAY, int, time);
  FUNC1(DAY, int, timestamp);
  FUNC1(DAY, int, datetime);
  FUNC1(DAYNAME, varchar, date);
  FUNC1(DAYNAME, varchar, time);
  FUNC1(DAYNAME, varchar, timestamp);
  FUNC1(DAYNAME, varchar, datetime);
  FUNC1(DAYOFMONTH, int, date);
  FUNC1(DAYOFMONTH, int, time);
  FUNC1(DAYOFMONTH, int, timestamp);
  FUNC1(DAYOFMONTH, int, datetime);
  FUNC1(DAYOFWEEK, int, date);
  FUNC1(DAYOFWEEK, int, time);
  FUNC1(DAYOFWEEK, int, timestamp);
  FUNC1(DAYOFWEEK, int, datetime);
  FUNC1(DAYOFYEAR, int, date);
  FUNC1(DAYOFYEAR, int, time);
  FUNC1(DAYOFYEAR, int, timestamp);
  FUNC1(DAYOFYEAR, int, datetime);
  FUNC1(HOUR, int, date);
  FUNC1(HOUR, int, time);
  FUNC1(HOUR, int, timestamp);
  FUNC1(HOUR, int, datetime);
  FUNC1(LAST_DAY, date, date);
  FUNC1(LAST_DAY, date, timestamp);
  FUNC1(LAST_DAY, date, datetime);
  FUNC1(MINUTE, int, date);
  FUNC1(MINUTE, int, time);
  FUNC1(MINUTE, int, timestamp);
  FUNC1(MINUTE, int, datetime);
  FUNC1(MONTH, int, date);
  FUNC1(MONTH, int, time);
  FUNC1(MONTH, int, timestamp);
  FUNC1(MONTH, int, datetime);
  FUNC1(MONTHNAME, varchar, date);
  FUNC1(MONTHNAME, varchar, time);
  FUNC1(MONTHNAME, varchar, timestamp);
  FUNC1(MONTHNAME, varchar, datetime);
  FUNC1(QUARTER, int, date);
  FUNC1(QUARTER, int, time);
  FUNC1(QUARTER, int, timestamp);
  FUNC1(QUARTER, int, datetime);
  FUNC1(QUARTER, varchar, date);
  FUNC1(QUARTER, varchar, time);
  FUNC1(QUARTER, varchar, timestamp);
  FUNC1(QUARTER, varchar, datetime);
  FUNC1(SECOND, int, date);
  FUNC1(SECOND, int, time);
  FUNC1(SECOND, int, timestamp);
  FUNC1(SECOND, int, datetime);
  FUNC1(SECOND, varchar, date);
  FUNC1(SECOND, varchar, time);
  FUNC1(SECOND, varchar, timestamp);
  FUNC1(SECOND, varchar, datetime);
  FUNC1(TIME_TO_SEC, int, date);
  FUNC1(TIME_TO_SEC, int, time);
  FUNC1(TIME_TO_SEC, int, timestamp);
  FUNC1(TIME_TO_SEC, int, datetime);
  FUNC1(TIME_TO_SEC, varchar, date);
  FUNC1(TIME_TO_SEC, varchar, time);
  FUNC1(TIME_TO_SEC, varchar, timestamp);
  FUNC1(TIME_TO_SEC, varchar, datetime);
  FUNC1(TO_DAYS, int, date);
  FUNC1(TO_DAYS, int, timestamp);
  FUNC1(TO_DAYS, int, datetime);
  FUNC1(TO_DAYS, varchar, date);
  FUNC1(TO_DAYS, varchar, time);
  FUNC1(TO_DAYS, varchar, timestamp);
  FUNC1(TO_DAYS, varchar, datetime);  
  FUNC1(DATE, int, date);
  FUNC1(DATE, int, time);
  FUNC1(DATE, int, timestamp);
  FUNC1(DATE, int, datetime);
  FUNC1(DATE, varchar, date);
  FUNC1(DATE, varchar, time);
  FUNC1(DATE, varchar, timestamp);
  FUNC1(DATE, varchar, datetime);
  FUNC1(TIME, int, date);
  FUNC1(TIME, int, time);
  FUNC1(TIME, int, timestamp);
  FUNC1(TIME, int, datetime);
  FUNC1(TIME, varchar, date);
  FUNC1(TIME, varchar, time);
  FUNC1(TIME, varchar, timestamp);
  FUNC1(TIME, varchar, datetime);
  FUNC1(FROM_DAYS, int, date);
  FUNC1(FROM_DAYS, int, time);
  FUNC1(FROM_DAYS, int, timestamp);
  FUNC1(FROM_DAYS, int, datetime);
  FUNC1(FROM_DAYS, varchar, date);
  FUNC1(FROM_DAYS, varchar, time);
  FUNC1(FROM_DAYS, varchar, timestamp);
  FUNC1(FROM_DAYS, varchar, datetime);
  FUNC1(MICROSECOND, int, date);
  FUNC1(MICROSECOND, int, time);
  FUNC1(MICROSECOND, int, timestamp);
  FUNC1(MICROSECOND, int, datetime);
  FUNC1(MICROSECOND, varchar, date);
  FUNC1(MICROSECOND, varchar, time);
  FUNC1(MICROSECOND, varchar, timestamp);
  FUNC1(MICROSECOND, varchar, datetime);
  FUNC1(TO_SECONDS, int, date);
  FUNC1(TO_SECONDS, int, timestamp);
  FUNC1(TO_SECONDS, int, datetime);
  FUNC1(TO_SECONDS, varchar, date);
  FUNC1(TO_SECONDS, varchar, time);
  FUNC1(TO_SECONDS, varchar, timestamp);
  FUNC1(TO_SECONDS, varchar, datetime);
  FUNC1(TIMESTAMP, int, date);
  FUNC1(TIMESTAMP, int, time);
  FUNC1(TIMESTAMP, int, timestamp);
  FUNC1(TIMESTAMP, int, datetime);
  FUNC1(TIMESTAMP, varchar, date);
  FUNC1(TIMESTAMP, varchar, time);
  FUNC1(TIMESTAMP, varchar, timestamp);
  FUNC1(TIMESTAMP, varchar, datetime);  
  FUNC1(SEC_TO_TIME, time, int);
  FUNC2(ADDDATE, date, date, int);
  FUNC2(ADDTIME, time, time, int);  
  FUNC2(ADDTIME, datetime, datetime, int); 
  FUNC2(DATEDIFF, int, datetime, datetime);
  FUNC2(MAKEDATE, date, year, int);  
  FUNC2(PERIOD_ADD, int, datetime, int);
  FUNC2(PERIOD_DIFF, int, datetime, int);  
  FUNC2(STR_TO_DATE, datetime, datetime, datetime);
  FUNC2(SUBDATE, date, datetime, int); 
  FUNC2(SUBTIME, datetime, datetime, time);
  FUNC2(SUBTIME, time, time, time);
  FUNC2(TIME_FORMAT, datetime, datetime, datetime);
  FUNC2(TIME_FORMAT, varchar, time, varchar);
  FUNC2(TIMEDIFF, int, datetime, datetime);
  FUNC3(MAKETIME, time, int, int, int); 

  //数学函数
  FUNC1(ABS, float, float);
  FUNC1(ABS, float, decimal);
  FUNC1(ABS, float, bigint);
  FUNC1(ABS, float, tinyint);
  FUNC1(ABS, float, int);
  FUNC1(ABS, decimal, float);
  FUNC1(ABS, decimal, decimal);
  FUNC1(ABS, decimal, bigint);
  FUNC1(ABS, decimal, tinyint);
  FUNC1(ABS, decimal, int);
  FUNC1(ABS, int, float);
  FUNC1(ABS, int, decimal);
  FUNC1(ABS, int, bigint);
  FUNC1(ABS, int, tinyint);
  FUNC1(ABS, int, int);
  FUNC1(ABS, varchar, float);
  FUNC1(ABS, varchar, decimal);
  FUNC1(ABS, varchar, bigint);
  FUNC1(ABS, varchar, tinyint);
  FUNC1(ABS, varchar, int);
  FUNC1(ACOS, float, float);
  FUNC1(ACOS, float, decimal);
  FUNC1(ACOS, float, bigint);
  FUNC1(ACOS, float, tinyint);
  FUNC1(ACOS, float, int);
  FUNC1(ACOS, decimal, float);
  FUNC1(ACOS, decimal, decimal);
  FUNC1(ACOS, decimal, bigint);
  FUNC1(ACOS, decimal, tinyint);
  FUNC1(ACOS, decimal, int);
  FUNC1(ACOS, int, float);
  FUNC1(ACOS, int, decimal);
  FUNC1(ACOS, int, bigint);
  FUNC1(ACOS, int, tinyint);
  FUNC1(ACOS, int, int);
  FUNC1(ACOS, varchar, float);
  FUNC1(ACOS, varchar, decimal);
  FUNC1(ACOS, varchar, bigint);
  FUNC1(ACOS, varchar, tinyint);
  FUNC1(ACOS, varchar, int);
  FUNC1(ASIN, float, float);
  FUNC1(ASIN, float, decimal);
  FUNC1(ASIN, float, bigint);
  FUNC1(ASIN, float, tinyint);
  FUNC1(ASIN, float, int);
  FUNC1(ASIN, decimal, float);
  FUNC1(ASIN, decimal, decimal);
  FUNC1(ASIN, decimal, bigint);
  FUNC1(ASIN, decimal, tinyint);
  FUNC1(ASIN, decimal, int);
  FUNC1(ASIN, int, float);
  FUNC1(ASIN, int, decimal);
  FUNC1(ASIN, int, bigint);
  FUNC1(ASIN, int, tinyint);
  FUNC1(ASIN, int, int);
  FUNC1(ASIN, varchar, float);
  FUNC1(ASIN, varchar, decimal);
  FUNC1(ASIN, varchar, bigint);
  FUNC1(ASIN, varchar, tinyint);
  FUNC1(ASIN, varchar, int);
  FUNC1(ATAN, float, float);
  FUNC1(ATAN, float, decimal);
  FUNC1(ATAN, float, bigint);
  FUNC1(ATAN, float, tinyint);
  FUNC1(ATAN, float, int);
  FUNC1(ATAN, decimal, float);
  FUNC1(ATAN, decimal, decimal);
  FUNC1(ATAN, decimal, bigint);
  FUNC1(ATAN, decimal, tinyint);
  FUNC1(ATAN, decimal, int);
  FUNC1(ATAN, int, float);
  FUNC1(ATAN, int, decimal);
  FUNC1(ATAN, int, bigint);
  FUNC1(ATAN, int, tinyint);
  FUNC1(ATAN, int, int);
  FUNC1(ATAN, varchar, float);
  FUNC1(ATAN, varchar, decimal);
  FUNC1(ATAN, varchar, bigint);
  FUNC1(ATAN, varchar, tinyint);
  FUNC1(ATAN, varchar, int);
  FUNC1(CEIL, float, float);
  FUNC1(CEIL, float, decimal);
  FUNC1(CEIL, float, bigint);
  FUNC1(CEIL, float, tinyint);
  FUNC1(CEIL, float, int);
  FUNC1(CEIL, decimal, float);
  FUNC1(CEIL, decimal, decimal);
  FUNC1(CEIL, decimal, bigint);
  FUNC1(CEIL, decimal, tinyint);
  FUNC1(CEIL, decimal, int);
  FUNC1(CEIL, int, float);
  FUNC1(CEIL, int, decimal);
  FUNC1(CEIL, int, bigint);
  FUNC1(CEIL, int, tinyint);
  FUNC1(CEIL, int, int);
  FUNC1(CEIL, varchar, float);
  FUNC1(CEIL, varchar, decimal);
  FUNC1(CEIL, varchar, bigint);
  FUNC1(CEIL, varchar, tinyint);
  FUNC1(CEIL, varchar, int);
  FUNC1(CEILING, float, float);
  FUNC1(CEILING, float, decimal);
  FUNC1(CEILING, float, bigint);
  FUNC1(CEILING, float, tinyint);
  FUNC1(CEILING, float, int);
  FUNC1(CEILING, decimal, float);
  FUNC1(CEILING, decimal, decimal);
  FUNC1(CEILING, decimal, bigint);
  FUNC1(CEILING, decimal, tinyint);
  FUNC1(CEILING, decimal, int);
  FUNC1(CEILING, int, float);
  FUNC1(CEILING, int, decimal);
  FUNC1(CEILING, int, bigint);
  FUNC1(CEILING, int, tinyint);
  FUNC1(CEILING, int, int);
  FUNC1(CEILING, varchar, float);
  FUNC1(CEILING, varchar, decimal);
  FUNC1(CEILING, varchar, bigint);
  FUNC1(CEILING, varchar, tinyint);
  FUNC1(CEILING, varchar, int);
  FUNC1(COS, float, float);
  FUNC1(COS, float, decimal);
  FUNC1(COS, float, bigint);
  FUNC1(COS, float, tinyint);
  FUNC1(COS, float, int);
  FUNC1(COS, decimal, float);
  FUNC1(COS, decimal, decimal);
  FUNC1(COS, decimal, bigint);
  FUNC1(COS, decimal, tinyint);
  FUNC1(COS, decimal, int);
  FUNC1(COS, int, float);
  FUNC1(COS, int, decimal);
  FUNC1(COS, int, bigint);
  FUNC1(COS, int, tinyint);
  FUNC1(COS, int, int);
  FUNC1(COS, varchar, float);
  FUNC1(COS, varchar, decimal);
  FUNC1(COS, varchar, bigint);
  FUNC1(COS, varchar, tinyint);
  FUNC1(COS, varchar, int);
  FUNC1(COT, float, float);
  FUNC1(COT, float, decimal);
  FUNC1(COT, float, bigint);
  FUNC1(COT, float, tinyint);
  FUNC1(COT, float, int);
  FUNC1(COT, decimal, float);
  FUNC1(COT, decimal, decimal);
  FUNC1(COT, decimal, bigint);
  FUNC1(COT, decimal, tinyint);
  FUNC1(COT, decimal, int);
  FUNC1(COT, int, float);
  FUNC1(COT, int, decimal);
  FUNC1(COT, int, bigint);
  FUNC1(COT, int, tinyint);
  FUNC1(COT, int, int);
  FUNC1(COT, varchar, float);
  FUNC1(COT, varchar, decimal);
  FUNC1(COT, varchar, bigint);
  FUNC1(COT, varchar, tinyint);
  FUNC1(COT, varchar, int);
  FUNC1(DEGREES, float, float);
  FUNC1(DEGREES, float, decimal);
  FUNC1(DEGREES, float, bigint);
  FUNC1(DEGREES, float, tinyint);
  FUNC1(DEGREES, float, int);
  FUNC1(DEGREES, decimal, float);
  FUNC1(DEGREES, decimal, decimal);
  FUNC1(DEGREES, decimal, bigint);
  FUNC1(DEGREES, decimal, tinyint);
  FUNC1(DEGREES, decimal, int);
  FUNC1(DEGREES, int, float);
  FUNC1(DEGREES, int, decimal);
  FUNC1(DEGREES, int, bigint);
  FUNC1(DEGREES, int, tinyint);
  FUNC1(DEGREES, int, int);
  FUNC1(DEGREES, varchar, float);
  FUNC1(DEGREES, varchar, decimal);
  FUNC1(DEGREES, varchar, bigint);
  FUNC1(DEGREES, varchar, tinyint);
  FUNC1(DEGREES, varchar, int);
  FUNC1(EXP, float, float);
  FUNC1(EXP, float, decimal);
  FUNC1(EXP, float, bigint);
  FUNC1(EXP, float, tinyint);
  FUNC1(EXP, float, int);
  FUNC1(EXP, decimal, float);
  FUNC1(EXP, decimal, decimal);
  FUNC1(EXP, decimal, bigint);
  FUNC1(EXP, decimal, tinyint);
  FUNC1(EXP, decimal, int);
  FUNC1(EXP, int, float);
  FUNC1(EXP, int, decimal);
  FUNC1(EXP, int, bigint);
  FUNC1(EXP, int, tinyint);
  FUNC1(EXP, int, int);
  FUNC1(EXP, varchar, float);
  FUNC1(EXP, varchar, decimal);
  FUNC1(EXP, varchar, bigint);
  FUNC1(EXP, varchar, tinyint);
  FUNC1(EXP, varchar, int);
  FUNC1(FLOOR, float, float);
  FUNC1(FLOOR, float, decimal);
  FUNC1(FLOOR, float, bigint);
  FUNC1(FLOOR, float, tinyint);
  FUNC1(FLOOR, float, int);
  FUNC1(FLOOR, decimal, float);
  FUNC1(FLOOR, decimal, decimal);
  FUNC1(FLOOR, decimal, bigint);
  FUNC1(FLOOR, decimal, tinyint);
  FUNC1(FLOOR, decimal, int);
  FUNC1(FLOOR, int, float);
  FUNC1(FLOOR, int, decimal);
  FUNC1(FLOOR, int, bigint);
  FUNC1(FLOOR, int, tinyint);
  FUNC1(FLOOR, int, int);
  FUNC1(FLOOR, varchar, float);
  FUNC1(FLOOR, varchar, decimal);
  FUNC1(FLOOR, varchar, bigint);
  FUNC1(FLOOR, varchar, tinyint);
  FUNC1(FLOOR, varchar, int);
  FUNC1(LN, float, float);
  FUNC1(LN, float, decimal);
  FUNC1(LN, float, bigint);
  FUNC1(LN, float, tinyint);
  FUNC1(LN, float, int);
  FUNC1(LN, decimal, float);
  FUNC1(LN, decimal, decimal);
  FUNC1(LN, decimal, bigint);
  FUNC1(LN, decimal, tinyint);
  FUNC1(LN, decimal, int);
  FUNC1(LN, int, float);
  FUNC1(LN, int, decimal);
  FUNC1(LN, int, bigint);
  FUNC1(LN, int, tinyint);
  FUNC1(LN, int, int);
  FUNC1(LN, varchar, float);
  FUNC1(LN, varchar, decimal);
  FUNC1(LN, varchar, bigint);
  FUNC1(LN, varchar, tinyint);
  FUNC1(LN, varchar, int);
  FUNC1(LOG, float, float);
  FUNC1(LOG, float, decimal);
  FUNC1(LOG, float, bigint);
  FUNC1(LOG, float, tinyint);
  FUNC1(LOG, float, int);
  FUNC1(LOG, decimal, float);
  FUNC1(LOG, decimal, decimal);
  FUNC1(LOG, decimal, bigint);
  FUNC1(LOG, decimal, tinyint);
  FUNC1(LOG, decimal, int);
  FUNC1(LOG, int, float);
  FUNC1(LOG, int, decimal);
  FUNC1(LOG, int, bigint);
  FUNC1(LOG, int, tinyint);
  FUNC1(LOG, int, int);
  FUNC1(LOG, varchar, float);
  FUNC1(LOG, varchar, decimal);
  FUNC1(LOG, varchar, bigint);
  FUNC1(LOG, varchar, tinyint);
  FUNC1(LOG, varchar, int);
  FUNC1(LOG10, float, float);
  FUNC1(LOG10, float, decimal);
  FUNC1(LOG10, float, bigint);
  FUNC1(LOG10, float, tinyint);
  FUNC1(LOG10, float, int);
  FUNC1(LOG10, decimal, float);
  FUNC1(LOG10, decimal, decimal);
  FUNC1(LOG10, decimal, bigint);
  FUNC1(LOG10, decimal, tinyint);
  FUNC1(LOG10, decimal, int);
  FUNC1(LOG10, int, float);
  FUNC1(LOG10, int, decimal);
  FUNC1(LOG10, int, bigint);
  FUNC1(LOG10, int, tinyint);
  FUNC1(LOG10, int, int);
  FUNC1(LOG10, varchar, float);
  FUNC1(LOG10, varchar, decimal);
  FUNC1(LOG10, varchar, bigint);
  FUNC1(LOG10, varchar, tinyint);
  FUNC1(LOG10, varchar, int);
  FUNC1(LOG2, float, float);
  FUNC1(LOG2, float, decimal);
  FUNC1(LOG2, float, bigint);
  FUNC1(LOG2, float, tinyint);
  FUNC1(LOG2, float, int);
  FUNC1(LOG2, decimal, float);
  FUNC1(LOG2, decimal, decimal);
  FUNC1(LOG2, decimal, bigint);
  FUNC1(LOG2, decimal, tinyint);
  FUNC1(LOG2, decimal, int);
  FUNC1(LOG2, int, float);
  FUNC1(LOG2, int, decimal);
  FUNC1(LOG2, int, bigint);
  FUNC1(LOG2, int, tinyint);
  FUNC1(LOG2, int, int);
  FUNC1(LOG2, varchar, float);
  FUNC1(LOG2, varchar, decimal);
  FUNC1(LOG2, varchar, bigint);
  FUNC1(LOG2, varchar, tinyint);
  FUNC1(LOG2, varchar, int);
  FUNC1(RADIANS, float, float);
  FUNC1(RADIANS, float, decimal);
  FUNC1(RADIANS, float, bigint);
  FUNC1(RADIANS, float, tinyint);
  FUNC1(RADIANS, float, int);
  FUNC1(RADIANS, decimal, float);
  FUNC1(RADIANS, decimal, decimal);
  FUNC1(RADIANS, decimal, bigint);
  FUNC1(RADIANS, decimal, tinyint);
  FUNC1(RADIANS, decimal, int);
  FUNC1(RADIANS, int, float);
  FUNC1(RADIANS, int, decimal);
  FUNC1(RADIANS, int, bigint);
  FUNC1(RADIANS, int, tinyint);
  FUNC1(RADIANS, int, int);
  FUNC1(RADIANS, varchar, float);
  FUNC1(RADIANS, varchar, decimal);
  FUNC1(RADIANS, varchar, bigint);
  FUNC1(RADIANS, varchar, tinyint);
  FUNC1(RADIANS, varchar, int);
  FUNC1(ROUND, float, float);
  FUNC1(ROUND, float, decimal);
  FUNC1(ROUND, float, bigint);
  FUNC1(ROUND, float, tinyint);
  FUNC1(ROUND, float, int);
  FUNC1(ROUND, decimal, float);
  FUNC1(ROUND, decimal, decimal);
  FUNC1(ROUND, decimal, bigint);
  FUNC1(ROUND, decimal, tinyint);
  FUNC1(ROUND, decimal, int);
  FUNC1(ROUND, int, float);
  FUNC1(ROUND, int, decimal);
  FUNC1(ROUND, int, bigint);
  FUNC1(ROUND, int, tinyint);
  FUNC1(ROUND, int, int);
  FUNC1(ROUND, varchar, float);
  FUNC1(ROUND, varchar, decimal);
  FUNC1(ROUND, varchar, bigint);
  FUNC1(ROUND, varchar, tinyint);
  FUNC1(ROUND, varchar, int);
  FUNC1(SIGN, float, float);
  FUNC1(SIGN, float, decimal);
  FUNC1(SIGN, float, bigint);
  FUNC1(SIGN, float, tinyint);
  FUNC1(SIGN, float, int);
  FUNC1(SIGN, decimal, float);
  FUNC1(SIGN, decimal, decimal);
  FUNC1(SIGN, decimal, bigint);
  FUNC1(SIGN, decimal, tinyint);
  FUNC1(SIGN, decimal, int);
  FUNC1(SIGN, int, float);
  FUNC1(SIGN, int, decimal);
  FUNC1(SIGN, int, bigint);
  FUNC1(SIGN, int, tinyint);
  FUNC1(SIGN, int, int);
  FUNC1(SIGN, varchar, float);
  FUNC1(SIGN, varchar, decimal);
  FUNC1(SIGN, varchar, bigint);
  FUNC1(SIGN, varchar, tinyint);
  FUNC1(SIGN, varchar, int);
  FUNC1(SIN, float, float);
  FUNC1(SIN, float, decimal);
  FUNC1(SIN, float, bigint);
  FUNC1(SIN, float, tinyint);
  FUNC1(SIN, float, int);
  FUNC1(SIN, decimal, float);
  FUNC1(SIN, decimal, decimal);
  FUNC1(SIN, decimal, bigint);
  FUNC1(SIN, decimal, tinyint);
  FUNC1(SIN, decimal, int);
  FUNC1(SIN, int, float);
  FUNC1(SIN, int, decimal);
  FUNC1(SIN, int, bigint);
  FUNC1(SIN, int, tinyint);
  FUNC1(SIN, int, int);
  FUNC1(SIN, varchar, float);
  FUNC1(SIN, varchar, decimal);
  FUNC1(SIN, varchar, bigint);
  FUNC1(SIN, varchar, tinyint);
  FUNC1(SIN, varchar, int);
  FUNC1(SQRT, float, float);
  FUNC1(SQRT, float, decimal);
  FUNC1(SQRT, float, bigint);
  FUNC1(SQRT, float, tinyint);
  FUNC1(SQRT, float, int);
  FUNC1(SQRT, decimal, float);
  FUNC1(SQRT, decimal, decimal);
  FUNC1(SQRT, decimal, bigint);
  FUNC1(SQRT, decimal, tinyint);
  FUNC1(SQRT, decimal, int);
  FUNC1(SQRT, int, float);
  FUNC1(SQRT, int, decimal);
  FUNC1(SQRT, int, bigint);
  FUNC1(SQRT, int, tinyint);
  FUNC1(SQRT, int, int);
  FUNC1(SQRT, varchar, float);
  FUNC1(SQRT, varchar, decimal);
  FUNC1(SQRT, varchar, bigint);
  FUNC1(SQRT, varchar, tinyint);
  FUNC1(SQRT, varchar, int);
  FUNC1(TAN, float, float);
  FUNC1(TAN, float, decimal);
  FUNC1(TAN, float, bigint);
  FUNC1(TAN, float, tinyint);
  FUNC1(TAN, float, int);
  FUNC1(TAN, decimal, float);
  FUNC1(TAN, decimal, decimal);
  FUNC1(TAN, decimal, bigint);
  FUNC1(TAN, decimal, tinyint);
  FUNC1(TAN, decimal, int);
  FUNC1(TAN, int, float);
  FUNC1(TAN, int, decimal);
  FUNC1(TAN, int, bigint);
  FUNC1(TAN, int, tinyint);
  FUNC1(TAN, int, int);
  FUNC1(TAN, varchar, float);
  FUNC1(TAN, varchar, decimal);
  FUNC1(TAN, varchar, bigint);
  FUNC1(TAN, varchar, tinyint);
  FUNC1(TAN, varchar, int);
  FUNC1(CRC32, varchar, varchar);
  FUNC1(HEX, varchar, varchar);
  FUNC1(HEX, varchar, int);
  FUNC2(MOD, int, int, int);
  FUNC2(POW, bigint, int, int);
  FUNC2(POWER, bigint, int, int);
  FUNC2(POWER, float, float, int);
  FUNC2(ADDDATE, date, date, int);
  FUNC2(FORMAT, float, float, int);
  FUNC3(CONV, varchar, varchar, int, int);
  FUNC3(CONV, varchar, int, int, int);
  
#define AGG(n,r, a) do {                                                \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    register_aggregate(proc);                                           \
  } while(0)
// 新增：零参与双参聚合注册宏，用于窗口排名/值函数
#define AGG0(n,r) do {                                                   \
    routine proc("", "", sqltype::get(#r), #n);                         \
    register_aggregate(proc);                                           \
  } while(0)
#define AGG2(n,r,a,b) do {                                               \
    routine proc("", "", sqltype::get(#r), #n);                         \
    proc.argtypes.push_back(sqltype::get(#a));                          \
    proc.argtypes.push_back(sqltype::get(#b));                          \
    register_aggregate(proc);                                           \
  } while(0)

  AGG(AVG, int, int);
  AGG(AVG, int, float);
  AGG(AVG, int, varchar);
  AGG(AVG, int, datetime);
  AGG(AVG, varchar, int);
  AGG(AVG, varchar, float);
  AGG(AVG, varchar, varchar);
  AGG(AVG, varchar, datetime);
  AGG(BIT_AND, int, int);
  AGG(BIT_AND, int, float);
  AGG(BIT_AND, int, varchar);
  AGG(BIT_AND, int, datetime);
  AGG(BIT_AND, varchar, int);
  AGG(BIT_AND, varchar, float);
  AGG(BIT_AND, varchar, varchar);
  AGG(BIT_AND, varchar, datetime);
  AGG(BIT_OR, int, int);
  AGG(BIT_OR, int, float);
  AGG(BIT_OR, int, varchar);
  AGG(BIT_OR, int, datetime);
  AGG(BIT_OR, varchar, int);
  AGG(BIT_OR, varchar, float);
  AGG(BIT_OR, varchar, varchar);
  AGG(BIT_OR, varchar, datetime);
  AGG(BIT_XOR, int, int);
  AGG(BIT_XOR, int, float);
  AGG(BIT_XOR, int, varchar);
  AGG(BIT_XOR, int, datetime);
  AGG(BIT_XOR, varchar, int);
  AGG(BIT_XOR, varchar, float);
  AGG(BIT_XOR, varchar, varchar);
  AGG(BIT_XOR, varchar, datetime);
  AGG(COUNT, int, int);
  AGG(COUNT, int, float);
  AGG(COUNT, int, varchar);
  AGG(COUNT, int, datetime);
  AGG(COUNT, varchar, int);
  AGG(COUNT, varchar, float);
  AGG(COUNT, varchar, varchar);
  AGG(COUNT, varchar, datetime);
  AGG(GROUP_CONCAT, int, int);
  AGG(GROUP_CONCAT, int, float);
  AGG(GROUP_CONCAT, int, varchar);
  AGG(GROUP_CONCAT, int, datetime);
  AGG(GROUP_CONCAT, varchar, int);
  AGG(GROUP_CONCAT, varchar, float);
  AGG(GROUP_CONCAT, varchar, varchar);
  AGG(GROUP_CONCAT, varchar, datetime);
  AGG(MAX, int, int);
  AGG(MAX, int, float);
  AGG(MAX, int, varchar);
  AGG(MAX, int, datetime);
  AGG(MAX, varchar, int);
  AGG(MAX, varchar, float);
  AGG(MAX, varchar, varchar);
  AGG(MAX, varchar, datetime);
  AGG(MIN, int, int);
  AGG(MIN, int, float);
  AGG(MIN, int, varchar);
  AGG(MIN, int, datetime);
  AGG(MIN, varchar, int);
  AGG(MIN, varchar, float);
  AGG(MIN, varchar, varchar);
  AGG(MIN, varchar, datetime);
  AGG(STD, int, int);
  AGG(STD, int, float);
  AGG(STD, int, varchar);
  AGG(STD, int, datetime);
  AGG(STD, varchar, int);
  AGG(STD, varchar, float);
  AGG(STD, varchar, varchar);
  AGG(STD, varchar, datetime);
  AGG(STDDEV, int, int);
  AGG(STDDEV, int, float);
  AGG(STDDEV, int, varchar);
  AGG(STDDEV, int, datetime);
  AGG(STDDEV, varchar, int);
  AGG(STDDEV, varchar, float);
  AGG(STDDEV, varchar, varchar);
  AGG(STDDEV, varchar, datetime);
  AGG(STDDEV_POP, int, int);
  AGG(STDDEV_POP, int, float);
  AGG(STDDEV_POP, int, varchar);
  AGG(STDDEV_POP, int, datetime);
  AGG(STDDEV_POP, varchar, int);
  AGG(STDDEV_POP, varchar, float);
  AGG(STDDEV_POP, varchar, varchar);
  AGG(STDDEV_POP, varchar, datetime);
  AGG(STDDEV_SAMP, int, int);
  AGG(STDDEV_SAMP, int, float);
  AGG(STDDEV_SAMP, int, varchar);
  AGG(STDDEV_SAMP, int, datetime);
  AGG(STDDEV_SAMP, varchar, int);
  AGG(STDDEV_SAMP, varchar, float);
  AGG(STDDEV_SAMP, varchar, varchar);
  AGG(STDDEV_SAMP, varchar, datetime);
  AGG(SUM, int, int);
  AGG(SUM, int, float);
  AGG(SUM, int, varchar);
  AGG(SUM, int, datetime);
  AGG(SUM, varchar, int);
  AGG(SUM, varchar, float);
  AGG(SUM, varchar, varchar);
  AGG(SUM, varchar, datetime);
  AGG(VAR_POP, int, int);
  AGG(VAR_POP, int, float);
  AGG(VAR_POP, int, varchar);
  AGG(VAR_POP, int, datetime);
  AGG(VAR_POP, varchar, int);
  AGG(VAR_POP, varchar, float);
  AGG(VAR_POP, varchar, varchar);
  AGG(VAR_POP, varchar, datetime);
  AGG(VAR_SAMP, int, int);
  AGG(VAR_SAMP, int, float);
  AGG(VAR_SAMP, int, varchar);
  AGG(VAR_SAMP, int, datetime);
  AGG(VAR_SAMP, varchar, int);
  AGG(VAR_SAMP, varchar, float);
  AGG(VAR_SAMP, varchar, varchar);
  AGG(VAR_SAMP, varchar, datetime);
  AGG(VARIANCE, int, int);
  AGG(VARIANCE, int, float);
  AGG(VARIANCE, int, varchar);
  AGG(VARIANCE, int, datetime);
  AGG(VARIANCE, varchar, int);
  AGG(VARIANCE, varchar, float);
  AGG(VARIANCE, varchar, varchar);
  AGG(VARIANCE, varchar, datetime);

  // ===== Window functions (MySQL 8.0) — 注册为“可窗口化的聚合/窗口值函数” =====
  // 排名/分布：无参窗口函数（不追加 *），返回浮点
  AGG0(CUME_DIST, float);
  AGG0(PERCENT_RANK, float);
  // 分桶：NTILE(n) 返回整数，n 为正整数（2..128），在打印阶段强制范围
  AGG(NTILE, int, int);
  // 值函数：返回与输入同型
  AGG(FIRST_VALUE, int, int);
  AGG(FIRST_VALUE, float, float);
  AGG(FIRST_VALUE, varchar, varchar);
  AGG(FIRST_VALUE, datetime, datetime);
  AGG(LAST_VALUE, int, int);
  AGG(LAST_VALUE, float, float);
  AGG(LAST_VALUE, varchar, varchar);
  AGG(LAST_VALUE, datetime, datetime);
  // NTH_VALUE(expr, n)：第二参数限制为整数（打印阶段强制 1..128）
  AGG2(NTH_VALUE, int, int, int);
  AGG2(NTH_VALUE, float, float, int);
  AGG2(NTH_VALUE, varchar, varchar, int);
  AGG2(NTH_VALUE, datetime, datetime, int);

  // ===== JSON functions (MySQL 8.0) =====
  // Constructors
  FUNC2(JSON_OBJECT, json, varchar, varchar); // key, value
  FUNC4(JSON_OBJECT, json, varchar, varchar, varchar, varchar);
  FUNC2(JSON_ARRAY, json, varchar, varchar);
  FUNC4(JSON_ARRAY, json, varchar, varchar, varchar, varchar);
  FUNC1(JSON_QUOTE, varchar, varchar);
  // Accessors / Modifiers / Predicates / Info
  FUNC2(JSON_EXTRACT, json, json, varchar);
  FUNC3(JSON_SET, json, json, varchar, varchar);
  FUNC2(JSON_REMOVE, json, json, varchar);
  FUNC3(JSON_INSERT, json, json, varchar, varchar);
  FUNC3(JSON_REPLACE, json, json, varchar, varchar);
  FUNC2(JSON_MERGE_PATCH, json, json, json);
  FUNC2(JSON_MERGE_PRESERVE, json, json, json);
  FUNC2(JSON_OVERLAPS, int, json, json);
  FUNC2(JSON_CONTAINS, int, json, json);
  FUNC2(JSON_CONTAINS_PATH, int, json, varchar);
  FUNC1(JSON_UNQUOTE, varchar, varchar);
  FUNC1(JSON_TYPE, varchar, json);
  FUNC1(JSON_PRETTY, varchar, json);
  FUNC1(JSON_STORAGE_SIZE, int, json);
  FUNC1(JSON_VALID, int, json);
  FUNC1(JSON_VALID, int, varchar); // also string input
  FUNC1(JSON_DEPTH, int, json);
  FUNC1(JSON_KEYS, json, json);
  FUNC2(JSON_KEYS, json, json, varchar);
  FUNC1(JSON_LENGTH, int, json);
  FUNC2(JSON_LENGTH, int, json, varchar);
  FUNC2(JSON_SEARCH, varchar, json, varchar);
  FUNC2(JSON_SCHEMA_VALID, int, json, json);
  FUNC2(JSON_SCHEMA_VALIDATION_REPORT, json, json, json);

  // Aggregates
  AGG(JSON_ARRAYAGG, json, varchar);
  { routine proc("", "", sqltype::get("json"), "JSON_OBJECTAGG"); proc.argtypes.push_back(sqltype::get("varchar")); proc.argtypes.push_back(sqltype::get("varchar")); register_aggregate(proc); }

  // ===== Spatial functions (MySQL 8.0) =====
  // Constructors
  FUNC2(ST_GeomFromText, geometry, varchar, int);
  FUNC2(ST_GeomFromWKB, geometry, varchar, int);
  FUNC2(ST_GeomFromGeoJSON, geometry, varchar, int);
  FUNC3(ST_GeomFromGeoJSON, geometry, varchar, int, int);
  FUNC2(Point, geometry, float, float);
  // Outputs
  FUNC1(ST_AsText, varchar, geometry);
  FUNC1(ST_AsWKB, varchar, geometry);
  FUNC1(ST_AsGeoJSON, varchar, geometry);
  // Measurements / Accessors
  FUNC1(ST_SRID, int, geometry);
  FUNC2(ST_SRID, geometry, geometry, int);
  FUNC1(ST_IsValid, int, geometry);
  FUNC1(ST_IsSimple, int, geometry);
  FUNC1(ST_Area, float, geometry);
  FUNC1(ST_Length, float, geometry);
  FUNC1(ST_NumPoints, int, geometry);
  FUNC2(ST_PointN, geometry, geometry, int);
  FUNC1(ST_Dimension, int, geometry);
  FUNC1(ST_ExteriorRing, geometry, geometry);
  FUNC1(ST_NumInteriorRings, int, geometry);
  FUNC1(ST_StartPoint, geometry, geometry);
  FUNC1(ST_EndPoint, geometry, geometry);
  FUNC2(ST_GeometryN, geometry, geometry, int);
  // Relations / Predicates
  FUNC2(ST_Contains, int, geometry, geometry);
  FUNC2(ST_Within, int, geometry, geometry);
  FUNC2(ST_Intersects, int, geometry, geometry);
  FUNC2(ST_Touches, int, geometry, geometry);
  FUNC2(ST_Crosses, int, geometry, geometry);
  FUNC2(ST_Overlaps, int, geometry, geometry);
  FUNC2(ST_Disjoint, int, geometry, geometry);
  FUNC2(ST_Equals, int, geometry, geometry);
  // Operations
  FUNC2(ST_Buffer, geometry, geometry, float);
  FUNC1(ST_Envelope, geometry, geometry);

  booltype = sqltype::get("tinyint");
  inttype = sqltype::get("int");
  types.push_back(booltype);
  types.push_back(inttype);       

  internaltype = sqltype::get("internal");
  arraytype = sqltype::get("ARRAY");
  types.push_back(internaltype);
  types.push_back(arraytype);

  generate_indexes();

}

dut_mysql::dut_mysql(const std::string &conninfo)
  : mysql_connection(conninfo)
{

}

void dut_mysql::test(const std::string &stmt)
{
  if (mysql_query(con, stmt.c_str())) {
    const char *msg = mysql_error(con);
    const char *sqlstate = mysql_sqlstate(con);
    int myerrno = mysql_errno(con);
    switch(myerrno) {
    case 1149:
    case 1064:
         throw dut::syntax(msg, sqlstate);
      break;
    case 2006:
         throw dut::broken(msg, sqlstate);
    default:
      if (regex_match(msg, e_syntax))
        throw dut::syntax(msg, sqlstate);
      else
        throw dut::failure(msg, sqlstate);
    }
  }
  MYSQL_RES *result = mysql_store_result(con);
  mysql_free_result(result);    
}

// 一次性采样初始化：在加载完表/列后调用；多次调用将直接跳过
void schema_mysql::init_samples(int sample_limit_or_minus1) {
  if (samples_initialized) {
    std::cerr << "[SchemaMySQL] init_samples skip (already initialized)" << std::endl;
    return;
  }
  std::cerr << "[SchemaMySQL] init_samples begin" << std::endl;
  base_samples_cache.load_base_samples(*this, sample_limit_or_minus1);
  samples_initialized = true;
  std::cerr << "[SchemaMySQL] init_samples end" << std::endl;
}
