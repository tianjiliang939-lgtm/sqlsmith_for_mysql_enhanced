#ifndef MYSQL_HH
#define MYSQL_HH

#include "config.h"
#include <string>
#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"
#include "value_catalog.hh"

#ifdef HAVE_LIBMYSQLCLIENT
extern "C" {
// #define bool_defined
// #include <my_global.h>
#include <mysql/mysql.h>
}
#else
struct MYSQL; // forward declaration to allow compilation without MySQL headers
#endif

struct mysql_connection {
  MYSQL *con;
  mysql_connection(const std::string &conninfo);
  virtual ~mysql_connection();
  void q(std::string s);
  void error();
};

struct schema_mysql : schema, mysql_connection {
  schema_mysql(const std::string &conninfo);
  // 声明 register_routine 保持声明/定义一致
  void register_routine(routine& r) override;
  // 聚合注册过滤：仅 numeric→numeric；JSON/GROUP 聚合禁止空间入参
  void register_aggregate(routine& r) override; 
  virtual std::string quote_name(const std::string &id) {
    return "\"" + id + "\"";
  }
  // 基表样本缓存（一次性初始化）
  ValueCatalog base_samples_cache;
  bool samples_initialized = false;
  // 一次性采样初始化（K<0 表示不加 LIMIT 采全集）
  void init_samples(int sample_limit_or_minus1);
};

struct dut_mysql : dut_base, mysql_connection {
  virtual void test(const std::string &stmt);
  dut_mysql(const std::string &conninfo);
};

#endif
