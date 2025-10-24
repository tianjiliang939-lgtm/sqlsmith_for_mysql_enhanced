/// @file
/// @brief Base class providing schema information to grammar

#ifndef SCHEMA_HH
#define SCHEMA_HH

#include <string>
#include <iostream>
#ifdef HAVE_POSTGRESQL
#include <pqxx/pqxx>
#endif
#include <numeric>
#include <memory>

#include "relmodel.hh"
#include "random.hh"

// 新增特性承载结构体：CLI 开关映射到生成层
struct schema_features {
  // 窗口函数总开关
  bool window_enabled = true;
  // 聚合窗口函数开关（受 window_enabled 约束）
  bool window_agg_enabled = true;
  // 临时数据集 provider（VALUES/TABLE/JSON_TABLE）总开关（由 --enable-temp-set 控制，默认 OFF）
  bool temp_dataset_enabled = false; 
  // 新增：统一位置序号 ORDER BY 开关（默认关闭）
  bool force_order_by = false; 
  // 新增：恒假条件开关（默认关闭）
  bool allow_false_cond = false;     
  // 新增：禁用不确定系统函数（默认关闭）
  bool disable_nondeterministic_funcs = false; 
  // 新增：CASE 类型一致性严格检查（默认关闭）
  bool strict_case_type_check = false; 
  // 全文检索开关与概率（默认关闭，概率 0.8）
  bool fulltext_enable = false;
  double fulltext_prob = 0.8;
  // 语句类型开关（默认仅 select 开启；当 CLI 显式提供任意 enable-* 时改为按显式集合）
  bool stmt_enable_select = false;
  bool stmt_enable_insert = false;
  // 控制 delete_returning
  bool stmt_enable_delete = false;      
  bool stmt_enable_merge  = false;
  bool stmt_enable_upsert = false;
  // 控制 update_returning
  bool stmt_enable_update = false;      
  // DML RETURNING 控制（默认关闭；支持全局与按类型开关）
  bool ret_enable_global = false;
  // --enable-insert-returning
  bool ret_enable_insert = false; 
  // --enable-upsert-returning
  bool ret_enable_upsert = false; 
  // --enable-merge-returning
  bool ret_enable_merge  = false;       
  // 排除特定系统函数（FOUND_ROWS）
  bool exclude_found_rows = false;      
  // LATERAL 开关（默认关闭；仅支持方言下生效）
  bool lateral_enabled = false;          
};

struct schema {
  // 扩展配置：JSON/Spatial/CTE 开关与密度、SRID 集合
  bool enable_json = false;
  bool enable_spatial = false;
  bool enable_cte = false;
  // 用户自定义 hint（默认空字符串；非空时插入在最外层 SELECT 后）
  std::string customer_hint;
  // MySQL 特殊模式：禁用 LATERAL、限制窗口聚合（JSON_ARRAYAGG/JSON_OBJECTAGG 不允许 OVER）
  bool mysql_mode = false;
  double json_density = 0.2; // 0..1
  double spatial_density = 0.2; // 0..1
  std::vector<int> srid_set;
  std::map<std::string,int> column_srid;

  // 条件生成与日志选项（默认关闭，不改变原始行为）,独立调试开关（或受 --verbose 控制）
  bool condgen_debug = false;     
  // 由 CLI --verbose 映射
  bool verbose = false;
  // 0..1，显式提供时控制附加条件比例；<0 表示未提供，保持现状
  double extra_conds_density = -1.0; 

  // 特性承载，用于 CLI → 生成层的统一 wiring
  schema_features feature;

  sqltype *booltype;
  sqltype *inttype;
  sqltype *internaltype;
  sqltype *arraytype;

  std::vector<sqltype *> types;
  
  std::vector<table> tables;
  std::vector<op> operators;
  std::vector<routine> routines;
  std::vector<routine> aggregates;

  typedef std::tuple<sqltype *,sqltype *,sqltype *> typekey;
  std::multimap<typekey, op> index;
  typedef std::multimap<typekey, op>::iterator op_iterator;

  std::map<sqltype*, std::vector<routine*>>  routines_returning_type;
  std::map<sqltype*, std::vector<routine*>>  aggregates_returning_type;
  std::map<sqltype*, std::vector<routine*>>  parameterless_routines_returning_type;
  std::map<sqltype*, std::vector<table*>> tables_with_columns_of_type;
  std::map<sqltype*, std::vector<op*>> operators_returning_type;
  std::map<sqltype*, std::vector<sqltype*>> concrete_type;
  std::vector<table*> base_tables;

  string version;
  int version_num; 

  const char *true_literal = "true";
  const char *false_literal = "false";

  // 统计窗口聚合处理事件（仅 MySQL 模式下使用）
  // 不兼容窗口聚合被过滤的次数
  long window_agg_filtered = 0; 
  // 不兼容窗口聚合降级为普通聚合的次数
  long window_agg_downgraded = 0; 
  // 不兼容窗口聚合替换为兼容窗口聚合的次数
  long window_agg_replaced = 0;   
  
  virtual std::string quote_name(const std::string &id) = 0;
  
  void summary() {
    std::cout << "Found " << tables.size() <<
      " user table(s) in information schema." << std::endl;
  }
  void fill_scope(struct scope &s) {
    for (auto &t : tables)
      s.tables.push_back(&t);
    s.schema = this;
  }
  virtual void register_operator(op& o) {
    operators.push_back(o);
    typekey t(o.left, o.right, o.result);
    index.insert(std::pair<typekey,op>(t,o));
  }
  virtual void register_routine(routine& r) {
    routines.push_back(r);
  }
  virtual void register_aggregate(routine& r) {
    aggregates.push_back(r);
  }
  virtual op_iterator find_operator(sqltype *left, sqltype *right, sqltype *res) {
    typekey t(left, right, res);
    auto cons = index.equal_range(t);
    if (cons.first == cons.second)
      return index.end();
    else
      return random_pick<>(cons.first, cons.second);
  }
  schema() { }
  // 用于方言构造完成后构建类型/例程/表的索引视图；当前为安全最小实现（no-op/轻量遍历）
  void generate_indexes();
};

#endif
