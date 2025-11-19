/// @file
/// @brief MySQL 8.0 临时数据集 provider：基于 VALUES ... ROW(...) 与 TABLE ... 语法生成派生表/数据源
/// @details
/// - 随机生成 1~10 列、1~50 行，覆盖常见 MySQL 数据类型；注入 NULL/空/边界值
/// - 空间类型统一使用 SRID=4326（ST_GeomFromText(..., 4326) 或 ST_SRID(..., 4326)）
/// - 片段可嵌入 CTE（WITH）、FROM 派生表、子查询、JOIN ... ON ...、EXISTS
/// - VALUES 派生表强制列别名 "AS <alias>(c1,...)"，确保列数一致；对不兼容类型做 CAST 或回退
/// - TABLE 语法：优先选择真实表，若不可用则回退为 VALUES 派生表
#ifndef TEMP_DATASET_HH
#define TEMP_DATASET_HH

#include <string>
#include <vector>
#include <memory>
#include <ostream>
#include "grammar.hh"
#include "random.hh"
#include "relmodel.hh"

struct temp_dataset_ref : table_ref {
  // 构造与输出
  temp_dataset_ref(prod *p);
  virtual ~temp_dataset_ref() { }
  virtual void out(std::ostream &out);

  // 配置与生成结果
  bool use_values; // true: 输出 VALUES 派生表；false: 输出 TABLE 语法
  std::string alias; // 派生表别名
  relation values_rel; // 当 use_values=true 时列方案
  std::vector<std::string> col_names; // 列名列表（c1..cn）
  std::vector<std::string> col_types; // 列类型名称（int/varchar/...）
  std::vector<std::vector<std::string>> rows; // 行值：每行一个表达式列表（已按类型生成）

  // TABLE 模式选中表
  table *chosen_table = nullptr;

private:
  void decide_mode(); // 选择 VALUES 或 TABLE
  void build_values_schema(); // 生成列方案（1..10 列）
  void fill_values_rows();    // 生成行值（1..50 行）
  // 生成某类型的安全字面量/表达式
  std::string gen_value_for_type(const std::string &tname);
  // 简单工具
  static inline std::string q(const std::string &s) { return std::string("'") + s + "'"; }
};

#endif
