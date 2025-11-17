#pragma once
#include <string>

namespace setops {

enum class SetOpType {
  UNION_DISTINCT,
  UNION_ALL,
  INTERSECT_DISTINCT,
  EXCEPT_DISTINCT
};

struct Flags {
  bool enable_union = false;
  bool enable_union_all = false;
  bool enable_intersect = false;
  bool enable_except = false;
  double prob = 0.0; // 触发概率（与既有谓词同级）
};

// 入口：在生成最终 SQL 前尝试包装为集合表达式；失败回退原始 SELECT
// 参数：
// - mysql_mode: 仅在 mysql 模式下启用集合操作
// - flags: 集合操作开关与概率
// - base_sql: 已生成的标准 SELECT 语句（大写关键字）
// - force_outer_order: 若开启，则在集合表达式最外层附加 ORDER BY 1,2,...
// 返回：集合表达式 SQL 或原始 base_sql
std::string maybe_apply_set_ops(bool mysql_mode, const Flags& flags,
                                const std::string& base_sql,
                                bool force_outer_order);

} // namespace setops
