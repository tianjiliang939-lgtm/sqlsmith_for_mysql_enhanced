/// @file
/// @brief ConditionGenerator（AST 语义版）：持有 ValueCatalog 与 ConditionBuilder，
/// 在不同上下文插入条件子树，由原序列化器输出（仅 MySQL 模式，默认关闭）。
#ifndef CONDITION_GENERATOR_HH
#define CONDITION_GENERATOR_HH

#include <vector>
#include <memory>
#include <string>

#include "schema.hh"
#include "mysql.hh"
#include "grammar.hh"
#include "expr.hh"
#include "value_catalog.hh"
#include "condition_builder.hh"
#include "temp_dataset_adapter.hh"

// 轻量的表引用集合类型：来自 from_clause 中的别名化关系引用
using table_ref_set = std::vector<std::shared_ptr<named_relation>>;

struct ConditionGenerator {
  // 依赖：ValueCatalog 与 ConditionBuilder
  ValueCatalog vc_;
  ConditionBuilder cb_;
  schema* schema_ = nullptr;

  // 初始化：加载基表样本（K<0 采全集）
  void init_sampling(schema_mysql& sch, int sample_limit_or_minus1);

  // 在 JOIN ON / WHERE / HAVING 上下文插入条件子树
  // 接口统一：移除未定义 RNG 类型，改用全局 d6/d100，保持开关关闭时不消耗 RNG
  void attach_to_on(struct query_spec& q);
  void attach_to_where(struct query_spec& q);
  void attach_to_having(struct query_spec& q);

  // 递归应用到子查询：对当前 q 执行插入，并探查 EXISTS 子树递归调用
  void apply_recursively(struct query_spec& q);

  ConditionGenerator(schema* sch) : schema_(sch) {
    // 注入基表样本缓存（schema_mysql 持有），本地仅写入临时数据集样本
    if (schema_ && schema_->mysql_mode) {
#ifdef HAVE_LIBMYSQLCLIENT
      if (auto sm = dynamic_cast<schema_mysql*>(schema_)) {
        vc_.set_fallback(&sm->base_samples_cache);
      }
#endif
    }
    // 日志开关：受 sch.condgen_debug 或 sch.verbose 控制
    if (schema_) {
      vc_.set_debug(schema_->condgen_debug || schema_->verbose);
      vc_.set_schema(schema_);
    }
  }

private:
  void apply_recursively_impl(struct query_spec& q);
  // 强制保证 HAVING 至少包含一条“物理列 + 缓存值”谓词；仅在 has_agg 时调用
  bool ensure_having_physical_cached_predicate(struct query_spec& q);
  bool ensure_having_count_fallback(struct query_spec& q);
};

#endif // CONDITION_GENERATOR_HH
