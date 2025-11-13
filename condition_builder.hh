/// @file
/// @brief ConditionBuilder：基于 AST 的条件构造器，按类型守卫在 WHERE / JOIN ON / HAVING 上下文插入条件子树。
/// - 不做字符串拼接；所有新增条件均为 AST 节点，由原序列化器输出。
/// - 复杂类型按安全清单限制（JSON/空间/blob/bit/时间/枚举）。
#ifndef CONDITION_BUILDER_HH
#define CONDITION_BUILDER_HH

#include <string>
#include <vector>
#include <memory>

#include "expr.hh"
#include "grammar.hh"
#include "random.hh"
#include "value_catalog.hh"

// 上下文枚举
enum class CondContext { WHERE, HAVING, JOIN_ON };

// 列引用封装：用于精准选定“别名.列”
struct ColumnRef {
  named_relation* rel;
  column col;
  ColumnRef(named_relation* r, const column& c) : rel(r), col(c) {}
};

// 子句扩展（最小实现）：目前仅用于避免 limit 重复；其它子句（group by/having/order by）保持占位
struct ClauseExtras {
  bool avoid_limit_duplicate = false;
};

struct ConditionBuilder {
  // 构建统计（仅用于日志，不参与语义）
  struct BuildStats {
    int k = 0;                // 请求的总谓词数
    int quota = 0;            // 基础表列配额（基类谓词数）
    int extras = 0;           // 附加谓词数（非基础表列）
    int extras_tagged = 0;    // 已在日志中标注的附加谓词数
    int candidates = 0;       // 作用域候选列数
    int physical = 0;         // 物理表候选列数
    int whitelist_hits = 0;   // 白名单命中数（此实现按物理列计）
    int type_guard_ok = 0;    // 类型守卫通过数
    int gate = 0;             // d100 门控值
    double base_weight = 0.0; // 基础权重
    double density = -1.0;    // 独立密度（未提供时为 -1）
    // [约束计数，仅日志]
    int const_bool_count = 0; // 作用域内常量 true/false 条件计数
    int is_null_count = 0;    // 作用域内 IS NULL/IS NOT NULL 条件计数
    int has_column_predicate = 0; // 是否存在至少一个列条件（0/1）
  };

  // 最近一次构建统计（供 ConditionGenerator 打印汇总）
  const BuildStats& get_last_stats() const { return last_stats_; }
  
private:
  BuildStats last_stats_;
public:
  // 针对列生成单条谓词（AST 节点）；失败时返回空指针
  std::shared_ptr<bool_expr> build_predicate_for_column(prod* pctx, const ColumnRef& ref, CondContext ctx, const ValueCatalog& vc);

  // 在作用域 refs 中选列（only_physical=true 时仅选择物理表列）
  std::vector<ColumnRef> pick_scope_columns(const std::vector<std::shared_ptr<named_relation>>& scope_refs, bool only_physical);

  // 基于作用域生成 count 条谓词，并以 base_table_weight 权重优先选择物理表列；附加项按 10% 概率加入
  std::shared_ptr<bool_expr> build_predicates_for_scope(prod* pctx, const std::vector<std::shared_ptr<named_relation>>& scope_refs,
                                                       CondContext ctx, const ValueCatalog& vc, int count, double base_table_weight);
  // 重载：JOIN joinscope.refs 为原始指针数组
  std::shared_ptr<bool_expr> build_predicates_for_scope(prod* pctx, const std::vector<named_relation*>& raw_refs,
                                                       CondContext ctx, const ValueCatalog& vc, int count, double base_table_weight);

  // 组合 AND/OR（1~3 条），沿用当前 d6 分布；返回复合 AST
  std::shared_ptr<bool_expr> compose_predicates(prod* pctx, const std::vector<std::shared_ptr<bool_expr>>& preds);

  // 构造子句扩展占位信息
  ClauseExtras build_extras(const std::vector<std::shared_ptr<named_relation>>& refs, bool avoid_limit_duplicate);
};

#endif

// ---- 辅助自由函数声明（供 sqlsmith.cc 调用） ----
namespace condgen_utils {
  // 语句级清理：移除当前语句键下的 IN 集合指纹（ConditionBuilder 路径）
  void clear_inset_registry_for_stmt_key(const void* key);
}
