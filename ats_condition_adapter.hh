/// @file
/// @brief ATS 条件适配模块（仅 MySQL 模式，默认关闭，最小侵入）。
///
/// 设计目标：
/// - 当 ATS 可用（HAVE_ATS）时，提供示例性事务钩子说明，读取开关与 K 值，执行 DISTINCT 采样，解析 FROM 别名，建立列与样本映射，
///   并在 AST 层插入条件，最终仍由原序列化器输出。
/// - 当 ATS 不可用时，提供同名类与方法的降级实现（可编译、空操作或记录），接口一致；内部仅调用现有 ValueCatalog/ConditionBuilder
///   验证映射流程与 AST 注入能力，不改变原生成逻辑；开关关闭时不消耗 RNG，保证 seed 复现。
/// - 列与样本数据对应保证：引入 AliasResolver 与 ColumnBinding，将别名列还原到“真实表.列”，在 ValueCatalog 中命中样本（temp 优先、base 回退），
///   映射失败时降级为安全条件（IS [NOT] NULL）或移除该条件。
#pragma once

#include "config.h"
#include <string>
#include <vector>
#include <map>
#include <memory>

#include "schema.hh"
#include "mysql.hh"
#include "grammar.hh"
#include "expr.hh"
#include "value_catalog.hh"
#include "condition_builder.hh"
#include "condition_generator.hh"
#include "temp_dataset_adapter.hh"

// 列绑定：用于将“别名.列”还原为真实来源（schema.table.col），并标注样本来源
struct ColumnBinding {
  std::string alias;      // 别名（或真实表名）
  std::string schema;     // 真实 schema（基表时有效）
  std::string table;      // 真实表名（基表时有效）
  std::string column;     // 列名
  SourceTag src = SourceTag::Base; // 样本来源（优先 Temp）
};

// 别名解析器：from_clause → alias 映射到真实表
struct AliasResolver {
  std::map<std::string, std::pair<std::string,std::string>> alias_to_real; // alias → (schema, table)
  void build(const from_clause* fc) {
    alias_to_real.clear();
    if (!fc) return;
    for (auto &tr : fc->reflist) {
      if (!tr) continue;
      // named_relation：具名关系（真实表或派生表）
      // table_ref 持有 refs：命名关系列表（可能是 table/aliased_relation/派生）
      if (auto trp = dynamic_cast<table_ref*>(tr.get())) {
        for (auto &nrsp : trp->refs) {
          auto *nr = nrsp.get();
          if (!nr) continue;
          std::string alias = nr->ident();
          if (auto t = dynamic_cast<table*>(nr)) {
            // 真实基表：映射到 schema.table
            alias_to_real[alias] = std::make_pair(t->schema, t->name);
          } else if (auto ar = dynamic_cast<aliased_relation*>(nr)) {
            // 具名别名关系：若其底层是 table，则解析；否则占位
            if (auto t2 = dynamic_cast<table*>(ar->rel)) {
              alias_to_real[alias] = std::make_pair(t2->schema, t2->name);
            } else {
              alias_to_real[alias] = std::make_pair(std::string(), std::string());
            }
          } else {
            alias_to_real[alias] = std::make_pair(std::string(), std::string());
          }
        }
      }
      // temp_dataset_ref：VALUES/TABLE 等临时数据集
      if (auto td = dynamic_cast<temp_dataset_ref*>(tr.get())) {
        std::string alias = td->refs.empty() ? td->alias : td->refs[0]->ident();
        alias_to_real[alias] = std::make_pair(std::string(), std::string());
      }
      if (auto tsq = dynamic_cast<table_subquery*>(tr.get())) {
        std::string alias = tsq->refs.empty() ? std::string("subq") : tsq->refs[0]->ident();
        alias_to_real[alias] = std::make_pair(std::string(), std::string());
      }
    }
  }
};

class AtsCondGen {
public:
  AtsCondGen(schema* sch) : schema_(sch) {}

  // 初始化 ATS 开关与 DISTINCT K 值；默认关闭
  void init_from_config(bool enable, int distinct_limit) {
    enabled_ = enable;
    distinct_limit_ = distinct_limit; // K<0 表示不加 LIMIT，采全集
  }

  // 加载样本（仅 MySQL 模式下）：基表 DISTINCT 采样 + 临时数据集样本
  void load_samples(schema_mysql& sm) {
    if (!enabled_) return;
    // 基表样本（K<0 采全集）一次性初始化到 schema 级缓存
    sm.init_samples(distinct_limit_);
  }

  // 绑定列：扫描 FROM 建立 alias→real 映射，同时将临时数据集样本写入 ValueCatalog（SourceTag=Temp）
  void bind_columns(const from_clause* from, ValueCatalog& vc) {
    if (!enabled_) return;
    aliasr_.build(from);
    // 将基表样本复制到别名键（alias.col）以保证样本命中；temp 样本优先
    for (const auto &kv : aliasr_.alias_to_real) {
      const std::string alias = kv.first;
      const std::string sch = kv.second.first;
      const std::string tbl = kv.second.second;
      if (!sch.empty() && !tbl.empty()) {
        // 在 schema_ 中定位该表的列集合
        for (auto &t : schema_->tables) {
          if (t.schema == sch && t.name == tbl) {
            for (auto &c : t.columns()) {
              const SampleList* sl = vc.get(t.ident(), c.name);
              if (sl) {
                vc.set_temp_samples(alias, c.name, sl->values, sl->family, sl->src);
              }
            }
          }
        }
      }
    }
    TempDatasetAdapter tda;
    tda.scan_from_clause_and_set_samples(from, vc);
  }

  // 针对单条查询在 AST 层插入条件（WHERE/ON/HAVING），并进行样本命中校验
  void generate_conditions_for_query(query_spec& q) {
    if (!enabled_ || !schema_ || !schema_->mysql_mode) return;
    // 仅 MySQL 模式适配；保持最小侵入：使用已存在 ConditionGenerator 门面进行 AST 注入
    ConditionGenerator cg(schema_);
#ifdef HAVE_LIBMYSQLCLIENT
    if (auto sm = dynamic_cast<schema_mysql*>(schema_)) {
      sm->init_samples(distinct_limit_);
    }
#endif
    // 在本次 query 的 FROM 上解析临时数据集样本，以确保 alias 映射与样本命中
    bind_columns(q.from_clause.get(), vc_);
    // AST 条件注入：列命中失败时，由 ConditionBuilder 内部降级到 IS [NOT] NULL 或剔除
    cg.apply_recursively(q);
  }

private:
  bool enabled_ = false;         // 默认关闭
  int distinct_limit_ = -1;      // K；-1 表示不加 LIMIT 采全集
  schema* schema_ = nullptr;     // 方言/特性开关判断（仅 MySQL 模式）
  ValueCatalog vc_;              // 样本缓存（基表 + 临时）
  AliasResolver aliasr_;         // 别名映射
};

#ifdef HAVE_ATS
// ATS 可用分支（示例性钩子伪代码）：
// 说明：本分支仅提供方法与钩子调用示例，不强依赖 ATS 头文件，避免在非 ATS 环境下编译失败。
// 实际集成时：在 TSHttpTxn 的 ReadRequestHdr/SendRequestHdr 钩子中：
// 1) 读取插件配置（enable, distinct_limit）；
// 2) 初始化 AtsCondGen 并调用 load_samples(schema_mysql)、bind_columns(from_clause, vc)；
// 3) 对即将执行的 AST（query_spec）调用 generate_conditions_for_query(q)；
// 4) 将序列化后的 SQL 写回请求或交由原执行器使用。
// 伪代码：
// void on_txn_send_request(TSHttpTxn txnp) {
//   bool enable = read_conf_bool("enable_condgen");
//   int K = read_conf_int("distinct_sample_limit", -1);
//   AtsCondGen gen(current_schema);
//   gen.init_from_config(enable, K);
//   if (!enable || !current_schema->mysql_mode) return; // 默认关闭且仅 MySQL
//   schema_mysql* sm = get_mysql_schema();
//   if (sm) gen.load_samples(*sm);
//   const query_spec* q = extract_ast_from_txn(txnp); // 利用现有管道拿到 AST
//   if (q) {
//     gen.bind_columns(q->from.get(), /*vc*/);
//     gen.generate_conditions_for_query(*const_cast<query_spec*>(q));
//   }
// }
#endif
