/// @file
/// @brief ATS 条件适配模块实现（默认关闭，最小侵入，不改变原生成逻辑）。
///
/// 编译策略：
/// - #ifdef HAVE_ATS：提供示例性钩子函数（伪代码占位），不强依赖 ATS 头文件，避免在无 ATS 环境下编译失败。
/// - #else：降级实现（可编译）：接口一致，内部调用现有 ValueCatalog/ConditionBuilder/ConditionGenerator 完成样本绑定与 AST 条件注入；
///   当开关关闭时做空操作，不消耗 RNG，保证 seed 复现。

#include "ats_condition_adapter.hh"
#include "random.hh"
#include <iostream>

using namespace std;

#ifdef HAVE_ATS
// ========== ATS 可用分支（占位/伪代码） ==========
// 说明：此分支仅示例 TSHttpTxn 钩子时机与调用流程，避免在无 ATS 环境下编译失败。
// 真正集成到 ATS 插件时：
// - 在插件初始化读取配置（enable, distinct_limit）；
// - 在 ReadRequestHdr/SendRequestHdr 等钩子中调用 AtsCondGen::load_samples/bind_columns/generate_conditions_for_query；
// - 由原序列化器输出 SQL。

// 示例占位（非可编译 ATS 代码）：
static void ats_on_send_request(/*TSHttpTxn txnp*/ void* /*ignored*/, AtsCondGen& gen, schema_mysql* sm, query_spec* q) {
  (void)gen; (void)sm; (void)q;
  // 占位：真实 ATS 环境中按需调用 gen.init_from_config / gen.load_samples / gen.bind_columns / gen.generate_conditions_for_query
}

#else
// ========== 降级实现（可编译） ==========

// 采用头文件中的类定义，具体逻辑在此实现；保持最小侵入与默认关闭

// 注意：AtsCondGen 的方法主体已在头文件声明，这里不做额外扩展；若需日志，可按需输出

#endif // HAVE_ATS
