/// @file
/// @brief ValueCatalog：按“表.列 → 样本列表”缓存真实样本，用于 AST 条件生成（仅 MySQL 模式）。
/// 行为约束：
/// - 基表采样：在加载表信息阶段按列执行 SELECT DISTINCT col FROM schema.table [LIMIT K]；K<0 表示不加 LIMIT 采全集。
/// - 临时数据集：直接基于派生表/VALUES/子查询的字面量行集写入样本（SourceTag=temp），不访问基表。
/// - NULL 过滤与标注：可选择跳过 NULL 或将 is_null=true 标注；当失败或空集时，提供类型安全常量回退。
/// - 仅 MySQL 模式生效；默认关闭。
#ifndef VALUE_CATALOG_HH
#define VALUE_CATALOG_HH

#include "config.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <sstream>
#include <algorithm>

#include "relmodel.hh"
#include "schema.hh"
struct schema_mysql; // 前向声明，避免头文件循环依赖

// 类型族（用于条件构造的类型守卫）
enum class TypeFamily {
  Numeric,
  String,
  Time,
  Bit,
  Json,
  Blob,
  EnumSet,
  Spatial,
  Unknown
};

// 样本来源标记：基表或临时数据集
enum class SourceTag { Base, Temp, Synthetic };

struct Value {
  std::string literal;   // 可直接打印的 SQL 字面量或表达式（已按类型安全包装）
  bool is_null = false;  // 是否为 NULL（如需区分）
  TypeFamily family = TypeFamily::Unknown;
  SourceTag src = SourceTag::Base;
  // 最小构造：支持 brace-init 四参用法
  Value() = default;
  Value(std::string lit, bool null, TypeFamily fam, SourceTag src_)
    : literal(std::move(lit)), is_null(null), family(fam), src(src_) {}
};

struct SampleList {
  TypeFamily family = TypeFamily::Unknown;
  SourceTag src = SourceTag::Base;
  std::vector<Value> values;
};

// 工具：类型名映射到 TypeFamily
static inline TypeFamily family_of(const std::string &name) {
  if (name=="int"||name=="tinyint"||name=="smallint"||name=="mediumint"||name=="bigint"||name=="float"||name=="double"||name=="decimal") return TypeFamily::Numeric;
  if (name=="varchar"||name=="char"||name=="text") return TypeFamily::String;
  if (name=="date"||name=="datetime"||name=="timestamp"||name=="time"||name=="year") return TypeFamily::Time;
  if (name=="bit") return TypeFamily::Bit;
  if (name=="json") return TypeFamily::Json;
  if (name.find("blob")!=std::string::npos || name=="binary" || name=="varbinary") return TypeFamily::Blob;
  if (name=="enum"||name=="set") return TypeFamily::EnumSet;
  if (name=="geometry"||name=="point"||name=="linestring"||name=="polygon") return TypeFamily::Spatial;
  return TypeFamily::Unknown;
}

// 工具：安全单引号包裹并转义（最小实现）
static inline std::string sql_quote(const std::string &s) {
  std::ostringstream os; os << "'";
  for (char c : s) { if (c=='\'') os << "''"; else os << c; }
  os << "'"; return os.str();
}

class ValueCatalog {
public:
  // 键为 "rel_ident.col_name"（基表使用 schema.table，临时数据集使用别名 alias）
  using Key = std::string;

  // 加载基表样本：遍历 schema_mysql 的表与列执行 DISTINCT 采样；K<0 采全集
  void load_base_samples(schema_mysql &sch, int sample_limit_or_minus1);

  // 写入临时数据集样本：table_alias + col_name 对应样本列表，family/来源标记
  void set_temp_samples(const std::string &table_alias, const std::string &col_name,
                        const std::vector<Value> &vals, TypeFamily fam, SourceTag src = SourceTag::Temp);

  // 获取样本列表指针，不存在返回空指针（在 debug_on_ 时输出 [VC][get] 命中/回退/缺失日志）
  const SampleList* get(const std::string &tbl, const std::string &col) const;
  // 后备数据源（基表样本缓存）：本地 miss 时回退读取
  void set_fallback(const ValueCatalog* fb) { fallback_ = fb; }
  // 调试开关（由上层按 sch.condgen_debug 或 sch.verbose 设置）
  void set_debug(bool on) { debug_on_ = on; }
  // 注入 schema 指针以支持类型查询（仅用于合成样本兜底，默认可为空）
  void set_schema(schema* sch) { schema_ = sch; }

  // 枚举所有缓存键（用于日志输出），键格式：rel_ident.col
  std::vector<Key> keys() const;

private:
  std::map<Key, SampleList> store_;
  const ValueCatalog* fallback_ = nullptr;
  bool debug_on_ = false;
  schema* schema_ = nullptr;

  // 工具：按列类型包装安全字面量
  static Value make_safe_literal(const std::string &tname, const char *raw, SourceTag src);
};

#endif
