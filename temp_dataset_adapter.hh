/// @file
/// @brief TempDatasetAdapter：解析 FROM 中的临时数据源（VALUES/TABLE/子查询），
/// 抽取字面列值为样本并写入 ValueCatalog（SourceTag=temp）。
#ifndef TEMP_DATASET_ADAPTER_HH
#define TEMP_DATASET_ADAPTER_HH

#include <string>
#include <vector>
#include <memory>

#include "value_catalog.hh"
#include "grammar.hh"
#include "json_table_ref.hh"
#include "temp_dataset.hh"

struct TempDatasetAdapter {
  // 扫描 from_clause 中的 table_ref，抽取临时数据集的样本写入 ValueCatalog
  void scan_from_clause_and_set_samples(const from_clause* fc, ValueCatalog& vc) {
    if (!fc) return;
    // 当临时数据集总开关关闭时，跳过 VALUES/JSON_TABLE 扫描与样本写入
    if (fc->scope && fc->scope->schema && !fc->scope->schema->feature.temp_dataset_enabled) return;
    for (auto& tr : fc->reflist) {
      if (!tr) continue;
      if (auto td = dynamic_cast<temp_dataset_ref*>(tr.get())) {
        // VALUES 模式：alias + values_rel + rows + col_types
        const std::string alias = td->refs.empty() ? td->alias : td->refs[0]->ident();
        const auto& cols = td->values_rel.columns();
        for (size_t i = 0; i < cols.size(); ++i) {
          const std::string colname = cols[i].name;
          const std::string tname = td->col_types.size() > i ? td->col_types[i] : std::string("varchar");
          TypeFamily fam = family_of(tname);
          std::vector<Value> vals;
          for (auto& row : td->rows) {
            if (i < row.size()) {
              Value v; v.family = fam; v.src = SourceTag::Temp; v.is_null = (row[i] == "NULL"); v.literal = row[i];
              vals.push_back(v);
            }
          }
          vc.set_temp_samples(alias, colname, vals, fam, SourceTag::Temp);
        }
      }
      // JSON_TABLE：使用派生列定义，按别名写入空样本（交由 ConditionBuilder 做兜底）
      if (auto jtr = dynamic_cast<json_table_ref*>(tr.get())) {
        const std::string alias = (!jtr->refs.empty()
                                   ? (!jtr->refs[0]->ident().empty() ? jtr->refs[0]->ident() : jtr->alias)
                                   : jtr->alias);
        const auto& cols = jtr->derived.columns();
        for (auto& c : cols) {
          std::vector<Value> vals;
          std::string tname = safe_type_name(c.type);
          TypeFamily fam = family_of(tname);
          // 为 JSON_TABLE 派生列注入少量合成样本，避免后续条件生成走到字符串兜底
          if (fam == TypeFamily::Numeric) {
            vals.push_back(Value{"0", false, fam, SourceTag::Synthetic});
            vals.push_back(Value{"1", false, fam, SourceTag::Synthetic});
          } else if (fam == TypeFamily::String) {
            vals.push_back(Value{sql_quote("x"), false, fam, SourceTag::Synthetic});
            vals.push_back(Value{sql_quote("y"), false, fam, SourceTag::Synthetic});
          } else if (fam == TypeFamily::Time) {
            // JSON_TABLE 不直接产生时间；若遇到时间族，给出保守值
            vals.push_back(Value{sql_quote("1970-01-01"), false, fam, SourceTag::Synthetic});
          } else if (fam == TypeFamily::Json) {
            vals.push_back(Value{"CAST('{}' AS JSON)", false, fam, SourceTag::Synthetic});
          }
          vc.set_temp_samples(alias, c.name, vals, fam, SourceTag::Temp);
        }
      }
    }
  }
};

#endif
