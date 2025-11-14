#include "condition_generator.hh"
#include <functional>
#include <sstream>
#include <iostream>
#include "random.hh"
#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif
#include "postgres.hh"

// batch probability extern
extern "C" double batch_get_p_cond();
extern "C" double batch_get_p_having();

using namespace std;

// ===== Fulltext helpers (string family detection, cached value pick, dialect predicate builders) =====
static inline bool is_string_family(sqltype* t) { return safe_type_family(t) == SafeTypeFamily::String; }
static std::string strip_quotes(const std::string& s){ if(s.size()>=2 && s.front()=='\'' && s.back()=='\''){ return s.substr(1, s.size()-2); } return s; }
static std::string escape_single_quotes(const std::string& s){ std::string o; o.reserve(s.size()); for(char c: s){ if(c=='\'') o.push_back('\''); o.push_back(c); } return o; }
static std::string trunc_safe(const std::string& s, size_t n){ if(s.size()<=n) return s; return s.substr(0,n); }
struct ft_pred_text : bool_expr { std::string text; ft_pred_text(prod* p, const std::string& t): bool_expr(p), text(t){} void out(std::ostream& o){ o << text; } void accept(prod_visitor* v){ v->visit(this);} };
static std::string pick_fulltext_value(named_relation* rel, const column& c, const ValueCatalog& vc){ if(!rel) return std::string("foo"); const SampleList* sl = vc.get(rel->ident(), c.name); if(sl && !sl->values.empty()){ for(const auto &v: sl->values){ if(!v.is_null && !v.literal.empty()){ std::string inner = strip_quotes(v.literal); inner = trunc_safe(inner, 64); return inner.empty() ? std::string("foo") : inner; } } }
  return std::string("foo"); }
static std::shared_ptr<bool_expr> make_fulltext_pred_mysql(prod* p, const std::vector<ColumnRef>& cols, const std::string& val){ if(cols.empty()) return nullptr; // build column list
  std::ostringstream os; os << "MATCH("; for(size_t i=0;i<cols.size() && i<2;++i){ if(i) os << ","; os << cols[i].rel->ident() << "." << cols[i].col.name; } os << ") AGAINST('" << escape_single_quotes(val) << "' "; if(d6()>3) os << "IN NATURAL LANGUAGE MODE"; else os << "IN BOOLEAN MODE"; os << ")"; return std::make_shared<ft_pred_text>(p, os.str()); }
static std::shared_ptr<bool_expr> make_fulltext_pred_pg(prod* p, const ColumnRef& c, const std::string& val){ std::ostringstream os; os << "to_tsvector('simple', " << c.rel->ident() << "." << c.col.name << ") @@ websearch_to_tsquery('" << escape_single_quotes(val) << "')"; return std::make_shared<ft_pred_text>(p, os.str()); }
static bool sqlite_is_fts_table(named_relation* r){ if(!r) return false; std::string n; if(auto ar = dynamic_cast<aliased_relation*>(r)){ if(ar->rel){ if(auto t = dynamic_cast<table*>(ar->rel)){ n = t->name; } } } else if(auto t = dynamic_cast<table*>(r)){ n = t->name; } std::string low; low.reserve(n.size()); for(char c: n){ low.push_back((char)std::tolower((unsigned char)c)); }
  return low.find("fts") != std::string::npos; }
static std::shared_ptr<bool_expr> make_fulltext_pred_sqlite(prod* p, const ColumnRef& c, const std::string& val){ if(!sqlite_is_fts_table(c.rel)) return nullptr; std::ostringstream os; // Simplified FTS: column MATCH 'val'
  os << c.rel->ident() << "." << c.col.name << " MATCH '" << escape_single_quotes(val) << "'"; return std::make_shared<ft_pred_text>(p, os.str()); }

void ConditionGenerator::init_sampling(schema_mysql& sch, int sample_limit_or_minus1) {
  // 已迁移：采样由 schema_mysql::init_samples 一次性完成；此处仅确保 fallback 指向缓存
  (void)sample_limit_or_minus1;
  vc_.set_fallback(&sch.base_samples_cache);
}

// 工具：拍平 from_clause 的 refs
static table_ref_set flatten_refs(from_clause* fc) {
  table_ref_set refs;
  if (!fc) return refs;
  for (auto& tr : fc->reflist) {
    if (!tr) continue;
    for (auto& nr : tr->refs) refs.push_back(nr);
  }
  return refs;
}

void ConditionGenerator::attach_to_on(query_spec& q) {
  // 接口统一：移除未定义 RNG 类型，改用全局 d6/d100，保持开关关闭时不消耗 RNG
  bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
  // 概率门控：按批次控制的 p_cond 决定是否注入 JOIN_ON 附加条件
  double pcond = batch_get_p_cond();
  if (d100() >= (int)(pcond * 100.0)) {
    if (log_on) std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=PROB_GATE p_cond=" << pcond << std::endl;
    return;
  }
  if (!q.from_clause) {
    if (log_on) std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=NO_JOIN" << std::endl;
    return;
  }

  TempDatasetAdapter tda; tda.scan_from_clause_and_set_samples(q.from_clause.get(), vc_);
  // [Projection Sample Propagation][JOIN_ON] 递归扫描 FROM 中的子查询（table_subquery），将其 SELECT 投影的样本与类型族复制到外层别名键，确保外层 ON 能命中
  if (q.from_clause) {
    for (auto &tr : q.from_clause->reflist) {
      if (!tr) continue;
      if (auto tsq = dynamic_cast<table_subquery*>(tr.get())) {
        // 先在子查询内部扫描临时数据集样本（VALUES/JSON_TABLE），以便从源别名列复制样本
        if (tsq->query && tsq->query->from_clause) {
          tda.scan_from_clause_and_set_samples(tsq->query->from_clause.get(), vc_);
        }
        std::string outer_alias = (!tsq->refs.empty() && tsq->refs[0]) ? tsq->refs[0]->ident() : std::string();
        if (outer_alias.empty()) continue;
        auto &dst_cols = tsq->query->select_list->derived_table.columns();
        auto &exprs = tsq->query->select_list->value_exprs;
        size_t n = std::min(dst_cols.size(), exprs.size());
        for (size_t i = 0; i < n; ++i) {
          const column &dstc = dst_cols[i];
          std::vector<Value> vals;
          TypeFamily fam = family_of(safe_type_name(dstc.type));
          if (auto cref = dynamic_cast<column_reference*>(exprs[i].get())) {
            std::string alias_col = cref->reference; std::string src_alias, src_col;
            size_t dot = alias_col.find('.');
            if (dot != std::string::npos && dot>0 && dot+1<alias_col.size()) { src_alias = alias_col.substr(0, dot); src_col = alias_col.substr(dot+1); }
            if (!src_alias.empty() && !src_col.empty()) {
              const SampleList* sl_src = vc_.get(src_alias, src_col);
              if (sl_src && !sl_src->values.empty()) { vals = sl_src->values; fam = sl_src->family; }
            }
          }
          vc_.set_temp_samples(outer_alias, dstc.name, vals, fam, vals.empty()?SourceTag::Temp:vals.front().src);
        }
      }
    }
  }
  // 基表样本复制到别名键，保证 JOIN 作用域跨别名命中
  {
    // 构建 alias → (schema, table) 映射
    std::map<std::string,std::pair<std::string,std::string>> alias2real;
    for (auto &tr : q.from_clause->reflist) {
      if (!tr) continue;
      if (auto trp = dynamic_cast<table_ref*>(tr.get())) {
        for (auto &nrsp : trp->refs) {
          auto *nr = nrsp.get(); if (!nr) continue;
          std::string alias = nr->ident();
          if (auto t = dynamic_cast<table*>(nr)) {
            alias2real[alias] = std::make_pair(t->schema, t->name);
          } else if (auto ar = dynamic_cast<aliased_relation*>(nr)) {
            if (auto t2 = dynamic_cast<table*>(ar->rel)) alias2real[alias] = std::make_pair(t2->schema, t2->name);
            else alias2real[alias] = std::make_pair(std::string(), std::string());
          } else {
            alias2real[alias] = std::make_pair(std::string(), std::string());
          }
        }
      }
    }
#ifdef HAVE_LIBMYSQLCLIENT
    if (schema_ && schema_->mysql_mode) {
      if (auto sm = dynamic_cast<schema_mysql*>(schema_)) {
        for (const auto &kv : alias2real) {
          const std::string &alias = kv.first; const std::string &sch = kv.second.first; const std::string &tbl = kv.second.second;
          if (sch.empty() || tbl.empty()) continue;
          // 在 schema_ 中定位该表的列集合，并从后备缓存复制样本到 alias
          for (auto &t : schema_->tables) {
            if (t.schema == sch && t.name == tbl) {
              for (auto &c : t.columns()) {
                const SampleList* sl = sm->base_samples_cache.get(t.ident(), c.name);
                if (sl && !sl->values.empty()) {
                  vc_.set_temp_samples(alias, c.name, sl->values, sl->family, sl->src);
                } else {
                  // 即使在客户端不可用或列样本为空，也注入空样本但保留类型族，避免后续 family=Unknown
                  std::vector<Value> empty;
                  TypeFamily fam = family_of(safe_type_name(c.type));
                  vc_.set_temp_samples(alias, c.name, empty, fam, SourceTag::Temp);
                }
              }
            }
          }
        }
      }
    }
#else
    // 无 MySQL 客户端库时：仍执行 alias→real 的类型族复制（写入空样本，但保留 family），确保 ValueCatalog::get 能合成同族安全常量
    if (schema_ && schema_->mysql_mode) {
      for (const auto &kv : alias2real) {
        const std::string &alias = kv.first; const std::string &sch = kv.second.first; const std::string &tbl = kv.second.second;
        if (sch.empty() || tbl.empty()) continue;
        for (auto &t : schema_->tables) {
          if (t.schema == sch && t.name == tbl) {
            for (auto &c : t.columns()) {
              std::vector<Value> empty;
              TypeFamily fam = family_of(safe_type_name(c.type));
              vc_.set_temp_samples(alias, c.name, empty, fam, SourceTag::Temp);
            }
          }
        }
      }
    }
#endif
  }
  // 统计 JOIN 条件上下文的候选列数量与样本命中率
  int candidates = 0; int sample_hits = 0; int sample_total = 0;
  for (auto &tr : q.from_clause->reflist) {
    if (!tr) continue;
    if (auto jt = dynamic_cast<joined_table*>(tr.get())) {
      if (auto ejc = dynamic_cast<expr_join_cond*>(jt->condition.get())) {
        const auto& jrefs = ejc->joinscope.refs;
        for (auto *nr : jrefs) {
          if (!nr) continue;
          auto cols_list = nr->columns();
          for (auto &c : cols_list) {
            candidates++;
            sample_total++;
            const SampleList* sl = vc_.get(nr->ident(), c.name);
            if (sl && !sl->values.empty()) sample_hits++;
          }
        }
      }
    }
  }
  double density = (schema_? schema_->extra_conds_density : -1.0);
  double base_wt = 0.9; if (schema_ && density>=0.0 && density<=1.0) base_wt = std::max(0.0, std::min(1.0, 1.0 - density));
  int gate = d100();
  if (log_on) {
    std::cerr << "[EXTRA][scope=JOIN_ON] try candidates=" << candidates << " sample_hit_rate=" << (sample_total? (double)sample_hits/sample_total : 0.0)
                         << " density=" << (density<0? -1.0 : density) << " base_weight=" << base_wt << " gate=" << gate << std::endl;
    // 候选列列表
    std::ostringstream oss; oss << "[EXTRA][scope=JOIN_ON] try candidates=[";
    bool first=true;
    for (auto &tr : q.from_clause->reflist) {
      if (!tr) continue;
      if (auto jt = dynamic_cast<joined_table*>(tr.get())) {
        if (auto ejc = dynamic_cast<expr_join_cond*>(jt->condition.get())) {
          const auto& jrefs = ejc->joinscope.refs;
          for (auto *nr : jrefs) {
            if (!nr) continue;
            auto cols_list = nr->columns();
            for (auto &c : cols_list) { if(!first) oss << ","; first=false; oss << nr->ident() << "." << c.name; }
          }
        }
      }
    }
    oss << "]"; std::cerr << oss.str() << std::endl;
    // 口径解释
    std::cerr << "[EXTRA][scope=JOIN_ON] sample_hit_rate=" << (sample_total? (double)sample_hits/sample_total : 0.0)
              << " note=\"作用域候选列维度统计，不代表全局样本命中\"" << std::endl;
  }

  // 遍历 join 条件，若为 expr_join_cond 则合并布尔项
  int injected_total = 0; bool has_join = false;
  int extras_sum = 0; int base_sum = 0; int total_sum = 0; int extras_tagged_sum = 0;
  int const_bool_sum = 0; int is_null_sum = 0; int has_col_any = 0;
  for (auto& tr : q.from_clause->reflist) {
    if (!tr) continue;
    if (auto jt = dynamic_cast<joined_table*>(tr.get())) {
      if (auto ejc = dynamic_cast<expr_join_cond*>(jt->condition.get())) {
        has_join = true;
        // 作用域 refs（join 范围）
        const auto& jrefs = ejc->joinscope.refs; // vector<named_relation*>
        // 优先尝试在 JOIN ON 上注入全文谓词（字符串族；概率 fulltext_prob）
        if (schema_ && schema_->feature.fulltext_enable && d100() < (int)(schema_->feature.fulltext_prob * 100.0)) {
          std::vector<ColumnRef> ftcols;
          for (auto *nr : jrefs) { if (!nr) continue; for (auto &c : nr->columns()) { if (is_string_family(c.type)) { ftcols.emplace_back(nr, c); break; } } if (!ftcols.empty()) break; }
          if (!ftcols.empty()) {
            std::string val = pick_fulltext_value(ftcols[0].rel, ftcols[0].col, vc_);
            std::shared_ptr<bool_expr> pft;
            if (schema_->mysql_mode) { pft = make_fulltext_pred_mysql(ejc, ftcols, val); if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][MYSQL] scope=JOIN_ON val='" << trunc_safe(val,32) << "'" << std::endl; }
            else if (dynamic_cast<schema_pqxx*>(schema_)) { pft = make_fulltext_pred_pg(ejc, ftcols[0], val); if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][PG] scope=JOIN_ON val='" << trunc_safe(val,32) << "'" << std::endl; }
            else if (dynamic_cast<schema_sqlite*>(schema_)) { pft = make_fulltext_pred_sqlite(ejc, ftcols[0], val); if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][SQLITE] scope=JOIN_ON val='" << trunc_safe(val,32) << "'" << std::endl; }
            if (!pft) { std::ostringstream os; os << ftcols[0].rel->ident() << "." << ftcols[0].col.name << " LIKE '%" << escape_single_quotes(val) << "%'"; pft = std::make_shared<ft_pred_text>(ejc, os.str()); if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][FALLBACK] scope=JOIN_ON LIKE val='" << trunc_safe(val,32) << "'" << std::endl; }
            if (pft) { std::vector<std::shared_ptr<bool_expr>> pair; pair.push_back(ejc->search); pair.push_back(pft); ejc->search = cb_.compose_predicates(ejc, pair); }
          }
        }
        // 独立密度开关：仅当显式提供时生效；默认保持 0.9
        double wt = base_wt;
        // 90% 权重来自物理表列；生成 1~3 条
        auto combo = cb_.build_predicates_for_scope(ejc, jrefs, CondContext::JOIN_ON, vc_, 1 + (d6()%3), wt);
        // 读取 builder 统计用于汇总，仅日志，不影响语义
        const auto &st = cb_.get_last_stats(); extras_sum += st.extras; base_sum += st.quota; total_sum += st.k; extras_tagged_sum += st.extras_tagged; const_bool_sum += st.const_bool_count; is_null_sum += st.is_null_count; has_col_any |= st.has_column_predicate;
        if (combo) {
          // 将原 search 与新组合以 AND/OR 结合
          vector<shared_ptr<bool_expr>> pair;
          pair.push_back(ejc->search);
          pair.push_back(combo);
          ejc->search = cb_.compose_predicates(ejc, pair);
          injected_total++;
        }
      }
    }
  }
  if (log_on) {
    if (!has_join) std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=NO_JOIN" << std::endl;
    else if (injected_total>0) std::cerr << "[EXTRA][scope=JOIN_ON] injected=" << injected_total << std::endl;
    else if (candidates==0) std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=NO_COLUMNS" << std::endl;
    else if (sample_hits==0) std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=SAMPLE_MISS" << std::endl;
    else std::cerr << "[EXTRA][scope=JOIN_ON] skip reason=TYPE_MISMATCH" << std::endl;
    // 分类计数与汇总（仅日志）
    if (has_join) {
      std::cerr << "[EXTRA][scope=JOIN_ON] extras_injected=" << extras_sum
                << " base_predicates=" << base_sum
                << " total_predicates=" << total_sum
                << " extras_tagged_in_sql=" << extras_tagged_sum
                << std::endl;
      // 约束状态快照
      std::cerr << "[EXTRA][enforce] summary scope=JOIN_ON const_bool_count=" << const_bool_sum
                << " is_null_count=" << is_null_sum
                << " has_column_predicate=" << (has_col_any?1:0)
                << std::endl;
    }
  }

}


void ConditionGenerator::attach_to_where(query_spec& q) {
  // 接口统一：移除未定义 RNG 类型，改用全局 d6/d100，保持开关关闭时不消耗 RNG
  bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
  double pcond = batch_get_p_cond();
  if (d100() >= (int)(pcond * 100.0)) {
    if (log_on) std::cerr << "[EXTRA][scope=WHERE] skip reason=PROB_GATE p_cond=" << pcond << std::endl;
    return;
  }
  // 适配临时数据集样本
  TempDatasetAdapter tda; tda.scan_from_clause_and_set_samples(q.from_clause.get(), vc_);
  // [Projection Sample Propagation][WHERE] 递归扫描 FROM 中的子查询（table_subquery），将其 SELECT 投影的样本与类型族复制到外层别名键，确保外层 WHERE 能命中
  if (q.from_clause) {
    for (auto &tr : q.from_clause->reflist) {
      if (!tr) continue;
      if (auto tsq = dynamic_cast<table_subquery*>(tr.get())) {
        if (tsq->query && tsq->query->from_clause) { tda.scan_from_clause_and_set_samples(tsq->query->from_clause.get(), vc_); }
        std::string outer_alias = (!tsq->refs.empty() && tsq->refs[0]) ? tsq->refs[0]->ident() : std::string();
        if (outer_alias.empty()) continue;
        auto &dst_cols = tsq->query->select_list->derived_table.columns();
        auto &exprs = tsq->query->select_list->value_exprs;
        size_t n = std::min(dst_cols.size(), exprs.size());
        for (size_t i=0;i<n;++i){ const column &dstc = dst_cols[i]; std::vector<Value> vals; TypeFamily fam = family_of(safe_type_name(dstc.type));
          if (auto cref = dynamic_cast<column_reference*>(exprs[i].get())) { std::string alias_col = cref->reference; std::string src_alias, src_col; size_t dot = alias_col.find('.'); if (dot != std::string::npos && dot>0 && dot+1<alias_col.size()) { src_alias = alias_col.substr(0,dot); src_col = alias_col.substr(dot+1);} if (!src_alias.empty() && !src_col.empty()) { const SampleList* sl_src = vc_.get(src_alias, src_col); if (sl_src && !sl_src->values.empty()) { vals = sl_src->values; fam = sl_src->family; } } }
          vc_.set_temp_samples(outer_alias, dstc.name, vals, fam, vals.empty()?SourceTag::Temp:vals.front().src);
        }
      }
    }
  }
  // 基表样本复制到别名键，保证 WHERE 作用域跨别名命中
  {
    std::map<std::string,std::pair<std::string,std::string>> alias2real;
    if (q.from_clause) {
      for (auto &tr : q.from_clause->reflist) {
        if (!tr) continue;
        if (auto trp = dynamic_cast<table_ref*>(tr.get())) {
          for (auto &nrsp : trp->refs) {
            auto *nr = nrsp.get(); if (!nr) continue;
            std::string alias = nr->ident();
            if (auto t = dynamic_cast<table*>(nr)) alias2real[alias] = std::make_pair(t->schema, t->name);
            else if (auto ar = dynamic_cast<aliased_relation*>(nr)) {
              if (auto t2 = dynamic_cast<table*>(ar->rel)) alias2real[alias] = std::make_pair(t2->schema, t2->name);
              else alias2real[alias] = std::make_pair(std::string(), std::string());
            } else alias2real[alias] = std::make_pair(std::string(), std::string());
          }
        }
      }
    }
    if (schema_ && schema_->mysql_mode) {
#ifdef HAVE_LIBMYSQLCLIENT
      if (auto sm = dynamic_cast<schema_mysql*>(schema_)) {
        for (const auto &kv : alias2real) {
          const std::string &alias = kv.first; const std::string &sch = kv.second.first; const std::string &tbl = kv.second.second;
          if (sch.empty() || tbl.empty()) continue;
          for (auto &t : schema_->tables) {
            if (t.schema == sch && t.name == tbl) {
              for (auto &c : t.columns()) {
                const SampleList* sl = sm->base_samples_cache.get(t.ident(), c.name);
                if (sl && !sl->values.empty()) {
                  vc_.set_temp_samples(alias, c.name, sl->values, sl->family, sl->src);
                } else {
                  // 保留类型族信息以避免 family=Unknown
                  std::vector<Value> empty; TypeFamily fam = family_of(safe_type_name(c.type));
                  vc_.set_temp_samples(alias, c.name, empty, fam, SourceTag::Temp);
                }
              }
            }
          }
        }
      }
#else
      for (const auto &kv : alias2real) {
        const std::string &alias = kv.first; const std::string &sch = kv.second.first; const std::string &tbl = kv.second.second;
        if (sch.empty() || tbl.empty()) continue;
        for (auto &t : schema_->tables) {
          if (t.schema == sch && t.name == tbl) {
            for (auto &c : t.columns()) {
              std::vector<Value> empty; TypeFamily fam = family_of(safe_type_name(c.type));
              vc_.set_temp_samples(alias, c.name, empty, fam, SourceTag::Temp);
            }
          }
        }
      }
#endif
    }
  }
  auto refs = flatten_refs(q.from_clause.get());
  // 统计候选列数量与样本命中率
  int candidates = 0; int sample_hits = 0; int sample_total = 0;
  for (auto &nrsp : refs) {
    auto *nr = nrsp.get(); if (!nr) continue;
    auto cols_list = nr->columns();
    for (auto &c : cols_list) {
      candidates++;
      sample_total++;
      const SampleList* sl = vc_.get(nr->ident(), c.name);
      if (sl && !sl->values.empty()) sample_hits++;
    }
  }
  // 独立密度开关：仅当显式提供时生效；默认采用 0.8 物理列比例（const 比例 0.2）
  double wt = 0.8;
  if (schema_) {
    double dopt = schema_->extra_conds_density;
    if (dopt >= 0.0 && dopt <= 1.0) {
      wt = std::max(0.0, std::min(1.0, 1.0 - dopt));
    }
  }
  int gate = d100();
  if (log_on) {
    std::cerr << "[EXTRA][scope=WHERE] try candidates=" << candidates << " sample_hit_rate=" << (sample_total? (double)sample_hits/sample_total : 0.0)
                         << " density=" << (schema_? schema_->extra_conds_density : -1.0) << " base_weight=" << wt << " gate=" << gate
                         << " physical_ratio=" << wt << ", const_ratio=" << (1.0 - wt) << std::endl;
    // 候选列列表
    std::ostringstream oss; oss << "[EXTRA][scope=WHERE] try candidates=[";
    for (size_t i=0; i<refs.size(); ++i) {
      auto *nr = refs[i].get(); if (!nr) continue;
      auto cols_list = nr->columns();
      for (size_t j=0;j<cols_list.size();++j) { oss << nr->ident() << "." << cols_list[j].name; if (i+1<refs.size() || j+1<cols_list.size()) oss << ","; }
    }
    oss << "]"; std::cerr << oss.str() << std::endl;
    // 口径解释
    std::cerr << "[EXTRA][scope=WHERE] sample_hit_rate=" << (sample_total? (double)sample_hits/sample_total : 0.0)
              << " note=\"作用域候选列维度统计，不代表全局样本命中\"" << std::endl;
  }
  // 优先尝试全文检索谓词（字符串族列；命中概率 fulltext_prob）
  if (schema_ && (schema_->condgen_debug || schema_->verbose)) {
    if (schema_->feature.fulltext_enable) std::cerr << "[FT][ENABLE] scope=WHERE prob=" << schema_->feature.fulltext_prob << std::endl;
  }
  if (schema_ && schema_->feature.fulltext_enable && d100() < (int)(schema_->feature.fulltext_prob * 100.0)) {
    // 收集字符串族列（优先最近别名）
    std::vector<ColumnRef> ftcols;
    for (auto &nrsp : refs) {
      auto *nr = nrsp.get(); if (!nr) continue;
      for (auto &c : nr->columns()) { if (is_string_family(c.type)) { ftcols.emplace_back(nr, c); break; } }
      if (!ftcols.empty()) break;
    }
    if (!ftcols.empty()) {
      std::string val = pick_fulltext_value(ftcols[0].rel, ftcols[0].col, vc_);
      std::shared_ptr<bool_expr> pft;
      if (schema_->mysql_mode) {
        pft = make_fulltext_pred_mysql(&q, ftcols, val);
        if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][MYSQL] val='" << trunc_safe(val, 32) << "'" << std::endl;
      } else if (dynamic_cast<schema_pqxx*>(schema_)) {
        pft = make_fulltext_pred_pg(&q, ftcols[0], val);
        if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][PG] val='" << trunc_safe(val, 32) << "'" << std::endl;
      } else if (dynamic_cast<schema_sqlite*>(schema_)) {
        pft = make_fulltext_pred_sqlite(&q, ftcols[0], val);
        if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][SQLITE] val='" << trunc_safe(val, 32) << "'" << std::endl;
      }
      if (!pft) {
        // 回退：LIKE 或等值兜底
        std::ostringstream os;
        os << ftcols[0].rel->ident() << "." << ftcols[0].col.name << " LIKE '%" << escape_single_quotes(val) << "%'";
        pft = std::make_shared<ft_pred_text>(&q, os.str());
        if ((schema_->condgen_debug || schema_->verbose)) std::cerr << "[FT][FALLBACK] use LIKE for val='" << trunc_safe(val, 32) << "'" << std::endl;
      }
      if (pft) {
        std::vector<std::shared_ptr<bool_expr>> pair; pair.push_back(q.search); pair.push_back(pft);
        q.search = cb_.compose_predicates(&q, pair);
      }
    }
  }
  // 90% 权重基于物理表列；生成 1~3 条
  auto combo = cb_.build_predicates_for_scope(&q, refs, CondContext::WHERE, vc_, 1 + (d6()%3), wt);
  int injected = 0;
  if (combo) {
    vector<shared_ptr<bool_expr>> pair;
    pair.push_back(q.search);
    pair.push_back(combo);
    q.search = cb_.compose_predicates(&q, pair);
    injected = 1;
  }
  if (log_on) {
    if (injected > 0) std::cerr << "[EXTRA][scope=WHERE] injected=" << injected << std::endl;
    else if (candidates == 0) std::cerr << "[EXTRA][scope=WHERE] skip reason=NO_COLUMNS" << std::endl;
    else if (sample_hits == 0) std::cerr << "[EXTRA][scope=WHERE] skip reason=SAMPLE_MISS" << std::endl;
    else std::cerr << "[EXTRA][scope=WHERE] skip reason=TYPE_MISMATCH" << std::endl;
    // 分类计数与汇总（仅日志）
    const auto &st = cb_.get_last_stats();
    std::cerr << "[EXTRA][scope=WHERE] extras_injected=" << st.extras
              << " base_predicates=" << st.quota
              << " total_predicates=" << st.k
              << " extras_tagged_in_sql=" << st.extras_tagged
              << std::endl;
    // 约束状态快照
    std::cerr << "[EXTRA][enforce] scope=WHERE const_bool_count=" << st.const_bool_count
              << " is_null_count=" << st.is_null_count
              << " has_column_predicate=" << st.has_column_predicate
              << std::endl;
  }
}

// 本地定义：通用比较 AST（lhs op rhs），避免跨编译单元引用 condition_builder.cc 内部定义
struct cmp_expr_ast : bool_expr {
  std::shared_ptr<value_expr> lhs, rhs; std::string op;
  cmp_expr_ast(prod* p, const std::shared_ptr<value_expr>& l, const std::string& oper, const std::shared_ptr<value_expr>& r)
    : bool_expr(p), lhs(l), rhs(r), op(oper) {}
  virtual void out(std::ostream& out) { out << *lhs << " " << op << " " << *rhs; }
  virtual void accept(prod_visitor* v) { v->visit(this); lhs->accept(v); rhs->accept(v); }
};

// 解析 "alias.col" 字符串为别名与列名；失败返回 false
static bool parse_alias_col_text(const std::string& s, std::string& alias, std::string& col) {
  alias.clear(); col.clear();
  size_t dot = s.find('.'); if (dot == std::string::npos) return false;
  // 提取由字母数字或下划线组成的左右两段
  size_t i = dot; size_t j = dot;
  while (i>0 && (isalnum((unsigned char)s[i-1]) || s[i-1]=='_')) --i;
  while (j+1 < s.size() && (isalnum((unsigned char)s[j+1]) || s[j+1]=='_')) ++j;
  if (i<dot && j>dot) { alias = s.substr(i, dot-i); col = s.substr(dot+1, j-dot); return true; }
  return false;
}

void ConditionGenerator::attach_to_having(query_spec& q) {
  // 仅在 SELECT 存在聚合时生成 HAVING 条件
  auto is_agg_only = [](value_expr* e)->bool {
    if (!e) return false;
    if (auto f = dynamic_cast<funcall*>(e)) return f->is_aggregate;
    if (dynamic_cast<window_function*>(e)) return false; // HAVING 中禁止窗口
    return false;
  };
  bool has_agg = false;
  if (q.select_list) {
    for (auto &ve : q.select_list->value_exprs) { if (is_agg_only(ve.get())) { has_agg = true; break; } }
  }
  bool log_on_local = (schema_ && (schema_->condgen_debug || schema_->verbose));
  if (log_on_local) {
    std::cerr << "[HAVING] try has_agg=" << (has_agg?1:0) << std::endl;
  }
  if (!has_agg) {
    bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
    if (log_on) std::cerr << "[HAVING] skip reason=NO_AGG" << std::endl;
    return;
  }
  // 概率门控：按批次控制的 p_having 决定是否注入 HAVING 条件
  double phav = batch_get_p_having();
  if (d100() >= (int)(phav * 100.0)) {
    if (log_on_local) std::cerr << "[HAVING] skip reason=PROB_GATE p_having=" << phav << std::endl;
    return;
  }

  // 在合并前进行第一次强制注入（方案1优先：使用聚合参数列），失败则保留后续组合再做兜底
  bool injected_before = ensure_having_physical_cached_predicate(q);
  // 组装一个 HAVING 谓词：
  // 1) 聚合结果与安全常量比较（=,<,>,BETWEEN,IN）
  // 2) 分组物理列（group_by_exprs）与缓存值比较（优先样本，其次常量兜底）
  std::vector<std::shared_ptr<bool_expr>> parts;

  // 1) 聚合结果比较
  try {
    // 在 HAVING 上下文允许聚合（having_guard）
    having_guard hctx(&q);
    auto agg_expr = std::make_shared<funcall>(&hctx, nullptr, true);
    // 右侧常量：按聚合返回类型族选择安全常量
    sqltype* rt = agg_expr->type;
    if (!rt || safe_type_family(rt) == SafeTypeFamily::Unknown) {
      rt = (q.scope && q.scope->schema && q.scope->schema->inttype) ? q.scope->schema->inttype : sqltype::get("int");
    }
    std::shared_ptr<value_expr> rhs;
    // 构造 IN 集合或区间/关系操作
    int pick = d6();
    if (safe_type_family(rt) == SafeTypeFamily::Numeric) {
      if (pick == 1) {
        // BETWEEN
        auto a = std::make_shared<const_expr>(&hctx, rt);
        auto b = std::make_shared<const_expr>(&hctx, rt);
        struct cmp_between_ast : bool_expr {
          std::shared_ptr<value_expr> lhs, lo, hi; cmp_between_ast(prod* p, const std::shared_ptr<value_expr>& l, const std::shared_ptr<value_expr>& a, const std::shared_ptr<value_expr>& b) : bool_expr(p), lhs(l), lo(a), hi(b) {}
          void out(std::ostream& o){ o << *lhs << " BETWEEN " << *lo << " AND " << *hi; }
          void accept(prod_visitor* v){ v->visit(this); lhs->accept(v); lo->accept(v); hi->accept(v);} };
        parts.push_back(std::make_shared<cmp_between_ast>(&hctx, std::static_pointer_cast<value_expr>(agg_expr), a, b));
      } else if (pick == 2 || pick == 3) {
        // IN 集合（最多 3 个）
        std::vector<std::shared_ptr<value_expr>> set;
        int k = 1 + (d6()%3);
        for (int i=0;i<k;++i) set.push_back(std::make_shared<const_expr>(&hctx, rt));
        struct in_expr_ast2 : bool_expr { std::shared_ptr<value_expr> lhs; std::vector<std::shared_ptr<value_expr>> set; bool negate; in_expr_ast2(prod* p, const std::shared_ptr<value_expr>& l, const std::vector<std::shared_ptr<value_expr>>& s, bool n) : bool_expr(p), lhs(l), set(s), negate(n) {}
          void out(std::ostream& o){ o << *lhs << (negate?" NOT IN (":" IN ("); for(size_t i=0;i<set.size();++i){ o<<*set[i]; if(i+1!=set.size()) o<<", "; } o<<")"; }
          void accept(prod_visitor* v){ v->visit(this); lhs->accept(v); for(auto &e:set) e->accept(v);} };
        parts.push_back(std::make_shared<in_expr_ast2>(&hctx, std::static_pointer_cast<value_expr>(agg_expr), set, (pick==3)));
      } else {
        // 关系比较
        rhs = std::make_shared<const_expr>(&hctx, rt);
        parts.push_back(std::make_shared<cmp_expr_ast>(&hctx, std::static_pointer_cast<value_expr>(agg_expr), (d6()>3?">":"<"), rhs));
      }
    } else {
      // 非数值族：用等值或不等
      rhs = std::make_shared<const_expr>(&hctx, rt);
      parts.push_back(std::make_shared<cmp_expr_ast>(&hctx, std::static_pointer_cast<value_expr>(agg_expr), (d6()>3?"=":"!="), rhs));
    }
  } catch (...) {
    // 安全降级：避免影响整体 HAVING
  }

  // 2) 基于分组物理列的条件（优先 group_by_exprs 中的列）
  try {
    std::vector<std::shared_ptr<bool_expr>> col_parts;
    // 收集 group_by 的列引用
    std::vector<ColumnRef> group_cols;
    for (auto &ge : q.group_by_exprs) {
      if (!ge) continue;
      if (auto cref = dynamic_cast<column_reference*>(ge.get())) {
        std::string alias, colname; if (!parse_alias_col_text(cref->reference, alias, colname)) continue;
        // 在当前作用域查找该别名对应的 named_relation 与列对象
        named_relation* rel = nullptr; for (auto r : q.scope->refs) { if (r && r->ident() == alias) { rel = r; break; } }
        if (!rel) continue;
        const column* picked = nullptr; for (auto &c : rel->columns()) { if (c.name == colname) { picked = &c; break; } }
        if (!picked) continue;
        group_cols.emplace_back(rel, *picked);
      }
    }
    // 若未能从 group_by 收集到列，回退到最近别名的一个安全列
    if (group_cols.empty()) {
      if (!q.scope->refs.empty() && q.scope->refs.back()) {
        named_relation* rel = q.scope->refs.back();
        auto cols = rel->columns();
        if (!cols.empty()) {
          // 选择首个非 JSON/几何的安全列
          const column* pc = nullptr;
          for (auto &c : cols) { sqltype* t = c.type; if (!t) continue; std::string tn = safe_type_name(t); if (tn=="json"||tn=="geometry") continue; pc = &c; break; }
          if (!pc) { pc = &cols[0]; }
          group_cols.emplace_back(rel, *pc);
        }
      }
    }
    // 构造 1~2 条列谓词，使用缓存值/样本（由 ValueCatalog 与 ConditionBuilder 负责）
    int want = 1 + (d6()%2);
    for (int i=0; i<want && !group_cols.empty(); ++i) {
      const ColumnRef &cr = group_cols[(d100()-1) % group_cols.size()];
      auto p = cb_.build_predicate_for_column(&q, cr, CondContext::HAVING, vc_);
      if (p) col_parts.push_back(p);
    }
    if (!col_parts.empty()) {
      auto combo = cb_.compose_predicates(&q, col_parts);
      if (combo) parts.push_back(combo);
    }
  } catch (...) {
    // 忽略列条件失败，保持已有部分
  }

  // 强制注入至少一条“物理列 + 缓存值”谓词（优先使用 GROUP BY 列）
  ensure_having_physical_cached_predicate(q);

  if (parts.empty()) {
    // 合并后第二次强制注入（仍失败则 COUNT(col) 兜底）
    bool injected_after = ensure_having_physical_cached_predicate(q);
    if (!injected_before && !injected_after) {
      (void)ensure_having_count_fallback(q);
    }
    return; // 避免覆盖既有 having
  }
  auto merged = cb_.compose_predicates(&q, parts);
  if (merged) {
    if (q.having) {
      std::vector<std::shared_ptr<bool_expr>> pair = { q.having, merged };
      q.having = cb_.compose_predicates(&q, pair);
    } else {
      q.having = merged;
    }
  }
  // 合并后第二次强制注入（仍失败则 COUNT(col) 兜底）
  bool injected_after = ensure_having_physical_cached_predicate(q);
  if (!injected_before && !injected_after) {
    (void)ensure_having_count_fallback(q);
  }
}

// 强制保证 HAVING 至少包含一条“物理列 + 缓存值”的谓词；优先使用 GROUP BY 列
bool ConditionGenerator::ensure_having_physical_cached_predicate(query_spec& q) {
  bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
  // 1) 构造候选列集合：优先从 group_by_exprs 解析 alias.col；否则选取当前作用域的一个安全物理列
  std::vector<ColumnRef> candidates;
  for (auto &ge : q.group_by_exprs) {
    if (!ge) continue;
    if (auto cref = dynamic_cast<column_reference*>(ge.get())) {
      std::string alias, colname; if (!parse_alias_col_text(cref->reference, alias, colname)) continue;
      named_relation* rel = nullptr; for (auto r : q.scope->refs) { if (r && r->ident() == alias) { rel = r; break; } }
      if (!rel) continue;
      const column* picked = nullptr; for (auto &c : rel->columns()) { if (c.name == colname) { picked = &c; break; } }
      if (picked) candidates.emplace_back(rel, *picked);
    }
  }
  if (candidates.empty()) {
    // 从作用域 refs 中收集“物理列”
    std::vector<ColumnRef> phys_cols;
    for (auto r : q.scope->refs) {
      if (!r) continue;
      bool is_physical = false;
      if (dynamic_cast<table*>(r)) {
        is_physical = true;
      } else if (auto ar = dynamic_cast<aliased_relation*>(r)) {
        if (dynamic_cast<table*>(ar->rel)) is_physical = true;
      }
      if (!is_physical) continue;
      for (auto &c : r->columns()) { phys_cols.emplace_back(r, c); }
    }
    // 过滤掉 JSON/geometry 以优先选择常见安全类型；若为空再允许扩展类型
    std::vector<ColumnRef> filtered;
    for (auto &cr : phys_cols) { std::string tn = safe_type_name(cr.col.type); if (tn!="json" && tn!="geometry") filtered.push_back(cr); }
    if (filtered.empty()) filtered = phys_cols;
    if (!filtered.empty()) candidates.push_back(filtered[(d100()-1) % filtered.size()]);
  }
  if (candidates.empty()) return false;
  const ColumnRef cr = candidates[(d100()-1) % candidates.size()];

  // 2) 基于缓存值生成列谓词；若缓存缺失则使用安全常量
  std::shared_ptr<bool_expr> pred = cb_.build_predicate_for_column(&q, cr, CondContext::HAVING, vc_);
  if (!pred) {
    // 本地固定列引用 + 字面量表达式（用于兜底）
    struct fixed_column_ref_local : value_expr { named_relation* rel; column col; fixed_column_ref_local(prod* p, named_relation* r, const column& c) : value_expr(p), rel(r), col(c) { type = c.type; } virtual void out(std::ostream& o){ o << rel->ident() << "." << col.name; } };
    struct literal_expr_local : value_expr { std::string lit; literal_expr_local(prod* p, sqltype* t, const std::string& s) : value_expr(p), lit(s) { type = t; } virtual void out(std::ostream& o){ o << lit; } };
    auto lhs = std::make_shared<fixed_column_ref_local>(&q, cr.rel, cr.col);
    // 取样本；若无则构造安全常量
    std::string rhs_lit;
    const SampleList* sl = vc_.get(cr.rel->ident(), cr.col.name);
    if (sl && !sl->values.empty()) {
      const Value& v = sl->values[(d100()-1) % sl->values.size()]; rhs_lit = v.literal;
    } else {
      std::string tn = safe_type_name(cr.col.type);
      if (tn=="varchar") rhs_lit = "''";
      else if (tn=="date") rhs_lit = "'1970-01-01'";
      else if (tn=="datetime" || tn=="timestamp") rhs_lit = "'1970-01-01 00:00:00'";
      else if (tn=="time") rhs_lit = "'00:00:00'";
      else if (tn=="year") rhs_lit = "1970";
      else rhs_lit = "0";
    }
    auto rhs = std::make_shared<literal_expr_local>(&q, cr.col.type, rhs_lit);
    // 关系操作符白名单：= 或 !=（随机）
    const char* op = (d6()>3?"=":"!=");
    pred = std::make_shared<cmp_expr_ast>(&q, std::static_pointer_cast<value_expr>(lhs), op, std::static_pointer_cast<value_expr>(rhs));
  }
  if (!pred) return false;

  // 3) 合并到 HAVING：若已有则 AND 组合；否则直接设为该谓词
  if (q.having) {
    std::vector<std::shared_ptr<bool_expr>> pair = { q.having, pred };
    q.having = cb_.compose_predicates(&q, pair);
  } else {
    q.having = pred;
  }
  if (log_on) {
    std::cerr << "[HAVING] injected via agg-arg (physical+cached predicate)" << std::endl;
  }
  return true;
}

// COUNT(col) 兜底：当无法注入“物理列+缓存值”谓词时，注入 COUNT(col) 比较，确保 HAVING 非空
bool ConditionGenerator::ensure_having_count_fallback(query_spec& q) {
  bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
  // 列选择：优先使用 group_by_exprs 中的物理列；否则回退到 scope->refs 的安全列
  named_relation* rel = nullptr; const column* picked = nullptr; std::string alias; std::string colname;
  for (auto &ge : q.group_by_exprs) {
    if (!ge) continue;
    if (auto cref = dynamic_cast<column_reference*>(ge.get())) {
      if (parse_alias_col_text(cref->reference, alias, colname)) {
        for (auto r : q.scope->refs) { if (r && r->ident() == alias) { rel = r; break; } }
        if (rel) { for (auto &c : rel->columns()) { if (c.name == colname) { picked = &c; break; } } }
        if (rel && picked) break;
      }
    }
  }
  if (!rel || !picked) {
    if (!q.scope->refs.empty() && q.scope->refs.back()) {
      rel = q.scope->refs.back();
      auto cols = rel->columns(); if (!cols.empty()) {
        const column* pc = nullptr; for (auto &c : cols) { sqltype* t=c.type; if(!t) continue; std::string tn=safe_type_name(t); if(tn=="json"||tn=="geometry") continue; pc = &c; break; }
        if (!pc) { pc = &cols[0]; }
        picked = pc;
      }
    }
  }
  if (!rel || !picked) {
    if (log_on) std::cerr << "[RET][FALLBACK] 未解析到物理列名，放弃 COUNT(col) 注入" << std::endl;
    return false;
  }
  // 本地 COUNT(arg) 表达式
  struct count_call_local : value_expr {
    named_relation* rel;
    column col;
    // 安全列名来源：使用 std::string 显式传入，并在 column 构造中统一守卫
    count_call_local(prod* p, named_relation* r, const std::string& cname, sqltype* ctype)
      : value_expr(p), rel(r), col(cname, ctype) { type = sqltype::get("int"); }
    void out(std::ostream& o){ o << "COUNT(" << rel->ident() << "." << col.name << ")"; }
  };
  having_guard hctx(&q);
  // 列名安全来源与占位回退
  std::string safe_name = picked ? picked->name : std::string();
  if (safe_name.empty() || safe_name.size() > 1024) {
    safe_name = std::string("ret_col");
    if (log_on) std::cerr << "[RET][SAFE] 使用安全名称构造列 name=ret_col" << std::endl;
  }
  sqltype* ctype = picked ? picked->type : (q.scope && q.scope->schema && q.scope->schema->inttype ? q.scope->schema->inttype : sqltype::get("int"));
  auto cnt = std::make_shared<count_call_local>(&hctx, rel, safe_name, ctype);
  // COUNT(col) 比较：> 0 或 BETWEEN 小范围（0..1）
  int pick = d6();
  std::shared_ptr<bool_expr> pred;
  if (pick > 3) {
    auto zero = std::make_shared<const_expr>(&hctx, sqltype::get("int"));
    struct cmp_expr_ast2 : bool_expr { std::shared_ptr<value_expr> lhs, rhs; std::string op; cmp_expr_ast2(prod* p,const std::shared_ptr<value_expr>& l,const std::string& oper,const std::shared_ptr<value_expr>& r):bool_expr(p),lhs(l),rhs(r),op(oper){} void out(std::ostream& o){ o<<*lhs<<" "<<op<<" "<<*rhs;} void accept(prod_visitor* v){ v->visit(this); lhs->accept(v); rhs->accept(v);} };
    pred = std::make_shared<cmp_expr_ast>(&hctx, std::static_pointer_cast<value_expr>(cnt), std::string(">"), zero);
/* op set duplicated cleaned */
  } else {
    struct cmp_between_ast2 : bool_expr { std::shared_ptr<value_expr> lhs, lo, hi; cmp_between_ast2(prod* p,const std::shared_ptr<value_expr>& l,const std::shared_ptr<value_expr>& a,const std::shared_ptr<value_expr>& b):bool_expr(p),lhs(l),lo(a),hi(b){} void out(std::ostream& o){ o<<*lhs<<" BETWEEN "<<*lo<<" AND "<<*hi; } void accept(prod_visitor* v){ v->visit(this); lhs->accept(v); lo->accept(v); hi->accept(v);} };
    auto lo = std::make_shared<const_expr>(&hctx, sqltype::get("int"));
    auto hi = std::make_shared<const_expr>(&hctx, sqltype::get("int"));
    pred = std::make_shared<cmp_between_ast2>(&hctx, std::static_pointer_cast<value_expr>(cnt), lo, hi);
  }
  if (!pred) return false;
  if (q.having) {
    std::vector<std::shared_ptr<bool_expr>> pair = { q.having, pred };
    q.having = cb_.compose_predicates(&q, pair);
  } else {
    q.having = pred;
  }
  if (log_on) { std::cerr << "[HAVING] injected via COUNT(col) fallback" << std::endl; }
  return true;
}

// 递归应用到子查询：对当前 q 执行插入，并探查 EXISTS 子树递归调用
void ConditionGenerator::apply_recursively_impl(query_spec& q) {
  // 先对当前查询插入
  attach_to_on(q);
  attach_to_where(q);
  attach_to_having(q);
  // 遍历 FROM 中的派生/子查询（table_subquery），递归应用于其内部 query
  if (q.from_clause) {
    for (auto& tr : q.from_clause->reflist) {
      if (!tr) continue;
      if (auto tsq = dynamic_cast<table_subquery*>(tr.get())) {
        if (tsq->query) apply_recursively_impl(*tsq->query);
      }
    }
  }
  // 遍历布尔搜索树，遇到 EXISTS 则递归
  std::function<void(const std::shared_ptr<value_expr>&)> walk_value;
  walk_value = [this, &walk_value](const std::shared_ptr<value_expr>& ve) {
    if (!ve) return;
    if (auto ex = dynamic_cast<exists_predicate*>(ve.get())) {
      if (ex->subquery) {
        bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
        if (log_on) std::cerr << "[EXTRA][scope=EXISTS] enter subquery" << std::endl;
        apply_recursively_impl(*ex->subquery);
      }
      return;
    }
    if (auto bt = dynamic_cast<bool_term*>(ve.get())) {
      walk_value(bt->lhs);
      walk_value(bt->rhs);
      return;
    }
    if (auto cmp = dynamic_cast<comparison_op*>(ve.get())) {
      walk_value(cmp->lhs);
      walk_value(cmp->rhs);
      return;
    }
    if (auto dp = dynamic_cast<distinct_pred*>(ve.get())) {
      walk_value(dp->lhs);
      walk_value(dp->rhs);
      return;
    }
    if (auto np = dynamic_cast<null_predicate*>(ve.get())) {
      walk_value(np->expr);
      return;
    }
    // 其他表达式类型不含子查询，忽略
  };
  // 从 WHERE 搜索树入口开始
  walk_value(std::static_pointer_cast<value_expr>(q.search));
};

void ConditionGenerator::apply_recursively(query_spec& q) {
  bool log_on = (schema_ && (schema_->condgen_debug || schema_->verbose));
  if (!q.from_clause) {
    if (log_on) std::cerr << "[EXTRA][scope=SKIP] reason=NO_CONTEXT" << std::endl;
  }
  apply_recursively_impl(q);
}
