#include "config.h"
#include "value_catalog.hh"
#include "expr.hh"
#include "mysql.hh"
#ifdef HAVE_LIBMYSQLCLIENT
#include <mysql/mysql.h>
#endif

using namespace std;

static inline string key_of(const string &rel_ident, const string &col) {
  return rel_ident + "." + col;
}

Value ValueCatalog::make_safe_literal(const std::string &tname, const char *raw, SourceTag src) {
  Value v; v.src = src; v.family = family_of(tname);
  if (!raw) { v.is_null = true; v.literal = "NULL"; return v; }
  string s(raw);
  switch (v.family) {
    case TypeFamily::Numeric:
      v.literal = s.size() ? s : string("0"); break;
    case TypeFamily::String:
      v.literal = sql_quote(s); break;
    case TypeFamily::Time:
      {
        std::string tn = tname;
        if (tn=="datetime" || tn=="timestamp") {
          std::string dt = s.size() ? sql_quote(s) : sql_quote("1970-01-01 00:00:00");
          v.literal = std::string("CAST(") + dt + " AS DATETIME)";
        } else if (tn=="date") {
          std::string d = s.size() ? sql_quote(s) : sql_quote("1970-01-01");
          v.literal = std::string("CAST(") + d + " AS DATE)";
        } else if (tn=="time") {
          std::string t = s.size() ? sql_quote(s) : sql_quote("00:00:00");
          v.literal = std::string("CAST(") + t + " AS TIME)";
        } else if (tn=="year") {
          v.literal = s.size() ? s : std::string("1970");
        } else {
          // fallback to DATE cast
          std::string d = s.size() ? sql_quote(s) : sql_quote("1970-01-01");
          v.literal = std::string("CAST(") + d + " AS DATE)";
        }
      }
      break;
    case TypeFamily::Bit:
      // MySQL 返回位串十进制或二进制；统一回退为 IN(0,1) 的源，不直接保存具体值
      v.literal = s.size() ? s : string("0"); break;
    case TypeFamily::Json:
      // 统一用 CAST('..' AS JSON) 包裹（避免伪 JSON 文本导致错误）
      if (s.empty()) s = "{}";
      v.literal = string("CAST(") + sql_quote(s) + " AS JSON)"; break;
    case TypeFamily::Blob:
      // 仅记录长度信息：空串→X''；其它用 HEX 包装（最小实现）
      if (s.empty()) v.literal = "X''";
      else v.literal = string("X'") + s + "'"; // 注意：真实 BLOB 可能不是文本，此处作为安全占位
      break;
    case TypeFamily::EnumSet:
      v.literal = s.size() ? sql_quote(s) : sql_quote("a"); break;
    case TypeFamily::Spatial:
      // 无法直接解析 WKB/WKT，统一回退为安全常量
      v.literal = "ST_GeomFromText('POINT(0 0)', 4326)"; break;
    default:
      v.literal = s.size() ? sql_quote(s) : string("NULL"); break;
  }
  return v;
}

void ValueCatalog::load_base_samples(schema_mysql &sch, int sample_limit_or_minus1) {
  // 无论是否具备 MySQL 客户端库，均打印进入日志与每个基表标识，确保可观察性（受开关控制）
  bool log_on = (sch.condgen_debug || sch.verbose);
  if (log_on) std::cerr << "[VC] Load Table Samples enter" << std::endl;
  for (auto &t : sch.tables) {
    // 仅基表参与采样
    table *tab = dynamic_cast<table*>(&t);
    if (!tab || !tab->is_base_table) continue;
    string rel_ident = tab->ident();
    if (log_on) std::cerr << "Load Table value:" << rel_ident << std::endl;
#ifdef HAVE_LIBMYSQLCLIENT
    // 有 MySQL 客户端库：执行 DISTINCT 采样
    for (auto &c : tab->columns()) {
      string tname = c.type ? safe_type_name(c.type) : string("varchar");
      string qs = string("SELECT DISTINCT ") + c.name + " FROM " + rel_ident + " WHERE " + c.name + " IS NOT NULL";
      if (sample_limit_or_minus1 >= 0) {
        qs += " LIMIT " + to_string(sample_limit_or_minus1);
      }
      try {
        if (log_on) {
          std::cerr << "[VC] sampling " << rel_ident << "." << c.name << " begin" << std::endl;
          std::cerr << "[VC][sql] " << qs << std::endl;
        }
        sch.q(qs);
        MYSQL_RES *res = mysql_store_result(sch.con);
        if (!res) { sch.error(); }
        MYSQL_ROW row;
        SampleList sl; sl.family = family_of(tname); sl.src = SourceTag::Base;
        int cnt = 0;
        while ((row = mysql_fetch_row(res))) {
          Value v = make_safe_literal(tname, row[0], SourceTag::Base);
          // 约定：默认过滤 NULL；如需保留，可将 is_null=true 的样本也 push_back
          if (!v.is_null) { sl.values.push_back(v); cnt++; }
        }
        mysql_free_result(res);
        // 回退策略：失败或空集，补入类型安全常量
        if (sl.values.empty()) {
          Value fallback;
          fallback.family = sl.family; fallback.src = SourceTag::Base; fallback.is_null = false;
          switch (sl.family) {
            case TypeFamily::Numeric: fallback.literal = "0"; break;
            case TypeFamily::String: fallback.literal = sql_quote("a"); break;
            case TypeFamily::Time: fallback.literal = sql_quote("1970-01-01"); break;
            case TypeFamily::Json: fallback.literal = "CAST('{}' AS JSON)"; break;
            case TypeFamily::Blob: fallback.literal = "X''"; break;
            case TypeFamily::Bit: fallback.literal = "0"; break;
            case TypeFamily::EnumSet: fallback.literal = sql_quote("a"); break;
            case TypeFamily::Spatial: fallback.literal = "ST_GeomFromText('POINT(0 0)', 4326)"; break;
            default: fallback.literal = "NULL"; break;
          }
          sl.values.push_back(fallback);
        }
        store_[key_of(rel_ident, c.name)] = std::move(sl);
        if (log_on) std::cerr << "[VC] sampling " << rel_ident << "." << c.name << " done; count=" << cnt << std::endl;
      } catch (std::exception &e) {
        // 失败兜底：写入单个安全常量
        SampleList sl; sl.family = family_of(tname); sl.src = SourceTag::Base;
        Value fb = make_safe_literal(tname, nullptr, SourceTag::Base);
        sl.values.push_back(fb);
        store_[key_of(rel_ident, c.name)] = std::move(sl);
        if (log_on) std::cerr << "[VC] sampling " << rel_ident << "." << c.name << " failed; fallback one" << std::endl;
      }
    }
#else
    // 无 MySQL 客户端库：不执行 SELECT，但仍输出列级 skip 日志与占位缓存键，避免全程静默
    for (auto &c : tab->columns()) {
      string tname = c.type ? safe_type_name(c.type) : string("varchar");
      if (log_on) std::cerr << "[VC] skip SELECT DISTINCT (no client) " << rel_ident << "." << c.name << std::endl;
      SampleList sl; sl.family = family_of(tname); sl.src = SourceTag::Base;
      // 不填充值，保持占位，后续条件构造将做安全回退
      store_[key_of(rel_ident, c.name)] = std::move(sl);
    }
#endif
  }
}

void ValueCatalog::set_temp_samples(const std::string &table_alias, const std::string &col_name,
                                    const std::vector<Value> &vals, TypeFamily fam, SourceTag src) {
  SampleList sl; sl.family = fam; sl.src = src; sl.values = vals;
  store_[key_of(table_alias, col_name)] = std::move(sl);
}

const SampleList* ValueCatalog::get(const std::string &tbl, const std::string &col) const {
  auto k = key_of(tbl, col);
  auto it = store_.find(k);
  if (it != store_.end()) {
    // 本地命中：若值为空，尝试后备或合成样本兜底
    if (it->second.values.empty()) {
      if (fallback_) {
        const SampleList* fb = fallback_->get(tbl, col);
        if (fb && !fb->values.empty()) {
          if (debug_on_) {
            std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=1 source=fallback" << std::endl;
          }
          return fb;
        }
      }
      // 使用已知 family 合成样本（不增加数据库交互）
      SampleList synth; synth.src = SourceTag::Synthetic; synth.family = it->second.family;
      auto push_val = [&](const std::string &lit, TypeFamily fam){ Value v; v.literal = lit; v.is_null = false; v.family = fam; v.src = SourceTag::Synthetic; synth.values.push_back(v); };
      switch (synth.family) {
        case TypeFamily::Numeric: push_val("0", TypeFamily::Numeric); push_val("1", TypeFamily::Numeric); break;
        case TypeFamily::String: push_val(sql_quote("a"), TypeFamily::String); push_val(sql_quote("b"), TypeFamily::String); break;
        case TypeFamily::Time: push_val(sql_quote("1970-01-01"), TypeFamily::Time); break;
        case TypeFamily::Bit: push_val("0", TypeFamily::Bit); push_val("1", TypeFamily::Bit); break;
        case TypeFamily::Json: push_val("CAST('{}' AS JSON)", TypeFamily::Json); break;
        case TypeFamily::Blob: push_val("X''", TypeFamily::Blob); break;
        case TypeFamily::EnumSet: push_val(sql_quote("a"), TypeFamily::EnumSet); break;
        case TypeFamily::Spatial: push_val("ST_GeomFromText('POINT(0 0)', 4326)", TypeFamily::Spatial); break;
        default: { Value vnull = make_safe_literal(std::string("varchar"), nullptr, SourceTag::Synthetic); vnull.is_null = true; synth.values.push_back(vnull); push_val(sql_quote("a"), TypeFamily::String); } break;
      }
      const_cast<ValueCatalog*>(this)->store_[k] = synth;
      if (debug_on_) {
        std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=1 source=synthetic" << std::endl;
      }
      return &const_cast<ValueCatalog*>(this)->store_[k];
    }
    if (debug_on_) {
      std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=1 source=local" << std::endl;
    }
    return &it->second;
  }
  // 本地 miss：若存在后备数据源则回退
  if (fallback_) {
    const SampleList* fb = fallback_->get(tbl, col);
    if (fb) {
      if (debug_on_) {
        std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=1 source=fallback" << std::endl;
      }
      return fb;
    } else {
      if (debug_on_) {
        std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=0 source=fallback" << std::endl;
        std::cerr << "[VC][get] miss reason=SAMPLE_EMPTY" << std::endl;
      }
    }
  } else {
    if (debug_on_) {
      std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=0 source=local" << std::endl;
      std::cerr << "[VC][get] miss reason=SAMPLE_EMPTY" << std::endl;
    }
  }
  // 合成样本兜底：不增加数据库交互；在类型守卫下生成安全最小集合
  // 通过 schema_ 查询类型；若不可得则按 Unknown 处理（保守常量）
  SampleList synth;
  synth.src = SourceTag::Synthetic;
  // 默认族 Unknown
  synth.family = TypeFamily::Unknown;
  std::string tname;
  if (schema_) {
    for (auto &t : schema_->tables) {
      // 基表键为 schema.table，需与 ident() 比较
      if (t.ident() == tbl) {
        for (auto &c : t.columns()) {
          if (c.name == col) {
            tname = c.type ? safe_type_name(c.type) : std::string("varchar");
            synth.family = family_of(tname);
            break;
          }
        }
        break;
      }
    }
  }
  auto push_val = [&](const std::string &lit, TypeFamily fam){ Value v; v.literal = lit; v.is_null = false; v.family = fam; v.src = SourceTag::Synthetic; synth.values.push_back(v); };
  if (synth.family == TypeFamily::Numeric) {
    push_val("0", TypeFamily::Numeric);
    push_val("1", TypeFamily::Numeric);
    push_val("-1", TypeFamily::Numeric);
  } else if (synth.family == TypeFamily::String || synth.family == TypeFamily::EnumSet) {
    push_val(sql_quote("a"), TypeFamily::String);
    push_val(sql_quote("b"), TypeFamily::String);
    push_val(sql_quote("aa"), TypeFamily::String);
  } else if (synth.family == TypeFamily::Time) {
    if (tname == "datetime" || tname == "timestamp") {
      push_val("CAST('1970-01-01 00:00:00' AS DATETIME)", TypeFamily::Time);
      push_val("CAST('1992-03-04 00:00:00' AS DATETIME)", TypeFamily::Time);
    } else if (tname == "date") {
      push_val("CAST('1970-01-01' AS DATE)", TypeFamily::Time);
      push_val("CAST('1992-03-04' AS DATE)", TypeFamily::Time);
    } else if (tname == "time") {
      push_val("CAST('00:00:00' AS TIME)", TypeFamily::Time);
      push_val("CAST('23:59:59' AS TIME)", TypeFamily::Time);
    } else if (tname == "year") {
      push_val("1970", TypeFamily::Time);
      push_val("1971", TypeFamily::Time);
    } else {
      push_val("CAST('1970-01-01' AS DATE)", TypeFamily::Time);
      push_val("CAST('1992-03-04' AS DATE)", TypeFamily::Time);
    }
  } else if (synth.family == TypeFamily::Json) {
    push_val("CAST('{}' AS JSON)", TypeFamily::Json);
  } else if (synth.family == TypeFamily::Blob) {
    push_val("X''", TypeFamily::Blob);
  } else if (synth.family == TypeFamily::Bit) {
    push_val("0", TypeFamily::Bit);
    push_val("1", TypeFamily::Bit);
  } else if (synth.family == TypeFamily::Spatial) {
    push_val("ST_GeomFromText('POINT(0 0)', 4326)", TypeFamily::Spatial);
  } else {
    // Unknown：保守常量集合，避免类型不匹配；倾向字符串与 NULL，占比低
    Value vnull = make_safe_literal(std::string("varchar"), nullptr, SourceTag::Synthetic);
    vnull.is_null = true; synth.values.push_back(vnull);
    push_val(sql_quote("a"), TypeFamily::String);
  }
  // 若仍为空，补 1 个保守常量
  if (synth.values.empty()) {
    push_val(sql_quote("a"), TypeFamily::String);
  }
  // 缓存并返回
  const_cast<ValueCatalog*>(this)->store_[k] = synth;
  if (debug_on_) {
    std::cerr << "[VC][get] key=" << tbl << "." << col << " hit=1 source=synthetic" << std::endl;
  }
  return &const_cast<ValueCatalog*>(this)->store_[k];
}

std::vector<ValueCatalog::Key> ValueCatalog::keys() const {
  std::vector<Key> ks; ks.reserve(store_.size());
  for (const auto &kv : store_) {
    ks.push_back(kv.first);
  }
  return ks;
}
