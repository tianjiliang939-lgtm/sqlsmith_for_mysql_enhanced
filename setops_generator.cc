#include "setops_generator.hh"
#include "random.hh"
#include <sstream>
#include <algorithm>

namespace setops {
namespace {

static bool rand_hit(double p){
  if (p <= 0.0) { return false; }
  if (p >= 1.0) { return true; }
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  return dist(smith::rng) < p;
}

// 粗略统计 SELECT 列数：在顶层括号深度为 0 的情况下统计逗号个数 + 1，直到遇到 FROM
static int guess_select_col_count(const std::string& sql){
  // 输入为已 uppercaser/normalize 的 SQL；目标仅估计列数用于 ORDER BY N 构造
  // 若失败，返回 1 作为兜底
  try {
    size_t pos_sel = sql.find("SELECT");
    if (pos_sel == std::string::npos) return 1;
    size_t i = pos_sel + 6; // after SELECT
    // 跳过空白
    while (i < sql.size() && (sql[i]==' '||sql[i]=='\t'||sql[i]=='\n'||sql[i]=='\r')) i++;
    int depth = 0; bool in_sq=false,in_dq=false,in_bt=false,in_lc=false,in_bc=false;
    int commas = 0; 
    for (; i < sql.size(); ++i){
      char c = sql[i];
      // 行注释
      if (in_lc){ if (c=='\n') in_lc=false; continue; }
      if (in_bc){ if (c=='*' && i+1<sql.size() && sql[i+1]=='/') { i++; in_bc=false; } continue; }
      if (in_sq){ if (c=='\''){ if (i+1<sql.size() && sql[i+1]=='\'') i++; else in_sq=false; } continue; }
      if (in_dq){ if (c=='\"'){ if (i+1<sql.size() && sql[i+1]=='\"') i++; else in_dq=false; } continue; }
      if (in_bt){ if (c=='`'){ if (i+1<sql.size() && sql[i+1]=='`') i++; else in_bt=false; } continue; }
      // 进入注释或字符串
      if (c=='-' && i+1<sql.size() && sql[i+1]=='-'){ i++; in_lc=true; continue; }
      if (c=='#'){ in_lc=true; continue; }
      if (c=='/' && i+1<sql.size() && sql[i+1]=='*'){ i++; in_bc=true; continue; }
      if (c=='\''){ in_sq=true; continue; }
      if (c=='\"'){ in_dq=true; continue; }
      if (c=='`'){ in_bt=true; continue; }
      if (c=='(') { depth++; continue; }
      if (c==')') { depth = std::max(0, depth-1); continue; }
      // 终止于顶层 FROM
      if (depth==0){
        // 识别 FROM 关键字（不在字符串/注释）
        if (c=='F' && i+3<sql.size() && sql.substr(i,4)=="FROM"){
          break;
        }
        if (c==',') commas++;
      }
    }
    return std::max(1, commas+1);
  } catch(...) { return 1; }
}

static std::string make_outer_order_by(int n){
  if (n <= 0) { n = 1; }
  std::ostringstream os; os << " ORDER BY ";
  for (int i=1;i<=n;i++){ os << i; if (i<n) os << ","; }
  return os.str();
}

} // anon

std::string maybe_apply_set_ops(bool mysql_mode, const Flags& flags,
                                const std::string& base_sql,
                                bool force_outer_order)
{
  // 约束守卫
  if (!mysql_mode) return base_sql;
  bool any_on = flags.enable_union || flags.enable_union_all || flags.enable_intersect || flags.enable_except;
  if (!any_on) return base_sql;
  if (!rand_hit(std::max(0.0, std::min(1.0, flags.prob)))) return base_sql;

  // 随机选择集合操作类型（在开启的集合中随机挑一个）
  std::vector<SetOpType> choices;
  if (flags.enable_union)       choices.push_back(SetOpType::UNION_DISTINCT);
  if (flags.enable_union_all)   choices.push_back(SetOpType::UNION_ALL);
  if (flags.enable_intersect)   choices.push_back(SetOpType::INTERSECT_DISTINCT);
  if (flags.enable_except)      choices.push_back(SetOpType::EXCEPT_DISTINCT);
  if (choices.empty()) return base_sql;

  std::uniform_int_distribution<size_t> pick(0, choices.size()-1);
  SetOpType ty = choices[pick(smith::rng)];

  // 构造左右子查询：为保证列数与类型一致，最小实现使用 base_sql 本身作为左右两侧
  std::string left = base_sql;
  std::string right = base_sql;

  // 外层 ORDER BY 列数估计（仅用于外层 ORDER WHEN force_outer_order）
  int ncols = 1;
  try { ncols = guess_select_col_count(base_sql); } catch(...) { ncols = 1; }

  // 生成集合表达式；最外层括号包裹子查询，外层可追加 ORDER BY
  std::ostringstream out;
  out << "(" << left << ") ";
  switch (ty) {
    case SetOpType::UNION_DISTINCT: out << "UNION "; break;
    case SetOpType::UNION_ALL:      out << "UNION ALL "; break;
    case SetOpType::INTERSECT_DISTINCT: out << "INTERSECT "; break;
    case SetOpType::EXCEPT_DISTINCT:    out << "EXCEPT "; break;
  }
  out << "(" << right << ")";
  if (force_outer_order) {
    out << make_outer_order_by(ncols);
  }
  // LIMIT 外层附加：保持最小嵌入，不尝试从子查询迁移；如需，可后续增强解析迁移
  return out.str();
}

} // namespace setops
