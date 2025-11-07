#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"
#include "condition_generator.hh"
#include "condition_builder.hh"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "mysql.hh"
#include "setops_generator.hh"

// Fallback stub schema when MySQL client library is unavailable
struct schema_dummy : public schema {
  schema_dummy() { mysql_mode = true; }
  virtual std::string quote_name(const std::string &id) { return std::string("\"") + id + "\""; }
};

#include "postgres.hh"

using namespace std;

using namespace std::chrono;

// ===== Batch Probability Controller (跨语句 1:1 概率控制) =====
#include <deque>
struct BatchProbController {
  long total = 0;            // 目标批次总数（max-queries）
  long generated = 0;        // 已生成语句数
  long win_cnt = 0;          // 含窗口的语句计数
  long agg_cnt = 0;          // 含聚合的语句计数
  double target_win = 0.5;   // 目标比例（默认 50%）
  double target_agg = 0.5;   // 目标比例（默认 50%）
  int W = 50;                // 滑动窗口大小
  std::deque<int> sw_win;    // 最近 W 条窗口出现标记
  std::deque<int> sw_agg;    // 最近 W 条聚合出现标记
  double p_next_win = 0.5;   // 下一条语句窗口倾向概率（动态）
  double p_next_agg = 0.5;   // 下一条语句聚合倾向概率（动态）
  double col_scale_win = 0.2;// 列级窗口概率缩放（约 0.2）
  double col_scale_agg = 0.2;// 列级聚合概率缩放（约 0.2）
  double p_cond = 0.3;       // 条件注入概率（WHERE/ON/HAVING 非必选）
  double p_having = 0.3;     // HAVING 注入概率（当 has_agg）
  bool verbose = false;
  bool condgen_debug = false;
  void init(long tot, double pwin, double pagg, double pcond_, double phav_, bool v, bool cd) {
    total = tot>0?tot:0; generated = 0; win_cnt = 0; agg_cnt = 0;
    target_win = std::max(0.0,std::min(1.0,pwin));
    target_agg = std::max(0.0,std::min(1.0,pagg));
    p_cond = std::max(0.0,std::min(1.0,pcond_));
    p_having = std::max(0.0,std::min(1.0,phav_));
    verbose = v; condgen_debug = cd;
    sw_win.clear(); sw_agg.clear();
    p_next_win = target_win; p_next_agg = target_agg;
  }
  static double clamp01(double x){ return std::max(0.0,std::min(1.0,x)); }
  void prepare_next(){
    long remaining = (total>generated) ? (total - generated) : 1;
    double need_win = std::max(0.0, target_win*total - win_cnt);
    double need_agg = std::max(0.0, target_agg*total - agg_cnt);
    double base_win = clamp01(need_win / (double)remaining);
    double base_agg = clamp01(need_agg / (double)remaining);
    // 滑动窗口软校准：最近 W 条的实际比例偏差>10% 时提高偏向概率
    auto soft_bias = [&](const std::deque<int>& sw, double target){
      if (sw.empty()) return 0.0;
      int sum = 0; for (int x: sw) sum += x; double ratio = (double)sum / (double)sw.size();
	  // 10%
      double eps = 0.10; 
	  // 偏少，提高概率
      if (ratio + eps < target) return +0.10; 
	  // 偏多，降低概率
      if (ratio > target + eps) return -0.10; 
      return 0.0;
    };
    double bias_win = soft_bias(sw_win, target_win);
    double bias_agg = soft_bias(sw_agg, target_agg);
    p_next_win = clamp01(base_win + bias_win);
    p_next_agg = clamp01(base_agg + bias_agg);
    if (verbose || condgen_debug) {
      std::cerr << "[BATCH] total=" << total << " gen=" << generated
                << " win_cnt=" << win_cnt << " agg_cnt=" << agg_cnt
                << " target_win=" << target_win << " target_agg=" << target_agg << std::endl;
      std::cerr << "[PROB] p_next_win=" << p_next_win << " p_next_agg=" << p_next_agg
                << " col_p_win~" << (col_scale_win*p_next_win)
                << " col_p_agg~" << (col_scale_agg*p_next_agg)
                << " p_cond=" << p_cond << " p_having=" << p_having << std::endl;
    }
  }
  void notify_stmt(bool has_win, bool has_agg){
    generated++;
    if (has_win) { win_cnt++; sw_win.push_back(1); } else { sw_win.push_back(0); }
    if (has_agg) { agg_cnt++; sw_agg.push_back(1); } else { sw_agg.push_back(0); }
    while ((int)sw_win.size() > W) sw_win.pop_front();
    while ((int)sw_agg.size() > W) sw_agg.pop_front();
  }
  double col_p_win() const { return clamp01(col_scale_win * p_next_win); }
  double col_p_agg() const { return clamp01(col_scale_agg * p_next_agg); }
};
static BatchProbController g_batch;
extern "C" double batch_get_p_next_win(){ return g_batch.p_next_win; }
extern "C" double batch_get_p_next_agg(){ return g_batch.p_next_agg; }
extern "C" double batch_get_col_p_win(){ return g_batch.col_p_win(); }
extern "C" double batch_get_col_p_agg(){ return g_batch.col_p_agg(); }
extern "C" double batch_get_p_cond(){ return g_batch.p_cond; }
extern "C" double batch_get_p_having(){ return g_batch.p_having; }
extern "C" void batch_prepare_next(){ g_batch.prepare_next(); }
extern "C" void batch_on_stmt_generated(int has_win, int has_agg){ g_batch.notify_stmt(has_win!=0, has_agg!=0); }


// --- Keyword Uppercaser (safe: skip strings, backticks, comments) ---
#include <unordered_set>
#include <sstream>
#include <cctype>
#include <algorithm>
namespace {
inline bool is_tok_char(unsigned char c){ return std::isalnum(c) || c=='_'; }
std::string to_lower(std::string s){ for(char &ch: s){ ch = (char)std::tolower((unsigned char)ch); } return s; }
std::string to_upper(std::string s){ for(char &ch: s){ ch = (char)std::toupper((unsigned char)ch); } return s; }
const std::unordered_set<std::string>& keyword_set(){ static const std::unordered_set<std::string> k = {
"select","insert","update","delete","from","where","join","inner","left","right","full","outer","on","group","by","having","order","limit","offset","union","all","distinct","as","case","when","then","else","end","and","or","not","in","exists","between","like","is","null","over","partition","rows","range","unbounded","preceding","following","current","row","window","create","table","alter","drop","primary","key","foreign","references","check","default","view","trigger","procedure","function","index","using","into","values","set","with","recursive","coalesce","nullif","lateral","cross","json_table","cast"
}; return k; }
std::string uppercaser(const std::string &sql){
  const auto &kw = keyword_set();
  std::string out; out.reserve(sql.size());
  bool in_sq=false, in_dq=false, in_bt=false, in_lc=false, in_bc=false;
  size_t n = sql.size();
  for(size_t i=0; i<n;){
    char c = sql[i];
    if(in_lc){ out.push_back(c); if(c=='\n'){ in_lc=false; } i++; continue; }
    if(in_bc){
      if(c=='*' && i+1<n && sql[i+1]=='/'){ out.push_back('*'); out.push_back('/'); i+=2; in_bc=false; continue; }
      out.push_back(c); i++; continue;
    }
    if(in_sq){
      out.push_back(c);
      if(c=='\''){ if(i+1<n && sql[i+1]=='\''){ out.push_back('\''); i+=2; continue; } in_sq=false; } i++; continue;
    }
    if(in_dq){
      out.push_back(c);
      if(c=='\"'){ if(i+1<n && sql[i+1]=='\"'){ out.push_back('\"'); i+=2; continue; } in_dq=false; } i++; continue;
    }
    if(in_bt){
      out.push_back(c);
      if(c=='`'){ if(i+1<n && sql[i+1]=='`'){ out.push_back('`'); i+=2; continue; } in_bt=false; } i++; continue;
    }
    // not in any protected state
    if(c=='-' && i+1<n && sql[i+1]=='-'){ out.push_back('-'); out.push_back('-'); i+=2; in_lc=true; continue; }
    if(c=='#'){ out.push_back('#'); i++; in_lc=true; continue; }
    if(c=='/' && i+1<n && sql[i+1]=='*'){ out.push_back('/'); out.push_back('*'); i+=2; in_bc=true; continue; }
    if(c=='\''){ out.push_back(c); i++; in_sq=true; continue; }
    if(c=='\"'){ out.push_back(c); i++; in_dq=true; continue; }
    if(c=='`'){ out.push_back(c); i++; in_bt=true; continue; }
    if(is_tok_char((unsigned char)c)){
      size_t j=i; std::string tok; tok.reserve(16);
      while(j<n && is_tok_char((unsigned char)sql[j])){ tok.push_back(sql[j]); j++; }
      std::string lower = to_lower(tok);
      if(kw.find(lower)!=kw.end()) out += to_upper(tok); else out += tok;
      i=j; continue;
    }
    out.push_back(c); i++;
  }
  return out;
}
static std::string normalize_sql_spacing(const std::string &sql){
  std::string out; out.reserve(sql.size());
  bool in_sq=false, in_dq=false, in_bt=false, in_lc=false, in_bc=false;
  std::string line; line.reserve(128);
  auto flush_line = [&](bool append_newline){
    if(!in_sq && !in_dq && !in_bt && !in_lc && !in_bc){
      size_t end = line.size();
      while(end>0 && (line[end-1]==' ' || line[end-1]=='\t')) end--;
      if(end==0){ line.clear(); return; }
      out.append(line, 0, end);
      if(append_newline) out.push_back('\n');
    } else {
      // 保留原样，不进行右侧 trim（避免误伤字符串/注释内容）
      out.append(line);
      if(append_newline) out.push_back('\n');
    }
    line.clear();
  };
  size_t n = sql.size();
  for(size_t i=0; i<n;){
    char c = sql[i];
    // 统一处理 CRLF/CR 为换行
    if(c=='\r'){
      flush_line(true);
      in_lc = false;
      if(i+1<n && sql[i+1]=='\n'){ i+=2; } else { i++; }
      continue;
    }
    if(c=='\n'){
      flush_line(true);
      in_lc = false;
      i++; continue;
    }
    if(in_lc){ line.push_back(c); i++; continue; }
    if(in_bc){
      if(c=='*' && i+1<n && sql[i+1]=='/'){
        line.push_back('*'); line.push_back('/'); i+=2; in_bc=false; continue;
      }
      line.push_back(c); i++; continue;
    }
    if(in_sq){
      line.push_back(c);
      if(c=='\''){
        if(i+1<n && sql[i+1]=='\''){ line.push_back('\''); i+=2; continue; }
        in_sq=false;
      }
      i++; continue;
    }
    if(in_dq){
      line.push_back(c);
      if(c=='\"'){
        if(i+1<n && sql[i+1]=='\"'){ line.push_back('\"'); i+=2; continue; }
        in_dq=false;
      }
      i++; continue;
    }
    if(in_bt){
      line.push_back(c);
      if(c=='`'){
        if(i+1<n && sql[i+1]=='`'){ line.push_back('`'); i+=2; continue; }
        in_bt=false;
      }
      i++; continue;
    }
    // 不在任何受保护状态下
    if(c=='-' && i+1<n && sql[i+1]=='-'){ line.push_back('-'); line.push_back('-'); i+=2; in_lc=true; continue; }
    if(c=='#'){ line.push_back('#'); i++; in_lc=true; continue; }
    if(c=='/' && i+1<n && sql[i+1]=='*'){ line.push_back('/'); line.push_back('*'); i+=2; in_bc=true; continue; }
    if(c=='\''){ line.push_back(c); i++; in_sq=true; continue; }
    if(c=='\"'){ line.push_back(c); i++; in_dq=true; continue; }
    if(c=='`'){ line.push_back(c); i++; in_bt=true; continue; }
    // 默认拷贝字符
    line.push_back(c); i++;
  }
  // 刷新最后一行（不追加换行符）
  flush_line(false);
  return out;
}


// --- Error log: environment and sample cache printer ---
static std::string parse_mysql_field(const std::string &conninfo, const char *field) {
  // conninfo: host:port/dbname?username=xxx&password=xxx
  // field: host|port|dbname|dbuser|userpassword
  if (conninfo.empty()) return std::string("");
  // naive split
  std::string host, port, dbname, user, pass;
  try {
    size_t p1 = conninfo.find(':');
    size_t p2 = conninfo.find('/');
    size_t p3 = conninfo.find('?');
    if (p1!=std::string::npos) host = conninfo.substr(0, p1);
    if (p1!=std::string::npos && p2!=std::string::npos && p2>p1) port = conninfo.substr(p1+1, p2-p1-1);
    if (p2!=std::string::npos && p3!=std::string::npos && p3>p2) dbname = conninfo.substr(p2+1, p3-p2-1);
    if (p3!=std::string::npos) {
	  // username=xx&password=yy
      std::string tail = conninfo.substr(p3+1); 
      size_t a = tail.find("username=");
      size_t amp = tail.find('&');
      if (a!=std::string::npos) {
        size_t start = a+9; size_t end = amp==std::string::npos ? tail.size() : amp;
        user = tail.substr(start, end-start);
      }
      size_t b = tail.find("password=");
      if (b!=std::string::npos) {
        size_t start = b+9; pass = tail.substr(start);
      }
    }
  } catch (...) {}
  if      (std::string(field)=="host") return host;
  else if (std::string(field)=="port") return port;
  else if (std::string(field)=="dbname") return dbname;
  else if (std::string(field)=="dbuser") return user;
  else if (std::string(field)=="userpassword") return pass;
  return std::string("");
}

static std::string truncate_ellip(const std::string &s, size_t maxlen) {
  if (s.size() <= maxlen) return s;
  std::string t = s.substr(0, maxlen);
  return t + "...";
}

static std::string compact_literal_for_log(const Value &v) {
  // 为日志做适度压缩/截断，避免过长与换行
  std::string lit = v.literal;
  // 替换换行与制表为空格
  for (char &c : lit) { if (c=='\n' || c=='\r' || c=='\t') c=' '; }
  switch (v.family) {
    case TypeFamily::String:
    case TypeFamily::EnumSet:
      return truncate_ellip(lit, 64);
    case TypeFamily::Json:
      return truncate_ellip(lit, 96);
    case TypeFamily::Blob: {
      // 仅打印长度与前若干字节的十六进制片段（如果是 X'..' 形式）
      if (lit.size()>=3 && lit[0]=='X' && lit[1]=='\'' ) {
        size_t end = lit.rfind('\'');
        std::string hex = (end!=std::string::npos && end>2) ? lit.substr(2, end-2) : std::string("");
        std::string head = hex.substr(0, 16);
        return std::string("0x") + head + (hex.size()>16?"...":"") + "(len=" + std::to_string(hex.size()) + ")";
      }
      return truncate_ellip(lit, 32);
    }
    case TypeFamily::Spatial:
      return truncate_ellip(lit, 64);
    default:
      return truncate_ellip(lit, 64);
  }
}

static void log_env_and_samples(schema *sch, const std::string &mysql_conninfo, const ValueCatalog *vc) {
  // 1) 连接信息
  std::cerr << "host:" << parse_mysql_field(mysql_conninfo, "host") << std::endl;
  std::cerr << "port:" << parse_mysql_field(mysql_conninfo, "port") << std::endl;
  std::cerr << "dbname:" << parse_mysql_field(mysql_conninfo, "dbname") << std::endl;
  std::cerr << "dbuser:" << parse_mysql_field(mysql_conninfo, "dbuser") << std::endl;
  std::cerr << "userpassowrd:" << parse_mysql_field(mysql_conninfo, "userpassword") << std::endl;
#ifdef HAVE_LIBMYSQLCLIENT
  std::cerr << "MySQL client version: " << mysql_get_client_info() << std::endl;
  if (auto sm = dynamic_cast<schema_mysql*>(sch)) {
    std::cerr << "MySQL server version: " << sm->version << std::endl;
  } else {
    std::cerr << "MySQL server version: " << std::string("") << std::endl;
  }
#else
  std::cerr << "MySQL client version: " << std::string("") << std::endl;
  std::cerr << "MySQL server version: " << std::string("") << std::endl;
#endif
  // 2) 加载过程标识与类型提示
  std::cerr << "Loading tables...done." << std::endl;
  std::cerr << "Loading columns...done." << std::endl;
  for (auto &t : sqltype::typemap) {
    if (t.second) std::cerr << "type found: " << t.second->name << std::endl;
  }
  // 3) 表样本缓存（每列最多5个）
  if (vc) {
    bool log_values = false;
    if (sch) {
      // 仅当 verbose 或 condgen_debug 开启时打印 value begin/done 与样本列表
      log_values = (sch->verbose || sch->condgen_debug);
    }
	// 默认关闭，避免噪音
    if (!log_values) return; 
    // 聚合为：table -> vector of (col, sample list)
    std::map<std::string, std::vector<std::pair<std::string,const SampleList*>>> grouped;
    auto keys = vc->keys();
    for (const auto &k : keys) {
      size_t pos = k.rfind('.');
      if (pos==std::string::npos) continue;
      std::string tbl = k.substr(0, pos);
      std::string col = k.substr(pos+1);
      const SampleList* sl = vc->get(tbl, col);
      grouped[tbl].push_back({col, sl});
    }
    for (const auto &it : grouped) {
      const std::string &tbl = it.first;
      std::cerr << tbl << " value begin ..." << std::endl;
      // 按列名排序
      auto cols = it.second;
      std::sort(cols.begin(), cols.end(), [](const std::pair<std::string,const SampleList*> &a, const std::pair<std::string,const SampleList*> &b){ return a.first < b.first; });
      for (const auto &p : cols) {
        const std::string &col = p.first;
        const SampleList* sl = p.second;
        std::cerr << tbl << "->" << col << ": ";
        int cnt = 0;
        if (sl) {
          for (const auto &v : sl->values) {
            std::cerr << compact_literal_for_log(v);
            cnt++;
            if (cnt>=5) break;
            if (cnt < (int)sl->values.size() && cnt<5) std::cerr << ",";
          }
        }
        std::cerr << std::endl;
      }
      std::cerr << tbl << " value done" << std::endl;
    }
  }
}
} // anonymous namespace

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

/* make the cerr logger globally accessible so we can emit one last
   report on SIGINT */
cerr_logger *global_cerr_logger;

extern "C" void cerr_log_handler(int)
{
  if (global_cerr_logger)
    global_cerr_logger->report();
  exit(1);
}

int main(int argc, char *argv[])
{
  cerr << PACKAGE_NAME " " GITREV << endl;

  map<string,string> options;
  regex optregex("--(help|log-to|verbose|target|sqlite|mysql|monetdb|version|dump-all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-state|exclude-catalog|enable-json|enable-spatial|srid-set|json-density|spatial-density|enable-window|disable-window|enable-window-agg|disable-window-agg|enable-temp-dataset|disable-temp-dataset|enable-temp-set|enable-condgen|mysql-extra-conds|distinct-sample-limit|extra-conds-density|condgen-debug|force-order-by|allow-false-cond|disable-nondeterministic-funcs|exclude-found_rows|enable-select|enable-insert|enable-delete|enable-merge|enable-upsert|enable-update|enable-returning|enable-insert-returning|enable-upsert-returning|enable-merge-returning|enable-fulltext|fulltext-prob|enable-union|enable-union-all|enable-intersect|enable-except|enable-cte|enable-lateral|customer_hint)(?:=((?:.|\n)*))?");
  
  for(char **opt = argv+1 ;opt < argv+argc; opt++) {
    smatch match;
    string s(*opt);
    if (regex_match(s, match, optregex)) {
      options[string(match[1])] = match[2];
    } else {
      cerr << "Cannot parse option: " << *opt << endl;
      options["help"] = "";
    }
  }
  // 参数解析结果（临时日志）：MySQL 连接与关键开关
  std::cerr << "[CLI] mysql=" << (options.count("mysql")?options["mysql"]:std::string(""))
            << " enable-condgen=" << (options.count("enable-condgen")?1:0)
            << " mysql-extra-conds=" << (options.count("mysql-extra-conds")?1:0)
            << " distinct-sample-limit=" << (options.count("distinct-sample-limit")?options["distinct-sample-limit"]:std::string(""))
            << " dry-run=" << (options.count("dry-run")?1:0)
            << std::endl;

  if (options.count("help")) {
    cerr <<
      "    --target=connstr     postgres database to send queries to" << endl <<
#ifdef HAVE_LIBSQLITE3
      "    --sqlite=URI         SQLite database to send queries to" << endl <<
#endif
#ifdef HAVE_LIBMYSQLCLIENT
      "    --mysql=conninfo     MySQL database to send queries to" << endl <<
#endif
#ifdef HAVE_MONETDB
      "    --monetdb=connstr    MonetDB database to send queries to" <<endl <<
#endif
      "    --log-to=connstr     log errors to postgres database" << endl <<
      "    --seed=int           seed RNG with specified int instead of PID" << endl <<
      "    --dump-all-queries   print queries as they are generated" << endl <<
      "    --dump-all-graphs    dump generated ASTs" << endl <<
      "    --dry-run            print queries instead of executing them" << endl <<
      "    --exclude-catalog    don't generate queries using catalog relations" << endl <<
      "    --max-queries=long   terminate after generating this many queries" << endl <<
      "    --rng-state=string    deserialize dumped rng state" << endl <<
      "    --verbose            emit progress output" << endl <<
      "    --version            print version information and exit" << endl <<
      "    --help               print available command line options and exit" << endl <<
      "    --enable-json        enable JSON expression/table generation (default OFF)" << endl <<
      "    --enable-spatial     enable Spatial expression generation (default OFF)" << endl <<
      "    --enable-window      enable window function generation (default OFF)" << endl <<
      "    --enable-cte         enable CTE generation (MySQL only; default OFF)" << endl <<
      "    --customer_hint      user-defined hint inserted right after SELECT of the outermost query (default: empty; inserts nothing)" << endl <<
      "    --srid-set           [deprecated] fixed to 4326; ignored if provided" << endl <<
      "    --json-density=float density 0..1 controlling JSON expr frequency" << endl <<
      "    --spatial-density=float density 0..1 controlling Spatial expr frequency" << endl <<
      "    --enable-condgen       enable condition generation (AST semantics, MySQL only, default off)" << endl <<
      "    --distinct-sample-limit=int   DISTINCT sampling limit per column (K); if omitted, sample all (no LIMIT)" << endl <<
      "    --extra-conds-density=float   independent density 0..1 controlling extra conditions share; only effective if provided" << endl <<
      "    --condgen-debug               enable ConditionGen debug logs ([VC] sampling and value begin/done); default off" << endl <<
      "    --force-order-by         when enabled, append ORDER BY 1,2,... to every query (including subqueries); default off" << endl <<
      "    --allow-false-cond       when enabled, allow constant false conditions in WHERE/ON to appear at original probability; default off" << endl <<
      "    --disable-nondeterministic-funcs  when enabled, filter out nondeterministic MySQL functions from generation (NOW/SYSDATE/RAND/UUID/CONNECTION_ID/etc.); default off" << endl <<
      "    --exclude-found_rows  exclude FOUND_ROWS system function from generation (default OFF)" << endl <<
      "    --enable-select        enable SELECT statement generation (default ON if no statement flags given)" << endl <<
      "    --enable-insert        enable INSERT statement generation (default OFF)" << endl <<
      "    --enable-delete        enable DELETE RETURNING statement generation (default OFF)" << endl <<
      "    --enable-merge         enable MERGE statement generation (default OFF)" << endl <<
      "    --enable-upsert        enable UPSERT (INSERT ... ON CONFLICT DO UPDATE) statement generation (default OFF)" << endl <<
      "    --enable-update        enable UPDATE RETURNING statement generation (default OFF)" << endl <<
      "    --enable-returning     globally enable DML RETURNING for supported dialects (default OFF)" << endl <<
      "    --enable-insert-returning  enable INSERT ... RETURNING (default OFF; requires dialect support)" << endl <<
      "    --enable-upsert-returning  enable UPSERT ... RETURNING (default OFF; requires dialect support)" << endl <<
      "    --enable-merge-returning   enable MERGE ... RETURNING (default OFF; requires dialect support)" << endl <<
      "    --enable-fulltext       enable fulltext predicate generation on string columns (WHERE/ON; default OFF)" << endl <<
      "    --fulltext-prob=float  probability 0..1 to prefer fulltext predicate when enabled (default 0.8)" << endl <<
      "    --enable-union         enable UNION (MySQL mode only; default OFF)" << endl <<
      "    --enable-union-all     enable UNION ALL (MySQL mode only; default OFF)" << endl <<
      "    --enable-intersect     enable INTERSECT (MySQL 8.0+; default OFF; no hard version check)" << endl <<
      "    --enable-except        enable EXCEPT (MySQL 8.0+; default OFF; no hard version check)" << endl <<
      "    --enable-temp-set      enable temporary datasets (VALUES/JSON_TABLE) generation and scanning (default OFF)" << endl <<
      "    --enable-lateral       enable LATERAL derived table generation (default OFF; effective for PostgreSQL and MySQL 8.0 dialects)" << endl;

    return 0;
  } else if (options.count("version")) {
    return 0;
  }

  try
    {
      shared_ptr<schema> schema;
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
        schema = make_shared<schema_sqlite>(options["sqlite"], options.count("exclude-catalog"));
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        return 1;
#endif
      }
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB
        schema = make_shared<schema_monetdb>(options["monetdb"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
        return 1;
#endif
      } else if (options.count("mysql")) {

#ifdef HAVE_LIBMYSQLCLIENT
try {
schema = make_shared<schema_mysql>(options["mysql"]);
// [MySQL Mode] CLI 指定 product=mysql 时立即启用 mysql_mode（双保险，构造已启用）
if (schema) schema->mysql_mode = true;
} catch (const std::exception &e) {
  // 远端不可达或连接失败：回退 dummy schema 继续 dry-run 路径；stderr 打印连接错误
  std::cerr << e.what() << std::endl;
  schema = make_shared<schema_dummy>();
  if (schema) schema->mysql_mode = true;
  // 填充最小基表与列，确保 dry-run 生成稳定（避免空表导致的随机选择异常）
  {
    table t("t_dummy", "sqlsmith_test", true /*insertable*/, true /*base_table*/);
    t.columns().push_back(column("bigint_col", sqltype::get("bigint")));
    t.columns().push_back(column("varchar_col", sqltype::get("varchar")));
    t.columns().push_back(column("date_col", sqltype::get("date")));
    t.columns().push_back(column("binary_col", sqltype::get("binary")));
    t.columns().push_back(column("json_col", sqltype::get("json")));
    t.columns().push_back(column("geom_col", sqltype::get("geometry")));
    schema->tables.push_back(t);
    schema->base_tables.push_back(&schema->tables.back());
  }
}
#else
schema = make_shared<schema_dummy>();
// 没有 MySQL 客户端库时，使用 dummy schema 继续 dry-run/生成流程
if (schema) schema->mysql_mode = true;
// 填充最小基表与列，确保 dry-run 生成稳定（避免空表导致的随机选择异常）
{
  table t("t_dummy", "sqlsmith_test", true /*insertable*/, true /*base_table*/);
  t.columns().push_back(column("bigint_col", sqltype::get("bigint")));
  t.columns().push_back(column("varchar_col", sqltype::get("varchar")));
  t.columns().push_back(column("date_col", sqltype::get("date")));
  t.columns().push_back(column("binary_col", sqltype::get("binary")));
  t.columns().push_back(column("json_col", sqltype::get("json")));
  t.columns().push_back(column("geom_col", sqltype::get("geometry")));
  schema->tables.push_back(t);
  schema->base_tables.push_back(&schema->tables.back());
}
#endif

      }
      else
        schema = make_shared<schema_pqxx>(options["target"], options.count("exclude-catalog"));

      // [EARLY INIT] 在 fill_scope 之前将 CLI 开关与密度、SRID 注入到 schema
      if (schema) {
        schema->enable_json = options.count("enable-json");
        schema->enable_spatial = options.count("enable-spatial");
        schema->enable_cte = options.count("enable-cte");
        // 客户自定义 hint：默认空字符串，非空时注入到最外层 SELECT 后
        schema->customer_hint = options.count("customer_hint") ? options["customer_hint"] : std::string("");
        if (options.count("json-density")) {
          try { schema->json_density = stod(options["json-density"]); } catch(...) {}
        }
        if (options.count("spatial-density")) {
          try { schema->spatial_density = stod(options["spatial-density"]); } catch(...) {}
        }
        if (options.count("srid-set")) {
          // [deprecated] 固定 SRID=4326；忽略传入值，仅打印一次性提示
          std::cerr << "[warn] srid-set已废弃，系统固定使用4326（忽略传入值）" << std::endl;
          // 无操作：生成层统一使用 4326（参见 spatial_expr::pick_srid）
          schema->srid_set.clear();
        }
        // 开关：窗口函数 / 聚合窗口函数 / 临时数据集（默认开启）
        {
          bool window_on = false;
          if (options.count("enable-window")) window_on = true;
          if (options.count("disable-window")) window_on = false;
          schema->feature.window_enabled = window_on;

          bool window_agg_on = options.count("enable-window-agg");
          if (options.count("disable-window-agg")) window_agg_on = false;
		  // 窗口关闭则窗口聚合强制关闭
          if (!window_on) window_agg_on = false; 
          schema->feature.window_agg_enabled = window_agg_on;

          bool temp_on = false;
          if (options.count("enable-temp-set")) temp_on = true;
          else {
            if (options.count("disable-temp-dataset")) temp_on = false;
            else if (options.count("enable-temp-dataset")) temp_on = true;
          }
          schema->feature.temp_dataset_enabled = temp_on;
        }
        // LATERAL：仅在支持方言（PostgreSQL）下允许开启；默认关闭
        {
          bool lateral_on = options.count("enable-lateral");
          bool supported = (dynamic_cast<schema_pqxx*>(schema.get()) != nullptr) || (schema->mysql_mode);
          schema->feature.lateral_enabled = lateral_on; // 统一开关仅表示“用户请求开启”，具体输出由 supports_lateral(schema) 控制
          if (lateral_on && !supported) {
            std::cerr << "[LATERAL] requested but not supported under current dialect; printing will fallback to ordinary subquery." << std::endl;
          }
        }
        // 统一 ORDER BY 开关（默认关闭）：开启时对每个查询与子查询均生成基于列位置的 ORDER BY 1,2,...
        schema->feature.force_order_by = options.count("force-order-by");
        // 恒假条件开关（默认关闭）：开启后恢复原有 false 注入概率；关闭时禁止在 WHERE/ON 注入恒假常量
        schema->feature.allow_false_cond = options.count("allow-false-cond");
        // 不确定函数禁用开关（默认关闭）：开启后清单函数不会出现在生成的 SQL 中
        schema->feature.disable_nondeterministic_funcs = options.count("disable-nondeterministic-funcs");
        // FOUND_ROWS 排除开关（默认关闭）：开启后剔除 FOUND_ROWS 系统函数
        schema->feature.exclude_found_rows = options.count("exclude-found_rows");
        // 全文检索开关与概率（默认关闭；概率默认 0.8）
        schema->feature.fulltext_enable = options.count("enable-fulltext");
        schema->feature.fulltext_prob = 0.8;
        if (options.count("fulltext-prob")) { try { schema->feature.fulltext_prob = stod(options["fulltext-prob"]); } catch(...) {} }
        if (schema->verbose || schema->condgen_debug) {
          std::cerr << "[CFG] fulltext_enable=" << (schema->feature.fulltext_enable?1:0)
                    << " fulltext_prob=" << schema->feature.fulltext_prob << std::endl;
        }
        // 语句类型开关（默认 select-only；若任一 enable-* 显式出现，则按显式集合启用）
        {
          bool any_stmt_flag = options.count("enable-select") || options.count("enable-insert") || options.count("enable-delete") ||
                               options.count("enable-merge")  || options.count("enable-upsert") || options.count("enable-update");
          if (!any_stmt_flag) {
            schema->feature.stmt_enable_select = true;
            schema->feature.stmt_enable_insert = false;
            schema->feature.stmt_enable_delete = false;
            schema->feature.stmt_enable_merge  = false;
            schema->feature.stmt_enable_upsert = false;
            schema->feature.stmt_enable_update = false;
          } else {
            schema->feature.stmt_enable_select = options.count("enable-select");
            schema->feature.stmt_enable_insert = options.count("enable-insert");
            schema->feature.stmt_enable_delete = options.count("enable-delete");
            schema->feature.stmt_enable_merge  = options.count("enable-merge");
            schema->feature.stmt_enable_upsert = options.count("enable-upsert");
            schema->feature.stmt_enable_update = options.count("enable-update");
          }
        }
        // RETURNING 控制开关（默认关闭；支持全局与按类型）
        {
          schema->feature.ret_enable_global = options.count("enable-returning");
          bool rin = options.count("enable-insert-returning") || schema->feature.ret_enable_global;
          bool rup = options.count("enable-upsert-returnning") || options.count("enable-upsert-returning") || schema->feature.ret_enable_global;
          bool rmg = options.count("enable-merge-returning") || schema->feature.ret_enable_global;
          schema->feature.ret_enable_insert = rin;
          schema->feature.ret_enable_upsert = rup;
          schema->feature.ret_enable_merge  = rmg;
        }
        // 条件生成附加参数：独立密度与调试开关（默认关闭）
        schema->verbose = options.count("verbose");
        schema->condgen_debug = options.count("condgen-debug");
        if (options.count("extra-conds-density")) {
          try { schema->extra_conds_density = stod(options["extra-conds-density"]); } catch(...) { schema->extra_conds_density = -1.0; }
        }
      }

      // ===== Init Batch Prob Controller (after schema + options wiring) =====
      {
        long mq = options.count("max-queries") ? stol(options["max-queries"]) : 100;
        // env overrides: SQLSMITH_P_WIN / SQLSMITH_P_AGG / SQLSMITH_P_COND / SQLSMITH_P_HAVING
        auto get_envf = [&](const char* k, double defv){ const char* v = getenv(k); if(!v) return defv; try { return stod(std::string(v)); } catch(...) { return defv; } };
        double pwin = get_envf("SQLSMITH_P_WIN", 0.5);
        double pagg = get_envf("SQLSMITH_P_AGG", 0.5);
        double pcond = get_envf("SQLSMITH_P_COND", 0.3);
        double phav = get_envf("SQLSMITH_P_HAVING", 0.3);
        g_batch.init(mq, pwin, pagg, pcond, phav, schema->verbose, schema->condgen_debug);
        if (schema->verbose || schema->condgen_debug) {
          std::cerr << "[BATCH] init total=" << mq << " p_win=" << pwin << " p_agg=" << pagg
                    << " p_cond=" << pcond << " p_having=" << phav << std::endl;
        }
      }

      // 环境快照（仅在 --condgen-debug 或 --verbose 开启时输出）
      if (options.count("condgen-debug") || options.count("verbose")) {
        std::cerr << "[CFG] enable-condgen=" << (options.count("enable-condgen")?1:0)
                  << " mysql-extra-conds=" << (options.count("mysql-extra-conds")?1:0)
                  << " extra-conds-density=" << (options.count("extra-conds-density")?options["extra-conds-density"]:std::string(""))
                  << " condgen-debug=" << (options.count("condgen-debug")?1:0)
                  << " verbose=" << (options.count("verbose")?1:0)
                  << " stmt-enable-select=" << (schema->feature.stmt_enable_select?1:0)
                  << " stmt-enable-insert=" << (schema->feature.stmt_enable_insert?1:0)
                  << " stmt-enable-delete=" << (schema->feature.stmt_enable_delete?1:0)
                  << " stmt-enable-merge=" << (schema->feature.stmt_enable_merge?1:0)
                  << " stmt-enable-upsert=" << (schema->feature.stmt_enable_upsert?1:0)
                  << " stmt-enable-update=" << (schema->feature.stmt_enable_update?1:0)
                  << " ret-global=" << (schema->feature.ret_enable_global?1:0)
                  << " ret-insert=" << (schema->feature.ret_enable_insert?1:0)
                  << " ret-upsert=" << (schema->feature.ret_enable_upsert?1:0)
                  << " ret-merge=" << (schema->feature.ret_enable_merge?1:0)
                  << std::endl;
      }

      scope scope;
      long queries_generated = 0;
      schema->fill_scope(scope);
      // 当 product=mysql 而 mysql_mode 未开启时，发警告并强制开启，防止时序缺陷
      if (options.count("mysql") && !schema->mysql_mode) {
        cerr << "[warn] product=mysql but mysql_mode was false; enabling early for grammar/factory" << endl;
        schema->mysql_mode = true;
      }
      std::cerr << "[Mode] mysql_mode=" << (schema->mysql_mode?1:0) << std::endl;

      if (options.count("rng-state")) {
           istringstream(options["rng-state"]) >> smith::rng;
      } else {
           smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());
      }

      vector<shared_ptr<logger> > loggers;

      loggers.push_back(make_shared<impedance_feedback>());

      if (options.count("log-to"))
        loggers.push_back(make_shared<pqxx_logger>(
             options.count("sqlite") ? options["sqlite"] : options["target"],
             options["log-to"], *schema));

      if (options.count("verbose")) {
        auto l = make_shared<cerr_logger>();
        global_cerr_logger = &*l;
        loggers.push_back(l);
        signal(SIGINT, cerr_log_handler);
      }
      
      if (options.count("dump-all-graphs"))
        loggers.push_back(make_shared<ast_logger>());

      if (options.count("dump-all-queries"))
        loggers.push_back(make_shared<query_dumper>());

      if (options.count("dry-run")) {
        while (1) {
          batch_prepare_next();
          shared_ptr<prod> gen = statement_factory(&scope);
          bool condgen_on = (schema && schema->mysql_mode && (options.count("enable-condgen") || options.count("mysql-extra-conds")));
          if (condgen_on) {
            // AST 语义：在序列化前将条件子树插入到 AST
            if (auto q = dynamic_cast<query_spec*>(gen.get())) {
              // 初始化采样（仅一次），并在本次语句中解析临时数据集样本
              int sample_lim = -1;
              if (options.count("distinct-sample-limit")) {
                try { sample_lim = stoi(options["distinct-sample-limit"]); } catch(...) { sample_lim = -1; }
              }
              ConditionGenerator cg(schema.get());
#ifdef HAVE_LIBMYSQLCLIENT
              if (auto sm = dynamic_cast<schema_mysql*>(schema.get())) {
                std::cerr << "[SchemaMySQL] init_samples begin" << std::endl;
                sm->init_samples(sample_lim);
                std::cerr << "[SchemaMySQL] init_samples end" << std::endl;
              }
#else
              std::cerr << "[CondGen] init_sampling skip (no MySQL client)" << std::endl;
#endif
              cg.apply_recursively(*q);
              // 打印环境信息与样本缓存（每列最多5个），不影响 stdout 的 SQL
              log_env_and_samples(schema.get(), options.count("mysql")?options["mysql"]:std::string(""), &cg.vc_);
            }
            {
              std::ostringstream __s; gen->out(__s);
              std::string __sql = normalize_sql_spacing(uppercaser(__s.str()));
              setops::Flags __so;
              __so.enable_union = options.count("enable-union");
              __so.enable_union_all = options.count("enable-union-all");
              __so.enable_intersect = options.count("enable-intersect");
              __so.enable_except = options.count("enable-except");
              __so.prob = batch_get_p_cond();
              std::string __sql2 = setops::maybe_apply_set_ops(schema->mysql_mode, __so, __sql, schema->feature.force_order_by);
              cout << __sql2;
            }
          } else {
            {
              std::ostringstream __s; gen->out(__s);
              // 打印环境信息与样本缓存（可能为空 ValueCatalog）
              log_env_and_samples(schema.get(), options.count("mysql")?options["mysql"]:std::string(""), nullptr);
              std::string __sql = normalize_sql_spacing(uppercaser(__s.str()));
              setops::Flags __so;
              __so.enable_union = options.count("enable-union");
              __so.enable_union_all = options.count("enable-union-all");
              __so.enable_intersect = options.count("enable-intersect");
              __so.enable_except = options.count("enable-except");
              __so.prob = batch_get_p_cond();
              std::string __sql2 = setops::maybe_apply_set_ops(schema->mysql_mode, __so, __sql, schema->feature.force_order_by);
              cout << __sql2;
            }
          }
          for (auto l : loggers)
            l->generated(*gen);
          cout << ";" << endl;
          queries_generated++;
          // 清理 IN 集合注册表（expr/condgen），避免跨语句复用/UAF
          const void* stmt_key = (const void*)scope.stmt_seq.get();
          expr_utils::clear_inset_registry_for_stmt_key(stmt_key);
          condgen_utils::clear_inset_registry_for_stmt_key(stmt_key);

          if (options.count("max-queries")
              && (queries_generated >= stol(options["max-queries"])))
              return 0;
        }
      }

      shared_ptr<dut_base> dut;
      
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
        dut = make_shared<dut_sqlite>(options["sqlite"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        return 1;
#endif
      }
#ifdef HAVE_LIBMYSQLCLIENT
      else if (options.count("mysql")) {
                  dut = make_shared<dut_mysql>(options["mysql"]);
      }
#else
      else if (options.count("mysql")) {
                  // 无 MySQL 客户端库：执行路径降级为 libpq，若 target 未提供则直接进行 dry-run
                  dut = make_shared<dut_libpq>(options["target"]);
      }
#endif
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB        
        dut = make_shared<dut_monetdb>(options["monetdb"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
        return 1;
#endif
      }
      else
        dut = make_shared<dut_libpq>(options["target"]);

      while (1) /* Loop to recover connection loss */
      {
        try {
            while (1) { /* Main loop */

            if (options.count("max-queries")
                && (++queries_generated > stol(options["max-queries"]))){
              if (global_cerr_logger)
                global_cerr_logger->report();
              return 0;
            }
            
            /* Invoke top-level production to generate AST */
            batch_prepare_next();
            shared_ptr<prod> gen = statement_factory(&scope);

            for (auto l : loggers)
              l->generated(*gen);
          
            // 条件生成钩子：仅 MySQL 模式下且开关开启（AST 注入）
            bool condgen_on = (schema && schema->mysql_mode && (options.count("enable-condgen") || options.count("mysql-extra-conds")));
            string sql_to_exec;
            const ValueCatalog* vc_ptr_for_log = nullptr;
            if (condgen_on) {
              if (auto q = dynamic_cast<query_spec*>(gen.get())) {
                int sample_lim = -1;
                if (options.count("distinct-sample-limit")) {
                  try { sample_lim = stoi(options["distinct-sample-limit"]); } catch(...) { sample_lim = -1; }
                }
                ConditionGenerator cg(schema.get());
#ifdef HAVE_LIBMYSQLCLIENT
                if (auto sm = dynamic_cast<schema_mysql*>(schema.get())) {
                  std::cerr << "[SchemaMySQL] init_samples begin" << std::endl;
                  sm->init_samples(sample_lim);
                  std::cerr << "[SchemaMySQL] init_samples end" << std::endl;
                }
#else
                std::cerr << "[CondGen] init_sampling skip (no MySQL client)" << std::endl;
#endif
                cg.apply_recursively(*q);
                vc_ptr_for_log = &cg.vc_;
              }
            }

            /* Generate SQL from AST */
            ostringstream s;
            gen->out(s);
            // 打印环境信息与样本缓存（在 SQL 文本缓冲完成后、执行之前）
            log_env_and_samples(schema.get(), options.count("mysql")?options["mysql"]:std::string(""), vc_ptr_for_log);
            sql_to_exec = normalize_sql_spacing(uppercaser(s.str()));
            {
              setops::Flags __so;
              __so.enable_union = options.count("enable-union");
              __so.enable_union_all = options.count("enable-union-all");
              __so.enable_intersect = options.count("enable-intersect");
              __so.enable_except = options.count("enable-except");
              __so.prob = batch_get_p_cond();
              sql_to_exec = setops::maybe_apply_set_ops(schema->mysql_mode, __so, sql_to_exec, schema->feature.force_order_by);
            }

            /* Try to execute it */
            try {
              dut->test(sql_to_exec);
              for (auto l : loggers)
                l->executed(*gen);
              // 清理 IN 集合注册表（expr/condgen），避免跨语句复用/UAF
              {
                const void* stmt_key = (const void*)scope.stmt_seq.get();
                expr_utils::clear_inset_registry_for_stmt_key(stmt_key);
                condgen_utils::clear_inset_registry_for_stmt_key(stmt_key);
              }
            } catch (const dut::failure &e) {
              for (auto l : loggers)
                try {
                  l->error(*gen, e);
                } catch (runtime_error &e) {
                  cerr << endl << "log failed: " << typeid(*l).name() << ": "
                       << e.what() << endl;
                }
              if ((dynamic_cast<const dut::broken *>(&e))) {
                /* re-throw to outer loop to recover session. */
                throw;
              }
            }
          }
        }
        catch (const dut::broken &e) {
          /* Give server some time to recover. */
          this_thread::sleep_for(milliseconds(1000));
        }
      }
    }
  catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }
}
