/// @file
/// @brief supporting classes for the grammar

#ifndef RELMODEL_HH
#define RELMODEL_HH
#include <string>
#include <vector>
#include <map>
#include <utility>
#include <memory>
#include <cassert>

using std::string;
using std::vector;
using std::map;
using std::pair;
using std::make_pair;
using std::shared_ptr;

struct sqltype {
  string name;
  static map<string, struct sqltype*> typemap;
  static struct sqltype *get(string s);
  sqltype(string n) : name(n) { }

  /** This function is used to model postgres-style pseudotypes.
      A generic type is consistent with a more concrete type.
      E.G., anyarray->consistent(intarray) is true
            while int4array->consistent(anyarray) is false

      There must not be cycles in the consistency graph, since the
      grammar will use fixpoint iteration to resolve type conformance
      situations in the direction of more concrete types  */
  virtual bool consistent(struct sqltype *rvalue);
};

struct column {
  string name;
  sqltype *type;
  // 安全构造：统一从 std::string 安全来源赋值，避免裸指针/未终止内存
  column(const char* cname) : name(cname ? std::string(cname) : std::string("")), type(nullptr) {
    // 长度与空值守卫
    if (name.empty() || name.size() > 1024) {
      name = std::string("ret_col");
    }
  }
  column(string name) : name(name), type(nullptr) {
    if (this->name.empty() || this->name.size() > 1024) {
      this->name = std::string("ret_col");
    }
  }
  column(string name, sqltype *t) : name(name), type(t) {
    assert(t);
    if (this->name.empty() || this->name.size() > 1024) {
      this->name = std::string("ret_col");
    }
  }
};

struct relation {
  vector<column> cols;
  virtual vector<column> &columns() { return cols; }
};

struct named_relation : relation {
  string name;
  virtual string ident() { return name; }
  virtual ~named_relation() { }
  named_relation(string n) : name(n) { }
};

struct aliased_relation : named_relation {
  relation *rel;
  virtual ~aliased_relation() { }
  aliased_relation(string n, relation* r) : named_relation(n), rel(r) { }
  virtual vector<column>& columns() { return rel->columns(); }
};

struct table : named_relation {
  string schema;
  bool is_insertable;
  bool is_base_table;
  vector<string> constraints;
  table(string name, string schema, bool insertable, bool base_table)
    : named_relation(name),
      schema(schema),
      is_insertable(insertable),
      is_base_table(base_table) { }
  virtual string ident() { return schema + "." + name; }
  virtual ~table() { };
};

struct scope {
  struct scope *parent;
  /// available to table_ref productions
  vector<named_relation*> tables;
 /// available to column_ref productions
  vector<named_relation*> refs;
  struct schema *schema;
  /// Counters for prefixed stmt-unique identifiers
  shared_ptr<map<string,unsigned int> > stmt_seq;
  scope(struct scope *parent = 0) : parent(parent) {
    if (parent) {
      schema = parent->schema;
      tables = parent->tables;
      refs = parent->refs;
      stmt_seq = parent->stmt_seq;
    }
  }
  vector<pair<named_relation*, column> > refs_of_type(sqltype *t) {
    vector<pair<named_relation*, column> > result;
    // Defensive: type constraint may be null in complex recursion; return empty to allow literal fallback
    if (!t) return result;
    auto type_family = [](sqltype* tt)->int {
      if (!tt) return 0;
      auto* TINT = sqltype::get("int");
      auto* TTINY = sqltype::get("tinyint");
      auto* TSMALL = sqltype::get("smallint");
      auto* TMED = sqltype::get("mediumint");
      auto* TBIG = sqltype::get("bigint");
      auto* TFLOAT = sqltype::get("float");
      auto* TDOUBLE = sqltype::get("double");
      auto* TDEC = sqltype::get("decimal");
      auto* TVARCHAR = sqltype::get("varchar");
      auto* TCHAR = sqltype::get("char");
      auto* TTEXT = sqltype::get("text");
      auto* TENUM = sqltype::get("enum");
      auto* TDATE = sqltype::get("date");
      auto* TDATETIME = sqltype::get("datetime");
      auto* TSTAMP = sqltype::get("timestamp");
      auto* TTIME = sqltype::get("time");
      auto* TYEAR = sqltype::get("year");
      auto* TBIN = sqltype::get("binary");
      auto* TVARBIN = sqltype::get("varbinary");
      auto* TBLOB = sqltype::get("blob");
      auto* TJSON = sqltype::get("json");
      auto* TGEOM = sqltype::get("geometry");
      auto is_any = [&](sqltype* x, std::initializer_list<sqltype*> lst){ for (auto* y: lst){ if (x==y) return true; } return false; };
      if (is_any(tt,{TINT,TTINY,TSMALL,TMED,TBIG,TFLOAT,TDOUBLE,TDEC})) return 1;
      if (is_any(tt,{TVARCHAR,TCHAR,TTEXT,TENUM})) return 2;
      if (is_any(tt,{TDATE,TDATETIME,TSTAMP,TTIME,TYEAR})) return 3;
      if (is_any(tt,{TBIN,TVARBIN,TBLOB})) return 4;
      if (tt==TJSON) return 5;
      if (tt==TGEOM) return 6;
      return 0;
    };
    // inline safety: guard against null/unknown before consistency check
    auto consistent_safe = [&](sqltype* lhs, sqltype* rhs)->bool {
      if (!lhs || !rhs) return false;
      int lf = type_family(lhs);
      int rf = type_family(rhs);
      if (lf == 0 || rf == 0) return false; // Unknown/uninitialized type — treat as non-matching
      return lhs->consistent(rhs);
    };
    int tf = type_family(t);
    for (auto r : refs) {
      if (!r) continue;
      if (auto ar = dynamic_cast<aliased_relation*>(r)) { if (!ar->rel) continue; }
      auto &cols = r->columns();
      for (auto c : cols) {
        sqltype *ct = c.type;
        if (!ct) continue; // skip columns without resolved type
        /* name-based check removed: rely on pointer identity and consistency */
        int cf = type_family(ct);
        if (tf && cf && tf != cf) continue; // quick family mismatch pruning
        if (consistent_safe(t, ct)) {
          result.push_back(make_pair(r, c));
        }
      }
    }
    return result;
  }
  /** Generate unique identifier with prefix. */
  string stmt_uid(const char* prefix) {
    string result(prefix);
    result += "_";
    result += std::to_string((*stmt_seq)[result]++);
    return result;
  }
  /** Reset unique identifier counters and clear per-statement column refs to avoid dangling pointers across queries. */
  void new_stmt() {
    // Reset unique id counters for this statement
    stmt_seq = std::make_shared<map<string,unsigned int> >();
    // Clear column reference cache: previous statement's named_relation* entries may point to AST-owned nodes
    // that have been destroyed after serialization; keeping them leads to use-after-free in the next query.
    // We rebuild refs from the new from_clause for each statement.
    refs.clear();
  }
};

struct op {
  string name;
  sqltype *left;
  sqltype *right;
  sqltype *result;
  op(string n,sqltype *l,sqltype *r, sqltype *res)
    : name(n), left(l), right(r), result(res) { }
  op() { }
};

struct routine {
  string specific_name;
  string schema;
  vector<sqltype *> argtypes;
  sqltype *restype;
  string name;
  routine(string schema, string specific_name, sqltype* data_type, string name)
    : specific_name(specific_name), schema(schema), restype(data_type), name(name) {
    assert(data_type);
  }
  virtual string ident() {
    if (schema.size())
      return schema + "." + name;
    else
      return name;
  }
};

#endif
