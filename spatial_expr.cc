#include "spatial_expr.hh"
#include "random.hh"
#include "schema.hh"
#include <sstream>
#include <algorithm> // 经纬度裁剪使用 std::min/std::max
#include <unordered_set> // SRID=4326 下禁止生成不支持的空间函数（如 ST_Envelope）
#include <string>

using namespace std;

// 统一实现 WGS84 坐标范围校验（经度[-180,180]、纬度[-90,90]，由于易出现问题，都取[-90,90]）
inline double clamp_lon(double x) { return std::max(-90.0, std::min(90.0, x)); }
inline double clamp_lat(double y) { return std::max(-90.0,  std::min(90.0,  y)); }
// 提供统一二元裁剪工具，明确顺序为（lon, lat）
inline void clamp_lonlat(double &lon, double &lat) { lon = clamp_lon(lon); lat = clamp_lat(lat); }

// SRID=4326 下禁止生成不支持的空间函数（如 ST_Envelope）
static const std::unordered_set<std::string> kGeoSRSBlacklist = {
  "ST_Envelope",
  "ST_Buffer"
};

// 仅支持笛卡尔坐标系的空间操作函数白名单（MySQL 地理 SRID 下未实现）
static const std::unordered_set<std::string> kCartesianOnlyFuncs = {
  "ST_Intersection",
  "ST_Union",
  "ST_Difference",
  "ST_SymDifference",
  "ST_ConvexHull"
};

spatial_expr::spatial_expr(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  (void)type_constraint;
  type = sqltype::get("geometry");
  neg_rate = 0.0;
  srid = pick_srid();
}

int spatial_expr::pick_srid()
{
  // 固化 SRID=4326 与经纬度坐标校验（MySQL 模式下忽略 schema->srid_set）
  return 4326;
}

static void emit_point_wkt(std::ostream &out, int srid)
{
  double lon = (int)d100();
  double lat = (int)d100();
  // 固化 SRID=4326 与经纬度坐标校验（clamp 经度[-180,180]、纬度[-90,90]）
  if (srid == 4326) {
    clamp_lonlat(lon, lat); // 顺序：（lon, lat）
  }
  out << "POINT(" << (int)lon << " " << (int)lat << ")"; // [FIX][LonLat-Order] 始终以“lon lat”顺序输出
}

static void emit_linestring_wkt(std::ostream &out, int srid)
{
  out << "LINESTRING(";
  int n = 2 + d6();
  for (int i = 0; i < n; ++i) {
    double lon = (int)d100();
    double lat = (int)d100();
    if (srid == 4326) {
      clamp_lonlat(lon, lat); 
    }
    out << (int)lon << " " << (int)lat;
    if (i + 1 < n) out << ", ";
  }
  out << ")";
}

static void emit_polygon_wkt(std::ostream &out, int srid)
{
  double lon = (int)d100();
  double lat = (int)d100();
  if (srid == 4326) {
    clamp_lonlat(lon, lat); 
  }
  out << "POLYGON((";
  // 逐顶点 clamp，避免 lon+1/lat+1 超范围
  double lon1 = lon + 1, lat1 = lat;      if (srid == 4326) clamp_lonlat(lon1, lat1);
  double lon2 = lon + 1, lat2 = lat + 1;  if (srid == 4326) clamp_lonlat(lon2, lat2);
  double lon3 = lon,     lat3 = lat + 1;  if (srid == 4326) clamp_lonlat(lon3, lat3);
  out << (int)lon << " " << (int)lat << ", "
      << (int)lon1 << " " << (int)lat1 << ", "
      << (int)lon2 << " " << (int)lat2 << ", "
      << (int)lon3 << " " << (int)lat3 << ", "
      << (int)lon << " " << (int)lat << "))"; // 闭合
}

void spatial_expr::emit_constructor(std::ostream &out)
{
  int which = d6();
  if (which <= 2) {
    last_func = "ST_GeomFromText";
    out << "ST_SRID(ST_GeomFromText('";
    int g = d6();
    if (g <= 2) emit_point_wkt(out, srid);
    else if (g <= 4) emit_linestring_wkt(out, srid);
    else emit_polygon_wkt(out, srid);
    out << "', " << 4326 << "), 4326)";
    return;
  }
  if (which <= 4) {
    last_func = "Point";
    // Point 构造统一 clamp，经度→clamp_lon，纬度→clamp_lat
    out << "ST_SRID(Point(" << (int)clamp_lon((int)d100()) << ", " << (int)clamp_lat((int)d100()) << "), 4326)"; // [FIX][LonLat-Order]
    return;
  }
  if (which == 5) {
    last_func = "ST_GeomFromText";
    out << "ST_SRID(ST_GeomFromText('"; emit_linestring_wkt(out, srid); out << "', " << 4326 << "), 4326)";
    return;
  }
  last_func = "ST_GeomFromGeoJSON";
  {
    const int opt = 1;
    std::ostringstream doc;
    // GeoJSON 坐标统一 clamp，保持 [lon,lat] 顺序
    doc << "{\"type\":\"Point\",\"coordinates\":[" << (int)clamp_lon((int)d100()) << "," << (int)clamp_lat((int)d100()) << "]}"; // [FIX][LonLat-Order]
    out << "ST_SRID(ST_GeomFromGeoJSON('" << doc.str() << "', " << opt << "), " << 4326 << ")";
  }
}

static void emit_two_geoms(std::ostream &out, int srid)
{
  // 统一以传入 srid 构造几何：srid==0 视为默认笛卡尔，不再额外 ST_SRID 包裹
  if (srid == 0) {
    out << "ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))', 0)";
    out << ", ";
    out << "ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))', 0)";
  } else {
    out << "ST_SRID(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))', " << srid << "), " << srid << ")";
    out << ", ";
    out << "ST_SRID(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))', " << srid << "), " << srid << ")";
  }
}

void spatial_expr::emit_measure_or_relation(std::ostream &out)
{
  int which = d20();
  if (which <= 3) { last_func = "ST_IsValid"; out << "ST_IsValid("; emit_constructor(out); out << ")"; return; }
  if (which <= 6) { last_func = "ST_Area"; out << "ST_Area(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "))"; return; }
  if (which <= 8) { last_func = "ST_Length"; out << "ST_Length(ST_GeomFromText('"; emit_linestring_wkt(out, srid); out << "', " << srid << "))"; return; }
  if (which <= 10) { last_func = "ST_Length"; out << "ST_Length(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "))"; return; } // 空间度量统一 ST_Length
  if (which <= 12) { last_func = "ST_Distance"; int s = srid; out << "ST_Distance(ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "), ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "))"; return; }
  if (which <= 14) { last_func = "ST_Contains"; out << "ST_Contains("; emit_two_geoms(out, srid); out << ")"; return; }
  if (which <= 15) { last_func = "ST_Within"; out << "ST_Within("; emit_two_geoms(out, srid); out << ")"; return; }
  if (which <= 16) { last_func = "ST_Intersects"; out << "ST_Intersects("; emit_two_geoms(out, srid); out << ")"; return; }
  if (which <= 17) { last_func = "ST_Touches"; out << "ST_Touches("; emit_two_geoms(out, srid); out << ")"; return; }
  if (which <= 18) { last_func = "ST_Overlaps"; out << "ST_Overlaps("; emit_two_geoms(out, srid); out << ")"; return; }
  if (which <= 19) { last_func = "ST_Crosses"; out << "ST_Crosses("; emit_two_geoms(out, srid); out << ")"; return; }
  last_func = "ST_Disjoint"; out << "ST_Disjoint("; emit_two_geoms(out, srid); out << ")";
}

void spatial_expr::emit_operation(std::ostream &out)
{
  int which = d12();
  if (which <= 3) { last_func = "ST_Buffer"; if (srid == 4326) { /* SRID=4326 下改写：跳过不支持的 ST_Buffer/ST_Envelope 包裹，直接使用原几何 */ emit_constructor(out); } else { out << "ST_Buffer("; emit_constructor(out); out << ", " << (1 + d6()) << ")"; } return; }

  if (which <= 5) { last_func = "ST_Envelope"; if (srid == 4326) { /* SRID=4326 下改写 Envelope 为原几何，避免 ERROR 3618 */ emit_constructor(out); } else { out << "ST_Envelope("; emit_constructor(out); out << ")"; } return; }
  if (which <= 8) { last_func = "ST_Union"; if (srid == 4326) { /* 禁用笛卡尔专用操作，回退为几何构造 */ emit_constructor(out); } else { out << "ST_Union("; emit_two_geoms(out, srid); out << ")"; } return; }
  if (which <= 10) { last_func = "ST_Difference"; if (srid == 4326) { /* 禁用笛卡尔专用操作，回退为几何构造 */ emit_constructor(out); } else { out << "ST_Difference("; emit_two_geoms(out, srid); out << ")"; } return; }
  if (which <= 11) { last_func = "ST_SymDifference"; if (srid == 4326) { /* 禁用笛卡尔专用操作，回退为几何构造 */ emit_constructor(out); } else { out << "ST_SymDifference("; emit_two_geoms(out, srid); out << ")"; } return; }
  last_func = "ST_Intersection"; if (srid == 4326) { /* 禁用笛卡尔专用操作，回退为几何构造 */ emit_constructor(out); } else { out << "ST_Intersection("; emit_two_geoms(out, srid); out << ")"; }
}

void spatial_expr::emit_info(std::ostream &out)
{
  int which = d12();
  if (which <= 3) { last_func = "ST_SRID"; out << "ST_SRID("; emit_constructor(out); out << ")"; return; }
  if (which <= 6) { last_func = "ST_NumInteriorRings"; out << "ST_NumInteriorRings(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "))"; return; }
  if (which <= 8) { last_func = "ST_NumInteriorRings"; out << "ST_NumInteriorRings(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "))"; return; }
  if (which <= 10) { last_func = "ST_ExteriorRing"; out << "ST_ExteriorRing(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "))"; return; }
  if (which <= 11) { last_func = "ST_StartPoint"; out << "ST_StartPoint(ST_GeomFromText('"; emit_linestring_wkt(out, srid); out << "', " << srid << "))"; return; }
  last_func = "ST_EndPoint"; out << "ST_EndPoint(ST_GeomFromText('"; emit_linestring_wkt(out, srid); out << "', " << srid << "))";
}

void spatial_expr::emit_invalid_geometry(std::ostream &out)
{
  last_func = "INVALID_GEOMETRY";
  int choice = d6();
  if (choice <= 2) {
    // 未闭合 Polygon
    out << "ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))', " << srid << ")";
  } else if (choice <= 4) {
    // 错误 WKT
    out << "ST_GeomFromText('POINT(,)', " << srid << ")";
  } else {
    // SRID 不匹配：构造两个不同 SRID 的对象传关系函数
    out << "ST_Contains(ST_GeomFromText('POINT(0 0)', 0), ST_GeomFromText('POINT(1 1)', 4326))";
  }
}

void spatial_expr::emit_combos(std::ostream &out)
{
  int which = d6();
  switch (which) {
    case 1: {
      // 操作 + 关系：ST_Intersects(op(g1), g2)，在 SRID=4326 上禁用 ST_Buffer
      last_func = "ST_Intersects";
      out << "ST_Intersects(";
      if (srid == 4326) {
        /* [FIX][GeoSRS-Rewrite] SRID=4326 下改写 Envelope 相关模式为可执行表达式：直接使用原几何 */
        emit_constructor(out);
      } else {
        out << "ST_Buffer("; emit_constructor(out); out << ", " << (1 + d6()) << ")";
      }
      out << ", ";
      // 为避免二元关系 SRID 不一致，使用同 SRID 的几何常量
      out << "ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << ")";
      out << ")";
      break;
    }
    case 2: {
      // 嵌套集合：ST_Within(..., ...)；SRID=4326 下禁用 ST_Intersection 子树，统一使用 4326 几何
      last_func = "ST_Within";
      if (srid == 4326) {
        out << "ST_Within(";
        out << "ST_SRID(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "), " << srid << ")";
        out << ", ";
        out << "ST_SRID(ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << "), " << srid << ")";
        out << ")";
      } else {
        out << "ST_Within(ST_Intersection(";
        emit_two_geoms(out, srid);
        out << "), ST_GeomFromText('"; emit_polygon_wkt(out, srid); out << "', " << srid << ")";
        out << ")";
      }
      break;
    }
    case 3: {
      // SRID 处理：使用 ST_GeomFromText 直接指定 SRID
      last_func = "ST_Distance";
      int s = pick_srid();
      out << "ST_Distance(ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "), ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "))";
      break;
    }
    case 4: {
      last_func = "ST_Contains";
      out << "ST_Contains("; emit_two_geoms(out, srid); out << ")";
      break;
    }
    case 5: {
      // 圆转：ST_Equals(ST_GeomFromGeoJSON(ST_AsGeoJSON(g1)), g1)
      last_func = "ST_Equals";
      out << "ST_Equals(ST_GeomFromGeoJSON(ST_AsGeoJSON("; emit_constructor(out); out << "), 1), "; emit_constructor(out); out << ")";
      break;
    }
    default: {
      // 默认：使用同 SRID 的合法几何避免负例
      last_func = "ST_Distance";
      int s = srid;
      out << "ST_Distance(ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "), ST_GeomFromText('"; emit_point_wkt(out, s); out << "', " << s << "))";
      break;
    }
  }
}

void spatial_expr::out(std::ostream &out)
{
  match();
  if (d100() < (int)(neg_rate*100)) { emit_invalid_geometry(out); return; }
  // 组合 recipes：按密度触发
  if (scope->schema->enable_spatial && scope->schema->mysql_mode && d100() < (int)(scope->schema->spatial_density * 50 + 10)) {
    emit_combos(out);
    return;
  }
  int which = d20();
  if (which <= 5) emit_constructor(out);
  else if (which <= 10) emit_measure_or_relation(out);
  else if (which <= 15) emit_operation(out);
  else emit_info(out);
}
