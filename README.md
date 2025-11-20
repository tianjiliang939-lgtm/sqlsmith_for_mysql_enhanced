# SQLsmith MySQL 8.0: JSON & Spatial 扩展（含窗口、CTE、集合操作与功能开关）

本目录为在原始 `sqlsmith-master-for-base` 基础上实现的增强版，用于对 MySQL 8.0 的 JSON/几何/窗口/CTE/集合操作等进行全面、随机、非破坏性、以错误稳定性为核心的测试。保持工程原有结构与风格，未修改构建脚本（Makefile.am、config.h）。

## 新增特性
- 表达式与语法生成：
  - JSON：随机生成 JSON_OBJECT/JSON_ARRAY 常量、JSON_EXTRACT/SET/REMOVE/INSERT/REPLACE、JSON_CONTAINS/OVERLAPS 等；支持负例（无效 JSON 文本）。
  - Spatial：支持 ST_GeomFromText/WKB/GeoJSON、Point/LineString/Polygon 构造，ST_IsValid/Area/Length/Contains/Within/Intersects/Touches/Overlaps/Crosses/Disjoint 等测度与关系；支持负例（非法 WKT、未闭合 polygon、SRID 不匹配）。
  - 窗口函数（Window）：遵循 MySQL 8.0 规则的窗口函数生成；结合 GROUP BY 的合规重写/守卫，确保窗口函数不出现在 GROUP BY 等禁用位置，规避 3593 错误（You cannot use the window function '...' in this context）。
  - CTE（WITH/RECURSIVE）：仅在 MySQL 模式下，同概率生成 WITH 与 WITH RECURSIVE；在 `grammar.cc` 的 `common_table_expression::out` 中实现，递归项使用 UNION ALL 并带守卫（异常时回退普通 WITH）。默认关闭，由 `--enable-cte` 控制。
  - 集合操作（SetOps）：在 MySQL 模式下支持 UNION/UNION ALL/INTERSECT/EXCEPT 的最小嵌入式生成，外层可按需追加 ORDER BY 1,2,...。生成器文件已重命名为小写+下划线：`setops_generator.hh/.cc`（引用与构建清单已更新）。
- 适配层与特性开关：
  - 新增/统一 CLI 参数开关（默认均为 OFF/false；关闭不影响原有逻辑）：
    - `--enable-json`        enable JSON expression/table generation（default OFF）
    - `--enable-spatial`     enable Spatial expression generation（default OFF）
    - `--enable-window`      enable window function generation（default OFF）
    - `--enable-cte`         enable CTE generation（MySQL only; default OFF）
    - 语句类：`--enable-insert`、`--enable-delete`（DELETE RETURNING）、`--enable-merge`、`--enable-upsert`、`--enable-update`（UPDATE RETURNING）（均 default OFF）
    - 过滤：`--exclude-found_rows`（default false；true 时过滤 FOUND_ROWS 系统函数）
  - SRID 限制：`srid-set` 已废弃，统一固定 SRID=4326（EPSG:4326），在空间函数生成点直接注入 4326；CLI 上若传入 `--srid-set` 会打印一次性警告并忽略参数。
  - 用户自定义 hint：新增 `--customer_hint`（默认空）。非空时将在主查询（最外层）SELECT 后原样插入（前置空格），不做格式校验。
- 稳定/合规控制：
  - 负例生成结合阻抗机制自适应抑制；窗口/聚合/类型一致性在工程内多处做最小防御，避免语义不合法与编译/执行错误。

## 构建与运行

### 0. 在 Debian/Ubuntu 安装依赖
- 必需：C++11、libpqxx（PostgreSQL）、autotools
- 可选：boost::regex（当 std::regex 在你的编译器中不稳定时）、SQLite3、MonetDB

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential autoconf autoconf-archive \
  libpqxx-dev libboost-regex-dev libsqlite3-dev \
  monetdb-mapi # 可选
```

### 1. 使用 Autotools 构建（与原工程一致）
```bash
cd sqlsmith_for_mysql_enhanced-master
autoreconf -fiv
./configure
make
```

### 2. 使用 CMake 构建（可选）
```bash
mkdir build && cd build
cmake ..
make -j
```
生成的可执行文件为 `sqlsmith`。

## 运行示例

- 连接 MySQL 并开启 JSON/Spatial/窗口/CTE 测试（SRID 固定 4326）：
```bash
./sqlsmith --mysql=127.0.0.1:3306/testdb?username=root&password=xxx \
  --enable-json --enable-spatial --enable-window --enable-cte \
  --json-density=0.35 --spatial-density=0.25 \
  --verbose --max-queries=100
```
- Dry-run 查看生成的 SQL 并过滤 FOUND_ROWS：
```bash
./sqlsmith --mysql=127.0.0.1:3306/testdb?username=root&password=xxx \
  --enable-json --enable-spatial --dry-run --dump-all-queries --max-queries=20 \
  --exclude-found_rows
```
- 在主查询 SELECT 注入自定义 hint（示例为 MySQL Optimizer Hint，内容与数量完全由用户自定义）：
```bash
./sqlsmith --mysql=127.0.0.1:3306/testdb?username=root&password=xxx \
  --enable-window --customer_hint='/*+ SET_VAR(sort_buffer_size=262144) */' \
  --dry-run --dump-all-queries --max-queries=10
```
- 集合操作（MySQL 模式下，按开启类型与概率随机选择）：
```bash
./sqlsmith --mysql=127.0.0.1:3306/testdb?username=root&password=xxx \
  --enable-union --enable-union-all --enable-intersect --enable-except \
  --force-order-by --dry-run --dump-all-queries --max-queries=10
```

## 设计说明
- 遵循原工具“非破坏性、随机、多样、以错误/稳定性为中心”的哲学：
  - 不加入重型语义断言，仅进行可执行性与错误分类。
  - JSON/Spatial 的负例（无效 JSON/非法几何、SRID 不匹配）用于触发引擎边界与不稳定组合，再由阻抗黑名单自适应抑制。
- 生成器实现：
  - `json_expr` 和 `spatial_expr` 专用表达式类，避免通用 `funcall` 中的强制 `cast(... as char)` 影响类型语义；空间表达统一固定 SRID=4326 并进行经纬度裁剪（lon、lat）。
  - `grammar.table_ref.factory` 注入 `json_table_ref`；`query_spec::out` 在最外层 SELECT 后注入 `customer_hint`（非空时）。
  - `common_table_expression::out` 承担 CTE 输出职责：命中概率后在 MySQL 模式下 50% 生成 WITH、50% 生成 WITH RECURSIVE；异常时安全回退。
  - 窗口函数与 GROUP BY 的合规生成/重写：窗口仅出现在允许的位置，GROUP BY 项过滤掉窗口表达式，HAVING 中禁止窗口。
  - 集合操作生成通过 `setops_generator.hh/.cc` 在最终 SQL 串接阶段包裹原始 SELECT；外层可按需附加 ORDER BY。
- 适配层：
  - `schema_mysql` 解析列类型与 SRID；注册常用函数，按开关过滤 FOUND_ROWS；确保 `json`、`geometry` 类型加入类型目录（即使当前库无相关列）。

## 已知限制与注意事项
- ONLY_FULL_GROUP_BY 约束：开启窗口/CTE 时需遵循已有守卫策略；当选择列与 GROUP BY 不兼容时已进行最小重写与安全回退。
- SRID 固定的影响：若确需非 4326 的 SRID，需另行方案（当前版本统一为 4326 以保证行为一致性）。
- FOUND_ROWS 过滤：`--exclude-found_rows` 开启时将剔除该系统函数（仅过滤生成）；不影响其他函数。
- 语句类开关均默认关闭；未显式开启时以 SELECT 为主。
