-include Makefile.local

# 可选的 ccache 加速变量（通过环境注入启用，例如：CCACHE=ccache）；默认空，不影响正常构建
CCACHE ?=

BUILT_SOURCES = gitrev.h
CLEANFILES = gitrev.h

bin_PROGRAMS = sqlsmith

DUT = postgres.cc

if DUT_MONETDB
DUT += monetdb.cc
endif

if DUT_SQLITE
DUT += sqlite.cc
endif

if DUT_MYSQL
DUT += mysql.cc
endif

sqlsmith_SOURCES = relmodel.cc schema.cc $(DUT) \
    random.cc prod.cc expr.cc grammar.cc log.cc dump.cc impedance.cc \
    sqlsmith.cc json_expr.cc spatial_expr.cc json_table_ref.cc temp_dataset.cc value_catalog.cc condition_builder.cc condition_generator.cc ats_condition_adapter.cc setops_generator.cc

sqlsmith_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(LIBMYSQLCLIENT_LIBS)

AM_LDFLAGS = $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS)
AM_CPPFLAGS = $(BOOST_CPPFLAGS) $(LIBPQXX_CFLAGS) $(POSTGRESQL_CPPFLAGS) $(MONETDB_MAPI_CFLAGS) -Wall -Wextra


EXTRA_DIST = gitrev.h dump.hh expr.hh grammar.hh log.hh prod.hh \
    random.hh relmodel.hh schema.hh impedance.hh known.txt known_re.txt log.sql \
    README.org TODO.org ast.png logo.png dump.xsl util.hh sqlite.hh temp_dataset.hh  \
    dut.hh postgres.hh monetdb.hh mysql.hh log-v1.0-to-v1.2.sql boring_sqlstates.txt value_catalog.hh condition_builder.hh condition_generator.hh temp_dataset_adapter.hh ats_condition_adapter.hh

gitrev.h: $(HEADERS) $(SOURCES)
        -if git describe --exclude='debian*' --dirty --tags --always > /dev/null ; then \
        echo "#define GITREV \"$$(git describe --exclude='debian*' --dirty --tags --always)\"" > $@ ;\
        else \
        echo "#define GITREV \"unreleased\"" > $@ ;\
        fi

filterdump:
        psql -Xc 'copy (select error from known) to stdout' |sort -u > known.txt
        psql -Xc 'copy (select re from known_re) to stdout' |sort -u > known_re.txt
        psql -Xc 'copy (select sqlstate from boring_sqlstates) to stdout' |sort -u > boring_sqlstates.txt

.PHONY: filterdump

# 显式编译规则：仅覆盖 mysql.o 的对象级编译，其他对象与链接保持原设置（包括 sanitizer）；
# 轻量化标志（-O0 -g -pipe），避免重复包含路径；AM_CPPFLAGS/CPPFLAGS 由项目统一管理。
mysql.o: mysql.cc
        $(AM_V_CXX) $(CCACHE) $(CXX) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(AM_CPPFLAGS) $(CPPFLAGS) -Wall -Wextra -O0 -g -pipe -MT $@ -MD -MP -MF .deps/mysql.Tpo -c -o $@ $<
