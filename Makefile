
SOURCES += main.cpp

LDLIBS += -lodbc32

PROJECT_BASENAME = odbc

include external/ncbind/Rules.lib.make
