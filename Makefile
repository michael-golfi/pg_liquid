DISTDIR := dist
BUILD_SENTINEL ?= 0
LEGACY_ARTIFACTS := src/pg_liquid.o src/engine.o src/scan.o src/catalog.o src/parser.o src/assertion_compiler.o \
	src/pg_liquid.bc src/engine.bc src/scan.bc src/catalog.bc src/parser.bc src/assertion_compiler.bc \
	pg_liquid.so pg_liquid.dylib pg_liquid.dll libpg_liquid.a libpg_liquid.pc

ifeq ($(BUILD_SENTINEL),0)
.PHONY: all install clean installcheck check bench bench-check bench-guard package-check pgxn-package clean-legacy-artifacts

package-check:
	node scripts/validate_pgxn_package.mjs

pgxn-package: package-check
	bash scripts/package_pgxn.sh

$(DISTDIR):
	mkdir -p $@

clean-legacy-artifacts:
	rm -f $(LEGACY_ARTIFACTS)
	rm -rf $(DISTDIR)

all install clean check bench bench-check bench-guard: clean-legacy-artifacts | $(DISTDIR)
	$(MAKE) -C $(DISTDIR) -f ../Makefile BUILD_SENTINEL=1 srcdir=.. PG_CONFIG="$(PG_CONFIG)" $@

installcheck: clean-legacy-artifacts | $(DISTDIR)
	$(MAKE) -C $(DISTDIR) -f ../Makefile BUILD_SENTINEL=1 srcdir=.. PG_CONFIG="$(PG_CONFIG)" install
	$(MAKE) -C $(DISTDIR) -f ../Makefile BUILD_SENTINEL=1 srcdir=.. PG_CONFIG="$(PG_CONFIG)" installcheck

else
MODULE_big = pg_liquid
OBJS = pg_liquid.o engine.o scan.o catalog.o parser.o assertion_compiler.o
DEPS = $(OBJS:.o=.d)
EXTENSION = pg_liquid
DATA = $(patsubst $(srcdir)/%,%,$(sort $(wildcard $(srcdir)/sql/pg_liquid--*.sql)))
PG_CPPFLAGS = -g -O2 -I$(libdir)/src/include -I$(srcdir)/src
SHLIB_LINK = -lpthread
BENCH_DB ?= postgres
BENCH_GUARD_MODE ?= check
BENCH_GUARD_RUNS ?= 7
BENCH_GUARD_BASELINE ?= $(srcdir)/.bench/guard_baseline.tsv
EXTRA_CLEAN += $(EXTENSION).control $(DEPS)

REGRESS = liquid_blog liquid_edge_cases liquid_normalizers liquid_upgrade liquid_security liquid_ai_context liquid_policy_model liquid_agent_memory liquid_memories_policies liquid_ontology_agent_workflows

bench:
	psql "$(BENCH_DB)" -v ON_ERROR_STOP=1 -f $(srcdir)/sql/liquid_bench.sql

bench-check: install
	bash $(srcdir)/scripts/bench_check.sh

bench-guard: install
	bash $(srcdir)/scripts/bench_guard.sh $(BENCH_GUARD_MODE) --runs $(BENCH_GUARD_RUNS) --baseline $(BENCH_GUARD_BASELINE)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(strip $(CLANG)),)
CLANG = clang
endif

-include $(DEPS)

$(EXTENSION).control: $(srcdir)/$(EXTENSION).control
	cp $< $@

%.o: $(srcdir)/src/%.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -MMD -MP -MF $(@:.o=.d) -c $< -o $@

%.bc: $(srcdir)/src/%.c
	$(COMPILE.c.bc) -o $@ $<
endif
