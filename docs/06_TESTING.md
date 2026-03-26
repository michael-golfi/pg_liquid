# 6. Testing

## Regression Coverage

The active regression suite is:

- `liquid_blog`
- `liquid_edge_cases`
- `liquid_normalizers`
- `liquid_upgrade`
- `liquid_security`
- `liquid_ai_context`
- `liquid_policy_model`
- `liquid_agent_memory`
- Vitest integration coverage for ontology validation and privilege hardening

These cover:

- Liquid blog examples
- bootstrap schema behavior
- compounds and `cid` binding
- legacy syntax rejection
- comment handling and `_`
- escaped string parsing
- larger generated Liquid programs
- low-budget edge-cache fallback correctness
- tombstone invisibility and reactivation
- install/upgrade surface validation
- data-bearing `1.0.0 -> 0.1.0` upgrades
- multi-recursive-body semi-naive evaluation
- principal-scoped derived subject/object/compound CLS policies,
  `liquid/acts_for` inheritance, and exact-triple grants
- trusted `query_as(...)`, least-privilege `read_as(...)`, first-class explicit
  grant compounds, first-class derived policy compounds, `Principal@(...)`
  metadata, and legacy compatibility
- row normalizer backfill, mutation diffing, deprojection, and rebuild
- clearer normalizer DX failures for non-object mappings, duplicate
  registrations, and missing rebuild/drop targets
- public ontology helper coverage: `compound_roles(...)`,
  `validate_taxonomy(...)`, and `validate_instances(...)`
- revoked internal/operator surfaces for `_` helpers and row normalizer
  management functions

## Benchmarking

The repository ships:

- a manual benchmark smoke script in `sql/liquid_bench.sql`
- a machine-readable benchmark runner in `scripts/run_benchmarks.mjs`
- a wrapper target in `make -C libs/pg_liquid bench-check`
- a repeated-run regression guard runner in `scripts/bench_guard.sh`
- Vitest benchmark + integration suites in `tests/`

Run it with:

```sh
make -C libs/pg_liquid bench BENCH_DB='<connection-string>'
make -C libs/pg_liquid bench-check BENCH_DB='<connection-string>'
make -C libs/pg_liquid bench-guard BENCH_GUARD_MODE=baseline
make -C libs/pg_liquid bench-guard BENCH_GUARD_MODE=check
npm test
```

The benchmark focuses on:

- bulk Liquid assertion load
- point lookup through `liquid.query(...)`
- principal-scoped `liquid.read_as(...)` lookup overhead
- equality-heavy base storage scans
- recursive closure
- branchy transit-style reachability stress
- compound lookup
- row normalizer backfill cost
- row normalizer insert/update/delete trigger cost
- ontology validation runtime

`bench-guard` runs `bench-check` multiple times (default `7`) and compares
median/p90 metrics against a saved baseline snapshot in
`libs/pg_liquid/.bench/guard_baseline.tsv`. The benchmark runner can also emit a
JSON artifact with workload settings, PostgreSQL settings, and metric values;
the checked-in reference artifact lives at
`libs/pg_liquid/.bench/reference_results.json`.

## Remaining Hardening Work

Useful next additions beyond the current audited state:

- malformed-program fuzz cases
- prolonged soak and failure-injection runs
- production-dataset benchmark baselines on larger hardware classes
