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
- principal-scoped derived subject/object/compound CLS policies, `liquid/acts_for` inheritance, and exact-triple grants
- AI-style principal-scoped context compounds, hidden raw role edges, and untrusted principal isolation
- trusted `query_as(...)`, least-privilege `read_as(...)`, first-class explicit grant compounds, first-class derived policy compounds, `Principal@(...)` metadata, and legacy compatibility
- user-profile and memory-style compound retrieval through `read_as(...)`, including `liquid/acts_for`-based user scoping and cross-user isolation
- row normalizer backfill, mutation diffing, deprojection, and rebuild
- clearer normalizer DX failures for non-object mappings, duplicate registrations, and missing rebuild/drop targets

## Benchmarking

The repository ships:

- a manual benchmark smoke script in `sql/liquid_bench.sql`
- a machine-readable benchmark runner in `scripts/run_benchmarks.mjs`
- a wrapper target in `make -C libs/pg_liquid bench-check`
- a repeated-run regression guard runner in `scripts/bench_guard.sh`
- Vitest benchmark + integration suites in `tests/` (inline Datalog strings with per-file setup/teardown)

Run it with:

```sh
make -C libs/pg_liquid bench BENCH_DB='<connection-string>'
make -C libs/pg_liquid bench-check BENCH_DB='<connection-string>'
make -C libs/pg_liquid bench-guard BENCH_GUARD_MODE=baseline
make -C libs/pg_liquid bench-guard BENCH_GUARD_MODE=check
yarn vitest run -c libs/pg_liquid/vitest.config.ts
```

The benchmark focuses on:

- bulk Liquid assertion load
- point lookup through `liquid.query(...)`
- equality-heavy base storage scans
- recursive closure
- branchy transit-style reachability stress (shortest-path-shaped graph)
- compound lookup
- installed index inventory

The shipped `bench-check` / CI recursive closure gate uses an `80`-edge
baseline chain and a `240`-edge stress chain. The manual
`sql/liquid_bench.sql` smoke path remains separate.

`bench-guard` runs `bench-check` multiple times (default `7`) and compares
median/p90 metrics against a saved baseline snapshot.

The CI benchmark rules and query bodies are loaded from `.dl` fixtures, so
Datalog intent is visible outside SQL orchestration.

## Remaining Hardening Work

Useful next additions beyond the current audited state:

- malformed-program fuzz cases
- prolonged soak and failure-injection runs
- production-dataset benchmark baselines
