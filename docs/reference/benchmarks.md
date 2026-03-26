# Benchmarks

`pg_liquid` ships a checked-in benchmark harness and reference artifacts for the
current release, not just ad hoc smoke queries.

## Workloads

The `0.1.6` runner measures:

- `bulk_load_ms`
- `point_lookup_ms`
- `read_as_lookup_ms`
- `base_scan_ms`
- `recursive_closure_ms`
- `recursive_closure_stress_ms`
- `shortest_path_stress_ms`
- `compound_lookup_ms`
- `row_normalizer_backfill_ms`
- `row_normalizer_trigger_insert_ms`
- `row_normalizer_trigger_update_ms`
- `row_normalizer_trigger_delete_ms`
- `ontology_validation_ms`

## Method

Run the current harness:

```sh
node scripts/run_benchmarks.mjs
BENCH_ARTIFACT_PATH=.bench/reference_results.json node scripts/run_benchmarks.mjs
make bench-check
make bench-guard BENCH_GUARD_MODE=check
```

The runner records workload sizes, PostgreSQL settings, and metric values in a
JSON artifact when `BENCH_ARTIFACT_PATH` is set.

## Checked-In Artifacts

- `.bench/reference_results.json`: one current-environment reference run
- `.bench/guard_baseline.tsv`: repeated-run baseline used by `bench-guard`

These are environment-specific reference points, not universal promises. Compare
them only against similar PostgreSQL versions, `work_mem` settings, hardware,
and JIT configuration.

## Current Reference Environment

The checked-in reference artifact was captured with:

- PostgreSQL `16.8`
- `work_mem=4MB`
- `jit=on`
- workload sizes:
  - `bench_n=3000`
  - `chain_n=80`
  - `chain_n_stress=120`
  - `sp_width=8`
  - `ontology_class_n=180`
  - `normalizer_rows=400`

## Interpreting Results

- compare medians before comparing single-run outliers
- read `bench-guard` p90 drift as the early warning for planner or cache regressions
- treat `read_as_lookup_ms` separately from `point_lookup_ms`; it measures CLS overhead
- watch row-normalizer metrics together, since backfill, insert, update, and delete exercise different paths
- validate ontology performance with the same predicate shapes you will run in production
