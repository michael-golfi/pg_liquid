#!/usr/bin/env bash
set -euo pipefail

BENCH_DB="${BENCH_DB:-pg_liquid_bench_${USER:-bench}_$$}"
AUTO_CREATE_BENCH_DB="${AUTO_CREATE_BENCH_DB:-1}"
BENCH_N="${BENCH_N:-3000}"
CHAIN_N="${CHAIN_N:-80}"
CHAIN_N_STRESS="${CHAIN_N_STRESS:-120}"

BULK_LOAD_MAX_MS="${BULK_LOAD_MAX_MS:-8000}"
POINT_LOOKUP_MAX_MS="${POINT_LOOKUP_MAX_MS:-250}"
BASE_SCAN_MAX_MS="${BASE_SCAN_MAX_MS:-250}"
RECURSIVE_CLOSURE_MAX_MS="${RECURSIVE_CLOSURE_MAX_MS:-5000}"
RECURSIVE_CLOSURE_STRESS_MAX_MS="${RECURSIVE_CLOSURE_STRESS_MAX_MS:-1000}"
SHORTEST_PATH_STRESS_MAX_MS="${SHORTEST_PATH_STRESS_MAX_MS:-1000}"
COMPOUND_LOOKUP_MAX_MS="${COMPOUND_LOOKUP_MAX_MS:-500}"

WORKDIR="$(cd "$(dirname "$0")/.." && pwd)"
CLEANUP_OUTPUT_FILE=0
OUTPUT_FILE="${OUTPUT_FILE:-}"

if [[ -z "$OUTPUT_FILE" ]]; then
  tmp_output_file="$(mktemp "${TMPDIR:-/tmp}/pg_liquid_bench_metrics.XXXXXX")"
  OUTPUT_FILE="${tmp_output_file}.tsv"
  mv "$tmp_output_file" "$OUTPUT_FILE"
  CLEANUP_OUTPUT_FILE=1
fi

cleanup() {
  if [[ "$CLEANUP_OUTPUT_FILE" == "1" ]]; then
    rm -f "$OUTPUT_FILE"
  fi
}

trap cleanup EXIT

mkdir -p "$(dirname "$OUTPUT_FILE")"

if [[ "$AUTO_CREATE_BENCH_DB" == "1" ]]; then
  dropdb --if-exists "$BENCH_DB" >/dev/null 2>&1 || true
  createdb -T template0 "$BENCH_DB"
  trap 'dropdb --if-exists "$BENCH_DB" >/dev/null 2>&1 || true' EXIT
fi

(
  cd "$WORKDIR"
  BENCH_DB="$BENCH_DB" \
  BENCH_N="$BENCH_N" \
  CHAIN_N="$CHAIN_N" \
  CHAIN_N_STRESS="$CHAIN_N_STRESS" \
  node scripts/run_benchmarks.mjs
) | tee "$OUTPUT_FILE"

metric_value() {
  local metric_name="$1"
  awk -F'|' -v metric="$metric_name" '$1 == metric { print $2 }' "$OUTPUT_FILE" | tail -n 1
}

assert_metric() {
  local metric_name="$1"
  local max_ms="$2"
  local value

  value="$(metric_value "$metric_name")"
  if [[ -z "$value" ]]; then
    echo "missing benchmark metric: $metric_name" >&2
    exit 1
  fi

  awk -v metric="$metric_name" -v value="$value" -v max="$max_ms" '
    BEGIN {
      if ((value + 0.0) > (max + 0.0)) {
        printf("benchmark threshold exceeded for %s: %.3f ms > %.3f ms\n", metric, value + 0.0, max + 0.0) > "/dev/stderr";
        exit 1;
      }
    }'
}

assert_metric "bulk_load_ms" "$BULK_LOAD_MAX_MS"
assert_metric "point_lookup_ms" "$POINT_LOOKUP_MAX_MS"
assert_metric "base_scan_ms" "$BASE_SCAN_MAX_MS"
assert_metric "recursive_closure_ms" "$RECURSIVE_CLOSURE_MAX_MS"
assert_metric "recursive_closure_stress_ms" "$RECURSIVE_CLOSURE_STRESS_MAX_MS"
assert_metric "shortest_path_stress_ms" "$SHORTEST_PATH_STRESS_MAX_MS"
assert_metric "compound_lookup_ms" "$COMPOUND_LOOKUP_MAX_MS"
