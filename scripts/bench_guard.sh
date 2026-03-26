#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-check}"
RUNS=7
BASELINE_FILE="$WORKDIR/.bench/guard_baseline.tsv"
TMP_DIR="$(mktemp -d)"

METRICS=(
  "bulk_load_ms"
  "point_lookup_ms"
  "base_scan_ms"
  "recursive_closure_ms"
  "recursive_closure_stress_ms"
  "shortest_path_stress_ms"
  "compound_lookup_ms"
)

STRESS_METRICS=(
  "recursive_closure_stress_ms"
  "shortest_path_stress_ms"
)

usage() {
  cat <<'USAGE'
Usage:
  bench_guard.sh baseline [--runs N] [--baseline FILE]
  bench_guard.sh check    [--runs N] [--baseline FILE]

Policy (check mode):
  - stress metric medians must improve (strictly lower than baseline)
  - non-stress metric medians must be <= baseline * 1.05
  - all metric p90 values must be <= baseline * 1.20
  - each run is validated by bench_check.sh hard thresholds
USAGE
}

if [[ $# -gt 0 ]]; then
  shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --baseline)
      BASELINE_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 3 ]]; then
  echo "--runs must be an integer >= 3" >&2
  exit 1
fi

trap 'rm -rf "$TMP_DIR"' EXIT

is_stress_metric() {
  local metric="$1"
  local stress
  for stress in "${STRESS_METRICS[@]}"; do
    if [[ "$stress" == "$metric" ]]; then
      return 0
    fi
  done
  return 1
}

float_le() {
  local left="$1"
  local right="$2"
  awk -v l="$left" -v r="$right" 'BEGIN { exit !(l + 0.0 <= r + 0.0) }'
}

float_lt() {
  local left="$1"
  local right="$2"
  awk -v l="$left" -v r="$right" 'BEGIN { exit !(l + 0.0 < r + 0.0) }'
}

float_mul() {
  local value="$1"
  local factor="$2"
  awk -v v="$value" -v f="$factor" 'BEGIN { printf "%.6f", (v + 0.0) * (f + 0.0) }'
}

compute_stats() {
  local values_file="$1"
  local stats

  stats="$(sort -n "$values_file" | awk '
    {
      vals[++n] = $1
    }
    END {
      if (n == 0) {
        exit 1
      }
      mid = int((n + 1) / 2)
      p90 = int((n * 9 + 5) / 10)
      if (p90 < 1) p90 = 1
      if (p90 > n) p90 = n
      printf "%s|%s\n", vals[mid], vals[p90]
    }'
  )" || {
    echo "no values captured in $values_file" >&2
    exit 1
  }

  printf "%s\n" "$stats"
}

collect_runs() {
  local prefix="$1"
  local run
  local metric

  for metric in "${METRICS[@]}"; do
    : > "$TMP_DIR/${prefix}_${metric}.vals"
  done

  # Warm up extension/planner/JIT once before sampled runs.
  BULK_LOAD_MAX_MS=999999999 \
  POINT_LOOKUP_MAX_MS=999999999 \
  BASE_SCAN_MAX_MS=999999999 \
  RECURSIVE_CLOSURE_MAX_MS=999999999 \
  RECURSIVE_CLOSURE_STRESS_MAX_MS=999999999 \
  SHORTEST_PATH_STRESS_MAX_MS=999999999 \
  COMPOUND_LOOKUP_MAX_MS=999999999 \
  OUTPUT_FILE="$TMP_DIR/${prefix}_warmup.tsv" \
  bash "$WORKDIR/scripts/bench_check.sh" >/dev/null

  for ((run = 1; run <= RUNS; run++)); do
    local run_file="$TMP_DIR/${prefix}_run_${run}.tsv"
    BULK_LOAD_MAX_MS=999999999 \
    POINT_LOOKUP_MAX_MS=999999999 \
    BASE_SCAN_MAX_MS=999999999 \
    RECURSIVE_CLOSURE_MAX_MS=999999999 \
    RECURSIVE_CLOSURE_STRESS_MAX_MS=999999999 \
    SHORTEST_PATH_STRESS_MAX_MS=999999999 \
    COMPOUND_LOOKUP_MAX_MS=999999999 \
    OUTPUT_FILE="$run_file" \
    bash "$WORKDIR/scripts/bench_check.sh" >/dev/null

    for metric in "${METRICS[@]}"; do
      local value
      value="$(awk -F'|' -v m="$metric" '$1 == m { print $2 }' "$run_file" | tail -n 1)"
      if [[ -z "$value" ]]; then
        echo "missing metric $metric in run $run" >&2
        exit 1
      fi
      echo "$value" >> "$TMP_DIR/${prefix}_${metric}.vals"
    done
  done
}

if [[ "$MODE" == "baseline" ]]; then
  collect_runs "baseline"
  mkdir -p "$(dirname "$BASELINE_FILE")"

  {
    echo "# pg_liquid benchmark baseline snapshot"
    echo "# runs=$RUNS"
    for metric in "${METRICS[@]}"; do
      stats="$(compute_stats "$TMP_DIR/baseline_${metric}.vals")"
      median="${stats%%|*}"
      p90="${stats##*|}"
      printf "%s|%s|%s\n" "$metric" "$median" "$p90"
    done
  } > "$BASELINE_FILE"

  echo "baseline written: $BASELINE_FILE"
  exit 0
fi

if [[ "$MODE" != "check" ]]; then
  echo "unknown mode: $MODE" >&2
  usage >&2
  exit 1
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "missing baseline file: $BASELINE_FILE" >&2
  echo "create one with: $0 baseline --runs $RUNS --baseline $BASELINE_FILE" >&2
  exit 1
fi

# Preserve existing absolute benchmark thresholds as a hard fail.
bash "$WORKDIR/scripts/bench_check.sh" >/dev/null

collect_runs "current"

CURRENT_STATS_FILE="$TMP_DIR/current_stats.txt"
: > "$CURRENT_STATS_FILE"

for metric in "${METRICS[@]}"; do
  stats="$(compute_stats "$TMP_DIR/current_${metric}.vals")"
  median="${stats%%|*}"
  p90="${stats##*|}"
  printf "%s|%s|%s\n" "$metric" "$median" "$p90" >> "$CURRENT_STATS_FILE"
done

echo "metric|baseline_median|current_median|baseline_p90|current_p90|result"

for metric in "${METRICS[@]}"; do
  baseline_line="$(awk -F'|' -v m="$metric" '$1 == m { print; exit }' "$BASELINE_FILE")"
  current_line="$(awk -F'|' -v m="$metric" '$1 == m { print; exit }' "$CURRENT_STATS_FILE")"

  local_baseline_median=""
  local_baseline_p90=""
  local_current_median=""
  local_current_p90=""
  result="OK"

  if [[ -z "$baseline_line" ]] || [[ -z "$current_line" ]]; then
    echo "missing baseline metric: $metric" >&2
    exit 1
  fi

  IFS='|' read -r _ local_baseline_median local_baseline_p90 <<< "$baseline_line"
  IFS='|' read -r _ local_current_median local_current_p90 <<< "$current_line"

  if is_stress_metric "$metric"; then
    if ! float_lt "$local_current_median" "$local_baseline_median"; then
      result="FAIL(stress median did not improve)"
    fi
  else
    median_limit="$(float_mul "$local_baseline_median" "1.05")"
    if ! float_le "$local_current_median" "$median_limit"; then
      result="FAIL(non-stress median regression)"
    fi
  fi

  p90_limit="$(float_mul "$local_baseline_p90" "1.20")"
  if [[ "$result" == "OK" ]] && ! float_le "$local_current_p90" "$p90_limit"; then
    result="FAIL(p90 regression)"
  fi

  printf "%s|%s|%s|%s|%s|%s\n" \
    "$metric" \
    "$local_baseline_median" \
    "$local_current_median" \
    "$local_baseline_p90" \
    "$local_current_p90" \
    "$result"

  if [[ "$result" != "OK" ]]; then
    echo "benchmark guard failed for $metric" >&2
    exit 1
  fi
done

echo "benchmark guard check passed"
