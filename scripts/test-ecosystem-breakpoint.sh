#!/usr/bin/env bash
set -euo pipefail
script_dir="$(dirname "$0")"
target_runs="${RTDB_ECOSYSTEM_RUNS_PER_TIER:-2}"
for target in 100 150 200 250 500; do
  echo "=== breakpoint tier: ${target} synchronized paths (${target_runs} runs) ==="
  tier_capacity=0
  tier_pass=0
  for run in $(seq 1 "$target_runs"); do
    echo "--- breakpoint run ${run}/${target_runs}: ${target} paths ---"
    set +e
    RTDB_ECOSYSTEM_PATHS="$target" RTDB_ECOSYSTEM_GENERATIONS="${RTDB_ECOSYSTEM_GENERATIONS:-100}" \
      "$script_dir/test-ecosystem-stress.sh" breakpoint
    status=$?
    set -e
    case "$status" in
      0) tier_pass=1 ;;
      75) tier_capacity=1; echo "run ${run}: CAPACITY_LIMIT at ${target} paths" ;;
      1) echo "breakpoint found CORRECTNESS_FAILURE at ${target} paths (run ${run})" >&2; exit 1 ;;
      *) echo "breakpoint found HARNESS_OR_ENVIRONMENT_FAILURE at ${target} paths (run ${run})" >&2; exit 2 ;;
    esac
  done
  if [[ "$tier_capacity" -eq 1 && "$tier_pass" -eq 1 ]]; then
    echo "breakpoint tier ${target} is UNSTABLE: mixed PASS/CAPACITY_LIMIT results" >&2
    exit 2
  elif [[ "$tier_capacity" -eq 1 ]]; then
    echo "breakpoint reached at ${target} synchronized paths (CAPACITY_LIMIT; ${target_runs} runs)"
    exit 0
  fi
done
echo "breakpoint escalation reached 500 synchronized paths without a correctness failure"
