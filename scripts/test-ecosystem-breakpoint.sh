#!/usr/bin/env bash
set -euo pipefail
script_dir="$(dirname "$0")"
for target in 100 250 500 1000; do
  echo "=== breakpoint escalation: ${target} synchronized paths ==="
  set +e
  RTDB_ECOSYSTEM_PATHS="$target" RTDB_ECOSYSTEM_GENERATIONS="${RTDB_ECOSYSTEM_GENERATIONS:-100}" \
    "$script_dir/test-ecosystem-stress.sh" breakpoint
  status=$?
  set -e
  case "$status" in
    0) continue ;;
    75) echo "breakpoint reached at ${target} synchronized paths (CAPACITY_LIMIT)"; exit 0 ;;
    1) echo "breakpoint found CORRECTNESS_FAILURE at ${target} synchronized paths" >&2; exit 1 ;;
    *) echo "breakpoint found HARNESS_OR_ENVIRONMENT_FAILURE at ${target} synchronized paths" >&2; exit 2 ;;
  esac
done
echo "breakpoint escalation reached 1000 synchronized paths without a correctness failure"
