#!/usr/bin/env bash
set -euo pipefail
script_dir="$(dirname "$0")"
for target in 100 250 500 1000; do
  echo "=== breakpoint escalation: ${target} synchronized paths ==="
  if RTDB_ECOSYSTEM_PATHS="$target" RTDB_ECOSYSTEM_GENERATIONS="${RTDB_ECOSYSTEM_GENERATIONS:-100}" \
      "$script_dir/test-ecosystem-stress.sh" breakpoint; then
    continue
  fi
  echo "breakpoint reached at ${target} synchronized paths; see artifacts/ecosystem for the failure and resource samples"
  exit 0
done
echo "breakpoint escalation reached 1000 synchronized paths without a correctness failure"
