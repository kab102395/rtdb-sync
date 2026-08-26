#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-sync}"
if [[ "$project_id" != demo-* ]]; then
  echo "Refusing emulator tests for non-demo project: $project_id" >&2
  exit 2
fi

for cycle in $(seq 1 10); do
  echo "Resilience cycle $cycle/10"
  RTDB_RECOVERY_PATHS=64 FIREBASE_PROJECT_ID="$project_id" \
    ./scripts/test-emulator-recovery.sh
done
echo "Emulator 64-path ten-cycle resilience profile passed"
