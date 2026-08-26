#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-sync}"
if [[ "$project_id" != demo-* ]]; then
  echo "Refusing emulator tests for non-demo project: $project_id" >&2
  exit 2
fi
if ! command -v npx >/dev/null; then
  echo "npx is required; install Node.js first" >&2
  exit 2
fi

echo "Starting heavy local RTDB profile for $project_id"
npx --yes firebase-tools emulators:exec --only database --project "$project_id" \
  "RTDB_SYNC_HEAVY=1 cargo test --all-targets --all-features"
