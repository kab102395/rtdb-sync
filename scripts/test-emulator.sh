#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-sync}"

if [[ "$project_id" != demo-* ]]; then
  echo "Refusing emulator tests for non-demo project: $project_id" >&2
  exit 2
fi

for port in 9000 4000; do
  if (command -v ss >/dev/null && ss -ltn | awk '{print $4}' | grep -Eq ":${port}$") || \
     (command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1); then
    echo "Refusing to start Firebase: host port $port is already listening" >&2
    exit 2
  fi
done

if ! command -v npx >/dev/null; then
  echo "npx is required; install Node.js first" >&2
  exit 2
fi

echo "Starting local RTDB emulator for $project_id"
npx --yes firebase-tools emulators:exec \
  --only database \
  --project "$project_id" \
  "cargo test --all-targets --all-features"
