#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-ecosystem-durable}"
[[ "$project_id" == demo-* ]] || { echo "Refusing durable ecosystem test for non-demo project: $project_id" >&2; exit 2; }
for port in 9000 4000 4400 4500; do
  if (command -v ss >/dev/null && ss -ltn | awk '{print $4}' | grep -Eq ":${port}$") ||
     (command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1); then
    echo "Refusing to start Firebase: port $port is already listening" >&2; exit 2
  fi
done

store="$(mktemp -d "${TMPDIR:-/tmp}/rtdb-ecosystem-durable.XXXXXX")"
run="durable-$(date -u +%Y%m%dT%H%M%SZ)-$$"
root="ecosystem/$run"
cleanup() { rm -rf "$store"; }
trap cleanup EXIT

run_phase() {
  local phase="$1" mode="$2" crash="${3:-}"
  local -a envs=(
    "FIREBASE_PROJECT_ID=$project_id"
    "RTDB_ECOSYSTEM_DURABLE_STORE=$store"
    "RTDB_ECOSYSTEM_DURABLE_NAMESPACE=$project_id"
    "RTDB_ECOSYSTEM_DURABLE_ROOT=$root"
    "RTDB_ECOSYSTEM_DURABLE_BACKLOG=${RTDB_ECOSYSTEM_DURABLE_BACKLOG:-100}"
    "RTDB_ECOSYSTEM_DURABLE_PHASE=$phase"
  )
  [[ "$mode" == offline ]] && envs+=("FIREBASE_DATABASE_EMULATOR_HOST=127.0.0.1:9")
  [[ "$crash" == yes ]] && envs+=("RTDB_ECOSYSTEM_DURABLE_CRASH=1")
  echo "Durable ecosystem phase: $phase"
  if [[ "$mode" == offline ]]; then
    env "${envs[@]}" cargo test --test ecosystem -- --ignored --exact integrated_ecosystem_durable_replay_with_active_remote_writers
  else
    env "${envs[@]}" npx --yes firebase-tools emulators:exec --only database --project "$project_id" \
      "cargo test --test ecosystem -- --ignored --exact integrated_ecosystem_durable_replay_with_active_remote_writers"
  fi
}

run_phase seed live
run_phase queue offline yes
run_phase replay live
echo "Durable ecosystem outage/process-death/replay acceptance passed"
