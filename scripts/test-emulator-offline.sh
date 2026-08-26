#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-sync}"
if [[ "$project_id" != demo-* ]]; then
  echo "Refusing emulator tests for non-demo project: $project_id" >&2
  exit 2
fi

store_dir="$(mktemp -d /tmp/rtdb-sync-offline.XXXXXX)"
log_file="$(mktemp /tmp/rtdb-sync-offline-log.XXXXXX)"
emulator_pid=""
cleanup() {
  [[ -n "$emulator_pid" ]] && kill -- "-$emulator_pid" 2>/dev/null || kill "$emulator_pid" 2>/dev/null || true
  rm -rf "$store_dir" "$log_file"
}
trap cleanup EXIT INT TERM

start_emulator() {
  setsid npx --yes firebase-tools emulators:start --only database --project "$project_id" >"$log_file" 2>&1 &
  emulator_pid=$!
  for _ in $(seq 1 60); do
    if (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then exec 3>&-; return 0; fi
    sleep 1
  done
  echo "emulator did not start; log: $log_file" >&2
  exit 1
}

stop_emulator() {
  kill -- "-$emulator_pid" 2>/dev/null || kill "$emulator_pid"
  wait "$emulator_pid" 2>/dev/null || true
  emulator_pid=""
  for _ in $(seq 1 30); do
    if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then return 0; fi
    exec 3>&-
    sleep 1
  done
  echo "emulator did not stop; log: $log_file" >&2
  exit 1
}

run_phase() {
  RTDB_DURABLE_STORE="$store_dir" FIREBASE_DATABASE_EMULATOR_HOST=127.0.0.1:9000 \
    cargo test --lib "tests::$1" -- --ignored --exact
}

echo "Phase 1: persist snapshot against emulator"
start_emulator
run_phase durable_emulator_seed_persists_snapshot
stop_emulator
echo "Phase 2: queue mutation with emulator stopped"
run_phase durable_emulator_queues_while_database_is_down
echo "Phase 3: restart emulator and replay durable mutation"
start_emulator
run_phase durable_emulator_replays_after_database_returns
echo "Durable offline emulator acceptance passed"
