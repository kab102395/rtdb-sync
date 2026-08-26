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

control_dir="$(mktemp -d /tmp/rtdb-sync-recovery.XXXXXX)"
ready="$control_dir/ready"
restored="$control_dir/restored"
log_file="$control_dir/firebase.log"
emulator_pid=""
test_pid=""

cleanup() {
  [[ -n "$test_pid" ]] && kill "$test_pid" 2>/dev/null || true
  [[ -n "$emulator_pid" ]] && kill -- "-$emulator_pid" 2>/dev/null || kill "$emulator_pid" 2>/dev/null || true
  rm -rf "$control_dir"
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
  return 1
}

wait_for_port_close() {
  for _ in $(seq 1 30); do
    if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then return 0; fi
    exec 3>&-
    sleep 1
  done
  echo "emulator did not stop; log: $log_file" >&2
  return 1
}

echo "Starting emulator recovery test for $project_id"
start_emulator
FIREBASE_DATABASE_EMULATOR_HOST=127.0.0.1:9000 RTDB_RECOVERY_READY="$ready" RTDB_RECOVERY_RESTORED="$restored" \
  cargo test --lib tests::emulator_restart_rehydrates_and_recovers -- --ignored --exact &
test_pid=$!
for _ in $(seq 1 60); do [[ -f "$ready" ]] && break; sleep 1; done
[[ -f "$ready" ]] || { echo "synchronizer did not become ready" >&2; exit 1; }
echo "Stopping emulator to force stream outage"
kill -- "-$emulator_pid" 2>/dev/null || kill "$emulator_pid"
wait "$emulator_pid" 2>/dev/null || true
emulator_pid=""
wait_for_port_close
echo "Restarting emulator"
start_emulator
touch "$restored"
wait "$test_pid"
test_pid=""
echo "Emulator recovery test passed"
