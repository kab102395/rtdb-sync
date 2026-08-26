#!/usr/bin/env bash
set -euo pipefail

project_id="${FIREBASE_PROJECT_ID:-demo-rtdb-ecosystem}"
[[ "$project_id" == demo-* ]] || { echo "Refusing ecosystem stress for non-demo project: $project_id" >&2; exit 2; }
command -v npx >/dev/null || { echo "npx is required" >&2; exit 2; }
for port in 9000 4000 4400 4500; do
  if (command -v ss >/dev/null && ss -ltn | awk '{print $4}' | grep -Eq ":${port}$") ||
     (command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1); then
    echo "Refusing to start Firebase: port $port is already listening" >&2; exit 2
  fi
done

profile="${1:-standard}"
case "$profile" in
  standard) paths=100; generations=100 ;;
  heavy) paths="${RTDB_ECOSYSTEM_PATHS:-500}"; generations="${RTDB_ECOSYSTEM_GENERATIONS:-200}" ;;
  soak) paths="${RTDB_ECOSYSTEM_PATHS:-250}"; generations="${RTDB_ECOSYSTEM_GENERATIONS:-1800}" ;;
  breakpoint) paths="${RTDB_ECOSYSTEM_PATHS:-100}"; generations="${RTDB_ECOSYSTEM_GENERATIONS:-100}" ;;
  *) echo "usage: $0 [standard|heavy|soak|breakpoint]" >&2; exit 2 ;;
esac

artifact_dir="${RTDB_ECOSYSTEM_ARTIFACT_DIR:-artifacts/ecosystem}"
mkdir -p "$artifact_dir"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
artifact="$artifact_dir/${profile}-${stamp}.log"
metadata="$artifact_dir/${profile}-${stamp}.env"
resource_samples="$artifact_dir/${profile}-${stamp}.resources.tsv"
{
  echo "profile=$profile"
  echo "project=$project_id"
  echo "paths=$paths"
  echo "generations=$generations"
  echo "rust=$(rustc --version)"
  echo "node=$(node --version)"
  echo "firebase=$(npx --yes firebase-tools --version 2>/dev/null | head -1)"
  echo "rtdb_sync_commit=$(git rev-parse HEAD)"
  echo "rtdb_rs_commit=$(git -C ../rtdb-rs rev-parse HEAD)"
  echo "rtdb_typed_commit=$(git -C ../rtdb-typed rev-parse HEAD)"
  echo "rtdb_admin_commit=$(git -C ../rtdb-admin rev-parse HEAD)"
  echo "host=$(uname -a)"
  echo "cpu=$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  echo "ram=$(free -h 2>/dev/null | awk '/^Mem:/ {print $2}' || true)"
} | tee "$metadata"

echo "Starting $profile four-crate ecosystem profile; results: $artifact"
export FIREBASE_PROJECT_ID="$project_id"
export RTDB_ECOSYSTEM_PATHS="$paths"
export RTDB_ECOSYSTEM_GENERATIONS="$generations"
export RTDB_ECOSYSTEM_RUN="${profile}-${stamp}"
start_ns="$(date +%s)"
echo -e "timestamp\tpid\tcpu_percent\trss_kb" > "$resource_samples"
resource_monitor() {
  while true; do
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ps -eo pid=,pcpu=,rss=,args= | awk -v now="$now" '/firebase-database-emulator-v.*jar/ {print now "\t" $1 "\t" $2 "\t" $3}' >> "$resource_samples"
    sleep 2
  done
}
resource_monitor &
monitor_pid=$!
cleanup_monitor() { kill "$monitor_pid" 2>/dev/null || true; }
trap cleanup_monitor EXIT
set +e
npx --yes firebase-tools emulators:exec --only database --project "$project_id" \
  "env FIREBASE_PROJECT_ID='$project_id' RTDB_ECOSYSTEM_PATHS='$paths' RTDB_ECOSYSTEM_GENERATIONS='$generations' RTDB_ECOSYSTEM_RUN='$profile-$stamp' /usr/bin/time -v cargo test --test ecosystem -- --ignored --exact integrated_ecosystem_standard_and_profiles_converge" \
  2>&1 | tee "$artifact"
test_status=${PIPESTATUS[0]}
set -e
end_ns="$(date +%s)"
cleanup_monitor
if [[ "$test_status" -ne 0 ]]; then
  if rg -q 'sync handle did not connect|has been running for over|Elapsed \(\(\)\)' "$artifact"; then
    echo "result=CAPACITY_LIMIT" | tee -a "$metadata" "$artifact"
    exit 75
  elif rg -q 'panicked at|FAILED|test result: FAILED' "$artifact"; then
    echo "result=CORRECTNESS_FAILURE" | tee -a "$metadata" "$artifact"
    exit 1
  else
    echo "result=HARNESS_OR_ENVIRONMENT_FAILURE" | tee -a "$metadata" "$artifact"
    exit 2
  fi
fi
echo "duration_seconds=$((end_ns-start_ns))" | tee -a "$metadata" "$artifact"
echo "resource_samples=$resource_samples" | tee -a "$metadata"
echo "profile=$profile result=passed artifact=$artifact resource_samples=$resource_samples"
