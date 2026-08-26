#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/test-ecosystem-stress.sh" heavy
