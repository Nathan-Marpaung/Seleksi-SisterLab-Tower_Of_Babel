#!/usr/bin/env bash
#
# Runs the whole demonstration end to end.
#
#   docker compose up -d
#   ./demo/run-all.sh
#
# Pass --core or --bonus to run only one half.

set -uo pipefail
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEMO_DIR/.."

what="${1:-all}"

case "$what" in
  --core)  bash "$DEMO_DIR/01-core.sh" ;;
  --bonus) bash "$DEMO_DIR/02-bonus.sh" ;;
  all|"")
    bash "$DEMO_DIR/01-core.sh"
    bash "$DEMO_DIR/02-bonus.sh"
    ;;
  *)
    echo "usage: $0 [--core|--bonus]" >&2
    exit 2
    ;;
esac

# Leave the environment exactly as it was found, so a second run starts from
# the same place as the first.
curl -sS -m 10 -X POST "${CONTROL:-http://localhost:8090}/reset" \
  -H "X-Babel-Control-Token: ${BABEL_CONTROL_TOKEN:-babel-local-dev}" >/dev/null 2>&1 || true
echo
echo "Fault environment reset. Gateway left running."
