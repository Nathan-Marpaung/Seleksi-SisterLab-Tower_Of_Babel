#!/usr/bin/env bash
# Shared helpers for the demonstration scripts.
#
# Everything here talks to the gateway and to Grader Hiura's fault control plane
# over plain HTTP, so the demos are readable as a transcript: what was asked,
# what came back, and what the backend's own execution ledger recorded.

set -uo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
CONTROL="${CONTROL:-http://localhost:8090}"
CONTROL_TOKEN="${BABEL_CONTROL_TOKEN:-babel-local-dev}"
ADMIN_TOKEN="${BABEL_ADMIN_TOKEN:-}"

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_HEAD=$'\033[1;36m'; C_STEP=$'\033[1m'
  C_OK=$'\033[32m'; C_WARN=$'\033[33m'; C_DIM=$'\033[2m'
else
  C_RESET=""; C_HEAD=""; C_STEP=""; C_OK=""; C_WARN=""; C_DIM=""
fi

# pretty pipes JSON through a formatter when one is available, and passes it
# through untouched when none is, so the scripts run on a bare machine.
pretty() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  elif command -v python3 >/dev/null 2>&1; then
    python3 -m json.tool 2>/dev/null || cat
  else
    cat
  fi
}

section() {
  printf '\n%s================================================================%s\n' "$C_HEAD" "$C_RESET"
  printf '%s%s%s\n' "$C_HEAD" "$1" "$C_RESET"
  printf '%s================================================================%s\n' "$C_HEAD" "$C_RESET"
}

step() { printf '\n%s--- %s%s\n' "$C_STEP" "$1" "$C_RESET"; }
note() { printf '%s    %s%s\n' "$C_DIM" "$1" "$C_RESET"; }
ok()   { printf '%s    %s%s\n' "$C_OK" "$1" "$C_RESET"; }
warn() { printf '%s    %s%s\n' "$C_WARN" "$1" "$C_RESET"; }

# execute POSTs one operation to the gateway.
#   execute <request_id> <operation> [arguments_json] [options_json]
execute() {
  local rid="$1" op="$2" args="${3:-{\}}" opts="${4:-{\}}"
  curl -sS -m 30 -X POST "$GATEWAY/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"request_id\":\"$rid\",\"operation\":\"$op\",\"arguments\":$args,\"options\":$opts}"
}

# execute_show prints the request line and the envelope it produced.
execute_show() {
  local rid="$1" op="$2" args="${3:-{\}}" opts="${4:-{\}}"
  note "POST /execute  operation=$op arguments=$args options=$opts"
  execute "$rid" "$op" "$args" "$opts" | pretty
}

gw_get() { curl -sS -m 15 "$GATEWAY$1"; }

admin() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -m 30 -X "$method" "$GATEWAY$path" -H 'Content-Type: application/json')
  [[ -n "$ADMIN_TOKEN" ]] && args+=(-H "X-Babel-Admin-Token: $ADMIN_TOKEN")
  [[ -n "$body" ]] && args+=(-d "$body")
  curl "${args[@]}"
}

control() {
  local method="$1" path="$2"
  curl -sS -m 15 -X "$method" "$CONTROL$path" -H "X-Babel-Control-Token: $CONTROL_TOKEN"
}

scenario() {
  if ! control POST /reset >/dev/null 2>&1; then
    warn "control plane at $CONTROL is not answering; scenario \"$1\" was NOT applied"
    return 1
  fi
  if [[ "$1" != "normal" ]]; then
    control POST "/scenario/$1" >/dev/null 2>&1
  fi
  note "fault scenario active: $1"
}

reset_faults() { control POST /reset >/dev/null; }

# reset_breakers clears circuit state so a section starts from a clean slate
# instead of inheriting the previous section's tripped breakers.
reset_breakers() { admin POST /admin/breakers/reset '{}' >/dev/null; }

# ledger_executions prints how many times each backend actually *ran* an
# operation, which is how the no-duplicate-work claims below are checked rather
# than asserted. The control plane records a "received" and an "executed" stage
# per request; only "executed" means work was really done.
ledger_executions() {
  if ! command -v python3 >/dev/null 2>&1; then
    note "(ledger summary needs python3; raw ledger available at $CONTROL/ledger)"
    return
  fi
  local body
  body="$(control GET /ledger 2>/dev/null)"
  # The control plane is Grader Hiura's, not ours, and it is not part of what is
  # being demonstrated. If it is momentarily unreachable, say so plainly instead
  # of letting a JSON decode error land in the middle of the transcript.
  if [[ -z "$body" || "${body:0:1}" != "[" ]]; then
    warn "control plane at $CONTROL is not answering; skipping the ledger summary"
    return
  fi
  printf '%s' "$body" | python3 -c '
import json, sys
want = sys.argv[1] if len(sys.argv) > 1 else None
rows = [e for e in json.load(sys.stdin) if e.get("stage") == "executed"]
if want:
    rows = [e for e in rows if e.get("operation") == want]
for e in rows:
    print("    executed: {:<10} {}".format(e["service_id"], e["operation"]))
if not rows:
    print("    (no operation was executed by any backend)")
print("    total executions: {}".format(len(rows)))
' "$@"
}

require_gateway() {
  if ! curl -sS -m 5 "$GATEWAY/healthz" >/dev/null 2>&1; then
    printf '%sThe gateway is not reachable at %s.%s\n' "$C_WARN" "$GATEWAY" "$C_RESET" >&2
    printf 'Start the stack first:  docker compose up -d\n' >&2
    exit 1
  fi
}

pause_between() { sleep "${DEMO_PAUSE:-1}"; }
