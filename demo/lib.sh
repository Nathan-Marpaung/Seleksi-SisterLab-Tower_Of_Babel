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

# pretty formats JSON when a formatter is available, and prints the input
# unchanged when there is none or when the input is not JSON at all. It must
# never fail, because it sits at the end of almost every pipeline here.
pretty() {
  local body
  body="$(cat)"
  if [[ -z "$body" ]]; then
    printf '    (no response body)\n'
    return
  fi
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$body" | jq . 2>/dev/null && return
  elif command -v python3 >/dev/null 2>&1; then
    printf '%s' "$body" | python3 -m json.tool 2>/dev/null && return
  fi
  printf '%s\n' "$body"
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
#
# It always writes a valid envelope to stdout, including when curl itself fails
# before any envelope could be received. Two reasons for that.
#
# First, everything downstream parses this output as JSON, and a client-side
# hiccup should not turn into a Python traceback in the middle of the
# transcript. Under WSL in particular, the localhost forwarder occasionally
# resets a connection to a Windows-published port.
#
# Second, the substitute envelope is labelled CLIENT_TRANSPORT_ERROR precisely
# so it can never be mistaken for something the gateway said. The failure is in
# the demo client, and the transcript should say so.
#
# Deliberately no retry here. If curl dropped the connection after the gateway
# had already dispatched the work, re-sending would execute the operation a
# second time, and section 6 relies on the backend ledger showing exactly one
# execution. A missing answer is better than a corrupted measurement.
execute() {
  local rid="$1" op="$2" args="${3:-{\}}" opts="${4:-{\}}"
  local body
  body="$(curl -sS -m 30 -X POST "$GATEWAY/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"request_id\":\"$rid\",\"operation\":\"$op\",\"arguments\":$args,\"options\":$opts}" 2>/dev/null)"

  if [[ -z "$body" || "${body:0:1}" != "{" ]]; then
    printf '{"request_id":"%s","status":"error","service_id":null,"operation":"%s","result":null,"error":{"code":"CLIENT_TRANSPORT_ERROR","message":"the demo client could not read a reply from the gateway; this is a client-side failure, not a gateway response","retryable":true}}' \
      "$rid" "$op"
    return
  fi
  printf '%s' "$body"
}

# execute_show prints the request line and the envelope it produced.
execute_show() {
  local rid="$1" op="$2" args="${3:-{\}}" opts="${4:-{\}}"
  note "POST /execute  operation=$op arguments=$args options=$opts"
  execute "$rid" "$op" "$args" "$opts" | pretty
}

# gw_get reads a gateway endpoint, and like execute it guarantees JSON on
# stdout so the parsers further down the pipeline cannot blow up on an empty
# body.
gw_get() {
  local body
  body="$(curl -sS -m 15 "$GATEWAY$1" 2>/dev/null)"
  if [[ -z "$body" || ( "${body:0:1}" != "{" && "${body:0:1}" != "[" ) ]]; then
    printf '{"status":"error","error":{"code":"CLIENT_TRANSPORT_ERROR","message":"could not read %s from the gateway"},"services":[],"backends":{},"runtime":{},"metrics":{},"breakers":{},"transports":{}}' "$1"
    return
  fi
  printf '%s' "$body"
}

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
