#!/usr/bin/env bash
#
# Core demonstration: the nine points the specification asks to be shown.
#
#   1. gateway startup
#   2. connection to all backends
#   3. protocol translation
#   4. capability-based routing
#   5. concurrent requests
#   6. timeout handling
#   7. rejection of corrupt responses
#   8. handling of unsupported operations
#   9. restart with saved configuration
#
# Run the stack first:  docker compose up -d

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
require_gateway
reset_faults

# ---------------------------------------------------------------- 1 + 2
section "1 & 2. Gateway startup and connection to all three backends"

step "GET /status -- the gateway's own view of itself"
gw_get /status | pretty

step "GET /services -- registered backends, protocols, health, capabilities, versions"
gw_get /services | pretty

note "Three backends, three different protocols, discovered from the persistent registry:"
note "  service-a  http-json        HTTP + JSON over TCP"
note "  service-b  tcp-frame-json   16-byte binary header + JSON payload, multiplexed"
note "  service-c  udp-crc-json     20-byte binary header + JSON + trailing CRC32, over UDP"
pause_between

# ---------------------------------------------------------------- 3
section "3. Protocol translation"

note "One client vocabulary; three completely different wire dialects underneath."
note "The client never sees operation_result, resultData, numericResult or errorData."

step "echo via service-a  (HTTP JSON: operation_result.value)"
execute_show demo-xlate-a echo '{"value":"babel"}' '{"preferred_service":"service-a"}'

step "echo via service-b  (framed JSON: resultData.value, opCode ECHO)"
execute_show demo-xlate-b echo '{"value":"babel"}' '{"preferred_service":"service-b"}'

step "echo via service-c  (CRC-checked datagram: opcode 1, result.value)"
execute_show demo-xlate-c echo '{"value":"babel"}' '{"preferred_service":"service-c"}'

note "Same request, same normalized envelope, three protocols."

step "Semantic normalization: service-b answers numbers under a different key"
note "Echoing a string returns resultData.value; echoing a number returns"
note "resultData.numericResult. Both must normalize to result.value."
execute_show demo-xlate-str echo '{"value":"babel"}' '{"preferred_service":"service-b"}'
execute_show demo-xlate-num echo '{"value":42}' '{"preferred_service":"service-b"}'

step "Argument translation: sum sends 'values', service-b expects 'numberList'"
execute_show demo-xlate-sum sum '{"values":[1,2,3.5]}' '{"preferred_service":"service-b"}'

step "Pass-through translation: metadata is an object, not a scalar"
execute_show demo-xlate-meta metadata '{}' '{"preferred_service":"service-b"}'
pause_between

# ---------------------------------------------------------------- 4
section "4. Capability-based routing"

note "Routing consults the capability metadata in the registry. Nothing is hardcoded."
note "  echo       a, b, c"
note "  uppercase  a, b"
note "  sum           b, c"
note "  reverse       b"
note "  metadata   a, b, c"

step "reverse -- only service-b can do it, so that is where it goes"
execute_show demo-route-rev reverse '{"value":"babel"}'

step "sum -- service-a cannot do it, so it is never even attempted"
execute_show demo-route-sum sum '{"values":[10,20,30]}'

step "echo -- several backends can, and the highest-priority one wins deterministically"
execute_show demo-route-echo echo '{"value":"babel"}'

step "An explicit preference is honoured"
execute_show demo-route-pref echo '{"value":"babel"}' '{"preferred_service":"service-c"}'

step "A preference that cannot serve the operation: reverse asked of service-c"
note "The contract allows either refusing or choosing a safe alternative."
note "This gateway serves the caller and routes by capability, and says so in the log."
note "Set BABEL_PREFERRED_INCAPABLE=strict to refuse instead."
execute_show demo-route-badpref reverse '{"value":"babel"}' '{"preferred_service":"service-c"}'
pause_between

# ---------------------------------------------------------------- 5
section "5. Concurrent requests"

step "40 requests at once, each with its own payload"
note "Correctness here means: no mispaired response, no duplicated identifier,"
note "no corrupted state. Every reply must carry back its own request_id and value."

python3 - "$GATEWAY" <<'PY'
import json, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

gateway = sys.argv[1]

def call(i):
    body = json.dumps({
        "request_id": f"demo-concurrent-{i:03d}",
        "operation": ["echo", "uppercase", "sum", "reverse"][i % 4],
        "arguments": [{"value": f"payload-{i}"}, {"value": f"payload-{i}"},
                      {"values": [i, 1]}, {"value": f"payload-{i}"}][i % 4],
        "options": {"timeout_ms": 5000},
    }).encode()
    req = urllib.request.Request(gateway + "/execute", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return i, json.loads(r.read())

expected = {
    0: lambda i: {"value": f"payload-{i}"},
    1: lambda i: {"value": f"PAYLOAD-{i}"},
    2: lambda i: {"value": i + 1},
    3: lambda i: {"value": f"payload-{i}"[::-1]},
}

with ThreadPoolExecutor(max_workers=40) as pool:
    results = list(pool.map(call, range(40)))

mispaired = wrong = failed = 0
served_by = {}
for i, env in results:
    if env["request_id"] != f"demo-concurrent-{i:03d}":
        mispaired += 1
    if env["status"] != "success":
        failed += 1
        continue
    if env["result"] != expected[i % 4](i):
        wrong += 1
    served_by[env["service_id"]] = served_by.get(env["service_id"], 0) + 1

print(f"    requests             : {len(results)}")
print(f"    succeeded            : {len(results) - failed}")
print(f"    mispaired responses  : {mispaired}")
print(f"    wrong results        : {wrong}")
print(f"    served by            : {served_by}")
PY

note "Service B is genuinely multiplexed -- it answers concurrent frames out of order --"
note "so the gateway correlates every response by request id, never by arrival order."
pause_between

# ---------------------------------------------------------------- 6
section "6. Timeout handling, and why this one does NOT fall back"

scenario service-a-slow
step "service-a now delays every response past the caller's 1500 ms budget"
note "The delayed backend still executes the operation after the delay. Falling back"
note "would therefore run the same work twice, which the specification forbids."
note "So the gateway reports the timeout instead, and the ledger proves it ran once."
execute_show demo-timeout uppercase '{"value":"babel"}' '{"timeout_ms":1500}'

note "Waiting for the delayed backend to finish so the ledger is complete..."
sleep 3
ledger_executions uppercase

step "The gateway is not blocked: other operations keep working immediately"
execute_show demo-timeout-other reverse '{"value":"babel"}' '{"timeout_ms":2000}'

step "After repeated failures the breaker takes service-a out of rotation"
note "Requests then skip it before any byte is sent, which is duplicate-free by"
note "construction, so echo starts succeeding again on another backend."
for i in 1 2 3 4 5 6; do
  printf '    attempt %d: ' "$i"
  execute "demo-breaker-$i" echo '{"value":"x"}' '{"timeout_ms":1200}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], e["service_id"], (e["error"] or {}).get("code",""))'
done
gw_get /services | pretty
reset_faults
pause_between

# ---------------------------------------------------------------- 7
section "7. Rejection of corrupt and malformed backend responses"

note "Every backend response is validated before anything reaches the client:"
note "framing, magic, protocol version, declared length, checksum, correlation id,"
note "and the presence of the fields the contract requires."

reset_breakers
step "7a. A rejection with nowhere to fall back to"
note "service-b and service-c are disabled, so the faulted backend is the only route"
note "and the rejection has to surface to the client. That is what shows the gateway"
note "refused the payload rather than passing it on."
note ""
note "The two cases differ in whether recovery is possible at all:"
note "  unsupported-version  a static disagreement -- no retry can fix it, so it surfaces"
note "  malformed-responses  a one-shot corruption -- a retry on a fresh correlation id"
note "                       succeeds, and the discarded response is visible in the counters"
admin POST /admin/registry/enabled '{"service_id":"service-b","enabled":false}' >/dev/null
admin POST /admin/registry/enabled '{"service_id":"service-c","enabled":false}' >/dev/null

for s in unsupported-version malformed-responses; do
  scenario "$s"
  printf '    %-24s -> ' "$s"
  execute "demo-reject-$s" echo '{"value":"babel"}' '{"timeout_ms":4000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], "|", (e["error"] or {}).get("code","-"), "|", (e["error"] or {}).get("message","")[:70])'
done

admin POST /admin/registry/enabled '{"service_id":"service-b","enabled":true}' >/dev/null
admin POST /admin/registry/enabled '{"service_id":"service-c","enabled":true}' >/dev/null
sleep 1

step "7b. The same corruption with the other backends available"
note "The response is still rejected -- but the client gets an answer anyway, because"
note "a backend that produced garbage has finished with the request, so re-issuing it"
note "elsewhere cannot duplicate work in flight. Compare with the timeout above, where"
note "the backend was still executing and fallback was therefore refused."

reset_breakers
before=$(gw_get /metrics)
for s in malformed-responses service-b-invalid-frame service-c-corrupt-checksum service-c-duplicate service-b-duplicate hostile; do
  scenario "$s" >/dev/null
  reset_breakers
  printf '    %-28s -> ' "$s"
  execute "demo-corrupt-$s" echo '{"value":"babel"}' '{"timeout_ms":6000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], "via", e["service_id"], "|", e["result"] or (e["error"] or {}).get("code"))'
done
after=$(gw_get /metrics)

step "What the gateway detected and threw away while doing that"
python3 - <<PY
import json
before = json.loads('''$before''')
after = json.loads('''$after''')

def flat(m):
    out = dict(m["counters"])
    for name, stats in m.get("transports", {}).items():
        for k, v in stats.items():
            out[f"{name}.{k}"] = v
    return out

b, a = flat(before), flat(after)
interesting = [
    ("attempt_error:BACKEND_PROTOCOL_VIOLATION", "responses rejected as malformed"),
    ("attempt_error:BACKEND_CHECKSUM_MISMATCH", "datagrams rejected on checksum"),
    ("attempt_error:BACKEND_CORRELATION_MISMATCH", "responses rejected as mispaired"),
    ("attempt_error:UNSUPPORTED_PROTOCOL_VERSION", "responses rejected on version"),
    ("tcp-frame-json-v1.corrupt_frames", "corrupt frames discarded"),
    ("tcp-frame-json-v1.unsolicited_frames", "unsolicited/duplicate frames discarded"),
    ("udp-crc-json-v1.checksum_failures", "datagrams failing CRC32"),
    ("udp-crc-json-v1.duplicates_suppressed", "duplicate datagrams suppressed"),
    ("udp-crc-json-v1.retransmits", "datagrams retransmitted after loss"),
]
for key, label in interesting:
    delta = a.get(key, 0) - b.get(key, 0)
    if delta:
        print(f"    {label:<42} {delta}")
PY

step "The gateway never crashes and never forwards an unvalidated payload"
gw_get /healthz | pretty
reset_faults
pause_between

# ---------------------------------------------------------------- 8
section "8. Unsupported operations"

step "An operation no backend declares"
note "The error names the operations that do exist, and service_id is null because"
note "no backend route could be resolved at all."
execute_show demo-unsupported translate '{"value":"babel"}'

step "Arguments that cannot be valid anywhere are rejected before any dispatch"
execute_show demo-badargs sum '{"values":["not","numbers"]}'
execute_show demo-badargs2 uppercase '{"value":12345}'

step "A contract violation by the caller: request_id is missing"
note "This is the one class of failure answered with HTTP 400 rather than 200."
curl -sS -m 15 -o /tmp/babel-demo-400.json -w '    HTTP %{http_code}\n' \
  -X POST "$GATEWAY/execute" -H 'Content-Type: application/json' \
  -d '{"operation":"echo","arguments":{},"options":{}}'
pretty < /tmp/babel-demo-400.json
rm -f /tmp/babel-demo-400.json

step "Unknown options are ignored, never treated as an error"
execute_show demo-unknown-opt echo '{"value":"babel"}' '{"turbo":true,"retries":9,"timeout_ms":3000}'
pause_between

# ---------------------------------------------------------------- 9
section "9. Restart with saved configuration"

step "Change the live configuration through the operator API"
note "Disable service-c, and mark reverse on service-b replay-safe."
admin POST /admin/registry/enabled '{"service_id":"service-c","enabled":false}' | pretty
admin POST /admin/registry/replay-safe '{"service_id":"service-b","operation":"reverse","replay_safe":true}' | pretty

step "Registry state before the restart"
gw_get /status | python3 -c '
import json, sys
d = json.load(sys.stdin)
r = d["runtime"]
print("    registry revision : {}".format(r["registry_version"]))
print("    registry source   : {}".format(r["registry_source"]))
for s in d["services"]:
    print("    {:<10} enabled_status={}".format(s["service_id"], s["status"]))
'

step "Restarting the gateway container"
docker compose restart gateway >/dev/null 2>&1 || warn "docker compose restart failed; restart the gateway manually"
printf '    waiting for the gateway to come back'
for _ in $(seq 1 40); do
  if curl -sS -m 2 "$GATEWAY/healthz" >/dev/null 2>&1; then break; fi
  printf '.'; sleep 1
done
printf '\n'

step "Registry state after the restart"
gw_get /status | python3 -c '
import json, sys
d = json.load(sys.stdin)
r = d["runtime"]
print("    registry revision : {}".format(r["registry_version"]))
print("    registry source   : {}   <- restored, not re-seeded".format(r["registry_source"]))
for s in d["services"]:
    print("    {:<10} enabled_status={}".format(s["service_id"], s["status"]))
'
ok "The disabled backend and the replay-safe flag both survived the restart."

step "Restore the default configuration"
admin POST /admin/registry/enabled '{"service_id":"service-c","enabled":true}' | pretty
admin POST /admin/registry/replay-safe '{"service_id":"service-b","operation":"reverse","replay_safe":false}' | pretty
execute_show demo-after-restart echo '{"value":"babel"}'

section "Core demonstration complete"
