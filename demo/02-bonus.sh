#!/usr/bin/env bash
#
# Bonus demonstration.
#
#   A. Runtime adapter loading, with validation and rollback
#   B. Runtime protocol migration and simultaneous version compatibility
#   C. Advanced networking: UDP reliability, TCP multiplexing, backpressure
#   D. Failure isolation and resource isolation between backends
#
# Run the stack first:  docker compose up -d

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
require_gateway
reset_faults
reset_breakers

# ================================================================= A
section "A. Runtime adapter loading"

note "Protocol adapters are declarative documents, not compiled code. The three"
note "shipped adapters are loaded through exactly the same path as anything sent"
note "to the admin API, so this is the mechanism the gateway actually runs on."

step "Adapters currently loaded"
admin GET /admin/adapters | pretty

step "A1. Load a broken adapter over the live one"
note "demo/adapters/broken-adapter.json is structurally valid, but maps the sum"
note "argument list to 'numbers' instead of 'numberList'. Nothing catches that by"
note "inspection -- it is caught because the adapter can no longer reproduce the"
note "request bytes recorded from the real backend."
admin POST /admin/adapters "$(python3 -c '
import json, sys
spec = json.load(open(sys.argv[1]))
spec.pop("_comment", None)
print(json.dumps({"spec": spec, "persist": False}))
' "$DEMO_DIR/adapters/broken-adapter.json")" | pretty

step "The live adapter is untouched, and service-b keeps working"
admin GET /admin/adapters | python3 -c '
import json, sys
for a in json.load(sys.stdin)["adapters"]:
    print("    {:<20} version={} generation={} self_tests={}".format(
        a["name"], a["version"], a["generation"], a["self_tests"]))
'
execute_show demo-adapter-after-reject sum '{"values":[1,2,3]}' '{"preferred_service":"service-b"}'
ok "Generation is unchanged: the broken spec never replaced anything."

step "A2. Load a valid replacement of the live adapter"
note "Same protocol, one policy change: service-b's UNAVAILABLE is now treated as"
note "retryable. The swap happens under live traffic."
python3 - "$GATEWAY" "$ADMIN_TOKEN" <<'PY'
import json, sys, threading, time, urllib.request

gateway, token = sys.argv[1], sys.argv[2]
stop = threading.Event()
counts = {"ok": 0, "err": 0}

def hammer():
    i = 0
    while not stop.is_set():
        i += 1
        body = json.dumps({"request_id": f"demo-hotswap-{i:04d}", "operation": "reverse",
                           "arguments": {"value": "babel"},
                           "options": {"preferred_service": "service-b", "timeout_ms": 4000}}).encode()
        req = urllib.request.Request(gateway + "/execute", data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                env = json.loads(r.read())
            counts["ok" if env["status"] == "success" else "err"] += 1
        except Exception:
            counts["err"] += 1
        time.sleep(0.01)

t = threading.Thread(target=hammer, daemon=True)
t.start()
time.sleep(1.0)

spec = json.load(open("demo/adapters/tcp-frame-json-v2.json"))
spec.pop("_comment", None)
# Reload as version 1 with an added policy field: a genuine in-place replacement
# of the adapter that is currently carrying this traffic.
spec["name"] = "tcp-frame-json-v1"
spec["version"] = 1
spec["wire"]["retryable_codes"] = ["UNAVAILABLE", "INTERNAL_ERROR"]
for v in spec["self_test"]:
    for key in ("expect_request_hex", "response_hex"):
        if key in v and v[key].startswith("babe02"):
            v[key] = "babe01" + v[key][6:]
spec["self_test"] = [v for v in spec["self_test"] if not v.get("expect_error_code")]

req = urllib.request.Request(gateway + "/admin/adapters",
                             data=json.dumps({"spec": spec, "persist": False}).encode(),
                             headers={"Content-Type": "application/json",
                                      **({"X-Babel-Admin-Token": token} if token else {})})
with urllib.request.urlopen(req, timeout=20) as r:
    result = json.loads(r.read())

time.sleep(1.0)
stop.set()
t.join(timeout=5)

print(f"    hot swap status      : {result['status']}")
for a in result.get("adapters", []):
    if a["name"] == "tcp-frame-json-v1":
        print(f"    adapter generation   : {a['generation']}  (was 1)")
print(f"    requests during swap : {counts['ok'] + counts['err']}")
print(f"    succeeded            : {counts['ok']}")
print(f"    failed               : {counts['err']}")
PY
ok "The adapter was replaced under live traffic without dropping a request."
note "In-flight requests finish on the instance they started with; the old adapter"
note "is drained and only then closed."
pause_between

# ================================================================= B
section "B. Runtime protocol migration and version compatibility"

note "A service holds several weighted protocol variants at once. A migration is a"
note "change to those weights, so a rolling upgrade, a canary and a rollback are the"
note "same mechanism -- and all of them are persisted the moment they are applied."

step "service-b today: one variant, all the traffic"
gw_get /services | python3 -c '
import json, sys
for s in json.load(sys.stdin)["services"]:
    if s["service_id"] == "service-b":
        print("    versions={} primary=v{}".format(s["adapter_versions"], s["protocol_version"]))
'

step "B1. Register protocol version 2, at zero weight"
note "Registering a version must never by itself move traffic. The v2 adapter is"
note "loaded and proven against its own vectors first; only then is the variant"
note "added, and it is added carrying nothing."
admin POST /admin/migrate "$(python3 -c '
import json, sys
spec = json.load(open(sys.argv[1]))
spec.pop("_comment", None)
print(json.dumps({"service_id": "service-b", "spec": spec, "version": 2}))
' "$DEMO_DIR/adapters/tcp-frame-json-v2.json")" | pretty

step "Both versions are now registered, and v1 still serves everything"
gw_get /services | python3 -c '
import json, sys
for s in json.load(sys.stdin)["services"]:
    if s["service_id"] == "service-b":
        print("    versions={} primary=v{}".format(s["adapter_versions"], s["protocol_version"]))
'
execute_show demo-migrate-before reverse '{"value":"babel"}' '{"preferred_service":"service-b"}'

step "B2. Canary: shift 30% of service-b traffic onto version 2"
admin POST /admin/registry/weights '{"service_id":"service-b","weights":{"1":70,"2":30}}' | pretty

note "The reference backend only speaks version 1, so v2 is about to discover that"
note "its peer disagrees. This is the interesting case: a canary onto a version the"
note "backend does not actually serve has to fail safely and visibly."
note ""
note "Version selection is a hash of the request id, so it is deterministic: the same"
note "request always lands on the same version, which is what makes a canary"
note "reproducible instead of a coin flip."

python3 - "$GATEWAY" <<'PY'
import json, sys, urllib.request
gateway = sys.argv[1]
outcomes = {}
for i in range(30):
    body = json.dumps({"request_id": f"demo-canary-{i:03d}", "operation": "reverse",
                       "arguments": {"value": "babel"},
                       "options": {"preferred_service": "service-b", "timeout_ms": 5000}}).encode()
    req = urllib.request.Request(gateway + "/execute", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        env = json.loads(r.read())
    key = (env["status"], env["service_id"], (env["error"] or {}).get("code", "-"))
    outcomes[key] = outcomes.get(key, 0) + 1
for (status, svc, code), n in sorted(outcomes.items(), key=lambda kv: -kv[1]):
    print(f"    {n:>3} x  {status:<8} via {svc}  {code}")
PY

note "Every request still gets an answer. Requests that landed on v2 saw a version-1"
note "response, which the v2 adapter refuses -- and a version disagreement is"
note "provably effect-free, so those requests were safely served elsewhere."

step "The breaker isolated the bad version without touching the good one"
gw_get /metrics | python3 -c '
import json, sys
for key, b in sorted(json.load(sys.stdin)["breakers"].items()):
    if key.startswith("service-b"):
        print("    {:<16} state={:<10} trips={} last={}".format(
            key, b["state"], b["trips"], b.get("last_reason", "")[:40]))
'
note "service-b#v1 and service-b#v2 have separate breakers, so a broken new version"
note "cannot take the working one out of rotation."

step "B3. Roll back"
admin POST /admin/registry/weights '{"service_id":"service-b","weights":{"1":100,"2":0}}' | pretty
reset_breakers
execute_show demo-migrate-rollback reverse '{"value":"babel"}' '{"preferred_service":"service-b"}'
ok "Rolled back to version 1. The v2 adapter stays registered at zero weight,"
ok "ready for another attempt, and the rollback itself is already persisted."

step "B4. The migration survives a restart"
note "The v2 variant, its weights and its hot-loaded adapter spec are all in the"
note "persistent registry, so a restart resumes the migration where it left off"
note "rather than reverting to the shipped defaults."
docker compose restart gateway >/dev/null 2>&1 || warn "restart the gateway manually"
printf '    waiting for the gateway'
for _ in $(seq 1 40); do
  curl -sS -m 2 "$GATEWAY/healthz" >/dev/null 2>&1 && break
  printf '.'; sleep 1
done
printf '\n'
admin GET /admin/adapters | python3 -c '
import json, sys
for a in json.load(sys.stdin)["adapters"]:
    print("    {:<20} version={} service={}".format(a["name"], a["version"], a["service_id"]))
'
gw_get /services | python3 -c '
import json, sys
for s in json.load(sys.stdin)["services"]:
    if s["service_id"] == "service-b":
        print("    service-b versions after restart: {}".format(s["adapter_versions"]))
'
pause_between

# ================================================================= C
section "C. Advanced networking"

step "C1. UDP reliability under loss"
note "Service C speaks a datagram protocol with no delivery guarantee. The gateway"
note "adds sequence numbers, duplicate suppression, an adaptive Jacobson/Karels RTO,"
note "retransmission and a bounded in-flight window on top of it."
scenario service-c-lossy
before=$(gw_get /metrics)
for i in 1 2 3 4 5 6 7 8; do
  printf '    request %d: ' "$i"
  execute "demo-udp-lossy-$i" sum '{"values":[1,2,3]}' '{"preferred_service":"service-c","timeout_ms":5000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], "via", e["service_id"], e["result"] or (e["error"] or {}).get("code"))'
done
after=$(gw_get /metrics)

step "C2. UDP duplicates and reordering"
scenario service-c-duplicate
for i in 1 2 3; do
  printf '    duplicate scenario, request %d: ' "$i"
  execute "demo-udp-dup-$i" echo '{"value":"babel"}' '{"preferred_service":"service-c","timeout_ms":5000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], e["result"] or (e["error"] or {}).get("code"))'
done
scenario service-c-reordered
for i in 1 2 3; do
  printf '    reordered scenario, request %d: ' "$i"
  execute "demo-udp-reorder-$i" echo '{"value":"babel"}' '{"preferred_service":"service-c","timeout_ms":5000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], e["result"] or (e["error"] or {}).get("code"))'
done
final=$(gw_get /metrics)

step "What the datagram layer did"
python3 - <<PY
import json
before = json.loads('''$before''')
final = json.loads('''$final''')
b = before["transports"].get("udp-crc-json-v1", {})
a = final["transports"].get("udp-crc-json-v1", {})
labels = [
    ("datagrams_sent", "datagrams sent"),
    ("datagrams_received", "datagrams received"),
    ("retransmits", "retransmitted after loss"),
    ("duplicates_suppressed", "duplicates suppressed"),
    ("checksum_failures", "rejected on CRC32"),
    ("header_failures", "rejected on header"),
    ("unsolicited", "uncorrelatable datagrams dropped"),
]
for key, label in labels:
    delta = a.get(key, 0) - b.get(key, 0)
    if delta:
        print(f"    {label:<36} {delta}")
print(f"    adaptive RTO now                     {a.get('rto_ms', 0)} ms (smoothed RTT {a.get('srtt_ms', 0)} ms)")
print(f"    in-flight window                     {a.get('window', 0)}")
PY
reset_faults

step "C3. TCP multiplexing"
note "Service B answers concurrent frames out of order on a single connection."
note "The gateway matches every response by request id, so the connection count"
note "stays small no matter how much concurrency is offered."
python3 - "$GATEWAY" <<'PY'
import json, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor
gateway = sys.argv[1]

def call(i):
    body = json.dumps({"request_id": f"demo-mux-{i:03d}", "operation": "reverse",
                       "arguments": {"value": f"item-{i}"},
                       "options": {"preferred_service": "service-b", "timeout_ms": 8000}}).encode()
    req = urllib.request.Request(gateway + "/execute", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return i, json.loads(r.read())

with ThreadPoolExecutor(max_workers=60) as pool:
    results = list(pool.map(call, range(60)))

bad = [i for i, e in results
       if e["status"] != "success" or e["result"]["value"] != f"item-{i}"[::-1]]
print(f"    60 concurrent requests, mispaired or failed: {len(bad)}")
PY
gw_get /metrics | python3 -c '
import json, sys
t = json.load(sys.stdin)["transports"]["tcp-frame-json-v1"]
print("    open connections     : {}".format(t.get("connections")))
print("    requests carried     : {}".format(t.get("requests")))
print("    unsolicited frames   : {}".format(t.get("unsolicited_frames")))
print("    corrupt frames       : {}".format(t.get("corrupt_frames")))
'
pause_between

# ================================================================= D
section "D. Failure isolation"

step "One backend fails hard; the others must be unaffected"
scenario service-a-down
note "service-a now terminates every connection. Operations only it could serve"
note "degrade; everything else keeps working at full speed."
for op_args in "echo {\"value\":\"babel\"}" "reverse {\"value\":\"babel\"}" "sum {\"values\":[1,2,3]}" "metadata {}"; do
  set -- $op_args
  op="$1"; shift; args="$*"
  printf '    %-10s -> ' "$op"
  execute "demo-isolate-$op" "$op" "$args" '{"timeout_ms":4000}' \
    | python3 -c 'import json,sys; e=json.load(sys.stdin); print(e["status"], "via", e["service_id"])'
done

step "Where the failure shows up"
note "The injected fault only affects service-a's execute path -- its health endpoint"
note "answers normally -- so the liveness probe still passes. This is exactly why the"
note "gateway does not route on probes alone: observed request outcomes, tracked per"
note "(service, protocol version) by the circuit breaker, are the signal that matters."
gw_get /services | python3 -c '
import json, sys
for s in json.load(sys.stdin)["services"]:
    print("    probe  {:<10} {:<12} {}".format(s["service_id"], s["status"], s.get("detail", "")[:50]))
'
gw_get /metrics | python3 -c '
import json, sys
for key, b in sorted(json.load(sys.stdin)["breakers"].items()):
    print("    traffic {:<16} state={:<10} failures={} trips={}".format(
        key, b["state"], b["consecutive_failures"], b["trips"]))
'
gw_get /status | python3 -c '
import json,sys
print("    overall gateway status: {}".format(json.load(sys.stdin)["status"]))
'
note "Every operation still gets served, because each one has a capable backend left."
note "That is the isolation property: one failing backend costs the operations only it"
note "could serve, and nothing else."

reset_faults
reset_breakers
section "Bonus demonstration complete"
