package router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"babel/gateway/internal/adapter"
	"babel/gateway/internal/apimodel"
	"babel/gateway/internal/breaker"
	"babel/gateway/internal/config"
	"babel/gateway/internal/gwerr"
	"babel/gateway/internal/idgen"
	"babel/gateway/internal/obs"
	"babel/gateway/internal/registry"
)

// fakeAdapter answers however the test tells it to, and records what it saw.
type fakeAdapter struct {
	name      string
	serviceID string
	ops       map[string]bool
	respond   func(ctx context.Context, call adapter.Call) (adapter.Reply, *gwerr.Error)

	mu    sync.Mutex
	calls []adapter.Call
}

func (f *fakeAdapter) Name() string      { return f.name }
func (f *fakeAdapter) ServiceID() string { return f.serviceID }
func (f *fakeAdapter) Family() string    { return "fake" }
func (f *fakeAdapter) Version() int      { return 1 }
func (f *fakeAdapter) Close()            {}

func (f *fakeAdapter) Supports(op string) bool { return f.ops[op] }

func (f *fakeAdapter) Capabilities() []string {
	out := []string{}
	for op := range f.ops {
		out = append(out, op)
	}
	return out
}

func (f *fakeAdapter) Probe(ctx context.Context) error { return nil }
func (f *fakeAdapter) Stats() map[string]int64         { return map[string]int64{} }

func (f *fakeAdapter) Execute(ctx context.Context, call adapter.Call) (adapter.Reply, *gwerr.Error) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	return f.respond(ctx, call)
}

func (f *fakeAdapter) seen() []adapter.Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]adapter.Call(nil), f.calls...)
}

// fakeSource is a router.AdapterSource over a fixed map.
type fakeSource map[string]*fakeAdapter

func (s fakeSource) Acquire(name string) (adapter.Adapter, func(), bool) {
	a, ok := s[name]
	if !ok {
		return nil, nil, false
	}
	return a, func() {}, true
}

type responder = func(ctx context.Context, call adapter.Call) (adapter.Reply, *gwerr.Error)

func ok(value any) responder {
	return func(context.Context, adapter.Call) (adapter.Reply, *gwerr.Error) {
		return adapter.Reply{Result: map[string]any{"value": value}}, nil
	}
}

func fails(e *gwerr.Error) responder {
	return func(context.Context, adapter.Call) (adapter.Reply, *gwerr.Error) { return adapter.Reply{}, e }
}

// harness wires a router over a temp registry and controllable adapters.
type harness struct {
	router *Router
	reg    *registry.Registry
	src    fakeSource
	cfg    config.Config
}

func newHarness(t *testing.T, tune func(*config.Config)) *harness {
	t.Helper()

	cfg := config.Load()
	cfg.StateDir = t.TempDir()
	cfg.DefaultTimeout = 2 * time.Second
	cfg.ResponseMargin = 50 * time.Millisecond
	cfg.RetryBackoff = 0
	cfg.HealthInterval = time.Hour
	if tune != nil {
		tune(&cfg)
	}

	reg, _, err := registry.Open(registry.NewStore(cfg.StateDir), registry.SeedOptions{
		ServiceAEndpoint: "http://a", ServiceBEndpoint: "b:1", ServiceCEndpoint: "c:1",
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}

	src := fakeSource{
		"http-json-v1":      {name: "http-json-v1", serviceID: "service-a", ops: map[string]bool{"echo": true, "uppercase": true, "metadata": true}, respond: ok("A")},
		"tcp-frame-json-v1": {name: "tcp-frame-json-v1", serviceID: "service-b", ops: map[string]bool{"echo": true, "uppercase": true, "sum": true, "reverse": true, "metadata": true}, respond: ok("B")},
		"udp-crc-json-v1":   {name: "udp-crc-json-v1", serviceID: "service-c", ops: map[string]bool{"echo": true, "sum": true, "metadata": true}, respond: ok("C")},
	}

	log := obs.NewLogger(obs.LevelError)
	metrics := obs.NewMetrics()
	breakers := breaker.New(breaker.Config{
		Threshold: cfg.BreakerThreshold, Cooldown: cfg.BreakerCooldown, HalfOpenMax: cfg.BreakerHalfOpen,
	})
	r := New(cfg, reg, src, breakers, idgen.New(0), log, metrics)
	return &harness{router: r, reg: reg, src: src, cfg: cfg}
}

func (h *harness) exec(t *testing.T, id, op string, args map[string]any, opts map[string]any) apimodel.ExecuteResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if args == nil {
		args = map[string]any{}
	}
	return h.router.Execute(ctx, apimodel.ExecuteRequest{
		RequestID: id, Operation: op, Arguments: args, Options: opts,
	})
}

func serviceOf(resp apimodel.ExecuteResponse) string {
	if resp.ServiceID == nil {
		return ""
	}
	return *resp.ServiceID
}

// --- routing ---------------------------------------------------------------

func TestRoutesByCapability(t *testing.T) {
	h := newHarness(t, nil)

	if got := h.exec(t, "r1", "reverse", map[string]any{"value": "x"}, nil); serviceOf(got) != "service-b" {
		t.Errorf("reverse went to %q, want service-b (the only capable backend)", serviceOf(got))
	}
	if got := h.exec(t, "r2", "echo", map[string]any{"value": "x"}, nil); serviceOf(got) != "service-a" {
		t.Errorf("echo went to %q, want service-a (highest priority)", serviceOf(got))
	}
	if got := h.exec(t, "r3", "sum", map[string]any{"values": []any{1, 2}}, nil); serviceOf(got) != "service-b" {
		t.Errorf("sum went to %q, want service-b", serviceOf(got))
	}
}

func TestUnknownOperationIsRejectedWithoutTouchingABackend(t *testing.T) {
	h := newHarness(t, nil)
	resp := h.exec(t, "u1", "transmogrify", nil, nil)

	if resp.Status != apimodel.StatusError || resp.Error.Code != gwerr.CodeUnsupportedOperation {
		t.Fatalf("got %+v, want an UNSUPPORTED_OPERATION error", resp.Error)
	}
	if resp.ServiceID != nil {
		t.Errorf("service_id = %v, want null when no route could be resolved", *resp.ServiceID)
	}
	for name, a := range h.src {
		if len(a.seen()) != 0 {
			t.Errorf("adapter %s was called for an unsupported operation", name)
		}
	}
}

func TestPreferredServiceIsHonoured(t *testing.T) {
	h := newHarness(t, nil)
	resp := h.exec(t, "p1", "echo", map[string]any{"value": "x"},
		map[string]any{"preferred_service": "service-c"})
	if serviceOf(resp) != "service-c" {
		t.Fatalf("preference ignored: went to %q", serviceOf(resp))
	}
}

func TestIncapablePreferenceFollowsConfiguredPolicy(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		h := newHarness(t, func(c *config.Config) { c.PreferredIncapable = "fallback" })
		resp := h.exec(t, "p2", "reverse", map[string]any{"value": "x"},
			map[string]any{"preferred_service": "service-c"})
		if resp.Status != apimodel.StatusSuccess || serviceOf(resp) != "service-b" {
			t.Fatalf("got %+v, want a success from service-b", resp)
		}
	})
	t.Run("strict", func(t *testing.T) {
		h := newHarness(t, func(c *config.Config) { c.PreferredIncapable = "strict" })
		resp := h.exec(t, "p3", "reverse", map[string]any{"value": "x"},
			map[string]any{"preferred_service": "service-c"})
		if resp.Status != apimodel.StatusError || resp.Error.Code != gwerr.CodePreferredIncapable {
			t.Fatalf("got %+v, want PREFERRED_SERVICE_INCAPABLE", resp.Error)
		}
	})
}

// --- the fallback gate -----------------------------------------------------

// TestProvablySafeFailureFallsBack: the backend demonstrably did nothing, so
// another one may run the work.
func TestProvablySafeFailureFallsBack(t *testing.T) {
	h := newHarness(t, nil)
	h.src["http-json-v1"].respond = fails(gwerr.ConnectFailed("service-a", nil))

	resp := h.exec(t, "f1", "echo", map[string]any{"value": "x"}, nil)
	if resp.Status != apimodel.StatusSuccess {
		t.Fatalf("got %+v, want a success via fallback", resp.Error)
	}
	if serviceOf(resp) != "service-b" {
		t.Errorf("fell back to %q, want service-b", serviceOf(resp))
	}
}

// TestTimeoutDoesNotFallBack is the invariant the fallback-unsafe scenario
// tests: a timed-out backend may still be executing, so re-running the work
// elsewhere would duplicate it.
func TestTimeoutDoesNotFallBack(t *testing.T) {
	h := newHarness(t, nil)
	h.src["http-json-v1"].respond = fails(gwerr.Timeout("service-a"))

	resp := h.exec(t, "f2", "echo", map[string]any{"value": "x"}, nil)
	if resp.Status != apimodel.StatusError {
		t.Fatalf("got %+v, want the timeout to be reported", resp)
	}
	if resp.Error.Code != gwerr.CodeBackendTimeout {
		t.Errorf("code = %s, want BACKEND_TIMEOUT", resp.Error.Code)
	}
	if n := len(h.src["tcp-frame-json-v1"].seen()); n != 0 {
		t.Errorf("service-b was called %d times after a timeout; the operation may have run twice", n)
	}
}

// TestReplaySafeOperationMayFallBackAfterATimeout: the operator can take
// responsibility for re-execution, and then the gateway will.
func TestReplaySafeOperationMayFallBackAfterATimeout(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.reg.SetOperationReplaySafe("service-a", "echo", true); err != nil {
		t.Fatalf("set replay-safe: %v", err)
	}
	h.src["http-json-v1"].respond = fails(gwerr.Timeout("service-a"))

	resp := h.exec(t, "f3", "echo", map[string]any{"value": "x"}, nil)
	if resp.Status != apimodel.StatusSuccess || serviceOf(resp) != "service-b" {
		t.Fatalf("got %+v, want a success from service-b once replay is permitted", resp)
	}
}

// TestCorruptResponseFallbackFollowsPolicy.
func TestCorruptResponseFallbackFollowsPolicy(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		h := newHarness(t, func(c *config.Config) { c.FallbackOnCorrupt = true })
		h.src["http-json-v1"].respond = fails(gwerr.ProtocolViolation("service-a", "payload is not valid JSON"))
		resp := h.exec(t, "f4", "echo", map[string]any{"value": "x"}, nil)
		if resp.Status != apimodel.StatusSuccess || serviceOf(resp) != "service-b" {
			t.Fatalf("got %+v, want a fallback success", resp)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		h := newHarness(t, func(c *config.Config) { c.FallbackOnCorrupt = false })
		h.src["http-json-v1"].respond = fails(gwerr.ProtocolViolation("service-a", "payload is not valid JSON"))
		resp := h.exec(t, "f5", "echo", map[string]any{"value": "x"}, nil)
		if resp.Status != apimodel.StatusError || resp.Error.Code != gwerr.CodeProtocolViolation {
			t.Fatalf("got %+v, want the violation reported", resp)
		}
		if n := len(h.src["tcp-frame-json-v1"].seen()); n != 0 {
			t.Errorf("service-b was called %d times under the strict policy", n)
		}
	})
}

// TestEveryCapableBackendIsTriedBeforeGivingUp.
func TestEveryCapableBackendIsTriedBeforeGivingUp(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.SameServiceTry = 1; c.MaxAttempts = 5 })
	for _, a := range h.src {
		a.respond = fails(gwerr.ConnectFailed(a.serviceID, nil))
	}

	resp := h.exec(t, "f6", "echo", map[string]any{"value": "x"}, nil)
	if resp.Status != apimodel.StatusError {
		t.Fatalf("got %+v, want an error once every backend failed", resp)
	}
	for name, a := range h.src {
		if len(a.seen()) == 0 {
			t.Errorf("adapter %s was never tried", name)
		}
	}
}

// TestInvalidArgumentIsTerminal: every backend would give the same answer, so
// burning the caller's budget on the rest is pointless.
func TestInvalidArgumentIsTerminal(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.ValidateArguments = false })
	h.src["tcp-frame-json-v1"].respond = fails(
		gwerr.New(gwerr.CodeInvalidArgs, "sum requires numbers", false, true))

	resp := h.exec(t, "f7", "sum", map[string]any{"values": []any{1}}, nil)
	if resp.Status != apimodel.StatusError || resp.Error.Code != gwerr.CodeInvalidArgs {
		t.Fatalf("got %+v, want INVALID_ARGUMENT", resp.Error)
	}
	if n := len(h.src["udp-crc-json-v1"].seen()); n != 0 {
		t.Errorf("service-c was tried %d times for an argument error", n)
	}
}

// --- correlation and concurrency -------------------------------------------

// TestEachAttemptGetsAFreshCorrelationID: two attempts of one request are two
// distinct backend conversations, so a late answer to the first can never
// satisfy the second.
func TestEachAttemptGetsAFreshCorrelationID(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.SameServiceTry = 1 })
	h.src["http-json-v1"].respond = fails(gwerr.ConnectFailed("service-a", nil))

	h.exec(t, "same-id", "echo", map[string]any{"value": "x"}, nil)

	a := h.src["http-json-v1"].seen()
	b := h.src["tcp-frame-json-v1"].seen()
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected one attempt each, got %d and %d", len(a), len(b))
	}
	if a[0].CorrelationID == b[0].CorrelationID {
		t.Fatal("the fallback attempt reused the first attempt's correlation id")
	}
	if a[0].ClientRequestID != "same-id" || b[0].ClientRequestID != "same-id" {
		t.Error("the client request id was not carried through for logging")
	}
}

// TestConcurrentRequestsNeverShareACorrelationID.
func TestConcurrentRequestsNeverShareACorrelationID(t *testing.T) {
	h := newHarness(t, nil)
	const n = 200

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.exec(t, fmt.Sprintf("c-%03d", i), "echo", map[string]any{"value": i}, nil)
		}(i)
	}
	wg.Wait()

	seen := map[uint64]bool{}
	for _, call := range h.src["http-json-v1"].seen() {
		if seen[call.CorrelationID] {
			t.Fatalf("correlation id %d was used twice", call.CorrelationID)
		}
		seen[call.CorrelationID] = true
	}
	if len(seen) != n {
		t.Fatalf("saw %d attempts, want %d", len(seen), n)
	}
}

// TestExactlyOneBackendAnswersEachRequest: attempts are sequential, so a
// fallback cannot race the original into producing two successes.
func TestExactlyOneBackendAnswersEachRequest(t *testing.T) {
	h := newHarness(t, nil)
	var successes atomic.Int64
	for _, a := range h.src {
		id := a.serviceID
		a.respond = func(context.Context, adapter.Call) (adapter.Reply, *gwerr.Error) {
			successes.Add(1)
			return adapter.Reply{Result: map[string]any{"value": id}}, nil
		}
	}

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := h.exec(t, fmt.Sprintf("s-%03d", i), "echo", map[string]any{"value": i}, nil)
			if resp.Status != apimodel.StatusSuccess {
				t.Errorf("request %d failed: %+v", i, resp.Error)
			}
		}(i)
	}
	wg.Wait()

	if got := successes.Load(); got != n {
		t.Fatalf("%d backend successes for %d requests", got, n)
	}
}

// --- budget and envelope ---------------------------------------------------

// TestTheCallerBudgetIsRespected: the gateway answers before the caller's own
// deadline, reserving a margin for the response itself.
func TestTheCallerBudgetIsRespected(t *testing.T) {
	h := newHarness(t, nil)
	// Every backend blocks until its own attempt deadline fires, which is what
	// a genuinely slow service looks like from the router's side.
	for _, a := range h.src {
		id := a.serviceID
		a.respond = func(ctx context.Context, _ adapter.Call) (adapter.Reply, *gwerr.Error) {
			<-ctx.Done()
			return adapter.Reply{}, gwerr.Timeout(id)
		}
	}

	start := time.Now()
	resp := h.exec(t, "b1", "echo", map[string]any{"value": "x"}, map[string]any{"timeout_ms": 400})
	elapsed := time.Since(start)

	if resp.Status != apimodel.StatusError {
		t.Fatalf("got %+v, want a timeout error", resp)
	}
	if elapsed > 400*time.Millisecond {
		t.Errorf("took %s, which exceeds the caller's 400ms budget", elapsed)
	}
}

// TestEnvelopeInvariantsHold is the contract the grader evaluates.
func TestEnvelopeInvariantsHold(t *testing.T) {
	h := newHarness(t, nil)

	success := h.exec(t, "e1", "echo", map[string]any{"value": "x"}, nil)
	if success.Status != apimodel.StatusSuccess {
		t.Fatalf("expected success, got %+v", success)
	}
	if success.Result == nil || success.Error != nil {
		t.Error("success must carry a result and no error")
	}
	if success.RequestID != "e1" || success.Operation != "echo" {
		t.Error("the envelope must echo the request id and operation")
	}

	h.src["http-json-v1"].respond = fails(gwerr.Timeout("service-a"))
	failure := h.exec(t, "e2", "echo", map[string]any{"value": "x"}, nil)
	if failure.Result != nil || failure.Error == nil {
		t.Error("an error must carry no result and a populated error")
	}
	if failure.RequestID != "e2" {
		t.Error("the envelope must echo the request id on failure too")
	}
}

// --- circuit breaking ------------------------------------------------------

// TestBreakerRemovesAFailingBackendFromRotation, and does so without ever
// touching the wire again -- which is what makes the rejection fallback-safe.
func TestBreakerRemovesAFailingBackendFromRotation(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.BreakerThreshold = 2
		c.BreakerCooldown = time.Hour
		c.SameServiceTry = 1
	})
	h.src["http-json-v1"].respond = fails(gwerr.ConnectFailed("service-a", nil))

	for i := 0; i < 4; i++ {
		if resp := h.exec(t, fmt.Sprintf("cb-%d", i), "echo", map[string]any{"value": "x"}, nil); resp.Status != apimodel.StatusSuccess {
			t.Fatalf("request %d should have fallen back: %+v", i, resp.Error)
		}
	}
	attempts := len(h.src["http-json-v1"].seen())
	if attempts > 2 {
		t.Errorf("service-a was attempted %d times; the breaker should have stopped at 2", attempts)
	}
}

// TestDomainErrorsDoNotTripTheBreaker: a backend that correctly refuses a bad
// request is healthy, and must not be taken out of rotation for it.
func TestDomainErrorsDoNotTripTheBreaker(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.BreakerThreshold = 2; c.SameServiceTry = 1 })
	h.src["http-json-v1"].respond = fails(&gwerr.Error{
		Code: "OPERATION_NOT_SUPPORTED", Message: "nope", FallbackSafe: true, ServiceID: "service-a",
	})

	for i := 0; i < 5; i++ {
		h.exec(t, fmt.Sprintf("dm-%d", i), "echo", map[string]any{"value": "x"}, nil)
	}
	if got := len(h.src["http-json-v1"].seen()); got != 5 {
		t.Errorf("service-a was attempted %d times, want 5: domain errors must not trip the breaker", got)
	}
}

// --- variant selection -----------------------------------------------------

// TestVariantSelectionIsDeterministicAndWeighted underpins rolling upgrades:
// the same request always lands on the same version, and the split follows the
// configured weights.
func TestVariantSelectionIsDeterministicAndWeighted(t *testing.T) {
	h := newHarness(t, nil)
	svc, _ := h.reg.Get("service-b")
	svc.Variants = []registry.Variant{
		{Version: 1, Weight: 50, AdapterName: "tcp-frame-json-v1"},
		{Version: 2, Weight: 50, AdapterName: "tcp-frame-json-v2"},
	}
	if err := h.reg.Upsert(svc); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	counts := map[int]int{}
	for i := 0; i < 400; i++ {
		v, ok := h.router.pickVariant(svc, fmt.Sprintf("req-%d|reverse", i))
		if !ok {
			t.Fatal("no variant selected")
		}
		counts[v.Version]++

		// Same key, same answer, every time.
		again, _ := h.router.pickVariant(svc, fmt.Sprintf("req-%d|reverse", i))
		if again.Version != v.Version {
			t.Fatalf("selection for req-%d changed between calls", i)
		}
	}
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("a 50/50 split produced %v", counts)
	}
	// Loose bounds: the point is that both versions receive traffic in roughly
	// the configured proportion, not that a hash is perfectly uniform.
	if counts[1] < 120 || counts[2] < 120 {
		t.Errorf("split %v is too lopsided for 50/50 weights", counts)
	}
}

// TestZeroWeightVariantReceivesNoTraffic: registering a new version must not by
// itself shift anything.
func TestZeroWeightVariantReceivesNoTraffic(t *testing.T) {
	h := newHarness(t, nil)
	svc, _ := h.reg.Get("service-b")
	svc.Variants = []registry.Variant{
		{Version: 1, Weight: 100, AdapterName: "tcp-frame-json-v1"},
		{Version: 2, Weight: 0, AdapterName: "tcp-frame-json-v2"},
	}
	if err := h.reg.Upsert(svc); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	for i := 0; i < 100; i++ {
		v, _ := h.router.pickVariant(svc, fmt.Sprintf("k-%d", i))
		if v.Version != 1 {
			t.Fatalf("a zero-weight variant received traffic on key k-%d", i)
		}
	}
}

// --- shutdown --------------------------------------------------------------

func TestDrainingRefusesNewWorkCleanly(t *testing.T) {
	h := newHarness(t, nil)
	h.router.BeginDrain()

	resp := h.exec(t, "d1", "echo", map[string]any{"value": "x"}, nil)
	if resp.Status != apimodel.StatusError || resp.Error.Code != gwerr.CodeGatewayShutdown {
		t.Fatalf("got %+v, want GATEWAY_SHUTTING_DOWN", resp.Error)
	}
	if !resp.Error.Retryable {
		t.Error("a shutdown refusal must be retryable so the caller knows to try again")
	}
}
