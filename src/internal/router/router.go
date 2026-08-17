// Package router turns a validated client request into at most one backend
// success.
//
// The invariants it exists to hold, stated plainly because everything below is
// in service of them:
//
//  1. Exactly one envelope per client request. Attempts are strictly
//     sequential, so two backends can never both answer the same call.
//  2. Every backend attempt carries a globally unique correlation identifier.
//     Nothing is ever correlated by arrival order.
//  3. Routing is deterministic. The same registry, the same request and the
//     same health state always produce the same candidate order, so a fallback
//     can be reasoned about and reproduced rather than guessed at.
//  4. Fallback never duplicates work. Moving to another backend requires proof
//     that the failed attempt produced no backend-visible effect -- or an
//     explicit operator declaration that re-execution is acceptable.
//  5. The caller's timeout is a hard budget. The gateway reserves a response
//     margin and never spends longer than the caller allowed, whatever the
//     backends do.
package router

import (
	"context"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
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

// AdapterSource hands out live adapter instances by name.
//
// The router depends on this narrow interface rather than on the adapter
// manager directly. That is what lets the routing rules -- especially the
// fallback gate, whose whole job is to decide correctly about failures that are
// awkward to produce on a real network -- be tested against adapters that fail
// exactly on demand.
type AdapterSource interface {
	// Acquire takes a reference to a loaded adapter. The release function must
	// be called exactly once when the attempt finishes.
	Acquire(name string) (adapter.Adapter, func(), bool)
}

// Router executes client requests against backends.
type Router struct {
	cfg      config.Config
	reg      *registry.Registry
	adapters AdapterSource
	breakers *breaker.Set
	ids      *idgen.Generator
	log      *obs.Logger
	metrics  *obs.Metrics

	// global bounds total concurrent client requests. It is admission control:
	// beyond this the gateway refuses fast instead of degrading for everyone.
	global chan struct{}
	// perBackend bounds concurrent attempts against a single backend, so one
	// slow service cannot consume the whole gateway's capacity.
	perBackendMu sync.Mutex
	perBackend   map[string]chan struct{}

	inFlight atomic.Int64
	draining atomic.Bool
}

// New builds a router.
func New(cfg config.Config, reg *registry.Registry, adapters AdapterSource,
	breakers *breaker.Set, ids *idgen.Generator, log *obs.Logger, metrics *obs.Metrics) *Router {
	return &Router{
		cfg: cfg, reg: reg, adapters: adapters, breakers: breakers,
		ids: ids, log: log, metrics: metrics,
		global:     make(chan struct{}, max(1, cfg.MaxInFlight)),
		perBackend: map[string]chan struct{}{},
	}
}

// InFlight reports current concurrent requests, for /status.
func (r *Router) InFlight() int { return int(r.inFlight.Load()) }

// Draining reports whether shutdown has begun.
func (r *Router) Draining() bool { return r.draining.Load() }

// BeginDrain stops admitting new work. In-flight requests are left to finish.
func (r *Router) BeginDrain() { r.draining.Store(true) }

// attempt records one backend try, for logging and for the final error.
type attempt struct {
	serviceID string
	version   int
	adapter   string
	reply     adapter.Reply
	err       *gwerr.Error
	elapsed   time.Duration
}

// Execute runs one client request to completion.
func (r *Router) Execute(ctx context.Context, req apimodel.ExecuteRequest) apimodel.ExecuteResponse {
	operation := strings.ToLower(strings.TrimSpace(req.Operation))
	opts := apimodel.ParseOptions(req.Options)
	log := r.log.With(map[string]any{"request_id": req.RequestID, "operation": operation})

	r.metrics.Inc("requests_total")

	if r.draining.Load() {
		r.metrics.Inc("requests_rejected_draining")
		return apimodel.Failure(req.RequestID, req.Operation, "",
			gwerr.New(gwerr.CodeGatewayShutdown, "Gateway is shutting down and is not accepting new work.", true, true))
	}

	// Argument validation happens before routing so that a malformed call gets
	// the same answer no matter which backend would have served it, and never
	// consumes a backend attempt or a slice of the caller's budget.
	if r.cfg.ValidateArguments {
		if e := apimodel.ValidateArguments(operation, req.Arguments); e != nil {
			r.metrics.Inc("requests_invalid_arguments")
			return apimodel.Failure(req.RequestID, req.Operation, "", e)
		}
	}

	// Establish the budget. Everything downstream slices from this one
	// deadline, so the gateway's answer always beats the caller's own timeout.
	budget := r.cfg.ClampTimeout(time.Duration(opts.TimeoutMS) * time.Millisecond)
	effective := budget - r.cfg.ResponseMargin
	if effective < r.cfg.MinTimeout {
		effective = r.cfg.MinTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, effective)
	defer cancel()

	// Admission control. A full gateway refuses immediately rather than
	// queueing work it cannot finish inside the caller's budget.
	select {
	case r.global <- struct{}{}:
		defer func() { <-r.global }()
	case <-ctx.Done():
		r.metrics.Inc("requests_rejected_overloaded")
		return apimodel.Failure(req.RequestID, req.Operation, "",
			gwerr.New(gwerr.CodeGatewayOverloaded, "Gateway is at its concurrency limit.", true, true))
	}

	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)

	candidates, routeErr := r.candidates(operation, opts.PreferredService, log)
	if routeErr != nil {
		r.metrics.Inc("requests_unrouted")
		return apimodel.Failure(req.RequestID, req.Operation, "", routeErr)
	}

	resp, attempts := r.dispatch(ctx, req, operation, candidates, log)
	r.logOutcome(log, resp, attempts)
	return resp
}

// candidates resolves and orders the backends that may serve an operation.
func (r *Router) candidates(operation, preferred string, log *obs.Logger) ([]registry.Service, *gwerr.Error) {
	capable := r.reg.Candidates(operation)

	if len(capable) == 0 {
		known := r.reg.KnownOperations()
		for _, k := range known {
			if k == operation {
				// The operation exists in the registry but nothing enabled can
				// currently serve it -- a configuration state, not a client
				// mistake, so it is reported as retryable.
				return nil, gwerr.Newf(gwerr.CodeNoRoute, true, true,
					"No enabled backend is currently able to serve %q.", operation)
			}
		}
		return nil, gwerr.Newf(gwerr.CodeUnsupportedOperation, false, true,
			"Operation %q is not supported. Known operations: %s.", operation, strings.Join(known, ", "))
	}

	preferred = strings.TrimSpace(preferred)
	if preferred == "" {
		return capable, nil
	}

	for i, svc := range capable {
		if svc.ServiceID == preferred {
			// Pin the preference to the front, keeping the deterministic order
			// of the rest as the fallback sequence.
			ordered := make([]registry.Service, 0, len(capable))
			ordered = append(ordered, svc)
			ordered = append(ordered, capable[:i]...)
			ordered = append(ordered, capable[i+1:]...)
			return ordered, nil
		}
	}

	// The preference names a backend that cannot serve this operation (or does
	// not exist). The Gateway API contract permits either refusing or picking
	// a safe alternative, provided the choice is documented. The default is to
	// serve the caller: the request is well-formed and a capable backend
	// exists, so failing it would be a worse answer than honouring the intent
	// of the call. BABEL_PREFERRED_INCAPABLE=strict selects the other reading.
	if r.cfg.PreferredIncapable == "strict" {
		return nil, gwerr.Newf(gwerr.CodePreferredIncapable, false, true,
			"Preferred service %q cannot serve operation %q.", preferred, operation)
	}
	log.Warn("preferred service cannot serve this operation, routing by capability instead",
		map[string]any{"preferred_service": preferred, "chosen": capable[0].ServiceID})
	r.metrics.Inc("preferred_service_ignored")
	return capable, nil
}

// dispatch walks the candidate list and performs attempts until one succeeds,
// the budget runs out, or a failure that cannot be safely retried is reached.
func (r *Router) dispatch(ctx context.Context, req apimodel.ExecuteRequest, operation string,
	candidates []registry.Service, log *obs.Logger) (apimodel.ExecuteResponse, []attempt) {

	var attempts []attempt
	var last *gwerr.Error
	lastService := ""

	for _, svc := range candidates {
		if len(attempts) >= r.cfg.MaxAttempts {
			break
		}
		opMeta, _ := svc.Operation(operation)

		for try := 0; try < r.cfg.SameServiceTry && len(attempts) < r.cfg.MaxAttempts; try++ {
			if err := ctx.Err(); err != nil {
				return r.timeoutResponse(req, lastService, last), attempts
			}
			if try > 0 {
				// Deterministic backoff: derived from the attempt index, never
				// randomized, so a failure sequence replays identically.
				if !r.sleep(ctx, r.backoff(try)) {
					return r.timeoutResponse(req, lastService, last), attempts
				}
			}

			at := r.attemptOne(ctx, req, operation, svc)
			attempts = append(attempts, at)
			if at.err == nil {
				return apimodel.Success(req.RequestID, req.Operation, svc.ServiceID, at.reply.Result), attempts
			}
			last, lastService = at.err, svc.ServiceID

			if isTerminal(at.err) {
				// The answer would be the same everywhere; trying again only
				// wastes the caller's budget.
				return apimodel.Failure(req.RequestID, req.Operation, svc.ServiceID, at.err), attempts
			}
			if !r.mayRetryElsewhere(at.err, opMeta) {
				// Ambiguous failure: the backend may already have executed the
				// operation, so re-issuing it anywhere would risk a duplicate.
				// Reporting the failure is the correct answer.
				r.metrics.Inc("fallback_suppressed")
				log.Warn("not falling back after an ambiguous failure", map[string]any{
					"service_id": svc.ServiceID, "code": at.err.Code,
					"reason": "attempt outcome is unknown and the operation is not declared replay-safe",
				})
				return apimodel.Failure(req.RequestID, req.Operation, svc.ServiceID, at.err), attempts
			}
			if !at.err.Retryable {
				break // move to the next backend rather than retrying this one
			}
		}
	}

	if last == nil {
		last = gwerr.New(gwerr.CodeNoRoute, "No backend attempt could be made.", true, true)
	}
	if len(attempts) > 1 {
		r.metrics.Inc("fallback_exhausted")
	}
	return apimodel.Failure(req.RequestID, req.Operation, lastService, last), attempts
}

// attemptOne performs a single backend call under its own deadline slice.
func (r *Router) attemptOne(ctx context.Context, req apimodel.ExecuteRequest,
	operation string, svc registry.Service) attempt {

	variant, ok := r.pickVariant(svc, req.RequestID+"|"+operation)
	if !ok {
		return attempt{serviceID: svc.ServiceID, err: gwerr.Newf(gwerr.CodeNoRoute, true, true,
			"Service %s has no active protocol variant.", svc.ServiceID)}
	}
	at := attempt{serviceID: svc.ServiceID, version: variant.Version, adapter: variant.AdapterName}

	// Breaker state is per (service, version): during a rolling upgrade a
	// broken v2 must not take the healthy v1 out of rotation.
	key := breaker.Key(svc.ServiceID, variant.Version)
	allowed, state := r.breakers.Allow(key)
	if !allowed {
		r.metrics.Inc("breaker_rejections")
		// Rejected before any byte was sent, so this is provably safe to
		// route elsewhere -- which is the whole point of failing fast here.
		at.err = gwerr.Newf(gwerr.CodeServiceUnavailable, true, true,
			"Service %s is not accepting traffic (circuit %s).", svc.ServiceID, state).
			WithService(svc.ServiceID)
		return at
	}

	ad, release, ok := r.adapters.Acquire(variant.AdapterName)
	if !ok {
		r.breakers.Failure(key, "adapter unavailable")
		at.err = gwerr.Newf(gwerr.CodeAdapterRejected, true, true,
			"Adapter %s is not loaded.", variant.AdapterName).WithService(svc.ServiceID)
		return at
	}
	// The adapter instance is held for the whole attempt. A hot swap during
	// this window installs a new instance for later requests and drains this
	// one; the in-flight call never changes protocol underneath itself.
	defer release()

	if !ad.Supports(operation) {
		at.err = gwerr.Newf(gwerr.CodeUnsupportedOperation, false, true,
			"Adapter %s cannot translate operation %q.", ad.Name(), operation).WithService(svc.ServiceID)
		r.breakers.Success(key) // the backend is fine; the mapping is not
		return at
	}

	slot, releaseSlot, ok := r.acquireBackendSlot(ctx, svc.ServiceID)
	if !ok {
		at.err = gwerr.Newf(gwerr.CodeServiceUnavailable, true, true,
			"Service %s is at its per-backend concurrency limit.", svc.ServiceID).WithService(svc.ServiceID)
		return at
	}
	_ = slot
	defer releaseSlot()

	remaining := time.Until(deadlineOf(ctx))
	if remaining <= 0 {
		at.err = gwerr.New(gwerr.CodeGatewayTimeout,
			"Timeout budget was exhausted before the attempt could start.", true, true).
			WithService(svc.ServiceID)
		return at
	}
	slice := remaining
	if r.cfg.MaxAttemptSlice > 0 && slice > r.cfg.MaxAttemptSlice {
		slice = r.cfg.MaxAttemptSlice
	}
	attemptCtx, cancel := context.WithTimeout(ctx, slice)
	defer cancel()

	call := adapter.Call{
		ClientRequestID: req.RequestID,
		Operation:       operation,
		Arguments:       req.Arguments,
		// A fresh identifier per attempt, never the client's. Two attempts of
		// the same request are two distinct backend conversations, so a late
		// response to the first can never satisfy the second.
		CorrelationID: r.ids.Next(),
	}

	start := time.Now()
	reply, e := ad.Execute(attemptCtx, call)
	at.elapsed = time.Since(start)
	at.reply = reply

	if e == nil {
		r.breakers.Success(key)
		r.metrics.Inc("attempts_success")
		r.metrics.Inc("attempts_success:" + svc.ServiceID)
		return at
	}

	at.err = e
	r.metrics.Inc("attempts_failed")
	r.metrics.Inc("attempt_error:" + e.Code)
	if countsAgainstBackend(e) {
		r.breakers.Failure(key, e.Code)
	} else {
		// A backend that correctly refuses a bad argument is working
		// perfectly. Counting domain errors would trip the breaker on client
		// mistakes and remove a healthy backend from rotation.
		r.breakers.Success(key)
	}
	return at
}

// acquireBackendSlot enforces the per-backend concurrency limit.
func (r *Router) acquireBackendSlot(ctx context.Context, serviceID string) (struct{}, func(), bool) {
	r.perBackendMu.Lock()
	sem, ok := r.perBackend[serviceID]
	if !ok {
		sem = make(chan struct{}, max(1, r.cfg.PerBackendLimit))
		r.perBackend[serviceID] = sem
	}
	r.perBackendMu.Unlock()

	select {
	case sem <- struct{}{}:
		return struct{}{}, func() { <-sem }, true
	case <-ctx.Done():
		return struct{}{}, func() {}, false
	}
}

// pickVariant chooses a protocol version deterministically.
//
// Selection is a hash of the client request id and operation, not a counter or
// a random draw. That makes a weighted rollout reproducible: the same request
// always lands on the same version, so a canary can be reasoned about and a
// retry of the same call does not silently change protocol mid-incident.
func (r *Router) pickVariant(svc registry.Service, key string) (registry.Variant, bool) {
	active := svc.ActiveVariants()
	switch len(active) {
	case 0:
		return registry.Variant{}, false
	case 1:
		return active[0], true
	}
	total := 0
	for _, v := range active {
		total += v.Weight
	}
	if total <= 0 {
		return active[0], true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	pick := int(h.Sum32() % uint32(total))
	for _, v := range active {
		pick -= v.Weight
		if pick < 0 {
			return v, true
		}
	}
	return active[len(active)-1], true
}

// mayRetryElsewhere is the fallback gate. Three independent grounds permit
// moving on to another backend.
//
// First, the failure is provably effect-free. A refused connection, a rejection
// by the circuit breaker, a backend's own structured error, a payload the
// transport declined to send: in all of these the backend did not execute the
// operation, so another one running it cannot duplicate anything.
//
// Second, the response was corrupt. The backend has finished with the request
// but produced something unusable, so the caller gets nothing unless the
// gateway tries elsewhere. See config.FallbackOnCorrupt for why the default is
// to try, and why this is a weaker claim than the first ground.
//
// Third, the operator has declared the operation replay-safe, accepting that a
// second execution is acceptable in this deployment.
//
// Everything else -- above all a timeout, where the backend is still working on
// the request -- leaves the outcome unknown, and an unknown outcome is not a
// licence to run the work again.
func (r *Router) mayRetryElsewhere(e *gwerr.Error, opMeta registry.Operation) bool {
	if e.FallbackSafe {
		return true
	}
	if r.cfg.FallbackOnCorrupt && isCorruptResponse(e) {
		return true
	}
	return opMeta.ReplaySafe && e.Retryable
}

// isCorruptResponse marks failures where the backend answered but the answer
// could not be trusted: unparseable or incomplete payloads, a failed checksum,
// a mismatched identifier, or a protocol version the adapter cannot interpret.
// In all of these the backend has finished with the request, which is what
// separates them from a timeout.
func isCorruptResponse(e *gwerr.Error) bool {
	switch e.Code {
	case gwerr.CodeProtocolViolation, gwerr.CodeChecksumMismatch,
		gwerr.CodeCorrelationMismatch, gwerr.CodeUnsupportedVersion:
		return true
	}
	return false
}

// isTerminal marks failures that every backend would repeat, so that trying
// another one only burns the caller's budget for the same answer.
func isTerminal(e *gwerr.Error) bool {
	switch e.Code {
	case gwerr.CodeInvalidArgs, gwerr.CodeInvalidRequest:
		return true
	}
	return false
}

// countsAgainstBackend decides whether a failure is evidence of an unhealthy
// backend. Transport and protocol failures are; domain refusals are not.
func countsAgainstBackend(e *gwerr.Error) bool {
	switch e.Code {
	case gwerr.CodeBackendTimeout, gwerr.CodeBackendUnavailable, gwerr.CodeConnectionFailed,
		gwerr.CodeProtocolViolation, gwerr.CodeChecksumMismatch, gwerr.CodeCorrelationMismatch,
		gwerr.CodeUnsupportedVersion, gwerr.CodeRateLimited, gwerr.CodeAdapterRejected:
		return true
	}
	return false
}

func (r *Router) timeoutResponse(req apimodel.ExecuteRequest, serviceID string, last *gwerr.Error) apimodel.ExecuteResponse {
	r.metrics.Inc("requests_timed_out")
	if last != nil && last.Code == gwerr.CodeBackendTimeout {
		return apimodel.Failure(req.RequestID, req.Operation, serviceID, last)
	}
	return apimodel.Failure(req.RequestID, req.Operation, serviceID,
		gwerr.New(gwerr.CodeGatewayTimeout,
			"Gateway did not obtain a valid backend response within the requested timeout.", true, true))
}

func (r *Router) backoff(attemptIndex int) time.Duration {
	d := r.cfg.RetryBackoff * time.Duration(attemptIndex)
	if r.cfg.MaxRetryBackoff > 0 && d > r.cfg.MaxRetryBackoff {
		d = r.cfg.MaxRetryBackoff
	}
	return d
}

// sleep waits, reporting false if the budget ended first.
func (r *Router) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Router) logOutcome(log *obs.Logger, resp apimodel.ExecuteResponse, attempts []attempt) {
	trail := make([]map[string]any, 0, len(attempts))
	for _, a := range attempts {
		entry := map[string]any{
			"service_id": a.serviceID, "protocol_version": a.version,
			"adapter": a.adapter, "elapsed_ms": a.elapsed.Milliseconds(),
		}
		if a.err != nil {
			entry["error_code"] = a.err.Code
			entry["retryable"] = a.err.Retryable
			entry["fallback_safe"] = a.err.FallbackSafe
		}
		trail = append(trail, entry)
	}
	fields := map[string]any{"status": resp.Status, "attempts": trail}
	if resp.ServiceID != nil {
		fields["service_id"] = *resp.ServiceID
	}
	if resp.Error != nil {
		fields["error_code"] = resp.Error.Code
		log.Warn("request failed", fields)
		return
	}
	log.Info("request served", fields)
}

func deadlineOf(ctx context.Context) time.Time {
	if dl, ok := ctx.Deadline(); ok {
		return dl
	}
	return time.Now().Add(time.Hour)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
