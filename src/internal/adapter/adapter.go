// Package adapter is the protocol translation layer.
//
// An adapter is the only thing in the gateway that knows a backend's dialect.
// Above it everything speaks one canonical vocabulary -- a logical operation
// name, a plain argument object, a gateway-allocated correlation id -- and
// below it lives HTTP-JSON, length-prefixed frames, or CRC-checked datagrams.
//
// Two design choices are worth stating up front.
//
// First, adapters are *described* rather than hand-written. A Spec captures the
// endpoint, wire parameters, per-operation mapping and validation rules of one
// protocol version, and three family implementations interpret those specs. The
// built-in adapters are specs the gateway ships with; a runtime-loaded adapter
// is a spec that arrived over the admin API. There is exactly one code path, so
// a hot-loaded adapter cannot behave differently from a built-in one, and a
// protocol version bump is usually a data change.
//
// Second, an adapter instance is immutable once built. Reconfiguration produces
// a new instance and swaps the pointer; a request that already began holds its
// instance until it finishes. That is what makes runtime migration safe: no
// in-flight request ever observes half of an old adapter and half of a new one.
package adapter

import (
	"context"
	"time"

	"babel/gateway/internal/gwerr"
)

// Call is a backend-neutral request.
type Call struct {
	// ClientRequestID is the caller's identifier. It is used for logging and is
	// never sent as a backend correlation id -- clients may reuse or collide on
	// theirs, and backend correlation must be collision-free.
	ClientRequestID string

	Operation string
	Arguments map[string]any

	// CorrelationID is the gateway-allocated, globally unique backend
	// identifier for this attempt.
	CorrelationID uint64
	// Seq is the datagram sequence number for families that carry one.
	Seq uint32
}

// Reply is a validated, normalized backend response.
type Reply struct {
	ServiceID string
	// Result is already in Gateway API shape: either {"value": ...} or a
	// pass-through metadata object. No backend-specific key survives here.
	Result map[string]any
	// Version is the protocol version the response was decoded under.
	Version int
	// Attempts records transport-level attempts, for observability.
	Attempts int
}

// Adapter translates for exactly one (service, protocol version) pair.
type Adapter interface {
	// Name is the adapter identity referenced by registry variants.
	Name() string
	ServiceID() string
	Family() string
	// Version is the backend protocol version this adapter speaks.
	Version() int
	// Supports reports whether the adapter can translate a logical operation.
	Supports(operation string) bool
	// Capabilities lists supported logical operations, sorted.
	Capabilities() []string

	// Execute performs one attempt. It must respect ctx, must never panic, and
	// must return either a validated Reply or a classified *gwerr.Error.
	Execute(ctx context.Context, call Call) (Reply, *gwerr.Error)

	// Probe is a cheap liveness check used by health monitoring. It must not
	// execute a business operation.
	Probe(ctx context.Context) error

	// Close releases transport resources. It must be safe to call twice, and
	// safe to call while requests are still draining.
	Close()

	// Stats exposes transport counters for observability.
	Stats() map[string]int64
}

// Codec is the pure, network-free half of an adapter: the part that turns a
// Call into bytes and bytes back into a Reply.
//
// Separating it out is what makes the self-test possible. Golden vectors run
// against the codec alone, so an adapter can be proven to encode and decode
// correctly before it is ever allowed to touch a live backend.
type Codec interface {
	EncodeRequest(call Call) ([]byte, *gwerr.Error)
	DecodeResponse(raw []byte, call Call) (Reply, *gwerr.Error)
}

// Health probe outcome shared by adapters that have no dedicated endpoint.
const probeGrace = 50 * time.Millisecond

// deadlineFor derives a per-attempt deadline, leaving a small grace so the
// adapter can produce a classified error rather than being cut off mid-write.
func deadlineFor(ctx context.Context) (time.Time, bool) {
	dl, ok := ctx.Deadline()
	if !ok {
		return time.Time{}, false
	}
	if time.Until(dl) > probeGrace {
		return dl.Add(-probeGrace / 2), true
	}
	return dl, true
}

// errUnsupported is the shared "this adapter cannot do that" error.
func errUnsupported(serviceID, operation string) *gwerr.Error {
	return gwerr.Newf(gwerr.CodeUnsupportedOperation, false, true,
		"%s does not support operation %q", serviceID, operation).WithService(serviceID)
}
