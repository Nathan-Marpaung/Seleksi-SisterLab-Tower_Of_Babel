package adapter

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// ErrProbeUnsupported means an adapter has no way to check liveness without
// executing real work. Health monitoring treats it as "no opinion" rather than
// as a failure, so an unprobeable backend is never marked down on the strength
// of a probe that was never possible.
var ErrProbeUnsupported = errors.New("adapter: no non-intrusive probe available")

// IntrusiveProber is implemented by adapters whose only way to check liveness
// is to send a real request.
//
// A datagram transport has no handshake to observe, so the only evidence
// available is an answered operation -- which the backend records in its
// execution ledger exactly like client traffic. Declaring that here lets the
// health monitor probe such a backend sparingly in steady state and escalate
// only when something looks wrong, instead of manufacturing a steady trickle of
// operations nobody asked for.
type IntrusiveProber interface {
	ProbeIsIntrusive() bool
}

// IsIntrusive reports whether probing an adapter costs a real backend
// operation.
func IsIntrusive(a Adapter) bool {
	p, ok := a.(IntrusiveProber)
	return ok && p.ProbeIsIntrusive()
}

// BuildOptions carries deployment policy into adapter construction. The spec
// owns protocol shape; these own resource limits and timing.
type BuildOptions struct {
	// Endpoint is the address from the registry. A spec-level endpoint wins,
	// which is how a side-by-side version rollout on a second port is done.
	Endpoint string
	TCP      TCPOptions
	UDP      UDPOptions

	// SelfTestTimeout bounds the golden-vector run so a pathological spec
	// cannot hang the load.
	SelfTestTimeout time.Duration
}

type probeIDKey struct{}

// WithProbeID supplies a correlation identifier for adapters whose liveness
// probe has to send a real datagram.
func WithProbeID(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, probeIDKey{}, id)
}

func probeCorrelation(ctx context.Context) uint64 {
	if v, ok := ctx.Value(probeIDKey{}).(uint64); ok {
		return v
	}
	return 0
}

// Build validates a spec, constructs the adapter and proves it with its own
// golden vectors before returning it.
//
// Nothing here is allowed to escape as a panic. A hot-loaded spec is untrusted
// input, and the whole point of the loading path is that a broken adapter fails
// its own load rather than taking the gateway down -- so construction and the
// self-test run under recovery and a deadline, and any failure returns an error
// with the partially built adapter already closed.
func Build(spec *Spec, opt BuildOptions) (ad Adapter, err error) {
	if spec == nil {
		return nil, errors.New("adapter spec is nil")
	}
	spec = spec.Clone()
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	endpoint := spec.Endpoint
	if endpoint == "" {
		endpoint = opt.Endpoint
	}
	if endpoint == "" {
		return nil, fmt.Errorf("adapter %s: no endpoint configured", spec.Name)
	}

	timeout := opt.SelfTestTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	type outcome struct {
		ad  Adapter
		err error
	}
	done := make(chan outcome, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- outcome{err: fmt.Errorf("adapter %s panicked during load: %v\n%s",
					spec.Name, r, debug.Stack())}
			}
		}()

		var built Adapter
		var berr error
		switch spec.Family {
		case FamilyHTTPJSON:
			built, berr = newHTTPAdapter(spec, endpoint)
		case FamilyTCPFrameJSON:
			built, berr = newTCPAdapter(spec, endpoint, opt.TCP)
		case FamilyUDPCRCJSON:
			built, berr = newUDPAdapter(spec, endpoint, opt.UDP)
		default:
			berr = fmt.Errorf("adapter %s: unsupported family %q", spec.Name, spec.Family)
		}
		if berr != nil {
			done <- outcome{err: berr}
			return
		}

		// The self-test is not optional. An adapter that cannot reproduce
		// recorded backend bytes is wrong, and installing it would corrupt
		// live traffic in a way no amount of downstream validation recovers.
		if codec, ok := built.(Codec); ok {
			if terr := RunSelfTest(spec, codec); terr != nil {
				built.Close()
				done <- outcome{err: fmt.Errorf("adapter %s failed its self-test: %w", spec.Name, terr)}
				return
			}
		} else if len(spec.SelfTest) > 0 {
			built.Close()
			done <- outcome{err: fmt.Errorf("adapter %s declares self-test vectors but exposes no codec", spec.Name)}
			return
		}

		done <- outcome{ad: built}
	}()

	select {
	case res := <-done:
		return res.ad, res.err
	case <-time.After(timeout):
		// The goroutine may still finish later; it closes whatever it built
		// only on its own error paths, so drain it in the background to avoid
		// leaking a live transport.
		go func() {
			if res := <-done; res.ad != nil {
				res.ad.Close()
			}
		}()
		return nil, fmt.Errorf("adapter %s: load exceeded %s", spec.Name, timeout)
	}
}
