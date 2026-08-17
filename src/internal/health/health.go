// Package health probes backends on a timer and publishes what it observes.
//
// The gateway deliberately treats probes as a *liveness* signal only. Routing
// quality comes from observed request outcomes through the circuit breaker,
// because a probe can succeed while real traffic fails -- in the reference
// environment the injected faults apply to the execute path while the health
// endpoint stays healthy, which is exactly the situation a gateway that trusted
// its probes would get wrong.
//
// So the reported status combines both sources:
//
//	probe fails                      -> unavailable
//	probe passes, breaker open       -> degraded  (observed traffic disagrees)
//	probe passes, breaker closed     -> available
//	no probe possible                -> derived from the breaker alone
package health

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"babel/gateway/internal/adapter"
	"babel/gateway/internal/apimodel"
	"babel/gateway/internal/breaker"
	"babel/gateway/internal/config"
	"babel/gateway/internal/idgen"
	"babel/gateway/internal/obs"
	"babel/gateway/internal/registry"
)

// AdapterSource hands out live adapter instances by name; see router.AdapterSource.
type AdapterSource interface {
	Acquire(name string) (adapter.Adapter, func(), bool)
}

// Monitor runs the probe loop.
type Monitor struct {
	cfg      config.Config
	reg      *registry.Registry
	adapters AdapterSource
	breakers *breaker.Set
	ids      *idgen.Generator
	log      *obs.Logger
	metrics  *obs.Metrics

	// consecutive probe failures per service, so one dropped datagram does not
	// take a backend out of rotation. Guarded because a sweep probes every
	// backend concurrently.
	mu       sync.Mutex
	failures map[string]int
	// lastIntrusive is when a probe that costs a real backend operation was
	// last sent, per service.
	lastIntrusive map[string]time.Time
}

func (m *Monitor) noteFailure(serviceID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[serviceID]++
	return m.failures[serviceID]
}

// clearFailures resets the streak and reports whether there was one.
func (m *Monitor) clearFailures(serviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	had := m.failures[serviceID] > 0
	m.failures[serviceID] = 0
	return had
}

// intrusiveDue rate-limits probes that cost a real backend operation, and
// records the decision so the interval is measured from the last probe actually
// sent.
func (m *Monitor) intrusiveDue(serviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	last, seen := m.lastIntrusive[serviceID]
	if !seen {
		// Always probe once at startup, so /services is meaningful before
		// the first client request arrives.
		m.lastIntrusive[serviceID] = time.Now()
		return true
	}
	interval := m.cfg.IntrusiveProbeInterval
	if m.failures[serviceID] > 0 {
		// Already suspect: fall back to the normal cadence so recovery is
		// noticed promptly.
		interval = m.cfg.HealthInterval
	}
	if time.Since(last) < interval {
		return false
	}
	m.lastIntrusive[serviceID] = time.Now()
	return true
}

// New builds a monitor.
func New(cfg config.Config, reg *registry.Registry, adapters AdapterSource,
	breakers *breaker.Set, ids *idgen.Generator, log *obs.Logger, metrics *obs.Metrics) *Monitor {
	return &Monitor{
		cfg: cfg, reg: reg, adapters: adapters, breakers: breakers,
		ids: ids, log: log, metrics: metrics,
		failures: map[string]int{}, lastIntrusive: map[string]time.Time{},
	}
}

// Run probes until the context is cancelled. The first sweep happens
// immediately so /services is meaningful within a moment of startup rather
// than after a full interval.
func (m *Monitor) Run(ctx context.Context) {
	m.Sweep(ctx)
	ticker := time.NewTicker(m.cfg.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Sweep(ctx)
		}
	}
}

// Sweep probes every registered service once, concurrently.
//
// Concurrency here is not an optimization -- it is isolation. Probing serially
// would let one unreachable backend delay the health refresh of every other
// service by its full probe timeout.
func (m *Monitor) Sweep(ctx context.Context) {
	services := m.reg.List()
	done := make(chan struct{}, len(services))

	for _, svc := range services {
		go func(svc registry.Service) {
			defer func() {
				// A panic inside one probe must not take down the monitor and
				// with it the health of every other backend.
				if r := recover(); r != nil {
					m.log.Error("health probe panicked", map[string]any{
						"service_id": svc.ServiceID, "panic": fmt.Sprint(r),
					})
				}
				done <- struct{}{}
			}()
			m.probe(ctx, svc)
		}(svc)
	}
	for range services {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) probe(ctx context.Context, svc registry.Service) {
	if !svc.Enabled {
		m.reg.SetHealth(svc.ServiceID, apimodel.HealthDisabled, "service is disabled in the registry")
		return
	}
	variants := svc.ActiveVariants()
	if len(variants) == 0 {
		m.reg.SetHealth(svc.ServiceID, apimodel.HealthUnavailable, "no protocol variant carries traffic")
		return
	}
	// Probe the variant that carries the most traffic; a canary at low weight
	// should not dominate the reported health of the service.
	primary := variants[0]
	for _, v := range variants {
		if v.Weight > primary.Weight {
			primary = v
		}
	}

	ad, release, ok := m.adapters.Acquire(primary.AdapterName)
	if !ok {
		m.reg.SetHealth(svc.ServiceID, apimodel.HealthUnavailable,
			"adapter "+primary.AdapterName+" is not loaded")
		return
	}
	defer release()

	// Probing a datagram backend costs a real operation, which the backend
	// records exactly like client traffic. Running that every interval would
	// mean the gateway continuously manufactures work nobody asked for, so an
	// intrusive probe runs on a slow cadence -- unless the backend already
	// looks unhealthy, in which case confirming recovery quickly matters more
	// than the cost of asking.
	if adapter.IsIntrusive(ad) && !m.intrusiveDue(svc.ServiceID) {
		return
	}

	pctx, cancel := context.WithTimeout(ctx, m.cfg.HealthTimeout)
	defer cancel()
	// Adapters whose probe has to send a real datagram need a correlation id
	// from the same allocator as production traffic, so a probe response can
	// never be mistaken for a client's.
	pctx = adapter.WithProbeID(pctx, m.ids.Next())

	err := ad.Probe(pctx)
	key := breaker.Key(svc.ServiceID, primary.Version)
	breakerState := m.breakers.State(key)

	switch {
	case errors.Is(err, adapter.ErrProbeUnsupported):
		// No non-intrusive probe exists. Say so rather than inventing a
		// verdict: the breaker is then the only evidence.
		status, detail := fromBreaker(breakerState)
		m.reg.SetHealth(svc.ServiceID, status, detail+" (no liveness probe available)")

	case err != nil:
		streak := m.noteFailure(svc.ServiceID)
		m.metrics.Inc("health_probe_failures")
		m.reg.SetHealth(svc.ServiceID, apimodel.HealthUnavailable, truncate(err.Error(), 200))
		if streak >= 2 {
			// Two consecutive failures is enough to stop sending traffic into
			// a backend that is not answering at all; one could be a blip.
			m.breakers.Trip(key, "liveness probe failed twice")
		}
		m.log.Warn("health probe failed", map[string]any{
			"service_id": svc.ServiceID, "adapter": primary.AdapterName,
			"consecutive_failures": streak, "error": err.Error(),
		})

	default:
		wasFailing := m.clearFailures(svc.ServiceID)
		m.metrics.Inc("health_probe_successes")
		if wasFailing {
			// The backend is answering again; let real traffic decide from
			// here rather than keeping it excluded for a full cooldown.
			m.breakers.Reset(key)
			breakerState = breaker.StateClosed
		}
		if breakerState == breaker.StateOpen {
			m.reg.SetHealth(svc.ServiceID, apimodel.HealthDegraded,
				"liveness probe passes but recent requests are failing")
			return
		}
		m.reg.SetHealth(svc.ServiceID, apimodel.HealthAvailable, "liveness probe passed")
	}
}

func fromBreaker(state breaker.State) (string, string) {
	switch state {
	case breaker.StateOpen:
		return apimodel.HealthUnavailable, "circuit is open after repeated failures"
	case breaker.StateHalfOpen:
		return apimodel.HealthDegraded, "circuit is half-open and probing"
	default:
		return apimodel.HealthUnknown, "no observations yet"
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
