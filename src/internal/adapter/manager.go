package adapter

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Manager owns the live adapter instances and performs hot swaps.
//
// The lifecycle rule it enforces is simple to state and is what makes runtime
// adapter replacement and protocol migration safe:
//
//	A request that has acquired an adapter keeps that exact instance until it
//	finishes. A swap installs a new instance for future acquisitions and
//	retires the old one, closing it only after the last in-flight request has
//	released it or a drain deadline expires.
//
// Consequently an upgrade never tears a request in half, a rollback never
// resurrects a closed transport, and a failed load never touches what is
// already serving: the new adapter is fully built and self-tested *before* the
// pointer moves, so a rejected spec leaves the previous adapter in place with
// no observable interruption.
type Manager struct {
	mu     sync.RWMutex
	leases map[string]*lease
	closed bool

	// DrainTimeout bounds how long a retired adapter waits for its in-flight
	// requests before it is closed anyway. Waiting forever would let one stuck
	// request pin a transport for the life of the process.
	DrainTimeout time.Duration

	// OnEvent reports lifecycle transitions for observability.
	OnEvent func(event string, fields map[string]any)
}

type lease struct {
	mu      sync.Mutex
	ad      Adapter
	spec    *Spec
	retired bool
	refs    int
	// loadedAt and generation make a hot swap visible in /status.
	loadedAt   time.Time
	generation int
	drained    chan struct{}
}

// NewManager creates an empty manager.
func NewManager(drainTimeout time.Duration) *Manager {
	if drainTimeout <= 0 {
		drainTimeout = 5 * time.Second
	}
	return &Manager{leases: map[string]*lease{}, DrainTimeout: drainTimeout}
}

func (m *Manager) event(name string, fields map[string]any) {
	if m.OnEvent != nil {
		m.OnEvent(name, fields)
	}
}

// Load builds and installs an adapter under spec.Name.
//
// On any failure the previously installed adapter is left untouched and serving
// -- that is the rollback guarantee. The error describes precisely which gate
// rejected the spec so an operator can fix it.
func (m *Manager) Load(spec *Spec, opt BuildOptions) error {
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return fmt.Errorf("adapter manager is shut down")
	}

	built, err := Build(spec, opt)
	if err != nil {
		m.event("adapter.load_rejected", map[string]any{
			"adapter": spec.Name, "error": err.Error(),
		})
		return err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		built.Close()
		return fmt.Errorf("adapter manager is shut down")
	}
	old := m.leases[spec.Name]
	gen := 1
	if old != nil {
		gen = old.generation + 1
	}
	m.leases[spec.Name] = &lease{
		ad: built, spec: spec.Clone(),
		loadedAt: time.Now(), generation: gen, drained: make(chan struct{}),
	}
	m.mu.Unlock()

	m.event("adapter.loaded", map[string]any{
		"adapter": spec.Name, "service_id": spec.ServiceID,
		"family": spec.Family, "version": spec.Version, "generation": gen,
		"replaced": old != nil,
	})

	if old != nil {
		go m.retire(spec.Name, old)
	}
	return nil
}

// retire drains and closes a superseded adapter without blocking the swap.
func (m *Manager) retire(name string, l *lease) {
	l.mu.Lock()
	l.retired = true
	idle := l.refs == 0
	l.mu.Unlock()

	if !idle {
		select {
		case <-l.drained:
		case <-time.After(m.DrainTimeout):
			m.event("adapter.drain_timeout", map[string]any{
				"adapter": name, "generation": l.generation, "timeout": m.DrainTimeout.String(),
			})
		}
	}
	l.ad.Close()
	m.event("adapter.retired", map[string]any{"adapter": name, "generation": l.generation})
}

// Acquire takes a reference to a live adapter. The returned release function
// must be called exactly once, or a retired adapter will never be closed.
func (m *Manager) Acquire(name string) (Adapter, func(), bool) {
	m.mu.RLock()
	l, ok := m.leases[name]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, false
	}

	l.mu.Lock()
	if l.retired {
		// A swap happened between lookup and acquisition. Refusing here rather
		// than handing out a draining instance keeps the drain bounded.
		l.mu.Unlock()
		return nil, nil, false
	}
	l.refs++
	ad := l.ad
	l.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			l.mu.Lock()
			l.refs--
			done := l.retired && l.refs == 0
			l.mu.Unlock()
			if done {
				select {
				case <-l.drained:
				default:
					close(l.drained)
				}
			}
		})
	}
	return ad, release, true
}

// Peek returns the live adapter without taking a reference. For read-only
// inspection such as /services rendering only.
func (m *Manager) Peek(name string) (Adapter, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	l, ok := m.leases[name]
	if !ok {
		return nil, false
	}
	return l.ad, true
}

// Remove retires an adapter entirely.
func (m *Manager) Remove(name string) bool {
	m.mu.Lock()
	l, ok := m.leases[name]
	if ok {
		delete(m.leases, name)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	go m.retire(name, l)
	return true
}

// Names lists the loaded adapter names, sorted.
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.leases))
	for name := range m.leases {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Info is the observability view of one loaded adapter.
type Info struct {
	Name         string   `json:"name"`
	ServiceID    string   `json:"service_id"`
	Family       string   `json:"family"`
	Version      int      `json:"version"`
	Capabilities []string `json:"capabilities"`
	Generation   int      `json:"generation"`
	LoadedAt     string   `json:"loaded_at"`
	InFlight     int      `json:"in_flight"`
	SelfTests    int      `json:"self_tests"`
}

// Describe snapshots every loaded adapter.
func (m *Manager) Describe() []Info {
	m.mu.RLock()
	leases := make(map[string]*lease, len(m.leases))
	for k, v := range m.leases {
		leases[k] = v
	}
	m.mu.RUnlock()

	out := make([]Info, 0, len(leases))
	for name, l := range leases {
		l.mu.Lock()
		refs := l.refs
		l.mu.Unlock()
		out = append(out, Info{
			Name:         name,
			ServiceID:    l.ad.ServiceID(),
			Family:       l.ad.Family(),
			Version:      l.ad.Version(),
			Capabilities: l.ad.Capabilities(),
			Generation:   l.generation,
			LoadedAt:     l.loadedAt.UTC().Format(time.RFC3339Nano),
			InFlight:     refs,
			SelfTests:    len(l.spec.SelfTest),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Spec returns a copy of the spec an adapter was loaded from.
func (m *Manager) Spec(name string) (*Spec, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	l, ok := m.leases[name]
	if !ok {
		return nil, false
	}
	return l.spec.Clone(), true
}

// Stats aggregates transport counters per adapter.
func (m *Manager) Stats() map[string]map[string]int64 {
	m.mu.RLock()
	leases := make(map[string]*lease, len(m.leases))
	for k, v := range m.leases {
		leases[k] = v
	}
	m.mu.RUnlock()

	out := make(map[string]map[string]int64, len(leases))
	for name, l := range leases {
		out[name] = l.ad.Stats()
	}
	return out
}

// Close retires every adapter. Safe to call once at shutdown.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	leases := m.leases
	m.leases = map[string]*lease{}
	m.mu.Unlock()

	var wg sync.WaitGroup
	for name, l := range leases {
		wg.Add(1)
		go func(n string, le *lease) {
			defer wg.Done()
			m.retire(n, le)
		}(name, l)
	}
	wg.Wait()
}
