// Package breaker implements per-backend circuit breaking.
//
// Its job in this gateway is failure isolation. Without it, a backend that has
// gone away still costs every request its full attempt slice before the router
// gives up, so one dead backend degrades latency for operations that a healthy
// backend could have served immediately. The breaker converts "slow failure" to
// "immediate, fallback-safe refusal", which is the only way the routing layer
// can move on quickly without ever having touched the wire.
//
// The refusal is fallback-safe by construction: a breaker rejection happens
// before any bytes are sent, so the backend provably did not execute anything.
package breaker

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Key is the canonical breaker identity: one breaker per (service, protocol
// version). Defined here so every caller derives it the same way -- two
// spellings of the same key would silently create two independent breakers.
func Key(serviceID string, version int) string {
	return fmt.Sprintf("%s#v%d", serviceID, version)
}

// State of one breaker.
type State int

const (
	// Closed: traffic flows normally.
	StateClosed State = iota
	// Open: traffic is refused until the cooldown expires.
	StateOpen
	// HalfOpen: a limited number of probe requests are admitted to discover
	// whether the backend recovered.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// Config is the breaker policy.
type Config struct {
	// Threshold is the number of consecutive failures that opens the breaker.
	Threshold int
	// Cooldown is how long the breaker stays open before probing again.
	Cooldown time.Duration
	// HalfOpenMax is how many concurrent probe requests are admitted while
	// half-open. One is usually right: more probes against a backend that is
	// still down just re-create the stampede the breaker exists to prevent.
	HalfOpenMax int
}

type entry struct {
	state       State
	failures    int
	successes   int
	openedAt    time.Time
	halfOpenNow int

	// Observability.
	trips      int64
	rejections int64
	lastChange time.Time
	lastReason string
}

// Set is a collection of breakers keyed by backend identity.
//
// The key is (service_id, protocol version), not just service_id: during a
// rolling upgrade, v1 may be perfectly healthy while v2 is broken, and tripping
// them together would either hide the bad version or punish the good one.
type Set struct {
	mu   sync.Mutex
	cfg  Config
	byID map[string]*entry
	now  func() time.Time
}

// New creates a breaker set.
func New(cfg Config) *Set {
	if cfg.Threshold < 1 {
		cfg.Threshold = 1
	}
	if cfg.HalfOpenMax < 1 {
		cfg.HalfOpenMax = 1
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = time.Second
	}
	return &Set{cfg: cfg, byID: map[string]*entry{}, now: time.Now}
}

func (s *Set) get(key string) *entry {
	e, ok := s.byID[key]
	if !ok {
		e = &entry{state: StateClosed, lastChange: s.now()}
		s.byID[key] = e
	}
	return e
}

// Allow reports whether a call may proceed. When it returns true the caller
// holds a slot and MUST report the outcome through Success or Failure exactly
// once, otherwise a half-open breaker would leak its probe budget and never
// close again.
func (s *Set) Allow(key string) (bool, State) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.get(key)
	switch e.state {
	case StateClosed:
		return true, StateClosed

	case StateOpen:
		if s.now().Sub(e.openedAt) < s.cfg.Cooldown {
			e.rejections++
			return false, StateOpen
		}
		e.state = StateHalfOpen
		e.halfOpenNow = 1
		e.successes = 0
		e.lastChange = s.now()
		e.lastReason = "cooldown elapsed, probing"
		return true, StateHalfOpen

	default: // half-open
		if e.halfOpenNow >= s.cfg.HalfOpenMax {
			e.rejections++
			return false, StateHalfOpen
		}
		e.halfOpenNow++
		return true, StateHalfOpen
	}
}

// Success reports a healthy outcome.
func (s *Set) Success(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.get(key)
	e.failures = 0
	if e.state == StateHalfOpen {
		if e.halfOpenNow > 0 {
			e.halfOpenNow--
		}
		e.state = StateClosed
		e.lastChange = s.now()
		e.lastReason = "probe succeeded"
	}
}

// Failure reports an outcome that counts against the backend.
//
// Only transport- and protocol-level failures should be reported here. A
// backend that correctly answers "you gave me a bad argument" is working
// perfectly; counting domain errors would trip the breaker on client mistakes
// and take a healthy backend out of rotation.
func (s *Set) Failure(key, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.get(key)
	if e.state == StateHalfOpen {
		if e.halfOpenNow > 0 {
			e.halfOpenNow--
		}
		e.state = StateOpen
		e.openedAt = s.now()
		e.trips++
		e.lastChange = s.now()
		e.lastReason = "probe failed: " + reason
		return
	}
	e.failures++
	if e.state == StateClosed && e.failures >= s.cfg.Threshold {
		e.state = StateOpen
		e.openedAt = s.now()
		e.trips++
		e.lastChange = s.now()
		e.lastReason = reason
	}
}

// Trip forces a breaker open, used when health probing independently concludes
// a backend is down so the first real request does not have to discover it.
func (s *Set) Trip(key, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.get(key)
	if e.state != StateOpen {
		e.state = StateOpen
		e.trips++
		e.lastChange = s.now()
	}
	e.openedAt = s.now()
	e.lastReason = reason
}

// Reset closes a breaker, used when health probing observes recovery.
func (s *Set) Reset(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.get(key)
	if e.state != StateClosed {
		e.state = StateClosed
		e.lastChange = s.now()
		e.lastReason = "health probe recovered"
	}
	e.failures = 0
	e.halfOpenNow = 0
}

// ResetAll closes every breaker whose key has the given prefix, or all of them
// when the prefix is empty.
//
// This exists for the operator who has just fixed a backend and does not want
// to wait out a cooldown to confirm it. It is a manual override of an automatic
// safety mechanism, so it is deliberately explicit rather than something the
// gateway ever does to itself.
func (s *Set) ResetAll(keyPrefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key, e := range s.byID {
		if keyPrefix != "" && !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		if e.state != StateClosed {
			e.state = StateClosed
			e.lastChange = s.now()
			e.lastReason = "reset by operator"
		}
		e.failures = 0
		e.halfOpenNow = 0
		n++
	}
	return n
}

// Forget drops a breaker entirely, for when a service leaves the registry.
func (s *Set) Forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byID, key)
}

// State reports the current state without side effects.
func (s *Set) State(key string) State {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.get(key)
	if e.state == StateOpen && s.now().Sub(e.openedAt) >= s.cfg.Cooldown {
		// Report the state a caller would actually get, rather than a stale
		// "open" that makes /status look worse than reality.
		return StateHalfOpen
	}
	return e.state
}

// Stat is the observability view of one breaker.
type Stat struct {
	Key        string `json:"key"`
	State      string `json:"state"`
	Failures   int    `json:"consecutive_failures"`
	Trips      int64  `json:"trips"`
	Rejections int64  `json:"rejections"`
	LastChange string `json:"last_change,omitempty"`
	LastReason string `json:"last_reason,omitempty"`
}

// Stats snapshots every breaker.
func (s *Set) Stats() map[string]Stat {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Stat, len(s.byID))
	for k, e := range s.byID {
		st := Stat{
			Key: k, State: e.state.String(), Failures: e.failures,
			Trips: e.trips, Rejections: e.rejections, LastReason: e.lastReason,
		}
		if !e.lastChange.IsZero() {
			st.LastChange = e.lastChange.UTC().Format(time.RFC3339Nano)
		}
		out[k] = st
	}
	return out
}
