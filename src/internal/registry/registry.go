// Package registry owns the gateway's backend registry: which services exist,
// what protocol they speak, where they live, which operations they may serve,
// which protocol variants are deployed, and how healthy they currently are.
//
// Two properties are load-bearing:
//
//   - Everything structural is persisted. A restart restores the same routing
//     tables, capability metadata and adapter bindings, so operator changes made
//     at runtime are not lost.
//   - Memory and disk never diverge. A mutation is applied to a copy, validated
//     in full, written to disk, and only then published. If the write fails the
//     live configuration is untouched, so the gateway can never end up running a
//     configuration it would not be able to restore.
//
// Health is deliberately *not* in that second guarantee: it is volatile runtime
// observation, refreshed every probe interval, and persisting it on every probe
// would turn an observability signal into disk traffic. The last observed value
// is folded into the snapshot opportunistically so a freshly restarted gateway
// can report something better than "unknown" before its first probe lands.
package registry

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Health is the volatile view of one backend.
type Health struct {
	Status      string
	Detail      string
	LastChecked time.Time
}

// SeedOptions carries the endpoints used to build the first-boot registry.
type SeedOptions struct {
	ServiceAEndpoint string
	ServiceBEndpoint string
	ServiceCEndpoint string
}

// Source describes where the live registry came from, for observability.
const (
	SourceRestored   = "restored"
	SourceSeeded     = "seeded"
	SourceQuarantine = "seeded-after-quarantine"
)

// Registry is the concurrency-safe live registry.
type Registry struct {
	mu     sync.RWMutex
	snap   Snapshot
	health map[string]Health

	store  *Store
	source string

	// dirty tracks volatile fields (health, id high-water) that are worth
	// flushing eventually but not worth a synchronous write each time.
	dirty bool
}

// Open loads the registry from disk, seeding defaults when there is nothing to
// restore. A corrupt document is quarantined rather than overwritten, and the
// returned warning explains what happened so /status can surface it.
func Open(store *Store, seed SeedOptions) (*Registry, string, error) {
	r := &Registry{store: store, health: map[string]Health{}}

	snap, found, err := store.Load()
	switch {
	case err != nil:
		moved, qerr := store.Quarantine()
		warn := fmt.Sprintf("registry unusable (%v); ", err)
		if qerr != nil {
			warn += fmt.Sprintf("quarantine failed (%v); ", qerr)
		} else {
			warn += fmt.Sprintf("moved aside to %s; ", moved)
		}
		warn += "continuing with seeded defaults"
		r.snap = DefaultSnapshot(seed)
		r.source = SourceQuarantine
		if serr := store.Save(r.snap); serr != nil {
			return nil, warn, fmt.Errorf("persist seeded registry: %w", serr)
		}
		return r.finish(), warn, nil

	case !found:
		r.snap = DefaultSnapshot(seed)
		r.source = SourceSeeded
		if serr := store.Save(r.snap); serr != nil {
			return nil, "", fmt.Errorf("persist seeded registry: %w", serr)
		}
		return r.finish(), "", nil

	default:
		r.snap = snap
		r.source = SourceRestored
		return r.finish(), "", nil
	}
}

// finish seeds the volatile health map from the restored last-known values.
func (r *Registry) finish() *Registry {
	for id, svc := range r.snap.Services {
		status := svc.LastKnownHealth
		if status == "" {
			status = "unknown"
		}
		if !svc.Enabled {
			status = "disabled"
		}
		r.health[id] = Health{Status: status, Detail: "not probed since restart"}
	}
	return r
}

// Source reports where the live registry came from.
func (r *Registry) Source() string { return r.source }

// Revision is the monotonically increasing mutation counter.
func (r *Registry) Revision() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snap.Revision
}

// Get returns a deep copy of one service.
func (r *Registry) Get(id string) (Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.snap.Services[id]
	if !ok {
		return Service{}, false
	}
	return svc.Clone(), true
}

// List returns every service, ordered by service_id so that /services output is
// stable across calls.
func (r *Registry) List() []Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Service, 0, len(r.snap.Services))
	for _, svc := range r.snap.Services {
		out = append(out, svc.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

// Candidates returns the enabled services declaring the given operation,
// ordered deterministically: per-operation priority first, then service_id.
//
// The router layers health and breaker state on top; ordering here is purely a
// function of persisted metadata so the same registry always yields the same
// preference order, which is what makes fallback reproducible.
func (r *Registry) Candidates(operation string) []Service {
	r.mu.RLock()
	defer r.mu.RUnlock()

	type scored struct {
		svc      Service
		priority int
	}
	var list []scored
	for _, svc := range r.snap.Services {
		if !svc.Enabled {
			continue
		}
		op, ok := svc.Operation(operation)
		if !ok {
			continue
		}
		if len(svc.ActiveVariants()) == 0 {
			continue
		}
		list = append(list, scored{svc: svc.Clone(), priority: op.Priority})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].priority != list[j].priority {
			return list[i].priority < list[j].priority
		}
		return list[i].svc.ServiceID < list[j].svc.ServiceID
	})

	out := make([]Service, 0, len(list))
	for _, s := range list {
		out = append(out, s.svc)
	}
	return out
}

// KnownOperations is the union of every declared capability, used to tell
// "this operation exists but nothing healthy can serve it" apart from "this
// operation does not exist at all".
func (r *Registry) KnownOperations() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]bool{}
	for _, svc := range r.snap.Services {
		for _, op := range svc.Operations {
			seen[strings.ToLower(op.Name)] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SetHealth records a probe result. Volatile: never writes to disk directly.
func (r *Registry) SetHealth(id, status, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health[id] = Health{Status: status, Detail: detail, LastChecked: time.Now()}
	if svc, ok := r.snap.Services[id]; ok && svc.LastKnownHealth != status {
		svc.LastKnownHealth = status
		r.snap.Services[id] = svc
		r.dirty = true
	}
}

// HealthOf returns the last observed health of a service.
func (r *Registry) HealthOf(id string) Health {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.health[id]
	if !ok {
		return Health{Status: "unknown"}
	}
	return h
}

// AllHealth snapshots the health map.
func (r *Registry) AllHealth() map[string]Health {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Health, len(r.health))
	for k, v := range r.health {
		out[k] = v
	}
	return out
}

// AdapterSpecs returns the persisted runtime adapter definitions, as the exact
// bytes they were stored with.
func (r *Registry) AdapterSpecs() map[string]json.RawMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(r.snap.AdapterSpecs))
	for k, v := range r.snap.AdapterSpecs {
		out[k] = append(json.RawMessage(nil), v...)
	}
	return out
}

// NoteIDHighWater records the correlation-id allocator's progress. Volatile;
// flushed by Flush. The allocator floors itself by wall-clock time, so a lost
// update costs nothing unless the clock also goes backwards.
func (r *Registry) NoteIDHighWater(v uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v > r.snap.IDHighWater {
		r.snap.IDHighWater = v
		r.dirty = true
	}
}

// IDHighWater is the persisted allocator mark restored at boot.
func (r *Registry) IDHighWater() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snap.IDHighWater
}

// Flush writes volatile-but-worth-keeping state if anything changed. Called on
// a slow ticker and once during shutdown.
func (r *Registry) Flush() error {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	snap := r.cloneLocked()
	r.dirty = false
	r.mu.Unlock()
	return r.store.Save(snap)
}

// SnapshotView returns a deep copy of the persisted document.
func (r *Registry) SnapshotView() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cloneLocked()
}

func (r *Registry) cloneLocked() Snapshot {
	out := r.snap
	out.Services = make(map[string]Service, len(r.snap.Services))
	for k, v := range r.snap.Services {
		out.Services[k] = v.Clone()
	}
	out.AdapterSpecs = make(map[string]json.RawMessage, len(r.snap.AdapterSpecs))
	for k, v := range r.snap.AdapterSpecs {
		out.AdapterSpecs[k] = append(json.RawMessage(nil), v...)
	}
	return out
}

// mutate is the single write path.
//
// Copy -> apply -> validate everything -> persist -> publish. Validation runs
// over the whole document, not just the touched entry, so one bad edit can
// never leave a globally inconsistent registry, and publication happens only
// after the write succeeded.
func (r *Registry) mutate(apply func(*Snapshot) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := r.cloneLocked()
	if err := apply(&next); err != nil {
		return err
	}
	for id, svc := range next.Services {
		if svc.ServiceID != id {
			return fmt.Errorf("registry key %q does not match service_id %q", id, svc.ServiceID)
		}
		if err := svc.Validate(); err != nil {
			return err
		}
	}
	next.Revision = r.snap.Revision + 1
	if err := r.store.Save(next); err != nil {
		return fmt.Errorf("registry mutation rejected, live configuration unchanged: %w", err)
	}
	r.snap = next
	r.dirty = false
	return nil
}

// Upsert installs or replaces a service definition.
func (r *Registry) Upsert(svc Service) error {
	if err := svc.Validate(); err != nil {
		return err
	}
	return r.mutate(func(s *Snapshot) error {
		s.Services[svc.ServiceID] = svc.Clone()
		return nil
	})
}

// Remove deletes a service.
func (r *Registry) Remove(id string) error {
	return r.mutate(func(s *Snapshot) error {
		if _, ok := s.Services[id]; !ok {
			return fmt.Errorf("service %q is not registered", id)
		}
		delete(s.Services, id)
		return nil
	})
}

// SetEnabled turns a backend on or off for routing without forgetting it.
func (r *Registry) SetEnabled(id string, enabled bool) error {
	return r.mutate(func(s *Snapshot) error {
		svc, ok := s.Services[id]
		if !ok {
			return fmt.Errorf("service %q is not registered", id)
		}
		svc.Enabled = enabled
		s.Services[id] = svc
		return nil
	})
}

// AddVariant registers an additional protocol variant, defaulting to zero
// weight so that registering a new version never by itself shifts traffic.
func (r *Registry) AddVariant(id string, v Variant) error {
	return r.mutate(func(s *Snapshot) error {
		svc, ok := s.Services[id]
		if !ok {
			return fmt.Errorf("service %q is not registered", id)
		}
		for _, existing := range svc.Variants {
			if existing.Version == v.Version {
				return fmt.Errorf("service %q already has variant version %d", id, v.Version)
			}
		}
		svc.Variants = append(svc.Variants, v)
		s.Services[id] = svc
		return nil
	})
}

// SetVariantWeights rewrites the traffic split across protocol versions.
//
// This is the whole of "runtime protocol migration" and "simultaneous version
// compatibility": a rolling upgrade is {1:50, 2:50}, a cutover is {1:0, 2:100},
// a rollback is the inverse. Because it goes through the ordinary mutation
// path, the new split is durable immediately and a rejected split leaves the
// previous one running.
func (r *Registry) SetVariantWeights(id string, weights map[int]int) error {
	return r.mutate(func(s *Snapshot) error {
		svc, ok := s.Services[id]
		if !ok {
			return fmt.Errorf("service %q is not registered", id)
		}
		known := map[int]bool{}
		for _, v := range svc.Variants {
			known[v.Version] = true
		}
		for version := range weights {
			if !known[version] {
				return fmt.Errorf("service %q has no variant version %d", id, version)
			}
		}
		for i := range svc.Variants {
			if w, ok := weights[svc.Variants[i].Version]; ok {
				if w < 0 {
					return fmt.Errorf("weight for version %d must not be negative", svc.Variants[i].Version)
				}
				svc.Variants[i].Weight = w
			}
		}
		s.Services[id] = svc
		return nil
	})
}

// SetOperationReplaySafe flips the ambiguous-failure fallback gate for one
// capability at runtime. Operators need this because whether re-executing an
// operation is acceptable is a property of the deployment, not of the gateway.
func (r *Registry) SetOperationReplaySafe(id, operation string, replaySafe bool) error {
	return r.mutate(func(s *Snapshot) error {
		svc, ok := s.Services[id]
		if !ok {
			return fmt.Errorf("service %q is not registered", id)
		}
		for i := range svc.Operations {
			if strings.EqualFold(svc.Operations[i].Name, operation) {
				svc.Operations[i].ReplaySafe = replaySafe
				s.Services[id] = svc
				return nil
			}
		}
		return fmt.Errorf("service %q does not declare operation %q", id, operation)
	})
}

// PutAdapterSpec persists a runtime-loaded adapter definition so it survives a
// restart. The adapter manager validates the spec before this is ever called.
//
// The spec is serialized once, here, and the resulting bytes are what get
// written and read back. See Snapshot.AdapterSpecs for why the value is never
// allowed to pass through a generic decode.
func (r *Registry) PutAdapterSpec(name string, spec any) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("adapter name must not be empty")
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode adapter spec %q: %w", name, err)
	}
	return r.mutate(func(s *Snapshot) error {
		if s.AdapterSpecs == nil {
			s.AdapterSpecs = map[string]json.RawMessage{}
		}
		s.AdapterSpecs[name] = raw
		return nil
	})
}

// DeleteAdapterSpec removes a runtime adapter definition, refusing while any
// service still binds it -- dropping it would leave an unroutable variant.
func (r *Registry) DeleteAdapterSpec(name string) error {
	return r.mutate(func(s *Snapshot) error {
		for _, svc := range s.Services {
			for _, v := range svc.Variants {
				if v.AdapterName == name {
					return fmt.Errorf("adapter %q is still bound by service %q variant %d", name, svc.ServiceID, v.Version)
				}
			}
		}
		delete(s.AdapterSpecs, name)
		return nil
	})
}

// DefaultSnapshot builds the first-boot registry for the reference deployment.
//
// Priorities encode a preference for the more reliable transport when several
// backends can serve the same operation: HTTP over framed TCP over datagram
// UDP. They are data, so an operator can reorder routing without a rebuild.
//
// ReplaySafe is false everywhere by default. The operations are pure, but the
// environment's execution ledger makes a second execution externally visible,
// and the specification forbids fallback that creates duplicate operations. See
// Operation.ReplaySafe for the full argument.
func DefaultSnapshot(opts SeedOptions) Snapshot {
	a := Service{
		ServiceID: "service-a",
		Protocol:  ProtocolHTTPJSON,
		Endpoint:  opts.ServiceAEndpoint,
		Enabled:   true,
		Operations: []Operation{
			{Name: "echo", Priority: 10},
			{Name: "uppercase", Priority: 10},
			{Name: "metadata", Priority: 10},
		},
		Variants: []Variant{
			{Version: 1, Weight: 100, AdapterName: "http-json-v1"},
		},
	}
	b := Service{
		ServiceID: "service-b",
		Protocol:  ProtocolTCPFrameJSON,
		Endpoint:  opts.ServiceBEndpoint,
		Enabled:   true,
		Operations: []Operation{
			{Name: "echo", Priority: 20},
			{Name: "uppercase", Priority: 20},
			{Name: "sum", Priority: 10},
			{Name: "reverse", Priority: 10},
			{Name: "metadata", Priority: 20},
		},
		Variants: []Variant{
			{Version: 1, Weight: 100, AdapterName: "tcp-frame-json-v1"},
		},
	}
	c := Service{
		ServiceID: "service-c",
		Protocol:  ProtocolUDPCRCJSON,
		Endpoint:  opts.ServiceCEndpoint,
		Enabled:   true,
		Operations: []Operation{
			{Name: "echo", Priority: 30},
			{Name: "sum", Priority: 20},
			{Name: "metadata", Priority: 30},
		},
		Variants: []Variant{
			{Version: 1, Weight: 100, AdapterName: "udp-crc-json-v1"},
		},
	}

	return Snapshot{
		SchemaVersion: CurrentSchemaVersion,
		Revision:      1,
		Services: map[string]Service{
			a.ServiceID: a,
			b.ServiceID: b,
			c.ServiceID: c,
		},
		AdapterSpecs: map[string]json.RawMessage{},
	}
}
