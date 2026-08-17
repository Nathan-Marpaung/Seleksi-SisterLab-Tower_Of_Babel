package registry

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Protocol families the gateway can speak.
const (
	ProtocolHTTPJSON     = "http-json"
	ProtocolTCPFrameJSON = "tcp-frame-json"
	ProtocolUDPCRCJSON   = "udp-crc-json"
)

// Variant is one deployed protocol revision of a backend.
//
// Multiple variants of the same service may carry weight simultaneously; that
// is what makes a rolling upgrade expressible as data rather than as a special
// code path. A protocol migration is just "set weights to {old:0, new:100}".
type Variant struct {
	Version int `json:"version"`
	// Weight is the deterministic selection share. Zero means "registered and
	// loadable, but not receiving new traffic".
	Weight int `json:"weight"`
	// Endpoint overrides the service endpoint for this variant. Empty means
	// "inherit"; a non-empty value is how a genuine side-by-side rollout with
	// two listeners is described.
	Endpoint string `json:"endpoint,omitempty"`
	// AdapterName binds this variant to a loaded adapter spec.
	AdapterName string `json:"adapter_name"`
}

// Operation is the capability metadata that routing consults.
type Operation struct {
	Name string `json:"name"`
	// Priority orders candidate backends for this operation. Lower wins; ties
	// break on service_id so the order is total and reproducible.
	Priority int `json:"priority"`
	// ReplaySafe gates fallback after an *ambiguous* failure -- one where the
	// gateway cannot prove the backend did not already execute the operation,
	// such as a timeout.
	//
	// This is a stronger claim than "idempotent". All five operations in this
	// deployment are pure functions of their arguments, yet the environment
	// records every backend execution in an externally visible ledger, so a
	// second execution is observable even when it is semantically harmless --
	// and the specification forbids fallback that "creates duplicate
	// operations". The default is therefore false: after a timeout the gateway
	// reports the timeout instead of quietly running the work twice.
	//
	// Failures the gateway *can* prove were never executed (refused connection,
	// terminated connection, 503 before dispatch, breaker rejection) do not
	// consult this flag at all; those always permit fallback.
	ReplaySafe bool `json:"replay_safe"`
}

// Service is a registered backend.
type Service struct {
	ServiceID string `json:"service_id"`
	Protocol  string `json:"protocol"`
	Endpoint  string `json:"endpoint"`
	Enabled   bool   `json:"enabled"`

	// Operations is the authoritative capability metadata for routing.
	Operations []Operation `json:"operations"`

	Variants []Variant `json:"variants"`

	// LastKnownHealth is persisted purely so a restarted gateway can report
	// something better than "unknown" before the first probe completes. It is
	// never trusted for routing decisions.
	LastKnownHealth string `json:"last_known_health,omitempty"`
}

// Snapshot is the persisted document.
type Snapshot struct {
	// SchemaVersion guards against loading a document this build cannot
	// interpret. An unknown version is a hard refusal, not a silent
	// best-effort parse.
	SchemaVersion int `json:"schema_version"`
	// Revision increments on every committed mutation. Exposed through
	// /status so a demo can show that a restart resumed the same document.
	Revision int64 `json:"revision"`
	// IDHighWater persists the correlation-id allocator's high-water mark.
	IDHighWater uint64             `json:"id_high_water"`
	Services    map[string]Service `json:"services"`
	// AdapterSpecs holds runtime-loaded adapter definitions by name so that a
	// hot-loaded adapter is still there after a restart.
	//
	// Stored as raw JSON rather than as decoded values on purpose. Decoding
	// into `any` turns every number into a float64, which silently rounds the
	// 64-bit correlation identifiers inside a spec's golden vectors -- the
	// adapter then fails its own self-test after a restart even though nothing
	// about it changed. Keeping the bytes untouched removes the round trip
	// entirely.
	AdapterSpecs map[string]json.RawMessage `json:"adapter_specs,omitempty"`
	UpdatedAt    string                     `json:"updated_at"`
}

// CurrentSchemaVersion is the document version this build writes.
const CurrentSchemaVersion = 1

// Capabilities returns the sorted operation names of a service.
func (s Service) Capabilities() []string {
	out := make([]string, 0, len(s.Operations))
	for _, op := range s.Operations {
		out = append(out, op.Name)
	}
	sort.Strings(out)
	return out
}

// Operation looks up capability metadata by name.
func (s Service) Operation(name string) (Operation, bool) {
	for _, op := range s.Operations {
		if strings.EqualFold(op.Name, name) {
			return op, true
		}
	}
	return Operation{}, false
}

// Supports reports whether the service declares the operation.
func (s Service) Supports(name string) bool {
	_, ok := s.Operation(name)
	return ok
}

// ActiveVariants returns weight-carrying variants in a deterministic order.
func (s Service) ActiveVariants() []Variant {
	out := make([]Variant, 0, len(s.Variants))
	for _, v := range s.Variants {
		if v.Weight > 0 {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// VariantVersions lists every registered variant version, ascending.
func (s Service) VariantVersions() []int {
	out := make([]int, 0, len(s.Variants))
	for _, v := range s.Variants {
		out = append(out, v.Version)
	}
	sort.Ints(out)
	return out
}

// PrimaryVersion is the highest-weight variant, for display purposes.
func (s Service) PrimaryVersion() int {
	best, bestWeight := 0, -1
	for _, v := range s.Variants {
		if v.Weight > bestWeight || (v.Weight == bestWeight && v.Version > best) {
			best, bestWeight = v.Version, v.Weight
		}
	}
	return best
}

// EndpointFor resolves the effective endpoint of a variant.
func (s Service) EndpointFor(v Variant) string {
	if v.Endpoint != "" {
		return v.Endpoint
	}
	return s.Endpoint
}

// Validate enforces the structural invariants of a registry entry. It runs on
// every load and every mutation, so a corrupt or hostile document can never be
// committed into the live routing tables.
func (s Service) Validate() error {
	if strings.TrimSpace(s.ServiceID) == "" {
		return fmt.Errorf("service_id must not be empty")
	}
	switch s.Protocol {
	case ProtocolHTTPJSON, ProtocolTCPFrameJSON, ProtocolUDPCRCJSON:
	default:
		return fmt.Errorf("service %s: unknown protocol %q", s.ServiceID, s.Protocol)
	}
	if strings.TrimSpace(s.Endpoint) == "" {
		return fmt.Errorf("service %s: endpoint must not be empty", s.ServiceID)
	}
	if len(s.Variants) == 0 {
		return fmt.Errorf("service %s: at least one variant is required", s.ServiceID)
	}
	seenVersion := map[int]bool{}
	totalWeight := 0
	for _, v := range s.Variants {
		if v.Version <= 0 {
			return fmt.Errorf("service %s: variant version must be positive", s.ServiceID)
		}
		if seenVersion[v.Version] {
			return fmt.Errorf("service %s: duplicate variant version %d", s.ServiceID, v.Version)
		}
		seenVersion[v.Version] = true
		if v.Weight < 0 {
			return fmt.Errorf("service %s: variant %d has negative weight", s.ServiceID, v.Version)
		}
		if strings.TrimSpace(v.AdapterName) == "" {
			return fmt.Errorf("service %s: variant %d has no adapter_name", s.ServiceID, v.Version)
		}
		totalWeight += v.Weight
	}
	if s.Enabled && totalWeight == 0 {
		return fmt.Errorf("service %s: enabled service has no variant carrying weight", s.ServiceID)
	}
	seenOp := map[string]bool{}
	for _, op := range s.Operations {
		name := strings.ToLower(strings.TrimSpace(op.Name))
		if name == "" {
			return fmt.Errorf("service %s: operation name must not be empty", s.ServiceID)
		}
		if seenOp[name] {
			return fmt.Errorf("service %s: duplicate operation %q", s.ServiceID, name)
		}
		seenOp[name] = true
	}
	return nil
}

// Clone deep-copies a service so callers can never mutate registry state by
// holding on to a returned value.
func (s Service) Clone() Service {
	out := s
	out.Operations = append([]Operation(nil), s.Operations...)
	out.Variants = append([]Variant(nil), s.Variants...)
	return out
}
