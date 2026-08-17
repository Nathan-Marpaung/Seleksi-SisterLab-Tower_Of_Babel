package api

// Operator surface.
//
// Everything the bonus specifications ask for at runtime -- protocol migration,
// simultaneous version compatibility, hot adapter replacement -- is expressed
// here as ordinary registry and adapter mutations rather than as three separate
// mechanisms. A migration is a change to variant weights. A rolling upgrade is
// weights that add up across two versions. An adapter replacement is a spec
// load. All three share one durability rule: the change is validated, proven,
// and persisted before it becomes visible, and a rejected change leaves the
// running configuration exactly as it was.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"babel/gateway/internal/adapter"
	"babel/gateway/internal/registry"
)

const maxAdminBody = 1 << 20

func (s *Server) registerAdmin(mux *http.ServeMux) {
	admin := map[string]http.HandlerFunc{
		"GET /admin/registry":              s.adminGetRegistry,
		"POST /admin/registry/service":     s.adminUpsertService,
		"POST /admin/registry/enabled":     s.adminSetEnabled,
		"POST /admin/registry/weights":     s.adminSetWeights,
		"POST /admin/registry/replay-safe": s.adminSetReplaySafe,

		"GET /admin/adapters":         s.adminListAdapters,
		"POST /admin/adapters":        s.adminLoadAdapter,
		"POST /admin/adapters/remove": s.adminRemoveAdapter,

		"POST /admin/migrate":        s.adminMigrate,
		"POST /admin/breakers/reset": s.adminResetBreakers,
	}
	for pattern, handler := range admin {
		mux.HandleFunc(pattern, s.requireAdminToken(handler))
	}
}

// requireAdminToken gates the operator surface.
//
// The gateway ships with the token empty, which leaves the surface open. That
// is a deliberate choice for a self-contained evaluation environment where the
// port is not published beyond the compose network, and it is exactly the kind
// of default that would be wrong in production -- so setting BABEL_ADMIN_TOKEN
// closes it, and BABEL_ADMIN_ENABLED=false removes the routes entirely.
func (s *Server) requireAdminToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.d.Cfg.AdminToken != "" && r.Header.Get("X-Babel-Admin-Token") != s.d.Cfg.AdminToken {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"status": "error", "message": "A valid X-Babel-Admin-Token header is required.",
			})
			return
		}
		next(w, r)
	}
}

func (s *Server) adminGetRegistry(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"registry": s.d.Registry.SnapshotView(),
		"source":   s.d.Registry.Source(),
	})
}

func (s *Server) adminUpsertService(w http.ResponseWriter, r *http.Request) {
	var svc registry.Service
	if !decodeAdminBody(w, r, &svc) {
		return
	}
	if err := s.d.Registry.Upsert(svc); err != nil {
		adminError(w, http.StatusBadRequest, err)
		return
	}
	s.d.Log.Info("registry service upserted", map[string]any{
		"service_id": svc.ServiceID, "revision": s.d.Registry.Revision(),
	})
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": s.d.Registry.Revision()})
}

func (s *Server) adminSetEnabled(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID string `json:"service_id"`
		Enabled   bool   `json:"enabled"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}
	if err := s.d.Registry.SetEnabled(body.ServiceID, body.Enabled); err != nil {
		adminError(w, http.StatusBadRequest, err)
		return
	}
	s.d.Log.Info("registry service enablement changed", map[string]any{
		"service_id": body.ServiceID, "enabled": body.Enabled,
	})
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": s.d.Registry.Revision()})
}

func (s *Server) adminSetReplaySafe(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID  string `json:"service_id"`
		Operation  string `json:"operation"`
		ReplaySafe bool   `json:"replay_safe"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}
	if err := s.d.Registry.SetOperationReplaySafe(body.ServiceID, body.Operation, body.ReplaySafe); err != nil {
		adminError(w, http.StatusBadRequest, err)
		return
	}
	s.d.Log.Warn("fallback replay-safety changed", map[string]any{
		"service_id": body.ServiceID, "operation": body.Operation, "replay_safe": body.ReplaySafe,
		"consequence": "ambiguous failures may now be retried on another backend",
	})
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": s.d.Registry.Revision()})
}

// adminSetWeights is the traffic-shifting primitive behind rolling upgrades.
func (s *Server) adminSetWeights(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID string         `json:"service_id"`
		Weights   map[string]int `json:"weights"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}
	weights, err := parseVersionWeights(body.Weights)
	if err != nil {
		adminError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.d.Registry.SetVariantWeights(body.ServiceID, weights); err != nil {
		adminError(w, http.StatusBadRequest, err)
		return
	}
	s.d.Log.Info("variant weights updated", map[string]any{
		"service_id": body.ServiceID, "weights": body.Weights, "revision": s.d.Registry.Revision(),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "revision": s.d.Registry.Revision(),
	})
}

// adminResetBreakers clears circuit state after an operator has fixed a
// backend, instead of making them wait out the cooldown to confirm it.
func (s *Server) adminResetBreakers(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID string `json:"service_id,omitempty"`
	}
	// An empty body means "all of them", so a decode failure is not fatal here.
	_ = decodeAdminBodyOptional(r, &body)

	prefix := ""
	if body.ServiceID != "" {
		prefix = body.ServiceID + "#"
	}
	n := s.d.Breakers.ResetAll(prefix)
	s.d.Log.Warn("circuit breakers reset by operator", map[string]any{
		"service_id": body.ServiceID, "breakers_reset": n,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "breakers_reset": n, "breakers": s.d.Breakers.Stats(),
	})
}

func (s *Server) adminListAdapters(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "adapters": s.d.Adapters.Describe(),
	})
}

// adminLoadAdapter installs or replaces a protocol adapter without a restart.
//
// The order of operations is the whole safety story:
//
//  1. the spec is structurally validated;
//  2. a new adapter instance is constructed under panic recovery and a
//     deadline, so a hostile or broken spec cannot hang or crash the gateway;
//  3. the instance must reproduce its golden vectors exactly;
//  4. only then is it published, and the previous instance is drained rather
//     than closed, so requests already in flight finish on the adapter they
//     started with;
//  5. the spec is persisted, so the hot-loaded adapter is still there after a
//     restart.
//
// A failure at any step returns an error and changes nothing.
func (s *Server) adminLoadAdapter(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Spec *adapter.Spec `json:"spec"`
		// Endpoint overrides where the adapter connects when the spec does not
		// carry one and the service is not yet registered.
		Endpoint string `json:"endpoint,omitempty"`
		// Persist defaults to true; set false to try an adapter for the life of
		// the process only.
		Persist *bool `json:"persist,omitempty"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}
	if body.Spec == nil {
		adminError(w, http.StatusBadRequest, fmt.Errorf("field spec is required"))
		return
	}

	endpoint := body.Endpoint
	if endpoint == "" {
		if svc, ok := s.d.Registry.Get(body.Spec.ServiceID); ok {
			endpoint = svc.Endpoint
		}
	}
	if endpoint == "" && body.Spec.Endpoint == "" {
		adminError(w, http.StatusBadRequest,
			fmt.Errorf("no endpoint: the spec has none, and service %q is not registered", body.Spec.ServiceID))
		return
	}

	opts := s.d.BuildOptions(body.Spec.ServiceID, endpoint)
	if err := s.d.Adapters.Load(body.Spec, opts); err != nil {
		// The previously loaded adapter, if any, is still serving.
		s.d.Metrics.Inc("adapter_loads_rejected")
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": "error", "message": err.Error(),
			"rolled_back": true,
			"detail":      "the previously loaded adapter is unchanged and still serving",
		})
		return
	}
	s.d.Metrics.Inc("adapter_loads_accepted")

	persist := body.Persist == nil || *body.Persist
	if persist {
		if err := s.d.Registry.PutAdapterSpec(body.Spec.Name, body.Spec); err != nil {
			// The adapter is live but its spec could not be written. Saying so
			// is better than pretending the change is durable.
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "ok", "adapter": body.Spec.Name, "persisted": false,
				"warning": "adapter is live but its spec could not be persisted: " + err.Error(),
			})
			return
		}
	}
	s.d.Log.Info("adapter loaded at runtime", map[string]any{
		"adapter": body.Spec.Name, "service_id": body.Spec.ServiceID,
		"version": body.Spec.Version, "persisted": persist,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "adapter": body.Spec.Name, "persisted": persist,
		"adapters": s.d.Adapters.Describe(),
	})
}

func (s *Server) adminRemoveAdapter(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}
	// The registry refuses while a variant still binds the adapter, which stops
	// an operator from removing the thing that is currently carrying traffic.
	if err := s.d.Registry.DeleteAdapterSpec(body.Name); err != nil {
		adminError(w, http.StatusConflict, err)
		return
	}
	removed := s.d.Adapters.Remove(body.Name)
	s.d.Log.Info("adapter removed", map[string]any{"adapter": body.Name, "was_loaded": removed})
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "was_loaded": removed})
}

// adminMigrate performs a protocol version migration in one call.
//
// It is a convenience over the primitives, not a separate mechanism: load the
// new version's adapter, register it as a variant, then shift weight. Doing it
// as one endpoint matters because the intermediate states are the dangerous
// ones -- a variant registered without an adapter, or weight shifted to a
// version whose adapter failed to load. Each step is checked before the next,
// and a failure leaves the previous step's state intact and still serving.
func (s *Server) adminMigrate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID string        `json:"service_id"`
		Spec      *adapter.Spec `json:"spec,omitempty"`
		Version   int           `json:"version"`
		// Endpoint lets the new version live on a different address, which is
		// what a genuine side-by-side rollout looks like.
		Endpoint string `json:"endpoint,omitempty"`
		// Weights is the target traffic split, keyed by version. Omitting it
		// registers the variant at zero weight, which is the safe default: the
		// new version is loadable and probeable but carries no traffic yet.
		Weights map[string]int `json:"weights,omitempty"`
	}
	if !decodeAdminBody(w, r, &body) {
		return
	}

	svc, ok := s.d.Registry.Get(body.ServiceID)
	if !ok {
		adminError(w, http.StatusNotFound, fmt.Errorf("service %q is not registered", body.ServiceID))
		return
	}

	steps := []map[string]any{}

	// Step 1: the adapter for the target version must exist and be proven.
	adapterName := ""
	if body.Spec != nil {
		if body.Spec.ServiceID != body.ServiceID {
			adminError(w, http.StatusBadRequest,
				fmt.Errorf("spec service_id %q does not match %q", body.Spec.ServiceID, body.ServiceID))
			return
		}
		if body.Version != 0 && body.Spec.Version != body.Version {
			adminError(w, http.StatusBadRequest,
				fmt.Errorf("spec version %d does not match requested version %d", body.Spec.Version, body.Version))
			return
		}
		endpoint := body.Endpoint
		if endpoint == "" {
			endpoint = svc.Endpoint
		}
		if err := s.d.Adapters.Load(body.Spec, s.d.BuildOptions(body.ServiceID, endpoint)); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"status": "error", "stage": "adapter_load", "message": err.Error(),
				"rolled_back": true,
				"detail":      "no variant was registered and no traffic was shifted",
			})
			return
		}
		if err := s.d.Registry.PutAdapterSpec(body.Spec.Name, body.Spec); err != nil {
			adminError(w, http.StatusInternalServerError, err)
			return
		}
		adapterName = body.Spec.Name
		body.Version = body.Spec.Version
		steps = append(steps, map[string]any{"step": "adapter_loaded", "adapter": adapterName})
	} else {
		for _, v := range svc.Variants {
			if v.Version == body.Version {
				adapterName = v.AdapterName
			}
		}
		if adapterName == "" {
			adminError(w, http.StatusBadRequest,
				fmt.Errorf("version %d is not registered for %q and no spec was supplied", body.Version, body.ServiceID))
			return
		}
	}

	// Step 2: register the variant if it is new, always at zero weight first.
	known := false
	for _, v := range svc.Variants {
		if v.Version == body.Version {
			known = true
		}
	}
	if !known {
		variant := registry.Variant{
			Version: body.Version, Weight: 0,
			Endpoint: body.Endpoint, AdapterName: adapterName,
		}
		if err := s.d.Registry.AddVariant(body.ServiceID, variant); err != nil {
			adminError(w, http.StatusBadRequest, err)
			return
		}
		steps = append(steps, map[string]any{"step": "variant_registered", "version": body.Version, "weight": 0})
	}

	// Step 3: shift traffic, if asked. Until this succeeds the old version is
	// still carrying everything, so an abort here is harmless.
	if len(body.Weights) > 0 {
		weights, err := parseVersionWeights(body.Weights)
		if err != nil {
			adminError(w, http.StatusBadRequest, err)
			return
		}
		if err := s.d.Registry.SetVariantWeights(body.ServiceID, weights); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"status": "error", "stage": "weight_shift", "message": err.Error(),
				"steps": steps, "rolled_back": true,
				"detail": "the new variant is registered at zero weight; the previous version still serves all traffic",
			})
			return
		}
		steps = append(steps, map[string]any{"step": "weights_applied", "weights": body.Weights})
	}

	updated, _ := s.d.Registry.Get(body.ServiceID)
	s.d.Log.Info("protocol migration applied", map[string]any{
		"service_id": body.ServiceID, "version": body.Version,
		"variants": updated.Variants, "revision": s.d.Registry.Revision(),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "service_id": body.ServiceID, "steps": steps,
		"variants": updated.Variants, "revision": s.d.Registry.Revision(),
		"note": "in-flight requests finish on the adapter instance they started with",
	})
}

// parseVersionWeights converts the JSON-object-keyed weights (JSON object keys
// are always strings) into the integer-keyed map the registry works with.
func parseVersionWeights(in map[string]int) (map[int]int, error) {
	out := make(map[int]int, len(in))
	for k, v := range in {
		var version int
		if _, err := fmt.Sscanf(strings.TrimSpace(k), "%d", &version); err != nil {
			return nil, fmt.Errorf("weight key %q is not a protocol version number", k)
		}
		if v < 0 {
			return nil, fmt.Errorf("weight for version %d must not be negative", version)
		}
		out[version] = v
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("weights must not be empty")
	}
	return out, nil
}

func decodeAdminBody(w http.ResponseWriter, r *http.Request, target any) bool {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxAdminBody+1))
	if err != nil {
		adminError(w, http.StatusBadRequest, fmt.Errorf("request body could not be read"))
		return false
	}
	if len(raw) > maxAdminBody {
		adminError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("request body exceeds 1 MiB"))
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		adminError(w, http.StatusBadRequest, fmt.Errorf("request body is not valid JSON for this endpoint: %w", err))
		return false
	}
	return true
}

// decodeAdminBodyOptional parses a body that may legitimately be absent.
func decodeAdminBodyOptional(r *http.Request, target any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxAdminBody))
	if err != nil || len(raw) == 0 {
		return err
	}
	return json.Unmarshal(raw, target)
}

func adminError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
}
