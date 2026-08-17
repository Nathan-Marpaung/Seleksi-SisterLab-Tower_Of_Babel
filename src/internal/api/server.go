// Package api exposes the public Gateway API and the operator surface.
//
// The layer is deliberately thin. Everything it does is: bound the request,
// prove the envelope is well formed, hand off to the router, and serialize an
// answer that always satisfies the published contract. It contains no protocol
// knowledge and no routing policy, which is what keeps the contract invariants
// checkable in one place.
//
// One decision here is worth reading before the code. The Gateway API defines
// HTTP statuses for gateway-level conditions (408 on timeout, 503 when no route
// is available), yet it also states that the envelope is what is evaluated, and
// the reference client calls raise_for_status() -- so any non-2xx reply aborts
// it before the envelope is ever inspected. Returning 408 for a timeout would
// therefore replace a precise, machine-readable error envelope with a client
// exception. The gateway resolves this by answering 200 with a fully populated
// error envelope for every *domain-level* outcome, and reserving non-2xx for
// contract violations by the caller (400) and gateway defects (500). Setting
// BABEL_STRICT_HTTP_STATUS=true selects the literal status mapping instead; the
// envelope is identical either way.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"babel/gateway/internal/adapter"
	"babel/gateway/internal/apimodel"
	"babel/gateway/internal/breaker"
	"babel/gateway/internal/config"
	"babel/gateway/internal/gwerr"
	"babel/gateway/internal/idgen"
	"babel/gateway/internal/obs"
	"babel/gateway/internal/registry"
	"babel/gateway/internal/router"
)

// maxRequestBody bounds what the gateway will read from a client. Without it a
// single caller could grow gateway memory without limit.
const maxRequestBody = 1 << 20

// Deps are the collaborators the server needs.
type Deps struct {
	Cfg      config.Config
	Registry *registry.Registry
	Adapters *adapter.Manager
	Breakers *breaker.Set
	Router   *router.Router
	IDs      *idgen.Generator
	Log      *obs.Logger
	Metrics  *obs.Metrics

	StartedAt time.Time
	// RegistryWarning surfaces a boot-time problem (a quarantined registry, for
	// instance) through /status instead of only in the logs.
	RegistryWarning string
	// BuildOptions resolves adapter build policy for a service, used when an
	// adapter is loaded at runtime.
	BuildOptions func(serviceID, endpoint string) adapter.BuildOptions
}

// Server implements the HTTP surface.
type Server struct {
	d Deps
}

// New builds a server.
func New(d Deps) *Server { return &Server{d: d} }

// Handler returns the routed, instrumented handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /execute", s.handleExecute)
	mux.HandleFunc("GET /services", s.handleServices)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	if s.d.Cfg.AdminEnabled {
		s.registerAdmin(mux)
	}

	// Method mismatches on known paths get a clear answer rather than the
	// default 404, which would look like a missing gateway.
	mux.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
			"status": "error", "error": map[string]any{
				"code": gwerr.CodeInvalidRequest, "message": "POST is required for /execute.", "retryable": false,
			},
		})
	})

	return s.recoverPanics(mux)
}

// recoverPanics guarantees that a defect in one request cannot take down the
// gateway or leave a client hanging. A panic becomes a 500 with a valid JSON
// body, and the stack goes to the log where it belongs.
func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.d.Metrics.Inc("panics_recovered")
				s.d.Log.Error("handler panicked", map[string]any{
					"path": r.URL.Path, "panic": fmt.Sprint(rec), "stack": string(debug.Stack()),
				})
				writeJSON(w, http.StatusInternalServerError, apimodel.Failure("", "", "",
					gwerr.New(gwerr.CodeInternal, "Gateway encountered an internal error.", false, false)))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		s.writeEnvelope(w, http.StatusBadRequest, apimodel.Failure("", "", "",
			gwerr.New(gwerr.CodeInvalidRequest, "Request body could not be read.", false, false)))
		return
	}
	if len(body) > maxRequestBody {
		s.writeEnvelope(w, http.StatusBadRequest, apimodel.Failure("", "", "",
			gwerr.New(gwerr.CodeInvalidRequest, "Request body exceeds the 1 MiB limit.", false, false)))
		return
	}

	// Decoded twice on purpose: once loosely to see which keys were actually
	// present (the contract distinguishes "absent" from "null"), once into the
	// typed request.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		s.writeEnvelope(w, http.StatusBadRequest, apimodel.Failure("", "", "",
			gwerr.New(gwerr.CodeInvalidRequest, "Request body is not a JSON object.", false, false)))
		return
	}
	var req apimodel.ExecuteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeEnvelope(w, http.StatusBadRequest, apimodel.Failure("", "", "",
			gwerr.New(gwerr.CodeInvalidRequest, "Request body does not match the execute contract.", false, false)))
		return
	}
	if e := apimodel.ValidateEnvelope(&req, raw); e != nil {
		s.writeEnvelope(w, http.StatusBadRequest, apimodel.Failure(req.RequestID, req.Operation, "", e))
		return
	}
	if req.Arguments == nil {
		req.Arguments = map[string]any{}
	}

	resp := s.d.Router.Execute(r.Context(), req)
	s.writeEnvelope(w, s.statusFor(resp), resp)
}

// statusFor maps an envelope onto an HTTP status. See the package comment for
// why the default is 200 for domain-level errors.
func (s *Server) statusFor(resp apimodel.ExecuteResponse) int {
	if resp.Status == apimodel.StatusSuccess || resp.Error == nil {
		return http.StatusOK
	}
	if !s.d.Cfg.StrictHTTPStatus {
		return http.StatusOK
	}
	switch resp.Error.Code {
	case gwerr.CodeInvalidRequest, gwerr.CodeInvalidArgs:
		return http.StatusBadRequest
	case gwerr.CodeBackendTimeout, gwerr.CodeGatewayTimeout:
		return http.StatusRequestTimeout
	case gwerr.CodeRateLimited, gwerr.CodeGatewayOverloaded:
		return http.StatusTooManyRequests
	case gwerr.CodeNoRoute, gwerr.CodeServiceUnavailable, gwerr.CodeBackendUnavailable,
		gwerr.CodeConnectionFailed, gwerr.CodeGatewayShutdown:
		return http.StatusServiceUnavailable
	case gwerr.CodeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusOK
	}
}

// writeEnvelope serializes an execute response, enforcing the contract
// invariants one last time before the bytes leave the process.
//
// This is a belt-and-braces check. Every producer above already maintains them,
// but "success implies a result and no error" is the single property the whole
// contract rests on, and a cheap assertion here means a future bug becomes a
// logged internal error rather than a malformed envelope on the wire.
func (s *Server) writeEnvelope(w http.ResponseWriter, status int, resp apimodel.ExecuteResponse) {
	switch resp.Status {
	case apimodel.StatusSuccess:
		if resp.Result == nil || resp.Error != nil {
			s.d.Log.Error("envelope invariant violated on success", map[string]any{"request_id": resp.RequestID})
			resp = apimodel.Failure(resp.RequestID, resp.Operation, "",
				gwerr.New(gwerr.CodeInternal, "Gateway produced an inconsistent response.", false, false))
			status = http.StatusInternalServerError
		}
	case apimodel.StatusError:
		if resp.Error == nil {
			resp.Error = &apimodel.APIError{Code: gwerr.CodeInternal, Message: "Unspecified gateway error.", Retryable: false}
		}
		resp.Result = nil
	default:
		s.d.Log.Error("envelope carried an unknown status", map[string]any{"status": resp.Status})
		resp = apimodel.Failure(resp.RequestID, resp.Operation, "",
			gwerr.New(gwerr.CodeInternal, "Gateway produced an inconsistent response.", false, false))
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, resp)
}

func (s *Server) handleServices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, apimodel.ServicesResponse{Services: s.serviceViews()})
}

// serviceViews renders the registry plus live health for observability.
func (s *Server) serviceViews() []apimodel.ServiceView {
	services := s.d.Registry.List()
	out := make([]apimodel.ServiceView, 0, len(services))

	for _, svc := range services {
		h := s.d.Registry.HealthOf(svc.ServiceID)
		view := apimodel.ServiceView{
			ServiceID:       svc.ServiceID,
			Protocol:        svc.Protocol,
			Status:          h.Status,
			Capabilities:    svc.Capabilities(),
			ProtocolVersion: svc.PrimaryVersion(),
			Endpoint:        svc.Endpoint,
			AdapterVersions: svc.VariantVersions(),
			Detail:          h.Detail,
		}
		if !h.LastChecked.IsZero() {
			view.LastCheckedMS = time.Since(h.LastChecked).Milliseconds()
		}
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	views := s.serviceViews()
	backends := make(map[string]string, len(views))
	for _, v := range views {
		backends[v.ServiceID] = v.Status
	}

	metrics := s.d.Metrics.Snapshot()
	metrics["breakers"] = s.d.Breakers.Stats()
	metrics["transports"] = s.d.Adapters.Stats()

	resp := apimodel.StatusResponse{
		Status:    s.overallStatus(views),
		GatewayID: s.d.Cfg.GatewayID,
		UptimeMS:  time.Since(s.d.StartedAt).Milliseconds(),
		StartedAt: s.d.StartedAt.UTC().Format(time.RFC3339Nano),
		Backends:  backends,
		Services:  views,
		Runtime: apimodel.RuntimeView{
			InFlight:        s.d.Router.InFlight(),
			MaxInFlight:     s.d.Cfg.MaxInFlight,
			RegistryPath:    s.d.Cfg.StateDir,
			RegistryVersion: s.d.Registry.Revision(),
			RegistryLoaded:  s.d.Registry.Source(),
			AdaptersLoaded:  len(s.d.Adapters.Names()),
			Draining:        s.d.Router.Draining(),
			GoVersion:       runtime.Version(),
			Goroutines:      runtime.NumGoroutine(),
		},
		Metrics: metrics,
	}

	body := map[string]any{}
	raw, _ := json.Marshal(resp)
	_ = json.Unmarshal(raw, &body)
	body["adapters"] = s.d.Adapters.Describe()
	if s.d.RegistryWarning != "" {
		body["warning"] = s.d.RegistryWarning
	}
	writeJSON(w, http.StatusOK, body)
}

// overallStatus reports "ok" while the gateway can still serve something.
//
// A gateway with one dead backend out of three is degraded, not down: it can
// still answer every operation the remaining backends cover. Reporting "error"
// there would be misleading to anyone watching.
func (s *Server) overallStatus(views []apimodel.ServiceView) string {
	if s.d.Router.Draining() {
		return "draining"
	}
	available, total := 0, 0
	for _, v := range views {
		if v.Status == apimodel.HealthDisabled {
			continue
		}
		total++
		if v.Status == apimodel.HealthAvailable {
			available++
		}
	}
	switch {
	case total == 0:
		return "error"
	case available == total:
		return "ok"
	case available == 0:
		return "error"
	default:
		return "degraded"
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"counters":   s.d.Metrics.Snapshot(),
		"breakers":   s.d.Breakers.Stats(),
		"transports": s.d.Adapters.Stats(),
		"in_flight":  s.d.Router.InFlight(),
		"uptime_ms":  time.Since(s.d.StartedAt).Milliseconds(),
	})
}

// handleHealthz is the container liveness probe: it reports whether the process
// is serving, not whether the backends are, so a backend outage never causes an
// orchestrator to restart a perfectly healthy gateway.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.d.Router.Draining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	buf, err := json.Marshal(body)
	if err != nil {
		// Serialization must never leave the client with an empty response.
		buf = []byte(`{"status":"error","error":{"code":"INTERNAL_ERROR","message":"Response could not be serialized.","retryable":false}}`)
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}
