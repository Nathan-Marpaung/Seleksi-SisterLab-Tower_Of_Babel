// Package apimodel holds the wire types of the public Gateway API and the
// canonical, backend-neutral shapes the rest of the gateway works with.
//
// Nothing below this package is allowed to know about `operation_result`,
// `resultData`, `numericResult`, `errorData` or `serviceId`; those are backend
// dialects and adapters must translate them away.
package apimodel

import (
	"encoding/json"
	"fmt"
	"math"

	"babel/gateway/internal/gwerr"
)

// Operation names accepted by the gateway.
const (
	OpEcho      = "echo"
	OpUppercase = "uppercase"
	OpSum       = "sum"
	OpReverse   = "reverse"
	OpMetadata  = "metadata"
)

// ExecuteRequest is the POST /execute body.
type ExecuteRequest struct {
	RequestID string         `json:"request_id"`
	Operation string         `json:"operation"`
	Arguments map[string]any `json:"arguments"`
	Options   map[string]any `json:"options"`
}

// ExecuteResponse is the POST /execute envelope. Field order matches the
// published contract; all six keys are always present, including explicit
// nulls, because the reference client validates on key presence.
type ExecuteResponse struct {
	RequestID string         `json:"request_id"`
	Status    string         `json:"status"`
	ServiceID *string        `json:"service_id"`
	Operation string         `json:"operation"`
	Result    map[string]any `json:"result"`
	Error     *APIError      `json:"error"`
}

// APIError is the normalized error object.
type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

const (
	StatusSuccess = "success"
	StatusError   = "error"
)

// Success builds a success envelope. Callers must pass a non-nil result; a nil
// result would violate the contract invariant, so it is coerced to an empty
// object rather than silently emitting `"result": null` with `"status":
// "success"`.
func Success(requestID, operation, serviceID string, result map[string]any) ExecuteResponse {
	if result == nil {
		result = map[string]any{}
	}
	sid := serviceID
	var sidPtr *string
	if sid != "" {
		sidPtr = &sid
	}
	return ExecuteResponse{
		RequestID: requestID,
		Status:    StatusSuccess,
		ServiceID: sidPtr,
		Operation: operation,
		Result:    result,
		Error:     nil,
	}
}

// Failure builds an error envelope. serviceID may be empty, which renders as
// `"service_id": null` and means "the gateway could not safely resolve a
// backend route".
func Failure(requestID, operation, serviceID string, e *gwerr.Error) ExecuteResponse {
	var sidPtr *string
	if serviceID != "" {
		sid := serviceID
		sidPtr = &sid
	}
	if e == nil {
		e = gwerr.New(gwerr.CodeInternal, "Unspecified gateway error.", false, false)
	}
	return ExecuteResponse{
		RequestID: requestID,
		Status:    StatusError,
		ServiceID: sidPtr,
		Operation: operation,
		Result:    nil,
		Error:     &APIError{Code: e.Code, Message: e.Message, Retryable: e.Retryable},
	}
}

// Options are the recognized execution hints. Unknown option keys are ignored
// rather than rejected, as the contract requires.
type Options struct {
	PreferredService string
	TimeoutMS        int
	HasTimeout       bool
}

// ParseOptions extracts recognized hints. It never fails: an unusable value for
// a known key is treated as absent, because the contract forbids failing a
// request merely over option contents.
func ParseOptions(raw map[string]any) Options {
	var o Options
	if raw == nil {
		return o
	}
	if v, ok := raw["preferred_service"]; ok && v != nil {
		if s, ok := v.(string); ok {
			o.PreferredService = s
		}
	}
	if v, ok := raw["timeout_ms"]; ok && v != nil {
		if ms, ok := toFloat(v); ok && ms > 0 && !math.IsInf(ms, 0) && !math.IsNaN(ms) {
			o.TimeoutMS = int(ms)
			o.HasTimeout = true
		}
	}
	return o
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// ValidateEnvelope checks the request against the Gateway API contract. A
// failure here is a contract violation by the caller, not a domain error, and
// maps to HTTP 400.
func ValidateEnvelope(req *ExecuteRequest, raw map[string]json.RawMessage) *gwerr.Error {
	if _, ok := raw["request_id"]; !ok {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field request_id is required.", false, false)
	}
	if req.RequestID == "" {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field request_id must be a non-empty string.", false, false)
	}
	if len(req.RequestID) > 512 {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field request_id exceeds 512 characters.", false, false)
	}
	if _, ok := raw["operation"]; !ok {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field operation is required.", false, false)
	}
	if req.Operation == "" {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field operation must be a non-empty string.", false, false)
	}
	// `arguments` and `options` are required by the table but an explicit null
	// is treated as an empty object: rejecting that would be pedantry that
	// breaks well-behaved callers.
	if v, ok := raw["arguments"]; ok && !isNullOrObject(v) {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field arguments must be an object.", false, false)
	}
	if v, ok := raw["options"]; ok && !isNullOrObject(v) {
		return gwerr.New(gwerr.CodeInvalidRequest, "Field options must be an object.", false, false)
	}
	return nil
}

func isNullOrObject(raw json.RawMessage) bool {
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', 'n':
			return true
		default:
			return false
		}
	}
	return false
}

// int32 bounds for `sum` inputs, per the Gateway API contract.
const (
	sumMin = -2147483648.0
	sumMax = 2147483647.0
)

// ValidateArguments applies the documented per-operation argument rules before
// any backend is contacted.
//
// Pre-validating is deliberate: it makes argument errors deterministic and
// identical regardless of which backend routing would have picked, and it keeps
// a malformed call from consuming a backend attempt and a slice of the caller's
// timeout budget.
func ValidateArguments(operation string, args map[string]any) *gwerr.Error {
	switch operation {
	case OpEcho, OpMetadata:
		return nil

	case OpUppercase, OpReverse:
		v, ok := args["value"]
		if !ok {
			return gwerr.Newf(gwerr.CodeInvalidArgs, false, false, "%s requires a string value", operation)
		}
		if _, ok := v.(string); !ok {
			return gwerr.Newf(gwerr.CodeInvalidArgs, false, false, "%s requires a string value", operation)
		}
		return nil

	case OpSum:
		raw, ok := args["values"]
		if !ok {
			return gwerr.New(gwerr.CodeInvalidArgs, "sum requires a numeric values list", false, false)
		}
		list, ok := raw.([]any)
		if !ok {
			return gwerr.New(gwerr.CodeInvalidArgs, "sum requires a numeric values list", false, false)
		}
		for i, item := range list {
			f, ok := toFloat(item)
			if !ok {
				return gwerr.Newf(gwerr.CodeInvalidArgs, false, false,
					"sum requires numeric values; element %d is not a JSON number", i)
			}
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return gwerr.Newf(gwerr.CodeInvalidArgs, false, false,
					"sum requires finite values; element %d is not finite", i)
			}
			if f < sumMin || f > sumMax {
				return gwerr.Newf(gwerr.CodeInvalidArgs, false, false,
					"sum values must fit in signed 32-bit range; element %d is %v", i, f)
			}
		}
		return nil
	}

	// Unknown operations are a routing concern, not an argument concern; the
	// router reports them with the capability set it actually knows about.
	return nil
}

// ServiceView is one entry of GET /services.
type ServiceView struct {
	ServiceID       string   `json:"service_id"`
	Protocol        string   `json:"protocol"`
	Status          string   `json:"status"`
	Capabilities    []string `json:"capabilities"`
	ProtocolVersion int      `json:"protocol_version"`
	Endpoint        string   `json:"endpoint"`
	AdapterVersions []int    `json:"adapter_versions"`
	LastCheckedMS   int64    `json:"last_checked_ms"`
	Detail          string   `json:"detail,omitempty"`
}

// ServicesResponse is the GET /services body.
type ServicesResponse struct {
	Services []ServiceView `json:"services"`
}

// StatusResponse is the GET /status body. `status` at top level is mandated by
// the contract; everything else is the observability surface required by the
// specification (registered services, protocol type, health, capabilities and
// protocol versions).
type StatusResponse struct {
	Status    string            `json:"status"`
	GatewayID string            `json:"gateway_id"`
	UptimeMS  int64             `json:"uptime_ms"`
	StartedAt string            `json:"started_at"`
	Backends  map[string]string `json:"backends"`
	Services  []ServiceView     `json:"services"`
	Runtime   RuntimeView       `json:"runtime"`
	Metrics   map[string]any    `json:"metrics"`
}

// RuntimeView summarizes gateway-internal state for observability.
type RuntimeView struct {
	InFlight        int    `json:"in_flight"`
	MaxInFlight     int    `json:"max_in_flight"`
	RegistryPath    string `json:"registry_path"`
	RegistryVersion int64  `json:"registry_version"`
	RegistryLoaded  string `json:"registry_source"`
	AdaptersLoaded  int    `json:"adapters_loaded"`
	Draining        bool   `json:"draining"`
	GoVersion       string `json:"go_version"`
	Goroutines      int    `json:"goroutines"`
}

// Health values reported for a backend.
const (
	HealthAvailable   = "available"
	HealthDegraded    = "degraded"
	HealthUnavailable = "unavailable"
	HealthUnknown     = "unknown"
	HealthDisabled    = "disabled"
)

func (v ServiceView) String() string {
	return fmt.Sprintf("%s(%s/v%d)=%s", v.ServiceID, v.Protocol, v.ProtocolVersion, v.Status)
}
