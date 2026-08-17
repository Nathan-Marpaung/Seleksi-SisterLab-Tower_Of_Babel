// Package gwerr defines the gateway's normalized error taxonomy.
//
// Every failure inside the gateway -- transport, framing, validation, routing,
// or a domain error reported by a backend -- is funnelled through Error before
// it can reach a client. That is what lets the HTTP layer stay dumb: it never
// has to know whether "the frame magic was wrong" or "service-a said 429".
package gwerr

import (
	"errors"
	"fmt"
)

// Canonical gateway error codes.
//
// Codes fall in three groups:
//
//   - request-shape codes, produced before any backend is touched;
//   - routing codes, produced when no backend can safely serve the call;
//   - attempt codes, produced by an adapter while talking to a backend.
//
// Backend domain codes (OPERATION_NOT_SUPPORTED, INVALID_ARGUMENT, ...) are
// passed through verbatim rather than remapped, because they carry more
// information than any gateway-side substitute would.
const (
	// Request shape.
	CodeInvalidRequest = "INVALID_REQUEST"
	CodeInvalidArgs    = "INVALID_ARGUMENT"

	// Routing.
	CodeUnsupportedOperation = "UNSUPPORTED_OPERATION"
	CodeNoRoute              = "NO_ROUTE_AVAILABLE"
	CodeServiceUnavailable   = "SERVICE_UNAVAILABLE"
	CodePreferredIncapable   = "PREFERRED_SERVICE_INCAPABLE"

	// Attempt / transport.
	CodeBackendTimeout      = "BACKEND_TIMEOUT"
	CodeBackendUnavailable  = "BACKEND_UNAVAILABLE"
	CodeConnectionFailed    = "BACKEND_CONNECTION_FAILED"
	CodeProtocolViolation   = "BACKEND_PROTOCOL_VIOLATION"
	CodeChecksumMismatch    = "BACKEND_CHECKSUM_MISMATCH"
	CodeCorrelationMismatch = "BACKEND_CORRELATION_MISMATCH"
	CodeUnsupportedVersion  = "UNSUPPORTED_PROTOCOL_VERSION"
	CodePayloadTooLarge     = "PAYLOAD_TOO_LARGE"
	CodeBackendError        = "BACKEND_ERROR"
	CodeRateLimited         = "RATE_LIMITED"

	// Gateway self.
	CodeGatewayTimeout    = "GATEWAY_TIMEOUT"
	CodeGatewayOverloaded = "GATEWAY_OVERLOADED"
	CodeGatewayShutdown   = "GATEWAY_SHUTTING_DOWN"
	CodeInternal          = "INTERNAL_ERROR"
	CodeAdapterRejected   = "ADAPTER_REJECTED"
	CodeMigrationConflict = "MIGRATION_CONFLICT"
)

// Error is a normalized gateway error.
//
// Retryable states whether *a retry could plausibly succeed*. FallbackSafe is a
// strictly stronger claim: it says the gateway knows the failed attempt did not
// and will not produce a backend-visible effect, so re-issuing the operation
// somewhere else cannot duplicate work. A write that timed out mid-flight is
// Retryable but not FallbackSafe.
type Error struct {
	Code         string
	Message      string
	Retryable    bool
	FallbackSafe bool

	// ServiceID names the backend that produced the error, when known.
	ServiceID string
	// Cause is kept for logs only; it is never exposed to clients.
	Cause error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// New builds an error with explicit retry semantics.
func New(code, message string, retryable, fallbackSafe bool) *Error {
	return &Error{Code: code, Message: message, Retryable: retryable, FallbackSafe: fallbackSafe}
}

// Newf is New with a formatted message.
func Newf(code string, retryable, fallbackSafe bool, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Retryable: retryable, FallbackSafe: fallbackSafe}
}

// Wrap attaches a cause without changing the visible message.
func (e *Error) Wrap(cause error) *Error {
	clone := *e
	clone.Cause = cause
	return &clone
}

// WithService tags the error with the backend that produced it.
func (e *Error) WithService(serviceID string) *Error {
	clone := *e
	clone.ServiceID = serviceID
	return &clone
}

// As extracts a *Error from an error chain, or nil.
func As(err error) *Error {
	var ge *Error
	if errors.As(err, &ge) {
		return ge
	}
	return nil
}

// Internal wraps an unexpected error so it never escapes untyped.
func Internal(cause error) *Error {
	return &Error{
		Code:      CodeInternal,
		Message:   "Gateway encountered an internal error.",
		Retryable: false,
		Cause:     cause,
	}
}

// Common constructors used across adapters. Kept here so that retry semantics
// for a given failure class are decided in exactly one place.

func Timeout(serviceID string) *Error {
	return &Error{
		Code: CodeBackendTimeout, Message: "Backend did not respond within timeout.",
		// A timeout leaves the attempt's outcome unknown. It is only
		// fallback-safe for operations the router has proven idempotent;
		// the router applies that gate, not this constructor.
		Retryable: true, FallbackSafe: false, ServiceID: serviceID,
	}
}

func Unavailable(serviceID string, cause error) *Error {
	return &Error{
		Code: CodeBackendUnavailable, Message: "Backend is unavailable.",
		Retryable: true, FallbackSafe: true, ServiceID: serviceID, Cause: cause,
	}
}

// ConnectFailed means the gateway never got bytes onto the wire, so the backend
// provably did nothing: always fallback-safe.
func ConnectFailed(serviceID string, cause error) *Error {
	return &Error{
		Code: CodeConnectionFailed, Message: "Could not establish a usable connection to the backend.",
		Retryable: true, FallbackSafe: true, ServiceID: serviceID, Cause: cause,
	}
}

// ProtocolViolation means the backend answered but the answer is unusable. The
// backend already did the work, so a different backend redoing it is only safe
// for idempotent operations -- again gated by the router.
func ProtocolViolation(serviceID, detail string) *Error {
	return &Error{
		Code: CodeProtocolViolation, Message: "Backend response failed protocol validation: " + detail,
		Retryable: true, FallbackSafe: false, ServiceID: serviceID,
	}
}

func Checksum(serviceID string) *Error {
	return &Error{
		Code: CodeChecksumMismatch, Message: "Backend response failed checksum validation.",
		Retryable: true, FallbackSafe: false, ServiceID: serviceID,
	}
}

func Correlation(serviceID, detail string) *Error {
	return &Error{
		Code: CodeCorrelationMismatch, Message: "Backend response could not be correlated: " + detail,
		Retryable: true, FallbackSafe: false, ServiceID: serviceID,
	}
}

// UnsupportedVersion is raised when a backend *answers* under a protocol
// version the bound adapter does not speak.
//
// It is not retryable: the disagreement is static, so asking the same backend
// again cannot change it. It is also not fallback-safe, and that is deliberate
// even though it looks conservative. The backend did answer, which means it
// already ran the operation -- the reference environment's execution ledger
// confirms this for the unsupported-version fault. So this belongs with the
// other "backend finished, but the answer is unusable" failures, and the router
// treats it under the same corrupt-response policy rather than claiming a
// safety it cannot prove.
func UnsupportedVersion(serviceID string, got any) *Error {
	return &Error{
		Code: CodeUnsupportedVersion, Message: fmt.Sprintf("Backend protocol version %v is not supported by the bound adapter.", got),
		Retryable: false, FallbackSafe: false, ServiceID: serviceID,
	}
}
