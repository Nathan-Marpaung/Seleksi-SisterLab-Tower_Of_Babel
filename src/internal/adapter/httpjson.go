package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"babel/gateway/internal/gwerr"
)

// maxHTTPBody caps how much of a backend response the gateway will read. A
// backend that streams without end must not be able to grow gateway memory.
const maxHTTPBody = 4 << 20

type httpAdapter struct {
	spec   *Spec
	base   string
	client *http.Client

	requests atomic.Int64
	failures atomic.Int64
}

func newHTTPAdapter(spec *Spec, endpoint string) (Adapter, error) {
	base := strings.TrimRight(endpoint, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return &httpAdapter{
		spec: spec,
		base: base,
		client: &http.Client{
			// No client-level timeout: the per-attempt deadline lives on the
			// context, so the router owns the whole budget in one place.
			Transport: &http.Transport{
				Proxy:               nil, // never route backend calls through a proxy
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 32,
				MaxConnsPerHost:     128,
				IdleConnTimeout:     30 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   2 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2: false,
			},
		},
	}, nil
}

func (a *httpAdapter) Name() string      { return a.spec.Name }
func (a *httpAdapter) ServiceID() string { return a.spec.ServiceID }
func (a *httpAdapter) Family() string    { return FamilyHTTPJSON }
func (a *httpAdapter) Version() int      { return a.spec.Version }

func (a *httpAdapter) Supports(operation string) bool {
	_, ok := a.spec.Op(operation)
	return ok
}

func (a *httpAdapter) Capabilities() []string { return a.spec.Capabilities() }

func (a *httpAdapter) Stats() map[string]int64 {
	return map[string]int64{
		"requests": a.requests.Load(),
		"failures": a.failures.Load(),
	}
}

func (a *httpAdapter) Close() {
	a.client.CloseIdleConnections()
}

// EncodeRequest builds the JSON body. Exposed as part of Codec so golden
// vectors can pin the exact translation without a network.
func (a *httpAdapter) EncodeRequest(call Call) ([]byte, *gwerr.Error) {
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return nil, errUnsupported(a.spec.ServiceID, call.Operation)
	}
	body := map[string]any{
		a.spec.Wire.OperationField: op.Wire,
		a.spec.Wire.ArgumentsField: buildArguments(op, call.Arguments),
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, gwerr.Internal(err)
	}
	return raw, nil
}

// DecodeResponse validates and normalizes a JSON body.
func (a *httpAdapter) DecodeResponse(raw []byte, call Call) (Reply, *gwerr.Error) {
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(a.spec.ServiceID, call.Operation)
	}
	result, e := decodePayload(a.spec, op, raw, correlationToken(call.CorrelationID))
	if e != nil {
		return Reply{}, e
	}
	return Reply{ServiceID: a.spec.ServiceID, Result: result, Version: a.spec.Version}, nil
}

func (a *httpAdapter) Execute(ctx context.Context, call Call) (Reply, *gwerr.Error) {
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(a.spec.ServiceID, call.Operation)
	}
	a.requests.Add(1)

	body, e := a.EncodeRequest(call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}

	url := a.base + a.spec.Wire.ExecutePath
	method := a.spec.Wire.Method
	if method == "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		a.failures.Add(1)
		return Reply{}, gwerr.Internal(err).WithService(a.spec.ServiceID)
	}
	for k, v := range a.spec.Wire.Headers {
		req.Header.Set(k, v)
	}
	if a.spec.Wire.VersionHeader != "" {
		req.Header.Set(a.spec.Wire.VersionHeader, strconv.Itoa(a.spec.Version))
	}
	if a.spec.Wire.RequestIDHeader != "" {
		// The backend correlation id is the gateway's own, never the client's:
		// clients may repeat identifiers, and a repeated one would let a stale
		// response satisfy a live request.
		req.Header.Set(a.spec.Wire.RequestIDHeader, correlationToken(call.CorrelationID))
	}

	resp, err := a.client.Do(req)
	if err != nil {
		a.failures.Add(1)
		return Reply{}, a.classifyTransport(ctx, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPBody))
	if err != nil {
		a.failures.Add(1)
		return Reply{}, a.classifyTransport(ctx, err)
	}

	// Protocol version is validated on the response, not just asserted on the
	// request. A backend that answers under a version the bound adapter does
	// not speak has produced a payload whose meaning the gateway cannot
	// guarantee, so it must not be forwarded merely because it happens to
	// parse. This is the HTTP counterpart of the version byte the framed and
	// datagram families carry in their headers.
	if e := a.checkResponseVersion(resp); e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}

	reply, e := a.interpret(resp.StatusCode, raw, op, call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}
	return reply, nil
}

func (a *httpAdapter) checkResponseVersion(resp *http.Response) *gwerr.Error {
	header := a.spec.Wire.VersionHeader
	if header == "" {
		return nil
	}
	got := strings.TrimSpace(resp.Header.Get(header))
	if got == "" {
		// Absent is tolerated: the protocol document does not require the
		// header on responses, and inventing a failure for a silent backend
		// would break conformant implementations.
		return nil
	}
	if n, err := strconv.Atoi(got); err != nil || n != a.spec.Version {
		return gwerr.UnsupportedVersion(a.spec.ServiceID, got)
	}
	return nil
}

// interpret combines HTTP status with payload contents.
//
// The payload is authoritative whenever it parses: the backend's own error code
// carries more information than a status class. The status is the fallback for
// bodies that are missing, truncated or not JSON at all -- which is exactly
// what the invalid-json fault produces, and where a naive gateway would either
// crash or forward garbage.
func (a *httpAdapter) interpret(status int, raw []byte, op OpSpec, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID

	if status >= 200 && status < 300 {
		result, e := decodePayload(a.spec, op, raw, correlationToken(call.CorrelationID))
		if e != nil {
			return Reply{}, e
		}
		return Reply{ServiceID: svc, Result: result, Version: a.spec.Version}, nil
	}

	// Non-2xx: try the structured error first.
	if _, e := decodePayload(a.spec, op, raw, nil); e != nil && !isDecodeFailure(e) {
		return Reply{}, statusOverlay(e, status)
	}
	return Reply{}, a.statusError(status, raw)
}

// isDecodeFailure distinguishes "the body was unparseable" from "the body was a
// well-formed backend error".
func isDecodeFailure(e *gwerr.Error) bool {
	switch e.Code {
	case gwerr.CodeProtocolViolation, gwerr.CodeCorrelationMismatch, gwerr.CodeInternal:
		return true
	}
	return false
}

// statusOverlay lets an unambiguous status class upgrade a backend error's
// retryability -- a 503 is retryable even if the body forgot to say so.
func statusOverlay(e *gwerr.Error, status int) *gwerr.Error {
	switch status {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable,
		http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusInternalServerError:
		clone := *e
		clone.Retryable = true
		clone.FallbackSafe = true
		return &clone
	}
	return e
}

func (a *httpAdapter) statusError(status int, raw []byte) *gwerr.Error {
	svc := a.spec.ServiceID
	snippet := strings.TrimSpace(string(raw))
	if len(snippet) > 160 {
		snippet = snippet[:160] + "..."
	}

	switch {
	case status == http.StatusTooManyRequests:
		return gwerr.New(gwerr.CodeRateLimited, "Backend is rate limiting.", true, true).WithService(svc)
	case status == http.StatusUnsupportedMediaType:
		return gwerr.New(gwerr.CodeProtocolViolation,
			"Backend rejected the request content type.", false, true).WithService(svc)
	case status >= 500:
		return gwerr.Newf(gwerr.CodeBackendUnavailable, true, true,
			"Backend returned HTTP %d.", status).WithService(svc)
	default:
		return gwerr.Newf(gwerr.CodeBackendError, false, true,
			"Backend returned HTTP %d with an unreadable body: %s", status, snippet).WithService(svc)
	}
}

// classifyTransport turns a Go transport error into a gateway error with the
// right retry semantics.
//
// The distinction that matters is whether the backend might already have done
// the work. A timeout leaves that unknown, so it is retryable but NOT
// fallback-safe. A refused or reset connection means no response was produced;
// the reference environment's ledger confirms terminated connections never
// reach the execution stage, so those are fallback-safe.
func (a *httpAdapter) classifyTransport(ctx context.Context, err error) *gwerr.Error {
	svc := a.spec.ServiceID

	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
		return gwerr.Timeout(svc).Wrap(err)
	}
	if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
		return gwerr.New(gwerr.CodeGatewayShutdown, "Request was cancelled before completion.", true, false).
			WithService(svc).Wrap(err)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return gwerr.Timeout(svc).Wrap(err)
	}
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return gwerr.ConnectFailed(svc, err)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "EOF") {
		return gwerr.Unavailable(svc, err)
	}
	return gwerr.Unavailable(svc, err)
}

// Probe is the liveness check: a dedicated health endpoint, so no business
// operation is executed and nothing lands in the backend's execution ledger.
func (a *httpAdapter) Probe(ctx context.Context) error {
	path := a.spec.Wire.HealthPath
	if path == "" {
		return ErrProbeUnsupported
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health endpoint returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// correlationToken renders a correlation id for protocols that carry it as a
// string. One rendering, used on both the request and the comparison, so the
// two can never disagree.
func correlationToken(id uint64) string { return strconv.FormatUint(id, 10) }
