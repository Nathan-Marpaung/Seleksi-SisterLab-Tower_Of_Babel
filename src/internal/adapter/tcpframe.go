package adapter

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"babel/gateway/internal/gwerr"
	"babel/gateway/internal/transport/tcpmux"
)

type tcpAdapter struct {
	spec  *Spec
	addr  string
	magic [2]byte
	pool  *tcpmux.Pool

	requests atomic.Int64
	failures atomic.Int64
}

// TCPOptions are the transport knobs the manager passes down from policy
// configuration; the spec owns protocol shape, policy owns resource limits.
type TCPOptions struct {
	PoolSize         int
	MaxInFlight      int
	DialTimeout      time.Duration
	FrameBodyTimeout time.Duration
	OnEvent          func(event string, fields map[string]any)
}

func newTCPAdapter(spec *Spec, endpoint string, opt TCPOptions) (Adapter, error) {
	magic, err := parseMagic(spec.Wire.MagicHex)
	if err != nil {
		return nil, err
	}
	a := &tcpAdapter{spec: spec, addr: endpoint, magic: magic}
	a.pool = tcpmux.New(tcpmux.Options{
		Addr:             endpoint,
		Magic:            magic,
		Version:          byte(spec.Version),
		MaxPayload:       uint32(spec.Wire.MaxPayload),
		PoolSize:         opt.PoolSize,
		MaxInFlight:      opt.MaxInFlight,
		DialTimeout:      opt.DialTimeout,
		FrameBodyTimeout: opt.FrameBodyTimeout,
		OnEvent:          opt.OnEvent,
	})
	return a, nil
}

func (a *tcpAdapter) Name() string            { return a.spec.Name }
func (a *tcpAdapter) ServiceID() string       { return a.spec.ServiceID }
func (a *tcpAdapter) Family() string          { return FamilyTCPFrameJSON }
func (a *tcpAdapter) Version() int            { return a.spec.Version }
func (a *tcpAdapter) Capabilities() []string  { return a.spec.Capabilities() }
func (a *tcpAdapter) Stats() map[string]int64 { return a.mergedStats() }
func (a *tcpAdapter) Close()                  { a.pool.Close() }

func (a *tcpAdapter) mergedStats() map[string]int64 {
	out := a.pool.Stats()
	out["requests"] = a.requests.Load()
	out["failures"] = a.failures.Load()
	return out
}

func (a *tcpAdapter) Supports(operation string) bool {
	_, ok := a.spec.Op(operation)
	return ok
}

// EncodeRequest produces the complete frame -- header and payload -- so that a
// golden vector pins the framing, not just the JSON.
func (a *tcpAdapter) EncodeRequest(call Call) ([]byte, *gwerr.Error) {
	payload, e := a.encodePayload(call)
	if e != nil {
		return nil, e
	}
	buf := make([]byte, tcpmux.HeaderSize+len(payload))
	buf[0], buf[1] = a.magic[0], a.magic[1]
	buf[2] = byte(a.spec.Version)
	buf[3] = 0
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[8:16], call.CorrelationID)
	copy(buf[tcpmux.HeaderSize:], payload)
	return buf, nil
}

func (a *tcpAdapter) encodePayload(call Call) ([]byte, *gwerr.Error) {
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return nil, errUnsupported(a.spec.ServiceID, call.Operation)
	}
	body := map[string]any{a.spec.Wire.OperationField: op.Wire}
	args := buildArguments(op, call.Arguments)
	if a.spec.Wire.ArgumentsField != "" {
		body[a.spec.Wire.ArgumentsField] = args
	} else {
		for k, v := range args {
			body[k] = v
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, gwerr.Internal(err)
	}
	if len(raw) > a.spec.Wire.MaxPayload {
		return nil, gwerr.Newf(gwerr.CodePayloadTooLarge, false, true,
			"request payload of %d bytes exceeds the %d byte frame limit", len(raw), a.spec.Wire.MaxPayload).
			WithService(a.spec.ServiceID)
	}
	return raw, nil
}

// DecodeResponse parses a complete frame. It re-validates the header even
// though the transport already did, because the codec must be provable on its
// own: golden vectors feed it bytes the transport never saw.
func (a *tcpAdapter) DecodeResponse(raw []byte, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(svc, call.Operation)
	}
	if len(raw) < tcpmux.HeaderSize {
		return Reply{}, gwerr.ProtocolViolation(svc, "frame is shorter than its header")
	}
	if raw[0] != a.magic[0] || raw[1] != a.magic[1] {
		return Reply{}, gwerr.ProtocolViolation(svc, "frame magic does not match")
	}
	if int(raw[2]) != a.spec.Version {
		return Reply{}, gwerr.UnsupportedVersion(svc, raw[2])
	}
	length := binary.BigEndian.Uint32(raw[4:8])
	if int(length) > a.spec.Wire.MaxPayload {
		return Reply{}, gwerr.ProtocolViolation(svc, "declared frame length exceeds the protocol maximum")
	}
	if tcpmux.HeaderSize+int(length) != len(raw) {
		return Reply{}, gwerr.ProtocolViolation(svc, "declared frame length does not match the frame size")
	}
	if id := binary.BigEndian.Uint64(raw[8:16]); id != call.CorrelationID {
		return Reply{}, gwerr.Correlation(svc, "frame request id does not match the request")
	}
	return a.decodeFramePayload(op, raw[tcpmux.HeaderSize:], call)
}

func (a *tcpAdapter) decodeFramePayload(op OpSpec, payload []byte, call Call) (Reply, *gwerr.Error) {
	result, e := decodePayload(a.spec, op, payload, call.CorrelationID)
	if e != nil {
		return Reply{}, e
	}
	return Reply{ServiceID: a.spec.ServiceID, Result: result, Version: a.spec.Version}, nil
}

func (a *tcpAdapter) Execute(ctx context.Context, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(svc, call.Operation)
	}
	a.requests.Add(1)

	payload, e := a.encodePayload(call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}

	resp, err := a.pool.Do(ctx, tcpmux.Frame{RequestID: call.CorrelationID, Payload: payload})
	if err != nil {
		a.failures.Add(1)
		return Reply{}, a.classifyTransport(ctx, err)
	}

	reply, e := a.decodeFramePayload(op, resp.Payload, call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}
	return reply, nil
}

// classifyTransport maps transport outcomes onto gateway semantics.
func (a *tcpAdapter) classifyTransport(ctx context.Context, err error) *gwerr.Error {
	svc := a.spec.ServiceID
	switch {
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded:
		return gwerr.Timeout(svc).Wrap(err)
	case errors.Is(err, context.Canceled):
		return gwerr.New(gwerr.CodeGatewayShutdown, "Request was cancelled before completion.", true, false).
			WithService(svc).Wrap(err)
	case errors.Is(err, tcpmux.ErrDial):
		// Nothing was written, so another backend may safely run the work.
		return gwerr.ConnectFailed(svc, err)
	case errors.Is(err, tcpmux.ErrOversize):
		return gwerr.New(gwerr.CodePayloadTooLarge, "Payload exceeds the frame limit.", false, true).
			WithService(svc).Wrap(err)
	case errors.Is(err, tcpmux.ErrVersionMismatch):
		// Not fallback-safe for the same reason as gwerr.UnsupportedVersion:
		// the backend answered, so it already ran the operation.
		return gwerr.New(gwerr.CodeUnsupportedVersion,
			"Backend answered with a protocol version the bound adapter does not speak.",
			false, false).WithService(svc).Wrap(err)
	case errors.Is(err, tcpmux.ErrCorruptFrame):
		return gwerr.ProtocolViolation(svc, "frame failed header validation").Wrap(err)
	case errors.Is(err, tcpmux.ErrTruncated):
		return gwerr.ProtocolViolation(svc, "frame payload was truncated").Wrap(err)
	case errors.Is(err, tcpmux.ErrPoolClosed):
		return gwerr.New(gwerr.CodeGatewayShutdown, "Gateway is shutting down.", true, true).
			WithService(svc).Wrap(err)
	case errors.Is(err, tcpmux.ErrWrite), errors.Is(err, tcpmux.ErrClosed):
		// The frame may or may not have reached the backend before the stream
		// broke, so this is retryable but not provably safe to run elsewhere.
		return gwerr.New(gwerr.CodeConnectionFailed,
			"Connection to the backend broke before a response arrived.", true, false).
			WithService(svc).Wrap(err)
	default:
		return gwerr.Unavailable(svc, err)
	}
}

// Probe checks liveness by opening and immediately closing a connection.
//
// A TCP handshake is the strongest signal available without sending a frame,
// and it deliberately executes nothing: a probe must not add entries to the
// backend's execution ledger, or health checking would be indistinguishable
// from traffic.
func (a *tcpAdapter) Probe(ctx context.Context) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", a.addr)
	if err != nil {
		return err
	}
	return conn.Close()
}
