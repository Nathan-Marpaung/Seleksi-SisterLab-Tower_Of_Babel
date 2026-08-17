package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"babel/gateway/internal/gwerr"
	"babel/gateway/internal/transport/udprel"
)

type udpAdapter struct {
	spec   *Spec
	addr   string
	magic  [2]byte
	socket *udprel.Socket

	// seq numbers requests on this socket. It is independent of the
	// correlation id so that a fragmented payload can carry a run of
	// sequences under one identifier.
	seq atomic.Uint32

	requests atomic.Int64
	failures atomic.Int64
}

// UDPOptions are the policy knobs for the datagram transport.
type UDPOptions struct {
	Window     int
	InitialRTO time.Duration
	MinRTO     time.Duration
	MaxRTO     time.Duration
	MaxRetries int
	Retransmit bool
	Fragment   bool
	OnEvent    func(event string, fields map[string]any)
}

func newUDPAdapter(spec *Spec, endpoint string, opt UDPOptions) (Adapter, error) {
	magic, err := parseMagic(spec.Wire.MagicHex)
	if err != nil {
		return nil, err
	}
	sock, err := udprel.Dial(udprel.Options{
		Addr:       endpoint,
		Magic:      magic,
		Version:    byte(spec.Version),
		MaxPayload: spec.Wire.MaxPayload,
		Window:     opt.Window,
		InitialRTO: opt.InitialRTO,
		MinRTO:     opt.MinRTO,
		MaxRTO:     opt.MaxRTO,
		MaxRetries: opt.MaxRetries,
		Retransmit: opt.Retransmit,
		Fragment:   opt.Fragment,
		OnEvent:    opt.OnEvent,
	})
	if err != nil {
		return nil, err
	}
	return &udpAdapter{spec: spec, addr: endpoint, magic: magic, socket: sock}, nil
}

func (a *udpAdapter) Name() string           { return a.spec.Name }
func (a *udpAdapter) ServiceID() string      { return a.spec.ServiceID }
func (a *udpAdapter) Family() string         { return FamilyUDPCRCJSON }
func (a *udpAdapter) Version() int           { return a.spec.Version }
func (a *udpAdapter) Capabilities() []string { return a.spec.Capabilities() }
func (a *udpAdapter) Close()                 { a.socket.Close() }

func (a *udpAdapter) Stats() map[string]int64 {
	out := a.socket.Stats()
	out["requests"] = a.requests.Load()
	out["failures"] = a.failures.Load()
	return out
}

func (a *udpAdapter) Supports(operation string) bool {
	_, ok := a.spec.Op(operation)
	return ok
}

// EncodeRequest produces the full datagram including its trailing CRC32.
func (a *udpAdapter) EncodeRequest(call Call) ([]byte, *gwerr.Error) {
	p, e := a.packet(call)
	if e != nil {
		return nil, e
	}
	return udprel.Encode(a.magic, byte(a.spec.Version), p), nil
}

func (a *udpAdapter) packet(call Call) (udprel.Packet, *gwerr.Error) {
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return udprel.Packet{}, errUnsupported(a.spec.ServiceID, call.Operation)
	}
	body := buildArguments(op, call.Arguments)
	if a.spec.Wire.ArgumentsField != "" {
		body = map[string]any{a.spec.Wire.ArgumentsField: body}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return udprel.Packet{}, gwerr.Internal(err)
	}
	if len(raw) > a.spec.Wire.MaxPayload {
		// Refused before a byte is sent, so the router is free to pick the
		// stream-oriented backend instead. Reporting this as a distinct code
		// rather than a generic failure is what makes that routing decision
		// possible.
		return udprel.Packet{}, gwerr.Newf(gwerr.CodePayloadTooLarge, false, true,
			"request payload of %d bytes exceeds the %d byte datagram limit", len(raw), a.spec.Wire.MaxPayload).
			WithService(a.spec.ServiceID)
	}
	return udprel.Packet{
		Type:      udprel.TypeRequest,
		Seq:       call.Seq,
		RequestID: call.CorrelationID,
		OpCode:    byte(op.OpCode),
		Payload:   raw,
	}, nil
}

// DecodeResponse validates a complete datagram, checksum included.
func (a *udpAdapter) DecodeResponse(raw []byte, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(svc, call.Operation)
	}
	p, err := udprel.Decode(a.magic, byte(a.spec.Version), raw)
	if err != nil {
		switch {
		case udprel.IsChecksumError(err):
			return Reply{}, gwerr.Checksum(svc).Wrap(err)
		case udprel.IsVersionError(err):
			return Reply{}, gwerr.UnsupportedVersion(svc, raw[2]).Wrap(err)
		default:
			return Reply{}, gwerr.ProtocolViolation(svc, err.Error())
		}
	}
	if p.RequestID != call.CorrelationID {
		return Reply{}, gwerr.Correlation(svc, "datagram request id does not match the request")
	}
	return a.interpret(op, p, call)
}

func (a *udpAdapter) interpret(op OpSpec, p udprel.Packet, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID
	switch p.Type {
	case udprel.TypeResponse, udprel.TypeError:
		// Both carry the same payload shape; which one it is is decided by the
		// payload's error field, so a backend that mislabels the type still
		// produces a correct envelope.
	case udprel.TypeAck:
		return Reply{}, gwerr.ProtocolViolation(svc, "backend sent a bare acknowledgement instead of a response")
	default:
		return Reply{}, gwerr.ProtocolViolation(svc, "unexpected datagram message type")
	}
	result, e := decodePayload(a.spec, op, p.Payload, nil)
	if e != nil {
		return Reply{}, e
	}
	return Reply{ServiceID: svc, Result: result, Version: a.spec.Version}, nil
}

func (a *udpAdapter) Execute(ctx context.Context, call Call) (Reply, *gwerr.Error) {
	svc := a.spec.ServiceID
	op, ok := a.spec.Op(call.Operation)
	if !ok {
		return Reply{}, errUnsupported(svc, call.Operation)
	}
	a.requests.Add(1)

	if call.Seq == 0 {
		call.Seq = a.seq.Add(1)
	}
	p, e := a.packet(call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}

	resp, err := a.socket.Do(ctx, p)
	if err != nil {
		a.failures.Add(1)
		return Reply{}, a.classifyTransport(ctx, err)
	}

	reply, e := a.interpret(op, resp, call)
	if e != nil {
		a.failures.Add(1)
		return Reply{}, e
	}
	return reply, nil
}

func (a *udpAdapter) classifyTransport(ctx context.Context, err error) *gwerr.Error {
	svc := a.spec.ServiceID
	switch {
	case errors.Is(err, udprel.ErrTimeout), errors.Is(err, context.DeadlineExceeded),
		ctx.Err() == context.DeadlineExceeded:
		// The retry budget is already spent inside the transport, so by the
		// time this surfaces the datagram has been sent several times without
		// an answer. Whether the backend executed it is unknown.
		return gwerr.Timeout(svc).Wrap(err)
	case errors.Is(err, udprel.ErrWindowFull):
		// Backpressure. Nothing was sent, so this is safe to route elsewhere.
		return gwerr.New(gwerr.CodeBackendUnavailable,
			"Backend datagram window is saturated.", true, true).WithService(svc).Wrap(err)
	case errors.Is(err, udprel.ErrOversize):
		return gwerr.New(gwerr.CodePayloadTooLarge,
			"Payload exceeds the datagram limit.", false, true).WithService(svc).Wrap(err)
	case errors.Is(err, udprel.ErrClosed):
		return gwerr.New(gwerr.CodeGatewayShutdown, "Gateway is shutting down.", true, true).
			WithService(svc).Wrap(err)
	case errors.Is(err, udprel.ErrWriteFailed):
		return gwerr.ConnectFailed(svc, err)
	default:
		return gwerr.Unavailable(svc, err)
	}
}

// ProbeIsIntrusive reports that checking this backend costs a real operation,
// which the health monitor uses to probe it sparingly.
func (a *udpAdapter) ProbeIsIntrusive() bool { return true }

// Probe checks liveness with the backend's own metadata operation.
//
// A datagram socket offers no handshake, so there is no transport-level signal
// to observe -- the only way to learn whether the backend is answering is to
// ask it something. Metadata is chosen because it is the cheapest capability,
// takes no arguments, and reads rather than computes. The probe carries its own
// correlation id, so it can never be confused with client traffic.
func (a *udpAdapter) Probe(ctx context.Context) error {
	op, ok := a.spec.Op("metadata")
	if !ok {
		return ErrProbeUnsupported
	}
	p := udprel.Packet{
		Type: udprel.TypeRequest,
		// A probe uses a fresh sequence and a correlation id drawn from the
		// same generator as real traffic, supplied by the health monitor.
		Seq:       a.seq.Add(1),
		RequestID: probeCorrelation(ctx),
		OpCode:    byte(op.OpCode),
		Payload:   []byte(`{}`),
	}
	if p.RequestID == 0 {
		return ErrProbeUnsupported
	}
	resp, err := a.socket.Do(ctx, p)
	if err != nil {
		return err
	}
	if resp.Type != udprel.TypeResponse {
		return errors.New("metadata probe returned a non-response datagram")
	}
	return nil
}
