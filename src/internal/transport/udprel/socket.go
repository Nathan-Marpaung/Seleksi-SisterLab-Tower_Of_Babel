// Package udprel is a reliability layer over the CRC32-checked datagram
// protocol spoken by Service C.
//
// UDP gives the gateway no ordering, no delivery guarantee, no duplicate
// suppression and no integrity beyond what the protocol's own trailing CRC32
// provides. Everything the request path needs is therefore built here:
//
//   - correlation by (request id, sequence), never by arrival order, so a
//     reordered datagram is matched correctly instead of mispaired;
//   - duplicate suppression, so a datagram delivered twice yields exactly one
//     client-visible response;
//   - integrity validation before a payload is ever parsed -- a bad magic,
//     version, length or checksum makes the datagram disappear rather than
//     become an error, because on a lossy link a corrupt datagram is
//     indistinguishable from one that never arrived;
//   - adaptive retransmission with a Jacobson/Karels RTO estimator, so the
//     timeout tracks the path instead of being a guess;
//   - a bounded in-flight window, which is the backpressure signal that keeps a
//     slow backend from turning into unbounded gateway memory.
//
// Retransmission is at-least-once by nature: if a *response* was lost, the
// backend has already executed the operation and will execute it again. That is
// an accepted, documented property of this transport, and it is why the router
// never adds a cross-backend fallback on top of a UDP timeout -- doing both
// would compound the duplication rather than contain it.
package udprel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// HeaderSize is the fixed datagram header length; ChecksumSize trails the
// payload.
const (
	HeaderSize   = 20
	ChecksumSize = 4
)

// Message types.
const (
	TypeRequest  byte = 1
	TypeResponse byte = 2
	TypeError    byte = 3
	TypeAck      byte = 4
)

// FlagMoreFragments marks a datagram as a non-final fragment of a larger
// payload. The reference backend never sets it, but the receive path honours it
// so an upgraded backend needs no gateway change.
const FlagMoreFragments byte = 0x01

var (
	ErrTimeout     = errors.New("udprel: no response within deadline")
	ErrClosed      = errors.New("udprel: socket closed")
	ErrOversize    = errors.New("udprel: payload exceeds protocol maximum")
	ErrWindowFull  = errors.New("udprel: in-flight window is full")
	ErrWriteFailed = errors.New("udprel: datagram write failed")
)

// Packet is one protocol datagram at the application level.
type Packet struct {
	Type      byte
	Seq       uint32
	RequestID uint64
	OpCode    byte
	Flags     byte
	Payload   []byte
}

// Options configures a socket.
type Options struct {
	Addr       string
	Magic      [2]byte
	Version    byte
	MaxPayload int

	// Window bounds concurrent in-flight requests.
	Window int

	InitialRTO time.Duration
	MinRTO     time.Duration
	MaxRTO     time.Duration
	MaxRetries int
	// Retransmit enables ARQ. Disabling it makes the transport at-most-once at
	// the cost of failing on the first lost datagram.
	Retransmit bool
	// Fragment enables send-side fragmentation of oversized payloads. Off for
	// the reference backend, which would reject a fragmented request; with it
	// off an oversized payload is refused up front so the router can pick a
	// stream-oriented backend instead.
	Fragment bool

	OnEvent func(event string, fields map[string]any)
}

// Socket is a single connected UDP endpoint shared by all requests to one
// backend. One socket with one reader is deliberate: a socket per request would
// burn an ephemeral port per call and lose the shared RTT estimate.
type Socket struct {
	opt  Options
	conn *net.UDPConn

	sem chan struct{}

	mu      sync.Mutex
	pending map[uint64]*call
	// recent remembers identifiers whose request already completed, so a
	// duplicate that arrives after the waiter is gone is recognised as a
	// duplicate rather than misreported as traffic for an unknown request. It
	// is bounded and time-limited: this is observability, not correctness --
	// correctness comes from there being no waiter to deliver to.
	recent map[uint64]time.Time
	closed bool

	rttMu  sync.Mutex
	srtt   time.Duration
	rttvar time.Duration
	rto    time.Duration

	sent        atomic.Int64
	retransmits atomic.Int64
	received    atomic.Int64
	dropped     atomic.Int64
	badChecksum atomic.Int64
	badHeader   atomic.Int64
	duplicates  atomic.Int64
	unsolicited atomic.Int64
	reassembled atomic.Int64
	timeouts    atomic.Int64
}

type call struct {
	requestID uint64
	baseSeq   uint32
	done      chan Packet

	mu        sync.Mutex
	delivered bool
	// seen suppresses duplicate datagrams for this request.
	seen map[uint32]bool
	// frags holds out-of-order fragments until the run is complete.
	frags map[uint32][]byte
	// terminator is the sequence of the fragment without MORE_FRAGMENTS.
	terminator    uint32
	hasTerminator bool
	template      Packet
}

// Dial creates the socket and starts its reader.
func Dial(opt Options) (*Socket, error) {
	if opt.MaxPayload <= 0 {
		opt.MaxPayload = 4096
	}
	if opt.Window < 1 {
		opt.Window = 64
	}
	if opt.InitialRTO <= 0 {
		opt.InitialRTO = 150 * time.Millisecond
	}
	if opt.MinRTO <= 0 {
		opt.MinRTO = 40 * time.Millisecond
	}
	if opt.MaxRTO <= 0 {
		opt.MaxRTO = time.Second
	}
	if opt.MaxRetries < 0 {
		opt.MaxRetries = 0
	}

	addr, err := net.ResolveUDPAddr("udp", opt.Addr)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", opt.Addr, err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", opt.Addr, err)
	}

	s := &Socket{
		opt:     opt,
		conn:    conn,
		sem:     make(chan struct{}, opt.Window),
		pending: map[uint64]*call{},
		recent:  map[uint64]time.Time{},
		rto:     opt.InitialRTO,
	}
	go s.readLoop()
	return s, nil
}

func (s *Socket) event(name string, fields map[string]any) {
	if s.opt.OnEvent != nil {
		s.opt.OnEvent(name, fields)
	}
}

// Stats reports transport counters for observability.
func (s *Socket) Stats() map[string]int64 {
	s.rttMu.Lock()
	rto := s.rto
	srtt := s.srtt
	s.rttMu.Unlock()
	s.mu.Lock()
	inflight := len(s.pending)
	s.mu.Unlock()
	return map[string]int64{
		"datagrams_sent":        s.sent.Load(),
		"datagrams_received":    s.received.Load(),
		"retransmits":           s.retransmits.Load(),
		"dropped":               s.dropped.Load(),
		"checksum_failures":     s.badChecksum.Load(),
		"header_failures":       s.badHeader.Load(),
		"duplicates_suppressed": s.duplicates.Load(),
		"unsolicited":           s.unsolicited.Load(),
		"reassembled_payloads":  s.reassembled.Load(),
		"timeouts":              s.timeouts.Load(),
		"in_flight":             int64(inflight),
		"window":                int64(s.opt.Window),
		"rto_ms":                rto.Milliseconds(),
		"srtt_ms":               srtt.Milliseconds(),
	}
}

// Do sends a request and waits for its correlated response, retransmitting on
// the adaptive RTO until the context deadline or the retry budget is exhausted.
func (s *Socket) Do(ctx context.Context, p Packet) (Packet, error) {
	fragments, err := s.fragment(p)
	if err != nil {
		return Packet{}, err
	}

	// Backpressure: block for a window slot, but never past the caller's
	// deadline. Failing fast here is honest -- the request has not been sent,
	// so it is safe for the router to try elsewhere.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return Packet{}, ErrWindowFull
	}

	c := &call{
		requestID: p.RequestID,
		baseSeq:   p.Seq,
		done:      make(chan Packet, 1),
		seen:      map[uint32]bool{},
		frags:     map[uint32][]byte{},
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Packet{}, ErrClosed
	}
	if _, exists := s.pending[p.RequestID]; exists {
		s.mu.Unlock()
		return Packet{}, fmt.Errorf("udprel: request id %d is already in flight", p.RequestID)
	}
	s.pending[p.RequestID] = c
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.pending, p.RequestID)
		s.noteCompletedLocked(p.RequestID)
		s.mu.Unlock()
	}()

	sentAt := time.Now()
	if err := s.writeAll(fragments); err != nil {
		return Packet{}, err
	}

	attempts := 0
	timer := time.NewTimer(s.currentRTO())
	defer timer.Stop()

	for {
		select {
		case resp := <-c.done:
			// Karn's algorithm: only sample RTT from a transmission that was
			// never retransmitted, otherwise the sample may belong to an
			// earlier copy and would poison the estimator.
			if attempts == 0 {
				s.observeRTT(time.Since(sentAt))
			}
			return resp, nil

		case <-ctx.Done():
			s.timeouts.Add(1)
			return Packet{}, ErrTimeout

		case <-timer.C:
			if !s.opt.Retransmit || attempts >= s.opt.MaxRetries {
				s.timeouts.Add(1)
				return Packet{}, ErrTimeout
			}
			attempts++
			s.retransmits.Add(1)
			s.event("udprel.retransmit", map[string]any{
				"request_id": p.RequestID, "attempt": attempts, "rto_ms": s.currentRTO().Milliseconds(),
			})
			// The retransmission reuses the identical request id and sequence.
			// That is what lets the receive path recognise a second response as
			// a duplicate instead of a second logical answer.
			if err := s.writeAll(fragments); err != nil {
				return Packet{}, err
			}
			timer.Reset(s.backoffRTO(attempts))
		}
	}
}

// fragment splits an oversized payload, or refuses it when send-side
// fragmentation is disabled for this backend.
func (s *Socket) fragment(p Packet) ([]Packet, error) {
	if len(p.Payload) <= s.opt.MaxPayload {
		return []Packet{p}, nil
	}
	if !s.opt.Fragment {
		return nil, fmt.Errorf("%w: %d > %d", ErrOversize, len(p.Payload), s.opt.MaxPayload)
	}
	var out []Packet
	seq := p.Seq
	for off := 0; off < len(p.Payload); off += s.opt.MaxPayload {
		end := off + s.opt.MaxPayload
		if end > len(p.Payload) {
			end = len(p.Payload)
		}
		frag := p
		frag.Seq = seq
		frag.Payload = p.Payload[off:end]
		if end < len(p.Payload) {
			frag.Flags |= FlagMoreFragments
		} else {
			frag.Flags &^= FlagMoreFragments
		}
		out = append(out, frag)
		seq++
	}
	return out, nil
}

func (s *Socket) writeAll(packets []Packet) error {
	for _, p := range packets {
		buf := Encode(s.opt.Magic, s.opt.Version, p)
		if _, err := s.conn.Write(buf); err != nil {
			return fmt.Errorf("%w: %v", ErrWriteFailed, err)
		}
		s.sent.Add(1)
	}
	return nil
}

// currentRTO reads the adaptive timeout.
func (s *Socket) currentRTO() time.Duration {
	s.rttMu.Lock()
	defer s.rttMu.Unlock()
	return s.rto
}

// backoffRTO applies exponential backoff on repeated loss, clamped to MaxRTO.
// Backing off matters on a lossy path: retransmitting at the original rate
// while datagrams are being dropped just adds to the congestion causing them.
func (s *Socket) backoffRTO(attempt int) time.Duration {
	d := s.currentRTO()
	for i := 0; i < attempt && d < s.opt.MaxRTO; i++ {
		d *= 2
	}
	if d > s.opt.MaxRTO {
		d = s.opt.MaxRTO
	}
	return d
}

// observeRTT updates the Jacobson/Karels estimator (RFC 6298 constants).
func (s *Socket) observeRTT(sample time.Duration) {
	s.rttMu.Lock()
	defer s.rttMu.Unlock()
	if s.srtt == 0 {
		s.srtt = sample
		s.rttvar = sample / 2
	} else {
		diff := s.srtt - sample
		if diff < 0 {
			diff = -diff
		}
		s.rttvar = (3*s.rttvar + diff) / 4
		s.srtt = (7*s.srtt + sample) / 8
	}
	rto := s.srtt + 4*s.rttvar
	if rto < s.opt.MinRTO {
		rto = s.opt.MinRTO
	}
	if rto > s.opt.MaxRTO {
		rto = s.opt.MaxRTO
	}
	s.rto = rto
}

// readLoop owns the receive side of the socket.
func (s *Socket) readLoop() {
	// One datagram buffer sized for the worst legal case plus slack, so a
	// hostile oversized datagram is truncated by the kernel rather than
	// growing gateway memory.
	buf := make([]byte, HeaderSize+ChecksumSize+s.opt.MaxPayload+64)
	for {
		n, err := s.conn.Read(buf)
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			// A read error on a connected UDP socket is usually an ICMP port
			// unreachable. Waiters keep their own deadlines, so the loop simply
			// continues rather than tearing down every request.
			s.dropped.Add(1)
			continue
		}
		s.received.Add(1)
		s.dispatch(append([]byte(nil), buf[:n]...))
	}
}

// dispatch validates one received datagram and routes it to its waiter.
//
// Every rejection path here is a silent drop, not an error delivered upward.
// On a datagram transport a corrupt or unexpected packet carries no reliable
// information about any request, so the only safe interpretation is "this never
// arrived" -- and the retransmission timer already covers that case.
func (s *Socket) dispatch(raw []byte) {
	p, err := Decode(s.opt.Magic, s.opt.Version, raw)
	if err != nil {
		switch {
		case errors.Is(err, errChecksum):
			s.badChecksum.Add(1)
			s.event("udprel.checksum_mismatch", map[string]any{"bytes": len(raw)})
		default:
			s.badHeader.Add(1)
			s.event("udprel.header_invalid", map[string]any{"bytes": len(raw), "reason": err.Error()})
		}
		s.dropped.Add(1)
		return
	}

	s.mu.Lock()
	c, ok := s.pending[p.RequestID]
	_, wasRecent := s.recent[p.RequestID]
	s.mu.Unlock()
	if !ok {
		if wasRecent {
			// A second copy of an answer already delivered. Dropping it is the
			// duplicate suppression the protocol requires.
			s.duplicates.Add(1)
			s.event("udprel.duplicate_suppressed", map[string]any{"request_id": p.RequestID, "seq": p.Seq})
			return
		}
		// A late response to an abandoned request, or the backend's
		// uncorrelatable "invalid packet" reply (which carries request id 0).
		s.unsolicited.Add(1)
		s.event("udprel.unsolicited", map[string]any{"request_id": p.RequestID, "seq": p.Seq, "type": p.Type})
		return
	}
	c.offer(s, p)
}

// recentTTL and recentMax bound the completed-identifier memory. A duplicate
// that arrives later than this, or after a burst of unrelated traffic, is
// simply reported as unsolicited -- which costs nothing, because it is dropped
// either way.
const (
	recentTTL = 30 * time.Second
	recentMax = 4096
)

// noteCompletedLocked records a finished identifier. Caller holds s.mu.
func (s *Socket) noteCompletedLocked(id uint64) {
	if len(s.recent) >= recentMax {
		cutoff := time.Now().Add(-recentTTL)
		for k, at := range s.recent {
			if at.Before(cutoff) {
				delete(s.recent, k)
			}
		}
		if len(s.recent) >= recentMax {
			// Still full: drop the whole set rather than grow without bound.
			s.recent = map[uint64]time.Time{}
		}
	}
	s.recent[id] = time.Now()
}

// offer folds one validated datagram into a call, delivering when the logical
// payload is complete. It is the single place duplicate suppression and
// reassembly happen, and it delivers at most once per call.
func (c *call) offer(s *Socket, p Packet) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.delivered {
		s.duplicates.Add(1)
		return
	}
	if c.seen[p.Seq] {
		// Same (request id, sequence) twice: by the protocol's own definition
		// this is one logical response delivered twice.
		s.duplicates.Add(1)
		s.event("udprel.duplicate_suppressed", map[string]any{"request_id": p.RequestID, "seq": p.Seq})
		return
	}
	c.seen[p.Seq] = true
	c.frags[p.Seq] = p.Payload
	if p.Flags&FlagMoreFragments == 0 {
		c.terminator = p.Seq
		c.hasTerminator = true
		c.template = p
	}

	if !c.hasTerminator {
		return
	}
	// Deliver only once every fragment from the base sequence through the
	// terminator has arrived; fragments may arrive in any order.
	if c.terminator < c.baseSeq {
		// Sequence numbers wrapped or the backend answered with an unrelated
		// sequence; treat the single datagram as the whole payload.
		c.baseSeq = c.terminator
	}
	var seqs []uint32
	for seq := c.baseSeq; seq <= c.terminator; seq++ {
		if _, ok := c.frags[seq]; !ok {
			return
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	var payload []byte
	for _, seq := range seqs {
		payload = append(payload, c.frags[seq]...)
	}
	if len(seqs) > 1 {
		s.reassembled.Add(1)
		s.event("udprel.reassembled", map[string]any{
			"request_id": p.RequestID, "fragments": len(seqs), "bytes": len(payload),
		})
	}

	out := c.template
	out.Payload = payload
	out.Flags &^= FlagMoreFragments
	c.delivered = true
	c.done <- out
}

// Close stops the reader and releases the socket.
func (s *Socket) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	_ = s.conn.Close()
}

// --- wire codec -------------------------------------------------------------

var (
	errShort    = errors.New("datagram shorter than header plus checksum")
	errMagic    = errors.New("unexpected magic")
	errVersion  = errors.New("unsupported protocol version")
	errLength   = errors.New("declared payload length does not match datagram size")
	errChecksum = errors.New("checksum mismatch")
	errTooBig   = errors.New("declared payload length exceeds maximum")
)

// Encode serializes a packet, appending the IEEE CRC32 of everything before it.
func Encode(magic [2]byte, version byte, p Packet) []byte {
	buf := make([]byte, HeaderSize+len(p.Payload)+ChecksumSize)
	buf[0], buf[1] = magic[0], magic[1]
	buf[2] = version
	buf[3] = p.Type
	binary.BigEndian.PutUint32(buf[4:8], p.Seq)
	binary.BigEndian.PutUint64(buf[8:16], p.RequestID)
	buf[16] = p.OpCode
	buf[17] = p.Flags
	binary.BigEndian.PutUint16(buf[18:20], uint16(len(p.Payload)))
	copy(buf[HeaderSize:], p.Payload)
	sum := crc32.ChecksumIEEE(buf[:HeaderSize+len(p.Payload)])
	binary.BigEndian.PutUint32(buf[HeaderSize+len(p.Payload):], sum)
	return buf
}

// Decode validates and parses a datagram.
//
// Validation order matters: structure before integrity before content. The
// declared length is checked against the actual datagram size before the CRC is
// computed, so a hostile length field can never make the gateway read out of
// bounds.
func Decode(magic [2]byte, version byte, raw []byte) (Packet, error) {
	if len(raw) < HeaderSize+ChecksumSize {
		return Packet{}, errShort
	}
	if raw[0] != magic[0] || raw[1] != magic[1] {
		return Packet{}, errMagic
	}
	if raw[2] != version {
		return Packet{}, fmt.Errorf("%w: %d", errVersion, raw[2])
	}
	length := int(binary.BigEndian.Uint16(raw[18:20]))
	if HeaderSize+length+ChecksumSize != len(raw) {
		return Packet{}, errLength
	}
	if length > 65535 {
		return Packet{}, errTooBig
	}
	want := binary.BigEndian.Uint32(raw[HeaderSize+length:])
	if crc32.ChecksumIEEE(raw[:HeaderSize+length]) != want {
		return Packet{}, errChecksum
	}
	return Packet{
		Type:      raw[3],
		Seq:       binary.BigEndian.Uint32(raw[4:8]),
		RequestID: binary.BigEndian.Uint64(raw[8:16]),
		OpCode:    raw[16],
		Flags:     raw[17],
		Payload:   append([]byte(nil), raw[HeaderSize:HeaderSize+length]...),
	}, nil
}

// IsChecksumError reports whether a Decode error was an integrity failure,
// which callers distinguish from a structural one for reporting.
func IsChecksumError(err error) bool { return errors.Is(err, errChecksum) }

// IsVersionError reports whether a Decode error was a protocol version
// mismatch. Callers report that separately because it is a static disagreement
// -- retrying the same backend cannot fix it, but another one may be fine.
func IsVersionError(err error) bool { return errors.Is(err, errVersion) }
