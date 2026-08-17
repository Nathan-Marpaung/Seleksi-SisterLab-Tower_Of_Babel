// Package tcpmux is a multiplexing client for the length-prefixed frame
// protocol spoken by Service B.
//
// The backend genuinely multiplexes: probing it with seven pipelined frames on
// one socket returns them out of order. So the client is built the same way --
// one writer mutex, one reader goroutine, and a pending map keyed by request ID
// -- rather than the naive "one connection per in-flight request" design. That
// is what makes concurrency correct here: responses are matched by identifier,
// never by arrival order, so nothing can be mispaired no matter how the backend
// interleaves them.
//
// Failure handling is deliberately graded, because the fault catalogue
// distinguishes recoverable corruption from stream desynchronization:
//
//   - A frame whose request ID matches nothing pending is *unsolicited*. It is
//     counted and dropped. Duplicated responses land here too, which gives
//     duplicate suppression for free: the first response resolves and removes
//     the waiter, the second finds nothing.
//   - A frame with bad magic or an unexpected version, but a sane length, is a
//     corrupt frame. The connection is still synchronized, so the payload is
//     skipped and only the correlated request fails.
//   - A length the protocol forbids means the stream position is unknown. There
//     is no safe way to find the next frame boundary, so the connection is torn
//     down and every waiter on it fails. Other connections, and every other
//     backend, are untouched.
package tcpmux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// HeaderSize is the fixed frame header length.
const HeaderSize = 16

// Frame is one protocol frame.
type Frame struct {
	Version   byte
	Flags     byte
	RequestID uint64
	Payload   []byte
}

// Classified transport failures. The caller maps these onto gateway error
// codes; the pool does not know about gwerr so it stays reusable.
var (
	ErrDial         = errors.New("tcpmux: dial failed")
	ErrWrite        = errors.New("tcpmux: write failed")
	ErrClosed       = errors.New("tcpmux: connection closed before response")
	ErrCorruptFrame = errors.New("tcpmux: corrupt frame")
	ErrOversize     = errors.New("tcpmux: payload exceeds protocol maximum")
	ErrPoolClosed   = errors.New("tcpmux: pool is closed")
	// ErrVersionMismatch is kept distinct from ErrCorruptFrame: a frame that is
	// well formed but speaks another protocol version is a static disagreement
	// the caller can route around, not damage on the wire.
	ErrVersionMismatch = errors.New("tcpmux: unexpected protocol version")
	// ErrTruncated means a frame header arrived but its payload never did. The
	// backend answered and then stopped mid-frame, which is a protocol
	// violation rather than a timeout -- and reporting it as one matters,
	// because a timeout would otherwise consume the caller's entire budget
	// waiting for bytes that are never coming.
	ErrTruncated = errors.New("tcpmux: frame payload was truncated")
)

// Options configures a pool.
type Options struct {
	Addr        string
	Magic       [2]byte
	Version     byte
	MaxPayload  uint32
	PoolSize    int
	MaxInFlight int // per connection
	DialTimeout time.Duration
	// FrameBodyTimeout bounds how long the reader waits for a frame's payload
	// after its header has arrived.
	FrameBodyTimeout time.Duration
	// OnEvent reports notable transport events for observability. Optional.
	OnEvent func(event string, fields map[string]any)
}

// Pool owns a small set of multiplexed connections.
type Pool struct {
	opt Options

	mu     sync.Mutex
	conns  []*conn
	closed bool
	// dialing is non-nil while a connection is being opened, and is closed when
	// that attempt finishes. It serializes cold-start dialing.
	dialing chan struct{}

	// Counters surfaced through /status.
	unsolicited atomic.Int64
	corrupt     atomic.Int64
	resets      atomic.Int64
	dials       atomic.Int64
}

// New builds a pool. No connection is opened until the first call, so a dead
// backend costs nothing at startup.
func New(opt Options) *Pool {
	if opt.PoolSize < 1 {
		opt.PoolSize = 1
	}
	if opt.MaxInFlight < 1 {
		opt.MaxInFlight = 1
	}
	if opt.DialTimeout <= 0 {
		opt.DialTimeout = 2 * time.Second
	}
	if opt.MaxPayload == 0 {
		opt.MaxPayload = 65536
	}
	if opt.FrameBodyTimeout <= 0 {
		opt.FrameBodyTimeout = 2 * time.Second
	}
	return &Pool{opt: opt}
}

func (p *Pool) event(name string, fields map[string]any) {
	if p.opt.OnEvent != nil {
		p.opt.OnEvent(name, fields)
	}
}

// Stats reports transport counters.
func (p *Pool) Stats() map[string]int64 {
	p.mu.Lock()
	live := len(p.conns)
	p.mu.Unlock()
	return map[string]int64{
		"connections":          int64(live),
		"dials":                p.dials.Load(),
		"unsolicited_frames":   p.unsolicited.Load(),
		"corrupt_frames":       p.corrupt.Load(),
		"connection_teardowns": p.resets.Load(),
	}
}

// Do sends a frame and waits for the response carrying the same request ID.
//
// The request ID must be globally unique for the lifetime of the process (and
// across restarts); the caller's id generator guarantees that. Reusing one
// would let a late response from an abandoned request satisfy a new one.
func (p *Pool) Do(ctx context.Context, req Frame) (Frame, error) {
	if uint32(len(req.Payload)) > p.opt.MaxPayload {
		return Frame{}, fmt.Errorf("%w: %d bytes", ErrOversize, len(req.Payload))
	}

	c, err := p.acquire(ctx)
	if err != nil {
		return Frame{}, err
	}

	waiter := make(chan result, 1)
	if err := c.register(req.RequestID, waiter); err != nil {
		c.release()
		return Frame{}, err
	}
	// Whatever happens next, the slot must be given back exactly once.
	defer c.release()

	if err := c.write(req, p.opt.Magic, p.opt.Version); err != nil {
		c.unregister(req.RequestID)
		c.kill(err)
		return Frame{}, fmt.Errorf("%w: %v", ErrWrite, err)
	}

	select {
	case res := <-waiter:
		return res.frame, res.err
	case <-ctx.Done():
		// Deregister first, so a response that arrives a moment later is
		// treated as unsolicited and dropped rather than mispaired.
		c.unregister(req.RequestID)
		return Frame{}, ctx.Err()
	}
}

// acquire picks the least-loaded usable connection, dialing a new one when the
// existing ones are busy and the pool is not yet at its size limit.
//
// Dialing happens outside the pool lock, so a slow connect cannot stall every
// other request. That would let a burst of concurrent cold-start requests each
// open their own socket, so one dial at a time is admitted and the rest wait
// for it: a thundering herd at startup would open connections the pool is only
// going to throw away, and every one of them is a handshake the backend has to
// service.
func (p *Pool) acquire(ctx context.Context) (*conn, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}

		live := p.conns[:0]
		for _, c := range p.conns {
			if !c.isDead() {
				live = append(live, c)
			}
		}
		p.conns = live

		best := leastLoadedLocked(p.conns)
		// Only grow the pool when the least-loaded connection is already
		// carrying traffic; one idle connection is enough for a serial load.
		wantDial := (best == nil || best.load() > 0) && len(p.conns) < p.opt.PoolSize

		if !wantDial {
			p.mu.Unlock()
			if best == nil {
				return nil, fmt.Errorf("%w: pool exhausted", ErrDial)
			}
			if !best.acquireSlot(p.opt.MaxInFlight) {
				// Backpressure: every connection is saturated. Reporting this
				// distinctly beats silently queueing, because the caller still
				// owns a deadline it can honour.
				return nil, fmt.Errorf("%w: all connections saturated", ErrDial)
			}
			return best, nil
		}

		if p.dialing != nil {
			// Someone else is already opening a connection. Wait for it and
			// re-evaluate rather than opening a second one.
			wait := p.dialing
			p.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		done := make(chan struct{})
		p.dialing = done
		p.mu.Unlock()

		c, err := p.dial(ctx)

		p.mu.Lock()
		p.dialing = nil
		close(done)
		if err != nil {
			p.mu.Unlock()
			// Prefer an already-open socket over failing outright: a transient
			// dial failure should not break a request an existing connection
			// could have carried.
			if best != nil && !best.isDead() && best.acquireSlot(p.opt.MaxInFlight) {
				return best, nil
			}
			return nil, fmt.Errorf("%w: %v", ErrDial, err)
		}
		if p.closed {
			p.mu.Unlock()
			c.kill(ErrPoolClosed)
			return nil, ErrPoolClosed
		}
		p.conns = append(p.conns, c)
		p.mu.Unlock()

		if c.acquireSlot(p.opt.MaxInFlight) {
			return c, nil
		}
		return nil, ErrPoolClosed
	}
}

// leastLoadedLocked picks the connection carrying the fewest requests. Caller
// holds p.mu.
func leastLoadedLocked(conns []*conn) *conn {
	var best *conn
	bestLoad := 1 << 30
	for _, c := range conns {
		if n := c.load(); n < bestLoad {
			best, bestLoad = c, n
		}
	}
	return best
}

func (p *Pool) dial(ctx context.Context) (*conn, error) {
	d := net.Dialer{Timeout: p.opt.DialTimeout}
	nc, err := d.DialContext(ctx, "tcp", p.opt.Addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		// Frames are small and latency-sensitive; Nagle would batch them.
		_ = tc.SetNoDelay(true)
	}
	p.dials.Add(1)
	c := &conn{
		pool:    p,
		nc:      nc,
		pending: map[uint64]chan result{},
	}
	go c.readLoop()
	return c, nil
}

// Close tears down every connection. In-flight waiters are woken with ErrClosed
// rather than left to hit their own deadlines.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		c.kill(ErrPoolClosed)
	}
}

// result is what a waiter receives: exactly one frame or exactly one error.
type result struct {
	frame Frame
	err   error
}

type conn struct {
	pool *Pool
	nc   net.Conn

	wmu sync.Mutex // serializes writes; frames must not interleave on the wire

	mu       sync.Mutex
	pending  map[uint64]chan result
	inflight int
	dead     bool
	deadErr  error
}

func (c *conn) load() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight
}

func (c *conn) isDead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

func (c *conn) reason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deadErr != nil {
		return c.deadErr
	}
	return ErrClosed
}

func (c *conn) acquireSlot(max int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead || c.inflight >= max {
		return false
	}
	c.inflight++
	return true
}

func (c *conn) release() {
	c.mu.Lock()
	if c.inflight > 0 {
		c.inflight--
	}
	c.mu.Unlock()
}

func (c *conn) register(id uint64, ch chan result) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return c.deadErr
	}
	if _, exists := c.pending[id]; exists {
		// Would mean the id generator handed out a duplicate. Refuse loudly
		// rather than silently mispairing two live requests.
		return fmt.Errorf("tcpmux: request id %d is already in flight", id)
	}
	c.pending[id] = ch
	return nil
}

func (c *conn) unregister(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *conn) write(f Frame, magic [2]byte, version byte) error {
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0], buf[1] = magic[0], magic[1]
	buf[2] = version
	buf[3] = f.Flags
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(f.Payload)))
	binary.BigEndian.PutUint64(buf[8:16], f.RequestID)
	copy(buf[HeaderSize:], f.Payload)

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.isDead() {
		return c.reason()
	}
	_, err := c.nc.Write(buf)
	return err
}

// readLoop is the single reader for this connection. It owns the socket's read
// side entirely, which is what allows responses to be demultiplexed by id.
func (c *conn) readLoop() {
	header := make([]byte, HeaderSize)
	for {
		if _, err := io.ReadFull(c.nc, header); err != nil {
			c.kill(fmt.Errorf("%w: %v", ErrClosed, err))
			return
		}
		magic := [2]byte{header[0], header[1]}
		version := header[2]
		flags := header[3]
		length := binary.BigEndian.Uint32(header[4:8])
		reqID := binary.BigEndian.Uint64(header[8:16])

		if length > c.pool.opt.MaxPayload {
			// Cannot locate the next boundary: the stream is unusable.
			c.pool.corrupt.Add(1)
			c.pool.event("tcpmux.frame_length_invalid", map[string]any{
				"declared_length": length, "max": c.pool.opt.MaxPayload, "request_id": reqID,
			})
			c.kill(fmt.Errorf("%w: declared length %d exceeds maximum %d", ErrCorruptFrame, length, c.pool.opt.MaxPayload))
			return
		}

		// Once a header has been read the payload is committed to arrive, so a
		// bounded deadline applies for the rest of the frame. Without it a
		// backend that stops mid-frame would hold the reader until every
		// waiting request had spent its whole budget.
		payload := make([]byte, length)
		if length > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(c.pool.opt.FrameBodyTimeout))
		}
		_, err := io.ReadFull(c.nc, payload)
		_ = c.nc.SetReadDeadline(time.Time{})
		if err != nil {
			// The stream is now at an unknown offset, so the connection cannot
			// be reused. Everything waiting on it fails as a protocol
			// violation, not as a timeout: the backend did answer, it just
			// answered incorrectly.
			c.pool.corrupt.Add(1)
			c.pool.event("tcpmux.frame_truncated", map[string]any{
				"request_id": reqID, "declared_length": length, "error": err.Error(),
			})
			c.kill(fmt.Errorf("%w: %v", ErrTruncated, err))
			return
		}

		if magic != c.pool.opt.Magic || version != c.pool.opt.Version {
			// The frame is unusable but the stream is still aligned, because
			// the declared length was plausible and fully consumed. Fail only
			// the request this frame claims to answer, and keep the two causes
			// apart so the caller can route around a version disagreement.
			c.pool.corrupt.Add(1)
			c.pool.event("tcpmux.frame_header_invalid", map[string]any{
				"magic":   fmt.Sprintf("%02x%02x", magic[0], magic[1]),
				"version": version, "request_id": reqID,
			})
			cause := fmt.Errorf("%w: magic %02x%02x", ErrCorruptFrame, magic[0], magic[1])
			if magic == c.pool.opt.Magic {
				cause = fmt.Errorf("%w: %d", ErrVersionMismatch, version)
			}
			c.failOne(reqID, cause)
			continue
		}

		c.mu.Lock()
		ch, ok := c.pending[reqID]
		if ok {
			delete(c.pending, reqID)
		}
		c.mu.Unlock()

		if !ok {
			// Unsolicited, or a duplicate of a response already delivered.
			c.pool.unsolicited.Add(1)
			c.pool.event("tcpmux.unsolicited_frame", map[string]any{"request_id": reqID})
			continue
		}
		ch <- result{frame: Frame{Version: version, Flags: flags, RequestID: reqID, Payload: payload}}
	}
}

// failOne wakes a single waiter without disturbing the rest of the connection.
func (c *conn) failOne(id uint64, err error) {
	c.mu.Lock()
	ch, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if ok {
		ch <- result{err: err}
	}
}

// kill tears the connection down and wakes every waiter exactly once.
func (c *conn) kill(cause error) {
	c.mu.Lock()
	if c.dead {
		c.mu.Unlock()
		return
	}
	c.dead = true
	c.deadErr = cause
	pending := c.pending
	c.pending = map[uint64]chan result{}
	c.mu.Unlock()

	c.pool.resets.Add(1)
	_ = c.nc.Close()
	for _, ch := range pending {
		ch <- result{err: cause}
	}
}
