package tcpmux

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

var testMagic = [2]byte{0xBA, 0xBE}

// fakeServer speaks the frame protocol and lets each test decide what to send
// back, including deliberately malformed frames.
type fakeServer struct {
	ln      net.Listener
	addr    string
	handle  func(w *frameWriter, f Frame)
	wg      sync.WaitGroup
	mu      sync.Mutex
	accepts int
}

// frameWriter serializes writes for one connection, exactly as a real server
// would have to.
type frameWriter struct {
	mu sync.Mutex
	c  net.Conn
}

func (w *frameWriter) send(f Frame, magic [2]byte, version byte, declaredLen int) {
	if declaredLen < 0 {
		declaredLen = len(f.Payload)
	}
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0], buf[1] = magic[0], magic[1]
	buf[2] = version
	buf[3] = f.Flags
	binary.BigEndian.PutUint32(buf[4:8], uint32(declaredLen))
	binary.BigEndian.PutUint64(buf[8:16], f.RequestID)
	copy(buf[HeaderSize:], f.Payload)

	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.c.Write(buf)
}

// ok is the common case: a well-formed reply echoing the request id.
func (w *frameWriter) ok(f Frame) { w.send(f, testMagic, 1, -1) }

func newFakeServer(t *testing.T, handle func(w *frameWriter, f Frame)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{ln: ln, addr: ln.Addr().String(), handle: handle}
	s.wg.Add(1)
	go s.acceptLoop()
	t.Cleanup(func() { _ = ln.Close(); s.wg.Wait() })
	return s
}

func (s *fakeServer) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.accepts++
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer c.Close()
			w := &frameWriter{c: c}
			header := make([]byte, HeaderSize)
			for {
				if _, err := io.ReadFull(c, header); err != nil {
					return
				}
				n := binary.BigEndian.Uint32(header[4:8])
				payload := make([]byte, n)
				if _, err := io.ReadFull(c, payload); err != nil {
					return
				}
				s.handle(w, Frame{
					Version:   header[2],
					Flags:     header[3],
					RequestID: binary.BigEndian.Uint64(header[8:16]),
					Payload:   payload,
				})
			}
		}()
	}
}

func (s *fakeServer) acceptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepts
}

func newPool(t *testing.T, addr string, tune func(*Options)) *Pool {
	t.Helper()
	opt := Options{
		Addr: addr, Magic: testMagic, Version: 1, MaxPayload: 65536,
		PoolSize: 2, MaxInFlight: 16, DialTimeout: time.Second,
	}
	if tune != nil {
		tune(&opt)
	}
	p := New(opt)
	t.Cleanup(p.Close)
	return p
}

// TestResponsesAreMatchedByRequestID is the multiplexing invariant: the backend
// genuinely answers out of order, so nothing may depend on arrival sequence.
func TestResponsesAreMatchedByRequestID(t *testing.T) {
	var mu sync.Mutex
	var queued []func()

	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		mu.Lock()
		queued = append(queued, func() { w.ok(f) })
		full := len(queued) == 5
		mu.Unlock()
		if !full {
			return
		}
		mu.Lock()
		batch := queued
		queued = nil
		mu.Unlock()
		for i := len(batch) - 1; i >= 0; i-- {
			batch[i]()
		}
	})
	// One connection, so the five requests really do share a socket.
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errs := make(chan error, 5)
	for i := 1; i <= 5; i++ {
		go func(i int) {
			payload := []byte{byte(i)}
			resp, err := pool.Do(ctx, Frame{RequestID: uint64(i), Payload: payload})
			if err != nil {
				errs <- err
				return
			}
			if resp.RequestID != uint64(i) || len(resp.Payload) != 1 || resp.Payload[0] != byte(i) {
				errs <- errors.New("response was mispaired")
				return
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < 5; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if got := srv.acceptCount(); got != 1 {
		t.Errorf("server accepted %d connections, want 1 (requests were not multiplexed)", got)
	}
}

// TestDuplicateResponseIsDropped covers the duplicate-response fault.
func TestDuplicateResponseIsDropped(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		w.ok(f)
		w.ok(f)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pool.Do(ctx, Frame{RequestID: 7, Payload: []byte("x")}); err != nil {
		t.Fatalf("do: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	// A second request on the same connection must be unaffected by the stray
	// frame still sitting in the stream.
	if _, err := pool.Do(ctx, Frame{RequestID: 8, Payload: []byte("y")}); err != nil {
		t.Fatalf("second request after a duplicate: %v", err)
	}
	if got := pool.Stats()["unsolicited_frames"]; got < 1 {
		t.Errorf("unsolicited_frames = %d, want at least 1", got)
	}
}

// TestUnsolicitedFrameDoesNotDisturbOthers covers the unsolicited-response
// fault: a frame nobody is waiting for must be discarded without collateral
// damage.
func TestUnsolicitedFrameDoesNotDisturbOthers(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		w.ok(Frame{RequestID: 999999, Payload: []byte("ghost")})
		w.ok(f)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := pool.Do(ctx, Frame{RequestID: 21, Payload: []byte("real")})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if string(resp.Payload) != "real" {
		t.Errorf("payload = %q", resp.Payload)
	}
}

// TestCorruptHeaderFailsOnlyItsOwnRequest is the failure-isolation property: a
// frame that is unusable but leaves the stream aligned must not take down the
// requests sharing that connection.
func TestCorruptHeaderFailsOnlyItsOwnRequest(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		if f.RequestID == 1 {
			w.send(f, [2]byte{0xDE, 0xAD}, 1, -1) // wrong magic, correct length
			return
		}
		w.ok(f)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := pool.Do(ctx, Frame{RequestID: 1, Payload: []byte("bad")}); !errors.Is(err, ErrCorruptFrame) {
		t.Fatalf("err = %v, want ErrCorruptFrame", err)
	}
	// The connection must still be usable.
	resp, err := pool.Do(ctx, Frame{RequestID: 2, Payload: []byte("good")})
	if err != nil {
		t.Fatalf("connection was destroyed by a recoverable corrupt frame: %v", err)
	}
	if string(resp.Payload) != "good" {
		t.Errorf("payload = %q", resp.Payload)
	}
}

// TestVersionMismatchIsReportedDistinctly lets the router treat a version
// disagreement as routable-around rather than as wire damage.
func TestVersionMismatchIsReportedDistinctly(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		w.send(f, testMagic, 9, -1)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := pool.Do(ctx, Frame{RequestID: 31, Payload: []byte("x")})
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("err = %v, want ErrVersionMismatch", err)
	}
}

// TestAbsurdLengthTearsDownTheConnection: when the stream position is unknown
// there is no safe way to find the next boundary, so the connection must go.
func TestAbsurdLengthTearsDownTheConnection(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		w.send(f, testMagic, 1, 1<<30)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1; o.MaxPayload = 4096 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pool.Do(ctx, Frame{RequestID: 41, Payload: []byte("x")}); err == nil {
		t.Fatal("an unreadable frame length was accepted")
	}
	if got := pool.Stats()["connection_teardowns"]; got < 1 {
		t.Errorf("connection_teardowns = %d, want at least 1", got)
	}
}

// TestAbandonedRequestDoesNotMispair: after a caller's deadline expires, a late
// answer must be discarded rather than handed to whoever comes next.
func TestAbandonedRequestDoesNotMispair(t *testing.T) {
	release := make(chan struct{})
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		if f.RequestID == 51 {
			go func() { <-release; w.ok(f) }()
			return
		}
		w.ok(f)
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })

	short, cancelShort := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelShort()
	if _, err := pool.Do(short, Frame{RequestID: 51, Payload: []byte("slow")}); err == nil {
		t.Fatal("expected the abandoned request to fail")
	}

	close(release)
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := pool.Do(ctx, Frame{RequestID: 52, Payload: []byte("fresh")})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.RequestID != 52 || string(resp.Payload) != "fresh" {
		t.Fatalf("a stale answer was delivered to a new request: %+v", resp)
	}
}

// TestDuplicateInFlightIDIsRefused guards the correlation invariant at its
// source: two live requests may never share an identifier.
func TestDuplicateInFlightIDIsRefused(t *testing.T) {
	hold := make(chan struct{})
	srv := newFakeServer(t, func(w *frameWriter, f Frame) {
		go func() { <-hold; w.ok(f) }()
	})
	pool := newPool(t, srv.addr, func(o *Options) { o.PoolSize = 1 })
	defer close(hold)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = pool.Do(ctx, Frame{RequestID: 61, Payload: []byte("first")})
	}()
	<-started
	time.Sleep(100 * time.Millisecond)

	if _, err := pool.Do(ctx, Frame{RequestID: 61, Payload: []byte("second")}); err == nil {
		t.Fatal("a duplicate in-flight request id was accepted")
	}
}

// TestOversizePayloadIsRefusedBeforeDialing keeps a request the protocol cannot
// carry off the wire entirely.
func TestOversizePayloadIsRefusedBeforeDialing(t *testing.T) {
	srv := newFakeServer(t, func(w *frameWriter, f Frame) { w.ok(f) })
	pool := newPool(t, srv.addr, func(o *Options) { o.MaxPayload = 16 })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := pool.Do(ctx, Frame{RequestID: 71, Payload: make([]byte, 64)}); !errors.Is(err, ErrOversize) {
		t.Fatalf("err = %v, want ErrOversize", err)
	}
	if srv.acceptCount() != 0 {
		t.Error("an oversize request caused a connection to be opened")
	}
}

// TestDialFailureIsClassified: an unreachable backend must fail fast and
// distinctly, because that is what tells the router the attempt never happened.
func TestDialFailureIsClassified(t *testing.T) {
	// Port 1 on loopback is reliably refused.
	pool := newPool(t, "127.0.0.1:1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pool.Do(ctx, Frame{RequestID: 81, Payload: []byte("x")}); !errors.Is(err, ErrDial) {
		t.Fatalf("err = %v, want ErrDial", err)
	}
}
