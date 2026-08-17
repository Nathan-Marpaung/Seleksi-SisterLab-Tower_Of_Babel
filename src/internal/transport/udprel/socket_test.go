package udprel

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testMagic = [2]byte{0xC0, 0xDE}

// fakeServer is a controllable UDP peer. Each test installs a responder that
// decides what -- if anything -- comes back for a given request.
type fakeServer struct {
	conn    *net.UDPConn
	addr    string
	respond func(p Packet, reply func(Packet))
	seen    atomic.Int64
	wg      sync.WaitGroup

	peerMu sync.Mutex
	peer   *net.UDPAddr
}

func newFakeServer(t *testing.T, respond func(p Packet, reply func(Packet))) *fakeServer {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{conn: pc, addr: pc.LocalAddr().String(), respond: respond}
	s.wg.Add(1)
	go s.loop()
	t.Cleanup(func() { _ = pc.Close(); s.wg.Wait() })
	return s
}

func (s *fakeServer) loop() {
	defer s.wg.Done()
	buf := make([]byte, 65535)
	for {
		n, from, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p, err := Decode(testMagic, 1, append([]byte(nil), buf[:n]...))
		if err != nil {
			continue
		}
		s.seen.Add(1)
		s.peerMu.Lock()
		s.peer = from
		s.peerMu.Unlock()
		s.respond(p, func(out Packet) {
			_, _ = s.conn.WriteToUDP(Encode(testMagic, 1, out), from)
		})
	}
}

func dialTo(t *testing.T, addr string, tune func(*Options)) *Socket {
	t.Helper()
	opt := Options{
		Addr: addr, Magic: testMagic, Version: 1, MaxPayload: 4096,
		Window: 8, InitialRTO: 60 * time.Millisecond, MinRTO: 20 * time.Millisecond,
		MaxRTO: 300 * time.Millisecond, MaxRetries: 4, Retransmit: true,
	}
	if tune != nil {
		tune(&opt)
	}
	s, err := Dial(opt)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func req(id uint64, seq uint32, payload string) Packet {
	return Packet{Type: TypeRequest, Seq: seq, RequestID: id, OpCode: 1, Payload: []byte(payload)}
}

// TestOutOfOrderResponsesAreCorrelatedByID is the property that makes the
// transport usable concurrently: answers are matched by identifier, so the
// order they arrive in is irrelevant.
func TestOutOfOrderResponsesAreCorrelatedByID(t *testing.T) {
	var mu sync.Mutex
	var held []func()

	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		resp := Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: p.Payload}
		mu.Lock()
		held = append(held, func() { reply(resp) })
		n := len(held)
		mu.Unlock()

		if n == 4 {
			// Flush in reverse: the last request is answered first.
			mu.Lock()
			batch := held
			held = nil
			mu.Unlock()
			for i := len(batch) - 1; i >= 0; i-- {
				batch[i]()
			}
		}
	})

	sock := dialTo(t, srv.addr, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type got struct {
		id      uint64
		payload string
	}
	results := make(chan got, 4)
	for i := 1; i <= 4; i++ {
		go func(i int) {
			p, err := sock.Do(ctx, req(uint64(i), uint32(i), string(rune('a'+i))))
			if err != nil {
				results <- got{id: uint64(i), payload: "ERR:" + err.Error()}
				return
			}
			results <- got{id: p.RequestID, payload: string(p.Payload)}
		}(i)
	}
	for i := 0; i < 4; i++ {
		g := <-results
		want := string(rune('a' + int(g.id)))
		if g.payload != want {
			t.Errorf("request %d received %q, want %q", g.id, g.payload, want)
		}
	}
}

// TestDuplicateResponsesAreSuppressed covers the duplicate-datagram fault: two
// identical answers must produce exactly one delivery.
func TestDuplicateResponsesAreSuppressed(t *testing.T) {
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		resp := Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: p.Payload}
		reply(resp)
		reply(resp)
		reply(resp)
	})
	sock := dialTo(t, srv.addr, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := sock.Do(ctx, req(11, 1, "x")); err != nil {
		t.Fatalf("do: %v", err)
	}
	// Give the extra copies time to arrive and be discarded.
	time.Sleep(150 * time.Millisecond)

	if got := sock.Stats()["duplicates_suppressed"]; got < 2 {
		t.Errorf("duplicates_suppressed = %d, want at least 2", got)
	}
}

// TestCorruptDatagramsAreDroppedAndRecovered shows why a bad checksum is a drop
// rather than an error: on a datagram link it is indistinguishable from loss,
// and retransmission is what recovers it.
func TestCorruptDatagramsAreDroppedAndRecovered(t *testing.T) {
	var attempts atomic.Int32
	var srv *fakeServer
	srv = newFakeServer(t, func(p Packet, reply func(Packet)) {
		if attempts.Add(1) == 1 {
			// Answer with a corrupted checksum, built by hand so Encode cannot
			// quietly repair it.
			raw := Encode(testMagic, 1, Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: []byte("ok")})
			raw[len(raw)-1] ^= 0xFF
			_, _ = srv.writeRaw(raw)
			return
		}
		reply(Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: []byte("ok")})
	})
	sock := dialTo(t, srv.addr, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := sock.Do(ctx, req(21, 1, "x"))
	if err != nil {
		t.Fatalf("expected retransmission to recover, got %v", err)
	}
	if string(resp.Payload) != "ok" {
		t.Errorf("payload = %q, want ok", resp.Payload)
	}
	stats := sock.Stats()
	if stats["checksum_failures"] < 1 {
		t.Errorf("checksum_failures = %d, want at least 1", stats["checksum_failures"])
	}
	if stats["retransmits"] < 1 {
		t.Errorf("retransmits = %d, want at least 1", stats["retransmits"])
	}
}

// TestLostResponseIsRetransmitted covers plain loss.
func TestLostResponseIsRetransmitted(t *testing.T) {
	var attempts atomic.Int32
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		if attempts.Add(1) <= 2 {
			return // drop
		}
		reply(Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: []byte("late")})
	})
	sock := dialTo(t, srv.addr, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	resp, err := sock.Do(ctx, req(31, 1, "x"))
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if string(resp.Payload) != "late" {
		t.Errorf("payload = %q", resp.Payload)
	}
}

// TestRetriesAreBounded proves the transport gives up rather than hammering a
// backend that never answers, and reports a timeout the router can classify.
func TestRetriesAreBounded(t *testing.T) {
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {})
	sock := dialTo(t, srv.addr, func(o *Options) { o.MaxRetries = 2 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sock.Do(ctx, req(41, 1, "x")); err != ErrTimeout {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	// One original transmission plus at most MaxRetries copies.
	if got := srv.seen.Load(); got > 3 {
		t.Errorf("server saw %d datagrams, want at most 3", got)
	}
}

// TestFragmentsAreReassembledOutOfOrder exercises the receive path an upgraded
// backend would use. The reference service never fragments, so this is the only
// place the behaviour can be proven.
func TestFragmentsAreReassembledOutOfOrder(t *testing.T) {
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		// Three fragments, delivered last-first, with the terminator in the
		// middle of the delivery order.
		frag := func(seq uint32, more bool, body string) Packet {
			out := Packet{Type: TypeResponse, Seq: seq, RequestID: p.RequestID, Payload: []byte(body)}
			if more {
				out.Flags |= FlagMoreFragments
			}
			return out
		}
		reply(frag(p.Seq+2, false, "GAMMA"))
		reply(frag(p.Seq, true, "alpha-"))
		reply(frag(p.Seq+1, true, "beta-"))
	})
	sock := dialTo(t, srv.addr, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := sock.Do(ctx, req(51, 100, "x"))
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if string(resp.Payload) != "alpha-beta-GAMMA" {
		t.Fatalf("reassembled %q, want %q", resp.Payload, "alpha-beta-GAMMA")
	}
	if resp.Flags&FlagMoreFragments != 0 {
		t.Error("delivered payload still carries the MORE_FRAGMENTS flag")
	}
	if got := sock.Stats()["reassembled_payloads"]; got != 1 {
		t.Errorf("reassembled_payloads = %d, want 1", got)
	}
}

// TestUnsolicitedDatagramsAreIgnored covers the backend's uncorrelatable
// "invalid packet" reply, which carries request id zero.
func TestUnsolicitedDatagramsAreIgnored(t *testing.T) {
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		reply(Packet{Type: TypeError, Seq: 0, RequestID: 0, Payload: []byte(`{"error":"bad packet"}`)})
		reply(Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID, Payload: []byte("real")})
	})
	sock := dialTo(t, srv.addr, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := sock.Do(ctx, req(61, 1, "x"))
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if string(resp.Payload) != "real" {
		t.Errorf("payload = %q, want the correlated answer", resp.Payload)
	}
	if got := sock.Stats()["unsolicited"]; got < 1 {
		t.Errorf("unsolicited = %d, want at least 1", got)
	}
}

// TestOversizePayloadIsRefusedBeforeSending proves the router gets a clean
// signal it can route around instead of a truncated datagram on the wire.
func TestOversizePayloadIsRefusedBeforeSending(t *testing.T) {
	srv := newFakeServer(t, func(p Packet, reply func(Packet)) {
		reply(Packet{Type: TypeResponse, Seq: p.Seq, RequestID: p.RequestID})
	})
	sock := dialTo(t, srv.addr, func(o *Options) { o.MaxPayload = 32; o.Fragment = false })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := sock.Do(ctx, req(71, 1, string(make([]byte, 64))))
	if err == nil {
		t.Fatal("expected an oversize refusal")
	}
	if srv.seen.Load() != 0 {
		t.Error("an oversize payload reached the wire")
	}
}

// TestCodecRoundTrip pins the framing itself.
func TestCodecRoundTrip(t *testing.T) {
	in := Packet{Type: TypeResponse, Seq: 9, RequestID: 0x1122334455667788, OpCode: 3, Flags: 1,
		Payload: []byte(`{"value":42}`)}
	raw := Encode(testMagic, 1, in)

	out, err := Decode(testMagic, 1, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Type != in.Type || out.Seq != in.Seq || out.RequestID != in.RequestID ||
		out.OpCode != in.OpCode || out.Flags != in.Flags || string(out.Payload) != string(in.Payload) {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}

	t.Run("checksum", func(t *testing.T) {
		bad := append([]byte(nil), raw...)
		bad[len(bad)-2] ^= 0x01
		if _, err := Decode(testMagic, 1, bad); !IsChecksumError(err) {
			t.Fatalf("err = %v, want a checksum error", err)
		}
	})
	t.Run("version", func(t *testing.T) {
		bad := append([]byte(nil), raw...)
		bad[2] = 2
		if _, err := Decode(testMagic, 1, bad); !IsVersionError(err) {
			t.Fatalf("err = %v, want a version error", err)
		}
	})
	t.Run("declared length must match", func(t *testing.T) {
		bad := append([]byte(nil), raw...)
		bad[18], bad[19] = 0xFF, 0xFF
		if _, err := Decode(testMagic, 1, bad); err == nil {
			t.Fatal("a bogus length was accepted")
		}
	})
	t.Run("short datagram", func(t *testing.T) {
		if _, err := Decode(testMagic, 1, raw[:8]); err == nil {
			t.Fatal("a truncated datagram was accepted")
		}
	})
}

// writeRaw sends bytes the responder built itself, bypassing Encode. Only the
// corrupt-checksum test needs it.
func (s *fakeServer) writeRaw(raw []byte) (int, error) {
	s.peerMu.Lock()
	peer := s.peer
	s.peerMu.Unlock()
	return s.conn.WriteToUDP(raw, peer)
}
