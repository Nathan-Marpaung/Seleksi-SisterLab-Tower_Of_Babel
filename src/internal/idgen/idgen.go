// Package idgen allocates backend correlation identifiers.
//
// Invariant enforced here: no two backend attempts ever share an identifier,
// including across a gateway restart. Service B and Service C both correlate on
// a 64-bit request ID, and a collision after restart would let a straggling
// response from the previous process be matched to a fresh request -- exactly
// the class of bug the specification calls out.
//
// The generator is seeded from a high-water mark that is (a) persisted and (b)
// floored by wall-clock time, so a lost state file degrades to "restart-safe as
// long as the clock moves forward" rather than "collides immediately".
package idgen

import (
	"sync/atomic"
	"time"
)

// epochShift reserves the low 20 bits (~1M ids) for the in-process counter and
// uses milliseconds since the Unix epoch for the high bits. With int64 this
// stays well below the signed 64-bit ceiling until the year ~2255.
const epochShift = 20

// Generator hands out strictly increasing uint64 identifiers.
type Generator struct {
	next atomic.Uint64
	high atomic.Uint64 // observed high-water mark, for persistence
}

// New creates a generator whose first identifier is greater than both
// persistedHWM and the current clock-derived floor.
func New(persistedHWM uint64) *Generator {
	floor := uint64(time.Now().UnixMilli()) << epochShift
	start := floor
	if persistedHWM >= start {
		// Clock went backwards, or the previous process burned through more
		// than the per-millisecond budget. Continue from the persisted mark
		// with a safety gap so that concurrent stragglers cannot alias.
		start = persistedHWM + (1 << epochShift)
	}
	g := &Generator{}
	g.next.Store(start)
	g.high.Store(start)
	return g
}

// Next returns a fresh identifier. Safe for concurrent use.
func (g *Generator) Next() uint64 {
	id := g.next.Add(1)
	// Track the high-water mark without a lock; a lost race only ever records
	// a slightly stale value, which the +gap on restart already covers.
	for {
		cur := g.high.Load()
		if id <= cur || g.high.CompareAndSwap(cur, id) {
			return id
		}
	}
}

// HighWater reports the largest identifier handed out so far.
func (g *Generator) HighWater() uint64 { return g.high.Load() }
