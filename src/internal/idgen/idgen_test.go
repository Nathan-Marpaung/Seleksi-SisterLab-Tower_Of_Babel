package idgen

import (
	"sync"
	"testing"
	"time"
)

// TestIdentifiersAreUniqueUnderConcurrency is the correlation invariant at its
// source: two backend attempts must never share an identifier, or a response
// could be matched to the wrong request.
func TestIdentifiersAreUniqueUnderConcurrency(t *testing.T) {
	g := New(0)
	const workers, each = 16, 2000

	var mu sync.Mutex
	seen := make(map[uint64]bool, workers*each)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, each)
			for i := range local {
				local[i] = g.Next()
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if seen[id] {
					t.Errorf("identifier %d was handed out twice", id)
					return
				}
				seen[id] = true
			}
		}()
	}
	wg.Wait()

	if len(seen) != workers*each {
		t.Fatalf("got %d distinct identifiers, want %d", len(seen), workers*each)
	}
}

// TestRestartDoesNotReuseIdentifiers is the property that stops a straggling
// response from a previous process satisfying a fresh request.
func TestRestartDoesNotReuseIdentifiers(t *testing.T) {
	first := New(0)
	var last uint64
	for i := 0; i < 1000; i++ {
		last = first.Next()
	}

	// A restart within the same millisecond is the hard case: the clock floor
	// alone would hand out identifiers the previous process already used.
	second := New(first.HighWater())
	next := second.Next()

	if next <= last {
		t.Fatalf("after restart the first identifier was %d, which is not above the previous high water %d",
			next, last)
	}
}

// TestClockRegressionIsSurvivable: if the wall clock moves backwards, the
// persisted mark must still dominate.
func TestClockRegressionIsSurvivable(t *testing.T) {
	future := uint64(time.Now().Add(time.Hour).UnixMilli()) << epochShift
	g := New(future)
	if got := g.Next(); got <= future {
		t.Fatalf("next = %d, want above the persisted mark %d", got, future)
	}
}

// TestHighWaterTracksIssuedIdentifiers keeps persistence honest.
func TestHighWaterTracksIssuedIdentifiers(t *testing.T) {
	g := New(0)
	var last uint64
	for i := 0; i < 100; i++ {
		last = g.Next()
	}
	if hw := g.HighWater(); hw < last {
		t.Fatalf("high water %d is below the last issued identifier %d", hw, last)
	}
}
