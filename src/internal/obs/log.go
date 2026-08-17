// Package obs provides structured logging and counters.
//
// Logs are line-delimited JSON so that a demo run can be grepped and replayed;
// every line carries the client request_id when one exists, which is what makes
// concurrent traffic readable.
package obs

import (
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func ParseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "debug", "trace":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

// Logger writes JSON lines. It is safe for concurrent use.
type Logger struct {
	mu    sync.Mutex
	out   io.Writer
	level Level
	base  map[string]any
}

func NewLogger(level Level) *Logger {
	return &Logger{out: os.Stdout, level: level, base: map[string]any{}}
}

// With returns a logger that stamps every line with the given fields.
func (l *Logger) With(fields map[string]any) *Logger {
	merged := make(map[string]any, len(l.base)+len(fields))
	for k, v := range l.base {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	return &Logger{out: l.out, level: l.level, base: merged}
}

func (l *Logger) Enabled(level Level) bool { return level >= l.level }

func (l *Logger) log(level Level, msg string, fields map[string]any) {
	if level < l.level {
		return
	}
	rec := make(map[string]any, len(l.base)+len(fields)+3)
	for k, v := range l.base {
		rec[k] = v
	}
	for k, v := range fields {
		rec[k] = v
	}
	rec["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	rec["level"] = level.String()
	rec["msg"] = msg

	buf, err := json.Marshal(rec)
	if err != nil {
		buf = []byte(`{"level":"error","msg":"log encode failure"}`)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(buf, '\n'))
}

func (l *Logger) Debug(msg string, f map[string]any) { l.log(LevelDebug, msg, f) }
func (l *Logger) Info(msg string, f map[string]any)  { l.log(LevelInfo, msg, f) }
func (l *Logger) Warn(msg string, f map[string]any)  { l.log(LevelWarn, msg, f) }
func (l *Logger) Error(msg string, f map[string]any) { l.log(LevelError, msg, f) }

// Metrics is a tiny lock-free counter set exposed through /status and /metrics.
type Metrics struct {
	mu       sync.RWMutex
	counters map[string]*int64
}

func NewMetrics() *Metrics { return &Metrics{counters: map[string]*int64{}} }

func (m *Metrics) counter(name string) *int64 {
	m.mu.RLock()
	c, ok := m.counters[name]
	m.mu.RUnlock()
	if ok {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		return c
	}
	c = new(int64)
	m.counters[name] = c
	return c
}

func (m *Metrics) Inc(name string)          { atomic.AddInt64(m.counter(name), 1) }
func (m *Metrics) Add(name string, n int64) { atomic.AddInt64(m.counter(name), n) }
func (m *Metrics) Get(name string) int64    { return atomic.LoadInt64(m.counter(name)) }
func (m *Metrics) Set(name string, v int64) { atomic.StoreInt64(m.counter(name), v) }

// Snapshot returns a stable, sorted copy for serialization.
func (m *Metrics) Snapshot() map[string]any {
	m.mu.RLock()
	names := make([]string, 0, len(m.counters))
	for k := range m.counters {
		names = append(names, k)
	}
	m.mu.RUnlock()
	sort.Strings(names)

	out := make(map[string]any, len(names))
	for _, n := range names {
		out[n] = m.Get(n)
	}
	return out
}
