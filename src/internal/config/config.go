// Package config resolves gateway configuration from the environment.
//
// Everything here is a *policy* knob. Structural configuration -- which
// backends exist, what they can do, which adapter version is bound -- lives in
// the persistent registry instead, because it must survive restarts and be
// mutable at runtime.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved runtime policy.
type Config struct {
	GatewayID  string
	ListenAddr string

	// StateDir holds the persisted registry and adapter specs.
	StateDir string

	// Backend endpoints used to seed the registry on first boot only. Once a
	// registry file exists it wins, so operator edits are not clobbered.
	ServiceAURL  string
	ServiceBAddr string
	ServiceCAddr string

	// Timeout budget.
	DefaultTimeout  time.Duration
	MinTimeout      time.Duration
	MaxTimeout      time.Duration
	ResponseMargin  time.Duration // reserved so the envelope beats the caller's own deadline
	MaxAttemptSlice time.Duration // ceiling for a single backend attempt

	// Retry / fallback.
	MaxAttempts     int           // total backend attempts per client request
	SameServiceTry  int           // attempts on one backend before moving on
	RetryBackoff    time.Duration // deterministic, multiplied by attempt index
	MaxRetryBackoff time.Duration

	// Concurrency and isolation.
	MaxInFlight     int
	PerBackendLimit int
	TCPPoolSize     int
	TCPConnInFlight int
	// TCPFrameBodyTimeout bounds the wait for a frame payload once its header
	// has been read, so a backend that stops mid-frame fails fast.
	TCPFrameBodyTimeout time.Duration
	UDPWindow           int

	// Circuit breaker.
	BreakerThreshold int
	BreakerCooldown  time.Duration
	BreakerHalfOpen  int

	// Health probing.
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	// IntrusiveProbeInterval is the slow cadence for backends whose only
	// liveness check is a real operation, so health monitoring does not
	// manufacture a steady stream of work nobody asked for.
	IntrusiveProbeInterval time.Duration

	// UDP reliability.
	UDPInitialRTO time.Duration
	UDPMinRTO     time.Duration
	UDPMaxRTO     time.Duration
	UDPMaxRetries int

	// Behaviour policies.
	PreferredIncapable string // "fallback" | "strict"
	StrictHTTPStatus   bool   // emit 408/503/429 instead of 200 for domain errors
	ValidateArguments  bool

	// FallbackOnCorrupt decides what happens when a backend answers with a
	// response the gateway cannot trust -- unparseable JSON, a bad checksum, a
	// mismatched correlation identifier.
	//
	// Such a response is unusable either way, so the request has already
	// failed. The choice is between giving the caller nothing, or trying
	// another backend and accepting that in the worst case the operation runs
	// twice. The default is to try: every operation this gateway serves is a
	// pure function with no side effects, so a second execution changes no
	// state, and the alternative is a guaranteed failure. Setting this false
	// takes the strictest reading of the no-duplicate-operations rule and
	// makes a corrupt response terminal.
	//
	// Note this is a *weaker* claim than the timeout case, which is never
	// fallback-eligible by default: a timeout leaves a backend actively
	// working on the request, while a corrupt response means it has already
	// finished with it.
	FallbackOnCorrupt bool

	// Lifecycle.
	ShutdownGrace time.Duration
	AdminEnabled  bool
	AdminToken    string

	LogLevel string
}

// Load reads configuration from the environment, applying defaults chosen to be
// safe for the reference environment.
func Load() Config {
	c := Config{
		GatewayID:  env("BABEL_GATEWAY_ID", "babel-gateway"),
		ListenAddr: env("BABEL_LISTEN_ADDR", ":8080"),
		StateDir:   env("BABEL_GATEWAY_STATE_DIR", "/data"),

		ServiceAURL:  env("BABEL_SERVICE_A_URL", "http://service-a:8101"),
		ServiceBAddr: env("BABEL_SERVICE_B_ADDR", "service-b:8201"),
		ServiceCAddr: env("BABEL_SERVICE_C_ADDR", "service-c:8301"),

		DefaultTimeout:  envDur("BABEL_DEFAULT_TIMEOUT_MS", 2000),
		MinTimeout:      envDur("BABEL_MIN_TIMEOUT_MS", 50),
		MaxTimeout:      envDur("BABEL_MAX_TIMEOUT_MS", 60000),
		ResponseMargin:  envDur("BABEL_RESPONSE_MARGIN_MS", 120),
		MaxAttemptSlice: envDur("BABEL_ATTEMPT_MAX_MS", 5000),

		MaxAttempts:     envInt("BABEL_MAX_ATTEMPTS", 4),
		SameServiceTry:  envInt("BABEL_SAME_SERVICE_ATTEMPTS", 2),
		RetryBackoff:    envDur("BABEL_RETRY_BACKOFF_MS", 25),
		MaxRetryBackoff: envDur("BABEL_RETRY_BACKOFF_MAX_MS", 200),

		MaxInFlight:         envInt("BABEL_MAX_INFLIGHT", 512),
		PerBackendLimit:     envInt("BABEL_PER_BACKEND_LIMIT", 128),
		TCPPoolSize:         envInt("BABEL_TCP_POOL_SIZE", 4),
		TCPConnInFlight:     envInt("BABEL_TCP_CONN_INFLIGHT", 64),
		TCPFrameBodyTimeout: envDur("BABEL_TCP_FRAME_BODY_TIMEOUT_MS", 1500),
		UDPWindow:           envInt("BABEL_UDP_WINDOW", 128),

		BreakerThreshold: envInt("BABEL_BREAKER_THRESHOLD", 5),
		BreakerCooldown:  envDur("BABEL_BREAKER_COOLDOWN_MS", 3000),
		BreakerHalfOpen:  envInt("BABEL_BREAKER_HALF_OPEN", 1),

		HealthInterval:         envDur("BABEL_HEALTH_INTERVAL_MS", 2000),
		HealthTimeout:          envDur("BABEL_HEALTH_TIMEOUT_MS", 800),
		IntrusiveProbeInterval: envDur("BABEL_INTRUSIVE_PROBE_INTERVAL_MS", 30000),

		UDPInitialRTO: envDur("BABEL_UDP_RTO_MS", 120),
		UDPMinRTO:     envDur("BABEL_UDP_RTO_MIN_MS", 40),
		UDPMaxRTO:     envDur("BABEL_UDP_RTO_MAX_MS", 800),
		UDPMaxRetries: envInt("BABEL_UDP_MAX_RETRIES", 6),

		PreferredIncapable: strings.ToLower(env("BABEL_PREFERRED_INCAPABLE", "fallback")),
		StrictHTTPStatus:   envBool("BABEL_STRICT_HTTP_STATUS", false),
		ValidateArguments:  envBool("BABEL_VALIDATE_ARGUMENTS", true),
		FallbackOnCorrupt:  envBool("BABEL_FALLBACK_ON_CORRUPT", true),

		ShutdownGrace: envDur("BABEL_SHUTDOWN_GRACE_MS", 8000),
		AdminEnabled:  envBool("BABEL_ADMIN_ENABLED", true),
		AdminToken:    env("BABEL_ADMIN_TOKEN", ""),

		LogLevel: strings.ToLower(env("BABEL_LOG_LEVEL", "info")),
	}

	if c.PreferredIncapable != "strict" {
		c.PreferredIncapable = "fallback"
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 1
	}
	if c.SameServiceTry < 1 {
		c.SameServiceTry = 1
	}
	return c
}

// ClampTimeout applies the configured caller-timeout bounds.
func (c Config) ClampTimeout(requested time.Duration) time.Duration {
	if requested <= 0 {
		requested = c.DefaultTimeout
	}
	if requested < c.MinTimeout {
		return c.MinTimeout
	}
	if requested > c.MaxTimeout {
		return c.MaxTimeout
	}
	return requested
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envDur(key string, defMS int) time.Duration {
	return time.Duration(envInt(key, defMS)) * time.Millisecond
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}
