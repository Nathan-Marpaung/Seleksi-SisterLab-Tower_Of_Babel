// Command gateway is the Babel protocol gateway.
//
// Startup order is deliberate and is the reason a restart is uneventful:
//
//	config -> persistent registry -> correlation id allocator -> adapters
//	-> router -> health monitor -> HTTP listener
//
// The registry is restored before anything that depends on it, the id allocator
// is seeded from the restored high-water mark so no identifier is ever reused
// across a restart, and adapters are built and self-tested before the listener
// opens -- so the gateway either accepts traffic with a proven translation
// layer or does not accept traffic at all.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"babel/gateway/internal/adapter"
	"babel/gateway/internal/api"
	"babel/gateway/internal/breaker"
	"babel/gateway/internal/config"
	"babel/gateway/internal/health"
	"babel/gateway/internal/idgen"
	"babel/gateway/internal/obs"
	"babel/gateway/internal/registry"
	"babel/gateway/internal/router"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	started := time.Now()
	cfg := config.Load()
	log := obs.NewLogger(obs.ParseLevel(cfg.LogLevel))
	metrics := obs.NewMetrics()

	log.Info("gateway starting", map[string]any{
		"gateway_id": cfg.GatewayID, "listen": cfg.ListenAddr, "state_dir": cfg.StateDir,
	})

	// --- persistent registry -------------------------------------------------
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("prepare state directory %s: %w", cfg.StateDir, err)
	}
	store := registry.NewStore(cfg.StateDir)
	reg, warning, err := registry.Open(store, registry.SeedOptions{
		ServiceAEndpoint: cfg.ServiceAURL,
		ServiceBEndpoint: cfg.ServiceBAddr,
		ServiceCEndpoint: cfg.ServiceCAddr,
	})
	if err != nil {
		return fmt.Errorf("open registry: %w", err)
	}
	if warning != "" {
		log.Error("registry recovered with a warning", map[string]any{"warning": warning})
	}
	log.Info("registry ready", map[string]any{
		"source": reg.Source(), "revision": reg.Revision(),
		"path": store.Path(), "services": len(reg.List()),
	})

	// --- correlation identifiers --------------------------------------------
	// Seeded above the persisted high-water mark, so a response still in flight
	// from the previous process can never be matched to a fresh request.
	ids := idgen.New(reg.IDHighWater())

	// --- adapters ------------------------------------------------------------
	breakers := breaker.New(breaker.Config{
		Threshold:   cfg.BreakerThreshold,
		Cooldown:    cfg.BreakerCooldown,
		HalfOpenMax: cfg.BreakerHalfOpen,
	})

	adapters := adapter.NewManager(cfg.ShutdownGrace)
	adapters.OnEvent = func(event string, fields map[string]any) {
		metrics.Inc("event:" + event)
		log.Info(event, fields)
	}

	buildOptions := func(serviceID, endpoint string) adapter.BuildOptions {
		transportEvent := func(event string, fields map[string]any) {
			metrics.Inc("event:" + event)
			if fields == nil {
				fields = map[string]any{}
			}
			fields["service_id"] = serviceID
			log.Debug(event, fields)
		}
		return adapter.BuildOptions{
			Endpoint:        endpoint,
			SelfTestTimeout: 5 * time.Second,
			TCP: adapter.TCPOptions{
				PoolSize:         cfg.TCPPoolSize,
				MaxInFlight:      cfg.TCPConnInFlight,
				DialTimeout:      cfg.HealthTimeout,
				FrameBodyTimeout: cfg.TCPFrameBodyTimeout,
				OnEvent:          transportEvent,
			},
			UDP: adapter.UDPOptions{
				Window:     cfg.UDPWindow,
				InitialRTO: cfg.UDPInitialRTO,
				MinRTO:     cfg.UDPMinRTO,
				MaxRTO:     cfg.UDPMaxRTO,
				MaxRetries: cfg.UDPMaxRetries,
				Retransmit: true,
				// Send-side fragmentation stays off for the reference backend,
				// which would reject a fragmented request. Oversized payloads
				// are therefore refused before transmission so the router can
				// choose the stream-oriented backend instead.
				Fragment: false,
				OnEvent:  transportEvent,
			},
		}
	}

	if err := loadAdapters(reg, adapters, buildOptions, log); err != nil {
		return err
	}
	defer adapters.Close()

	// --- request path --------------------------------------------------------
	rt := router.New(cfg, reg, adapters, breakers, ids, log, metrics)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	monitor := health.New(cfg, reg, adapters, breakers, ids, log, metrics)
	go monitor.Run(ctx)
	go flushLoop(ctx, reg, ids, log)

	server := api.New(api.Deps{
		Cfg: cfg, Registry: reg, Adapters: adapters, Breakers: breakers,
		Router: rt, IDs: ids, Log: log, Metrics: metrics,
		StartedAt: started, RegistryWarning: warning,
		BuildOptions: buildOptions,
	})

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: server.Handler(),
		// Generous relative to the request budget: the gateway's own deadline
		// logic owns timing, and cutting a client off mid-response would turn a
		// well-formed error envelope into a transport failure.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("gateway listening", map[string]any{"addr": cfg.ListenAddr})
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("listener: %w", err)
	case <-ctx.Done():
	}

	return shutdown(httpServer, rt, reg, ids, adapters, cfg, log)
}

// loadAdapters builds every adapter the registry binds.
//
// Persisted runtime specs win over built-ins of the same name, which is what
// makes a hot-loaded adapter survive a restart. A variant whose adapter cannot
// be built is reported and skipped rather than aborting startup: one broken
// binding must not stop the gateway from serving the backends that are fine.
func loadAdapters(reg *registry.Registry, mgr *adapter.Manager,
	opts func(serviceID, endpoint string) adapter.BuildOptions, log *obs.Logger) error {

	specs := map[string]*adapter.Spec{}
	for _, s := range adapter.BuiltinSpecs() {
		specs[s.Name] = s
	}
	for name, raw := range reg.AdapterSpecs() {
		spec, err := decodeSpec(raw)
		if err != nil {
			log.Error("persisted adapter spec is unusable and was skipped", map[string]any{
				"adapter": name, "error": err.Error(),
			})
			continue
		}
		specs[name] = spec
		log.Info("restored a runtime-loaded adapter spec", map[string]any{
			"adapter": name, "service_id": spec.ServiceID, "version": spec.Version,
		})
	}

	loaded, failed := 0, 0
	for _, svc := range reg.List() {
		for _, variant := range svc.Variants {
			spec, ok := specs[variant.AdapterName]
			if !ok {
				failed++
				log.Error("registry binds an adapter that does not exist", map[string]any{
					"service_id": svc.ServiceID, "version": variant.Version, "adapter": variant.AdapterName,
				})
				continue
			}
			if _, already := mgr.Peek(variant.AdapterName); already {
				continue
			}
			if err := mgr.Load(spec, opts(svc.ServiceID, svc.EndpointFor(variant))); err != nil {
				failed++
				log.Error("adapter failed to load and its variant will not receive traffic",
					map[string]any{"adapter": variant.AdapterName, "error": err.Error()})
				continue
			}
			loaded++
		}
	}

	if loaded == 0 {
		// Nothing can be served at all; failing loudly beats accepting traffic
		// the gateway is structurally unable to route.
		return fmt.Errorf("no adapter could be loaded (%d failed); refusing to start", failed)
	}
	log.Info("adapters ready", map[string]any{"loaded": loaded, "failed": failed})
	return nil
}

func decodeSpec(raw json.RawMessage) (*adapter.Spec, error) {
	var spec adapter.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// flushLoop persists volatile-but-worth-keeping state on a slow timer.
//
// The correlation-id high-water mark and last observed health change constantly
// and are not worth a synchronous write each time; losing the most recent value
// costs nothing, because the allocator also floors itself by wall-clock time.
func flushLoop(ctx context.Context, reg *registry.Registry, ids *idgen.Generator, log *obs.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reg.NoteIDHighWater(ids.HighWater())
			if err := reg.Flush(); err != nil {
				log.Warn("periodic registry flush failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

// shutdown drains in an order that keeps every promise already made.
//
//  1. Stop admitting new work, so /status and /healthz report draining and new
//     requests get a clean, retryable refusal instead of a dropped connection.
//  2. Let in-flight requests finish inside the grace period.
//  3. Persist the id high-water mark and registry, so the next process cannot
//     reissue an identifier this one already used.
//  4. Close adapters last, since step 2 may still be using them.
func shutdown(srv *http.Server, rt *router.Router, reg *registry.Registry,
	ids *idgen.Generator, adapters *adapter.Manager, cfg config.Config, log *obs.Logger) error {

	log.Info("shutdown requested, draining", map[string]any{
		"in_flight": rt.InFlight(), "grace": cfg.ShutdownGrace.String(),
	})
	rt.BeginDrain()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()

	srvErr := srv.Shutdown(ctx)
	if srvErr != nil {
		log.Warn("some requests did not finish within the grace period", map[string]any{
			"error": srvErr.Error(), "in_flight": rt.InFlight(),
		})
	}

	reg.NoteIDHighWater(ids.HighWater())
	if err := reg.Flush(); err != nil {
		log.Error("final registry flush failed", map[string]any{"error": err.Error()})
	}

	adapters.Close()
	log.Info("gateway stopped", map[string]any{"high_water": ids.HighWater()})
	return nil
}
