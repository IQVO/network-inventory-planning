// Command a2a is the composition root for the network-inventory-planning A2A
// agent: it wires env config to the Postgres query adapter, that to the SAME
// read-side use cases the REST and MCP adapters use, and those to the inbound
// A2A adapter, then serves Agent2Agent JSON-RPC over HTTP with the Agent Card
// at the well-known path. It is a third, independent deployable alongside
// cmd/network-inventory-planning and cmd/mcp, per ADR-0020.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// The agent is READ-ONLY, like cmd/mcp (ADR-0008): it never starts the outbox
// relay, never dials Kafka, starts no consumer and writes no outbox row.
// Approving a transfer (and the saga/relay that carries it) stays in the api
// binary. It runs on real Postgres only: a missing mandatory variable is a
// boot error naming it, raised before telemetry, any connection and the
// listener.
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH          migrations directory (default the source tree path)
//	A2A_ADDR                 listen address (default :8093)
//	SERVICE_VERSION          card/telemetry version (default dev)
//	LOG_LEVEL                debug|info|warn|error (default info)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inbounda2a "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

const (
	envA2AAddr         = "A2A_ADDR"
	envAgentURL        = "A2A_AGENT_URL"
	envDatabaseURL     = "DATABASE_URL"
	envMigrationsPath  = "MIGRATIONS_PATH"
	envMigrationsDBURL = "MIGRATIONS_DATABASE_URL"
	envLogLevel        = "LOG_LEVEL"
)

const (
	defaultAddr           = ":8093"
	defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"
	shutdownTimeout       = 10 * time.Second
	// defaultServiceName labels this binary's spans and metrics (distinct
	// from the OLTP API's DefaultServiceName), overridable via
	// OTEL_SERVICE_NAME -- the same fleet convention as inventory-storage's
	// cmd/mcp.
	defaultServiceName = "network-inventory-planning-a2a"
)

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(os.Getenv(envLogLevel))
	slog.SetDefault(logger)

	// Fail fast on a bad configuration: before telemetry, any connection and
	// the listener.
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serviceName := getenv("OTEL_SERVICE_NAME", defaultServiceName)
	// OTLP trace + metric export and Go runtime metrics (ADR 0012): this
	// MUST run before newRouter -- otelchi and otelchimetric resolve the
	// global MeterProvider at construction. Export never blocks: a missing
	// Collector means dropped telemetry, not a service that will not start.
	otelShutdown, err := telemetry.Setup(ctx, serviceName, getenv("SERVICE_VERSION", "dev"),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	if err != nil {
		return fmt.Errorf("telemetry setup: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}()

	deps, closeAdapters, err := buildDeps(context.Background(), logger, cfg.databaseURL,
		getenv(envMigrationsDBURL, cfg.databaseURL), getenv(envMigrationsPath, defaultMigrationsPath))
	if err != nil {
		return err
	}
	defer closeAdapters()

	srv := &http.Server{
		Addr: getenv(envA2AAddr, defaultAddr),
		Handler: newRouter(
			inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
			inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: getenv("SERVICE_VERSION", "dev")}),
			serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveA2A(logger, srv)
}

// buildDeps wires the read-side use cases: run the idempotent migrations (the
// migrator's advisory lock makes concurrent starts with the api safe), then
// connect a pgx pool. migrationsDatabaseURL is used ONLY for the migration step
// (a DIRECT DSN where DATABASE_URL goes through PgBouncer, which cannot honour
// the session-scoped advisory lock); the runtime pool always uses databaseURL.
// The returned close func releases the pool. Postgres is the only runtime:
// there is no store-less mode (loadBootConfig refuses an empty DATABASE_URL).
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string) (inbounda2a.Deps, func(), error) {
	noop := func() {}
	if err := postgres.RunMigrations(migrationsDatabaseURL, migrationsPath); err != nil {
		return inbounda2a.Deps{}, noop, fmt.Errorf("run migrations: %w", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inbounda2a.Deps{}, noop, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return inbounda2a.Deps{}, noop, fmt.Errorf("ping postgres: %w", err)
	}
	logger.Info("postgres adapters configured", "migrations_path", migrationsPath)

	query := postgres.NewTransferQueryRepo(pool)
	return inbounda2a.Deps{
		GetTransfer:        &usecases.GetTransfer{Query: query},
		FindStuckTransfers: &usecases.FindStuckTransfers{Query: query, Now: time.Now},
	}, pool.Close, nil
}

// bootConfig is the mandatory configuration of cmd/a2a: real Postgres and the
// absolute URL the Agent Card advertises, with no default behind either.
type bootConfig struct {
	databaseURL string
	agentURL    string
}

// errNoDatabase and errNoAgentURL are the boot errors for the two mandatory
// variables; the render test greps "<VAR> is required" to prove the chart
// renders each of them.
var (
	errNoDatabase = errors.New("DATABASE_URL is required: this context runs on real Postgres")
	errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR-0020), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")
)

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract -- a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR-0020)", raw)
	}
	return nil
}

// loadBootConfig reads and validates the mandatory environment. Every problem
// is reported at once, each naming its variable.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		databaseURL: os.Getenv(envDatabaseURL),
		agentURL:    strings.TrimSpace(os.Getenv(envAgentURL)),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errNoDatabase)
	}
	if err := checkAgentURL(cfg.agentURL); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, err
	}
	return cfg, nil
}

// serveA2A runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveA2A(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC over HTTP)", "addr", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|error (case-insensitive), defaulting to Info.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
