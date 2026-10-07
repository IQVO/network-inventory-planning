// Command mcp is the composition root for the network-inventory-planning MCP
// server: it wires env config to the Postgres query adapter, that to the SAME
// read-side use cases the REST adapter uses, and those to the inbound MCP
// adapter, then serves MCP over Streamable HTTP (and nothing else: no stdio,
// no SSE). It is a second, independent deployable alongside
// cmd/network-inventory-planning, per ADR-0008 (warehouse-systems).
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// What this binary deliberately does NOT do: it never starts the outbox relay,
// never dials Kafka and starts no consumer. Every tool is read-only; the
// approval saga (and the relay that drains its outbox) runs in the api binary.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

const (
	envMCPAddr         = "MCP_ADDR"
	envDatabaseURL     = "DATABASE_URL"
	envMigrationsPath  = "MIGRATIONS_PATH"
	envMigrationsDBURL = "MIGRATIONS_DATABASE_URL"
	envMaxStaleness    = "PLANNING_MAX_STALENESS"
	envLogLevel        = "LOG_LEVEL"
)

const (
	defaultAddr           = ":8090"
	defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"
	// defaultMaxStaleness is the fail-closed freshness budget of the planning
	// snapshot when PLANNING_MAX_STALENESS is unset; it matches the api binary.
	defaultMaxStaleness = 10 * time.Minute
	shutdownTimeout     = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(os.Getenv(envLogLevel))
	slog.SetDefault(logger)

	maxStaleness, err := maxStalenessFromEnv()
	if err != nil {
		return err
	}
	databaseURL := os.Getenv(envDatabaseURL)
	deps, closeAdapters, err := buildDeps(context.Background(), logger, databaseURL,
		getenv(envMigrationsDBURL, databaseURL), getenv(envMigrationsPath, defaultMigrationsPath), maxStaleness)
	if err != nil {
		return err
	}
	defer closeAdapters()

	srv := &http.Server{
		Addr:              getenv(envMCPAddr, defaultAddr),
		Handler:           newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveMCP(logger, srv)
}

// buildDeps wires the read-side use cases. No DATABASE_URL means NO stores:
// the server still boots (so the endpoint and /healthz exist) but every tool
// answers an isError result naming the unavailable read side, exactly as the
// REST adapter answers 503, never a fabricated answer. A URL means migrate
// (idempotent; the migrator's advisory lock makes concurrent starts with the
// api safe), then connect a pgx pool. migrationsDatabaseURL is used ONLY for
// the migration step (a DIRECT DSN where DATABASE_URL goes through PgBouncer,
// which cannot honour the session-scoped advisory lock); the runtime pool
// always uses databaseURL. The returned close func releases the pool.
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string, maxStaleness time.Duration) (inboundmcp.Deps, func(), error) {
	noop := func() {}
	if databaseURL == "" {
		logger.Info("DATABASE_URL not configured; every tool will answer read-side-unavailable")
		return inboundmcp.Deps{}, noop, nil
	}
	if err := postgres.RunMigrations(migrationsDatabaseURL, migrationsPath); err != nil {
		return inboundmcp.Deps{}, noop, fmt.Errorf("run migrations: %w", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inboundmcp.Deps{}, noop, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return inboundmcp.Deps{}, noop, fmt.Errorf("ping postgres: %w", err)
	}
	logger.Info("postgres adapters configured", "migrations_path", migrationsPath)

	query := postgres.NewTransferQueryRepo(pool)
	return inboundmcp.Deps{
		GetTransfer:        &usecases.GetTransfer{Query: query},
		ListTransfers:      &usecases.ListTransfers{Query: query},
		FindStuckTransfers: &usecases.FindStuckTransfers{Query: query, Now: time.Now},
		Simulate: &usecases.SimulateTransferOptions{
			Snapshot:     postgres.NewSnapshotRepo(pool),
			MaxStaleness: maxStaleness,
			Now:          time.Now,
		},
	}, pool.Close, nil
}

// serveMCP runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveMCP(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", srv.Addr)
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

// maxStalenessFromEnv resolves PLANNING_MAX_STALENESS (default 10m).
func maxStalenessFromEnv() (time.Duration, error) {
	raw := os.Getenv(envMaxStaleness)
	if raw == "" {
		return defaultMaxStaleness, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", envMaxStaleness, raw)
	}
	return parsed, nil
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
