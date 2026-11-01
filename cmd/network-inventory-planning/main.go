package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	httpadapter "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment of the Phase-1 read-model side. Each consumer group has NO
// default: an unset variable means that consumer does not exist (a default
// would let a locally run process join the live cluster's group).
const (
	envDatabaseURL       = "DATABASE_URL"
	envMigrationsPath    = "MIGRATIONS_PATH"
	envMigrationsDBURL   = "MIGRATIONS_DATABASE_URL"
	envKafkaBrokers      = "KAFKA_BROKERS"
	envCapabilityGroup   = "SITE_CAPABILITY_CONSUMER_GROUP"
	envDemandGroup       = "SITE_SKU_DEMAND_CONSUMER_GROUP"
	envCapacityGroup     = "CAPACITY_PLAN_CONSUMER_GROUP"
	envTransferGroup     = "TRANSFER_REPLY_CONSUMER_GROUP"
	envTransferFactGrp   = "TRANSFER_FACT_CONSUMER_GROUP"
	envPackLevelGroup    = "PACK_LEVEL_CONSUMER_GROUP"
	envOutboxRelay       = "OUTBOX_RELAY_ENABLED"
	envOutboxMaxAttempts = "OUTBOX_MAX_ATTEMPTS"
	envMaxStaleness      = "PLANNING_MAX_STALENESS"
	envPickPathID        = "TRANSFER_PICK_PATH_ID"
	envPickCPTOffset     = "TRANSFER_PICK_CPT_OFFSET"
	envDispatchPathID    = "TRANSFER_DISPATCH_PATH_ID"
	envDispatchCPTOffs   = "TRANSFER_DISPATCH_CPT_OFFSET"
	// Phase-4 observability + scheduled runs (ADR 0007).
	envHealthInterval  = "NIP_HEALTH_CHECK_INTERVAL"
	envStuckThresholds = "NIP_STUCK_THRESHOLDS"
	envRebalSchedule   = "NIP_REBALANCE_SCHEDULE"
)

const defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"

// defaultMaxStaleness is the fail-closed freshness budget of the planning
// snapshot when PLANNING_MAX_STALENESS is unset.
const defaultMaxStaleness = 10 * time.Minute

// defaultHealthInterval is the saga-health check tick when
// NIP_HEALTH_CHECK_INTERVAL is unset (ADR 0007). "0" disables the check.
const defaultHealthInterval = 5 * time.Minute

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// logLevel maps LOG_LEVEL onto a slog level: debug, info, warn or warning
// (case-insensitive), error; anything else is info.
func logLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func run() error {
	// LOG_LEVEL sets the default logger's level (debug, info, warn/warning,
	// error), like the fleet's other binaries; the handler stays the
	// default one.
	slog.SetLogLoggerLevel(logLevel(os.Getenv("LOG_LEVEL")))
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// OTLP trace + metric export, W3C Trace Context propagation (the fleet's
	// Kafka propagation format, ADR 0006: the global propagator is what every
	// consumer's Extract and every encoder's Inject go through) and Go runtime
	// metrics, ADR 0012. Export never blocks: a missing Collector means
	// dropped telemetry, not a service that will not start. This MUST run
	// before handler.Routes(): otelhttp binds the global MeterProvider when
	// the handler is built.
	otelShutdown, err := telemetry.Setup(ctx, httpadapter.DefaultServiceName, getenv("SERVICE_VERSION", "dev"),
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

	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8080"
	}

	handler, runners, closeAll, err := wire(ctx, logger)
	if err != nil {
		return err
	}
	defer closeAll()

	server := &http.Server{
		Addr:              address,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("network-inventory-planning started")

	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	deaths := startRunners(ctx, runners, logger)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-deaths:
		// A consumer that stopped while the service was still meant to run
		// leaves a frozen read model behind a /readyz that still answers 200.
		// Exit instead, so the kubelet restarts the pod and the consumers
		// rejoin (ADR 0019).
		return err
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// wire assembles the adapters from env: DATABASE_URL set means Postgres
// (migrations + ping, or refuse to boot); unset means the zero-config
// diagnostic service with no read models and no consumers — byte-identical
// HTTP behaviour to before Phase 1 (the simulation endpoint then answers
// 503 rather than fabricating a snapshot).
func wire(ctx context.Context, logger *slog.Logger) (httpadapter.Handler, []func() error, func(), error) {
	var (
		handler     httpadapter.Handler
		runners     []func() error
		closers     []func() error
		databaseURL = os.Getenv(envDatabaseURL)
	)

	closeAll := func() {
		for _, c := range closers {
			if err := c(); err != nil {
				logger.Error("close failed", "error", err)
			}
		}
	}

	handler.Generate = usecases.GenerateTransferProposals{}

	if databaseURL == "" {
		logger.Info("DATABASE_URL unset: running without read models (diagnostic explicit-snapshot mode only)",
			"enable_with", envDatabaseURL)
		return handler, runners, closeAll, nil
	}

	migrationsPath := os.Getenv(envMigrationsPath)
	if migrationsPath == "" {
		migrationsPath = defaultMigrationsPath
	}
	if err := postgres.RunMigrations(migrationsURL(databaseURL), migrationsPath); err != nil {
		return handler, nil, closeAll, fmt.Errorf("run migrations: %w", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return handler, nil, closeAll, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return handler, nil, closeAll, fmt.Errorf("ping postgres: %w", err)
	}
	closers = append(closers, func() error { pool.Close(); return nil })

	maxStaleness, err := maxStalenessFromEnv()
	if err != nil {
		return handler, nil, closeAll, err
	}

	wired, err := wireSagas(logger, pool, maxStaleness)
	if err != nil {
		return handler, nil, closeAll, err
	}
	handler.Simulate = wired.simulate
	handler.Generate = wired.generate
	handler.Approve = wired.approve
	handler.ListRebalanceRuns = wired.runs
	handler.GetTransfer, handler.ListTransfers = wireTransferReadSide(pool)
	handler.PutPolicy, handler.ListPolicies = wired.putPolicy, wired.listPolicies
	handler.PutLane, handler.ListLanes = wired.putLane, wired.listLanes
	runners = append(runners, wired.runners...)
	closers = append(closers, wired.closers...)

	return handler, runners, closeAll, nil
}

// wireTransferReadSide builds the read-only transfer use cases over the
// Postgres query port: GET /v1/transfers and GET /v1/transfers/{id}.
func wireTransferReadSide(pool *pgxpool.Pool) (*usecases.GetTransfer, *usecases.ListTransfers) {
	query := postgres.NewTransferQueryRepo(pool)
	return &usecases.GetTransfer{Query: query}, &usecases.ListTransfers{Query: query}
}

// migrationsURL returns the DSN the golang-migrate step uses:
// MIGRATIONS_DATABASE_URL when set, else the runtime DSN. golang-migrate takes a
// session-scoped pg_advisory_lock that PgBouncer's transaction pooling cannot
// honour, so a deployment whose DATABASE_URL goes through a pooler supplies a
// DIRECT connection string here. The runtime pgxpool never uses it.
func migrationsURL(databaseURL string) string {
	if direct := os.Getenv(envMigrationsDBURL); direct != "" {
		return direct
	}
	return databaseURL
}

// maxStalenessFromEnv resolves PLANNING_MAX_STALENESS (default 10m).
func maxStalenessFromEnv() (time.Duration, error) {
	if raw := os.Getenv(envMaxStaleness); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("%s must be a positive duration, got %q", envMaxStaleness, raw)
		}
		return parsed, nil
	}
	return defaultMaxStaleness, nil
}

// getenv returns the environment variable key, or fallback when it is unset
// or empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// startRunners starts every consumer and returns a channel that receives the
// first UNEXPECTED stop: an error that is not the context's own cancellation,
// or a nil return while ctx is still live (a consumer is meant to run until
// ctx ends). Further deaths are logged and dropped, because run() exits on the
// first. A stop caused by ctx being cancelled is a clean shutdown and reports
// nothing.
func startRunners(ctx context.Context, runners []func() error, logger *slog.Logger) <-chan error {
	deaths := make(chan error, 1)
	for _, r := range runners {
		go func(r func() error) {
			err := r()
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if err == nil {
				err = errors.New("returned without an error while the service was still running")
			}
			logger.Error("consumer stopped", "error", err)
			select {
			case deaths <- fmt.Errorf("consumer stopped: %w", err):
			default:
			}
		}(r)
	}
	return deaths
}
