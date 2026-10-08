// Command nip-projector is the WRITER of the network-inventory-planning
// analytics read side (ADR 0009). It consumes the analytics topic
// (warehouse.network-inventory-planning.analytics) under a fixed, env-supplied
// consumer group, projects each saga-health event (TransferStateAdvanced,
// TransferStuckDetected, RebalanceRunCompleted) into the ANALYTICAL database
// (a separate database, own migrations) and serves only /healthz and /readyz
// on an admin port. It is the only writer of that database; the read-only
// reports binary is cmd/nip-reports.
//
// It never touches the OLTP database. Kafka is dialled lazily (the reader
// connects inside its first fetch), so a broker outage at boot does not crash
// the process; the analytical database dial and migrations are retried
// through internal/bootretry because the first outbound connection of an
// Istio-injected pod is reset ~10s after start.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-inventory-planning/internal/bootretry"
)

const (
	// shutdownTimeout bounds the admin server drain and the wait for the
	// consumer loop to commit/dead-letter the message it was handling.
	shutdownTimeout = 10 * time.Second

	// defaultConsumerGroup is the projector's fixed group when
	// ANALYTICS_CONSUMER_GROUP is unset (the chart always sets it): the fleet
	// charter's `<ctx>-analytics` naming. It is distinct from every OLTP
	// consumer group of this service.
	defaultConsumerGroup = "network-inventory-planning-analytics"

	// defaultMigrationsPath is the analytical migrations directory relative
	// to the repo root / the image's /app.
	defaultMigrationsPath = "analytics/migrations"
)

// config is the projector's environment.
type config struct {
	analyticsURL   string
	brokers        []string
	group          string
	migrationsPath string
	adminAddr      string
	logLevel       string
}

var (
	errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required: the projector writes only to the analytical database")
	errMissingBrokers      = errors.New("KAFKA_BROKERS is required: the projector consumes the analytics topic")
)

// loadConfig reads the environment through getenv (os.Getenv in main).
func loadConfig(getenv func(string) string) (config, error) {
	or := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	c := config{
		analyticsURL:   getenv("ANALYTICS_DATABASE_URL"),
		group:          or("ANALYTICS_CONSUMER_GROUP", defaultConsumerGroup),
		migrationsPath: or("ANALYTICS_MIGRATIONS_PATH", defaultMigrationsPath),
		adminAddr:      or("ADMIN_ADDR", ":8091"),
		logLevel:       or("LOG_LEVEL", "info"),
	}
	if c.analyticsURL == "" {
		return config{}, errMissingAnalyticsURL
	}
	for _, b := range strings.Split(getenv("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.brokers = append(c.brokers, b)
		}
	}
	if len(c.brokers) == 0 {
		return config{}, errMissingBrokers
	}
	return c, nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("nip-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)

	// Runtime metrics + traces (ADR 0011). Admin-only process: no HTTP RED
	// middleware on the probe endpoints. Non-fatal: telemetry never blocks boot.
	shutdownTelemetry, terr := telemetry.SetupFromEnv(context.Background(), telemetry.ServiceProjector)
	if terr != nil {
		logger.Warn("telemetry setup failed; continuing without OTLP export", "error", terr)
	}
	defer func() {
		flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFlush()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry shutdown failed", "error", err)
		}
	}()

	pool, err := openAnalyticsPool(context.Background(), logger, cfg.analyticsURL, cfg.migrationsPath)
	if err != nil {
		return err
	}
	defer pool.Close() // runs last: after the consumer loop has stopped touching it

	consumer := inboundkafka.NewAnalyticsConsumer(cfg.brokers, outboundkafka.AnalyticsTopic, cfg.group,
		analyticsstore.NewProjection(pool), logger)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("closing the analytics consumer failed", "error", err)
		}
	}()

	var notReady atomic.Bool
	admin := &http.Server{Addr: cfg.adminAddr, Handler: newAdminMux(&notReady), ReadHeaderTimeout: 5 * time.Second}
	adminErr := make(chan error, 1)
	go func() {
		logger.Info("projector admin server listening", "addr", cfg.adminAddr)
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			adminErr <- err
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	consumerDone := make(chan error, 1)
	go func() {
		logger.Info("analytics consumer running", "topic", outboundkafka.AnalyticsTopic, "group_id", cfg.group, "brokers", cfg.brokers)
		consumerDone <- consumer.Run(consumerCtx)
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-consumerDone:
		// The consumer only returns on cancellation or an unrecoverable
		// reader/commit failure: exit non-zero so the pod restarts.
		return fmt.Errorf("analytics consumer stopped unexpectedly: %w", err)
	case err := <-adminErr:
		return fmt.Errorf("admin server failed: %w", err)
	case <-sig.Done():
	}

	// Graceful shutdown: flip readiness first, drain the admin server, then
	// let the consumer finish the message in flight (commit or dead-letter)
	// before the pool closes.
	notReady.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := admin.Shutdown(ctx)
	cancelConsumer()
	select {
	case <-consumerDone:
	case <-ctx.Done():
		logger.Warn("analytics consumer did not stop before the shutdown deadline")
	}
	return shutdownErr
}

// newAdminMux serves /healthz (liveness, never flipped) and /readyz (flips to
// 503 as the first step of shutdown).
func newAdminMux(notReady *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if notReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	return mux
}

// openAnalyticsPool applies the analytical migrations over the direct DSN,
// opens the writer pool and pings it, each under boot retry.
func openAnalyticsPool(ctx context.Context, logger *slog.Logger, url, migrationsPath string) (*pgxpool.Pool, error) {
	if err := bootretry.Retry(ctx, logger, "run analytics migrations", func() error {
		return postgres.RunMigrations(url, migrationsPath)
	}); err != nil {
		return nil, err
	}
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := bootretry.Retry(ctx, logger, "ping analytics database", func() error { return pool.Ping(ctx) }); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

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
