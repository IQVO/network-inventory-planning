// Command nip-reports is the read-only HTTP reader of the
// network-inventory-planning analytics read side (ADR 0009): GET
// /reports/{transfer-funnel,state-dwell,stuck-transfers,rebalance-runs,freshness}
// and /healthz on :8092, answered from the ANALYTICAL database only (a
// read-only pool; the database role should be read-only too). It writes
// nothing and never opens the OLTP database or Kafka. No auth layer: access
// control is the in-cluster boundary (fleet rule).
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

	inboundhttp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/network-inventory-planning/internal/bootretry"
)

const shutdownTimeout = 10 * time.Second

var errMissingAnalyticsURL = errors.New("ANALYTICS_READER_DATABASE_URL (or ANALYTICS_DATABASE_URL) is required: the reports are served from the analytical database")

// config is the reports binary's environment.
type config struct {
	analyticsURL string
	httpAddr     string
	logLevel     string
}

// loadConfig reads the environment through getenv (os.Getenv in main). The
// reader DSN wins; ANALYTICS_DATABASE_URL (the projector's) is the fallback
// for local/dev runs with a single role.
func loadConfig(getenv func(string) string) (config, error) {
	or := func(fallback string, keys ...string) string {
		for _, k := range keys {
			if v := getenv(k); v != "" {
				return v
			}
		}
		return fallback
	}
	c := config{
		analyticsURL: or("", "ANALYTICS_READER_DATABASE_URL", "ANALYTICS_DATABASE_URL"),
		httpAddr:     or(":8092", "HTTP_ADDR"),
		logLevel:     or("info", "LOG_LEVEL"),
	}
	if c.analyticsURL == "" {
		return config{}, errMissingAnalyticsURL
	}
	return c, nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("nip-reports exited with error", "error", err)
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

	ctx := context.Background()

	// Metrics + traces over OTLP/gRPC (ADR 0011). Non-fatal; built BEFORE the
	// router so the HTTP RED middleware binds to the real MeterProvider.
	shutdownTelemetry, terr := telemetry.SetupFromEnv(ctx, telemetry.ServiceReports)
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

	pool, err := analyticsstore.NewReadOnlyPool(ctx, cfg.analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Retried: the first outbound dial of an injected pod is reset ~10s
	// after start (Istio native sidecars).
	if err := bootretry.Retry(ctx, logger, "ping analytics database", func() error { return pool.Ping(ctx) }); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           telemetry.HTTPMiddleware(telemetry.ServiceReports, (&inboundhttp.ReportsServer{Reader: analyticsstore.NewReader(pool)}).Routes()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("reports server listening", "addr", cfg.httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		return fmt.Errorf("reports server failed: %w", err)
	case <-sig.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
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
