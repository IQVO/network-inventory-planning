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
	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// Environment of the Phase-1 read-model side. Each consumer group has NO
// default: an unset variable means that consumer does not exist (a default
// would let a locally run process join the live cluster's group).
const (
	envDatabaseURL     = "DATABASE_URL"
	envMigrationsPath  = "MIGRATIONS_PATH"
	envKafkaBrokers    = "KAFKA_BROKERS"
	envCapabilityGroup = "SITE_CAPABILITY_CONSUMER_GROUP"
	envDemandGroup     = "SITE_SKU_DEMAND_CONSUMER_GROUP"
	envCapacityGroup   = "CAPACITY_PLAN_CONSUMER_GROUP"
	envMaxStaleness    = "PLANNING_MAX_STALENESS"
)

const defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"

// defaultMaxStaleness is the fail-closed freshness budget of the planning
// snapshot when PLANNING_MAX_STALENESS is unset.
const defaultMaxStaleness = 10 * time.Minute

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	for _, r := range runners {
		go func(r func() error) {
			if err := r(); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("consumer stopped", "error", err)
			}
		}(r)
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
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
	if err := postgres.RunMigrations(databaseURL, migrationsPath); err != nil {
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

	uow := postgres.NewUnitOfWork(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	capabilities := postgres.NewSiteCapabilityRepo(pool)
	demands := postgres.NewSiteSkuDemandRepo(pool)
	plans := postgres.NewPublishedCapacityPlanRepo(pool)
	snapshots := postgres.NewSnapshotRepo(pool)

	maxStaleness := defaultMaxStaleness
	if raw := os.Getenv(envMaxStaleness); raw != "" {
		parsed, parseErr := time.ParseDuration(raw)
		if parseErr != nil || parsed <= 0 {
			return handler, nil, closeAll, fmt.Errorf("%s must be a positive duration, got %q", envMaxStaleness, raw)
		}
		maxStaleness = parsed
	}
	handler.Simulate = &usecases.SimulateTransferOptions{
		Snapshot:     snapshots,
		MaxStaleness: maxStaleness,
		Now:          time.Now,
	}

	consumerRunners, consumerClosers, err := startConsumers(logger, consumerDeps{
		uow:             uow,
		processedEvents: processedEvents,
		capabilities:    capabilities,
		demands:         demands,
		plans:           plans,
	})
	if err != nil {
		return handler, nil, closeAll, err
	}
	runners = append(runners, consumerRunners...)
	closers = append(closers, consumerClosers...)

	return handler, runners, closeAll, nil
}

// consumerDeps carries the wired ports startConsumers needs.
type consumerDeps struct {
	uow             ports.UnitOfWork
	processedEvents ports.ProcessedEventRepository
	capabilities    ports.SiteCapabilityRepository
	demands         ports.SiteSkuDemandRepository
	plans           ports.PublishedCapacityPlanRepository
}

// consumer is the run/close shape every inbound consumer satisfies.
type consumer interface {
	Run(ctx context.Context) error
	Close() error
}

// startConsumers builds every ENABLED consumer (each group env var is its
// own off switch) and returns its Run funcs plus its Close funcs.
func startConsumers(logger *slog.Logger, d consumerDeps) ([]func() error, []func() error, error) {
	raw := os.Getenv(envKafkaBrokers)
	if raw == "" {
		logger.Warn("KAFKA_BROKERS not configured; read-model Kafka ingestion is disabled",
			"enable_with", strings.Join([]string{envCapabilityGroup, envDemandGroup, envCapacityGroup}, ", "))
		return nil, nil, nil
	}
	brokers := strings.Split(raw, ",")

	var runners, closers []func() error
	spawn := func(name, group string, build func() consumer) {
		if group == "" {
			logger.Info("consumer disabled", "name", name)
			return
		}
		c := build()
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		runners = append(runners, func() error { defer stop(); return c.Run(ctx) })
		closers = append(closers, c.Close)
		logger.Info("consumer running", "name", name, "group_id", group, "brokers", brokers)
	}

	spawn("site-capability", os.Getenv(envCapabilityGroup), func() consumer {
		return inboundkafka.NewSiteCapabilityConsumer(brokers, os.Getenv(envCapabilityGroup), d.capabilities, d.processedEvents, d.uow, logger)
	})
	spawn("site-sku-demand", os.Getenv(envDemandGroup), func() consumer {
		return inboundkafka.NewSiteSkuDemandConsumer(brokers, os.Getenv(envDemandGroup), d.demands, d.processedEvents, d.uow, logger)
	})
	spawn("capacity-plan", os.Getenv(envCapacityGroup), func() consumer {
		return inboundkafka.NewCapacityPlanConsumer(brokers, os.Getenv(envCapacityGroup), d.plans, d.processedEvents, d.uow, logger)
	})
	return runners, closers, nil
}
