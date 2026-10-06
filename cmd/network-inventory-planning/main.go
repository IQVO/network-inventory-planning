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
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/jackc/pgx/v5/pgxpool"
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
	envTransferGroup   = "TRANSFER_REPLY_CONSUMER_GROUP"
	envOutboxRelay     = "OUTBOX_RELAY_ENABLED"
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

	maxStaleness, err := maxStalenessFromEnv()
	if err != nil {
		return handler, nil, closeAll, err
	}

	wired, err := wireSagas(logger, pool, maxStaleness)
	if err != nil {
		return handler, nil, closeAll, err
	}
	handler.Simulate = wired.simulate
	handler.Approve = wired.approve
	runners = append(runners, wired.runners...)
	closers = append(closers, wired.closers...)

	return handler, runners, closeAll, nil
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

// sagaWiring is what wireSagas assembles: the two read/approve use cases
// plus the runner/closer funcs of the consumers and the outbox relay.
type sagaWiring struct {
	simulate *usecases.SimulateTransferOptions
	approve  *usecases.ApproveTransfer
	runners  []func() error
	closers  []func() error
}

// wireSagas wires the Phase-1 simulation, the Phase-2 approval saga, the
// transactional outbox relay and every inbound consumer over the pool.
func wireSagas(logger *slog.Logger, pool *pgxpool.Pool, maxStaleness time.Duration) (sagaWiring, error) {
	var out sagaWiring

	uow := postgres.NewUnitOfWork(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	snapshots := postgres.NewSnapshotRepo(pool)
	transfers := postgres.NewTransferRepo(pool)

	out.simulate = &usecases.SimulateTransferOptions{
		Snapshot:     snapshots,
		MaxStaleness: maxStaleness,
		Now:          time.Now,
	}

	// Transactional outbox: the approval writes TransferPlanApproved +
	// TransferAllocationRequested rows in its own transaction; the relay
	// drains them onto warehouse.network-inventory-planning.events.
	outboxPublisher, relayRunner := wireOutbox(logger, pool)
	if relayRunner != nil {
		out.runners = append(out.runners, relayRunner)
	}

	// The approval endpoint requires the outbox: without it the saga would
	// persist state and never emit the allocation command. Unconfigured
	// means the endpoint answers 503 (fail-closed), exactly like Simulate.
	if outboxPublisher != nil {
		out.approve = &usecases.ApproveTransfer{
			Transfers:    transfers,
			Events:       outboxPublisher,
			Snapshot:     snapshots,
			UoW:          uow,
			MaxStaleness: maxStaleness,
			Now:          time.Now,
		}
	} else {
		logger.Warn("KAFKA_BROKERS not configured; POST /v1/transfers:approve answers 503 (the saga cannot emit its allocation command without the outbox)")
	}

	replyAllocate := &usecases.ApplyTransferAllocation{Transfers: transfers, UoW: uow}
	replyReject := &usecases.ApplyTransferRejection{Transfers: transfers, UoW: uow}
	replyApplier := inboundkafka.ReplyUseCases{Allocate: *replyAllocate, Reject: *replyReject}

	consumerRunners, consumerClosers, err := startConsumers(logger, consumerDeps{
		uow:             uow,
		processedEvents: processedEvents,
		capabilities:    postgres.NewSiteCapabilityRepo(pool),
		demands:         postgres.NewSiteSkuDemandRepo(pool),
		plans:           postgres.NewPublishedCapacityPlanRepo(pool),
		replyApplier:    replyApplier,
	})
	if err != nil {
		return out, err
	}
	out.runners = append(out.runners, consumerRunners...)
	out.closers = append(out.closers, consumerClosers...)
	return out, nil
}

// consumerDeps carries the wired ports startConsumers needs.
type consumerDeps struct {
	uow             ports.UnitOfWork
	processedEvents ports.ProcessedEventRepository
	capabilities    ports.SiteCapabilityRepository
	demands         ports.SiteSkuDemandRepository
	plans           ports.PublishedCapacityPlanRepository
	replyApplier    inboundkafka.TransferReplyApplier
}

// wireOutbox builds the transactional-outbox publisher and, when
// OUTBOX_RELAY_ENABLED is set with brokers configured, the background
// relay runner draining outbox_events onto Kafka. Without brokers the
// publisher is nil (the approval endpoint then stays 503) and any rows
// written would wait for a later relay — the saga's durability never
// depends on the broker being up at approval time.
func wireOutbox(logger *slog.Logger, pool *pgxpool.Pool) (ports.TransferEventPublisher, func() error) {
	brokers := brokersFromEnv()
	if brokers == nil {
		logger.Warn("KAFKA_BROKERS not configured; outbox disabled (POST /v1/transfers:approve answers 503)",
			"enable_with", strings.Join([]string{envKafkaBrokers, envOutboxRelay}, ","))
		return nil, nil
	}
	encoder := outboundkafka.NewTransferEncoder()
	publisher := postgres.NewOutboxWriter(pool, encoder)
	if os.Getenv(envOutboxRelay) == "" {
		return publisher, nil
	}
	sink := outboundkafka.NewRelaySink(brokers)
	relay := postgres.NewOutboxRelay(pool, sink)
	relayCtx, relayStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	runner := func() error {
		defer relayStop()
		defer func() { _ = sink.Close() }()
		logger.Info("outbox relay running", "topic", outboundkafka.TransferTopic, "brokers", brokers)
		return relay.Run(relayCtx)
	}
	return publisher, runner
}

// brokersFromEnv returns the configured brokers or nil when unset.
func brokersFromEnv() []string {
	raw := os.Getenv(envKafkaBrokers)
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
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
	spawn("transfer-reply", os.Getenv(envTransferGroup), func() consumer {
		return inboundkafka.NewTransferReplyConsumer(brokers, os.Getenv(envTransferGroup), d.replyApplier, d.processedEvents, d.uow, logger)
	})
	return runners, closers, nil
}
