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
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Environment of the Phase-1 read-model side. Each consumer group has NO
// default: an unset variable means that consumer does not exist (a default
// would let a locally run process join the live cluster's group).
const (
	envDatabaseURL     = "DATABASE_URL"
	envMigrationsPath  = "MIGRATIONS_PATH"
	envMigrationsDBURL = "MIGRATIONS_DATABASE_URL"
	envKafkaBrokers    = "KAFKA_BROKERS"
	envCapabilityGroup = "SITE_CAPABILITY_CONSUMER_GROUP"
	envDemandGroup     = "SITE_SKU_DEMAND_CONSUMER_GROUP"
	envCapacityGroup   = "CAPACITY_PLAN_CONSUMER_GROUP"
	envTransferGroup   = "TRANSFER_REPLY_CONSUMER_GROUP"
	envTransferFactGrp = "TRANSFER_FACT_CONSUMER_GROUP"
	envOutboxRelay     = "OUTBOX_RELAY_ENABLED"
	envMaxStaleness    = "PLANNING_MAX_STALENESS"
	envPickPathID      = "TRANSFER_PICK_PATH_ID"
	envPickCPTOffset   = "TRANSFER_PICK_CPT_OFFSET"
	envDispatchPathID  = "TRANSFER_DISPATCH_PATH_ID"
	envDispatchCPTOffs = "TRANSFER_DISPATCH_CPT_OFFSET"
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

func run() error {
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// W3C Trace Context is the fleet's Kafka propagation format (ADR
	// 0006): the global propagator is what every consumer's Extract and
	// every encoder's Inject go through. OTel's default is a NO-OP
	// propagator, so without this line the header carriers exist but
	// move nothing. A full tracer provider (OTLP export) is a later
	// slice; propagation works with the no-op tracer — Inject only needs
	// a span on ctx, which inbound extraction provides.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

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
	handler.Approve = wired.approve
	handler.ListRebalanceRuns = wired.runs
	runners = append(runners, wired.runners...)
	closers = append(closers, wired.closers...)

	return handler, runners, closeAll, nil
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

// defaultCPTOffset is the release CPT horizon used when a leg's
// TRANSFER_*_CPT_OFFSET is unset: 2h out for the pick, 3h for the
// dispatch (the dispatch happens after the pick, so its CPT sits further
// out to preserve the pick-before-dispatch slack).
const (
	defaultPickCPTOffset   = 2 * time.Hour
	defaultDispatchCPTOffs = 3 * time.Hour
)

// cptOffsetFromEnv resolves one TRANSFER_*_CPT_OFFSET (default d).
func cptOffsetFromEnv(envName string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(envName)
	if raw == "" {
		return def, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", envName, raw)
	}
	return parsed, nil
}

// workReleaseConfigFromEnv resolves the ADR 0005 release configuration.
// TRANSFER_PICK_PATH_ID deliberately has NO default: unset means the
// approval endpoint answers 503 config-incomplete (fail-closed) rather
// than minting transfers whose pick demand could not be released.
func workReleaseConfigFromEnv(logger *slog.Logger) usecases.WorkReleaseConfig {
	pickOffset, err := cptOffsetFromEnv(envPickCPTOffset, defaultPickCPTOffset)
	if err != nil {
		logger.Warn("invalid TRANSFER_PICK_CPT_OFFSET; using default", "error", err, "default", defaultPickCPTOffset)
		pickOffset = defaultPickCPTOffset
	}
	dispatchOffset, err := cptOffsetFromEnv(envDispatchCPTOffs, defaultDispatchCPTOffs)
	if err != nil {
		logger.Warn("invalid TRANSFER_DISPATCH_CPT_OFFSET; using default", "error", err, "default", defaultDispatchCPTOffs)
		dispatchOffset = defaultDispatchCPTOffs
	}
	return usecases.WorkReleaseConfig{
		PickPathID:        os.Getenv(envPickPathID),
		PickCPTOffset:     pickOffset,
		DispatchPathID:    os.Getenv(envDispatchPathID),
		DispatchCPTOffset: dispatchOffset,
	}
}

// sagaWiring is what wireSagas assembles: the two read/approve use cases
// plus the runner/closer funcs of the consumers and the outbox relay.
type sagaWiring struct {
	simulate *usecases.SimulateTransferOptions
	approve  *usecases.ApproveTransfer
	runs     *usecases.ListRebalanceRuns
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
	// drains them onto warehouse.network-inventory-planning.events (and,
	// since ADR 0007, the analytics occurrences onto
	// warehouse.network-inventory-planning.analytics — the same relay,
	// the rows carry their own topic).
	outboxPublisher, relayRunner := wireOutbox(logger, pool)
	if relayRunner != nil {
		out.runners = append(out.runners, relayRunner)
	}

	// Work-release configuration (ADR 0005): the pick leg's path has no
	// default, so an unconfigured deployment fails approval closed (503
	// config-incomplete) instead of minting transfers whose pick demand
	// could never be released.
	release := workReleaseConfigFromEnv(logger)

	// The approval endpoint requires the outbox AND a valid pick-leg
	// release config: without either the saga would persist state and
	// never emit the allocation command or the pick demand. Unconfigured
	// means the endpoint answers 503 (fail-closed), exactly like Simulate.
	if outboxPublisher != nil {
		approve := &usecases.ApproveTransfer{
			Transfers:    transfers,
			Events:       outboxPublisher,
			Snapshot:     snapshots,
			UoW:          uow,
			MaxStaleness: maxStaleness,
			Release:      release,
			Now:          time.Now,
		}
		if err := approve.Release.Validate(); err != nil {
			logger.Warn("work release not configured; POST /v1/transfers:approve answers 503 (no transfer may be approved without a releasable pick leg)",
				"error", err, "enable_with", strings.Join([]string{envPickPathID, envPickCPTOffset}, ","))
			out.approve = approve // non-nil: the endpoint answers the typed 503 with the missing-var detail
		} else {
			out.approve = approve
		}
	} else {
		logger.Warn("KAFKA_BROKERS not configured; POST /v1/transfers:approve answers 503 (the saga cannot emit its allocation command without the outbox)")
	}

	replyAllocate := &usecases.ApplyTransferAllocation{
		Transfers: transfers,
		Events:    outboxPublisher,
		UoW:       uow,
		Release:   release,
		Now:       time.Now,
	}
	replyReject := &usecases.ApplyTransferRejection{Transfers: transfers, UoW: uow}
	replyStaged := &usecases.ApplyTransferReceiptStaged{Transfers: transfers, UoW: uow}
	replyStow := &usecases.ApplyTransferStow{Transfers: transfers, UoW: uow}
	replyApplier := inboundkafka.ReplyUseCases{Allocate: *replyAllocate, Reject: *replyReject, Staged: *replyStaged, Stow: *replyStow}

	factPick := &usecases.ApplyTransferPick{Transfers: transfers, Events: outboxPublisher, UoW: uow, Release: release}
	factDispatched := &usecases.ApplyTransferDispatched{Transfers: transfers, UoW: uow}
	factArrival := &usecases.ApplyTransferArrival{Transfers: transfers, UoW: uow}
	factApplier := inboundkafka.FactUseCases{Pick: *factPick, Dispatched: *factDispatched, Arrival: *factArrival}

	consumerRunners, consumerClosers, err := startConsumers(logger, consumerDeps{
		uow:             uow,
		processedEvents: processedEvents,
		capabilities:    postgres.NewSiteCapabilityRepo(pool),
		demands:         postgres.NewSiteSkuDemandRepo(pool),
		plans:           postgres.NewPublishedCapacityPlanRepo(pool),
		replyApplier:    replyApplier,
		factApplier:     factApplier,
	})
	if err != nil {
		return out, err
	}
	out.runners = append(out.runners, consumerRunners...)
	out.closers = append(out.closers, consumerClosers...)

	// Phase-4 observability + scheduled runs (ADR 0007). Both tickers are
	// OFF unless their env var is set (the health check defaults to 5m;
	// the rebalance schedule has NO default — a planner pass on a shared
	// fleet must be a deliberate deployment choice).
	rebalanceRuns := postgres.NewRebalanceRunRepo(pool)
	out.runs = &usecases.ListRebalanceRuns{Runs: rebalanceRuns}
	wireHealthTicker(logger, pool, outboxPublisher, &out)
	wireRebalanceTicker(logger, pool, snapshots, rebalanceRuns, outboxPublisher, maxStaleness, &out)
	return out, nil
}

// optionalIntervalFromEnv parses an interval env var. Unset means def;
// "0" means DISABLED (ok=false); anything unparsable or negative is a
// boot failure (a typo'd ticker interval would silently change cadence).
// A def of 0 with the variable unset ALSO disables (the rebalance
// schedule's default-off contract).
func optionalIntervalFromEnv(name string, def time.Duration) (d time.Duration, ok bool, err error) {
	raw := os.Getenv(name)
	if raw == "" {
		if def <= 0 {
			return 0, false, nil
		}
		return def, true, nil
	}
	parsed, perr := time.ParseDuration(raw)
	if perr != nil || parsed < 0 {
		return 0, false, fmt.Errorf("%s must be a non-negative duration, got %q", name, raw)
	}
	if parsed == 0 {
		return 0, false, nil
	}
	return parsed, true, nil
}

// wireHealthTicker starts the bounded saga-health check loop (ADR 0007):
// every NIP_HEALTH_CHECK_INTERVAL (default 5m, 0=off) it reads the
// non-terminal transfers and publishes TransferStuckDetected occurrences
// for those past their per-state threshold (NIP_STUCK_THRESHOLDS). The
// loop ONLY reads and publishes — it never mutates saga state.
func wireHealthTicker(logger *slog.Logger, pool *pgxpool.Pool, events ports.TransferEventPublisher, out *sagaWiring) {
	interval, ok, err := optionalIntervalFromEnv(envHealthInterval, defaultHealthInterval)
	if err != nil {
		logger.Error("invalid health check interval; check disabled", "error", err)
		return
	}
	if !ok {
		logger.Info("saga health check disabled", "env", envHealthInterval)
		return
	}
	if events == nil {
		logger.Warn("saga health check disabled: no outbox publisher configured (occurrences could never be drained)",
			"enable_with", strings.Join([]string{envKafkaBrokers, envOutboxRelay}, ","))
		return
	}
	thresholds, err := transfer.ParseStuckThresholds(os.Getenv(envStuckThresholds))
	if err != nil {
		logger.Error("invalid stuck thresholds; check disabled", "error", err, "env", envStuckThresholds)
		return
	}
	check := usecases.CheckStuckTransfers{
		Reader: postgres.NewTransferRepo(pool),
		Events: events,
		Check:  transfer.StuckCheck{Thresholds: thresholds},
		Limit:  500,
		Now:    time.Now,
	}
	runner := usecases.PeriodicRunner{
		Name:     "saga health check",
		Interval: interval,
		Tick: func(ctx context.Context) error {
			_, err := check.Execute(ctx)
			return err
		},
		Logger: logger,
	}
	hcCtx, hcStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	out.runners = append(out.runners, func() error { defer hcStop(); return runner.Run(hcCtx) })
	logger.Info("saga health check running",
		"interval", interval.String(), "thresholds_env", envStuckThresholds)
}

// wireRebalanceTicker starts the scheduled rebalance loop (ADR 0007):
// every NIP_REBALANCE_SCHEDULE (NO default — unset means off) it runs the
// SAME fail-closed snapshot build + planner as the simulation, persists a
// rebalance_runs row and publishes a RebalanceRunCompleted occurrence.
// Observe-only: no approval, no allocation command — ever.
func wireRebalanceTicker(
	logger *slog.Logger,
	pool *pgxpool.Pool,
	snapshots ports.PlanningSnapshotRepository,
	runs ports.RebalanceRunRepository,
	events ports.TransferEventPublisher,
	maxStaleness time.Duration,
	out *sagaWiring,
) {
	interval, ok, err := optionalIntervalFromEnv(envRebalSchedule, 0)
	if err != nil {
		logger.Error("invalid rebalance schedule; scheduled runs disabled", "error", err)
		return
	}
	if !ok {
		logger.Info("scheduled rebalance disabled (no default; deliberate deployment choice)",
			"enable_with", envRebalSchedule)
		return
	}
	if events == nil {
		logger.Warn("scheduled rebalance disabled: no outbox publisher configured",
			"enable_with", strings.Join([]string{envKafkaBrokers, envOutboxRelay, envRebalSchedule}, ","))
		return
	}
	run := usecases.RunScheduledRebalance{
		Snapshot:     snapshots,
		Planner:      transfer.Planner{},
		Runs:         runs,
		Events:       events,
		MaxStaleness: maxStaleness,
		Now:          time.Now,
	}
	runner := usecases.PeriodicRunner{
		Name:     "scheduled rebalance",
		Interval: interval,
		Tick: func(ctx context.Context) error {
			_, err := run.Execute(ctx)
			return err
		},
		Logger: logger,
	}
	rbCtx, rbStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	out.runners = append(out.runners, func() error { defer rbStop(); return runner.Run(rbCtx) })
	logger.Info("scheduled rebalance running", "interval", interval.String())
}

// consumerDeps carries the wired ports startConsumers needs.
type consumerDeps struct {
	uow             ports.UnitOfWork
	processedEvents ports.ProcessedEventRepository
	capabilities    ports.SiteCapabilityRepository
	demands         ports.SiteSkuDemandRepository
	plans           ports.PublishedCapacityPlanRepository
	replyApplier    inboundkafka.TransferReplyApplier
	factApplier     inboundkafka.TransferFactApplier
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
	// Both encoders fan out over the same outbox rows: the integration
	// encoder keeps warehouse.network-inventory-planning.events, the
	// analytics encoder adds warehouse.network-inventory-planning.analytics
	// (ADR 0007). The relay drains rows by each row's own topic.
	encoder := outboundkafka.NewTransferEncoder()
	analyticsEncoder := outboundkafka.NewAnalyticsEncoder()
	publisher := postgres.NewOutboxWriter(pool, encoder, analyticsEncoder)
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
	spawn("transfer-fact", os.Getenv(envTransferFactGrp), func() consumer {
		return inboundkafka.NewTransferFactConsumer(brokers, os.Getenv(envTransferFactGrp), d.factApplier, d.processedEvents, d.uow, logger)
	})
	return runners, closers, nil
}
