package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/jackc/pgx/v5/pgxpool"
)

// consumerDeps carries the wired ports startConsumers needs.
type consumerDeps struct {
	uow             ports.UnitOfWork
	processedEvents ports.ProcessedEventRepository
	capabilities    ports.SiteCapabilityRepository
	demands         ports.SiteSkuDemandRepository
	plans           ports.PublishedCapacityPlanRepository
	replyApplier    inboundkafka.TransferReplyApplier
	factApplier     inboundkafka.TransferFactApplier
	packLevels      inboundkafka.PackLevelApplier
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
	maxAttempts := outboxMaxAttemptsFromEnv(logger)
	relay := postgres.NewOutboxRelay(pool, sink, postgres.WithMaxAttempts(maxAttempts), postgres.WithRelayLogger(logger))
	relayCtx, relayStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	runner := func() error {
		defer relayStop()
		defer func() { _ = sink.Close() }()
		logger.Info("outbox relay running", "topic", outboundkafka.TransferTopic, "brokers", brokers, "max_attempts", maxAttempts)
		return relay.Run(relayCtx)
	}
	return publisher, runner
}

// outboxMaxAttemptsFromEnv reads OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR
// 0016): how many failed sends one outbox row may accumulate before the relay
// excludes it from claims. Unset means postgres.DefaultOutboxMaxAttempts (20);
// 0 means unlimited. A negative or unparsable value logs a WARN and uses the
// default, so a typo can never silently remove the cap.
func outboxMaxAttemptsFromEnv(logger *slog.Logger) int {
	raw := os.Getenv(envOutboxMaxAttempts)
	if raw == "" {
		return postgres.DefaultOutboxMaxAttempts
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		logger.Warn("invalid OUTBOX_MAX_ATTEMPTS; using the default",
			"value", raw, "default", postgres.DefaultOutboxMaxAttempts, "error", err)
		return postgres.DefaultOutboxMaxAttempts
	}
	return n
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
			"enable_with", strings.Join([]string{envCapabilityGroup, envDemandGroup, envCapacityGroup, envPackLevelGroup}, ", "))
		return nil, nil, nil
	}
	// Unlike the other group ids (each its own off switch), the pack-level
	// group is MANDATORY whenever Kafka is configured: without the local pack
	// view every case SKU would silently stop rounding to whole cases.
	if os.Getenv(envPackLevelGroup) == "" {
		return nil, nil, fmt.Errorf("%s is required when %s is set (ADR 0017): no default group id is safe", envPackLevelGroup, envKafkaBrokers)
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
	spawn("pack-level", os.Getenv(envPackLevelGroup), func() consumer {
		return inboundkafka.NewPackLevelConsumer(brokers, os.Getenv(envPackLevelGroup), d.packLevels, d.processedEvents, d.uow, logger)
	})
	return runners, closers, nil
}
