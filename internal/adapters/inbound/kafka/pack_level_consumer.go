package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// ProductMasterTopic is product-master's integration topic.
const ProductMasterTopic = "warehouse.product-master.events"

// The two product-master pack-level types, byte-identical to its
// apis/asyncapi.yaml on origin/develop (never guessed).
const (
	typePackLevelDeclared = "com.warehouse.wms.product-master.product.ProductPackLevelDeclared"
	typePackLevelRetired  = "com.warehouse.wms.product-master.product.ProductPackLevelRetired"
)

// packLevelConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const packLevelConsumerName = "pack-level-consumer"

// packLevelDeclaredData mirrors product-master's ProductPackLevelDeclared
// payload (snake_case, the fields this service reads), hand-mirrored locally.
type packLevelDeclaredData struct {
	SKU           string `json:"sku"`
	Level         string `json:"level"`
	GTIN          string `json:"gtin"`
	UnitsPerPack  int64  `json:"units_per_pack"`
	EffectiveFrom string `json:"effective_from"`
	Version       int64  `json:"version"`
}

// packLevelRetiredData mirrors product-master's ProductPackLevelRetired
// payload (the fields this service reads).
type packLevelRetiredData struct {
	SKU         string `json:"sku"`
	Level       string `json:"level"`
	GTIN        string `json:"gtin"`
	EffectiveTo string `json:"effective_to"`
	Version     int64  `json:"version"`
}

// PackLevelApplier is the application surface the consumer drives
// (usecases.ApplyPackLevel).
type PackLevelApplier interface {
	Declared(ctx context.Context, c usecases.PackLevelDeclaredChange) (usecases.PackLevelOutcome, error)
	Retired(ctx context.Context, c usecases.PackLevelRetiredChange) (usecases.PackLevelOutcome, error)
}

// PackLevelConsumer mirrors product-master's ProductPackLevelDeclared and
// ProductPackLevelRetired into the local pack_levels view (ADR 0017). Other
// types on the topic are ignored (full-type dispatch). Claim + apply commit in
// ONE unit of work; a payload that can never be applied is dead-lettered to
// "<topic>.dlq" and committed past; only infrastructure failures are retried.
type PackLevelConsumer struct {
	Reader          Reader
	Applier         PackLevelApplier
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy
	// DLQ receives poison messages; nil only logs them.
	DLQ DeadLetterWriter
	// DLQTopic names the dead-letter topic in logs.
	DLQTopic string

	sleep sleepFunc // test hook
}

// NewPackLevelConsumer constructs a PackLevelConsumer reading ProductMasterTopic
// under groupID (env-configured by the composition root:
// PACK_LEVEL_CONSUMER_GROUP).
func NewPackLevelConsumer(
	brokers []string,
	groupID string,
	applier PackLevelApplier,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *PackLevelConsumer {
	return &PackLevelConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, ProductMasterTopic, groupID)),
		Applier:         applier,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
		DLQ:             newDLQWriter(brokers, ProductMasterTopic+DLQSuffix),
		DLQTopic:        ProductMasterTopic + DLQSuffix,
	}
}

// Run consumes ProductMasterTopic until ctx is cancelled or the reader fails.
func (c *PackLevelConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: c.HandleKafkaMessage,
		logger: c.Logger,
		name:   "pack level consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage handles one raw value as if read from ProductMasterTopic.
func (c *PackLevelConsumer) HandleMessage(ctx context.Context, value []byte) error {
	return c.HandleKafkaMessage(ctx, kafkago.Message{Topic: ProductMasterTopic, Value: value})
}

// HandleKafkaMessage decodes one CloudEvents 1.0 message and applies it when
// it is a pack-level event. Non-nil ONLY for transient/infrastructure failures
// (a failed dead-letter write included); every deterministic outcome (not
// CloudEvents, unknown type, duplicate id, stale version) returns nil, and a
// payload that can never be applied is dead-lettered first.
func (c *PackLevelConsumer) HandleKafkaMessage(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents product-master message", "error", err)
		return nil
	}
	switch e.Type() {
	case typePackLevelDeclared:
		return c.handleDeclared(ctx, msg, e)
	case typePackLevelRetired:
		return c.handleRetired(ctx, msg, e)
	default:
		return nil // unknown type: ignore (full-type dispatch)
	}
}

func (c *PackLevelConsumer) handleDeclared(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data packLevelDeclaredData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the ProductPackLevelDeclared payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed ProductPackLevelDeclared payload %s: %w", e.ID(), err))
	}
	effectiveFrom, err := time.Parse(time.RFC3339, data.EffectiveFrom)
	if err != nil {
		//dlq:deterministic time.Parse of the payload's effective_from string (pure, no ctx, no I/O): the same bytes always fail
		return c.dlq().send(ctx, msg, fmt.Errorf("ProductPackLevelDeclared %s: unparsable effective_from: %w", e.ID(), err))
	}
	change := usecases.PackLevelDeclaredChange{
		SKU: data.SKU, Level: data.Level, GTIN: data.GTIN, UnitsPerPack: data.UnitsPerPack,
		EffectiveFrom: effectiveFrom, Version: data.Version,
	}
	return c.apply(ctx, msg, e, "ProductPackLevelDeclared", func(ctx context.Context) (usecases.PackLevelOutcome, error) {
		return c.Applier.Declared(ctx, change)
	})
}

func (c *PackLevelConsumer) handleRetired(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data packLevelRetiredData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the ProductPackLevelRetired payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed ProductPackLevelRetired payload %s: %w", e.ID(), err))
	}
	effectiveTo, err := time.Parse(time.RFC3339, data.EffectiveTo)
	if err != nil {
		//dlq:deterministic time.Parse of the payload's effective_to string (pure, no ctx, no I/O): the same bytes always fail
		return c.dlq().send(ctx, msg, fmt.Errorf("ProductPackLevelRetired %s: unparsable effective_to: %w", e.ID(), err))
	}
	change := usecases.PackLevelRetiredChange{
		SKU: data.SKU, Level: data.Level, GTIN: data.GTIN, EffectiveTo: effectiveTo, Version: data.Version,
	}
	return c.apply(ctx, msg, e, "ProductPackLevelRetired", func(ctx context.Context) (usecases.PackLevelOutcome, error) {
		return c.Applier.Retired(ctx, change)
	})
}

// apply runs claim + applier in ONE transaction. An invalid payload
// (usecases.ErrInvalidPackLevelEvent) is dead-lettered inside the transaction
// (a failed DLQ write rolls the claim back and is retried); any other error is
// transient and retried with the claim rolled back.
func (c *PackLevelConsumer) apply(ctx context.Context, msg kafkago.Message, e ce.Event, what string, op func(ctx context.Context) (usecases.PackLevelOutcome, error)) error {
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, packLevelConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed "+what+" event", "event_id", e.ID())
			return nil
		}
		outcome, err := op(ctx)
		if err != nil {
			if errors.Is(err, usecases.ErrInvalidPackLevelEvent) {
				//dlq:deterministic only usecases.ErrInvalidPackLevelEvent (raised solely by the pure packlevel.New / retirement field validation of the payload) reaches here; any other op error, cancellation included, is returned below as transient
				return c.dlq().send(ctx, msg, fmt.Errorf("%s %s refused: %w", what, e.ID(), err))
			}
			return fmt.Errorf("apply %s: %w", what, err)
		}
		c.Logger.InfoContext(ctx, "pack level event applied", "event", what, "event_id", e.ID(), "outcome", string(outcome))
		return nil
	})
}

func (c *PackLevelConsumer) dlq() deadLetterSink {
	return deadLetterSink{writer: c.DLQ, topic: c.DLQTopic, consumer: packLevelConsumerName, logger: c.Logger}
}

// Close releases the underlying Kafka reader and the dead-letter writer.
func (c *PackLevelConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
