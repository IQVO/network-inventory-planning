package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// FulfillmentTopic is fulfillment-execution's integration topic (its
// transfer facts, PR #162).
const FulfillmentTopic = "warehouse.fulfillment.events"

// The three fulfillment-execution transfer-fact types, byte-identical to
// its apis/asyncapi.yaml on origin/develop — never guessed.
const (
	typeTransferPicked     = "com.warehouse.wes.fulfillment-execution.transfer.TransferPicked"
	typeTransferDispatched = "com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched"
	typeTransferArrived    = "com.warehouse.wes.fulfillment-execution.transfer.TransferArrived"
)

// transferFactConsumerName namespaces this consumer's processed-event rows.
const transferFactConsumerName = "transfer-fact-consumer"

// transferFactData mirrors fulfillment-execution's TransferFactData
// payload: {transfer_ref, demand_id?, work_unit_id, task_id, work_kind,
// site_id?, sku?, quantity?} — subject/key is task_id.
type transferFactData struct {
	TransferRef string `json:"transfer_ref"`
	DemandID    string `json:"demand_id"`
	WorkUnitID  string `json:"work_unit_id"`
	TaskID      string `json:"task_id"`
	WorkKind    string `json:"work_kind"`
	SiteID      string `json:"site_id"`
	SKU         string `json:"sku"`
	Quantity    int    `json:"quantity"`
}

// TransferFactApplier is the application surface the fulfillment-fact
// consumer drives.
type TransferFactApplier interface {
	ApplyPick(ctx context.Context, in usecases.ApplyPickInput) error
	ApplyDispatched(ctx context.Context, in usecases.FactInput) error
	ApplyArrival(ctx context.Context, in usecases.FactInput) error
}

// FactUseCases triples the fulfillment-fact use cases into a
// TransferFactApplier.
type FactUseCases struct {
	Pick       usecases.ApplyTransferPick
	Dispatched usecases.ApplyTransferDispatched
	Arrival    usecases.ApplyTransferArrival
}

// ApplyPick implements TransferFactApplier.
func (f FactUseCases) ApplyPick(ctx context.Context, in usecases.ApplyPickInput) error {
	return f.Pick.Execute(ctx, in)
}

// ApplyDispatched implements TransferFactApplier.
func (f FactUseCases) ApplyDispatched(ctx context.Context, in usecases.FactInput) error {
	return f.Dispatched.Execute(ctx, in)
}

// ApplyArrival implements TransferFactApplier.
func (f FactUseCases) ApplyArrival(ctx context.Context, in usecases.FactInput) error {
	return f.Arrival.Execute(ctx, in)
}

// TransferFactConsumer consumes fulfillment-execution's three transfer
// facts on warehouse.fulfillment.events and drives the saga's
// fact-driven tail (ADR 0005):
//
//   - TransferPicked     → ALLOCATED → PICKED (picked_quantity recorded;
//     a short pick is recorded, not refused) + the dispatch
//     WorkDemandReleased carrying the PICKED quantity;
//   - TransferDispatched → PICKED → IN_TRANSIT;
//   - TransferArrived    → IN_TRANSIT → ARRIVED (reserved kind —
//     scan-driven receiving may bypass it via inventory-storage's
//     TransferReceiptStaged; both drive the same transition).
//
// A fact whose transfer_ref names no known transfer is WARN-logged and
// committed past, never retried and never crashed on: it belongs to a
// transfer this deployment never approved. Dedupe on the CE id in
// processed_events; claim + transition (+ outbox) commit in ONE
// UnitOfWork.
type TransferFactConsumer struct {
	Reader          Reader
	Applier         TransferFactApplier
	ProcessedEvents ReplyProcessedEvents
	UoW             ReplyUnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy
	// DLQ receives poison facts (ADR 0013); nil only logs them.
	DLQ DeadLetterWriter
	// DLQTopic names the dead-letter topic in logs (FulfillmentTopic +
	// DLQSuffix).
	DLQTopic string

	sleep sleepFunc // test hook
}

// NewTransferFactConsumer constructs a TransferFactConsumer reading
// FulfillmentTopic under groupID (env-configured by the composition
// root: TRANSFER_FACT_CONSUMER_GROUP).
func NewTransferFactConsumer(
	brokers []string,
	groupID string,
	applier TransferFactApplier,
	processedEvents ReplyProcessedEvents,
	uow ReplyUnitOfWork,
	logger *slog.Logger,
) *TransferFactConsumer {
	return &TransferFactConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, FulfillmentTopic, groupID)),
		Applier:         applier,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
		DLQ:             newDLQWriter(brokers, FulfillmentTopic+DLQSuffix),
		DLQTopic:        FulfillmentTopic + DLQSuffix,
	}
}

// Run consumes FulfillmentTopic until ctx is cancelled or the reader fails.
func (c *TransferFactConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: c.HandleKafkaMessage,
		logger: c.Logger,
		name:   "transfer fact consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage handles one raw fact value as if read from
// FulfillmentTopic (kept for callers that hold only the bytes).
func (c *TransferFactConsumer) HandleMessage(ctx context.Context, value []byte) error {
	return c.HandleKafkaMessage(ctx, kafkago.Message{Topic: FulfillmentTopic, Value: value})
}

// HandleKafkaMessage decodes one CloudEvents 1.0 fact and applies it.
// Non-nil ONLY for transient/infrastructure failures (including a failed
// dead-letter write); every deterministic outcome — unknown type,
// malformed payload, unknown transfer_ref, out-of-order fact, duplicate
// id — returns nil after logging, and a fact the aggregate refuses
// (transfer.ErrFactRefused) is dead-lettered first (ADR 0013).
func (c *TransferFactConsumer) HandleKafkaMessage(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents fulfillment fact", "error", err)
		return nil
	}

	switch e.Type() {
	case typeTransferPicked:
		return c.handlePicked(ctx, msg, e)
	case typeTransferDispatched:
		return c.handleDispatched(ctx, msg, e)
	case typeTransferArrived:
		return c.handleArrival(ctx, msg, e)
	default:
		return nil // unknown type: ignore (full-type dispatch)
	}
}

func (c *TransferFactConsumer) handlePicked(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferPicked payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferPicked payload %s: %w", e.ID(), err))
	}
	if data.Quantity <= 0 {
		// quantity is OPTIONAL on the wire; a pick fact without one
		// cannot drive the short-pick logic — a refused fact, dead-lettered
		// like every other refusal (ADR 0013).
		//dlq:deterministic payload validation of the decoded bytes (no positive quantity => transfer.ErrFactRefused), no ctx, no I/O: the same bytes always fail
		return c.dlq().send(ctx, msg, fmt.Errorf("%w: TransferPicked %s for %s carries no positive quantity", transfer.ErrFactRefused, e.ID(), data.TransferRef))
	}
	in := usecases.ApplyPickInput{
		TransferID: transfer.TransferID(data.TransferRef),
		Picked:     transfer.Picked{PickedQuantity: data.Quantity},
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferPicked", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyPick(ctx, in)
	})
}

func (c *TransferFactConsumer) handleDispatched(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferDispatched payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferDispatched payload %s: %w", e.ID(), err))
	}
	in := usecases.FactInput{
		TransferID: transfer.TransferID(data.TransferRef),
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferDispatched", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyDispatched(ctx, in)
	})
}

func (c *TransferFactConsumer) handleArrival(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferArrived payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferArrived payload %s: %w", e.ID(), err))
	}
	in := usecases.FactInput{
		TransferID: transfer.TransferID(data.TransferRef),
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferArrived", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyArrival(ctx, in)
	})
}

// apply runs claim + applier in ONE transaction. A refused payload
// (transfer.ErrFactRefused) is dead-lettered inside the transaction (a
// failed DLQ write rolls the claim back and is retried); any other
// deterministic apply failure — unknown transfer (WARN), out-of-order or
// replayed fact, an unconfigured release leg — is logged and SKIPPED (the
// claim stays committed so the offset moves on); anything else is
// transient and retried with the claim rolled back.
func (c *TransferFactConsumer) apply(ctx context.Context, msg kafkago.Message, e ce.Event, what string, id transfer.TransferID, op func(ctx context.Context) error) error {
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferFactConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed "+what+" event", "event_id", e.ID())
			return nil
		}
		if err := op(ctx); err != nil {
			if usecases.IsRefusedReply(err) {
				//dlq:deterministic only usecases.IsRefusedReply (transfer.ErrReplyRefused/ErrFactRefused, raised solely by the aggregate's payload validation in domain/transfer) reaches here; any other op error, cancellation included, is returned below as transient
				return c.dlq().send(ctx, msg, fmt.Errorf("%s %s refused: %w", what, e.ID(), err))
			}
			if usecases.IsDeterministicFact(err) {
				c.Logger.WarnContext(ctx, "skipping deterministic "+what+" failure (unknown or out-of-order transfer_ref)", "error", err, "event_id", e.ID(), "transfer_ref", string(id))
				return nil
			}
			return fmt.Errorf("apply %s: %w", what, err)
		}
		c.Logger.InfoContext(ctx, "transfer fact applied", "fact", what, "event_id", e.ID(), "transfer_ref", string(id))
		return nil
	})
}

func (c *TransferFactConsumer) dlq() deadLetterSink {
	return deadLetterSink{writer: c.DLQ, topic: c.DLQTopic, consumer: transferFactConsumerName, logger: c.Logger}
}

// Close releases the underlying Kafka reader and the dead-letter writer.
func (c *TransferFactConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
