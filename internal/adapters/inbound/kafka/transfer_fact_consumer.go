package kafka

import (
	"context"
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
	}
}

// Run consumes FulfillmentTopic until ctx is cancelled or the reader fails.
func (c *TransferFactConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: c.Logger,
		name:   "transfer fact consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 fact and applies it. Non-nil
// ONLY for transient/infrastructure failures; every deterministic
// outcome — unknown type, malformed payload, unknown transfer_ref,
// out-of-order fact, duplicate id — returns nil after logging.
func (c *TransferFactConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents fulfillment fact", "error", err)
		return nil
	}

	switch e.Type() {
	case typeTransferPicked:
		return c.handlePicked(ctx, e)
	case typeTransferDispatched:
		return c.handleDispatched(ctx, e)
	case typeTransferArrived:
		return c.handleArrival(ctx, e)
	default:
		return nil // unknown type: ignore (full-type dispatch)
	}
}

func (c *TransferFactConsumer) handlePicked(ctx context.Context, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferPicked payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.Quantity <= 0 {
		// quantity is OPTIONAL on the wire; a pick fact without one
		// cannot drive the short-pick logic — deterministic skip.
		c.Logger.WarnContext(ctx, "skipping TransferPicked without a positive quantity", "event_id", e.ID(), "transfer_ref", data.TransferRef)
		return nil
	}
	in := usecases.ApplyPickInput{
		TransferID: transfer.TransferID(data.TransferRef),
		Picked:     transfer.Picked{PickedQuantity: data.Quantity},
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, e, "TransferPicked", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyPick(ctx, in)
	})
}

func (c *TransferFactConsumer) handleDispatched(ctx context.Context, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferDispatched payload", "error", err, "event_id", e.ID())
		return nil
	}
	in := usecases.FactInput{
		TransferID: transfer.TransferID(data.TransferRef),
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, e, "TransferDispatched", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyDispatched(ctx, in)
	})
}

func (c *TransferFactConsumer) handleArrival(ctx context.Context, e ce.Event) error {
	var data transferFactData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferArrived payload", "error", err, "event_id", e.ID())
		return nil
	}
	in := usecases.FactInput{
		TransferID: transfer.TransferID(data.TransferRef),
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, e, "TransferArrived", in.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyArrival(ctx, in)
	})
}

// apply runs claim + applier in ONE transaction. A deterministic apply
// failure — unknown transfer (WARN), out-of-order/replayed fact, refused
// payload — is logged and SKIPPED (the claim stays committed so the
// offset moves on); anything else is transient and retried with the
// claim rolled back.
func (c *TransferFactConsumer) apply(ctx context.Context, e ce.Event, what string, id transfer.TransferID, op func(ctx context.Context) error) error {
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

// Close releases the underlying Kafka reader.
func (c *TransferFactConsumer) Close() error {
	return c.Reader.Close()
}
