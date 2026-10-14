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
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// InventoryTopic is inventory-storage's integration topic (the reply
// stream of the transfer saga).
const InventoryTopic = "warehouse.inventory.events"

// The two reply types, byte-identical to inventory-storage's
// apis/asyncapi.yaml on origin/develop — never guessed.
const (
	typeTransferStockAllocated          = "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated"
	typeTransferStockAllocationRejected = "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected"
)

// The two destination facts (the receiving side of the saga, ADR 0005),
// byte-identical to inventory-storage's apis/asyncapi.yaml — the contract
// being built in parallel; the strings below are the agreed shape.
const (
	typeTransferReceiptStaged = "com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged"
	typeTransferStockStowed   = "com.warehouse.wms.inventory-storage.stock.TransferStockStowed"
)

// transferReplyConsumerName namespaces this consumer's processed-event rows.
const transferReplyConsumerName = "transfer-reply-consumer"

// transferStockAllocatedData mirrors inventory-storage's
// TransferStockAllocated payload (data {transfer_id, transfer_line_id,
// origin_site_id, reservation_id, sku, quantity, allocations[],
// expires_at}).
type transferStockAllocatedData struct {
	TransferID     string `json:"transfer_id"`
	TransferLineID string `json:"transfer_line_id"`
	OriginSiteID   string `json:"origin_site_id"`
	ReservationID  string `json:"reservation_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	Allocations    []struct {
		StockUnitID string `json:"stock_unit_id"`
		BinID       string `json:"bin_id"`
		Quantity    int    `json:"quantity"`
	} `json:"allocations"`
	ExpiresAt string `json:"expires_at"`
}

// transferStockAllocationRejectedData mirrors inventory-storage's
// TransferStockAllocationRejected payload (data {transfer_id,
// transfer_line_id, origin_site_id, sku, requested_quantity, reason}).
type transferStockAllocationRejectedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	OriginSiteID      string `json:"origin_site_id"`
	SKU               string `json:"sku"`
	RequestedQuantity int    `json:"requested_quantity"`
	Reason            string `json:"reason"`
}

// transferReceiptStagedData mirrors inventory-storage's
// TransferReceiptStaged payload (data {transfer_id, transfer_line_id,
// destination_site_id, sku, expected_quantity, received_quantity,
// variance}; subject/key transfer_line_id).
type transferReceiptStagedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	DestinationSiteID string `json:"destination_site_id"`
	SKU               string `json:"sku"`
	ExpectedQuantity  int    `json:"expected_quantity"`
	ReceivedQuantity  int    `json:"received_quantity"`
	Variance          int    `json:"variance"`
}

// transferStockStowedData mirrors inventory-storage's
// TransferStockStowed payload (data {transfer_id, transfer_line_id,
// destination_site_id, sku, received_quantity, stowed_quantity,
// allocations:[{stock_unit_id, bin_id, quantity}]}; subject/key
// transfer_line_id).
type transferStockStowedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	DestinationSiteID string `json:"destination_site_id"`
	SKU               string `json:"sku"`
	ReceivedQuantity  int    `json:"received_quantity"`
	StowedQuantity    int    `json:"stowed_quantity"`
	Allocations       []struct {
		StockUnitID string `json:"stock_unit_id"`
		BinID       string `json:"bin_id"`
		Quantity    int    `json:"quantity"`
	} `json:"allocations"`
}

// TransferReplyApplier is the application surface the reply consumer
// drives. implemented by ReplyUseCases (a pair of the two use cases).
type TransferReplyApplier interface {
	ApplyAllocation(ctx context.Context, in usecases.ApplyAllocationInput) error
	ApplyRejection(ctx context.Context, in usecases.ApplyRejectionInput) error
	// The two destination facts (ADR 0005), same topic, same consumer.
	ApplyReceiptStaged(ctx context.Context, in usecases.ApplyReceiptStagedInput) error
	ApplyStowed(ctx context.Context, in usecases.ApplyStowInput) error
}

// ReplyUseCases pairs the reply use cases into a TransferReplyApplier.
type ReplyUseCases struct {
	Allocate usecases.ApplyTransferAllocation
	Reject   usecases.ApplyTransferRejection
	Staged   usecases.ApplyTransferReceiptStaged
	Stow     usecases.ApplyTransferStow
}

// ApplyAllocation implements TransferReplyApplier.
func (r ReplyUseCases) ApplyAllocation(ctx context.Context, in usecases.ApplyAllocationInput) error {
	return r.Allocate.Execute(ctx, in)
}

// ApplyRejection implements TransferReplyApplier.
func (r ReplyUseCases) ApplyRejection(ctx context.Context, in usecases.ApplyRejectionInput) error {
	return r.Reject.Execute(ctx, in)
}

// ApplyReceiptStaged implements TransferReplyApplier.
func (r ReplyUseCases) ApplyReceiptStaged(ctx context.Context, in usecases.ApplyReceiptStagedInput) error {
	return r.Staged.Execute(ctx, in)
}

// ApplyStowed implements TransferReplyApplier.
func (r ReplyUseCases) ApplyStowed(ctx context.Context, in usecases.ApplyStowInput) error {
	return r.Stow.Execute(ctx, in)
}

// TransferReplyConsumer consumes inventory-storage's two transfer
// allocation replies on warehouse.inventory.events and drives the saga:
// Allocated → ALLOCATED (reservation_id, allocations, expires_at
// persisted); Rejected → UNFULFILLABLE with the closed reason. Dedupe on
// the CE id in processed_events, claim + transition commit in ONE
// UnitOfWork, offset committed only after the transaction settles.
//
// Poison replies (ADR 0013): a reply whose payload cannot be decoded or
// that the aggregate refuses (usecases.IsRefusedReply — line-id mismatch,
// unknown rejection reason, any other allocation-reply mismatch) is
// DETERMINISTIC. It is published raw to DLQTopic
// (warehouse.inventory.events.dlq) and committed past, never retried: a
// retry could only wedge the partition behind it forever.
type TransferReplyConsumer struct {
	Reader          Reader
	Applier         TransferReplyApplier
	ProcessedEvents ReplyProcessedEvents
	UoW             ReplyUnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy
	// DLQ receives poison replies; nil only logs them (unit tests).
	DLQ DeadLetterWriter
	// DLQTopic names the dead-letter topic in logs (InventoryTopic +
	// DLQSuffix).
	DLQTopic string

	sleep sleepFunc // test hook
}

// ReplyProcessedEvents is the idempotency port subset.
type ReplyProcessedEvents interface {
	Claim(ctx context.Context, consumer, eventID string) (bool, error)
}

// ReplyUnitOfWork is the atomicity port subset.
type ReplyUnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

// NewTransferReplyConsumer constructs a TransferReplyConsumer reading
// InventoryTopic under groupID (env-configured by the composition root).
// Its dead-letter writer targets InventoryTopic + DLQSuffix.
func NewTransferReplyConsumer(
	brokers []string,
	groupID string,
	applier TransferReplyApplier,
	processedEvents ReplyProcessedEvents,
	uow ReplyUnitOfWork,
	logger *slog.Logger,
) *TransferReplyConsumer {
	return &TransferReplyConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, InventoryTopic, groupID)),
		Applier:         applier,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
		DLQ:             newDLQWriter(brokers, InventoryTopic+DLQSuffix),
		DLQTopic:        InventoryTopic + DLQSuffix,
	}
}

// Run consumes InventoryTopic until ctx is cancelled or the reader fails.
func (c *TransferReplyConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: c.HandleKafkaMessage,
		logger: c.Logger,
		name:   "transfer reply consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage handles one raw reply value as if read from
// InventoryTopic (kept for callers that hold only the bytes).
func (c *TransferReplyConsumer) HandleMessage(ctx context.Context, value []byte) error {
	return c.HandleKafkaMessage(ctx, kafkago.Message{Topic: InventoryTopic, Value: value})
}

// HandleKafkaMessage decodes one CloudEvents 1.0 reply and applies it.
// Non-nil ONLY for transient/infrastructure failures (the loop retries the
// same message) — including a failed dead-letter write. Everything
// deterministic returns nil: unknown type, non-CloudEvents bytes and
// duplicate ids are skipped; unknown transfers and illegal transitions
// (replays, out-of-order replies) are logged and skipped; malformed or
// refused replies are dead-lettered.
func (c *TransferReplyConsumer) HandleKafkaMessage(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents inventory reply", "error", err)
		return nil
	}

	switch e.Type() {
	case typeTransferStockAllocated:
		return c.handleAllocated(ctx, msg, e)
	case typeTransferStockAllocationRejected:
		return c.handleRejected(ctx, msg, e)
	case typeTransferReceiptStaged:
		return c.handleReceiptStaged(ctx, msg, e)
	case typeTransferStockStowed:
		return c.handleStowed(ctx, msg, e)
	default:
		return nil // unknown type: ignore (full-type dispatch)
	}
}

func (c *TransferReplyConsumer) dlq() deadLetterSink {
	return deadLetterSink{writer: c.DLQ, topic: c.DLQTopic, consumer: transferReplyConsumerName, logger: c.Logger}
}

func (c *TransferReplyConsumer) handleAllocated(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferStockAllocatedData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferStockAllocated payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferStockAllocated payload %s: %w", e.ID(), err))
	}
	expiresAt, err := time.Parse(time.RFC3339, data.ExpiresAt)
	if err != nil {
		//dlq:deterministic time.Parse of the payload's expires_at string is pure: an unparsable value always fails, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("TransferStockAllocated %s has an unparsable expires_at: %w", e.ID(), err))
	}
	allocations := make([]transfer.Allocation, 0, len(data.Allocations))
	for _, a := range data.Allocations {
		allocations = append(allocations, transfer.Allocation{StockUnitID: a.StockUnitID, BinID: a.BinID, Quantity: a.Quantity})
	}
	in := usecases.ApplyAllocationInput{
		TransferID: transfer.TransferID(data.TransferID),
		Allocation: transfer.StockAllocation{
			TransferLineID: data.TransferLineID,
			OriginSiteID:   data.OriginSiteID,
			SKU:            data.SKU,
			ReservationID:  data.ReservationID,
			Quantity:       data.Quantity,
			Allocations:    allocations,
			ExpiresAt:      expiresAt,
		},
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferStockAllocated", data.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyAllocation(ctx, in)
	})
}

func (c *TransferReplyConsumer) handleRejected(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferStockAllocationRejectedData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferStockAllocationRejected payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferStockAllocationRejected payload %s: %w", e.ID(), err))
	}
	reason := transfer.RejectionReason(data.Reason)
	if !reason.Valid() {
		// Same condition, same treatment as the aggregate's own
		// ErrUnknownRejectionReason: a contract break, dead-lettered.
		//dlq:deterministic RejectionReason.Valid() is a pure check against the closed vocabulary (transfer.ErrUnknownRejectionReason): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("%w: %q on event %s", transfer.ErrUnknownRejectionReason, data.Reason, e.ID()))
	}
	in := usecases.ApplyRejectionInput{
		TransferID: transfer.TransferID(data.TransferID),
		LineID:     data.TransferLineID,
		Reason:     reason,
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferStockAllocationRejected", data.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyRejection(ctx, in)
	})
}

// handleReceiptStaged applies inventory-storage's TransferReceiptStaged
// destination fact: IN_TRANSIT → ARRIVED (the scan-driven receiving path).
// The transfer_id (not the line-keyed subject) is the Load key; the
// variance is informational (recorded in the log, not the aggregate — v1
// has no variance disposition workflow).
func (c *TransferReplyConsumer) handleReceiptStaged(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferReceiptStagedData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferReceiptStaged payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferReceiptStaged payload %s: %w", e.ID(), err))
	}
	in := usecases.ApplyReceiptStagedInput{
		TransferID: transfer.TransferID(data.TransferID),
		LineID:     data.TransferLineID,
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferReceiptStaged", data.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyReceiptStaged(ctx, in)
	})
}

// handleStowed applies inventory-storage's TransferStockStowed destination
// fact: ARRIVED → RECEIVED (terminal) with the destination stow
// allocations persisted.
func (c *TransferReplyConsumer) handleStowed(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferStockStowedData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferStockStowed payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferStockStowed payload %s: %w", e.ID(), err))
	}
	allocations := make([]transfer.StowAllocation, 0, len(data.Allocations))
	for _, a := range data.Allocations {
		allocations = append(allocations, transfer.StowAllocation{StockUnitID: a.StockUnitID, BinID: a.BinID, Quantity: a.Quantity})
	}
	in := usecases.ApplyStowInput{
		TransferID: transfer.TransferID(data.TransferID),
		Stowed: transfer.Stowed{
			TransferLineID:   data.TransferLineID,
			DestinationSite:  data.DestinationSiteID,
			SKU:              data.SKU,
			ReceivedQuantity: data.ReceivedQuantity,
			StowedQuantity:   data.StowedQuantity,
			Allocations:      allocations,
		},
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferStockStowed", data.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyStowed(ctx, in)
	})
}

// apply runs claim + use case in ONE transaction (a failed apply rolls the
// claim back, so a redelivery is processed, not skipped). Outcomes:
//
//   - refused reply (usecases.IsRefusedReply): dead-lettered INSIDE the
//     transaction, so a failed DLQ write rolls the claim back and the loop
//     retries; a successful one commits the claim and the offset moves on;
//   - unknown transfer / illegal transition (replay, out-of-order): WARN
//     and skip, claim committed;
//   - anything else: transient, returned for the loop to retry.
func (c *TransferReplyConsumer) apply(ctx context.Context, msg kafkago.Message, e ce.Event, what, transferID string, op func(ctx context.Context) error) error {
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferReplyConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed "+what+" event", "event_id", e.ID())
			return nil
		}
		err = op(ctx)
		switch {
		case err == nil:
			c.Logger.InfoContext(ctx, "transfer reply applied", "reply", what, "event_id", e.ID(), "transfer_id", transferID)
			return nil
		case usecases.IsRefusedReply(err):
			//dlq:deterministic only usecases.IsRefusedReply (transfer.ErrReplyRefused/ErrFactRefused, raised solely by the aggregate's payload validation in domain/transfer) reaches this case; the default case returns every other error, cancellation included, as transient
			return c.dlq().send(ctx, msg, fmt.Errorf("%s %s refused: %w", what, e.ID(), err))
		case usecases.IsDeterministic(err) || usecases.IsDeterministicFact(err):
			c.Logger.WarnContext(ctx, "skipping deterministic "+what+" failure (unknown transfer or replayed/out-of-order reply)", "error", err, "event_id", e.ID(), "transfer_id", transferID)
			return nil
		default:
			return fmt.Errorf("apply %s: %w", what, err)
		}
	})
}

// Close releases the underlying Kafka reader and the dead-letter writer.
func (c *TransferReplyConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
