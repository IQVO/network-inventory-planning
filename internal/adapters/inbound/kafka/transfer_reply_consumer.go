package kafka

import (
	"context"
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
type TransferReplyConsumer struct {
	Reader          Reader
	Applier         TransferReplyApplier
	ProcessedEvents ReplyProcessedEvents
	UoW             ReplyUnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

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
	}
}

// Run consumes InventoryTopic until ctx is cancelled or the reader fails.
func (c *TransferReplyConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: c.Logger,
		name:   "transfer reply consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 reply and applies it. Non-nil
// ONLY for transient/infrastructure failures (the loop retries the same
// message); everything deterministic — unknown type, malformed payload,
// unknown transfer, illegal transition, duplicate id — returns nil after
// logging.
func (c *TransferReplyConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents inventory reply", "error", err)
		return nil
	}

	switch e.Type() {
	case typeTransferStockAllocated:
		return c.handleAllocated(ctx, e)
	case typeTransferStockAllocationRejected:
		return c.handleRejected(ctx, e)
	case typeTransferReceiptStaged:
		return c.handleReceiptStaged(ctx, e)
	case typeTransferStockStowed:
		return c.handleStowed(ctx, e)
	default:
		return nil // unknown type: ignore (full-type dispatch)
	}
}

func (c *TransferReplyConsumer) handleAllocated(ctx context.Context, e ce.Event) error {
	var data transferStockAllocatedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferStockAllocated payload", "error", err, "event_id", e.ID())
		return nil
	}
	expiresAt, err := time.Parse(time.RFC3339, data.ExpiresAt)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferStockAllocated with unparsable expires_at", "error", err, "event_id", e.ID())
		return nil
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

	// Claim + transition are ONE transaction: a failed apply rolls the
	// claim back, so the redelivery is processed, not skipped.
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferReplyConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed TransferStockAllocated event", "event_id", e.ID())
			return nil
		}
		if err := c.Applier.ApplyAllocation(ctx, in); err != nil {
			if usecases.IsDeterministic(err) {
				c.Logger.WarnContext(ctx, "skipping deterministic allocation-reply failure", "error", err, "event_id", e.ID(), "transfer_id", data.TransferID)
				return nil
			}
			return fmt.Errorf("apply TransferStockAllocated: %w", err)
		}
		c.Logger.InfoContext(ctx, "transfer allocated", "event_id", e.ID(), "transfer_id", data.TransferID, "reservation_id", data.ReservationID)
		return nil
	})
}

func (c *TransferReplyConsumer) handleRejected(ctx context.Context, e ce.Event) error {
	var data transferStockAllocationRejectedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferStockAllocationRejected payload", "error", err, "event_id", e.ID())
		return nil
	}
	reason := transfer.RejectionReason(data.Reason)
	if !reason.Valid() {
		c.Logger.WarnContext(ctx, "skipping TransferStockAllocationRejected with unknown reason", "reason", data.Reason, "event_id", e.ID())
		return nil
	}
	in := usecases.ApplyRejectionInput{
		TransferID: transfer.TransferID(data.TransferID),
		LineID:     data.TransferLineID,
		Reason:     reason,
		OccurredAt: e.Time().UTC(),
	}

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferReplyConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed TransferStockAllocationRejected event", "event_id", e.ID())
			return nil
		}
		if err := c.Applier.ApplyRejection(ctx, in); err != nil {
			if usecases.IsDeterministic(err) {
				c.Logger.WarnContext(ctx, "skipping deterministic rejection-reply failure", "error", err, "event_id", e.ID(), "transfer_id", data.TransferID)
				return nil
			}
			return fmt.Errorf("apply TransferStockAllocationRejected: %w", err)
		}
		c.Logger.InfoContext(ctx, "transfer unfulfillable", "event_id", e.ID(), "transfer_id", data.TransferID, "reason", data.Reason)
		return nil
	})
}

// handleReceiptStaged applies inventory-storage's TransferReceiptStaged
// destination fact: IN_TRANSIT → ARRIVED (the scan-driven receiving path).
// The transfer_id (not the line-keyed subject) is the Load key; the
// variance is informational (recorded in the log, not the aggregate — v1
// has no variance disposition workflow).
func (c *TransferReplyConsumer) handleReceiptStaged(ctx context.Context, e ce.Event) error {
	var data transferReceiptStagedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferReceiptStaged payload", "error", err, "event_id", e.ID())
		return nil
	}
	in := usecases.ApplyReceiptStagedInput{
		TransferID: transfer.TransferID(data.TransferID),
		LineID:     data.TransferLineID,
		OccurredAt: e.Time().UTC(),
	}

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferReplyConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed TransferReceiptStaged event", "event_id", e.ID())
			return nil
		}
		if err := c.Applier.ApplyReceiptStaged(ctx, in); err != nil {
			if usecases.IsDeterministic(err) || usecases.IsDeterministicFact(err) {
				c.Logger.WarnContext(ctx, "skipping deterministic TransferReceiptStaged failure (unknown or out-of-order transfer)", "error", err, "event_id", e.ID(), "transfer_id", data.TransferID)
				return nil
			}
			return fmt.Errorf("apply TransferReceiptStaged: %w", err)
		}
		c.Logger.InfoContext(ctx, "transfer receipt staged (ARRIVED)", "event_id", e.ID(), "transfer_id", data.TransferID, "variance", data.Variance)
		return nil
	})
}

// handleStowed applies inventory-storage's TransferStockStowed destination
// fact: ARRIVED → RECEIVED (terminal) with the destination stow
// allocations persisted.
func (c *TransferReplyConsumer) handleStowed(ctx context.Context, e ce.Event) error {
	var data transferStockStowedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed TransferStockStowed payload", "error", err, "event_id", e.ID())
		return nil
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

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, transferReplyConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed TransferStockStowed event", "event_id", e.ID())
			return nil
		}
		if err := c.Applier.ApplyStowed(ctx, in); err != nil {
			if usecases.IsDeterministic(err) || usecases.IsDeterministicFact(err) {
				c.Logger.WarnContext(ctx, "skipping deterministic TransferStockStowed failure (unknown or out-of-order transfer)", "error", err, "event_id", e.ID(), "transfer_id", data.TransferID)
				return nil
			}
			return fmt.Errorf("apply TransferStockStowed: %w", err)
		}
		c.Logger.InfoContext(ctx, "transfer stowed (RECEIVED, terminal)", "event_id", e.ID(), "transfer_id", data.TransferID, "stowed", data.StowedQuantity)
		return nil
	})
}

// Close releases the underlying Kafka reader.
func (c *TransferReplyConsumer) Close() error {
	return c.Reader.Close()
}
