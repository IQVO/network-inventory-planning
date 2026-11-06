package kafka

import (
	"context"
	"fmt"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// typeTransferStockAllocationReleased is the release fact of the transfer
// saga's compensation leg (ADR 0022), byte-identical to
// inventory-storage's apis/asyncapi.yaml.
const typeTransferStockAllocationReleased = "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased"

// transferStockAllocationReleasedData mirrors inventory-storage's
// TransferStockAllocationReleased payload (data {transfer_id,
// transfer_line_id, reservation_id, origin_site_id, sku, quantity,
// outcome, reason}, outcome RELEASED | ALREADY_RESOLVED; subject/key
// transfer_line_id — ADR 0022).
type transferStockAllocationReleasedData struct {
	TransferID     string `json:"transfer_id"`
	TransferLineID string `json:"transfer_line_id"`
	ReservationID  string `json:"reservation_id"`
	OriginSiteID   string `json:"origin_site_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	Outcome        string `json:"outcome"`
	Reason         string `json:"reason"`
}

// handleReleased applies inventory-storage's
// TransferStockAllocationReleased fact (ADR 0022): RELEASING → CANCELLED
// (terminal). The outcome (RELEASED | ALREADY_RESOLVED) settles the saga
// either way; the aggregate's own fact checks refuse anything that does
// not match the transfer being released.
func (c *TransferReplyConsumer) handleReleased(ctx context.Context, msg kafkago.Message, e ce.Event) error {
	var data transferStockAllocationReleasedData
	if err := e.DataAs(&data); err != nil {
		//dlq:deterministic e.DataAs decode failure of the TransferStockAllocationReleased payload bytes (pure JSON unmarshal, no ctx, no I/O): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("malformed TransferStockAllocationReleased payload %s: %w", e.ID(), err))
	}
	outcome := transfer.ReleaseOutcome(data.Outcome)
	if !outcome.Valid() {
		// Same condition, same treatment as the aggregate's own
		// checkReleaseFact: a contract break, dead-lettered.
		//dlq:deterministic ReleaseOutcome.Valid() is a pure check against the closed set (transfer.ErrFactRefused): the same bytes always fail, cancellation cannot produce it
		return c.dlq().send(ctx, msg, fmt.Errorf("%w: TransferStockAllocationReleased %s outcome %q", transfer.ErrFactRefused, e.ID(), data.Outcome))
	}
	in := usecases.ApplyReleasedInput{
		TransferID: transfer.TransferID(data.TransferID),
		Released: transfer.StockAllocationReleased{
			TransferLineID: data.TransferLineID,
			ReservationID:  data.ReservationID,
			OriginSiteID:   data.OriginSiteID,
			SKU:            data.SKU,
			Quantity:       data.Quantity,
			Outcome:        outcome,
			Reason:         data.Reason,
		},
		OccurredAt: e.Time().UTC(),
	}
	return c.apply(ctx, msg, e, "TransferStockAllocationReleased", data.TransferID, func(ctx context.Context) error {
		return c.Applier.ApplyReleased(ctx, in)
	})
}
