package kafka

import (
	"context"
	"fmt"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// The two destination facts (the receiving side of the saga, ADR 0005),
// byte-identical to inventory-storage's apis/asyncapi.yaml.
const (
	typeTransferReceiptStaged = "com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged"
	typeTransferStockStowed   = "com.warehouse.wms.inventory-storage.stock.TransferStockStowed"
)

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
