package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// fakeReplyApplier records the applied replies.
type fakeReplyApplier struct {
	mu          sync.Mutex
	allocations []usecases.ApplyAllocationInput
	rejections  []usecases.ApplyRejectionInput
	staged      []usecases.ApplyReceiptStagedInput
	stowed      []usecases.ApplyStowInput
	failAlloc   int
}

func (f *fakeReplyApplier) ApplyAllocation(_ context.Context, in usecases.ApplyAllocationInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAlloc > 0 {
		f.failAlloc--
		return fmt.Errorf("injected transient failure")
	}
	f.allocations = append(f.allocations, in)
	return nil
}

func (f *fakeReplyApplier) ApplyRejection(_ context.Context, in usecases.ApplyRejectionInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejections = append(f.rejections, in)
	return nil
}

func (f *fakeReplyApplier) ApplyReceiptStaged(_ context.Context, in usecases.ApplyReceiptStagedInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staged = append(f.staged, in)
	return nil
}

func (f *fakeReplyApplier) ApplyStowed(_ context.Context, in usecases.ApplyStowInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stowed = append(f.stowed, in)
	return nil
}

func newReplyConsumer(applier TransferReplyApplier, claims ReplyProcessedEvents) *TransferReplyConsumer {
	return &TransferReplyConsumer{
		Applier:         applier,
		ProcessedEvents: claims,
		UoW:             passUoW{},
		Logger:          quietLogger(),
	}
}

var replyOccurred = time.Date(2026, 10, 6, 12, 30, 0, 0, time.UTC)

func allocatedReplyValue(t *testing.T, transferID string) []byte {
	t.Helper()
	return encodeEvent(t, typeTransferStockAllocated, transferID+":1", replyOccurred, transferStockAllocatedData{
		TransferID:     transferID,
		TransferLineID: transferID + ":1",
		OriginSiteID:   "WH1",
		ReservationID:  "res-77",
		SKU:            "SKU-1",
		Quantity:       10,
		Allocations: []struct {
			StockUnitID string `json:"stock_unit_id"`
			BinID       string `json:"bin_id"`
			Quantity    int    `json:"quantity"`
		}{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
		ExpiresAt: replyOccurred.Add(24 * time.Hour).Format(time.RFC3339),
	})
}

func rejectedReplyValue(t *testing.T, transferID, reason string) []byte {
	t.Helper()
	return encodeEvent(t, typeTransferStockAllocationRejected, transferID+":1", replyOccurred, transferStockAllocationRejectedData{
		TransferID:        transferID,
		TransferLineID:    transferID + ":1",
		OriginSiteID:      "WH1",
		SKU:               "SKU-1",
		RequestedQuantity: 10,
		Reason:            reason,
	})
}

func TestTransferReplyConsumerAppliesAllocated(t *testing.T) {
	applier := &fakeReplyApplier{}
	c := newReplyConsumer(applier, newFakeClaims())

	if err := c.HandleMessage(context.Background(), allocatedReplyValue(t, "trf-a")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(applier.allocations) != 1 {
		t.Fatalf("applied allocations = %d", len(applier.allocations))
	}
	got := applier.allocations[0]
	if got.TransferID != transfer.TransferID("trf-a") || got.Allocation.ReservationID != "res-77" {
		t.Fatalf("applied = %+v", got)
	}
	if len(got.Allocation.Allocations) != 1 || got.Allocation.Allocations[0].StockUnitID != "su-1" {
		t.Fatalf("allocations = %+v", got.Allocation.Allocations)
	}
	if !got.Allocation.ExpiresAt.Equal(replyOccurred.Add(24 * time.Hour)) {
		t.Fatalf("expires at = %v", got.Allocation.ExpiresAt)
	}
	if !got.OccurredAt.Equal(replyOccurred) {
		t.Fatalf("occurred at = %v", got.OccurredAt)
	}

	// Redelivery of the same CE id is a no-op (dedupe claim).
	if err := c.HandleMessage(context.Background(), allocatedReplyValue(t, "trf-a")); err != nil {
		t.Fatalf("redelivery error: %v", err)
	}
	if len(applier.allocations) != 1 {
		t.Fatalf("applied allocations after redelivery = %d, want 1", len(applier.allocations))
	}
}

func TestTransferReplyConsumerAppliesRejected(t *testing.T) {
	applier := &fakeReplyApplier{}
	c := newReplyConsumer(applier, newFakeClaims())

	if err := c.HandleMessage(context.Background(), rejectedReplyValue(t, "trf-b", "INSUFFICIENT_USABLE")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(applier.rejections) != 1 {
		t.Fatalf("applied rejections = %d", len(applier.rejections))
	}
	got := applier.rejections[0]
	if got.TransferID != transfer.TransferID("trf-b") || got.Reason != transfer.RejectionInsufficientUsable {
		t.Fatalf("applied = %+v", got)
	}
}

func TestTransferReplyConsumerDeterministicSkips(t *testing.T) {
	applier := &fakeReplyApplier{}
	c := newReplyConsumer(applier, newFakeClaims())
	occurred := replyOccurred

	// Unknown type on the inventory topic.
	other := encodeEvent(t, "com.warehouse.wms.inventory-storage.stock.StockUnitRegistered", "su-9", occurred, map[string]string{"stock_unit_id": "su-9"})
	if err := c.HandleMessage(context.Background(), other); err != nil {
		t.Fatalf("unknown type must be ignored: %v", err)
	}
	// Not CloudEvents.
	if err := c.HandleMessage(context.Background(), []byte(`{"event_type":"legacy"}`)); err != nil {
		t.Fatalf("non-CloudEvents must be skipped: %v", err)
	}
	// Unknown rejection reason.
	badReason := encodeEvent(t, typeTransferStockAllocationRejected, "trf-c:1", occurred, transferStockAllocationRejectedData{
		TransferID: "trf-c", TransferLineID: "trf-c:1", Reason: "SOMETHING_ELSE",
	})
	if err := c.HandleMessage(context.Background(), badReason); err != nil {
		t.Fatalf("unknown reason must be skipped: %v", err)
	}
	// Unparsable expires_at.
	badExpiry := []byte(strings.Replace(string(allocatedReplyValue(t, "trf-d")), `"expires_at":"`, `"expires_at":"not-a-time`, 1))
	if err := c.HandleMessage(context.Background(), badExpiry); err != nil {
		t.Fatalf("bad expiry must be skipped: %v", err)
	}
	if len(applier.allocations)+len(applier.rejections) != 0 {
		t.Fatalf("nothing must be applied: %+v %+v", applier.allocations, applier.rejections)
	}
}

func TestTransferReplyConsumerRetriesTransientFailure(t *testing.T) {
	applier := &fakeReplyApplier{failAlloc: 1}
	claims := newFakeClaims()
	c := newReplyConsumer(applier, claims)

	if err := c.HandleMessage(context.Background(), allocatedReplyValue(t, "trf-e")); err == nil {
		t.Fatal("transient failure must be returned non-nil so the loop retries")
	}
	if len(applier.allocations) != 0 {
		t.Fatal("failed handling must apply nothing")
	}
	// The claim rolled back with the unit of work: a retry processes it.
	claims.rollback(transferReplyConsumerName, "id-trf-e:1-"+replyOccurred.Format(time.RFC3339Nano))
	if err := c.HandleMessage(context.Background(), allocatedReplyValue(t, "trf-e")); err != nil {
		t.Fatalf("retry after transient failure: %v", err)
	}
	if len(applier.allocations) != 1 {
		t.Fatal("retry must apply the allocation")
	}
}

func TestTransferReplyConsumerDeterministicApplyFailureIsSkipped(t *testing.T) {
	// An apply that fails with a DETERMINISTIC error (unknown transfer,
	// illegal transition) is logged and skipped — the claim STAYS
	// committed, the offset moves on.
	applier := &deterministicFailApplier{}
	c := newReplyConsumer(applier, newFakeClaims())

	if err := c.HandleMessage(context.Background(), allocatedReplyValue(t, "trf-f")); err != nil {
		t.Fatalf("deterministic apply failure must be skipped, got %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("calls = %d", applier.calls)
	}
}

type deterministicFailApplier struct{ calls int }

func (d *deterministicFailApplier) ApplyAllocation(_ context.Context, in usecases.ApplyAllocationInput) error {
	d.calls++
	return transfer.ErrTransferNotFound
}

func (d *deterministicFailApplier) ApplyRejection(_ context.Context, in usecases.ApplyRejectionInput) error {
	d.calls++
	return transfer.ErrTransferNotFound
}

func (d *deterministicFailApplier) ApplyReceiptStaged(_ context.Context, in usecases.ApplyReceiptStagedInput) error {
	d.calls++
	return transfer.ErrTransferNotFound
}

func (d *deterministicFailApplier) ApplyStowed(_ context.Context, in usecases.ApplyStowInput) error {
	d.calls++
	return transfer.ErrTransferNotFound
}

func TestTransferReplyConsumerAppliesReceiptStagedAndStowed(t *testing.T) {
	applier := &fakeReplyApplier{}
	c := newReplyConsumer(applier, newFakeClaims())

	stagedValue := encodeEvent(t, typeTransferReceiptStaged, "trf-x:1", replyOccurred, transferReceiptStagedData{
		TransferID:        "trf-x",
		TransferLineID:    "trf-x:1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		ExpectedQuantity:  5,
		ReceivedQuantity:  5,
		Variance:          0,
	})
	if err := c.HandleMessage(context.Background(), stagedValue); err != nil {
		t.Fatalf("receipt staged: %v", err)
	}
	if len(applier.staged) != 1 {
		t.Fatalf("staged = %d", len(applier.staged))
	}
	if applier.staged[0].TransferID != transfer.TransferID("trf-x") || applier.staged[0].LineID != "trf-x:1" {
		t.Fatalf("applied staged = %+v", applier.staged[0])
	}

	stowedAt := replyOccurred.Add(time.Second) // distinct CE id from the staged fact (encodeEvent keys on subject+time)
	stowedValue := encodeEvent(t, typeTransferStockStowed, "trf-x:1", stowedAt, transferStockStowedData{
		TransferID:        "trf-x",
		TransferLineID:    "trf-x:1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		ReceivedQuantity:  5,
		StowedQuantity:    5,
		Allocations: []struct {
			StockUnitID string `json:"stock_unit_id"`
			BinID       string `json:"bin_id"`
			Quantity    int    `json:"quantity"`
		}{{StockUnitID: "su-1", BinID: "BIN-DEST", Quantity: 5}},
	})
	if err := c.HandleMessage(context.Background(), stowedValue); err != nil {
		t.Fatalf("stowed: %v", err)
	}
	if len(applier.stowed) != 1 {
		t.Fatalf("stowed = %d", len(applier.stowed))
	}
	got := applier.stowed[0]
	if got.TransferID != transfer.TransferID("trf-x") || got.Stowed.StowedQuantity != 5 {
		t.Fatalf("applied stowed = %+v", got)
	}
	if len(got.Stowed.Allocations) != 1 || got.Stowed.Allocations[0].BinID != "BIN-DEST" {
		t.Fatalf("stow allocations = %+v", got.Stowed.Allocations)
	}

	// Redeliveries are no-ops (dedupe claim on the CE id).
	if err := c.HandleMessage(context.Background(), stagedValue); err != nil {
		t.Fatalf("staged redelivery: %v", err)
	}
	if err := c.HandleMessage(context.Background(), stowedValue); err != nil {
		t.Fatalf("stowed redelivery: %v", err)
	}
	if len(applier.staged) != 1 || len(applier.stowed) != 1 {
		t.Fatalf("after redeliveries: staged = %d stowed = %d", len(applier.staged), len(applier.stowed))
	}
}

func TestTransferReplyConsumerSkipsFactsForUnknownTransfers(t *testing.T) {
	// A destination fact naming a transfer this deployment never approved
	// is WARN-logged and committed past, never retried.
	applier := &deterministicFailApplier{}
	claims := newFakeClaims()
	c := newReplyConsumer(applier, claims)

	unknownStaged := encodeEvent(t, typeTransferReceiptStaged, "trf-ghost:1", replyOccurred, transferReceiptStagedData{
		TransferID: "trf-ghost", TransferLineID: "trf-ghost:1",
	})
	unknownStowed := encodeEvent(t, typeTransferStockStowed, "trf-ghost:1", replyOccurred.Add(time.Second), transferStockStowedData{
		TransferID: "trf-ghost", TransferLineID: "trf-ghost:1",
	})
	if err := c.HandleMessage(context.Background(), unknownStaged); err != nil {
		t.Fatalf("unknown staged must be skipped, got %v", err)
	}
	if err := c.HandleMessage(context.Background(), unknownStowed); err != nil {
		t.Fatalf("unknown stowed must be skipped, got %v", err)
	}
	if applier.calls != 2 {
		t.Fatalf("calls = %d, want 2", applier.calls)
	}
	// Malformed payloads skip too.
	bad := encodeEvent(t, typeTransferStockStowed, "trf-x:1", replyOccurred, map[string]string{"nonsense": "x"})
	if err := c.HandleMessage(context.Background(), bad); err != nil {
		t.Fatalf("malformed stowed payload must be skipped: %v", err)
	}
}
