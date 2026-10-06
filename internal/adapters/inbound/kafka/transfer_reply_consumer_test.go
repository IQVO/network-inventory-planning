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
