package transfer

import (
	"errors"
	"testing"
	"time"
)

// TestMarkAllocatedRefusalsAreTypedReplyRefused proves every way an
// allocation reply can fail the aggregate's consistency checks surfaces as
// the typed ErrReplyRefused sentinel (D4): the reply consumer classifies it
// as deterministic and dead-letters it instead of retrying the same bytes
// forever on a warehouse.inventory.events partition.
func TestMarkAllocatedRefusalsAreTypedReplyRefused(t *testing.T) {
	cases := []struct {
		name     string
		mut      func(*StockAllocation)
		sentinel error
	}{
		{"wrong line", func(a *StockAllocation) { a.TransferLineID = "trf-other:1" }, ErrReplyLineMismatch},
		{"wrong origin", func(a *StockAllocation) { a.OriginSiteID = "WH9" }, ErrReplyRefused},
		{"wrong sku", func(a *StockAllocation) { a.SKU = "SKU-9" }, ErrReplyRefused},
		{"no reservation id", func(a *StockAllocation) { a.ReservationID = " " }, ErrReplyRefused},
		{"no allocations", func(a *StockAllocation) { a.Allocations = nil }, ErrReplyRefused},
		{"allocation without stock unit", func(a *StockAllocation) { a.Allocations[0].StockUnitID = "" }, ErrReplyRefused},
		{"allocation without bin", func(a *StockAllocation) { a.Allocations[0].BinID = " " }, ErrReplyRefused},
		{"non-positive allocation", func(a *StockAllocation) { a.Allocations[0].Quantity = 0 }, ErrReplyRefused},
		{"quantity sum mismatch", func(a *StockAllocation) { a.Allocations[0].Quantity = 9 }, ErrReplyRefused},
		{"no expiry", func(a *StockAllocation) { a.ExpiresAt = time.Time{} }, ErrReplyRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			requestAllocationForTest(t, trf)
			reply := StockAllocation{
				TransferLineID: "trf-0001:1",
				OriginSiteID:   "WH1",
				SKU:            "SKU-1",
				ReservationID:  "res-9",
				Quantity:       10,
				Allocations:    []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
				ExpiresAt:      sagaNow.Add(time.Hour),
			}
			tc.mut(&reply)
			err := trf.MarkAllocated(reply, sagaNow)
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.sentinel)
			}
			if !errors.Is(err, ErrReplyRefused) {
				t.Fatalf("err = %v, want errors.Is ErrReplyRefused", err)
			}
		})
	}
}

// TestMarkUnfulfillableRefusalsAreTyped proves the two rejection-reply
// validation failures are distinct typed sentinels under ErrReplyRefused,
// and that a refused reply leaves the aggregate untouched.
func TestMarkUnfulfillableRefusalsAreTyped(t *testing.T) {
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)

	err := trf.MarkUnfulfillable("trf-other:1", RejectionInsufficientUsable, sagaNow)
	if !errors.Is(err, ErrReplyLineMismatch) || !errors.Is(err, ErrReplyRefused) {
		t.Fatalf("wrong line err = %v, want ErrReplyLineMismatch under ErrReplyRefused", err)
	}
	if errors.Is(err, ErrUnknownRejectionReason) {
		t.Fatalf("a line mismatch must not be classified as an unknown reason: %v", err)
	}

	err = trf.MarkUnfulfillable("trf-0001:1", RejectionReason("WEIRD"), sagaNow)
	if !errors.Is(err, ErrUnknownRejectionReason) || !errors.Is(err, ErrReplyRefused) {
		t.Fatalf("unknown reason err = %v, want ErrUnknownRejectionReason under ErrReplyRefused", err)
	}
	if errors.Is(err, ErrReplyLineMismatch) {
		t.Fatalf("an unknown reason must not be classified as a line mismatch: %v", err)
	}
	if trf.State() != StateAllocating || trf.RejectionReason() != "" || len(trf.Audit()) != 4 {
		t.Fatalf("refused replies must leave the aggregate untouched: state %s reason %q audit %d",
			trf.State(), trf.RejectionReason(), len(trf.Audit()))
	}
}
