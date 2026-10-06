package transfer

import (
	"errors"
	"testing"
	"time"
)

var factNow = sagaNow.Add(10 * time.Minute)

// allocatedForTest returns a fresh aggregate already driven to ALLOCATED.
func allocatedForTest(t *testing.T) *InterWarehouseTransfer {
	t.Helper()
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)
	applyAllocationForTest(t, trf, validAllocationReply())
	return trf
}

// pickedForTest drives an ALLOCATED aggregate to PICKED.
func pickedForTest(t *testing.T, trf *InterWarehouseTransfer, qty int) {
	t.Helper()
	if err := trf.MarkPicked(Picked{PickedQuantity: qty}, factNow); err != nil {
		t.Fatalf("mark picked: %v", err)
	}
}

func TestFactDrivenTailReachesReceived(t *testing.T) {
	trf := driveToReceived(t, allocatedForTest(t), 10)

	if trf.State() != StateReceived {
		t.Fatalf("state = %s, want RECEIVED", trf.State())
	}
	if len(trf.StowAllocations()) != 2 {
		t.Fatalf("stow allocations = %+v", trf.StowAllocations())
	}

	// RECEIVED is terminal: every fact command refuses.
	if err := trf.MarkDispatched(factNow); err == nil {
		t.Fatal("dispatch from RECEIVED must refuse")
	}
	if err := trf.Cancel("done anyway", factNow); err == nil {
		t.Fatal("cancel from RECEIVED must refuse")
	}
}

// driveToReceived pushes an ALLOCATED aggregate through the whole
// fact-driven tail and returns it (also asserting the full audit trail).
func driveToReceived(t *testing.T, trf *InterWarehouseTransfer, pickQty int) *InterWarehouseTransfer {
	t.Helper()
	if err := trf.MarkPicked(Picked{PickedQuantity: pickQty}, factNow); err != nil {
		t.Fatalf("pick: %v", err)
	}
	if trf.State() != StatePicked || trf.PickedQuantity() != pickQty {
		t.Fatalf("state = %s picked = %d", trf.State(), trf.PickedQuantity())
	}
	if trf.DispatchQuantity() != pickQty {
		t.Fatalf("dispatch quantity = %d, want %d", trf.DispatchQuantity(), pickQty)
	}
	if err := trf.MarkDispatched(factNow.Add(time.Minute)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := trf.MarkArrived("TransferArrived", factNow.Add(2*time.Minute)); err != nil {
		t.Fatalf("arrive: %v", err)
	}
	if err := trf.MarkStowed(validStowFact(), factNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("stow: %v", err)
	}

	// The full happy-path audit trail: drafted, proposed, approved,
	// allocating, allocated, picked, dispatched, arrived, stowed.
	audit := trf.Audit()
	if len(audit) != 9 {
		t.Fatalf("audit entries = %d, want 9: %+v", len(audit), audit)
	}
	wantEvents := []string{"TransferDrafted", "TransferProposed", "TransferPlanApproved",
		"TransferAllocationRequested", "TransferStockAllocated", "TransferPicked",
		"TransferDispatched", "TransferArrived", "TransferStockStowed"}
	for i, want := range wantEvents {
		if audit[i].Event != want {
			t.Fatalf("audit[%d].Event = %q, want %q", i, audit[i].Event, want)
		}
	}
	return trf
}

func TestArrivalAcceptsReceiptStagedAsWell(t *testing.T) {
	// Scan-driven receiving: TransferReceiptStaged drives the same
	// IN_TRANSIT -> ARRIVED transition as the reserved TransferArrived.
	trf := allocatedForTest(t)
	pickedForTest(t, trf, 10)
	if err := trf.MarkDispatched(factNow); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := trf.MarkArrived("TransferReceiptStaged", factNow.Add(time.Minute)); err != nil {
		t.Fatalf("arrive via receipt staged: %v", err)
	}
	if trf.State() != StateArrived {
		t.Fatalf("state = %s, want ARRIVED", trf.State())
	}
	last := trf.Audit()[len(trf.Audit())-1]
	if last.Event != "TransferReceiptStaged" {
		t.Fatalf("audit event = %q, want TransferReceiptStaged", last.Event)
	}
}

func TestShortPickIsRecordedNotRefused(t *testing.T) {
	trf := allocatedForTest(t) // allocated 10

	if err := trf.MarkPicked(Picked{PickedQuantity: 7}, factNow); err != nil {
		t.Fatalf("short pick must be recorded, not refused: %v", err)
	}
	if trf.State() != StatePicked || trf.PickedQuantity() != 7 {
		t.Fatalf("state = %s picked = %d", trf.State(), trf.PickedQuantity())
	}
	// The dispatch demand carries the PICKED quantity.
	if trf.DispatchQuantity() != 7 {
		t.Fatalf("dispatch quantity = %d, want the picked 7", trf.DispatchQuantity())
	}
	last := trf.Audit()[len(trf.Audit())-1]
	if last.Event != "TransferPicked" || last.Reason != "picked 7 of 10" {
		t.Fatalf("pick audit entry = %+v", last)
	}
}

func TestMarkPickedRefusesImpossibleQuantities(t *testing.T) {
	cases := []struct {
		name string
		qty  int
	}{
		{"zero", 0},
		{"negative", -1},
		{"above allocated", 11},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trf := allocatedForTest(t)
			err := trf.MarkPicked(Picked{PickedQuantity: tc.qty}, factNow)
			if err == nil {
				t.Fatal("impossible picked quantity must refuse")
			}
			if !errors.Is(err, ErrFactRefused) {
				t.Fatalf("err = %v, want ErrFactRefused", err)
			}
			if trf.State() != StateAllocated {
				t.Fatalf("state = %s, want ALLOCATED (unchanged)", trf.State())
			}
		})
	}
}

func TestFactDrivenTailRequiresStrictOrder(t *testing.T) {
	// Dispatch before pick.
	trf := allocatedForTest(t)
	if err := trf.MarkDispatched(factNow); err == nil {
		t.Fatal("dispatch from ALLOCATED must refuse")
	}
	var illegal *IllegalTransitionError
	if !errors.As(trf.MarkDispatched(factNow), &illegal) {
		t.Fatal("out-of-order fact must be an IllegalTransitionError (deterministic skip)")
	}

	// Stow before arrival.
	trf = allocatedForTest(t)
	pickedForTest(t, trf, 10)
	if err := trf.MarkStowed(validStowFact(), factNow); err == nil {
		t.Fatal("stow from PICKED must refuse")
	}

	// Pick twice (replay).
	trf = allocatedForTest(t)
	pickedForTest(t, trf, 10)
	if err := trf.MarkPicked(Picked{PickedQuantity: 10}, factNow); err == nil {
		t.Fatal("double pick must refuse")
	}

	// Arrival before dispatch.
	trf = allocatedForTest(t)
	pickedForTest(t, trf, 10)
	if err := trf.MarkArrived("TransferArrived", factNow); err == nil {
		t.Fatal("arrive from PICKED must refuse")
	}

	// Cancel once picked: the stock is in motion — pre-release only.
	trf = allocatedForTest(t)
	pickedForTest(t, trf, 10)
	if err := trf.Cancel("operator changed mind", factNow); err == nil {
		t.Fatal("cancel from PICKED must refuse")
	}
}

func TestMarkStowedValidatesFactAgainstTransfer(t *testing.T) {
	base := validStowFact()
	cases := []struct {
		name string
		mut  func(*Stowed)
	}{
		{"wrong line", func(s *Stowed) { s.TransferLineID = "trf-other:1" }},
		{"wrong destination", func(s *Stowed) { s.DestinationSite = "WH9" }},
		{"wrong sku", func(s *Stowed) { s.SKU = "SKU-9" }},
		{"zero stowed", func(s *Stowed) { s.StowedQuantity = 0 }},
		{"no allocations", func(s *Stowed) { s.Allocations = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trf := allocatedForTest(t)
			pickedForTest(t, trf, 10)
			if err := trf.MarkDispatched(factNow); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			if err := trf.MarkArrived("TransferArrived", factNow); err != nil {
				t.Fatalf("arrive: %v", err)
			}
			fact := base
			tc.mut(&fact)
			err := trf.MarkStowed(fact, factNow)
			if err == nil {
				t.Fatal("mismatched stow fact must refuse")
			}
			if !errors.Is(err, ErrFactRefused) {
				t.Fatalf("err = %v, want ErrFactRefused", err)
			}
			if trf.State() != StateArrived {
				t.Fatalf("state = %s, want ARRIVED (unchanged)", trf.State())
			}
		})
	}
}

func validStowFact() Stowed {
	return Stowed{
		TransferLineID:   "trf-0001:1",
		DestinationSite:  "WH2",
		SKU:              "SKU-1",
		ReceivedQuantity: 10,
		StowedQuantity:   10,
		Allocations: []StowAllocation{
			{StockUnitID: "su-1", BinID: "BIN-DEST-A", Quantity: 6},
			{StockUnitID: "su-2", BinID: "BIN-DEST-B", Quantity: 4},
		},
	}
}

func TestDemandIDIsDeterministic(t *testing.T) {
	id := TransferID("trf-0001")
	if id.DemandID(WorkKindTransferPick) != "trf-0001:pick" {
		t.Fatalf("pick demand id = %q", id.DemandID(WorkKindTransferPick))
	}
	if id.DemandID(WorkKindTransferDispatch) != "trf-0001:dispatch" {
		t.Fatalf("dispatch demand id = %q", id.DemandID(WorkKindTransferDispatch))
	}
	for _, k := range []WorkKind{"TRANSFER_PICK", "TRANSFER_DISPATCH", "TRANSFER_ARRIVAL"} {
		if !k.Valid() {
			t.Fatalf("work kind %q must be valid", k)
		}
	}
	if WorkKind("TRANSFER_FLY").Valid() {
		t.Fatal("unknown work kind must be invalid")
	}
}

func TestRehydrateRoundTripsFactTail(t *testing.T) {
	trf := allocatedForTest(t)
	if err := trf.MarkPicked(Picked{PickedQuantity: 8}, factNow); err != nil {
		t.Fatalf("pick: %v", err)
	}
	if err := trf.MarkDispatched(factNow.Add(time.Minute)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := trf.MarkArrived("TransferReceiptStaged", factNow.Add(2*time.Minute)); err != nil {
		t.Fatalf("arrive: %v", err)
	}
	stow := validStowFact()
	stow.StowedQuantity = 8
	stow.ReceivedQuantity = 8
	stow.Allocations = []StowAllocation{{StockUnitID: "su-1", BinID: "BIN-DEST-A", Quantity: 8}}
	if err := trf.MarkStowed(stow, factNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("stow: %v", err)
	}

	snap := Snapshot{
		ID: trf.ID(), IdempotencyKey: trf.IdempotencyKey(),
		OriginSiteID: trf.OriginSiteID(), DestinationSiteID: trf.DestinationSiteID(),
		SKU: trf.SKU(), Quantity: trf.Quantity(), PolicyVersion: trf.PolicyVersion(),
		OperatorReason: trf.OperatorReason(), ProposalAsOf: trf.ProposalAsOf(), ExpiresAt: trf.ExpiresAt(),
		State: trf.State(), ReservationID: trf.ReservationID(), Allocations: trf.Allocations(),
		AllocationExpiresAt: trf.AllocationExpiresAt(), PickedQuantity: trf.PickedQuantity(),
		StowAllocations: trf.StowAllocations(), RejectionReason: trf.RejectionReason(),
		Audit: trf.Audit(), CreatedAt: trf.CreatedAt(), UpdatedAt: trf.UpdatedAt(), Version: trf.Version(),
	}
	again := Rehydrate(snap)
	if again.State() != StateReceived || again.PickedQuantity() != 8 || len(again.StowAllocations()) != 1 {
		t.Fatalf("rehydrated = %s picked=%d stow=%d", again.State(), again.PickedQuantity(), len(again.StowAllocations()))
	}
}
