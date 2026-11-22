package transfer

import (
	"errors"
	"testing"
	"time"
)

// releasingForTest builds the saga at RELEASING: an ALLOCATED transfer
// whose operator cancel raised the release command (ADR 0022).
func releasingForTest(t *testing.T) *InterWarehouseTransfer {
	t.Helper()
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)
	applyAllocationForTest(t, trf, validAllocationReply())
	cancelAfterAllocationForTest(t, trf)
	return trf
}

func cancelAfterAllocationForTest(t *testing.T, trf *InterWarehouseTransfer) {
	t.Helper()
	if _, err := trf.CancelAfterAllocation("operator changed mind", sagaNow.Add(2*time.Minute)); err != nil {
		t.Fatalf("cancel after allocation: %v", err)
	}
}

func validReleasedFact() StockAllocationReleased {
	return StockAllocationReleased{
		TransferLineID: "trf-0001:1",
		ReservationID:  "res-9",
		OriginSiteID:   "WH1",
		SKU:            "SKU-1",
		Quantity:       10,
		Outcome:        ReleaseOutcomeReleased,
		Reason:         "revoked for cancelled transfer",
	}
}

func TestCancelAfterAllocationMovesToReleasingAndRaisesCommand(t *testing.T) {
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)
	applyAllocationForTest(t, trf, validAllocationReply())

	cmd, err := trf.CancelAfterAllocation("operator changed mind", sagaNow.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("cancel after allocation: %v", err)
	}
	if trf.State() != StateReleasing {
		t.Fatalf("state = %s, want RELEASING", trf.State())
	}
	if cmd.TransferID != TransferID("trf-0001") || cmd.TransferLineID != "trf-0001:1" ||
		cmd.ReservationID != "res-9" || cmd.OriginSiteID != "WH1" ||
		cmd.SKU != "SKU-1" || cmd.Quantity != 10 || cmd.Reason != "operator changed mind" {
		t.Fatalf("ReleaseRequested = %+v", cmd)
	}
	if cmd.EventName() != "TransferAllocationReleaseRequested" {
		t.Fatalf("event name = %s", cmd.EventName())
	}
	audit := trf.Audit()
	last := audit[len(audit)-1]
	if last.From != StateAllocated || last.To != StateReleasing || last.Event != "TransferAllocationReleaseRequested" || last.Reason != "operator changed mind" {
		t.Fatalf("release audit entry = %+v", last)
	}
}

func TestCancelAfterAllocationRequiresReasonAndAllocatedState(t *testing.T) {
	allocated := func() *InterWarehouseTransfer {
		trf := proposeForTest(t)
		approveForTest(t, trf)
		requestAllocationForTest(t, trf)
		applyAllocationForTest(t, trf, validAllocationReply())
		return trf
	}
	if _, err := allocated().CancelAfterAllocation("   ", sagaNow); err == nil {
		t.Fatal("empty reason must be refused")
	}

	picked := allocated()
	if err := picked.MarkPicked(Picked{PickedQuantity: 10}, sagaNow.Add(90*time.Minute)); err != nil {
		t.Fatalf("mark picked: %v", err)
	}
	if _, err := picked.CancelAfterAllocation("stop", sagaNow); err == nil {
		t.Fatal("cancel after allocation from PICKED must be illegal — the stock is physically in motion")
	}

	if _, err := releasingForTest(t).CancelAfterAllocation("stop", sagaNow); err == nil {
		t.Fatal("cancel after allocation from RELEASING must be illegal — the release is already in flight")
	}

	var illegal *IllegalTransitionError
	_, err := proposeForTest(t).CancelAfterAllocation("stop", sagaNow)
	if !errors.As(err, &illegal) {
		t.Fatalf("cancel after allocation from PROPOSED = %v, want IllegalTransitionError", err)
	}
}

func TestMarkReleasedSettlesReleasingToCancelledForBothOutcomes(t *testing.T) {
	for _, outcome := range []ReleaseOutcome{ReleaseOutcomeReleased, ReleaseOutcomeAlreadyResolved} {
		trf := releasingForTest(t)
		fact := validReleasedFact()
		fact.Outcome = outcome
		if err := trf.MarkReleased(fact, sagaNow.Add(3*time.Minute)); err != nil {
			t.Fatalf("outcome %s: mark released: %v", outcome, err)
		}
		if trf.State() != StateCancelled {
			t.Fatalf("outcome %s: state = %s, want CANCELLED", outcome, trf.State())
		}
		audit := trf.Audit()
		last := audit[len(audit)-1]
		if last.From != StateReleasing || last.To != StateCancelled || last.Event != "TransferStockAllocationReleased" {
			t.Fatalf("outcome %s: settle audit entry = %+v", outcome, last)
		}
	}
}

func TestMarkReleasedValidatesFactAgainstTransfer(t *testing.T) {
	mutations := map[string]func(*StockAllocationReleased){
		"wrong line":        func(f *StockAllocationReleased) { f.TransferLineID = "trf-9999:1" },
		"wrong reservation": func(f *StockAllocationReleased) { f.ReservationID = "res-666" },
		"wrong origin":      func(f *StockAllocationReleased) { f.OriginSiteID = "WH9" },
		"wrong sku":         func(f *StockAllocationReleased) { f.SKU = "SKU-9" },
		"wrong quantity":    func(f *StockAllocationReleased) { f.Quantity = 4 },
		"unknown outcome":   func(f *StockAllocationReleased) { f.Outcome = ReleaseOutcome("PARTIAL") },
		"no outcome":        func(f *StockAllocationReleased) { f.Outcome = "" },
	}
	for name, mutate := range mutations {
		trf := releasingForTest(t)
		fact := validReleasedFact()
		mutate(&fact)
		if err := trf.MarkReleased(fact, sagaNow.Add(3*time.Minute)); !errors.Is(err, ErrFactRefused) {
			t.Fatalf("%s: mark released = %v, want ErrFactRefused", name, err)
		}
		if trf.State() != StateReleasing {
			t.Fatalf("%s: a refused fact must not move the saga; state = %s", name, trf.State())
		}
	}
}

func TestMarkReleasedOnlyFromReleasing(t *testing.T) {
	// From ALLOCATED (no cancel yet): the settle fact cannot arrive before
	// the release command.
	allocated := func() *InterWarehouseTransfer {
		trf := proposeForTest(t)
		approveForTest(t, trf)
		requestAllocationForTest(t, trf)
		applyAllocationForTest(t, trf, validAllocationReply())
		return trf
	}
	var illegal *IllegalTransitionError
	if err := allocated().MarkReleased(validReleasedFact(), sagaNow); !errors.As(err, &illegal) {
		t.Fatalf("mark released from ALLOCATED = %v, want IllegalTransitionError", err)
	}
	// From CANCELLED (already settled): a replayed fact is an illegal
	// transition, deterministic for the consumer to skip.
	settled := releasingForTest(t)
	if err := settled.MarkReleased(validReleasedFact(), sagaNow.Add(4*time.Minute)); err != nil {
		t.Fatalf("first mark released must settle: %v", err)
	}
	if err := settled.MarkReleased(validReleasedFact(), sagaNow.Add(5*time.Minute)); err == nil {
		t.Fatal("replayed mark released from CANCELLED must be illegal")
	}
}

func TestPreReleaseCancelStillRefusesAfterAllocation(t *testing.T) {
	// The v1 pre-release Cancel stays narrow: ALLOCATED and RELEASING are
	// NOT pre-release — only CancelAfterAllocation (ADR 0022) moves them.
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)
	applyAllocationForTest(t, trf, validAllocationReply())
	if err := trf.Cancel("operator changed mind", sagaNow); err == nil {
		t.Fatal("plain Cancel from ALLOCATED must stay illegal")
	}
	cancelAfterAllocationForTest(t, trf)
	if err := trf.Cancel("operator changed mind", sagaNow); err == nil {
		t.Fatal("plain Cancel from RELEASING must stay illegal")
	}
}
