package transfer

import (
	"errors"
	"testing"
	"time"
)

// reservationAllowedIn mirrors, as plain literals, the database CHECK
// inter_warehouse_transfer_reservation_id_check (migration 0009): a
// reservation id may be present only while the saga is in one of these
// states. CANCELLED is deliberately absent — a cancelled transfer holds no
// reservation, so a release that settles it must let go of the reference.
var reservationAllowedIn = map[TransferState]bool{
	StateAllocated: true,
	StateReleasing: true,
	StatePicked:    true,
	StateInTransit: true,
	StateArrived:   true,
	StateReceived:  true,
}

// assertReservationInvariant fails when the aggregate carries a
// reservation id in a state the persistence CHECK refuses.
func assertReservationInvariant(t *testing.T, step string, trf *InterWarehouseTransfer) {
	t.Helper()
	if trf.ReservationID() != "" && !reservationAllowedIn[trf.State()] {
		t.Fatalf("%s: state %s carries reservation %q — persistence would refuse it (SQLSTATE 23514)", step, trf.State(), trf.ReservationID())
	}
}

func TestMarkReleasedLetsGoOfTheReservationForBothOutcomes(t *testing.T) {
	for _, outcome := range []ReleaseOutcome{ReleaseOutcomeReleased, ReleaseOutcomeAlreadyResolved} {
		trf := releasingForTest(t)
		if trf.ReservationID() != "res-9" {
			t.Fatalf("outcome %s: RELEASING must still carry the reservation it is releasing, got %q", outcome, trf.ReservationID())
		}
		fact := validReleasedFact()
		fact.Outcome = outcome
		if err := trf.MarkReleased(fact, sagaNow.Add(3*time.Minute)); err != nil {
			t.Fatalf("outcome %s: mark released: %v", outcome, err)
		}
		if trf.State() != StateCancelled {
			t.Fatalf("outcome %s: state = %s, want CANCELLED", outcome, trf.State())
		}
		if trf.ReservationID() != "" {
			t.Fatalf("outcome %s: CANCELLED transfer still carries reservation %q", outcome, trf.ReservationID())
		}
		if len(trf.Allocations()) != 0 {
			t.Fatalf("outcome %s: CANCELLED transfer still lists allocations %+v", outcome, trf.Allocations())
		}
		if !trf.AllocationExpiresAt().IsZero() {
			t.Fatalf("outcome %s: CANCELLED transfer still carries allocation expiry %s", outcome, trf.AllocationExpiresAt())
		}
		assertReservationInvariant(t, "settled "+string(outcome), trf)
	}
}

func TestMarkReleasedKeepsTheReservationInTheAuditTrail(t *testing.T) {
	trf := releasingForTest(t)
	if err := trf.MarkReleased(validReleasedFact(), sagaNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("mark released: %v", err)
	}
	found := false
	for _, e := range trf.Audit() {
		if e.To == StateAllocated && e.Reason == "reservation res-9" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the audit trail must keep the history of which reservation was held: %+v", trf.Audit())
	}
}

func TestRefusedReleaseFactKeepsTheReservationWhileReleasing(t *testing.T) {
	trf := releasingForTest(t)
	fact := validReleasedFact()
	fact.ReservationID = "res-666"
	if err := trf.MarkReleased(fact, sagaNow.Add(3*time.Minute)); !errors.Is(err, ErrFactRefused) {
		t.Fatalf("mark released = %v, want ErrFactRefused", err)
	}
	if trf.ReservationID() != "res-9" || len(trf.Allocations()) != 2 || trf.AllocationExpiresAt().IsZero() {
		t.Fatalf("a refused fact must not touch the hold: reservation %q allocations %+v expiry %s", trf.ReservationID(), trf.Allocations(), trf.AllocationExpiresAt())
	}
}

func TestReplayedReleaseFactLeavesTheSettledTransferUntouched(t *testing.T) {
	trf := releasingForTest(t)
	if err := trf.MarkReleased(validReleasedFact(), sagaNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("first release: %v", err)
	}
	before := len(trf.Audit())
	var illegal *IllegalTransitionError
	if err := trf.MarkReleased(validReleasedFact(), sagaNow.Add(4*time.Minute)); !errors.As(err, &illegal) {
		t.Fatalf("replayed release = %v, want IllegalTransitionError", err)
	}
	if trf.State() != StateCancelled || trf.ReservationID() != "" || len(trf.Audit()) != before {
		t.Fatalf("replay changed the transfer: state %s reservation %q audit %d->%d", trf.State(), trf.ReservationID(), before, len(trf.Audit()))
	}
}

// The saga offers no order in which a pick overtakes the release: a pick
// fact that arrives while RELEASING is illegal, and the release fact that
// arrives after a pick is illegal too. Either way the hold is untouched.
func TestReleaseAndPickNeverInterleave(t *testing.T) {
	releasing := releasingForTest(t)
	var illegal *IllegalTransitionError
	if err := releasing.MarkPicked(Picked{PickedQuantity: 10}, sagaNow.Add(3*time.Minute)); !errors.As(err, &illegal) {
		t.Fatalf("pick while RELEASING = %v, want IllegalTransitionError", err)
	}
	if releasing.State() != StateReleasing || releasing.ReservationID() != "res-9" {
		t.Fatalf("a pick while RELEASING changed the transfer: state %s reservation %q", releasing.State(), releasing.ReservationID())
	}

	picked := allocatedForTest(t)
	if err := picked.MarkPicked(Picked{PickedQuantity: 10}, sagaNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("pick: %v", err)
	}
	if err := picked.MarkReleased(validReleasedFact(), sagaNow.Add(4*time.Minute)); !errors.As(err, &illegal) {
		t.Fatalf("release fact after PICKED = %v, want IllegalTransitionError", err)
	}
	if picked.State() != StatePicked || picked.ReservationID() != "res-9" {
		t.Fatalf("a release fact after PICKED changed the transfer: state %s reservation %q", picked.State(), picked.ReservationID())
	}
}

// TestEveryTransitionKeepsTheReservationInvariant drives the aggregate
// through every transition it has — each path of the saga — and asserts
// after EVERY step that the reservation reference is legal for the state
// (the persistence CHECK, mirrored in reservationAllowedIn). This class of
// bug hides in the transitions nobody tests.
func TestEveryTransitionKeepsTheReservationInvariant(t *testing.T) {
	stowed := Stowed{
		TransferLineID:   "trf-0001:1",
		DestinationSite:  "WH2",
		SKU:              "SKU-1",
		ReceivedQuantity: 10,
		StowedQuantity:   10,
		Allocations:      []StowAllocation{{StockUnitID: "su-9", BinID: "BIN-Z", Quantity: 10}},
	}
	type step struct {
		name string
		do   func(trf *InterWarehouseTransfer) error
	}
	at := func(m int) time.Time { return sagaNow.Add(time.Duration(m) * time.Minute) }
	approve := step{"approve", func(trf *InterWarehouseTransfer) error { _, err := trf.Approve(sagaNow); return err }}
	request := step{"request allocation", func(trf *InterWarehouseTransfer) error { _, err := trf.RequestAllocation(sagaNow); return err }}
	allocate := step{"mark allocated", func(trf *InterWarehouseTransfer) error { return trf.MarkAllocated(validAllocationReply(), at(1)) }}
	reject := step{"mark unfulfillable", func(trf *InterWarehouseTransfer) error {
		return trf.MarkUnfulfillable("trf-0001:1", RejectionInsufficientUsable, at(1))
	}}
	cancel := step{"cancel", func(trf *InterWarehouseTransfer) error { return trf.Cancel("operator changed mind", at(2)) }}
	cancelAfter := step{"cancel after allocation", func(trf *InterWarehouseTransfer) error {
		_, err := trf.CancelAfterAllocation("operator changed mind", at(2))
		return err
	}}
	release := func(outcome ReleaseOutcome) step {
		return step{"mark released " + string(outcome), func(trf *InterWarehouseTransfer) error {
			fact := validReleasedFact()
			fact.Outcome = outcome
			return trf.MarkReleased(fact, at(3))
		}}
	}
	pick := step{"mark picked", func(trf *InterWarehouseTransfer) error { return trf.MarkPicked(Picked{PickedQuantity: 10}, at(3)) }}
	shortPick := step{"mark short picked", func(trf *InterWarehouseTransfer) error { return trf.MarkPicked(Picked{PickedQuantity: 4}, at(3)) }}
	dispatch := step{"mark dispatched", func(trf *InterWarehouseTransfer) error { return trf.MarkDispatched(at(4)) }}
	arrive := step{"mark arrived", func(trf *InterWarehouseTransfer) error { return trf.MarkArrived("TransferArrived", at(5)) }}
	stow := step{"mark stowed", func(trf *InterWarehouseTransfer) error { return trf.MarkStowed(stowed, at(6)) }}

	paths := map[string][]step{
		"cancelled while proposed":       {cancel},
		"cancelled while approved":       {approve, cancel},
		"cancelled while allocating":     {approve, request, cancel},
		"rejected":                       {approve, request, reject},
		"cancelled after allocation (R)": {approve, request, allocate, cancelAfter, release(ReleaseOutcomeReleased)},
		"cancelled after allocation (A)": {approve, request, allocate, cancelAfter, release(ReleaseOutcomeAlreadyResolved)},
		"full delivery":                  {approve, request, allocate, pick, dispatch, arrive, stow},
		"short pick delivery":            {approve, request, allocate, shortPick, dispatch, arrive, stow},
	}
	for name, steps := range paths {
		trf := proposeForTest(t)
		assertReservationInvariant(t, name+": proposed", trf)
		for _, s := range steps {
			if err := s.do(trf); err != nil {
				t.Fatalf("%s: %s: %v", name, s.name, err)
			}
			assertReservationInvariant(t, name+": after "+s.name, trf)
		}
	}
}
