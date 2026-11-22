package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var cancelNow = time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)

// publishedNames lists the event names a fake publisher recorded.
func publishedNames(pub *fakeEventPublisher) []string {
	names := make([]string, 0, len(pub.events))
	for _, e := range pub.events {
		names = append(names, e.EventName())
	}
	return names
}

func hasEvent(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// storedTransferAt seeds the memory store with a rehydrated transfer in
// the given state, carrying the reservation fields every
// post-allocation state has.
func storedTransferAt(t *testing.T, s *memory.TransferStore, id string, state transfer.TransferState) transfer.TransferID {
	t.Helper()
	snap := transfer.Snapshot{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id,
		OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1",
		Quantity: 10, PolicyVersion: "pv-1",
		ProposalAsOf: cancelNow.Add(-time.Hour), ExpiresAt: cancelNow.Add(24 * time.Hour),
		State:     state,
		Audit:     []transfer.AuditEntry{{To: state, Event: "seeded", Reason: "seed", OccurredAt: cancelNow}},
		CreatedAt: cancelNow, UpdatedAt: cancelNow,
	}
	if state != transfer.StateDraft && state != transfer.StateProposed &&
		state != transfer.StateApproved && state != transfer.StateAllocating {
		snap.ReservationID = "res-1"
		snap.Allocations = []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}}
		snap.AllocationExpiresAt = cancelNow.Add(12 * time.Hour)
	}
	trf := transfer.Rehydrate(snap)
	if _, err := s.Create(context.Background(), trf, "key-"+id); err != nil {
		t.Fatalf("Create(%s at %s): %v", id, state, err)
	}
	return trf.ID()
}

func cancelUseCase(s *memory.TransferStore, pub *fakeEventPublisher) CancelTransfer {
	return CancelTransfer{Transfers: s, Events: pub, UoW: passThroughUoW{}, Now: func() time.Time { return cancelNow }}
}

func TestCancelTransferPreReleaseIsSynchronous(t *testing.T) {
	for _, state := range []transfer.TransferState{
		transfer.StateDraft, transfer.StateProposed, transfer.StateApproved, transfer.StateAllocating,
	} {
		s := memory.NewTransferStore()
		pub := &fakeEventPublisher{}
		id := storedTransferAt(t, s, "trf-"+string(state), state)

		result, err := cancelUseCase(s, pub).Execute(context.Background(), CancelTransferInput{TransferID: id, Reason: "operator changed mind"})
		if err != nil {
			t.Fatalf("%s: cancel: %v", state, err)
		}
		if result.Async || result.Replayed {
			t.Fatalf("%s: result = async=%v replayed=%v, want the synchronous cancel", state, result.Async, result.Replayed)
		}
		if result.Transfer.State() != transfer.StateCancelled {
			t.Fatalf("%s: state = %s, want CANCELLED", state, result.Transfer.State())
		}
		if got := publishedNames(pub); !hasEvent(got, "TransferStateAdvanced") {
			t.Fatalf("%s: published %v, want a TransferStateAdvanced occurrence", state, got)
		}
		if hasEvent(publishedNames(pub), "TransferAllocationReleaseRequested") {
			t.Fatalf("%s: a pre-release cancel must not raise a release command", state)
		}
	}
}

func TestCancelTransferAllocatedGoesReleasingAndOutboxesCommand(t *testing.T) {
	s := memory.NewTransferStore()
	pub := &fakeEventPublisher{}
	id := storedTransferAt(t, s, "trf-alloc", transfer.StateAllocated)

	result, err := cancelUseCase(s, pub).Execute(context.Background(), CancelTransferInput{TransferID: id, Reason: "wrong site picked"})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !result.Async {
		t.Fatal("an ALLOCATED cancel must be async (RELEASING)")
	}
	if result.Transfer.State() != transfer.StateReleasing {
		t.Fatalf("state = %s, want RELEASING", result.Transfer.State())
	}
	if result.Command.TransferLineID != "trf-alloc:1" || result.Command.ReservationID != "res-1" ||
		result.Command.SKU != "SKU-1" || result.Command.Quantity != 10 || result.Command.Reason != "wrong site picked" {
		t.Fatalf("command = %+v", result.Command)
	}
	names := publishedNames(pub)
	if !hasEvent(names, "TransferAllocationReleaseRequested") {
		t.Fatalf("published %v, want the release command", names)
	}
	if !hasEvent(names, "TransferStateAdvanced") {
		t.Fatalf("published %v, want the analytics occurrence", names)
	}

	// The store sees RELEASING.
	loaded, err := s.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.State() != transfer.StateReleasing {
		t.Fatalf("stored state = %s, want RELEASING", loaded.State())
	}
}

func TestCancelTransferIsIdempotentOnTheSagaState(t *testing.T) {
	s := memory.NewTransferStore()
	pub := &fakeEventPublisher{}

	// Retried ALLOCATED cancel: RELEASING answers 202 again, no new events.
	releasingID := storedTransferAt(t, s, "trf-rel", transfer.StateReleasing)
	result, err := cancelUseCase(s, pub).Execute(context.Background(), CancelTransferInput{TransferID: releasingID, Reason: "retry"})
	if err != nil || !result.Async || !result.Replayed {
		t.Fatalf("RELEASING retry = %+v, %v; want async replay", result, err)
	}
	if len(pub.events) != 0 {
		t.Fatalf("RELEASING retry published %v, want nothing", publishedNames(pub))
	}

	// Retried settled cancel: CANCELLED answers 200 again.
	cancelledID := storedTransferAt(t, s, "trf-done", transfer.StateCancelled)
	result, err = cancelUseCase(s, pub).Execute(context.Background(), CancelTransferInput{TransferID: cancelledID, Reason: "retry"})
	if err != nil || result.Async || !result.Replayed {
		t.Fatalf("CANCELLED retry = %+v, %v; want sync replay", result, err)
	}
}

func TestCancelTransferRefusesTheFactDrivenTailAndTerminalStates(t *testing.T) {
	for _, state := range []transfer.TransferState{
		transfer.StatePicked, transfer.StateInTransit, transfer.StateArrived,
		transfer.StateReceived, transfer.StateUnfulfillable,
	} {
		s := memory.NewTransferStore()
		pub := &fakeEventPublisher{}
		id := storedTransferAt(t, s, "trf-"+string(state), state)
		if state == transfer.StatePicked {
			snap := transfer.Rehydrate(transfer.Snapshot{
				ID: id, OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 10,
				State: transfer.StatePicked, ReservationID: "res-1", PickedQuantity: 10,
				UpdatedAt: cancelNow,
			})
			if _, err := s.Create(context.Background(), snap, "k"); err != nil {
				t.Fatal(err)
			}
		}
		_, err := cancelUseCase(s, pub).Execute(context.Background(), CancelTransferInput{TransferID: id, Reason: "too late"})
		if !errors.Is(err, ErrCancelRefused) {
			t.Fatalf("%s: cancel = %v, want ErrCancelRefused", state, err)
		}
	}
}

func TestCancelTransferValidatesInputAndUnknownTransfer(t *testing.T) {
	s := memory.NewTransferStore()
	pub := &fakeEventPublisher{}
	uc := cancelUseCase(s, pub)

	if _, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "trf-x", Reason: "  "}); !errors.Is(err, ErrCancelInvalid) {
		t.Fatalf("empty reason = %v, want ErrCancelInvalid", err)
	}
	_, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "trf-missing", Reason: "reason"})
	if !errors.Is(err, transfer.ErrTransferNotFound) {
		t.Fatalf("unknown transfer = %v, want ErrTransferNotFound", err)
	}
}

func TestApplyTransferAllocationReleasedSettlesCancelled(t *testing.T) {
	s := memory.NewTransferStore()
	pub := &fakeEventPublisher{}
	id := storedTransferAt(t, s, "trf-settle", transfer.StateReleasing)

	uc := ApplyTransferAllocationReleased{Transfers: s, Events: pub, UoW: passThroughUoW{}}
	err := uc.Execute(context.Background(), ApplyReleasedInput{
		TransferID: id,
		Released: transfer.StockAllocationReleased{
			TransferLineID: "trf-settle:1", ReservationID: "res-1",
			OriginSiteID: "WH1", SKU: "SKU-1", Quantity: 10,
			Outcome: transfer.ReleaseOutcomeAlreadyResolved, Reason: "reservation had expired",
		},
		OccurredAt: cancelNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("apply released: %v", err)
	}
	loaded, err := s.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.State() != transfer.StateCancelled {
		t.Fatalf("state = %s, want CANCELLED", loaded.State())
	}
	if !hasEvent(publishedNames(pub), "TransferStateAdvanced") {
		t.Fatalf("published %v, want the analytics occurrence", publishedNames(pub))
	}

	// A refused fact is deterministic poison for the consumer.
	err = uc.Execute(context.Background(), ApplyReleasedInput{
		TransferID: storedTransferAt(t, s, "trf-other", transfer.StateReleasing),
		Released: transfer.StockAllocationReleased{
			TransferLineID: "trf-other:1", ReservationID: "res-WRONG",
			OriginSiteID: "WH1", SKU: "SKU-1", Quantity: 10,
			Outcome: transfer.ReleaseOutcomeReleased,
		},
		OccurredAt: cancelNow,
	})
	if !errors.Is(err, transfer.ErrFactRefused) {
		t.Fatalf("mismatched fact = %v, want ErrFactRefused", err)
	}
}
