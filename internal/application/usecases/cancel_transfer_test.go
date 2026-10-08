package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var cancelNow = time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)

// seedTransferInState puts a transfer persisted in the given state into
// the fake repo (Rehydrate trusts the store, exactly like the Postgres
// adapter), so every lifecycle state is reachable without driving the saga.
func seedTransferInState(repo *fakeTransferRepo, id string, state transfer.TransferState) {
	trf := transfer.Rehydrate(transfer.Snapshot{
		ID:                transfer.TransferID(id),
		IdempotencyKey:    "idem-" + id,
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		State:             state,
		CreatedAt:         cancelNow.Add(-time.Hour),
		UpdatedAt:         cancelNow.Add(-time.Hour),
	})
	repo.rows[id] = trf
}

func cancelUseCase(repo *fakeTransferRepo, pub *fakeEventPublisher) CancelTransfer {
	return CancelTransfer{
		Transfers: repo,
		Events:    pub,
		UoW:       passThroughUoW{},
		Now:       func() time.Time { return cancelNow },
	}
}

func TestCancelTransferCancelsEveryPreReleaseState(t *testing.T) {
	for _, from := range []transfer.TransferState{
		transfer.StateDraft, transfer.StateProposed, transfer.StateApproved, transfer.StateAllocating,
	} {
		t.Run(string(from), func(t *testing.T) {
			repo := newFakeTransferRepo()
			pub := &fakeEventPublisher{}
			seedTransferInState(repo, "trf-1", from)

			result, err := cancelUseCase(repo, pub).Execute(context.Background(),
				CancelTransferInput{TransferID: "trf-1", Reason: "  demand withdrawn  "})
			if err != nil {
				t.Fatalf("cancel from %s: %v", from, err)
			}
			if result.AlreadyCancelled {
				t.Fatal("first cancel must not be reported as already cancelled")
			}
			assertCancelledFrom(t, from, result.Transfer, repo, pub)
		})
	}
}

// assertCancelledFrom checks the full effect of one successful cancel.
func assertCancelledFrom(t *testing.T, from transfer.TransferState, got *transfer.InterWarehouseTransfer, repo *fakeTransferRepo, pub *fakeEventPublisher) {
	t.Helper()
	if got.State() != transfer.StateCancelled {
		t.Fatalf("state = %s, want CANCELLED", got.State())
	}
	audit := got.Audit()
	last := audit[len(audit)-1]
	if last.Event != "TransferCancelled" || last.From != from || last.To != transfer.StateCancelled || last.Reason != "demand withdrawn" {
		t.Fatalf("audit entry = %+v, want TransferCancelled %s->CANCELLED with the trimmed reason", last, from)
	}
	if repo.updates != 1 {
		t.Fatalf("updates = %d, want exactly 1 persisted state change", repo.updates)
	}
	// Only the analytics occurrence: no integration event exists for a
	// cancel (ADR 0011).
	if len(pub.events) != 1 {
		t.Fatalf("published = %d events, want exactly the TransferStateAdvanced occurrence", len(pub.events))
	}
	advanced, ok := pub.events[0].(transfer.StateAdvanced)
	if !ok || advanced.From != from || advanced.To != transfer.StateCancelled || advanced.TransferID != "trf-1" {
		t.Fatalf("event = %+v, want StateAdvanced %s->CANCELLED", pub.events[0], from)
	}
}

func TestCancelTransferRefusesStatesPastRelease(t *testing.T) {
	for _, from := range []transfer.TransferState{
		transfer.StateAllocated, transfer.StatePicked, transfer.StateInTransit,
		transfer.StateArrived, transfer.StateReceived, transfer.StateUnfulfillable,
	} {
		t.Run(string(from), func(t *testing.T) {
			repo := newFakeTransferRepo()
			pub := &fakeEventPublisher{}
			seedTransferInState(repo, "trf-1", from)

			_, err := cancelUseCase(repo, pub).Execute(context.Background(),
				CancelTransferInput{TransferID: "trf-1", Reason: "too late"})
			if !errors.Is(err, ErrTransferNotCancellable) {
				t.Fatalf("err = %v, want ErrTransferNotCancellable", err)
			}
			var illegal *transfer.IllegalTransitionError
			if !errors.As(err, &illegal) || illegal.From != from {
				t.Fatalf("err = %v, want the aggregate's IllegalTransitionError from %s", err, from)
			}
			if got := repo.rows["trf-1"].State(); got != from {
				t.Fatalf("state = %s, want it untouched at %s", got, from)
			}
			if repo.updates != 0 || len(pub.events) != 0 {
				t.Fatalf("a refused cancel must write nothing: updates=%d events=%d", repo.updates, len(pub.events))
			}
		})
	}
}

func TestCancelTransferIsIdempotent(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	seedTransferInState(repo, "trf-1", transfer.StateAllocating)
	uc := cancelUseCase(repo, pub)

	if _, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "trf-1", Reason: "first"}); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	second, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "trf-1", Reason: "second try"})
	if err != nil {
		t.Fatalf("cancelling an already CANCELLED transfer must succeed: %v", err)
	}
	if !second.AlreadyCancelled || second.Transfer.State() != transfer.StateCancelled {
		t.Fatalf("second = %+v, want an AlreadyCancelled no-op", second)
	}
	if repo.updates != 1 || len(pub.events) != 1 {
		t.Fatalf("the no-op must write nothing more: updates=%d events=%d, want 1 and 1", repo.updates, len(pub.events))
	}
	audit := second.Transfer.Audit()
	if audit[len(audit)-1].Reason != "first" {
		t.Fatalf("the original cancel reason must survive, got %+v", audit[len(audit)-1])
	}
}

func TestCancelTransferRejectsBlankReason(t *testing.T) {
	for _, reason := range []string{"", "   ", "\t\n"} {
		repo := newFakeTransferRepo()
		pub := &fakeEventPublisher{}
		seedTransferInState(repo, "trf-1", transfer.StateAllocating)
		_, err := cancelUseCase(repo, pub).Execute(context.Background(),
			CancelTransferInput{TransferID: "trf-1", Reason: reason})
		if !errors.Is(err, ErrInvalidCancellation) {
			t.Fatalf("reason %q: err = %v, want ErrInvalidCancellation", reason, err)
		}
		if repo.rows["trf-1"].State() != transfer.StateAllocating || repo.updates != 0 || len(pub.events) != 0 {
			t.Fatalf("reason %q: a refused cancel must change nothing", reason)
		}
	}
	_, err := cancelUseCase(newFakeTransferRepo(), &fakeEventPublisher{}).Execute(context.Background(),
		CancelTransferInput{TransferID: "  ", Reason: "x"})
	if !errors.Is(err, ErrInvalidCancellation) {
		t.Fatalf("blank id: err = %v, want ErrInvalidCancellation", err)
	}
}

func TestCancelTransferUnknownTransfer(t *testing.T) {
	_, err := cancelUseCase(newFakeTransferRepo(), &fakeEventPublisher{}).Execute(context.Background(),
		CancelTransferInput{TransferID: "trf-nope", Reason: "x"})
	if !errors.Is(err, transfer.ErrTransferNotFound) {
		t.Fatalf("err = %v, want ErrTransferNotFound", err)
	}
}

func TestCancelTransferSurfacesPublishFailure(t *testing.T) {
	// The occurrence rides the same UnitOfWork as the state change: a
	// publish failure must fail the cancel (the real UoW then rolls the
	// state change back), never be swallowed.
	repo := newFakeTransferRepo()
	seedTransferInState(repo, "trf-1", transfer.StateApproved)
	uc := cancelUseCase(repo, nil)
	uc.Events = failingPublisher{}
	if _, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "trf-1", Reason: "x"}); err == nil {
		t.Fatal("a failed analytics publish must fail the cancel")
	}
}

func TestCancelTransferRequiresClock(t *testing.T) {
	uc := cancelUseCase(newFakeTransferRepo(), &fakeEventPublisher{})
	uc.Now = nil
	if _, err := uc.Execute(context.Background(), CancelTransferInput{TransferID: "t", Reason: "r"}); err == nil {
		t.Fatal("a missing clock must be refused")
	}
}
