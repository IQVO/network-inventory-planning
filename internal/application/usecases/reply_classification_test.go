package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TestIsDeterministic_ClassifiesRefusedReplies is the D4 contract: a reply
// that fails the aggregate's validation (line-id mismatch, unknown
// rejection reason, any other allocation-reply mismatch) is DETERMINISTIC —
// the same bytes always fail the same way, so retrying only wedges the
// partition. IsRefusedReply singles those out for dead-lettering; a replay
// or an unknown transfer stays a plain skip.
func TestIsDeterministic_ClassifiesRefusedReplies(t *testing.T) {
	refused := []error{
		fmt.Errorf("wrap: %w", transfer.ErrReplyLineMismatch),
		fmt.Errorf("wrap: %w", transfer.ErrUnknownRejectionReason),
		fmt.Errorf("wrap: %w", transfer.ErrReplyRefused),
		fmt.Errorf("wrap: %w", transfer.ErrFactRefused),
	}
	for _, err := range refused {
		if !IsDeterministic(err) {
			t.Errorf("IsDeterministic(%v) = false, want true", err)
		}
		if !IsRefusedReply(err) {
			t.Errorf("IsRefusedReply(%v) = false, want true", err)
		}
	}

	benign := []error{
		transfer.ErrTransferNotFound,
		&transfer.IllegalTransitionError{ID: "trf-1", From: transfer.StateAllocated, Action: "mark allocated"},
	}
	for _, err := range benign {
		if !IsDeterministic(err) {
			t.Errorf("IsDeterministic(%v) = false, want true", err)
		}
		if IsRefusedReply(err) {
			t.Errorf("IsRefusedReply(%v) = true, want false (a replay/unknown transfer is a skip, not poison)", err)
		}
	}

	transient := errors.New("connection reset by peer")
	if IsDeterministic(transient) || IsRefusedReply(transient) {
		t.Fatalf("an infrastructure error must stay transient")
	}
}

// TestApplyTransferRejection_LineMismatchIsDeterministic drives the use
// case end-to-end over the fake repo: a rejection reply for a foreign line
// returns a deterministic, refused error and leaves the saga ALLOCATING
// with nothing published.
func TestApplyTransferRejection_LineMismatchIsDeterministic(t *testing.T) {
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	pub := &fakeEventPublisher{}
	rejUc := ApplyTransferRejection{Transfers: repo, Events: pub, UoW: passThroughUoW{}}
	err = rejUc.Execute(context.Background(), ApplyRejectionInput{
		TransferID: result.TransferID,
		LineID:     string(result.TransferID) + ":2",
		Reason:     transfer.RejectionInsufficientUsable,
		OccurredAt: approveNow.Add(time.Minute),
	})
	if !IsDeterministic(err) || !IsRefusedReply(err) {
		t.Fatalf("line mismatch err = %v, want deterministic refused reply", err)
	}
	loaded, lerr := repo.Load(context.Background(), result.TransferID)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if loaded.State() != transfer.StateAllocating {
		t.Fatalf("state = %s, want ALLOCATING", loaded.State())
	}
	if len(pub.events) != 0 {
		t.Fatalf("a refused reply must publish nothing, got %d events", len(pub.events))
	}
}
