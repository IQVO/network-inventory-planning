package usecases

import (
	"context"
	"sync"
	"testing"
)

// recordingMetrics is a ports.TransferMetrics fake recording every call.
type recordingMetrics struct {
	mu       sync.Mutex
	approved []string
}

func (r *recordingMetrics) TransferApproved(_ context.Context, outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.approved = append(r.approved, outcome)
}
func (r *recordingMetrics) TransferStateAdvanced(context.Context, string) {}
func (r *recordingMetrics) OutboxRelayed(context.Context, string)         {}

func TestApproveTransferRecordsOutcome(t *testing.T) {
	ctx := context.Background()
	repo := newFakeTransferRepo()
	metrics := &recordingMetrics{}
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	uc.Metrics = metrics

	// 1. a fresh approval -> approved
	if _, err := uc.Execute(ctx, baseApprovalInput()); err != nil {
		t.Fatalf("approve: %v", err)
	}
	// 2. same key + payload -> replayed
	if _, err := uc.Execute(ctx, baseApprovalInput()); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// 3. same key, different payload -> refused (idempotency conflict)
	conflicting := baseApprovalInput()
	conflicting.PolicyVersion = "policy-v9"
	if _, err := uc.Execute(ctx, conflicting); err == nil {
		t.Fatal("conflicting key reuse must fail")
	}
	// 4. invalid input -> refused
	invalid := baseApprovalInput()
	invalid.IdempotencyKey = ""
	if _, err := uc.Execute(ctx, invalid); err == nil {
		t.Fatal("missing idempotency key must fail")
	}

	want := []string{"approved", "replayed", "refused", "refused"}
	if len(metrics.approved) != len(want) {
		t.Fatalf("outcomes = %v, want %v", metrics.approved, want)
	}
	for i := range want {
		if metrics.approved[i] != want[i] {
			t.Fatalf("outcomes = %v, want %v", metrics.approved, want)
		}
	}
}

// TestApproveTransferNilMetricsIsNotInstrumented proves the documented nil
// path: no Metrics configured must neither panic nor change the result.
func TestApproveTransferNilMetricsIsNotInstrumented(t *testing.T) {
	uc := approveUseCase(newFakeTransferRepo(), &fakeEventPublisher{}, approveFacts(), nil)
	if uc.Metrics != nil {
		t.Fatal("precondition: Metrics must be nil")
	}
	if _, err := uc.Execute(context.Background(), baseApprovalInput()); err != nil {
		t.Fatalf("approve without metrics: %v", err)
	}
	invalid := baseApprovalInput()
	invalid.IdempotencyKey = ""
	if _, err := uc.Execute(context.Background(), invalid); err == nil {
		t.Fatal("invalid approval must still fail without metrics")
	}
}
