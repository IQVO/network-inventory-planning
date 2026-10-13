package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// failingPolicyRepo fails List.
type failingPolicyRepo struct{ fakePolicyRepo }

func (*failingPolicyRepo) List(context.Context) ([]transfer.RebalancePolicy, error) {
	return nil, errors.New("policies table unreachable")
}

func seedCatalogue(t *testing.T) (*fakePolicyRepo, *fakeLaneRepo) {
	t.Helper()
	ctx := context.Background()
	policies, lanes := newFakePolicyRepo(), newFakeLaneRepo()
	putPolicy := PutRebalancePolicy{Policies: policies, Now: func() time.Time { return stuckNow }}
	for _, p := range []transfer.Policy{
		{Version: "pv-wh1", Site: "WH1", SKU: "SKU-1", SafetyStock: 10, TargetStock: 100, UnitPriority: 1},
		{Version: "pv-wh2", Site: "WH2", SKU: "SKU-1", SafetyStock: 0, TargetStock: 100, UnitPriority: 10},
	} {
		if _, err := putPolicy.Execute(ctx, p, nil); err != nil {
			t.Fatalf("seed policy: %v", err)
		}
	}
	putLane := PutTransferLane{Lanes: lanes, Now: func() time.Time { return stuckNow }}
	if _, err := putLane.Execute(ctx, transfer.Lane{Origin: "WH1", Destination: "WH2", LeadTime: 24 * time.Hour, UnitHandlingCost: 1, Enabled: true}, nil); err != nil {
		t.Fatalf("seed lane: %v", err)
	}
	return policies, lanes
}

// TestRunScheduledRebalanceProposesFromTheCatalogue is D6: with the
// policy and lane catalogues filled, the scheduled pass is no longer a
// no-op — it records the planner's proposals and announces them.
func TestRunScheduledRebalanceProposesFromTheCatalogue(t *testing.T) {
	policies, lanes := seedCatalogue(t)
	runs := &fakeRebalanceRuns{}
	pub := &fakeEventPublisher{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Planner:      transfer.Planner{},
		Policies:     policies,
		Lanes:        lanes,
		Runs:         runs,
		Events:       pub,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	recorded := runs.runs[0]
	// WH2 needs min(demand 10, target 100) = 10; WH1 donates up to
	// 500 - 40 - 10 = 450.
	if recorded.Outcome != transfer.RebalanceCompleted || recorded.ProposalCount != 1 || recorded.RejectedCount != 0 {
		t.Fatalf("run = %+v, want COMPLETED with 1 proposal", recorded)
	}
	evt := pub.events[0].(transfer.RebalanceRunCompleted)
	if evt.ProposalCount != 1 || evt.RejectedCount != 0 {
		t.Fatalf("occurrence = %+v", evt)
	}
	for _, e := range pub.events {
		switch e.(type) {
		case transfer.PlanApproved, transfer.AllocationRequested, transfer.DemandReleased:
			t.Fatalf("a scheduled run is observe-only; it published %s", e.EventName())
		}
	}
}

// TestRunScheduledRebalanceCatalogueOutageIsAFailedRun: an unreadable
// catalogue is recorded like an unreadable read model — a FAILED run that
// says why — never a silent zero-proposal COMPLETED run.
func TestRunScheduledRebalanceCatalogueOutageIsAFailedRun(t *testing.T) {
	_, lanes := seedCatalogue(t)
	runs := &fakeRebalanceRuns{}
	pub := &fakeEventPublisher{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Policies:     &failingPolicyRepo{},
		Lanes:        lanes,
		Runs:         runs,
		Events:       pub,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("a catalogue outage is a RECORDED failed run: %v", err)
	}
	recorded := runs.runs[0]
	if recorded.Outcome != transfer.RebalanceFailed || recorded.FailClosedReason == nil ||
		!strings.Contains(*recorded.FailClosedReason, "load rebalance catalogue") {
		t.Fatalf("run = %+v", recorded)
	}
	if recorded.SnapshotAsOf.IsZero() {
		t.Fatal("the snapshot was built: its watermark belongs on the failed run")
	}
	if len(pub.events) != 0 {
		t.Fatalf("a failed run publishes nothing, got %d events", len(pub.events))
	}
}
