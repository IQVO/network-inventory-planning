package usecases

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// rebalancePolicies / rebalanceLanes are a minimal placement catalogue:
// enough for the run to have something to evaluate against. They are test
// inputs only — production wires NO catalogue in v1 and never invents one.
func rebalancePolicies() []transfer.Policy {
	return []transfer.Policy{{Version: "policy-v1", Site: "WH2", SKU: "SKU-1", SafetyStock: 5, TargetStock: 20, UnitPriority: 1}}
}

func rebalanceLanes() []transfer.Lane {
	return []transfer.Lane{{Origin: "WH1", Destination: "WH2", LeadTime: 6 * time.Hour, UnitHandlingCost: 1, Enabled: true}}
}

// TestRunScheduledRebalanceWithoutCatalogueIsAFailedRun is the sensor for
// the dishonest-zero escape: with fresh, complete facts but NO placement
// policies / lanes the planner evaluated nothing, so the run must be FAILED
// with the missing catalogue as its reason — never COMPLETED with 0
// proposals, which reads as "nothing to rebalance" — and no
// RebalanceRunCompleted may be published for a run that did not complete.
func TestRunScheduledRebalanceWithoutCatalogueIsAFailedRun(t *testing.T) {
	cases := []struct {
		name     string
		policies []transfer.Policy
		lanes    []transfer.Lane
	}{
		{"no policies, no lanes", nil, nil},
		{"policies but no lanes", rebalancePolicies(), nil},
		{"lanes but no policies", nil, rebalanceLanes()},
		{"empty (non-nil) sets", []transfer.Policy{}, []transfer.Lane{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runs := &fakeRebalanceRuns{}
			pub := &fakeEventPublisher{}
			uc := RunScheduledRebalance{
				Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()}, // fresh and complete
				Planner:      transfer.Planner{},
				Policies:     tc.policies,
				Lanes:        tc.lanes,
				Runs:         runs,
				Events:       pub,
				MaxStaleness: 10 * time.Minute,
				Now:          func() time.Time { return stuckNow },
			}
			id, err := uc.Execute(context.Background())
			if err != nil {
				t.Fatalf("execute: %v (a missing catalogue is a RECORDED failed run, not a failed pass)", err)
			}
			if id != 1 || len(runs.runs) != 1 {
				t.Fatalf("id = %d, runs = %d; want exactly one recorded run", id, len(runs.runs))
			}
			assertCatalogueRefusal(t, runs.runs[0])
			if len(pub.events) != 0 {
				t.Fatalf("published = %d, want 0 (no RebalanceRunCompleted for a run that did not complete)", len(pub.events))
			}
		})
	}
}

// assertCatalogueRefusal checks the recorded row of a run that could not
// evaluate anything.
func assertCatalogueRefusal(t *testing.T, run transfer.RebalanceRun) {
	t.Helper()
	if run.Outcome != transfer.RebalanceFailed {
		t.Fatalf("outcome = %s, want FAILED (nothing could be evaluated)", run.Outcome)
	}
	if run.FailClosedReason == nil || *run.FailClosedReason != NoPlanningCatalogueReason {
		t.Fatalf("fail_closed_reason = %v, want %q", run.FailClosedReason, NoPlanningCatalogueReason)
	}
	if *run.FailClosedReason != "no placement policies / lanes configured; the planner cannot evaluate any transfer" {
		t.Fatalf("the reason text is part of the operator contract, got %q", *run.FailClosedReason)
	}
	if run.ProposalCount != 0 || run.RejectedCount != 0 {
		t.Fatalf("counts = %d/%d, want 0/0", run.ProposalCount, run.RejectedCount)
	}
	if run.SnapshotAsOf.IsZero() {
		t.Fatal("the snapshot WAS built, so its watermark belongs on the row")
	}
}

// TestRunScheduledRebalanceStaleFactsKeepTheirOwnReason pins the ordering:
// a stale read model is reported as stale (its own reason), not masked by
// the catalogue refusal.
func TestRunScheduledRebalanceStaleFactsKeepTheirOwnReason(t *testing.T) {
	runs := &fakeRebalanceRuns{}
	stale := rebalanceFacts()
	for i := range stale.Capabilities {
		stale.Capabilities[i].AsOf = stuckNow.Add(-time.Hour)
	}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: stale},
		Planner:      transfer.Planner{},
		Runs:         runs,
		Events:       &fakeEventPublisher{},
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	reason := runs.runs[0].FailClosedReason
	if runs.runs[0].Outcome != transfer.RebalanceFailed || reason == nil || *reason == NoPlanningCatalogueReason {
		t.Fatalf("run = %+v, want FAILED with the staleness refusal", runs.runs[0])
	}
}

// TestRunScheduledRebalanceWithCatalogueEvaluates proves the structure is
// kept: once a catalogue exists the same path evaluates it, records
// COMPLETED and publishes the occurrence.
func TestRunScheduledRebalanceWithCatalogueEvaluates(t *testing.T) {
	runs := &fakeRebalanceRuns{}
	pub := &fakeEventPublisher{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Planner:      transfer.Planner{},
		Policies:     rebalancePolicies(),
		Lanes:        rebalanceLanes(),
		Runs:         runs,
		Events:       pub,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if runs.runs[0].Outcome != transfer.RebalanceCompleted || runs.runs[0].FailClosedReason != nil {
		t.Fatalf("run = %+v, want COMPLETED with no reason", runs.runs[0])
	}
	if len(pub.events) != 1 {
		t.Fatalf("published = %d, want the RebalanceRunCompleted occurrence", len(pub.events))
	}
}

func TestRunScheduledRebalanceRecordFailureOnMissingCatalogueSurfaces(t *testing.T) {
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Planner:      transfer.Planner{},
		Runs:         &fakeRebalanceRuns{fail: true},
		Events:       &fakeEventPublisher{},
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err == nil {
		t.Fatal("a failed run that cannot be recorded must surface as a pass error")
	}
}
