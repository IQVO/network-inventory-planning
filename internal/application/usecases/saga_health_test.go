package usecases

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// fakeStuckReader implements ports.StuckTransferReader.
type fakeStuckReader struct {
	views []transfer.StuckView
	err   error
}

func (f fakeStuckReader) ListNonTerminal(_ context.Context, _ int) ([]transfer.StuckView, error) {
	return f.views, f.err
}

var stuckNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func TestCheckStuckTransfersPublishesOccurrences(t *testing.T) {
	pub := &fakeEventPublisher{}
	reader := fakeStuckReader{views: []transfer.StuckView{
		{ID: "trf-stuck", State: transfer.StateAllocating, UpdatedAt: stuckNow.Add(-2 * time.Hour)},
		{ID: "trf-fresh", State: transfer.StateAllocating, UpdatedAt: stuckNow.Add(-5 * time.Minute)},
	}}
	uc := CheckStuckTransfers{
		Reader: reader,
		Events: pub,
		Check:  transfer.StuckCheck{Thresholds: transfer.DefaultStuckThresholds()},
		Now:    func() time.Time { return stuckNow },
	}
	got, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got != 1 {
		t.Fatalf("stuck count = %d, want 1", got)
	}
	if len(pub.events) != 1 {
		t.Fatalf("published = %d, want 1", len(pub.events))
	}
	evt, ok := pub.events[0].(transfer.StuckDetected)
	if !ok {
		t.Fatalf("event = %+v", pub.events[0])
	}
	if evt.TransferID != "trf-stuck" || evt.State != transfer.StateAllocating {
		t.Fatalf("event = %+v", evt)
	}
	if evt.AgeSeconds != 7200 || evt.ThresholdSeconds != 3600 {
		t.Fatalf("ages = %+v", evt)
	}
}

func TestCheckStuckTransfersNeverMutates(t *testing.T) {
	// The observe-only contract: even with EVERY transfer stuck, the pass
	// only publishes — the reader's views are the sole input and no
	// repository write port exists on the use case at all.
	pub := &fakeEventPublisher{}
	reader := fakeStuckReader{views: []transfer.StuckView{
		{ID: "trf-1", State: transfer.StateInTransit, UpdatedAt: stuckNow.Add(-200 * time.Hour)},
		{ID: "trf-2", State: transfer.StateInTransit, UpdatedAt: stuckNow.Add(-300 * time.Hour)},
	}}
	uc := CheckStuckTransfers{
		Reader: reader,
		Events: pub,
		Check:  transfer.StuckCheck{Thresholds: transfer.DefaultStuckThresholds()},
		Now:    func() time.Time { return stuckNow },
	}
	got, err := uc.Execute(context.Background())
	if err != nil || got != 2 {
		t.Fatalf("execute = %d, %v; want 2, nil", got, err)
	}
	if len(pub.events) != 2 {
		t.Fatalf("published = %d, want 2 (occurrences only, no state change path exists)", len(pub.events))
	}
}

func TestCheckStuckTransfersReaderErrorFailsPass(t *testing.T) {
	uc := CheckStuckTransfers{
		Reader: fakeStuckReader{err: fmt.Errorf("pool down")},
		Events: &fakeEventPublisher{},
		Check:  transfer.StuckCheck{Thresholds: transfer.DefaultStuckThresholds()},
		Now:    func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err == nil {
		t.Fatal("reader error must fail the pass (retried next tick), never be swallowed")
	}
}

// --- scheduled rebalance ------------------------------------------------------

// fakeRebalanceRuns implements ports.RebalanceRunRepository.
type fakeRebalanceRuns struct {
	mu   sync.Mutex
	runs []transfer.RebalanceRun
	next int64
	fail bool
}

func (f *fakeRebalanceRuns) Record(_ context.Context, run transfer.RebalanceRun) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, fmt.Errorf("injected failure")
	}
	f.next++
	run.ID = f.next
	f.runs = append(f.runs, run)
	return run.ID, nil
}

func (f *fakeRebalanceRuns) List(_ context.Context, _ int) ([]transfer.RebalanceRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]transfer.RebalanceRun, len(f.runs))
	copy(out, f.runs)
	return out, nil
}

// emptyFacts is a completely empty read model (BuildSnapshot refuses).
func emptyFacts() planning.Facts {
	return planning.Facts{}
}

// rebalanceFacts builds a fresh, complete fact set around stuckNow so
// the fail-closed snapshot build passes.
func rebalanceFacts() planning.Facts {
	due := stuckNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, stuckNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, stuckNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = stuckNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = stuckNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: stuckNow.Add(time.Hour), WindowEnd: stuckNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: stuckNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: stuckNow.Add(time.Hour), WindowEnd: stuckNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: stuckNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

func TestRunScheduledRebalanceRecordsCompletedRun(t *testing.T) {
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
	id, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if id != 1 {
		t.Fatalf("run id = %d, want 1", id)
	}
	recorded := runs.runs[0]
	if recorded.Outcome != transfer.RebalanceCompleted {
		t.Fatalf("outcome = %s, want COMPLETED", recorded.Outcome)
	}
	if recorded.FailClosedReason != nil {
		t.Fatalf("fail_closed_reason = %v, want nil", *recorded.FailClosedReason)
	}
	if len(pub.events) != 1 {
		t.Fatalf("published = %d, want the RebalanceRunCompleted occurrence", len(pub.events))
	}
	evt, ok := pub.events[0].(transfer.RebalanceRunCompleted)
	if !ok {
		t.Fatalf("event = %+v", pub.events[0])
	}
	if evt.ProposalCount != recorded.ProposalCount {
		t.Fatalf("occurrence proposals %d != row %d", evt.ProposalCount, recorded.ProposalCount)
	}
}

func TestRunScheduledRebalanceFailClosedRecordsReason(t *testing.T) {
	// EMPTY read models: BuildSnapshot refuses (no capability facts) —
	// the run must record outcome FAILED with the refusal as its reason,
	// not error the pass.
	runs := &fakeRebalanceRuns{}
	pub := &fakeEventPublisher{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: emptyFacts()},
		Planner:      transfer.Planner{},
		Runs:         runs,
		Events:       pub,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	id, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v (a fail-closed snapshot is a RECORDED run, not a failed pass)", err)
	}
	if id != 1 {
		t.Fatalf("run id = %d, want 1", id)
	}
	recorded := runs.runs[0]
	if recorded.Outcome != transfer.RebalanceFailed {
		t.Fatalf("outcome = %s, want FAILED", recorded.Outcome)
	}
	if recorded.FailClosedReason == nil || *recorded.FailClosedReason == "" {
		t.Fatal("fail_closed_reason must carry the BuildSnapshot refusal")
	}
	if len(pub.events) != 0 {
		t.Fatalf("published = %d, want 0 (a failed run completes, it does not celebrate)", len(pub.events))
	}
}

func TestRunScheduledRebalanceStaleFactsFailClosed(t *testing.T) {
	// Facts older than MaxStaleness refuse too.
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
	if runs.runs[0].Outcome != transfer.RebalanceFailed {
		t.Fatalf("outcome = %s, want FAILED (stale facts refuse the snapshot)", runs.runs[0].Outcome)
	}
}

func TestListRebalanceRunsClampsLimit(t *testing.T) {
	runs := &fakeRebalanceRuns{}
	uc := ListRebalanceRuns{Runs: runs}
	// Default.
	got, err := uc.Execute(context.Background(), 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("default limit: %v %v", got, err)
	}
	// The clamp lives in the use case: 0 -> 50, 999 -> 200. Prove via
	// what the repo was asked for.
	probing := &limitProbingRuns{}
	uc2 := ListRebalanceRuns{Runs: probing}
	if _, err := uc2.Execute(context.Background(), 0); err != nil || probing.lastLimit != 50 {
		t.Fatalf("limit 0 -> %d %v, want 50", probing.lastLimit, err)
	}
	if _, err := uc2.Execute(context.Background(), 999); err != nil || probing.lastLimit != 200 {
		t.Fatalf("limit 999 -> %d %v, want 200", probing.lastLimit, err)
	}
}

// limitProbingRuns records the limit it was asked for.
type limitProbingRuns struct{ lastLimit int }

func (f *limitProbingRuns) Record(context.Context, transfer.RebalanceRun) (int64, error) {
	return 1, nil
}
func (f *limitProbingRuns) List(_ context.Context, limit int) ([]transfer.RebalanceRun, error) {
	f.lastLimit = limit
	return nil, nil
}

func TestRunScheduledRebalanceDerivesRunID(t *testing.T) {
	pub := &fakeEventPublisher{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Planner:      transfer.Planner{},
		Policies:     rebalancePolicies(),
		Lanes:        rebalanceLanes(),
		Runs:         &fakeRebalanceRuns{},
		Events:       pub,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("published = %d, want 1", len(pub.events))
	}
	// The default run id is derived (stable shape), not a NewRunID call.
	evt := pub.events[0].(transfer.RebalanceRunCompleted)
	if evt.RunID == "" {
		t.Fatal("run id must not be empty")
	}
	if !strings.HasPrefix(evt.RunID, "rebal-") {
		t.Fatalf("run id = %q, want the derived rebal-<id>-<ts> shape", evt.RunID)
	}
}

func TestRunScheduledRebalanceLoadErrorRecordsFailedRun(t *testing.T) {
	runs := &fakeRebalanceRuns{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{err: fmt.Errorf("connection refused")},
		Runs:         runs,
		Events:       &fakeEventPublisher{},
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("a read-model outage is a RECORDED failed run, not a pass error: %v", err)
	}
	if runs.runs[0].Outcome != transfer.RebalanceFailed || runs.runs[0].FailClosedReason == nil {
		t.Fatalf("run = %+v, want FAILED with the load error as reason", runs.runs[0])
	}
}

func TestRunScheduledRebalancePublishFailureSurfaces(t *testing.T) {
	// The row persists; a publish failure must surface for the loop to log.
	runs := &fakeRebalanceRuns{}
	uc := RunScheduledRebalance{
		Snapshot:     fakeApproveSnapshot{facts: rebalanceFacts()},
		Planner:      transfer.Planner{},
		Policies:     rebalancePolicies(),
		Lanes:        rebalanceLanes(),
		Runs:         runs,
		Events:       &failingPublisher{},
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return stuckNow },
	}
	id, err := uc.Execute(context.Background())
	if err == nil {
		t.Fatal("publish failure must surface")
	}
	if id == 0 || len(runs.runs) != 1 {
		t.Fatalf("the row must be persisted despite the publish failure: id=%d runs=%d", id, len(runs.runs))
	}
}

// failingPublisher always fails.
type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, ...transfer.DomainEvent) error {
	return fmt.Errorf("injected publish failure")
}
