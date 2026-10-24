package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// RunScheduledRebalance is one OBSERVE-ONLY rebalance pass (ADR 0007): it
// builds the SAME fail-closed snapshot the simulation builds, runs the
// SAME planner, persists a rebalance_runs row and publishes one
// RebalanceRunCompleted analytics occurrence. It NEVER approves anything
// and NEVER emits an allocation command: proposals are advisory, exactly
// like POST /v1/transfer-proposals:generate — turning one into a real
// transfer stays an operator's explicit POST /v1/transfers:approve.
//
// A fail-closed snapshot build (stale/missing facts) is NOT an error of
// the pass: the run records outcome FAILED with the refusal as its
// fail_closed_reason, so a chronically stale read model is visible in the
// run history rather than swallowed.
type RunScheduledRebalance struct {
	// Snapshot loads the three read models.
	Snapshot ports.PlanningSnapshotRepository
	// Planner is the same deterministic planner the generate endpoint
	// uses; the scheduled pass calls its PlanFromSnapshot (ADR 0015).
	Planner transfer.Planner
	// Policies and Lanes are the operator-managed Rebalance Policy and
	// Transfer Lane catalogues (ADR 0015) the pass plans from. Nil means
	// an empty catalogue (the pass then has nothing to propose).
	Policies ports.RebalancePolicyRepository
	Lanes    ports.TransferLaneRepository
	// Packs is the local pack-level view (ADR 0017): a SKU with an active
	// CASE level is planned in whole cases. Nil plans every SKU in eaches.
	Packs ports.PackLevelView
	// Runs persists the rebalance_runs row.
	Runs ports.RebalanceRunRepository
	// Events publishes the RebalanceRunCompleted occurrence.
	Events ports.TransferEventPublisher
	// MaxStaleness bounds fact freshness (same budget as Simulate).
	MaxStaleness time.Duration
	// Now supplies the run clock.
	Now func() time.Time
	// NewRunID mints the run id (subject/key of the analytics
	// occurrence); nil means the derived "rebal-<id>-<startedAt>" form.
	NewRunID func() string
}

// Execute runs one pass and returns the persisted run id.
func (u RunScheduledRebalance) Execute(ctx context.Context) (int64, error) {
	if u.Now == nil {
		return 0, fmt.Errorf("run scheduled rebalance: clock is required")
	}
	startedAt := u.Now().UTC()

	facts, err := u.Snapshot.Load(ctx)
	if err != nil {
		// An unusable read side is a FAILED run too: the row makes the
		// outage visible in run history.
		run := transfer.RebalanceRun{
			StartedAt:        startedAt,
			Outcome:          transfer.RebalanceFailed,
			FailClosedReason: strPtr(fmt.Sprintf("load read models: %v", err)),
		}
		id, rerr := u.Runs.Record(ctx, run)
		if rerr != nil {
			return 0, fmt.Errorf("run scheduled rebalance: record failed run: %w (load error: %v)", rerr, err)
		}
		return id, nil
	}

	snap, err := planning.BuildSnapshot(planning.SnapshotInput{
		Capabilities: facts.Capabilities,
		Demands:      facts.Demands,
		Plans:        facts.Plans,
		MaxStaleness: u.MaxStaleness,
		Now:          func() time.Time { return startedAt },
	})
	if err != nil {
		run := transfer.RebalanceRun{
			StartedAt:        startedAt,
			SnapshotAsOf:     facts.Watermark(),
			Outcome:          transfer.RebalanceFailed,
			FailClosedReason: strPtr(err.Error()),
		}
		id, rerr := u.Runs.Record(ctx, run)
		if rerr != nil {
			return 0, fmt.Errorf("run scheduled rebalance: record failed run: %w (build error: %v)", rerr, err)
		}
		return id, nil
	}

	policies, lanes, err := u.loadCatalogue(ctx)
	if err != nil {
		// An unreadable catalogue is a FAILED run, never a silent
		// zero-proposal COMPLETED one.
		return u.failedRun(ctx, startedAt, snap.AsOf, fmt.Sprintf("load rebalance catalogue: %v", err), err)
	}
	multiples, err := ActiveCaseMultiples(ctx, u.Packs, policySKUs(policies), startedAt)
	if err != nil {
		// An unreadable pack view is a FAILED run too: planning in eaches
		// instead would silently break the case-multiple rule.
		return u.failedRun(ctx, startedAt, snap.AsOf, fmt.Sprintf("read pack levels: %v", err), err)
	}
	proposals, rejected := u.Planner.WithCaseMultiples(multiples).PlanFromSnapshot(snap, policies, lanes)

	run := transfer.RebalanceRun{
		StartedAt:     startedAt,
		SnapshotAsOf:  snap.AsOf,
		ProposalCount: len(proposals),
		RejectedCount: rejected,
		Outcome:       transfer.RebalanceCompleted,
	}
	id, err := u.Runs.Record(ctx, run)
	if err != nil {
		return 0, fmt.Errorf("run scheduled rebalance: record run: %w", err)
	}

	completedAt := u.Now().UTC()
	evt := transfer.RebalanceRunCompleted{
		RunID:         u.runID(id, startedAt),
		ProposalCount: len(proposals),
		RejectedCount: rejected,
		StaleFacts:    0,
		CompletedAt:   completedAt,
	}
	if err := u.Events.Publish(ctx, evt); err != nil {
		// The row is persisted; only the occurrence failed. The next
		// tick republishes nothing (occurrences are per-run), so surface
		// the error for the loop to log.
		return id, fmt.Errorf("run scheduled rebalance: publish RebalanceRunCompleted: %w", err)
	}
	return id, nil
}

// runID derives the analytics subject of one run: the database id plus
// the start time, stable and unique per row.
func (u RunScheduledRebalance) runID(id int64, startedAt time.Time) string {
	if u.NewRunID != nil {
		return u.NewRunID()
	}
	return fmt.Sprintf("rebal-%d-%s", id, startedAt.UTC().Format("20060102T150405Z"))
}

// loadCatalogue reads both catalogues as planner inputs. A nil
// repository is an empty catalogue.
func (u RunScheduledRebalance) loadCatalogue(ctx context.Context) ([]transfer.Policy, []transfer.Lane, error) {
	var (
		policies []transfer.Policy
		lanes    []transfer.Lane
	)
	if u.Policies != nil {
		entries, err := u.Policies.List(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("policies: %w", err)
		}
		for _, e := range entries {
			policies = append(policies, e.Policy)
		}
	}
	if u.Lanes != nil {
		entries, err := u.Lanes.List(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("lanes: %w", err)
		}
		for _, e := range entries {
			lanes = append(lanes, e.Lane)
		}
	}
	return policies, lanes, nil
}

// recordFailed persists a FAILED run; cause is only reported when the
// row itself cannot be written.
func (u RunScheduledRebalance) recordFailed(ctx context.Context, run transfer.RebalanceRun, cause error) (int64, error) {
	id, err := u.Runs.Record(ctx, run)
	if err != nil {
		return 0, fmt.Errorf("run scheduled rebalance: record failed run: %w (cause: %v)", err, cause)
	}
	return id, nil
}

// failedRun records a FAILED run for a fail-closed reason after the snapshot
// was built.
func (u RunScheduledRebalance) failedRun(ctx context.Context, startedAt, asOf time.Time, reason string, cause error) (int64, error) {
	return u.recordFailed(ctx, transfer.RebalanceRun{
		StartedAt:        startedAt,
		SnapshotAsOf:     asOf,
		Outcome:          transfer.RebalanceFailed,
		FailClosedReason: strPtr(reason),
	}, cause)
}

// strPtr is a tiny helper for the nullable fail_closed_reason.
func strPtr(s string) *string { return &s }

// ListRebalanceRuns serves GET /v1/rebalance-runs (ADR 0007): the run
// history newest-first. Read-only; an operator inspects run health here,
// and every run is observe-only by construction.
type ListRebalanceRuns struct {
	Runs ports.RebalanceRunRepository
}

// Execute lists runs bounded by limit (clamped to [1,200], default 50).
func (u ListRebalanceRuns) Execute(ctx context.Context, limit int) ([]transfer.RebalanceRun, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	return u.Runs.List(ctx, limit)
}
