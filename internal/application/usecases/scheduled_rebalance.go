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
	// uses.
	Planner transfer.Planner
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

	proposals := u.Planner.Generate(
		snapshotPositions(snap),
		nil, // policies: none projected in v1; the planner proposes from
		// positions+lanes only when policies exist, so an empty policy
		// set yields zero proposals without error — recorded as a
		// completed run that had nothing to propose.
		nil, // lanes: none projected in v1.
	)

	run := transfer.RebalanceRun{
		StartedAt:     startedAt,
		SnapshotAsOf:  snap.AsOf,
		ProposalCount: len(proposals),
		RejectedCount: 0,
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
		RejectedCount: 0,
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

// snapshotPositions projects the fail-closed snapshot into planner
// Positions. v1 records them empty: the planner's lane/policy inputs are
// not projected read models yet (a later phase wires the lane catalogue);
// the pass still exercises the SAME snapshot build + planner pipeline so
// the run history tracks snapshot health from day one.
func snapshotPositions(snap planning.PlanningSnapshot) []transfer.Position {
	return nil
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
