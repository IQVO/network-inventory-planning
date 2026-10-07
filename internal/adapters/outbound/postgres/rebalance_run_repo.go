package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Compile-time assertion that RebalanceRunRepo satisfies the port.
var _ ports.RebalanceRunRepository = (*RebalanceRunRepo)(nil)

// RebalanceRunRepo is the pgx-backed transfer.RebalanceRunRepository: one
// insert-only row per scheduled rebalance pass plus the newest-first
// listing GET /v1/rebalance-runs serves (ADR 0007).
type RebalanceRunRepo struct {
	pool *pgxpool.Pool
}

// NewRebalanceRunRepo constructs a RebalanceRunRepo over pool.
func NewRebalanceRunRepo(pool *pgxpool.Pool) *RebalanceRunRepo {
	return &RebalanceRunRepo{pool: pool}
}

// Record persists run and returns its database id.
func (r *RebalanceRunRepo) Record(ctx context.Context, run transfer.RebalanceRun) (int64, error) {
	q := queryFor(ctx, r.pool)
	var id int64
	err := q.QueryRow(ctx, `
		INSERT INTO rebalance_runs
			(started_at, snapshot_as_of, proposal_count, rejected_count, outcome, fail_closed_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, run.StartedAt, nullableTime(run.SnapshotAsOf), run.ProposalCount, run.RejectedCount,
		string(run.Outcome), run.FailClosedReason).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("record rebalance run: %w", err)
	}
	return id, nil
}

// List returns runs newest-first, bounded by limit (default 50).
func (r *RebalanceRunRepo) List(ctx context.Context, limit int) ([]transfer.RebalanceRun, error) {
	if limit <= 0 {
		limit = 50
	}
	q := queryFor(ctx, r.pool)
	rows, err := q.Query(ctx, `
		SELECT id, started_at, snapshot_as_of, proposal_count, rejected_count, outcome, fail_closed_reason
		FROM rebalance_runs
		ORDER BY started_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list rebalance runs: %w", err)
	}
	defer rows.Close()
	runs := []transfer.RebalanceRun{}
	for rows.Next() {
		var run transfer.RebalanceRun
		var outcome string
		var snapshotAsOf *time.Time
		if err := rows.Scan(&run.ID, &run.StartedAt, &snapshotAsOf, &run.ProposalCount,
			&run.RejectedCount, &outcome, &run.FailClosedReason); err != nil {
			return nil, fmt.Errorf("list rebalance runs: %w", err)
		}
		if snapshotAsOf != nil {
			run.SnapshotAsOf = *snapshotAsOf
		}
		run.Outcome = transfer.RebalanceOutcome(outcome)
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rebalance runs: %w", err)
	}
	return runs, nil
}

// nullableTime maps a zero time onto a NULL snapshot_as_of (a FAILED run
// may never have reached a watermark).
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
