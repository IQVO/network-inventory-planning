package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ListPage returns up to limit runs ordered by (started_at DESC, id DESC),
// continuing after the keyset (Sort = started_at RFC 3339 nano, ID = run id).
func (r *RebalanceRunRepo) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.RebalanceRun, error) {
	var (
		startedAfter any
		idAfter      any
	)
	if after != nil {
		at, err := time.Parse(time.RFC3339Nano, after.Sort)
		id, idErr := strconv.ParseInt(after.ID, 10, 64)
		if err != nil || idErr != nil {
			return nil, transfer.ErrInvalidPageAfter
		}
		startedAfter, idAfter = at, id
	}
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT id, started_at, snapshot_as_of, proposal_count, rejected_count, outcome, fail_closed_reason
		FROM rebalance_runs
		WHERE $1::timestamptz IS NULL OR (started_at, id) < ($1::timestamptz, $2::bigint)
		ORDER BY started_at DESC, id DESC
		LIMIT $3
	`, startedAfter, idAfter, limit)
	if err != nil {
		return nil, fmt.Errorf("list rebalance runs page: %w", err)
	}
	defer rows.Close()
	runs := []transfer.RebalanceRun{}
	for rows.Next() {
		var run transfer.RebalanceRun
		var outcome string
		var snapshotAsOf *time.Time
		if err := rows.Scan(&run.ID, &run.StartedAt, &snapshotAsOf, &run.ProposalCount,
			&run.RejectedCount, &outcome, &run.FailClosedReason); err != nil {
			return nil, fmt.Errorf("list rebalance runs page: %w", err)
		}
		if snapshotAsOf != nil {
			run.SnapshotAsOf = *snapshotAsOf
		}
		run.Outcome = transfer.RebalanceOutcome(outcome)
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rebalance runs page: %w", err)
	}
	return runs, nil
}
