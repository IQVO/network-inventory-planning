package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// Reader is the Postgres READER (report.Reader). Every query filters
// occurred_at with `>= from AND < to`: from inclusive, to exclusive. Days are
// UTC calendar days. Aggregation that is plain arithmetic (rates) is left to
// internal/analytics/report; SQL only counts and sums, plus percentile_cont
// for the age percentiles.
type Reader struct {
	pool *pgxpool.Pool
}

// NewReader constructs a Reader over the (read-only) reports pool.
func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

var _ report.Reader = (*Reader)(nil)

const funnelSQL = `
	SELECT (occurred_at AT TIME ZONE 'UTC')::date AS day, to_state, count(DISTINCT transfer_id)
	FROM transfer_state_advances
	WHERE occurred_at >= $1 AND occurred_at < $2
	GROUP BY day, to_state
	ORDER BY day, to_state`

// TransferFunnel implements report.Reader.
func (r *Reader) TransferFunnel(ctx context.Context, rg report.Range) ([]report.FunnelDay, error) {
	return collect(ctx, r.pool, funnelSQL, func(rows pgx.Rows) (report.FunnelDay, error) {
		var d report.FunnelDay
		err := rows.Scan(&d.Day, &d.State, &d.Transfers)
		d.Day = d.Day.UTC()
		return d, err
	}, rg.From, rg.To)
}

const dwellSQL = `
	SELECT (occurred_at AT TIME ZONE 'UTC')::date AS day, from_state, count(*),
	       count(*) FILTER (WHERE dwell_seconds IS NULL),
	       percentile_cont(0.5)  WITHIN GROUP (ORDER BY dwell_seconds::double precision),
	       percentile_cont(0.95) WITHIN GROUP (ORDER BY dwell_seconds::double precision)
	FROM transfer_state_advances
	WHERE occurred_at >= $1 AND occurred_at < $2 AND from_state <> '' AND from_state <> to_state
	GROUP BY day, from_state
	ORDER BY day, from_state`

// StateDwell implements report.Reader. percentile_cont ignores NULL
// dwell_seconds (events without the field are counted in without_dwell, not
// treated as zero) and is NULL when every row is NULL.
func (r *Reader) StateDwell(ctx context.Context, rg report.Range) ([]report.DwellDay, error) {
	return collect(ctx, r.pool, dwellSQL, func(rows pgx.Rows) (report.DwellDay, error) {
		var d report.DwellDay
		err := rows.Scan(&d.Day, &d.State, &d.Transitions, &d.WithoutDwell, &d.P50Seconds, &d.P95Seconds)
		d.Day = d.Day.UTC()
		return d, err
	}, rg.From, rg.To)
}

const stuckDaysSQL = `
	SELECT (occurred_at AT TIME ZONE 'UTC')::date AS day, state, count(*), count(DISTINCT transfer_id)
	FROM transfer_stuck_detections
	WHERE occurred_at >= $1 AND occurred_at < $2
	GROUP BY day, state
	ORDER BY day, state`

// StuckDays implements report.Reader.
func (r *Reader) StuckDays(ctx context.Context, rg report.Range) ([]report.StuckDay, error) {
	return collect(ctx, r.pool, stuckDaysSQL, func(rows pgx.Rows) (report.StuckDay, error) {
		var d report.StuckDay
		err := rows.Scan(&d.Day, &d.State, &d.Detections, &d.Transfers)
		d.Day = d.Day.UTC()
		return d, err
	}, rg.From, rg.To)
}

const stuckLatestSQL = `
	SELECT transfer_id, state, age_seconds, threshold_seconds, occurred_at
	FROM transfer_stuck_detections
	WHERE occurred_at >= $1 AND occurred_at < $2
	ORDER BY occurred_at DESC, transfer_id, event_id
	LIMIT $3`

// StuckLatest implements report.Reader.
func (r *Reader) StuckLatest(ctx context.Context, rg report.Range, limit int) ([]report.StuckOccurrence, error) {
	return collect(ctx, r.pool, stuckLatestSQL, func(rows pgx.Rows) (report.StuckOccurrence, error) {
		var o report.StuckOccurrence
		err := rows.Scan(&o.TransferID, &o.State, &o.AgeSeconds, &o.ThresholdSeconds, &o.DetectedAt)
		o.DetectedAt = o.DetectedAt.UTC()
		return o, err
	}, rg.From, rg.To, limit)
}

const rebalanceSQL = `
	SELECT (occurred_at AT TIME ZONE 'UTC')::date AS day, count(*),
	       COALESCE(sum(proposal_count), 0), COALESCE(sum(rejected_count), 0), COALESCE(sum(stale_facts), 0)
	FROM rebalance_run_facts
	WHERE occurred_at >= $1 AND occurred_at < $2
	GROUP BY day
	ORDER BY day`

// RebalanceDays implements report.Reader.
func (r *Reader) RebalanceDays(ctx context.Context, rg report.Range) ([]report.RebalanceDay, error) {
	return collect(ctx, r.pool, rebalanceSQL, func(rows pgx.Rows) (report.RebalanceDay, error) {
		var d report.RebalanceDay
		err := rows.Scan(&d.Day, &d.Runs, &d.Proposals, &d.Rejected, &d.StaleFacts)
		d.Day = d.Day.UTC()
		return d, err
	}, rg.From, rg.To)
}

// LastEventAt implements report.Reader: the newest CloudEvents time among the
// applied events, nil while none was applied.
func (r *Reader) LastEventAt(ctx context.Context) (*time.Time, error) {
	var at *time.Time
	if err := r.pool.QueryRow(ctx, `SELECT max(occurred_at) FROM analytics_processed_events`).Scan(&at); err != nil {
		return nil, fmt.Errorf("analyticsstore: last event: %w", err)
	}
	if at != nil {
		utc := at.UTC()
		at = &utc
	}
	return at, nil
}

// collect runs query with args and scans every row; the result is a non-nil
// slice even when empty.
func collect[T any](ctx context.Context, pool *pgxpool.Pool, query string, scan func(pgx.Rows) (T, error), args ...any) ([]T, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("analyticsstore: query: %w", err)
	}
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("analyticsstore: scan: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyticsstore: rows: %w", err)
	}
	return out, nil
}
