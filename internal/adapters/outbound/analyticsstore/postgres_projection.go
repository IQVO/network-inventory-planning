package analyticsstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// Projection is the Postgres WRITER (report.Projection). Apply claims the
// event id and appends the fact row in ONE transaction: the id is recorded
// if and only if its effect is, so a failure anywhere leaves nothing behind
// and the redelivered event is applied afresh, while a replay of an applied
// id is a no-op.
type Projection struct {
	pool *pgxpool.Pool
}

// NewProjection constructs a Projection over the writer pool.
func NewProjection(pool *pgxpool.Pool) *Projection { return &Projection{pool: pool} }

var _ report.Projection = (*Projection)(nil)

const claimSQL = `
	INSERT INTO analytics_processed_events (event_id, event_type, occurred_at)
	VALUES ($1, $2, $3)
	ON CONFLICT (event_id) DO NOTHING`

const (
	insertAdvanceSQL = `
	INSERT INTO transfer_state_advances (event_id, transfer_id, from_state, to_state, age_seconds, dwell_seconds, occurred_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

	insertStuckSQL = `
	INSERT INTO transfer_stuck_detections (event_id, transfer_id, state, age_seconds, threshold_seconds, occurred_at)
	VALUES ($1, $2, $3, $4, $5, $6)`

	insertRunSQL = `
	INSERT INTO rebalance_run_facts (event_id, run_id, proposal_count, rejected_count, stale_facts, occurred_at)
	VALUES ($1, $2, $3, $4, $5, $6)`
)

// Apply implements report.Projection.
func (p *Projection) Apply(ctx context.Context, e report.Event) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("analyticsstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	tag, err := tx.Exec(ctx, claimSQL, e.EventID, string(e.Kind), e.At)
	if err != nil {
		return false, classify(fmt.Errorf("analyticsstore: claim event %s: %w", e.EventID, err))
	}
	if tag.RowsAffected() == 0 {
		return false, nil // already applied: the deferred rollback ends the empty tx
	}
	if err := insertFact(ctx, tx, e); err != nil {
		return false, classify(fmt.Errorf("analyticsstore: project %s %s: %w", e.Kind, e.EventID, err))
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("analyticsstore: commit: %w", err)
	}
	return true, nil
}

// errUnknownKind is deterministic: no retry can make the store accept it.
var errUnknownKind = errors.New("unknown event kind")

func insertFact(ctx context.Context, tx pgx.Tx, e report.Event) error {
	var err error
	switch e.Kind {
	case report.KindStateAdvanced:
		// e.DwellSeconds is a *int64: pgx writes nil as NULL (never 0).
		_, err = tx.Exec(ctx, insertAdvanceSQL, e.EventID, e.TransferID, e.From, e.To, e.AgeSeconds, e.DwellSeconds, e.At)
	case report.KindStuckDetected:
		_, err = tx.Exec(ctx, insertStuckSQL, e.EventID, e.TransferID, e.State, e.AgeSeconds, e.ThresholdSeconds, e.At)
	case report.KindRebalanceRunCompleted:
		_, err = tx.Exec(ctx, insertRunSQL, e.EventID, e.RunID, e.ProposalCount, e.RejectedCount, e.StaleFacts, e.At)
	default:
		err = fmt.Errorf("%w %q", errUnknownKind, e.Kind)
	}
	return err
}

// classify wraps a Postgres data-exception (SQLSTATE class 22) or
// integrity-violation (class 23) error, and an unknown event kind, in
// report.ErrRejected: the same event can never succeed, so the consumer
// dead-letters it instead of retrying forever. Everything else (connection
// loss, timeouts, locks, failovers) stays transient.
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.Is(err, errUnknownKind) || (errors.As(err, &pgErr) && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23")) {
		return fmt.Errorf("%w: %w", report.ErrRejected, err)
	}
	return err
}
