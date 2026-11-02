package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

var _ ports.IdempotencyStore = (*IdempotencyStore)(nil)

// IdempotencyStore is the pgx-backed ports.IdempotencyStore. Do opens one
// transaction, claims the key with INSERT ... ON CONFLICT DO NOTHING (the
// unique index serialises concurrent requests: the loser waits for the
// winner's commit, then replays), runs the handler inside that transaction
// and stores the answer before committing.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

// NewIdempotencyStore constructs an IdempotencyStore over pool.
func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

// Do implements ports.IdempotencyStore.
func (s *IdempotencyStore) Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("begin idempotent request: %w", err)
	}
	// A rollback after Commit is a harmless no-op; it also covers a panic.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (idempotency_key, fingerprint, status)
		VALUES ($1, $2, 0) ON CONFLICT (idempotency_key) DO NOTHING`, key, fingerprint)
	if err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("claim idempotency key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.replay(ctx, tx, key, fingerprint)
	}

	resp, err := fn(pgtx.With(ctx, tx))
	if err != nil {
		return contract.StoredResponse{}, false, err
	}
	if resp.Status >= 500 {
		return resp, false, nil // rolled back by the deferred Rollback: a 5xx is never stored
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status = $2, content_type = $3, location = $4, body = $5
		WHERE idempotency_key = $1`, key, resp.Status, resp.ContentType, resp.Location, resp.Body); err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("store idempotent answer: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("commit idempotent request: %w", err)
	}
	return resp, false, nil
}

func (s *IdempotencyStore) replay(ctx context.Context, tx pgx.Tx, key, fingerprint string) (contract.StoredResponse, bool, error) {
	var (
		stored string
		resp   contract.StoredResponse
	)
	err := tx.QueryRow(ctx, `SELECT fingerprint, status, content_type, location, body
		FROM idempotency_keys WHERE idempotency_key = $1`, key).
		Scan(&stored, &resp.Status, &resp.ContentType, &resp.Location, &resp.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return contract.StoredResponse{}, false, fmt.Errorf("idempotency key %q vanished mid-claim", key)
	}
	if err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("read idempotency key: %w", err)
	}
	if stored != fingerprint {
		return contract.StoredResponse{}, false, contract.ErrIdempotencyKeyReused
	}
	return resp, true, nil
}
