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
// transaction, claims the key with INSERT ... ON CONFLICT (the unique index
// serialises concurrent requests: the loser waits for the winner's commit,
// then replays), runs the handler inside that transaction and stores the
// answer before committing. The retention lives here too (ADR 0024, addendum
// 2026-11-22): a stored answer lives for contract.IdempotencyTTL — the claim
// re-claims (refreshes) a row past its window, the replay read ignores a row
// older than the TTL, and PurgeExpired deletes the dead rows both already
// ignore, oldest first, bounded, never a row a request holds.
//
// The handler runs behind a SAVEPOINT. A mapped domain conflict (a lost race
// on a unique index, 23505 -> 409) fails a statement on the server and leaves
// the transaction ABORTED even though the handler answers a truthful 4xx; the
// outcome UPDATE would then hit 25P02 and the client would see a 500. Releasing
// the savepoint exposes the abort: ROLLBACK TO SAVEPOINT discards only the
// handler's partial writes, heals the transaction (the key-row claim before
// the savepoint survives) and the 4xx outcome commits like any other. A 2xx
// behind a failed release is a lie (its writes are gone): it rolls back and
// fails the request so the key retries.
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

	// The claim is INSERT ... ON CONFLICT DO NOTHING for a LIVE row (the
	// unique index serialises concurrent requests: the loser waits for the
	// winner's commit, then replays) and DO UPDATE for an EXPIRED one
	// (created_at <= now() - TTL): a key past its window is free again, so
	// the request re-claims the row — fingerprint replaced, answer columns
	// reset, created_at refreshed — and executes (ADR 0024, addendum
	// 2026-11-22). RowsAffected 1 = the key is ours (fresh or refreshed); 0
	// = a live row holds it, the replay path answers.
	tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (idempotency_key, fingerprint, status)
		VALUES ($1, $2, 0)
		ON CONFLICT (idempotency_key) DO UPDATE SET
			fingerprint = EXCLUDED.fingerprint, status = 0,
			content_type = '', location = '', body = ''::bytea, created_at = now()
		WHERE idempotency_keys.created_at <= now() - make_interval(secs => $3)`,
		key, fingerprint, contract.IdempotencyTTL.Seconds())
	if err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("claim idempotency key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.replay(ctx, tx, key, fingerprint)
	}

	if _, err := tx.Exec(ctx, "SAVEPOINT handler_run"); err != nil {
		return contract.StoredResponse{}, false, fmt.Errorf("open handler savepoint: %w", err)
	}
	resp, err := fn(pgtx.With(ctx, tx))
	if err != nil {
		return contract.StoredResponse{}, false, err
	}
	if resp.Status >= 500 || resp.Transient {
		// rolled back by the deferred Rollback: a 5xx is never stored, and a
		// RETRYABLE refusal (or one that declared no class) releases its key
		// exactly the same way — nothing is remembered, a retry re-executes.
		return resp, false, nil
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT handler_run"); err != nil {
		// A statement inside the handler failed on the server: the
		// transaction is aborted. Rolling back to the savepoint discards the
		// handler's partial writes (the right all-or-nothing semantics for
		// an error answer) and returns the transaction to health.
		if _, rerr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT handler_run"); rerr != nil {
			return contract.StoredResponse{}, false, fmt.Errorf("heal aborted transaction: %w", rerr)
		}
		if resp.Status < 400 {
			// A success whose writes are gone must neither be returned nor
			// cached: fail so the same key retries the real work.
			return contract.StoredResponse{}, false, fmt.Errorf("handler answered %d over an aborted transaction: %w", resp.Status, err)
		}
	}
	// A body-less answer (a bare 201 with only a Location header, a 204)
	// carries a nil Body, and pgx would send SQL NULL for it — the column
	// is NOT NULL. An empty byte string is what the row already holds.
	body := resp.Body
	if body == nil {
		body = []byte{}
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status = $2, content_type = $3, location = $4, body = $5
		WHERE idempotency_key = $1`, key, resp.Status, resp.ContentType, resp.Location, body); err != nil {
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
	// The age guard — created_at > now() - TTL — is the exact complement of
	// the claim's refresh predicate and the purge's delete predicate, over
	// the one contract.IdempotencyTTL: a row older than the TTL is invisible
	// here, so it can never answer a replay between expiry and the sweep.
	err := tx.QueryRow(ctx, `SELECT fingerprint, status, content_type, location, body
		FROM idempotency_keys
		WHERE idempotency_key = $1 AND created_at > now() - make_interval(secs => $2)`,
		key, contract.IdempotencyTTL.Seconds()).
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

// PurgeExpired implements ports.IdempotencyStore: it deletes at most limit
// rows older than contract.IdempotencyTTL, oldest first (the
// idempotency_keys_created_at_idx index of migration 0008 — the "sweep by
// age" its comment promised), and returns how many it deleted. It runs on the
// pool only (the background worker's call), never inside a request's unit of
// work. FOR UPDATE SKIP LOCKED leaves a row another transaction is writing —
// a request whose claim just refreshed an expired key — and the age test is
// repeated on the DELETE itself, so a row a request refreshed is never
// deleted. It takes no advisory lock and never waits on a row lock: a row it
// cannot take is skipped, and an expired row is invisible to the replay
// anyway, so deleting it changes nothing a request can observe.
func (s *IdempotencyStore) PurgeExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE idempotency_key IN (
		SELECT idempotency_key FROM idempotency_keys
		WHERE created_at <= now() - make_interval(secs => $1)
		ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED
	) AND created_at <= now() - make_interval(secs => $1)`,
		contract.IdempotencyTTL.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge idempotency keys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
