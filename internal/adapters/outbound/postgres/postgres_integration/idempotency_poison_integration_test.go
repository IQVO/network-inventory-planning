//go:build integration

package postgres_integration

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

func poisonPool(t *testing.T) (context.Context, *pgxpool.Pool, *postgres.IdempotencyStore) {
	t.Helper()
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool, postgres.NewIdempotencyStore(pool)
}

// poisonTx simulates a mapped domain conflict: a statement fails on the
// server (a unique violation, like a lost race on a unique index), the
// handler would translate it to a typed error and answer 4xx, but the shared
// transaction is ABORTED from then on.
func poisonTx(t *testing.T, ctx context.Context) {
	t.Helper()
	tx, ok := pgtx.From(ctx)
	if !ok {
		t.Fatal("the store did not bind a transaction into the handler context")
	}
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (idempotency_key, fingerprint, status) VALUES ('dup', 'fp', 0)`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Fatal("the duplicate insert did not abort the transaction")
}

// A 409 produced over an aborted transaction is still recorded, committed and
// replayed; the handler never re-runs.
func TestIdempotencyA409FromAnAbortedTransactionCommitsTheOutcome(t *testing.T) {
	ctx, pool, store := poisonPool(t)
	var runs atomic.Int32
	conflict := func(c context.Context) (contract.StoredResponse, error) {
		runs.Add(1)
		poisonTx(t, c)
		return contract.StoredResponse{Status: http.StatusConflict, ContentType: "application/problem+json", Body: []byte(`{"status":409}`)}, nil
	}
	first, replayed, err := store.Do(ctx, "k-poison", "fp", conflict)
	if err != nil || replayed || first.Status != http.StatusConflict {
		t.Fatalf("first = %+v replayed=%v err=%v, want the truthful 409", first, replayed, err)
	}
	var status int
	if err := pool.QueryRow(ctx, `SELECT status FROM idempotency_keys WHERE idempotency_key = 'k-poison'`).Scan(&status); err != nil || status != http.StatusConflict {
		t.Fatalf("cached status = %d err=%v", status, err)
	}
	var dup int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE idempotency_key = 'dup'`).Scan(&dup); err != nil || dup != 0 {
		t.Fatalf("handler's partial writes survived: %d rows, err=%v", dup, err)
	}
	replay, replayed, err := store.Do(ctx, "k-poison", "fp", conflict)
	if err != nil || !replayed || replay.Status != http.StatusConflict || string(replay.Body) != `{"status":409}` {
		t.Fatalf("replay = %+v replayed=%v err=%v", replay, replayed, err)
	}
	if runs.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", runs.Load())
	}
}

// A 2xx over an aborted transaction is a lie: never returned, never cached;
// the same key retries the real work.
func TestIdempotencyA2xxFromAnAbortedTransactionIsNeverCached(t *testing.T) {
	ctx, pool, store := poisonPool(t)
	var calls atomic.Int32
	created := func(c context.Context) (contract.StoredResponse, error) {
		if calls.Add(1) == 1 {
			poisonTx(t, c)
		}
		return contract.StoredResponse{Status: http.StatusCreated, Body: []byte(`{"id":"p1"}`)}, nil
	}
	if _, _, err := store.Do(ctx, "k-lie", "fp", created); err == nil {
		t.Fatal("first attempt succeeded; want a failure (the writes never happened)")
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE idempotency_key = 'k-lie'`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("a failed success left %d rows (err=%v): the key could not retry", rows, err)
	}
	got, replayed, err := store.Do(ctx, "k-lie", "fp", created)
	if err != nil || replayed || got.Status != http.StatusCreated || calls.Load() != 2 {
		t.Fatalf("retry = %+v replayed=%v err=%v calls=%d", got, replayed, err, calls.Load())
	}
}
