//go:build integration

package postgres_integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

// TestIdempotencyStoreAgainstRealPostgres covers fleet STANDARD 2.3 and 2.5 on
// a REAL Postgres (migration 0008): fresh key, byte-identical replay, reuse
// with another fingerprint, a stored 4xx, a never-stored 5xx, and N concurrent
// requests with one key executing exactly once.
func TestIdempotencyStoreAgainstRealPostgres(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	store := postgres.NewIdempotencyStore(pool)

	var runs atomic.Int32
	answer := func(status int, body string) func(context.Context) (contract.StoredResponse, error) {
		return func(context.Context) (contract.StoredResponse, error) {
			runs.Add(1)
			return contract.StoredResponse{Status: status, ContentType: "application/json", Body: []byte(body)}, nil
		}
	}

	first, replayed, err := store.Do(ctx, "k-fresh", "fp-1", answer(200, `{"n":1}`))
	if err != nil || replayed || first.Status != 200 {
		t.Fatalf("fresh key = %+v replayed=%v err=%v", first, replayed, err)
	}
	again, replayed, err := store.Do(ctx, "k-fresh", "fp-1", answer(200, `{"n":2}`))
	if err != nil || !replayed || string(again.Body) != `{"n":1}` || runs.Load() != 1 {
		t.Fatalf("replay = %+v replayed=%v err=%v runs=%d", again, replayed, err, runs.Load())
	}
	if _, _, err := store.Do(ctx, "k-fresh", "fp-other", answer(200, `{}`)); !errors.Is(err, contract.ErrIdempotencyKeyReused) {
		t.Fatalf("same key, other fingerprint: err = %v, want ErrIdempotencyKeyReused", err)
	}

	// A deterministic 4xx is stored and replayed.
	if _, _, err := store.Do(ctx, "k-4xx", "fp", answer(422, `{"e":1}`)); err != nil {
		t.Fatal(err)
	}
	rep, replayed, err := store.Do(ctx, "k-4xx", "fp", answer(200, `{}`))
	if err != nil || !replayed || rep.Status != 422 {
		t.Fatalf("4xx replay = %+v replayed=%v err=%v", rep, replayed, err)
	}

	// A 5xx is never stored: the retry executes again.
	before := runs.Load()
	if _, replayed, err := store.Do(ctx, "k-5xx", "fp", answer(503, `{}`)); err != nil || replayed {
		t.Fatalf("5xx first = replayed=%v err=%v", replayed, err)
	}
	if got, replayed, err := store.Do(ctx, "k-5xx", "fp", answer(200, `{"ok":true}`)); err != nil || replayed || got.Status != 200 || runs.Load() != before+2 {
		t.Fatalf("5xx retry = %+v replayed=%v err=%v runs=%d", got, replayed, err, runs.Load())
	}

	// N concurrent requests with one key: exactly one execution, the rest replay.
	runs.Store(0)
	const n = 12
	var wg sync.WaitGroup
	var replays atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, rp, err := store.Do(ctx, "k-race", "fp", answer(200, `{"race":true}`))
			if err != nil {
				t.Errorf("concurrent Do: %v", err)
				return
			}
			if rp {
				replays.Add(1)
			}
		}()
	}
	wg.Wait()
	if runs.Load() != 1 || replays.Load() != n-1 {
		t.Fatalf("concurrent: executions=%d replays=%d, want 1 and %d", runs.Load(), replays.Load(), n-1)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE idempotency_key = 'k-race'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows for k-race = %d err=%v", rows, err)
	}
}
