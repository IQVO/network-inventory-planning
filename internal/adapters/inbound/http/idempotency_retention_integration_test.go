//go:build integration

// ADR 0024, addendum (retention): until the TTL this store replayed a stored
// answer FOREVER — nothing ever deleted an idempotency_keys row (migration
// 0008's "sweep by age" was unimplemented). These proofs run the retention
// over a REAL Postgres next to the REAL middleware and the REAL store:
//
//   - a stored answer older than contract.IdempotencyTTL (24h, the fleet
//     default) never replays: the key is free again, the same request
//     EXECUTES, and a different body under it is a NEW request (not 409);
//   - the purge deletes only expired rows, at most one bounded batch per
//     call, and never a row a request holds (FOR UPDATE SKIP LOCKED: a row
//     it cannot take is skipped, never waited for);
//   - a purge racing requests on an expired key changes nothing the requests
//     can observe.
//
// Docker-only (pgtest starts a container); CI runs them under the
// integration tag.
package http_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpadapter "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/testing/pgtest"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAnExpiredAnswerExecutesAgainOverPostgres pins the age guard end to end:
// the middleware over the real store, a real stored 200, then the row ages
// past the TTL and the SAME key with the SAME body executes again — the
// stored answer never answers, and a different body under the freed key is a
// new request, not a 409.
func TestAnExpiredAnswerExecutesAgainOverPostgres(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	var calls atomic.Int32
	h := &fakeHandler{script: []answerFn{func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"n":1}`)
	}}}
	mw := httpadapter.IdempotentForTest(store, h)

	if first := post(t, mw, "k-ttl", `{"n":1}`); first.Code != http.StatusOK || replayed(first) || calls.Load() != 1 {
		t.Fatalf("first = %d replayed=%v calls=%d, want a fresh 200", first.Code, replayed(first), calls.Load())
	}
	if again := post(t, mw, "k-ttl", `{"n":1}`); again.Code != http.StatusOK || !replayed(again) || calls.Load() != 1 {
		t.Fatalf("repeat inside the window = %d replayed=%v calls=%d, want the stored 200 replayed", again.Code, replayed(again), calls.Load())
	}

	// A comfortable margin short of the window the answer still replays (a
	// real margin, not a microsecond: the row keeps ageing while the
	// request travels).
	ageKeyRow(t, pool, "k-ttl", contract.IdempotencyTTL-10*time.Minute)
	if near := post(t, mw, "k-ttl", `{"n":1}`); near.Code != http.StatusOK || !replayed(near) || calls.Load() != 1 {
		t.Fatalf("ten minutes short of the TTL = %d replayed=%v calls=%d, want the stored 200 replayed", near.Code, replayed(near), calls.Load())
	}

	// Past the window the key is free: the same request EXECUTES again...
	ageKeyRow(t, pool, "k-ttl", contract.IdempotencyTTL+10*time.Minute)
	if fresh := post(t, mw, "k-ttl", `{"n":1}`); fresh.Code != http.StatusOK || replayed(fresh) || calls.Load() != 2 {
		t.Fatalf("past the TTL = %d replayed=%v calls=%d, want a FRESH execution", fresh.Code, replayed(fresh), calls.Load())
	}
	// ...the refreshed answer is live and replays...
	if repeat := post(t, mw, "k-ttl", `{"n":1}`); repeat.Code != http.StatusOK || !replayed(repeat) || calls.Load() != 2 {
		t.Fatalf("repeat of the refreshed answer = %d replayed=%v calls=%d, want the stored 200 replayed", repeat.Code, replayed(repeat), calls.Load())
	}
	// ...and once IT expires too, a different body under the key is a NEW
	// request, not a 409 idempotency-key-reused.
	ageKeyRow(t, pool, "k-ttl", contract.IdempotencyTTL+time.Hour)
	other := post(t, mw, "k-ttl", `{"n":2}`)
	if other.Code != http.StatusOK || replayed(other) || calls.Load() != 3 {
		t.Fatalf("another body after expiry = %d replayed=%v calls=%d, want a fresh execution, not a 409", other.Code, replayed(other), calls.Load())
	}
}

// TestThePurgeDeletesExpiredRowsAndKeepsLiveOnes runs the bounded purge over
// seeded rows of every age: only the expired ones go, a second pass finds
// nothing, and the live ones are never touched.
func TestThePurgeDeletesExpiredRowsAndKeepsLiveOnes(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	ctx := context.Background()
	seedKeyRow(t, pool, "expired-a", contract.IdempotencyTTL+time.Hour)
	seedKeyRow(t, pool, "expired-b", 48*time.Hour)
	seedKeyRow(t, pool, "live-a", time.Hour)
	seedKeyRow(t, pool, "live-b", contract.IdempotencyTTL-time.Hour)

	n, err := store.PurgeExpired(ctx, 100)
	if err != nil || n != 2 {
		t.Fatalf("purge = %d, %v, want the 2 expired rows deleted", n, err)
	}
	if got := liveKeys(t, pool); !slices.Equal(got, []string{"live-a", "live-b"}) {
		t.Fatalf("rows left = %v, want only the live ones", got)
	}
	if n, err := store.PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second purge = %d, %v, want 0: nothing live is ever deleted", n, err)
	}
}

// TestThePurgeDeletesAtMostOneBatchPerCall proves the bound: a purge call
// deletes at most limit rows, however far behind it is; the worker's pass
// loops until a batch comes back short.
func TestThePurgeDeletesAtMostOneBatchPerCall(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	ctx := context.Background()
	for i := range 5 {
		seedKeyRow(t, pool, fmt.Sprintf("old-%d", i), contract.IdempotencyTTL+time.Hour)
	}
	seedKeyRow(t, pool, "live", time.Minute)

	if n, err := store.PurgeExpired(ctx, 2); err != nil || n != 2 {
		t.Fatalf("first batch = %d, %v, want 2", n, err)
	}
	if left := liveKeys(t, pool); len(left) != 4 {
		t.Fatalf("rows after one batch of 2 = %v, want 4 (3 expired + the live one)", left)
	}
	total := 2
	for {
		n, err := store.PurgeExpired(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if got := liveKeys(t, pool); total != 5 || !slices.Equal(got, []string{"live"}) {
		t.Fatalf("purged %d rows, left %v, want 5 and only the live key", total, got)
	}
}

// TestThePurgeNeverDeletesARowARequestHoldsAndNeverWaitsForIt drives the
// REAL store: a request claims an expired key and stays in flight inside its
// open transaction (its handler parks), exactly like a slow handler. The
// purge must skip the held row (not wait for it, not delete it), and after
// the request commits the refreshed row is live and survives the purge.
func TestThePurgeNeverDeletesARowARequestHoldsAndNeverWaitsForIt(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	ctx := context.Background()
	seedKeyRow(t, pool, "held", contract.IdempotencyTTL+time.Hour)

	claimed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan contract.StoredResponse, 1)
	go func() {
		resp, _, err := store.Do(ctx, "held", "fp", func(context.Context) (contract.StoredResponse, error) {
			close(claimed)
			<-release
			return contract.StoredResponse{Status: http.StatusOK, ContentType: "application/json", Body: []byte(`{"held":true}`)}, nil
		})
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-claimed

	purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	started := time.Now()
	if n, err := store.PurgeExpired(purgeCtx, 100); err != nil || n != 0 {
		t.Fatalf("purge while the request holds the row = %d, %v, want 0 and no wait", n, err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("purge took %s: it must SKIP a locked row, never wait for it", elapsed)
	}

	close(release)
	resp := <-done
	if resp.Status != http.StatusOK {
		t.Fatalf("the in-flight request = %d, want the 200 it earned", resp.Status)
	}
	if n, err := store.PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("purge after the request committed = %d, %v: the row was refreshed at claim, it is live", n, err)
	}
	if got := liveKeys(t, pool); !slices.Equal(got, []string{"held"}) {
		t.Fatalf("rows = %v, want the refreshed key", got)
	}
}

// TestThePurgeRacingRequestsOnAnExpiredKeyKeepsThemCorrect runs the real
// middleware: for each expired key, three concurrent duplicates race a purge.
// Every request must answer 200 (one fresh execution, the others replay the
// winner — never the expired row), leaving exactly one live row per key.
func TestThePurgeRacingRequestsOnAnExpiredKeyKeepsThemCorrect(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	ctx := context.Background()
	var calls atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	mw := httpadapter.IdempotentForTest(store, h)

	const rounds = 10
	for i := range rounds {
		key := fmt.Sprintf("race-%d", i)
		seedKeyRow(t, pool, key, contract.IdempotencyTTL+time.Hour)
		var wg sync.WaitGroup
		codes := make([]int, 3)
		bodies := make([]string, 3)
		for j := range codes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rec := post(t, mw, key, `{"n":1}`)
				codes[j] = rec.Code
				bodies[j] = rec.Body.String()
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = store.PurgeExpired(ctx, 100) }()
		wg.Wait()
		for j, c := range codes {
			if c != http.StatusOK {
				t.Fatalf("round %d: a request racing the purge answered %d %s, want 200", i, c, bodies[j])
			}
		}
	}
	if calls.Load() != rounds {
		t.Fatalf("the handler ran %d times for %d keys: an expired row is a fresh request, once; its duplicates replay", calls.Load(), rounds)
	}
	if got := len(liveKeys(t, pool)); got != rounds {
		t.Fatalf("%d key rows left, want exactly one live row per key (%d)", got, rounds)
	}
	if again := post(t, mw, "race-0", `{"n":1}`); !replayed(again) {
		t.Fatalf("a repeat of a stored key must replay: %v", again.Header())
	}
}

// seedKeyRow stores a key row created age ago (fingerprint fp, a 200 answer).
func seedKeyRow(t *testing.T, pool *pgxpool.Pool, key string, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO idempotency_keys (idempotency_key, fingerprint, status, body, created_at)
			VALUES ($1, 'fp', 200, '\x7b7d', now() - make_interval(secs => $2))`, key, age.Seconds()); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// ageKeyRow moves one key row's created_at back by age, exactly as time would.
func ageKeyRow(t *testing.T, pool *pgxpool.Pool, key string, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE idempotency_keys SET created_at = now() - make_interval(secs => $2) WHERE idempotency_key = $1`,
		key, age.Seconds()); err != nil {
		t.Fatalf("age %s: %v", key, err)
	}
}

// liveKeys returns the keys still present, ordered.
func liveKeys(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT idempotency_key FROM idempotency_keys ORDER BY idempotency_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
