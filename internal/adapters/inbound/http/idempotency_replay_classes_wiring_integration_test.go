//go:build integration

// Rendered from warehouse-harness-template
// templates/idempotency/replay_classes_wiring_test.go.tmpl (docs/
// idempotency-replay-classes.md): this repo's half of the replay-class
// tests. The key row and the handler's writes live in ONE Postgres
// transaction (the claim, the savepoint and the stored answer of
// postgres.IdempotencyStore), so the wiring is the real thing: the
// production store over a testcontainers pool (internal/testing/pgtest,
// which applies the repo's own migrations), a write through the transaction
// the middleware bound into the request context (pgtx), counts from the
// pool. The middleware comes from IdempotentForTest (the same closure
// Routes wraps every keyed route in); refusals go through writeProblem via
// ReplayEntry.Refuse, so the production class marking runs. ADR 0024.
package http_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpadapter "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/network-inventory-planning/internal/testing/pgtest"
)

// newReplayOps returns the six operations over ONE clean database per call
// (pgtest clones the migrated template, so every adapter starts empty).
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	pool := pgtest.NewPool(t)
	store := postgres.NewIdempotencyStore(pool)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE replay_probe (tag text NOT NULL PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// The production catalogue plus the synthetic OUTCOME probe (no real
	// problem type of this service commits state and then refuses; ADR 0024).
	catalogue := func() []problemEntry {
		rows := httpadapter.ReplayCatalogue()
		out := make([]problemEntry, 0, len(rows)+1)
		for _, r := range rows {
			out = append(out, problemEntry{Slug: r.Slug, Status: r.Status, Class: r.Class})
		}
		synthetic := httpadapter.SyntheticOutcomeEntry()
		out = append(out, problemEntry{Slug: synthetic.Slug, Status: synthetic.Status, Class: synthetic.Class})
		return out
	}
	entries := map[string]httpadapter.ReplayEntry{}
	for _, r := range httpadapter.ReplayCatalogue() {
		entries[r.Slug] = r
	}
	entries[httpadapter.SyntheticOutcomeEntry().Slug] = httpadapter.SyntheticOutcomeEntry()
	return replayOps{
		Catalogue:  catalogue,
		Middleware: func(_ *testing.T, next http.Handler) http.Handler { return httpadapter.IdempotentForTest(store, next) },
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			entry, ok := entries[e.Slug]
			if !ok {
				http.Error(w, "no catalogue entry for "+e.Slug, http.StatusInternalServerError)
				return
			}
			entry.Refuse(w, r)
		},
		Write: func(ctx context.Context, tag string) error {
			tx, ok := pgtx.From(ctx)
			if !ok {
				return context.Canceled // not inside the middleware's transaction
			}
			_, err := tx.Exec(ctx, `INSERT INTO replay_probe (tag) VALUES ($1)`, tag)
			return err
		},
		Committed: func(t *testing.T, tag string) int {
			t.Helper()
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			t.Helper()
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE idempotency_key = $1`, key)
		},
	}
}

// An error answer whose problem type declares no class (a handler that wrote
// a bare 4xx by hand) is not remembered: the fail-safe direction is to run
// the work again.
func TestAnUnclassifiedErrorAnswerIsNotStored(t *testing.T) {
	a := newReplayAdapter(t)
	bare := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
	}
	h := &fakeHandler{script: []answerFn{bare, succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	if first := post(t, mw, "k-bare", `{"n":1}`); first.Code != http.StatusConflict {
		t.Fatalf("first = %d, want the 409", first.Code)
	}
	if got := a.KeyRows(t, "k-bare"); got != 0 {
		t.Fatalf("key rows after an unclassified 409 = %d, want 0", got)
	}
	if retry := post(t, mw, "k-bare", `{"n":1}`); retry.Code != http.StatusCreated || h.calls != 2 || replayed(retry) {
		t.Fatalf("retry = %d replayed=%v calls=%d, want a fresh 201 after 2 calls", retry.Code, replayed(retry), h.calls)
	}
}

// A copy of a request that waits on the key while the first ends RETRYABLE
// blocks on the unique index until the first transaction rolls back; the row
// never committed, so the copy claims the key and runs the handler itself,
// and the copies behind it replay that run's answer.
func TestACopyWaitingOnARetryableFirstRunsItself(t *testing.T) {
	a := newReplayAdapter(t)
	var calls, active, peak atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		cur := active.Add(1)
		for p := peak.Load(); cur > p && !peak.CompareAndSwap(p, cur); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond) // let the copies queue behind the key
		active.Add(-1)
		if n == 1 {
			// The first copy is refused RETRYABLE through the production
			// writer (facts-incomplete: the real event-fed refusal).
			e := firstOf(t, a, classRetryable)
			a.Refuse(w, r, e)
			return
		}
		succeeding(a, "ok")(w, r)
	})
	mw := a.Middleware(t, h)

	const copies = 12
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		fresh   = map[int]int{}
		replays int
	)
	for range copies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := post(t, mw, "k-wait", `{"n":1}`)
			mu.Lock()
			defer mu.Unlock()
			if replayed(rec) {
				replays++
				if rec.Code != http.StatusCreated {
					t.Errorf("a replay answered %d: only the stored success may be replayed", rec.Code)
				}
				return
			}
			fresh[rec.Code]++
		}()
	}
	wg.Wait()
	if calls.Load() != 2 || peak.Load() != 1 {
		t.Fatalf("handler ran %d times with %d at once, want 2 and 1", calls.Load(), peak.Load())
	}
	if fresh[http.StatusUnprocessableEntity] != 1 || fresh[http.StatusCreated] != 1 || replays != copies-2 {
		t.Fatalf("fresh answers %v and %d replays, want one RETRYABLE refusal, one 201 and %d replays", fresh, replays, copies-2)
	}
	if got := a.KeyRows(t, "k-wait"); got != 1 {
		t.Fatalf("key rows = %d, want 1: the success is stored", got)
	}
}
