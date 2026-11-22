package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

// movingClock is a test clock: advance moves time, the store ages with it.
type movingClock struct{ t time.Time }

func (c *movingClock) Now() time.Time          { return c.t }
func (c *movingClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func answer(status int, body string) func(context.Context) (contract.StoredResponse, error) {
	return func(context.Context) (contract.StoredResponse, error) {
		return contract.StoredResponse{Status: status, ContentType: "application/json", Body: []byte(body)}, nil
	}
}

// The memory double carries the same retention as the Postgres store (ADR
// 0024, addendum 2026-11-22): a stored answer replays inside
// contract.IdempotencyTTL; past the window the key is free again — the same
// request executes, its new answer replaces the stale one, and a different
// fingerprint under the freed key is a NEW request, not a reuse.
func TestAnExpiredAnswerReleasesTheKey(t *testing.T) {
	clock := &movingClock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	store := NewIdempotencyStoreClock(clock.Now)
	ctx := context.Background()
	calls := 0
	run := func(body string) func(context.Context) (contract.StoredResponse, error) {
		return func(context.Context) (contract.StoredResponse, error) {
			calls++
			return contract.StoredResponse{Status: 200, ContentType: "application/json", Body: []byte(body)}, nil
		}
	}

	if resp, replayed, err := store.Do(ctx, "k", "fp-1", run(`{"n":1}`)); err != nil || replayed || resp.Status != 200 {
		t.Fatalf("first = %+v replayed=%v err=%v, want a fresh 200", resp, replayed, err)
	}
	// A microsecond short of the window: the stored answer still replays.
	clock.advance(contract.IdempotencyTTL - time.Microsecond)
	if resp, replayed, err := store.Do(ctx, "k", "fp-1", run(`{"n":9}`)); err != nil || !replayed || string(resp.Body) != `{"n":1}` {
		t.Fatalf("a microsecond short = %+v replayed=%v err=%v, want the stored answer", resp, replayed, err)
	}
	// Past the window: the key is free. The same fingerprint executes again.
	clock.advance(2 * time.Microsecond)
	if resp, replayed, err := store.Do(ctx, "k", "fp-1", run(`{"n":2}`)); err != nil || replayed || string(resp.Body) != `{"n":2}` || calls != 2 {
		t.Fatalf("past the TTL = %+v replayed=%v err=%v calls=%d, want a fresh execution", resp, replayed, err, calls)
	}
	// The refreshed answer is live: an exact repeat replays it, and a
	// different fingerprint inside ITS window is a reuse again.
	if _, replayed, err := store.Do(ctx, "k", "fp-1", run(`{}`)); err != nil || !replayed {
		t.Fatalf("repeat of the refreshed answer: replayed=%v err=%v, want a replay", replayed, err)
	}
	if _, _, err := store.Do(ctx, "k", "fp-other", run(`{}`)); !errors.Is(err, contract.ErrIdempotencyKeyReused) {
		t.Fatalf("another body inside the window: err = %v, want ErrIdempotencyKeyReused", err)
	}
	// Once IT expires too, the other fingerprint is a new request.
	clock.advance(contract.IdempotencyTTL + time.Microsecond)
	if _, replayed, err := store.Do(ctx, "k", "fp-other", run(`{}`)); err != nil || replayed || calls != 3 {
		t.Fatalf("another body after expiry: replayed=%v err=%v calls=%d, want a fresh execution, not a 409", replayed, err, calls)
	}
}

// The refresh rule only ever replaces a DEAD record: a 5xx or a RETRYABLE
// (Transient) answer under an expired key stores nothing, and the stale
// record stays for the purge — never replayed, exactly like the rolled-back
// Postgres refresh leaves the dead row for the sweep.
func TestAnUnstoredAnswerDoesNotRefreshAnExpiredRecord(t *testing.T) {
	clock := &movingClock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	store := NewIdempotencyStoreClock(clock.Now)
	ctx := context.Background()
	if _, _, err := store.Do(ctx, "k", "fp", answer(200, `{}`)); err != nil {
		t.Fatal(err)
	}
	clock.advance(contract.IdempotencyTTL + time.Minute)
	if _, replayed, err := store.Do(ctx, "k", "fp", func(context.Context) (contract.StoredResponse, error) {
		return contract.StoredResponse{Status: 503, Transient: true}, nil
	}); err != nil || replayed {
		t.Fatalf("expired key, transient answer: replayed=%v err=%v, want answered but not stored", replayed, err)
	}
	if n, err := store.PurgeExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v, want the stale record gone", n, err)
	}
}

// The purge deletes only expired records, at most limit per call.
func TestPurgeDeletesExpiredRecordsAndKeepsLiveOnes(t *testing.T) {
	clock := &movingClock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	store := NewIdempotencyStoreClock(clock.Now)
	ctx := context.Background()
	for _, key := range []string{"old-1", "old-2", "old-3"} {
		if _, _, err := store.Do(ctx, key, "fp", answer(200, `{}`)); err != nil {
			t.Fatal(err)
		}
	}
	clock.advance(contract.IdempotencyTTL + time.Minute)
	if _, _, err := store.Do(ctx, "fresh", "fp", answer(200, `{}`)); err != nil {
		t.Fatal(err)
	}

	if n, err := store.PurgeExpired(ctx, 2); err != nil || n != 2 {
		t.Fatalf("first batch = %d, %v, want the bound of 2", n, err)
	}
	if n, err := store.PurgeExpired(ctx, 2); err != nil || n != 1 {
		t.Fatalf("second batch = %d, %v, want the last expired record", n, err)
	}
	if n, err := store.PurgeExpired(ctx, 2); err != nil || n != 0 {
		t.Fatalf("third batch = %d, %v, want 0: only live records remain", n, err)
	}
	if _, replayed, err := store.Do(ctx, "fresh", "fp", answer(500, `{}`)); err != nil || !replayed {
		t.Fatalf("fresh after the purge: replayed=%v err=%v, want the live answer replayed", replayed, err)
	}
}

// A non-positive limit is a no-op, never an error.
func TestPurgeWithNoBudgetDeletesNothing(t *testing.T) {
	clock := &movingClock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	store := NewIdempotencyStoreClock(clock.Now)
	if _, _, err := store.Do(context.Background(), "k", "fp", answer(200, `{}`)); err != nil {
		t.Fatal(err)
	}
	clock.advance(2 * contract.IdempotencyTTL)
	if n, err := store.PurgeExpired(context.Background(), 0); err != nil || n != 0 {
		t.Fatalf("limit 0 = %d, %v, want a no-op", n, err)
	}
}
