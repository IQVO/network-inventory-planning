package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

var _ ports.IdempotencyStore = (*IdempotencyStore)(nil)

// IdempotencyStore is the in-memory ports.IdempotencyStore for the
// in-memory mode and tests. Do holds one lock while fn runs, so concurrent
// requests are serialised exactly like the Postgres unique index does.
//
// The retention matches the Postgres store (ADR 0024, addendum 2026-11-22):
// a stored answer lives for contract.IdempotencyTTL — a record past its
// window never replays (the key is free again, the request executes and its
// new answer replaces the stale one), and PurgeExpired drops the dead
// records.
type IdempotencyStore struct {
	mu      sync.Mutex
	records map[string]memoryRecord
	// now is the clock the store reads; tests inject a moving one to age
	// stored answers without sleeping.
	now func() time.Time
}

type memoryRecord struct {
	fingerprint string
	resp        contract.StoredResponse
	createdAt   time.Time
}

// NewIdempotencyStore returns an empty store reading the wall clock.
func NewIdempotencyStore() *IdempotencyStore {
	return NewIdempotencyStoreClock(time.Now)
}

// NewIdempotencyStoreClock returns an empty store reading clock.
func NewIdempotencyStoreClock(now func() time.Time) *IdempotencyStore {
	return &IdempotencyStore{records: map[string]memoryRecord{}, now: now}
}

// Do implements ports.IdempotencyStore.
func (s *IdempotencyStore) Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[key]; ok && s.now().Sub(rec.createdAt) <= contract.IdempotencyTTL {
		if rec.fingerprint != fingerprint {
			return contract.StoredResponse{}, false, contract.ErrIdempotencyKeyReused
		}
		return rec.resp, true, nil
	}
	resp, err := fn(ctx)
	if err != nil {
		return contract.StoredResponse{}, false, err
	}
	// The same rule as the Postgres store: a 5xx is never kept, and a
	// RETRYABLE refusal (ADR 0024's Transient mark) releases the key. An
	// expired record stays where it is until the purge — invisible to every
	// read, exactly like the rolled-back refresh leaves the dead Postgres
	// row for the sweep.
	if resp.Status < 500 && !resp.Transient {
		s.records[key] = memoryRecord{fingerprint: fingerprint, resp: resp, createdAt: s.now()}
	}
	return resp, false, nil
}

// PurgeExpired implements ports.IdempotencyStore: it deletes at most limit
// records older than contract.IdempotencyTTL and returns how many it
// deleted. The mutex that serialises Do serialises the purge too — the
// in-memory analogue of FOR UPDATE SKIP LOCKED: a purge can never delete a
// record a request holds, because the request holds the lock while it runs.
func (s *IdempotencyStore) PurgeExpired(_ context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for key, rec := range s.records {
		if deleted >= limit {
			break
		}
		if s.now().Sub(rec.createdAt) > contract.IdempotencyTTL {
			delete(s.records, key)
			deleted++
		}
	}
	return deleted, nil
}
