package memory

import (
	"context"
	"sync"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

var _ ports.IdempotencyStore = (*IdempotencyStore)(nil)

// IdempotencyStore is the in-memory ports.IdempotencyStore for the
// in-memory mode and tests. Do holds one lock while fn runs, so concurrent
// requests are serialised exactly like the Postgres unique index does.
type IdempotencyStore struct {
	mu      sync.Mutex
	records map[string]memoryRecord
}

type memoryRecord struct {
	fingerprint string
	resp        contract.StoredResponse
}

// NewIdempotencyStore returns an empty store.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{records: map[string]memoryRecord{}}
}

// Do implements ports.IdempotencyStore.
func (s *IdempotencyStore) Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[key]; ok {
		if rec.fingerprint != fingerprint {
			return contract.StoredResponse{}, false, contract.ErrIdempotencyKeyReused
		}
		return rec.resp, true, nil
	}
	resp, err := fn(ctx)
	if err != nil {
		return contract.StoredResponse{}, false, err
	}
	if resp.Status < 500 {
		s.records[key] = memoryRecord{fingerprint: fingerprint, resp: resp}
	}
	return resp, false, nil
}
