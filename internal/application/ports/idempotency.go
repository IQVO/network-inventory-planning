package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

// IdempotencyStore keeps Idempotency-Key records (fleet REST guideline
// section 2). The key ALONE identifies a record (global, not per route); the
// fingerprint (method + path + sha256(body)) decides replay (same) or
// contract.ErrIdempotencyKeyReused (different).
type IdempotencyStore interface {
	// Do runs fn at most once per key. fn executes INSIDE the unit of work
	// the store opens (the ctx it receives carries it), so the key row, the
	// aggregate change and the outbox rows commit together. A key already
	// seen with the same fingerprint returns the stored answer with
	// replayed true and does not call fn. fn's answer is stored and the unit
	// committed unless its Status is >= 500, or fn returns an error or
	// panics: then everything rolls back, nothing is stored, and the retry
	// re-executes. Concurrent calls with one key are serialised; the loser
	// replays the winner's answer.
	Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (resp contract.StoredResponse, replayed bool, err error)
}
