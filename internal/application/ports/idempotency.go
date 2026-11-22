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
	// committed unless its Status is >= 500 or it carries the Transient mark
	// (a refusal whose replay class is RETRYABLE, or none — ADR 0024), or fn
	// returns an error or panics: then everything rolls back, nothing is
	// stored, and the retry re-executes. Concurrent calls with one key are
	// serialised; the loser replays the winner's answer — or, when the
	// winner's transaction rolled back (a released key), claims the key and
	// runs fn itself.
	//
	// A stored answer lives for contract.IdempotencyTTL (ADR 0024, addendum
	// 2026-11-22): past the window the key is free again and Do executes fn.
	Do(ctx context.Context, key, fingerprint string, fn func(ctx context.Context) (contract.StoredResponse, error)) (resp contract.StoredResponse, replayed bool, err error)

	// PurgeExpired deletes at most limit rows older than
	// contract.IdempotencyTTL, oldest first, and returns how many it deleted
	// (a result below limit means nothing expired is left). It is the
	// background retention worker's call, never a request's: it must never
	// delete a row a request holds nor block on one — a row whose lock it
	// cannot take is skipped.
	PurgeExpired(ctx context.Context, limit int) (deleted int, err error)
}
