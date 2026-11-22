// Package contract holds the small value types shared by the application
// ports and the adapters that implement them (the ports package itself
// declares interfaces only).
package contract

import (
	"errors"
	"time"
)

// ErrIdempotencyKeyReused is what an IdempotencyStore answers when a key is
// seen again with a different request fingerprint (method + path + body
// hash). The HTTP adapter maps it to 409 idempotency-key-reused.
var ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")

// IdempotencyTTL is how long a stored answer is replayed: the fleet default
// of 24 hours (ADR 0024, addendum 2026-11-22). Until the addendum a stored
// answer replayed forever — nothing deleted an idempotency_keys row. The TTL
// is the ONE age the whole retention shares — the claim's refresh predicate
// (a key past its window is free again, so the new request re-claims the
// row), the replay's age guard (a row older than the TTL never answers) and
// the purge's delete predicate — so the read check, the claim and the sweep
// can never disagree about which rows are live.
const IdempotencyTTL = 24 * time.Hour

// StoredResponse is the deterministic answer kept for an Idempotency-Key and
// replayed byte for byte on a retry.
type StoredResponse struct {
	Status      int
	ContentType string
	Location    string
	Body        []byte
	// Transient marks an answer the store must NOT keep (ADR 0024): a
	// refusal whose problem type declared the replay class RETRYABLE — or
	// declared no class at all — is answered to the client, but the store
	// rolls its transaction back exactly like a 5xx, releases the key and a
	// retry executes again. The middleware sets it from the class the
	// handler's problem marked; a 2xx never carries it.
	Transient bool
}
