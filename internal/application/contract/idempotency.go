// Package contract holds the small value types shared by the application
// ports and the adapters that implement them (the ports package itself
// declares interfaces only).
package contract

import "errors"

// ErrIdempotencyKeyReused is what an IdempotencyStore answers when a key is
// seen again with a different request fingerprint (method + path + body
// hash). The HTTP adapter maps it to 409 idempotency-key-reused.
var ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")

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
