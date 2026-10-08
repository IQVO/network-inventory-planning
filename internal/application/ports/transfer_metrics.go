package ports

import "context"

// Bounded outcome vocabularies of the Tier-2 business counters (ADR 0011).
// They are constants here, not free-form strings at call sites, so the
// label cardinality of each counter is fixed at compile time.
const (
	// ApprovalApproved: a NEW transfer was created and its allocation
	// command written to the outbox.
	ApprovalApproved = "approved"
	// ApprovalReplayed: the Idempotency-Key already existed with the same
	// payload; the first transfer was returned, nothing new was created.
	ApprovalReplayed = "replayed"
	// ApprovalRefused: the approval ended in an error of any class
	// (invalid input, stale/incomplete facts, expired proposal,
	// idempotency conflict, work-release not configured, or a
	// dependency failure) -- the endpoint answered 4xx/5xx.
	ApprovalRefused = "refused"

	// RelayPublished: the outbox relay's sink delivered the row to Kafka.
	RelayPublished = "published"
	// RelayFailed: the sink returned an error for the row (it is retried
	// on the next pass).
	RelayFailed = "failed"
)

// TransferMetrics is the Tier-2 business-metrics port of the transfer saga
// (fleet standard-metrics convention). Its adapter lives in
// internal/adapters/outbound/telemetry. Every method is cheap, never
// returns an error and never blocks; a nil TransferMetrics is a valid
// "not instrumented" value at every call site (callers guard with
// `if m != nil`, and the telemetry adapter's methods are also nil-receiver
// safe).
//
// No method takes an identifier: transfer ids, SKUs and site ids are
// deliberately NOT metric labels.
type TransferMetrics interface {
	// TransferApproved counts one POST /v1/transfers:approve outcome:
	// one of ApprovalApproved, ApprovalReplayed, ApprovalRefused.
	TransferApproved(ctx context.Context, outcome string)
	// TransferStateAdvanced counts one saga transition INTO state `to`
	// (the bounded TransferState enum, e.g. "ALLOCATED").
	TransferStateAdvanced(ctx context.Context, to string)
	// OutboxRelayed counts one outbox row the relay tried to send: one of
	// RelayPublished, RelayFailed.
	OutboxRelayed(ctx context.Context, outcome string)
}
