// Package report is the self-contained read-model region of the analytics
// read side (ADR 0009): the shapes of the four saga-health reports, the
// parameter rules (date range, limit), the pure shaping and percentile logic,
// and the writer/reader ports the analytical store implements. It imports no
// other internal package, so the OLTP domain can never leak into the
// projection and the arch test can keep this region isolated.
package report

import (
	"context"
	"errors"
	"time"
)

// Kind is which of the three published saga-health events a fact came from.
type Kind string

// The three saga-health events the analytics topic carries; the Kind names
// are the CloudEvents event names (the last segment of `type`).
const (
	KindStateAdvanced         Kind = "TransferStateAdvanced"
	KindStuckDetected         Kind = "TransferStuckDetected"
	KindRebalanceRunCompleted Kind = "RebalanceRunCompleted"
)

// Event is one decoded, validated analytics event ready to project. It is
// the union of the three payloads; only the fields of its Kind are set.
type Event struct {
	Kind    Kind
	EventID string    // CloudEvents id: the idempotency key
	At      time.Time // CloudEvents time: when the event occurred

	// TransferStateAdvanced and TransferStuckDetected.
	TransferID string
	AgeSeconds int64

	// TransferStateAdvanced: From is "" for the creation entry.
	From string
	To   string

	// TransferStuckDetected.
	State            string
	ThresholdSeconds int64

	// RebalanceRunCompleted.
	RunID         string
	ProposalCount int
	RejectedCount int
	StaleFacts    int
}

// Projection is the WRITER port: Apply records the event id and appends the
// event's fact in ONE transaction. applied is false when the id was already
// recorded (a replay): nothing changed. An error wrapping ErrRejected is
// deterministic (the store can never accept this event: retry is pointless);
// any other error is transient and the same event may be retried.
type Projection interface {
	Apply(ctx context.Context, e Event) (applied bool, err error)
}

// ErrRejected marks an event the analytical store deterministically refuses
// (a data-exception or integrity-violation class error).
var ErrRejected = errors.New("report: event rejected by the analytical store")

// Reader is the READER port. Every method answers for the half-open range
// [From, To): From inclusive, To exclusive. Results are never nil.
type Reader interface {
	TransferFunnel(ctx context.Context, r Range) ([]FunnelDay, error)
	StateDwell(ctx context.Context, r Range) ([]DwellDay, error)
	StuckDays(ctx context.Context, r Range) ([]StuckDay, error)
	// StuckLatest is the newest `limit` stuck detections in the range.
	StuckLatest(ctx context.Context, r Range, limit int) ([]StuckOccurrence, error)
	RebalanceDays(ctx context.Context, r Range) ([]RebalanceDay, error)
	// LastEventAt is the CloudEvents time of the newest event the projection
	// has applied (nil while it has applied none): the basis of the freshness
	// lag.
	LastEventAt(ctx context.Context) (*time.Time, error)
}

// FunnelDay is how many DISTINCT transfers reached State on Day (UTC day of
// the transition), from TransferStateAdvanced `to`.
type FunnelDay struct {
	Day       time.Time
	State     string
	Transfers int
}

// DwellDay is the age_seconds distribution of the transitions that LEFT State
// on Day (TransferStateAdvanced `from`; the creation entry, whose `from` is
// empty, leaves no state and is not counted). age_seconds is the saga's age
// when it left the state, a monotone proxy for how long it took to get that
// far, not a per-state duration.
type DwellDay struct {
	Day         time.Time
	State       string
	Transitions int
	P50Seconds  float64
	P95Seconds  float64
}

// StuckDay is the TransferStuckDetected occurrences for State on Day. The
// ticker re-detects a transfer on every pass while it stays stuck, so
// Detections counts occurrences and Transfers the distinct transfers.
type StuckDay struct {
	Day        time.Time
	State      string
	Detections int
	Transfers  int
}

// StuckOccurrence is one TransferStuckDetected, for the latest-occurrences
// list.
type StuckOccurrence struct {
	TransferID       string
	State            string
	AgeSeconds       int64
	ThresholdSeconds int64
	DetectedAt       time.Time
}

// RebalanceDay sums the RebalanceRunCompleted events of Day: Runs passes that
// produced Proposals in total, of which Rejected were refused by the run's own
// safety checks, and StaleFacts stale facts seen.
type RebalanceDay struct {
	Day        time.Time
	Runs       int
	Proposals  int
	Rejected   int
	StaleFacts int
}
