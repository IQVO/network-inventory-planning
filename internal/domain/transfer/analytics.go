package transfer

import "time"

// StateAdvanced is the analytics occurrence of one saga state transition
// (ADR 0007): {transfer_id, from, to, age_seconds} — the age being how
// long the saga had lived when the transition fired. It is raised by the
// use cases in the SAME transaction as the transition (through the
// transactional outbox, onto the analytics topic), so an occurrence
// exists for every step of every transfer even though the integration
// topic only carries the published contract events.
type StateAdvanced struct {
	TransferID TransferID
	From       TransferState
	To         TransferState
	// AgeSeconds is occurred-at minus the saga's creation time.
	AgeSeconds int64
	OccurredAt time.Time
}

// EventName implements DomainEvent.
func (StateAdvanced) EventName() string { return "TransferStateAdvanced" }

// StuckDetected is the analytics occurrence the bounded health-check
// ticker emits for a non-terminal transfer whose age in state exceeded
// its per-state threshold (ADR 0007): {transfer_id, state, age_seconds,
// threshold_seconds}. The check loop ONLY reads and publishes — it never
// mutates saga state, cancels anything, or releases reservations; what an
// operator does with the signal is a human decision.
type StuckDetected struct {
	TransferID       TransferID
	State            TransferState
	AgeSeconds       int64
	ThresholdSeconds int64
	OccurredAt       time.Time
}

// EventName implements DomainEvent.
func (StuckDetected) EventName() string { return "TransferStuckDetected" }

// RebalanceRunCompleted is the analytics occurrence of one scheduled
// rebalance pass (ADR 0007): {proposal_count, rejected_count,
// stale_facts}. It flows through the same transactional-outbox port as
// the saga events (the port's marker is DomainEvent) even though it
// describes a planning run rather than one transfer: the run observes the
// transfer NETWORK, and the occurrence belongs on this context's
// analytics topic with the rest of its health signal.
type RebalanceRunCompleted struct {
	RunID         string
	ProposalCount int
	RejectedCount int
	StaleFacts    int
	CompletedAt   time.Time
}

// EventName implements DomainEvent.
func (RebalanceRunCompleted) EventName() string { return "RebalanceRunCompleted" }

// StuckView is a read-only projection of one non-terminal transfer for
// the health-check loop (ADR 0007): the two facts a stuck check needs,
// never a rehydrated aggregate. The loop must not load whole aggregates —
// it has no business reading reservation payloads, and a wide projection
// would couple the check to aggregate shape.
type StuckView struct {
	ID        TransferID
	State     TransferState
	UpdatedAt time.Time
}

// NonTerminal reports whether the state is one the saga can still leave:
// only non-terminal states can be stuck. RECEIVED, UNFULFILLABLE and
// CANCELLED are terminal.
func (s TransferState) NonTerminal() bool {
	switch s {
	case StateReceived, StateUnfulfillable, StateCancelled:
		return false
	default:
		return true
	}
}

// StateAdvancedSince derives one StateAdvanced fact per audit entry
// appended after persistedVersion (the same delta UpdateState persists —
// version == the number of persisted audit entries). A use case captures
// Version() after Load (or 0 for a fresh aggregate), applies its
// transition, persists, then publishes these in the same transaction.
func (t *InterWarehouseTransfer) StateAdvancedSince(persistedVersion int64) []StateAdvanced {
	audit := t.Audit()
	loaded := int(persistedVersion)
	if loaded < 0 || loaded > len(audit) {
		return nil
	}
	out := make([]StateAdvanced, 0, len(audit)-loaded)
	for _, e := range audit[loaded:] {
		age := int64(0)
		if !t.createdAt.IsZero() && e.OccurredAt.After(t.createdAt) {
			age = int64(e.OccurredAt.Sub(t.createdAt).Seconds())
		}
		out = append(out, StateAdvanced{
			TransferID: t.id,
			From:       e.From,
			To:         e.To,
			AgeSeconds: age,
			OccurredAt: e.OccurredAt,
		})
	}
	return out
}

// RebalanceRun is one persisted scheduled-rebalance outcome (ADR 0007).
// It lives in the domain (not ports — that package holds interfaces
// only) so the port, the adapter and the HTTP DTO all share ONE type.
type RebalanceRun struct {
	ID int64
	// StartedAt is when the pass began.
	StartedAt time.Time
	// SnapshotAsOf is the watermark of the facts the pass planned from.
	SnapshotAsOf time.Time
	// ProposalCount is the proposals the planner produced.
	ProposalCount int
	// RejectedCount is the proposals the run's own safety checks refused
	// (never an operator rejection: the run approves nothing).
	RejectedCount int
	// Outcome is COMPLETED or FAILED.
	Outcome RebalanceOutcome
	// FailClosedReason carries the BuildSnapshot refusal when Outcome is
	// FAILED; nil otherwise.
	FailClosedReason *string
}

// RebalanceOutcome is the terminal state of one run.
type RebalanceOutcome string

const (
	// RebalanceCompleted means the pass built a snapshot and ran the
	// planner (even if it proposed nothing).
	RebalanceCompleted RebalanceOutcome = "COMPLETED"
	// RebalanceFailed means the fail-closed snapshot build refused; the
	// reason is on the row.
	RebalanceFailed RebalanceOutcome = "FAILED"
)
