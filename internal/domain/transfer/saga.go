package transfer

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TransferState is the lifecycle state of an InterWarehouseTransfer (the
// Phase-2 saga aggregate). Legal progression:
//
//	DRAFT -> PROPOSED -> APPROVED -> ALLOCATING -> ALLOCATED -> PICKED
//	                                              -> IN_TRANSIT -> ARRIVED
//	                                              -> RECEIVED (terminal)
//	                                        \-> UNFULFILLABLE
//	Any pre-release state              -> CANCELLED
//	ALLOCATED  -> RELEASING -> CANCELLED            (ADR 0022)
//
// A reservation id (with its allocations and expiry) exists only from
// ALLOCATED through RECEIVED; migration 0009's CHECK enforces it. The release
// settle clears it on the way to CANCELLED (ADR 0023).
//
// CANCELLED is allowed only BEFORE the origin reservation exists (in v1,
// from DRAFT, PROPOSED, APPROVED or ALLOCATING): once stock is ALLOCATED
// the saga must not silently forget a hold inventory-storage is keeping;
// releasing it goes through RELEASING — the explicit revocation path of
// ADR 0022 — and settles to CANCELLED only on inventory-storage's
// TransferStockAllocationReleased fact. The fact-driven tail (PICKED
// onwards, ADR 0005) is likewise not cancellable: the stock is physically
// in motion.
type TransferState string

const (
	StateDraft         TransferState = "DRAFT"
	StateProposed      TransferState = "PROPOSED"
	StateApproved      TransferState = "APPROVED"
	StateAllocating    TransferState = "ALLOCATING"
	StateAllocated     TransferState = "ALLOCATED"
	StateReleasing     TransferState = "RELEASING"
	StatePicked        TransferState = "PICKED"
	StateInTransit     TransferState = "IN_TRANSIT"
	StateArrived       TransferState = "ARRIVED"
	StateReceived      TransferState = "RECEIVED"
	StateUnfulfillable TransferState = "UNFULFILLABLE"
	StateCancelled     TransferState = "CANCELLED"
)

// TransferID identifies one inter-warehouse transfer aggregate.
type TransferID string

// String returns the id as a plain string.
func (id TransferID) String() string { return string(id) }

// LineID is the v1 single transfer line identifier: "<transfer_id>:1".
// inventory-storage's transfer_allocations ledger is keyed by it, so the
// wire contract (TransferAllocationRequested.data.transfer_line_id and both
// reply types) uses exactly this string.
func (id TransferID) LineID() string { return string(id) + ":1" }

// RejectionReason is inventory-storage's CLOSED rejection vocabulary for a
// TransferStockAllocationRejected reply. Anything else is a contract break,
// not a rejection.
type RejectionReason string

const (
	RejectionOriginSiteUnknown   RejectionReason = "ORIGIN_SITE_UNKNOWN"
	RejectionInsufficientUsable  RejectionReason = "INSUFFICIENT_USABLE"
	RejectionIdempotencyConflict RejectionReason = "IDEMPOTENCY_CONFLICT"
)

// Valid reports whether r is one of the closed reasons.
func (r RejectionReason) Valid() bool {
	switch r {
	case RejectionOriginSiteUnknown, RejectionInsufficientUsable, RejectionIdempotencyConflict:
		return true
	default:
		return false
	}
}

// ErrTransferNotFound marks a Load of an absent transfer.
var ErrTransferNotFound = errors.New("transfer: not found")

// ErrFactRefused marks a fact whose payload fails the aggregate's
// consistency checks (an impossible quantity, a foreign line, a wrong
// site/SKU). Deterministic: consumers log it and commit past, never
// retry — the same payload always refuses the same way.
var ErrFactRefused = errors.New("transfer: fact refused")

// ErrReplyRefused marks an inventory-storage allocation reply
// (TransferStockAllocated / TransferStockAllocationRejected) whose payload
// fails the aggregate's consistency checks. Deterministic: the same bytes
// always refuse the same way, so the reply consumer dead-letters the
// message and commits past it instead of retrying it forever on its
// partition (ADR 0013).
var ErrReplyRefused = errors.New("transfer: allocation reply refused")

// ErrReplyLineMismatch marks a reply addressed to a transfer line other
// than this transfer's single line ("<transfer_id>:1"). It wraps
// ErrReplyRefused.
var ErrReplyLineMismatch = fmt.Errorf("%w: line id mismatch", ErrReplyRefused)

// ErrUnknownRejectionReason marks a rejection reply whose reason is outside
// inventory-storage's closed vocabulary. It wraps ErrReplyRefused.
var ErrUnknownRejectionReason = fmt.Errorf("%w: unknown rejection reason", ErrReplyRefused)

// ErrProposalExpired marks an approval attempted at or after expires_at.
var ErrProposalExpired = errors.New("transfer: proposal expired")

// IllegalTransitionError marks a command applied from a state it is not
// legal in. It is DETERMINISTIC: consumers log it and move on, never retry.
type IllegalTransitionError struct {
	ID     TransferID
	From   TransferState
	Action string
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("transfer %s: cannot %s from state %s", e.ID, e.Action, e.From)
}

// Allocation is one stock unit's contribution to the origin reservation,
// mirrored from inventory-storage's TransferStockAllocated payload.
type Allocation struct {
	StockUnitID string
	BinID       string
	Quantity    int
}

// StockAllocation mirrors inventory-storage's TransferStockAllocated reply
// payload (the fields this aggregate needs; the reply's transfer_id is the
// Load key and the CE time is the transition's occurred-at).
type StockAllocation struct {
	TransferLineID string
	OriginSiteID   string
	SKU            string
	ReservationID  string
	Quantity       int
	Allocations    []Allocation
	ExpiresAt      time.Time
}

// AuditEntry is one immutable state-transition record of the saga's
// event-sourced audit trail. Seq is assigned by persistence (0 while
// unsaved); From is "" for the aggregate's creation entry.
type AuditEntry struct {
	Seq        int64
	From       TransferState
	To         TransferState
	Event      string
	Reason     string
	OccurredAt time.Time
}

// InterWarehouseTransfer is the transfer saga aggregate: an operator-approved
// plan to move quantity of one SKU from an origin site to a destination
// site, driven to ALLOCATED by inventory-storage's allocation replies.
// Fields are unexported and mutated only through the command methods, so an
// illegal transition is impossible without going through the state machine.
type InterWarehouseTransfer struct {
	id                  TransferID
	idempotencyKey      string
	originSiteID        string
	destinationSiteID   string
	sku                 string
	quantity            int
	caseRounding        *CaseRounding
	policyVersion       string
	operatorReason      string
	proposalAsOf        time.Time
	expiresAt           time.Time
	state               TransferState
	reservationID       string
	allocations         []Allocation
	allocationExpiresAt time.Time
	pickedQuantity      int
	stowAllocations     []StowAllocation
	rejectionReason     RejectionReason
	audit               []AuditEntry
	createdAt           time.Time
	updatedAt           time.Time
	version             int64
}

// Approve records the operator's approval and raises TransferPlanApproved.
// Legal only from PROPOSED and only strictly before expires_at.
func (t *InterWarehouseTransfer) Approve(now time.Time) (PlanApproved, error) {
	if t.state != StateProposed {
		return PlanApproved{}, &IllegalTransitionError{ID: t.id, From: t.state, Action: "approve"}
	}
	if !t.expiresAt.After(now) {
		return PlanApproved{}, fmt.Errorf("%w: transfer %s expired at %s, now %s", ErrProposalExpired, t.id, t.expiresAt.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	occurred := now.UTC()
	t.transition(StateApproved, "TransferPlanApproved", t.operatorReason, occurred)
	return PlanApproved{
		TransferID:        t.id,
		OriginSiteID:      t.originSiteID,
		DestinationSiteID: t.destinationSiteID,
		SKU:               t.sku,
		Quantity:          t.quantity,
		CaseRounding:      copyRounding(t.caseRounding),
		PolicyVersion:     t.policyVersion,
		OperatorReason:    t.operatorReason,
		ProposalAsOf:      t.proposalAsOf,
		OccurredAt:        occurred,
	}, nil
}

// RequestAllocation moves the saga to ALLOCATING and raises the
// TransferAllocationRequested command event for inventory-storage.
func (t *InterWarehouseTransfer) RequestAllocation(now time.Time) (AllocationRequested, error) {
	if t.state != StateApproved {
		return AllocationRequested{}, &IllegalTransitionError{ID: t.id, From: t.state, Action: "request allocation"}
	}
	occurred := now.UTC()
	t.transition(StateAllocating, "TransferAllocationRequested", "origin allocation requested", occurred)
	return AllocationRequested{
		TransferID:     t.id,
		TransferLineID: t.id.LineID(),
		OriginSiteID:   t.originSiteID,
		SKU:            t.sku,
		Quantity:       t.quantity,
		OccurredAt:     occurred,
	}, nil
}

// MarkAllocated applies inventory-storage's TransferStockAllocated reply:
// state ALLOCATED with the reservation id, per-stock-unit allocations and
// the reservation expiry persisted on the aggregate. Every mismatch between
// the reply and the transfer it claims to answer is a deterministic error
// (the consumer skips the message), never a best-effort write.
func (t *InterWarehouseTransfer) MarkAllocated(a StockAllocation, now time.Time) error {
	if t.state != StateAllocating {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark allocated"}
	}
	if err := t.checkAllocationReply(a); err != nil {
		return err
	}
	t.reservationID = a.ReservationID
	t.allocations = append([]Allocation(nil), a.Allocations...)
	t.allocationExpiresAt = a.ExpiresAt.UTC()
	t.transition(StateAllocated, "TransferStockAllocated", "reservation "+a.ReservationID, now.UTC())
	return nil
}

// MarkUnfulfillable applies inventory-storage's
// TransferStockAllocationRejected reply with one of the closed reasons.
func (t *InterWarehouseTransfer) MarkUnfulfillable(lineID string, reason RejectionReason, now time.Time) error {
	if t.state != StateAllocating {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark unfulfillable"}
	}
	if !reason.Valid() {
		return fmt.Errorf("%w: transfer %s: reason %q", ErrUnknownRejectionReason, t.id, reason)
	}
	if lineID != t.id.LineID() {
		return fmt.Errorf("%w: transfer %s: rejection reply is for line %q, want %q", ErrReplyLineMismatch, t.id, lineID, t.id.LineID())
	}
	t.rejectionReason = reason
	t.transition(StateUnfulfillable, "TransferStockAllocationRejected", string(reason), now.UTC())
	return nil
}

// CancelAfterAllocation is the ALLOCATED transfer's cancel path (ADR 0022):
// it moves the saga to RELEASING and raises the
// TransferAllocationReleaseRequested command for inventory-storage, which
// resolves the origin reservation and answers
// TransferStockAllocationReleased; that fact settles the saga to CANCELLED
// (MarkReleased). The transfer itself never releases stock directly —
// inventory-storage owns the hold.
func (t *InterWarehouseTransfer) CancelAfterAllocation(reason string, now time.Time) (ReleaseRequested, error) {
	if t.state != StateAllocated {
		return ReleaseRequested{}, &IllegalTransitionError{ID: t.id, From: t.state, Action: "cancel after allocation"}
	}
	if strings.TrimSpace(reason) == "" {
		return ReleaseRequested{}, fmt.Errorf("transfer %s: cancel reason is required", t.id)
	}
	occurred := now.UTC()
	t.transition(StateReleasing, "TransferAllocationReleaseRequested", reason, occurred)
	return ReleaseRequested{
		TransferID:     t.id,
		TransferLineID: t.id.LineID(),
		ReservationID:  t.reservationID,
		OriginSiteID:   t.originSiteID,
		SKU:            t.sku,
		Quantity:       t.quantity,
		Reason:         reason,
		OccurredAt:     occurred,
	}, nil
}

// Cancel abandons the transfer. Legal only BEFORE the origin reservation
// exists (pre-release); an ALLOCATED transfer must be released explicitly,
// never silently cancelled.
func (t *InterWarehouseTransfer) Cancel(reason string, now time.Time) error {
	switch t.state {
	case StateDraft, StateProposed, StateApproved, StateAllocating:
	default:
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "cancel"}
	}
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("transfer %s: cancel reason is required", t.id)
	}
	t.transition(StateCancelled, "TransferCancelled", reason, now.UTC())
	return nil
}

// transition applies one state change and appends its immutable audit
// entry. The only writer of t.state and t.audit.
func (t *InterWarehouseTransfer) transition(to TransferState, event, reason string, occurredAt time.Time) {
	t.audit = append(t.audit, AuditEntry{
		From:       t.state,
		To:         to,
		Event:      event,
		Reason:     reason,
		OccurredAt: occurredAt,
	})
	t.state = to
	t.updatedAt = occurredAt
}

// checkAllocationReply proves the reply answers THIS transfer's single line
// with a complete reservation.
func (t *InterWarehouseTransfer) checkAllocationReply(a StockAllocation) error {
	if a.TransferLineID != t.id.LineID() {
		return fmt.Errorf("%w: transfer %s: allocation reply is for line %q, want %q", ErrReplyLineMismatch, t.id, a.TransferLineID, t.id.LineID())
	}
	if a.OriginSiteID != t.originSiteID {
		return fmt.Errorf("%w: transfer %s: allocation reply origin %q, want %q", ErrReplyRefused, t.id, a.OriginSiteID, t.originSiteID)
	}
	if a.SKU != t.sku {
		return fmt.Errorf("%w: transfer %s: allocation reply sku %q, want %q", ErrReplyRefused, t.id, a.SKU, t.sku)
	}
	if strings.TrimSpace(a.ReservationID) == "" {
		return fmt.Errorf("%w: transfer %s: allocation reply carries no reservation id", ErrReplyRefused, t.id)
	}
	if len(a.Allocations) == 0 {
		return fmt.Errorf("%w: transfer %s: allocation reply carries no stock allocations", ErrReplyRefused, t.id)
	}
	total := 0
	for _, alloc := range a.Allocations {
		if strings.TrimSpace(alloc.StockUnitID) == "" {
			return fmt.Errorf("%w: transfer %s: allocation for reservation %s has no stock unit id", ErrReplyRefused, t.id, a.ReservationID)
		}
		if strings.TrimSpace(alloc.BinID) == "" {
			return fmt.Errorf("%w: transfer %s: allocation for stock unit %s has no bin id", ErrReplyRefused, t.id, alloc.StockUnitID)
		}
		if alloc.Quantity <= 0 {
			return fmt.Errorf("%w: transfer %s: allocation for stock unit %s must be positive, got %d", ErrReplyRefused, t.id, alloc.StockUnitID, alloc.Quantity)
		}
		total += alloc.Quantity
	}
	if total != t.quantity {
		return fmt.Errorf("%w: transfer %s: allocation reply totals %d, want the requested %d", ErrReplyRefused, t.id, total, t.quantity)
	}
	if a.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: transfer %s: allocation reply carries no reservation expiry", ErrReplyRefused, t.id)
	}
	return nil
}
