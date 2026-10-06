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
//	DRAFT -> PROPOSED -> APPROVED -> ALLOCATING -> ALLOCATED
//	                                        \-> UNFULFILLABLE
//	Any pre-release state              -> CANCELLED
//
// CANCELLED is allowed only BEFORE the origin reservation exists (in v1,
// from DRAFT, PROPOSED, APPROVED or ALLOCATING): once stock is ALLOCATED
// the saga must not silently forget a hold inventory-storage is keeping;
// releasing it needs the explicit revocation path of a later phase.
type TransferState string

const (
	StateDraft         TransferState = "DRAFT"
	StateProposed      TransferState = "PROPOSED"
	StateApproved      TransferState = "APPROVED"
	StateAllocating    TransferState = "ALLOCATING"
	StateAllocated     TransferState = "ALLOCATED"
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
	policyVersion       string
	operatorReason      string
	proposalAsOf        time.Time
	expiresAt           time.Time
	state               TransferState
	reservationID       string
	allocations         []Allocation
	allocationExpiresAt time.Time
	rejectionReason     RejectionReason
	audit               []AuditEntry
	createdAt           time.Time
	updatedAt           time.Time
	version             int64
}

// ProposalInput is everything ProposeTransfer needs, including the clock
// (never time.Now inside the domain).
type ProposalInput struct {
	ID                TransferID
	IdempotencyKey    string
	OriginSiteID      string
	DestinationSiteID string
	SKU               string
	Quantity          int
	PolicyVersion     string
	OperatorReason    string
	ProposalAsOf      time.Time
	ExpiresAt         time.Time
	Now               time.Time
}

// ProposeTransfer validates the proposal snapshot and returns the aggregate
// in state PROPOSED, with the creation and proposal transitions already in
// its audit trail. The caller (the approval use case) then applies Approve
// and RequestAllocation in the same transaction.
func ProposeTransfer(in ProposalInput) (*InterWarehouseTransfer, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	now := in.Now.UTC()
	t := &InterWarehouseTransfer{
		id:                in.ID,
		idempotencyKey:    in.IdempotencyKey,
		originSiteID:      in.OriginSiteID,
		destinationSiteID: in.DestinationSiteID,
		sku:               in.SKU,
		quantity:          in.Quantity,
		policyVersion:     in.PolicyVersion,
		operatorReason:    in.OperatorReason,
		proposalAsOf:      in.ProposalAsOf.UTC(),
		expiresAt:         in.ExpiresAt.UTC(),
		state:             StateDraft,
		createdAt:         now,
		updatedAt:         now,
	}
	t.transition(StateDraft, "TransferDrafted", "drafted from advisory proposal snapshot", now)
	t.transition(StateProposed, "TransferProposed", in.OperatorReason, now)
	return t, nil
}

func (in ProposalInput) validate() error {
	if strings.TrimSpace(string(in.ID)) == "" {
		return fmt.Errorf("transfer proposal: transfer id is required")
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return fmt.Errorf("transfer proposal %s: idempotency key is required", in.ID)
	}
	if strings.TrimSpace(in.OriginSiteID) == "" {
		return fmt.Errorf("transfer proposal %s: origin site is required", in.ID)
	}
	if strings.TrimSpace(in.DestinationSiteID) == "" {
		return fmt.Errorf("transfer proposal %s: destination site is required", in.ID)
	}
	if in.OriginSiteID == in.DestinationSiteID {
		return fmt.Errorf("transfer proposal %s: origin and destination must differ", in.ID)
	}
	if strings.TrimSpace(in.SKU) == "" {
		return fmt.Errorf("transfer proposal %s: sku is required", in.ID)
	}
	if in.Quantity <= 0 {
		return fmt.Errorf("transfer proposal %s: quantity must be positive, got %d", in.ID, in.Quantity)
	}
	if strings.TrimSpace(in.PolicyVersion) == "" {
		return fmt.Errorf("transfer proposal %s: policy version is required", in.ID)
	}
	if in.ProposalAsOf.IsZero() {
		return fmt.Errorf("transfer proposal %s: proposal as-of is required", in.ID)
	}
	if in.Now.IsZero() {
		return fmt.Errorf("transfer proposal %s: clock is required", in.ID)
	}
	if in.ExpiresAt.IsZero() {
		return fmt.Errorf("transfer proposal %s: expiry is required", in.ID)
	}
	if !in.ExpiresAt.After(in.Now) {
		return fmt.Errorf("%w: transfer %s expires at %s, now %s", ErrProposalExpired, in.ID, in.ExpiresAt.UTC().Format(time.RFC3339), in.Now.UTC().Format(time.RFC3339))
	}
	return nil
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
		return fmt.Errorf("transfer %s: unknown allocation rejection reason %q", t.id, reason)
	}
	if lineID != t.id.LineID() {
		return fmt.Errorf("transfer %s: rejection reply is for line %q, want %q", t.id, lineID, t.id.LineID())
	}
	t.rejectionReason = reason
	t.transition(StateUnfulfillable, "TransferStockAllocationRejected", string(reason), now.UTC())
	return nil
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
		return fmt.Errorf("transfer %s: allocation reply is for line %q, want %q", t.id, a.TransferLineID, t.id.LineID())
	}
	if a.OriginSiteID != t.originSiteID {
		return fmt.Errorf("transfer %s: allocation reply origin %q, want %q", t.id, a.OriginSiteID, t.originSiteID)
	}
	if a.SKU != t.sku {
		return fmt.Errorf("transfer %s: allocation reply sku %q, want %q", t.id, a.SKU, t.sku)
	}
	if strings.TrimSpace(a.ReservationID) == "" {
		return fmt.Errorf("transfer %s: allocation reply carries no reservation id", t.id)
	}
	if len(a.Allocations) == 0 {
		return fmt.Errorf("transfer %s: allocation reply carries no stock allocations", t.id)
	}
	total := 0
	for _, alloc := range a.Allocations {
		if strings.TrimSpace(alloc.StockUnitID) == "" {
			return fmt.Errorf("transfer %s: allocation for reservation %s has no stock unit id", t.id, a.ReservationID)
		}
		if strings.TrimSpace(alloc.BinID) == "" {
			return fmt.Errorf("transfer %s: allocation for stock unit %s has no bin id", t.id, alloc.StockUnitID)
		}
		if alloc.Quantity <= 0 {
			return fmt.Errorf("transfer %s: allocation for stock unit %s must be positive, got %d", t.id, alloc.StockUnitID, alloc.Quantity)
		}
		total += alloc.Quantity
	}
	if total != t.quantity {
		return fmt.Errorf("transfer %s: allocation reply totals %d, want the requested %d", t.id, total, t.quantity)
	}
	if a.ExpiresAt.IsZero() {
		return fmt.Errorf("transfer %s: allocation reply carries no reservation expiry", t.id)
	}
	return nil
}

// Snapshot is the persisted shape of a transfer: everything Rehydrate
// needs. It exists so the Postgres repository can rebuild the aggregate
// without poking at unexported fields.
type Snapshot struct {
	ID                  TransferID
	IdempotencyKey      string
	OriginSiteID        string
	DestinationSiteID   string
	SKU                 string
	Quantity            int
	PolicyVersion       string
	OperatorReason      string
	ProposalAsOf        time.Time
	ExpiresAt           time.Time
	State               TransferState
	ReservationID       string
	Allocations         []Allocation
	AllocationExpiresAt time.Time
	RejectionReason     RejectionReason
	Audit               []AuditEntry
	CreatedAt           time.Time
	UpdatedAt           time.Time
	Version             int64
}

// Rehydrate rebuilds an aggregate from its persisted snapshot. It trusts
// the database (CHECK constraints guard the enum) and performs NO
// transitions: the audit trail comes back exactly as persisted.
func Rehydrate(s Snapshot) *InterWarehouseTransfer {
	return &InterWarehouseTransfer{
		id:                  s.ID,
		idempotencyKey:      s.IdempotencyKey,
		originSiteID:        s.OriginSiteID,
		destinationSiteID:   s.DestinationSiteID,
		sku:                 s.SKU,
		quantity:            s.Quantity,
		policyVersion:       s.PolicyVersion,
		operatorReason:      s.OperatorReason,
		proposalAsOf:        s.ProposalAsOf,
		expiresAt:           s.ExpiresAt,
		state:               s.State,
		reservationID:       s.ReservationID,
		allocations:         append([]Allocation(nil), s.Allocations...),
		allocationExpiresAt: s.AllocationExpiresAt,
		rejectionReason:     s.RejectionReason,
		audit:               append([]AuditEntry(nil), s.Audit...),
		createdAt:           s.CreatedAt,
		updatedAt:           s.UpdatedAt,
		version:             s.Version,
	}
}

// ID returns the transfer id.
func (t *InterWarehouseTransfer) ID() TransferID { return t.id }

// IdempotencyKey returns the transfer-level idempotency key (unique across
// transfers; the approval endpoint's Idempotency-Key header).
func (t *InterWarehouseTransfer) IdempotencyKey() string { return t.idempotencyKey }

// OriginSiteID returns the donating site.
func (t *InterWarehouseTransfer) OriginSiteID() string { return t.originSiteID }

// DestinationSiteID returns the receiving site.
func (t *InterWarehouseTransfer) DestinationSiteID() string { return t.destinationSiteID }

// SKU returns the stock keeping unit being moved.
func (t *InterWarehouseTransfer) SKU() string { return t.sku }

// Quantity returns the units to move.
func (t *InterWarehouseTransfer) Quantity() int { return t.quantity }

// PolicyVersion returns the policy version the proposal was scored under.
func (t *InterWarehouseTransfer) PolicyVersion() string { return t.policyVersion }

// OperatorReason returns the operator's captured approval reason.
func (t *InterWarehouseTransfer) OperatorReason() string { return t.operatorReason }

// ProposalAsOf returns the advisory proposal snapshot's as-of watermark.
func (t *InterWarehouseTransfer) ProposalAsOf() time.Time { return t.proposalAsOf }

// ExpiresAt returns the proposal's expiry deadline.
func (t *InterWarehouseTransfer) ExpiresAt() time.Time { return t.expiresAt }

// State returns the current lifecycle state.
func (t *InterWarehouseTransfer) State() TransferState { return t.state }

// ReservationID returns inventory-storage's reservation id (set once
// ALLOCATED).
func (t *InterWarehouseTransfer) ReservationID() string { return t.reservationID }

// Allocations returns a copy of the per-stock-unit origin allocations.
func (t *InterWarehouseTransfer) Allocations() []Allocation {
	return append([]Allocation(nil), t.allocations...)
}

// AllocationExpiresAt returns the origin reservation's expiry.
func (t *InterWarehouseTransfer) AllocationExpiresAt() time.Time { return t.allocationExpiresAt }

// RejectionReason returns inventory-storage's closed rejection reason (set
// once UNFULFILLABLE).
func (t *InterWarehouseTransfer) RejectionReason() RejectionReason { return t.rejectionReason }

// Audit returns a copy of the immutable state-transition trail.
func (t *InterWarehouseTransfer) Audit() []AuditEntry {
	return append([]AuditEntry(nil), t.audit...)
}

// CreatedAt returns the aggregate creation time.
func (t *InterWarehouseTransfer) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt returns the last transition time.
func (t *InterWarehouseTransfer) UpdatedAt() time.Time { return t.updatedAt }

// Version returns the persistence version for optimistic concurrency.
func (t *InterWarehouseTransfer) Version() int64 { return t.version }

// SetVersion is called by the repository after a successful UpdateState so
// a subsequent update in the same process carries the new version.
func (t *InterWarehouseTransfer) SetVersion(v int64) { t.version = v }
