package transfer

import (
	"time"
)

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
	CaseRounding        *CaseRounding
	PolicyVersion       string
	OperatorReason      string
	ProposalAsOf        time.Time
	ExpiresAt           time.Time
	State               TransferState
	ReservationID       string
	Allocations         []Allocation
	AllocationExpiresAt time.Time
	PickedQuantity      int
	StowAllocations     []StowAllocation
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
		caseRounding:        copyRounding(s.CaseRounding),
		policyVersion:       s.PolicyVersion,
		operatorReason:      s.OperatorReason,
		proposalAsOf:        s.ProposalAsOf,
		expiresAt:           s.ExpiresAt,
		state:               s.State,
		reservationID:       s.ReservationID,
		allocations:         append([]Allocation(nil), s.Allocations...),
		allocationExpiresAt: s.AllocationExpiresAt,
		pickedQuantity:      s.PickedQuantity,
		stowAllocations:     append([]StowAllocation(nil), s.StowAllocations...),
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

// Quantity returns the units (eaches) to move: for a SKU with an active CASE
// level, the rounded whole-case quantity.
func (t *InterWarehouseTransfer) Quantity() int { return t.quantity }

// CaseRounding returns a copy of the case-rounding report, or nil when the
// SKU had no active CASE level at approval (the transfer is in plain eaches).
func (t *InterWarehouseTransfer) CaseRounding() *CaseRounding { return copyRounding(t.caseRounding) }

// RequestedQuantity is the quantity asked for before any rounding: the
// demand total. It equals Quantity unless a case rounding applied.
func (t *InterWarehouseTransfer) RequestedQuantity() int {
	if t.caseRounding != nil {
		return t.caseRounding.RequestedEaches
	}
	return t.quantity
}

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

// PickedQuantity returns the quantity the origin pick actually picked
// (set once PICKED; equals the allocated quantity unless the pick was
// short).
func (t *InterWarehouseTransfer) PickedQuantity() int { return t.pickedQuantity }

// DispatchQuantity is the quantity the dispatch leg moves: the PICKED
// quantity once a pick has completed, else the allocated quantity.
func (t *InterWarehouseTransfer) DispatchQuantity() int {
	if t.pickedQuantity > 0 {
		return t.pickedQuantity
	}
	return t.quantity
}

// StowAllocations returns a copy of the per-stock-unit destination stow
// allocations (set once RECEIVED).
func (t *InterWarehouseTransfer) StowAllocations() []StowAllocation {
	return append([]StowAllocation(nil), t.stowAllocations...)
}

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
