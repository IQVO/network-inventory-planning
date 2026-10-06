package transfer

import (
	"time"
)

// DomainEvent is the marker interface every saga event satisfies so the
// outbox publisher port can encode a heterogeneous event list.
type DomainEvent interface {
	EventName() string
}

// PlanApproved is the TransferPlanApproved domain event raised by Approve:
// an operator turned an advisory proposal into an approved transfer plan.
// It is published through the transactional outbox in the SAME transaction
// as the state change (see ADR 0003).
type PlanApproved struct {
	TransferID        TransferID
	OriginSiteID      string
	DestinationSiteID string
	SKU               string
	Quantity          int
	PolicyVersion     string
	OperatorReason    string
	ProposalAsOf      time.Time
	OccurredAt        time.Time
}

// EventName implements DomainEvent.
func (PlanApproved) EventName() string { return "TransferPlanApproved" }

// AllocationRequested is the TransferAllocationRequested COMMAND event
// raised by RequestAllocation: inventory-storage allocates origin stock
// against transfer_line_id and replies with TransferStockAllocated or
// TransferStockAllocationRejected on warehouse.inventory.events. The wire
// payload is exactly inventory-storage's consumed contract: {transfer_id,
// transfer_line_id, origin_site_id, sku, quantity}, key transfer_line_id.
type AllocationRequested struct {
	TransferID     TransferID
	TransferLineID string
	OriginSiteID   string
	SKU            string
	Quantity       int
	OccurredAt     time.Time
}

// EventName implements DomainEvent.
func (AllocationRequested) EventName() string { return "TransferAllocationRequested" }
