package transfer

import "time"

// allStates lists every lifecycle state in saga order. It is the closed
// vocabulary the read side validates a requested state filter against.
var allStates = []TransferState{
	StateDraft, StateProposed, StateApproved, StateAllocating, StateAllocated,
	StatePicked, StateInTransit, StateArrived, StateReceived,
	StateUnfulfillable, StateCancelled,
}

// AllStates returns every lifecycle state (a copy).
func AllStates() []TransferState {
	return append([]TransferState(nil), allStates...)
}

// Valid reports whether s is one of the lifecycle states.
func (s TransferState) Valid() bool {
	for _, known := range allStates {
		if s == known {
			return true
		}
	}
	return false
}

// IsTerminal reports whether the saga can never advance out of s:
// RECEIVED (stock stowed at the destination), UNFULFILLABLE (origin
// refused the allocation) and CANCELLED (abandoned pre-release). Every
// other state is waiting for a next step and can therefore be "stuck".
func (s TransferState) IsTerminal() bool {
	switch s {
	case StateReceived, StateUnfulfillable, StateCancelled:
		return true
	default:
		return false
	}
}

// NonTerminalStates returns every state the saga can still advance out of.
func NonTerminalStates() []TransferState {
	out := make([]TransferState, 0, len(allStates))
	for _, s := range allStates {
		if !s.IsTerminal() {
			out = append(out, s)
		}
	}
	return out
}

// Paging bounds of the transfer read side (REST and MCP share them via the
// use cases; the Postgres adapter applies the default defensively).
const (
	// DefaultListLimit is the page size when the caller gives none.
	DefaultListLimit = 50
	// MaxListLimit is the largest page a caller may ask for.
	MaxListLimit = 200
)

// TransferOrder selects the ordering of a transfer listing.
type TransferOrder string

const (
	// OrderNewestFirst orders by creation time, newest first (the default
	// operator view), with the transfer id as a stable tie-break.
	OrderNewestFirst TransferOrder = "newest-first"
	// OrderStalestFirst orders by the last transition time, oldest first:
	// the transfers that have not advanced for longest come first.
	OrderStalestFirst TransferOrder = "stalest-first"
)

// ListFilter narrows a transfer listing. Zero-valued fields do not filter.
// The use cases validate and default Limit before it reaches the port.
type ListFilter struct {
	// States restricts to transfers currently in ANY of these states.
	States []TransferState
	// OriginSiteID / DestinationSiteID restrict to one origin / destination.
	OriginSiteID      string
	DestinationSiteID string
	// SiteID restricts to transfers whose origin OR destination is it.
	SiteID string
	// UpdatedBefore, when non-zero, restricts to transfers whose last
	// transition is strictly before it.
	UpdatedBefore time.Time
	Order         TransferOrder
	Limit         int
	Offset        int
}

// TransferPage is one page of a listing: the transfers (with their audit
// trails) plus the total number matching the filter before paging.
type TransferPage struct {
	Items []*InterWarehouseTransfer
	Total int
}
