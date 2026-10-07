package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrInvalidTransferQuery marks a read request the use case refuses on its
// face (unknown state, bad paging, blank id, non-positive threshold). The
// REST adapter maps it to 400, the MCP adapter to an invalid-query tool
// error.
var ErrInvalidTransferQuery = errors.New("transfer query: invalid request")

func invalidQuery(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidTransferQuery, fmt.Sprintf(format, args...))
}

// GetTransfer reads one transfer (with its audit trail) from the query port.
type GetTransfer struct {
	Query ports.TransferQuery
}

// Execute returns the transfer or transfer.ErrTransferNotFound.
func (u GetTransfer) Execute(ctx context.Context, id string) (*transfer.InterWarehouseTransfer, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, invalidQuery("transfer id is required")
	}
	return u.Query.Get(ctx, transfer.TransferID(id))
}

// ListTransfersInput is the optional filter + paging of a listing.
type ListTransfersInput struct {
	State             string
	OriginSiteID      string
	DestinationSiteID string
	// Site matches a transfer whose origin OR destination is this site
	// (combined with the other filters by AND).
	Site   string
	Limit  int
	Offset int
}

// ListTransfers lists transfers newest first.
type ListTransfers struct {
	Query ports.TransferQuery
}

// Execute validates the input and returns one page.
func (u ListTransfers) Execute(ctx context.Context, in ListTransfersInput) (transfer.TransferPage, error) {
	filter := transfer.ListFilter{
		OriginSiteID:      strings.TrimSpace(in.OriginSiteID),
		DestinationSiteID: strings.TrimSpace(in.DestinationSiteID),
		SiteID:            strings.TrimSpace(in.Site),
		Order:             transfer.OrderNewestFirst,
	}
	if state := strings.TrimSpace(in.State); state != "" {
		parsed, err := parseState(state)
		if err != nil {
			return transfer.TransferPage{}, err
		}
		filter.States = []transfer.TransferState{parsed}
	}
	var err error
	if filter.Limit, filter.Offset, err = normalizePaging(in.Limit, in.Offset); err != nil {
		return transfer.TransferPage{}, err
	}
	return u.Query.List(ctx, filter)
}

// FindStuckInput selects the transfers that have not advanced.
type FindStuckInput struct {
	// OlderThan is how long a transfer's state must have been unchanged
	// to count as stuck; it must be positive.
	OlderThan time.Duration
	// State optionally narrows to one NON-terminal state.
	State  string
	Limit  int
	Offset int
}

// FindStuckTransfers returns transfers whose state has not changed for
// longer than a threshold, excluding terminal states (a RECEIVED,
// UNFULFILLABLE or CANCELLED transfer is finished, not stuck). The
// staleness rule lives HERE, not in an adapter or tool: the port only
// filters and orders.
type FindStuckTransfers struct {
	Query ports.TransferQuery
	// Now supplies the clock (never time.Now directly).
	Now func() time.Time
}

// Execute returns the stuck transfers, stalest first.
func (u FindStuckTransfers) Execute(ctx context.Context, in FindStuckInput) (transfer.TransferPage, error) {
	if u.Now == nil {
		return transfer.TransferPage{}, fmt.Errorf("find stuck transfers: clock is required")
	}
	if in.OlderThan <= 0 {
		return transfer.TransferPage{}, invalidQuery("older_than must be positive, got %s", in.OlderThan)
	}
	states := transfer.NonTerminalStates()
	if state := strings.TrimSpace(in.State); state != "" {
		parsed, err := parseState(state)
		if err != nil {
			return transfer.TransferPage{}, err
		}
		if parsed.IsTerminal() {
			return transfer.TransferPage{}, invalidQuery("state %s is terminal: a finished transfer cannot be stuck", parsed)
		}
		states = []transfer.TransferState{parsed}
	}
	limit, offset, err := normalizePaging(in.Limit, in.Offset)
	if err != nil {
		return transfer.TransferPage{}, err
	}
	return u.Query.List(ctx, transfer.ListFilter{
		States:        states,
		UpdatedBefore: u.Now().Add(-in.OlderThan),
		Order:         transfer.OrderStalestFirst,
		Limit:         limit,
		Offset:        offset,
	})
}

// parseState validates a state name against the closed lifecycle vocabulary
// (case-insensitive, as operators type "allocating").
func parseState(raw string) (transfer.TransferState, error) {
	state := transfer.TransferState(strings.ToUpper(raw))
	if !state.Valid() {
		return "", invalidQuery("unknown state %q (want one of %v)", raw, transfer.AllStates())
	}
	return state, nil
}

// normalizePaging applies the default limit and rejects out-of-range paging.
func normalizePaging(limit, offset int) (int, int, error) {
	if limit < 0 || limit > transfer.MaxListLimit {
		return 0, 0, invalidQuery("limit must be between 0 and %d, got %d", transfer.MaxListLimit, limit)
	}
	if offset < 0 {
		return 0, 0, invalidQuery("offset must not be negative, got %d", offset)
	}
	if limit == 0 {
		limit = transfer.DefaultListLimit
	}
	return limit, offset, nil
}
