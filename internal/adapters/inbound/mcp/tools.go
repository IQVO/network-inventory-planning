package mcp

import (
	"context"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the SAME use cases the REST adapter uses; the adapter never
// constructs an outbound adapter itself. Any field may be nil (a process run
// without DATABASE_URL has no stores): the tool then answers an isError
// result naming the unavailable read side, never a fabricated answer.
type Deps struct {
	GetTransfer        *usecases.GetTransfer
	ListTransfers      *usecases.ListTransfers
	FindStuckTransfers *usecases.FindStuckTransfers
	// Simulate backs simulate_transfer_options (the advisory, read-only
	// simulation over the local read models).
	Simulate *usecases.SimulateTransferOptions
}

// --- shared views -------------------------------------------------------------

// transferView is the wire shape of one transfer. Optional facts are omitted
// until the saga has produced them.
type transferView struct {
	ID                string `json:"id"`
	State             string `json:"state"`
	SKU               string `json:"sku"`
	Quantity          int    `json:"quantity"`
	PickedQuantity    int    `json:"picked_quantity,omitempty"`
	OriginSiteID      string `json:"origin_site_id"`
	DestinationSiteID string `json:"destination_site_id"`
	PolicyVersion     string `json:"policy_version"`
	ReservationID     string `json:"reservation_id,omitempty"`
	RejectionReason   string `json:"rejection_reason,omitempty"`
	ExpiresAt         string `json:"expires_at"`
	CreatedAt         string `json:"created_at"`
	UpdatedAt         string `json:"updated_at"`
	Version           int64  `json:"version"`
}

// auditView is one state transition: when, what moved and the cause.
type auditView struct {
	Seq        int64  `json:"seq"`
	From       string `json:"from,omitempty"`
	To         string `json:"to"`
	Event      string `json:"event"`
	Cause      string `json:"cause"`
	OccurredAt string `json:"occurred_at"`
}

// rfc3339 renders a timestamp the way every tool does (UTC, RFC 3339).
func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func toTransferView(t *transfer.InterWarehouseTransfer) transferView {
	return transferView{
		ID:                string(t.ID()),
		State:             string(t.State()),
		SKU:               t.SKU(),
		Quantity:          t.Quantity(),
		PickedQuantity:    t.PickedQuantity(),
		OriginSiteID:      t.OriginSiteID(),
		DestinationSiteID: t.DestinationSiteID(),
		PolicyVersion:     t.PolicyVersion(),
		ReservationID:     t.ReservationID(),
		RejectionReason:   string(t.RejectionReason()),
		ExpiresAt:         rfc3339(t.ExpiresAt()),
		CreatedAt:         rfc3339(t.CreatedAt()),
		UpdatedAt:         rfc3339(t.UpdatedAt()),
		Version:           t.Version(),
	}
}

func toTransferViews(items []*transfer.InterWarehouseTransfer) []transferView {
	views := make([]transferView, 0, len(items))
	for _, t := range items {
		views = append(views, toTransferView(t))
	}
	return views
}

// transferPageOutput is one page of transfers; total counts every match
// before paging, so total > len(transfers) means there is more.
type transferPageOutput struct {
	Transfers []transferView `json:"transfers"`
	Total     int            `json:"total"`
}

// --- get_transfer -------------------------------------------------------------

type getTransferInput struct {
	TransferID string `json:"transfer_id" jsonschema:"the transfer's id, as returned by list_transfers or the approval response"`
}

type getTransferOutput struct {
	transferView
	Audit []auditView `json:"audit"`
}

func (d Deps) getTransfer(ctx context.Context, in getTransferInput) (getTransferOutput, error) {
	if d.GetTransfer == nil {
		return getTransferOutput{}, errReadSideUnavailable()
	}
	t, err := d.GetTransfer.Execute(ctx, in.TransferID)
	if err != nil {
		return getTransferOutput{}, mapError(err)
	}
	audit := t.Audit()
	entries := make([]auditView, 0, len(audit))
	for _, e := range audit {
		entries = append(entries, auditView{
			Seq: e.Seq, From: string(e.From), To: string(e.To), Event: e.Event, Cause: e.Reason, OccurredAt: rfc3339(e.OccurredAt),
		})
	}
	return getTransferOutput{transferView: toTransferView(t), Audit: entries}, nil
}

// --- list_transfers -----------------------------------------------------------

type listTransfersInput struct {
	State string `json:"state,omitempty" jsonschema:"only transfers currently in this state: DRAFT, PROPOSED, APPROVED, ALLOCATING, ALLOCATED, PICKED, IN_TRANSIT, ARRIVED, RECEIVED, UNFULFILLABLE or CANCELLED"`
	Site  string `json:"site,omitempty" jsonschema:"only transfers whose origin OR destination site is this site id, e.g. WH1"`
	Limit int    `json:"limit,omitempty" jsonschema:"page size, 1 to 200; defaults to 50"`
}

func (d Deps) listTransfers(ctx context.Context, in listTransfersInput) (transferPageOutput, error) {
	if d.ListTransfers == nil {
		return transferPageOutput{}, errReadSideUnavailable()
	}
	page, err := d.ListTransfers.Execute(ctx, usecases.ListTransfersInput{State: in.State, Site: in.Site, Limit: in.Limit})
	if err != nil {
		return transferPageOutput{}, mapError(err)
	}
	return transferPageOutput{Transfers: toTransferViews(page.Items), Total: page.Total}, nil
}

// --- find_stuck_transfers -----------------------------------------------------

type findStuckInput struct {
	OlderThanMinutes int    `json:"older_than_minutes" jsonschema:"a transfer is stuck when its state has not changed for MORE than this many minutes; must be positive"`
	State            string `json:"state,omitempty" jsonschema:"only look at transfers in this NON-terminal state, e.g. ALLOCATING; omit for every non-terminal state"`
	Limit            int    `json:"limit,omitempty" jsonschema:"page size, 1 to 200; defaults to 50"`
}

func (d Deps) findStuckTransfers(ctx context.Context, in findStuckInput) (transferPageOutput, error) {
	if d.FindStuckTransfers == nil {
		return transferPageOutput{}, errReadSideUnavailable()
	}
	page, err := d.FindStuckTransfers.Execute(ctx, usecases.FindStuckInput{
		OlderThan: time.Duration(in.OlderThanMinutes) * time.Minute,
		State:     in.State,
		Limit:     in.Limit,
	})
	if err != nil {
		return transferPageOutput{}, mapError(err)
	}
	return transferPageOutput{Transfers: toTransferViews(page.Items), Total: page.Total}, nil
}

// --- simulate_transfer_options ------------------------------------------------

type simulateInput struct{}

type siteSimulationView struct {
	Site               string  `json:"site"`
	OriginEnabled      bool    `json:"origin_enabled"`
	DestinationEnabled bool    `json:"destination_enabled"`
	TotalDemand        int     `json:"total_demand"`
	CapacityOverWindow float64 `json:"capacity_over_window"`
	CapacityHeadroom   int     `json:"capacity_headroom"`
	WindowStart        string  `json:"window_start"`
	WindowEnd          string  `json:"window_end"`
}

type simulateOutput struct {
	Advisory bool                 `json:"advisory"`
	AsOf     string               `json:"as_of"`
	Sites    []siteSimulationView `json:"sites"`
}

func (d Deps) simulateTransferOptions(ctx context.Context, _ simulateInput) (simulateOutput, error) {
	if d.Simulate == nil {
		return simulateOutput{}, toolError("read-models-unavailable", "this instance runs without the planning read models; the simulation requires them")
	}
	result, err := d.Simulate.Execute(ctx)
	if err != nil {
		// Fail-closed, as the REST endpoint: missing, stale or disabled
		// facts refuse rather than degrade into a simulation of nothing.
		return simulateOutput{}, toolError("read-models-incomplete", err.Error())
	}
	sites := make([]siteSimulationView, 0, len(result.Sites))
	for _, s := range result.Sites {
		sites = append(sites, siteSimulationView{
			Site: s.Site, OriginEnabled: s.OriginEnabled, DestinationEnabled: s.DestinationEnabled,
			TotalDemand: s.TotalDemand, CapacityOverWindow: s.CapacityOverWindow, CapacityHeadroom: s.CapacityHeadroom,
			WindowStart: rfc3339(s.WindowStart), WindowEnd: rfc3339(s.WindowEnd),
		})
	}
	return simulateOutput{Advisory: result.Advisory, AsOf: rfc3339(result.AsOf), Sites: sites}, nil
}

func errReadSideUnavailable() error {
	return toolError("read-side-unavailable", "this instance runs without DATABASE_URL, so there is no transfer store to read")
}

// --- registration -------------------------------------------------------------

// registerTools adds every tool to the server. ALL tools are read-only and
// annotated so; there is deliberately no write tool in this phase (the
// governance test pins the exact set).
func (d Deps) registerTools(server *mcp.Server) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	addTool(server, &mcp.Tool{
		Name: "get_transfer",
		Description: "Read one inter-warehouse transfer by id: its current state (DRAFT, PROPOSED, APPROVED, ALLOCATING, ALLOCATED, PICKED, IN_TRANSIT, ARRIVED, RECEIVED, UNFULFILLABLE or CANCELLED), " +
			"sku, quantity, picked quantity, origin and destination sites, reservation id once allocated, rejection reason once unfulfillable, " +
			"and the full audit trail of state transitions (timestamp and cause of each). Read-only; fails with transfer-not-found for an unknown id.",
		Annotations: readOnly,
	}, d.getTransfer)

	addTool(server, &mcp.Tool{
		Name: "list_transfers",
		Description: "Page through inter-warehouse transfers, newest first, optionally filtered by state and by a site (origin or destination). " +
			"total is the number of matches before paging. Read-only.",
		Annotations: readOnly,
	}, d.listTransfers)

	addTool(server, &mcp.Tool{
		Name: "find_stuck_transfers",
		Description: "Find transfers that are not advancing: those in a NON-terminal state (RECEIVED, UNFULFILLABLE and CANCELLED are finished and never reported) " +
			"whose state has been unchanged for longer than older_than_minutes, stalest first. Optionally narrow to one state, e.g. ALLOCATING for an allocation reply that never arrived. Read-only.",
		Annotations: readOnly,
	}, d.findStuckTransfers)

	addTool(server, &mcp.Tool{
		Name: "simulate_transfer_options",
		Description: "Read the advisory network simulation from the local read models: per site, whether it may originate or receive transfers, its total demand, " +
			"its capacity over the published plan window and the resulting headroom (negative means short). Advisory only: it reserves and moves nothing. " +
			"Fail-closed: fails with read-models-incomplete when facts are missing, stale or a site is disabled. Read-only.",
		Annotations: readOnly,
	}, d.simulateTransferOptions)
}

// addTool registers one tool. A handler error is returned to the SDK as the
// handler's error, which the SDK turns into an isError tool result (never a
// transport/protocol failure), carrying the error text.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		out, err := handle(ctx, in)
		if err != nil {
			var zero Out
			return nil, zero, err
		}
		return nil, out, nil
	})
}
