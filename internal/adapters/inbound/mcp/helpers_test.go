package mcp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var toolNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

// fakeQuery is an in-memory ports.TransferQuery. It honours the filter
// fields the tools exercise (states, site, updated-before, order, paging)
// so the tests prove the tools drive the use cases correctly end to end.
type fakeQuery struct {
	transfers []*transfer.InterWarehouseTransfer
	err       error
}

func (f *fakeQuery) Get(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	if f.err != nil {
		return nil, f.err
	}
	for _, t := range f.transfers {
		if t.ID() == id {
			return t, nil
		}
	}
	return nil, transfer.ErrTransferNotFound
}

func (f *fakeQuery) List(_ context.Context, filter transfer.ListFilter) (transfer.TransferPage, error) {
	if f.err != nil {
		return transfer.TransferPage{}, f.err
	}
	var matched []*transfer.InterWarehouseTransfer
	for _, t := range f.transfers {
		if len(filter.States) > 0 && !containsState(filter.States, t.State()) {
			continue
		}
		if filter.SiteID != "" && t.OriginSiteID() != filter.SiteID && t.DestinationSiteID() != filter.SiteID {
			continue
		}
		if !filter.UpdatedBefore.IsZero() && !t.UpdatedAt().Before(filter.UpdatedBefore) {
			continue
		}
		matched = append(matched, t)
	}
	total := len(matched)
	if filter.Limit > 0 && len(matched) > filter.Limit {
		matched = matched[:filter.Limit]
	}
	return transfer.TransferPage{Items: matched, Total: total}, nil
}

func containsState(states []transfer.TransferState, s transfer.TransferState) bool {
	for _, candidate := range states {
		if candidate == s {
			return true
		}
	}
	return false
}

// snapshotTransfer builds a transfer in a given state, last moved at updated.
func snapshotTransfer(id string, state transfer.TransferState, origin, destination string, updated time.Time) *transfer.InterWarehouseTransfer {
	return transfer.Rehydrate(transfer.Snapshot{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id, OriginSiteID: origin, DestinationSiteID: destination,
		SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance",
		ProposalAsOf: toolNow.Add(-48 * time.Hour), ExpiresAt: toolNow.Add(24 * time.Hour),
		State: state, CreatedAt: updated.Add(-time.Hour), UpdatedAt: updated, Version: 3,
		Audit: []transfer.AuditEntry{
			{Seq: 1, To: transfer.StateDraft, Event: "TransferDrafted", Reason: "drafted from advisory proposal snapshot", OccurredAt: updated.Add(-time.Hour)},
			{Seq: 2, From: transfer.StateDraft, To: state, Event: "TransferMoved", Reason: "cause-" + id, OccurredAt: updated},
		},
	})
}

func fixtureTransfers() []*transfer.InterWarehouseTransfer {
	return []*transfer.InterWarehouseTransfer{
		snapshotTransfer("tr-alloc-stale", transfer.StateAllocating, "WH1", "WH2", toolNow.Add(-3*time.Hour)),
		snapshotTransfer("tr-alloc-fresh", transfer.StateAllocating, "WH1", "WH3", toolNow.Add(-5*time.Minute)),
		snapshotTransfer("tr-transit-stale", transfer.StateInTransit, "WH2", "WH1", toolNow.Add(-26*time.Hour)),
		snapshotTransfer("tr-received-old", transfer.StateReceived, "WH1", "WH2", toolNow.Add(-72*time.Hour)),
		snapshotTransfer("tr-cancelled-old", transfer.StateCancelled, "WH3", "WH2", toolNow.Add(-90*time.Hour)),
	}
}

// stubSnapshot implements ports.PlanningSnapshotRepository.
type stubSnapshot struct {
	facts planning.Facts
	err   error
}

func (s stubSnapshot) Load(context.Context) (planning.Facts, error) { return s.facts, s.err }

func simulationFacts() planning.Facts {
	due := toolNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, toolNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, toolNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = toolNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = toolNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: toolNow.Add(time.Hour), WindowEnd: toolNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: toolNow.Add(-time.Minute),
	})
	plan2 := plan1
	plan2.PlanID = "plan-2"
	plan2.SiteID = "WH2"
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

func newDeps(q *fakeQuery, snapshot stubSnapshot) inboundmcp.Deps {
	return inboundmcp.Deps{
		GetTransfer:        &usecases.GetTransfer{Query: q},
		ListTransfers:      &usecases.ListTransfers{Query: q},
		FindStuckTransfers: &usecases.FindStuckTransfers{Query: q, Now: func() time.Time { return toolNow }},
		Simulate:           &usecases.SimulateTransferOptions{Snapshot: snapshot, MaxStaleness: time.Hour, Now: func() time.Time { return toolNow }},
	}
}

// harness is a connected in-memory MCP client over the real server wired to
// in-memory fakes, exactly as cmd/mcp wires it (minus Postgres).
type harness struct {
	session *sdk.ClientSession
}

func connectSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientTransport, serverTransport := sdk.NewInMemoryTransports()
	serverSession, err := inboundmcp.NewServer(deps).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return &harness{session: connectSession(t, newDeps(&fakeQuery{transfers: fixtureTransfers()}, stubSnapshot{facts: simulationFacts()}))}
}

// call invokes a tool and fails the test on a TRANSPORT/protocol error;
// tool-level failures come back as res.IsError.
func (h *harness) call(t *testing.T, name string, args map[string]any) *sdk.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport/protocol error (want a tool result): %v", name, err)
	}
	return res
}

// ok calls a tool, requires success, and returns its structured content.
func (h *harness) ok(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := h.call(t, name, args)
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, text(res))
	}
	sc, isMap := res.StructuredContent.(map[string]any)
	if !isMap {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// fail calls a tool and requires an isError result whose text contains want.
func (h *harness) fail(t *testing.T, name string, args map[string]any, want string) {
	t.Helper()
	res := h.call(t, name, args)
	if !res.IsError {
		t.Fatalf("%s: expected an isError tool result, got %#v", name, res.StructuredContent)
	}
	if got := text(res); !strings.Contains(got, want) {
		t.Fatalf("%s: error text %q does not contain %q", name, got, want)
	}
}

func text(res *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, isText := c.(*sdk.TextContent); isText {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}
