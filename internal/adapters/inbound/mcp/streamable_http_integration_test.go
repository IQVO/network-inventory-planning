//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: inboundmcp.Handler(server) mounted on an httptest.Server,
// driven by the SDK's own client (mcp.NewClient + StreamableClientTransport),
// with the REAL Postgres-backed read-side use cases behind it — exactly the
// deployment shape cmd/mcp serves (ADR-0008: Streamable HTTP only). This
// proves the wire contract (initialize, tools/list, tools/call) end-to-end
// over HTTP, not the tool handlers in isolation over in-memory transports.
//
// Postgres comes from testcontainers: one container for the whole package
// run, migrated once into a template database; each test gets a private
// clone (milliseconds). Never an external DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for
// the same pattern's rationale. Never an external DATABASE_URL, never
// t.Skip.
const mcpTemplateDB = "mcp_migrated_template"

var (
	mcpBaseURL string
	mcpDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(mcpRunTests(m))
}

func mcpRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_mcp"),
		tcpostgres.WithUsername("nip_test"),
		tcpostgres.WithPassword("nip_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	mcpBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	if err := mcpCreateDatabase(ctx, mcpTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(mcpWithDB(mcpBaseURL, mcpTemplateDB), mcpMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// mcpMigrationsDir resolves the repo's migrations directory relative to
// THIS FILE (go test's working directory varies by runner).
func mcpMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "internal", "adapters", "outbound", "postgres", "migrations")
}

// mcpWithDB rewrites the path of a connection URL to the named database.
func mcpWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// mcpCreateDatabase creates an empty database inside the shared container.
func mcpCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, mcpBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// mcpMigratedDB hands the test a connection URL to its own private database
// cloned from the migrated template.
func mcpMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_%d", mcpDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), mcpBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, mcpTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return mcpWithDB(mcpBaseURL, name)
}

// mcpNow is the deterministic clock for seeding.
var mcpNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// wiringFactsForMCP seeds the three read models (the same shape the wiring
// and saga suites seed) so the seeded approval and the simulation both
// pass their fail-closed validation: WH1 (origin, capacity 500, demand 40)
// → WH2 (destination, in-window SKU-1 demand 10).
func wiringFactsForMCP() planning.Facts {
	due := mcpNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, mcpNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, mcpNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = mcpNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = mcpNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: mcpNow.Add(time.Hour), WindowEnd: mcpNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: mcpNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: mcpNow.Add(time.Hour), WindowEnd: mcpNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: mcpNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

// mcpHarness wires the REAL production stack — Postgres query repo, the
// read-side use cases, inboundmcp.NewServer, inboundmcp.Handler — and
// serves it over Streamable HTTP. It returns a connected SDK client
// session; the test drives tools/list and tools/call exactly like a model
// host would. One transfer is seeded through the REAL approve use case so
// every read tool has genuine persisted state to read.
type mcpHarness struct {
	session  *sdkmcp.ClientSession
	seeding  seedResult
	transfer string // id of the seeded transfer
}

// seedResult carries the ids the seeded transfer exposes.
type seedResult struct{}

func newMCPHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, mcpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	uow := postgres.NewUnitOfWork(pool)
	transfers := postgres.NewTransferRepo(pool)
	approve := &usecases.ApproveTransfer{
		Transfers:    transfers,
		Events:       postgres.NewOutboxWriter(pool),
		Snapshot:     postgres.NewSnapshotRepo(pool),
		UoW:          uow,
		MaxStaleness: 10 * time.Minute,
		Release: usecases.WorkReleaseConfig{
			PickPathID:        "transfer-pick-path",
			PickCPTOffset:     2 * time.Hour,
			DispatchPathID:    "transfer-dispatch-path",
			DispatchCPTOffset: 3 * time.Hour,
		},
		Now: func() time.Time { return mcpNow },
	}

	// Seed the read models directly through the repos (the consumers are
	// Phase 1 and already covered by their own integration suite).
	facts := wiringFactsForMCP()
	if err := uow.Do(ctx, func(ctx context.Context) error {
		for _, c := range facts.Capabilities {
			if _, err := postgres.NewSiteCapabilityRepo(pool).Upsert(ctx, c); err != nil {
				return err
			}
		}
		for _, d := range facts.Demands {
			if _, err := postgres.NewSiteSkuDemandRepo(pool).Upsert(ctx, d); err != nil {
				return err
			}
		}
		for _, p := range facts.Plans {
			if _, err := postgres.NewPublishedCapacityPlanRepo(pool).Upsert(ctx, p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed read models: %v", err)
	}

	result, err := approve.Execute(ctx, usecases.ApproveTransferInput{
		IdempotencyKey:    "mcp-itcov-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "mcp integration seed",
		ProposalAsOf:      mcpNow.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seed approve: %v", err)
	}

	query := postgres.NewTransferQueryRepo(pool)
	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetTransfer:        &usecases.GetTransfer{Query: query},
		ListTransfers:      &usecases.ListTransfers{Query: query},
		FindStuckTransfers: &usecases.FindStuckTransfers{Query: query, Now: func() time.Time { return mcpNow.Add(30 * time.Minute) }},
		Simulate: &usecases.SimulateTransferOptions{
			Snapshot:     postgres.NewSnapshotRepo(pool),
			MaxStaleness: 10 * time.Minute,
			Now:          func() time.Time { return mcpNow },
		},
	})

	hs := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{session: session, seeding: seedResult{}, transfer: string(result.TransferID)}
}

// call invokes a tool and fails the test on a TRANSPORT/protocol error;
// tool-level failures come back as res.IsError (the contract this suite
// exists to prove).
func (h *mcpHarness) call(t *testing.T, name string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdkmcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport/protocol error (want a tool result): %v", name, err)
	}
	return res
}

// ok calls a tool, requires success, and returns its structured content.
func (h *mcpHarness) ok(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := h.call(t, name, args)
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, resText(res))
	}
	sc, isMap := res.StructuredContent.(map[string]any)
	if !isMap {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// fail calls a tool and requires an isError result whose text contains want.
func (h *mcpHarness) fail(t *testing.T, name string, args map[string]any, want string) {
	t.Helper()
	res := h.call(t, name, args)
	if !res.IsError {
		t.Fatalf("%s: expected an isError tool result, got %#v", name, res.StructuredContent)
	}
	if got := resText(res); !contains(got, want) {
		t.Fatalf("%s: error text %q does not contain %q", name, got, want)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOfSub(s, sub) >= 0)
}

func indexOfSub(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func resText(res *sdkmcp.CallToolResult) string {
	out := ""
	for _, c := range res.Content {
		if tc, isText := c.(*sdkmcp.TextContent); isText {
			out += tc.Text
		}
	}
	return out
}

// TestMCPStreamable_ListToolsExposesTheContract proves tools/list over the
// real HTTP transport exposes exactly the curated read-only tool set.
func TestMCPStreamable_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPHarness(t)

	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
		// Every tool of this context is read-only and must say so, so a
		// host can gate writes.
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %q must carry ReadOnlyHint=true", tool.Name)
		}
	}
	for _, want := range []string{"get_transfer", "list_transfers", "find_stuck_transfers", "simulate_transfer_options"} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
	if len(list.Tools) != 4 {
		t.Fatalf("tools/list advertised %d tools, want exactly the 4 curated ones", len(list.Tools))
	}
}

// TestMCPStreamable_CallToolRoundTripThroughPostgres drives EVERY tool
// through tools/call over Streamable HTTP against the real seeded
// Postgres: the single read, the paged list, the stuck finder and the
// simulation.
func TestMCPStreamable_CallToolRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)

	// get_transfer: the seeded transfer with its audit trail.
	got := h.ok(t, "get_transfer", map[string]any{"transfer_id": h.transfer})
	if got["id"] != h.transfer || got["state"] != "ALLOCATING" || got["sku"] != "SKU-1" {
		t.Fatalf("get_transfer = %#v", got)
	}
	audit, isArr := got["audit"].([]any)
	if !isArr || len(audit) != 4 {
		t.Fatalf("get_transfer audit = %#v, want the 4 creation transitions", got["audit"])
	}

	// list_transfers: the seeded transfer is the whole page.
	page := h.ok(t, "list_transfers", map[string]any{})
	if page["total"] != float64(1) {
		t.Fatalf("list_transfers total = %v, want 1", page["total"])
	}

	// find_stuck_transfers: the transfer's last transition was at mcpNow;
	// the finder runs 30 minutes later, so its age is 30 minutes. A 1-minute
	// threshold flags it as stuck; a 60-minute threshold does not.
	stuck := h.ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 1})
	if stuck["total"] != float64(1) {
		t.Fatalf("find_stuck_transfers total = %v, want 1 (the ALLOCATING seed, age 30m > 1m)", stuck["total"])
	}
	notStuck := h.ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 60})
	if notStuck["total"] != float64(0) {
		t.Fatalf("find_stuck_transfers total = %v, want 0 (age 30m is not > 60m)", notStuck["total"])
	}

	// simulate_transfer_options: the advisory simulation over the seeded
	// facts, fail-closed and green.
	sim := h.ok(t, "simulate_transfer_options", map[string]any{})
	if sim["advisory"] != true {
		t.Fatalf("simulate = %#v, want advisory true", sim)
	}
	sites, isArr := sim["sites"].([]any)
	if !isArr || len(sites) != 2 {
		t.Fatalf("simulate sites = %#v, want the 2 seeded sites", sim["sites"])
	}
}

// TestMCPStreamable_DomainRejectionsAreToolErrors proves the error
// contract: an unknown transfer and invalid input come back as isError
// TOOL results carrying the problem slug — never a transport failure, the
// one failure mode a model host cannot recover from.
func TestMCPStreamable_DomainRejectionsAreToolErrors(t *testing.T) {
	h := newMCPHarness(t)

	h.fail(t, "get_transfer", map[string]any{"transfer_id": "no-such-transfer"}, "transfer-not-found:")
	h.fail(t, "get_transfer", map[string]any{"transfer_id": "   "}, "invalid-query:")
	h.fail(t, "list_transfers", map[string]any{"state": "FLYING"}, "invalid-query:")
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 0}, "invalid-query:")
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 5, "state": "RECEIVED"}, "invalid-query:")
}
