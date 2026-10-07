//go:build integration

package main

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TestMCPOverRealPostgres boots the binary's wiring (migrations, pool, query
// adapter, use cases, MCP server, router) against a throwaway Postgres via
// testcontainers and drives the read tools through a Streamable HTTP client.
func TestMCPOverRealPostgres(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_mcp_test"),
		tcpostgres.WithUsername("nip_test"),
		tcpostgres.WithPassword("nip_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "internal", "adapters", "outbound", "postgres", "migrations")

	deps, closeFn, err := buildDeps(ctx, quietLogger(), dsn, dsn, migrations, defaultMaxStaleness)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeFn)

	// Seed one transfer through the write repository (a second pool; the
	// MCP binary itself never writes).
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	now := time.Now().UTC()
	tr, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: "tr-mcp-1", IdempotencyKey: "k-mcp-1", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 4,
		PolicyVersion: "p1", OperatorReason: "rebalance", ProposalAsOf: now.Add(-2 * time.Hour), ExpiresAt: now.Add(24 * time.Hour), Now: now.Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if existing, err := postgres.NewTransferRepo(pool).Create(ctx, tr, "k-mcp-1"); err != nil || existing != nil {
		t.Fatalf("create: %v (existing %v)", err, existing)
	}

	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))))
	t.Cleanup(srv.Close)
	client := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "get_transfer", Arguments: map[string]any{"transfer_id": "tr-mcp-1"}})
	if err != nil || res.IsError {
		t.Fatalf("get_transfer: err=%v res=%+v", err, res)
	}
	got := res.StructuredContent.(map[string]any)
	if got["state"] != "PROPOSED" || got["origin_site_id"] != "WH1" || len(got["audit"].([]any)) != 2 {
		t.Fatalf("get_transfer = %#v", got)
	}

	// Proposed two hours ago and never advanced: stuck past an hour.
	res, err = session.CallTool(ctx, &sdk.CallToolParams{Name: "find_stuck_transfers", Arguments: map[string]any{"older_than_minutes": 60}})
	if err != nil || res.IsError {
		t.Fatalf("find_stuck_transfers: err=%v res=%+v", err, res)
	}
	stuck := res.StructuredContent.(map[string]any)
	if stuck["total"] != float64(1) {
		t.Fatalf("find_stuck_transfers = %#v, want the one stuck transfer", stuck)
	}

	res, err = session.CallTool(ctx, &sdk.CallToolParams{Name: "get_transfer", Arguments: map[string]any{"transfer_id": "nope"}})
	if err != nil || !res.IsError {
		t.Fatalf("unknown transfer: err=%v res=%+v, want an isError result", err, res)
	}
}
