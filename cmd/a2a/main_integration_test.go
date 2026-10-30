//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inbounda2a "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TestA2AOverRealPostgres boots the binary's wiring (migrations, pool, query
// adapter, use cases, A2A executor, router) against a throwaway Postgres via
// testcontainers and drives both read skills through the SDK's JSON-RPC
// client. The agent is read-only, so it also proves the skills add ZERO
// outbox rows (nothing here may publish a fact).
func TestA2AOverRealPostgres(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_a2a_test"),
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

	deps, closeFn, err := buildDeps(ctx, quietLogger(), dsn, dsn, migrations)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeFn)

	// Seed one transfer through the write repository (a second pool; the A2A
	// binary itself never writes).
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	now := time.Now().UTC()
	tr, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: "tr-a2a-1", IdempotencyKey: "k-a2a-1", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 4,
		PolicyVersion: "p1", OperatorReason: "rebalance", ProposalAsOf: now.Add(-2 * time.Hour), ExpiresAt: now.Add(24 * time.Hour), Now: now.Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if existing, err := postgres.NewTransferRepo(pool).Create(ctx, tr, "k-a2a-1"); err != nil || existing != nil {
		t.Fatalf("create: %v (existing %v)", err, existing)
	}
	outboxBefore := outboxRows(t, ctx, pool)

	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})
	srv := httptest.NewServer(newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, "test-a2a"))
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/a2a", Version: "test"}))
	if err != nil {
		t.Fatal(err)
	}

	status := call(t, ctx, client, "transfer_status", map[string]any{"transferId": "tr-a2a-1"})
	ts, _ := status["data"].(map[string]any)["transferStatus"].(map[string]any)
	if ts["state"] != "PROPOSED" || ts["originSiteId"] != "WH1" || len(ts["audit"].([]any)) != 2 {
		t.Fatalf("transfer_status = %#v", status)
	}

	// Proposed two hours ago and never advanced: stuck past an hour.
	stuck := call(t, ctx, client, "find_stuck_transfers", map[string]any{"olderThanMinutes": 60})
	st, _ := stuck["data"].(map[string]any)["stuckTransfers"].(map[string]any)
	if st["total"] != float64(1) {
		t.Fatalf("find_stuck_transfers = %#v, want the one stuck transfer", stuck)
	}

	missing := call(t, ctx, client, "transfer_status", map[string]any{"transferId": "nope"})
	if p, _ := missing["problem"].(map[string]any); p == nil || p["status"] != float64(404) {
		t.Fatalf("unknown transfer = %#v, want an in-band 404 problem", missing)
	}

	if after := outboxRows(t, ctx, pool); after != outboxBefore {
		t.Fatalf("outbox rows %d -> %d: a read-only skill must not publish anything", outboxBefore, after)
	}
}

func outboxRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	return n
}

// call sends one skill request over the JSON-RPC wire and returns the decoded
// envelope.
func call(t *testing.T, ctx context.Context, client *a2aclient.Client, skill string, args map[string]any) map[string]any {
	t.Helper()
	resp, err := client.SendMessage(ctx, &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))})
	if err != nil {
		t.Fatalf("%s: %v", skill, err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) == 0 {
		t.Fatalf("%s: reply = %T", skill, resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("%s: first part is %T", skill, msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env
}
