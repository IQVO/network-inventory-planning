package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newTestServer wires the binary exactly as run() does, over the empty
// (no DATABASE_URL) dependencies, behind the real router.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	deps, closeFn, err := buildDeps(context.Background(), quietLogger(), "", "", defaultMigrationsPath, defaultMaxStaleness)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeFn)
	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))))
	t.Cleanup(srv.Close)
	return srv
}

func TestRouter_HealthzIsOpenAndServesOK(t *testing.T) {
	srv := newTestServer(t)
	// No Authorization header: /healthz is open.
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %q, want 200 {\"status\":\"ok\"}", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// Both mount points speak Streamable HTTP, with no credentials, and a real
// client can list the four tools and call one end to end.
func TestStreamableHTTP_RootAndMCPPathsServeTools(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/mcp"} {
		t.Run(path, func(t *testing.T) {
			client := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil)
			session, err := client.Connect(context.Background(), &sdk.StreamableClientTransport{Endpoint: srv.URL + path}, nil)
			if err != nil {
				t.Fatalf("connect %s: %v", path, err)
			}
			defer session.Close()

			tools, err := session.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatalf("list tools: %v", err)
			}
			if len(tools.Tools) != 4 {
				t.Fatalf("tools = %d, want 4", len(tools.Tools))
			}

			// No store configured: a tool answers an isError result, not a
			// transport failure.
			res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
				Name: "list_transfers", Arguments: map[string]any{},
			})
			if err != nil || !res.IsError {
				t.Fatalf("list_transfers without a store: err=%v res=%+v, want an isError result", err, res)
			}
		})
	}
}

// The binary reads no API key: requests carrying (or lacking) any
// Authorization header behave identically.
func TestNoAuthIsRequired(t *testing.T) {
	srv := newTestServer(t)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`
	for _, auth := range []string{"", "Bearer nonsense"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("initialize with Authorization=%q = %d, want 200", auth, resp.StatusCode)
		}
	}
}

func TestBuildDepsWithoutDatabaseURLWiresNothing(t *testing.T) {
	deps, closeFn, err := buildDeps(context.Background(), quietLogger(), "", "", defaultMigrationsPath, defaultMaxStaleness)
	if err != nil {
		t.Fatal(err)
	}
	closeFn()
	if deps.GetTransfer != nil || deps.ListTransfers != nil || deps.FindStuckTransfers != nil || deps.Simulate != nil {
		t.Fatalf("no DATABASE_URL must wire no use cases: %+v", deps)
	}
}

func TestMaxStalenessFromEnv(t *testing.T) {
	t.Setenv(envMaxStaleness, "")
	if got, err := maxStalenessFromEnv(); err != nil || got != defaultMaxStaleness {
		t.Fatalf("default = %v, %v", got, err)
	}
	t.Setenv(envMaxStaleness, "90s")
	if got, err := maxStalenessFromEnv(); err != nil || got != 90*time.Second {
		t.Fatalf("90s = %v, %v", got, err)
	}
	for _, bad := range []string{"soon", "0s", "-5m"} {
		t.Setenv(envMaxStaleness, bad)
		if _, err := maxStalenessFromEnv(); err == nil {
			t.Fatalf("%q must be refused", bad)
		}
	}
}

func TestGetenv(t *testing.T) {
	t.Setenv("NIP_MCP_TEST_SET", "v")
	t.Setenv("NIP_MCP_TEST_EMPTY", "")
	if got := getenv("NIP_MCP_TEST_SET", "d"); got != "v" {
		t.Errorf("set: %q", got)
	}
	if got := getenv("NIP_MCP_TEST_EMPTY", "d"); got != "d" {
		t.Errorf("empty: %q", got)
	}
}

func TestNewLoggerLevels(t *testing.T) {
	for level, want := range map[string]slog.Level{"debug": slog.LevelDebug, "WARN": slog.LevelWarn, "warning": slog.LevelWarn, "error": slog.LevelError, "": slog.LevelInfo, "bogus": slog.LevelInfo} {
		if got := newLogger(level); !got.Handler().Enabled(context.Background(), want) || (want > slog.LevelDebug && got.Handler().Enabled(context.Background(), want-1)) {
			t.Errorf("newLogger(%q): level %v not the minimum enabled", level, want)
		}
	}
}
