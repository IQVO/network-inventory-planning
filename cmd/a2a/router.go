package main

import (
	"log/slog"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	inboundhttp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
)

// newRouter wraps the A2A handler so the binary is deployable behind
// Kubernetes probes:
//
//   - GET /healthz -> 200 {"status":"ok"} (open; liveness/readiness probes).
//     A2A is unauthenticated by decision, so probes need no credentials, and
//     the route leaks no skill, resource or state.
//   - GET /.well-known/agent-card.json -> the Agent Card (the discovery
//     contract, ADR-0020).
//   - The JSON-RPC endpoint is mounted at BOTH "/" (the canonical root) and
//     "/a2a" (the fleet's <CTX>_A2A_ENDPOINT convention, mirroring cmd/mcp's
//     dual "/mcp" mount).
//
// Every request carries the fleet's Tier-1 HTTP RED middleware stack
// (ADR-0013): request ID, otelchi tracing, otelchi's RED duration metric,
// the request logger, then panic recovery -- the same order as the REST
// API's Routes(). otelchi.Middleware and otelchimetric.NewServerRequestDuration
// resolve the global MeterProvider at construction, so the caller must
// install it (telemetry.Setup) before calling newRouter.
//
// No auth middleware is ever added here (TestNoAuthMiddlewareReintroduced).
func newRouter(a2aHandler http.Handler, card *a2a.AgentCard, serviceName string) http.Handler {
	r := chi.NewRouter()
	metricCfg := otelchimetric.NewBaseConfig(serviceName)

	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(inboundhttp.RequestLogger(slog.Default()))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	r.Handle("/a2a", a2aHandler)
	r.Handle("/", a2aHandler)
	return r
}
