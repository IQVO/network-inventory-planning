package main

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	inboundhttp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
)

// newRouter wraps the MCP handler so the binary is deployable behind
// Kubernetes probes:
//
//   - GET /healthz  -> 200 {"status":"ok"} (open; liveness/readiness probes).
//   - /  and /mcp   -> the MCP Streamable HTTP handler. Both paths are served
//     so a "<host>/mcp" endpoint convention and a root-mounted endpoint both
//     work.
//
// Every request carries the fleet's Tier-1 HTTP RED middleware stack
// (ADR-0013): request ID, otelchi tracing, otelchi's RED duration metric,
// the request logger, then panic recovery -- the same order as the REST
// API's Routes(). otelchi.Middleware and otelchimetric.NewServerRequestDuration
// resolve the global MeterProvider at construction, so the caller must
// install it (telemetry.Setup) before calling newRouter.
//
// No auth middleware is ever added here (TestNoAuthMiddlewareReintroduced).
func newRouter(mcpHandler http.Handler, serviceName string) http.Handler {
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
	r.Handle("/mcp", mcpHandler)
	r.Handle("/mcp/*", mcpHandler)
	// "/*" also matches "/" itself in chi.
	r.Handle("/*", mcpHandler)
	return r
}
