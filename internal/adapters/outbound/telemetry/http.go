package telemetry

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPMiddleware wraps a stdlib ServeMux (or anything that sets
// http.Request.Pattern) with the fleet's Tier-1 HTTP RED instrumentation
// (standard-metrics convention, ADR 0011): a server span per request plus
// the http.server.request.duration histogram (Prometheus:
// http_server_request_duration_seconds_{count,sum,bucket}) and the request/
// response body-size histograms.
//
// The route label is the mux PATTERN ("GET /v1/transfers/{id}" is reported
// as http.route "/v1/transfers/{id}"), never the raw URL path, so label
// cardinality is bounded by the number of registered routes. An unmatched
// request (404 from the mux) carries no http.route at all.
//
// It binds to otel.GetMeterProvider() at construction time, so it must be
// built AFTER telemetry.Setup in a composition root. Before Setup (or in
// tests that never call it) the global provider is a no-op and the wrapper
// adds no observable behaviour: responses are passed through untouched.
func HTTPMiddleware(serviceName string, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, serviceName,
		otelhttp.WithSpanNameFormatter(spanName),
	)
}

// spanName names a span by the matched mux pattern, falling back to the
// bare method (never the raw path) when nothing matched.
func spanName(_ string, r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return r.Method
}
