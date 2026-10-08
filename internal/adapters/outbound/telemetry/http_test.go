package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// withManualMeterProvider installs a ManualReader-backed global
// MeterProvider for the test. HTTPMiddleware binds to the global provider
// at construction, so this must run BEFORE the middleware is built.
func withManualMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

func testMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /v1/transfers/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	return mux
}

// durationPoints returns the http.server.request.duration histogram data
// points, failing if the metric was not emitted.
func durationPoints(t *testing.T, reader sdkmetric.Reader) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	names := []string{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			names = append(names, md.Name)
			if md.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := md.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("http.server.request.duration is %T, want Histogram[float64]", md.Data)
			}
			return hist.DataPoints
		}
	}
	t.Fatalf("http.server.request.duration not emitted; got %v", names)
	return nil
}

func attrString(set attribute.Set, key string) (string, bool) {
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		return "", false
	}
	return v.String(), true
}

// TestHTTPMiddlewareEmitsRouteLabelledDuration proves the RED duration
// histogram exists and that its route label is the mux PATTERN (bounded),
// not the raw path carrying the transfer id (unbounded).
func TestHTTPMiddlewareEmitsRouteLabelledDuration(t *testing.T) {
	reader := withManualMeterProvider(t)
	h := HTTPMiddleware(ServiceAPI, testMux())

	for _, id := range []string{"trf-aaa", "trf-bbb", "trf-ccc"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/transfers/"+id, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != `{"ok":true}` {
			t.Fatalf("middleware changed the response: %d %q", rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("GET /healthz = %d, want 204", rec.Code)
	}

	points := durationPoints(t, reader)
	byRoute := map[string]uint64{}
	for _, p := range points {
		route, ok := attrString(p.Attributes, "http.route")
		if !ok {
			t.Fatalf("data point without http.route: %v", p.Attributes)
		}
		if route == "/v1/transfers/trf-aaa" || route == "/v1/transfers/trf-bbb" || route == "/v1/transfers/trf-ccc" {
			t.Fatalf("route label is the raw path %q: unbounded cardinality", route)
		}
		byRoute[route] += p.Count
	}
	if byRoute["/v1/transfers/{id}"] != 3 {
		t.Fatalf("routes = %v, want /v1/transfers/{id} counted 3 times", byRoute)
	}
	if byRoute["/healthz"] != 1 {
		t.Fatalf("routes = %v, want /healthz counted once", byRoute)
	}
}

// TestHTTPMiddlewareUnmatchedPathHasNoRawPathLabel: a 404 from the mux has
// no pattern, so it must carry no http.route and certainly no raw path.
func TestHTTPMiddlewareUnmatchedPathHasNoRawPathLabel(t *testing.T) {
	reader := withManualMeterProvider(t)
	h := HTTPMiddleware(ServiceAPI, testMux())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/no/such/route/12345", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unmatched = %d, want 404", rec.Code)
	}
	for _, p := range durationPoints(t, reader) {
		if route, ok := attrString(p.Attributes, "http.route"); ok {
			t.Fatalf("unmatched request carries http.route %q", route)
		}
		for _, kv := range p.Attributes.ToSlice() {
			if kv.Value.String() == "/no/such/route/12345" {
				t.Fatalf("raw path leaked into label %s", kv.Key)
			}
		}
		if code, _ := attrString(p.Attributes, "http.response.status_code"); code != "404" {
			t.Fatalf("status label = %q, want 404", code)
		}
	}
}
