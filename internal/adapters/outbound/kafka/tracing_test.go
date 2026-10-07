package kafka

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestHeaderCarrier_GetSetKeys(t *testing.T) {
	headers := []kafkago.Header{{Key: "existing", Value: []byte("value")}}
	carrier := headerCarrier{headers: &headers}

	if got := carrier.Get("existing"); got != "value" {
		t.Errorf("Get(existing) = %q, want value", got)
	}
	if got := carrier.Get("absent"); got != "" {
		t.Errorf("Get(absent) = %q, want empty string", got)
	}

	carrier.Set("traceparent", "00-abc-def-01")
	if got := carrier.Get("traceparent"); got != "00-abc-def-01" {
		t.Errorf("Get after Set = %q, want the value just set", got)
	}
	if len(headers) != 2 {
		t.Errorf("Set on a new key produced %d headers, want 2", len(headers))
	}

	// Re-setting must replace, not append: two traceparents on one message
	// would leave a consumer guessing which parent is real.
	carrier.Set("traceparent", "00-abc-999-01")
	if len(headers) != 2 {
		t.Errorf("Set on an existing key produced %d headers, want 2", len(headers))
	}
	if got := carrier.Get("traceparent"); got != "00-abc-999-01" {
		t.Errorf("Get after re-Set = %q, want the replacement value", got)
	}

	keys := carrier.Keys()
	if len(keys) != 2 || keys[0] != "existing" || keys[1] != "traceparent" {
		t.Errorf("Keys() = %v, want [existing traceparent]", keys)
	}
}

// TestHeaderCarrier_RoundTripsTraceContext is the cross-process contract
// in miniature: what this producer injects, a consumer in another repo
// extracts with the same propagator, landing on the same trace.
func TestHeaderCarrier_RoundTripsTraceContext(t *testing.T) {
	propagator := propagation.TraceContext{}

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	want := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	var headers []kafkago.Header
	producerCtx := trace.ContextWithSpanContext(context.Background(), want)
	propagator.Inject(producerCtx, headerCarrier{headers: &headers})

	if len(headers) == 0 {
		t.Fatal("Inject wrote no headers: the carrier is not being written to")
	}

	consumerCtx := propagator.Extract(context.Background(), headerCarrier{headers: &headers})
	got := trace.SpanContextFromContext(consumerCtx)

	if !got.Equal(want) {
		t.Errorf("extracted span context = %+v, want %+v", got, want)
	}
}
