package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var encNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func TestTransferEncoderEncodesPlanApproved(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-1" }}
	msgs, err := e.Encode(context.Background(), transfer.PlanApproved{
		TransferID:        transfer.TransferID("trf-1"),
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "rebalance",
		ProposalAsOf:      encNow.Add(-5 * time.Minute),
		OccurredAt:        encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	if string(m.Key) != "trf-1" {
		t.Fatalf("key = %q, want the transfer id", m.Key)
	}
	if m.EventType != "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved" {
		t.Fatalf("event type = %q", m.EventType)
	}

	var envelope struct {
		SpecVersion string         `json:"specversion"`
		ID          string         `json:"id"`
		Source      string         `json:"source"`
		Type        string         `json:"type"`
		Subject     string         `json:"subject"`
		Time        string         `json:"time"`
		DataSchema  string         `json:"dataschema"`
		Data        map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	assertPlanApprovedEnvelope(t, envelope, m.EventType)
	if len(m.Headers) == 0 || m.Headers[0].Key != "content-type" || !strings.Contains(string(m.Headers[0].Value), "application/cloudevents+json") {
		t.Fatalf("headers = %+v", m.Headers)
	}
}

func assertPlanApprovedEnvelope(t *testing.T, envelope struct {
	SpecVersion string         `json:"specversion"`
	ID          string         `json:"id"`
	Source      string         `json:"source"`
	Type        string         `json:"type"`
	Subject     string         `json:"subject"`
	Time        string         `json:"time"`
	DataSchema  string         `json:"dataschema"`
	Data        map[string]any `json:"data"`
}, wantType string) {
	t.Helper()
	if envelope.SpecVersion != "1.0" || envelope.ID != "ce-id-1" {
		t.Fatalf("envelope = %+v", envelope)
	}
	if envelope.Source != "/warehouse/network-inventory-planning" {
		t.Fatalf("source = %q", envelope.Source)
	}
	if envelope.Type != wantType {
		t.Fatalf("envelope type = %q, want %q", envelope.Type, wantType)
	}
	if envelope.Subject != "trf-1" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
	if envelope.DataSchema != "urn:warehouse:network-inventory-planning:events:TransferPlanApproved:v1" {
		t.Fatalf("dataschema = %q", envelope.DataSchema)
	}
	if envelope.Data["transfer_id"] != "trf-1" || envelope.Data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", envelope.Data)
	}
	if envelope.Data["destination_site_id"] != "WH2" || envelope.Data["policy_version"] != "policy-v3" {
		t.Fatalf("data = %+v", envelope.Data)
	}
}

func TestTransferEncoderEncodesAllocationRequested(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-2" }}
	msgs, err := e.Encode(context.Background(), transfer.AllocationRequested{
		TransferID:     transfer.TransferID("trf-1"),
		TransferLineID: "trf-1:1",
		OriginSiteID:   "WH1",
		SKU:            "SKU-1",
		Quantity:       10,
		OccurredAt:     encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	// inventory-storage keys and subjects the command on transfer_line_id.
	if string(m.Key) != "trf-1:1" {
		t.Fatalf("key = %q, want transfer_line_id", m.Key)
	}
	if m.EventType != "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested" {
		t.Fatalf("event type = %q", m.EventType)
	}

	var envelope struct {
		Type    string         `json:"type"`
		Subject string         `json:"subject"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	// EXACTLY inventory-storage's consumed contract: five fields, no more.
	if len(envelope.Data) != 5 {
		t.Fatalf("data fields = %d (%+v), want exactly 5", len(envelope.Data), envelope.Data)
	}
	if envelope.Data["transfer_id"] != "trf-1" ||
		envelope.Data["transfer_line_id"] != "trf-1:1" ||
		envelope.Data["origin_site_id"] != "WH1" ||
		envelope.Data["sku"] != "SKU-1" ||
		envelope.Data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", envelope.Data)
	}
	if envelope.Subject != "trf-1:1" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
}

func TestTransferEncoderSkipsUnknownEvent(t *testing.T) {
	e := NewTransferEncoder()
	msgs, err := e.Encode(context.Background(), fakeDomainEvent{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("messages = %d, want 0 (unknown event skipped)", len(msgs))
	}
}

func TestTransferEncoderEncodesWorkDemandReleased(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-3" }}
	msgs, err := e.Encode(context.Background(), transfer.DemandReleased{
		DemandID:    "trf-1:pick",
		WorkKind:    transfer.WorkKindTransferPick,
		TransferRef: transfer.TransferID("trf-1"),
		PathID:      "transfer-pick-path",
		SiteID:      "WH1",
		CPT:         encNow.Add(2 * time.Hour),
		SKU:         "SKU-1",
		Quantity:    10,
		OccurredAt:  encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	// WES's consumed contract: type names the workdemand entity, key and
	// subject are the demand_id.
	if m.EventType != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" {
		t.Fatalf("event type = %q", m.EventType)
	}
	if string(m.Key) != "trf-1:pick" {
		t.Fatalf("key = %q, want demand_id", m.Key)
	}

	var envelope struct {
		Type       string         `json:"type"`
		Subject    string         `json:"subject"`
		DataSchema string         `json:"dataschema"`
		Data       map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	assertWorkDemandEnvelope(t, envelope)
	assertWorkDemandData(t, envelope.Data)
}

func assertWorkDemandEnvelope(t *testing.T, envelope struct {
	Type       string         `json:"type"`
	Subject    string         `json:"subject"`
	DataSchema string         `json:"dataschema"`
	Data       map[string]any `json:"data"`
}) {
	t.Helper()
	if envelope.Type != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" {
		t.Fatalf("type = %q", envelope.Type)
	}
	if envelope.Subject != "trf-1:pick" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
	if envelope.DataSchema != "urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1" {
		t.Fatalf("dataschema = %q", envelope.DataSchema)
	}
}

// assertWorkDemandData proves the payload is EXACTLY WES's consumed
// contract: the six required fields plus sku and quantity, no more.
func assertWorkDemandData(t *testing.T, data map[string]any) {
	t.Helper()
	if len(data) != 8 {
		t.Fatalf("data fields = %d (%+v), want exactly 8", len(data), data)
	}
	if data["demand_id"] != "trf-1:pick" ||
		data["work_kind"] != "TRANSFER_PICK" ||
		data["transfer_ref"] != "trf-1" ||
		data["path_id"] != "transfer-pick-path" ||
		data["site_id"] != "WH1" ||
		data["cpt"] != encNow.Add(2*time.Hour).Format(time.RFC3339) ||
		data["sku"] != "SKU-1" ||
		data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", data)
	}
}

type fakeDomainEvent struct{}

func (fakeDomainEvent) EventName() string { return "SomethingElse" }

// --- OTel trace propagation (ADR 0007) ---------------------------------------
//
// The golden rule for these tests: assert header PRESENCE, never the
// trace-id/span-id VALUES. The ids differ per run and per sampler; what
// the contract guarantees is that a span-carrying ctx yields a
// traceparent header a consumer can Extract, and that a span-less ctx
// yields none (never an invalid all-zero one).

// installPropagator points the global propagator at the W3C TraceContext
// propagator for the duration of one test, restoring whatever was there.
func installPropagator(t *testing.T) {
	t.Helper()
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
}

// spanContextOn returns a ctx carrying a fixed, valid, sampled span
// context (Remote, like a propagated inbound one), without needing a
// TracerProvider: propagation only needs the SpanContext on ctx.
func spanContextOn(ctx context.Context) context.Context {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

// hasHeader reports whether headers carries key (value ignored).
func hasHeader(headers []kafkago.Header, key string) bool {
	for _, h := range headers {
		if h.Key == key {
			return true
		}
	}
	return false
}

func TestTransferEncoderInjectsTraceparentWhenSpanActive(t *testing.T) {
	installPropagator(t)
	e := NewTransferEncoder()
	msgs, err := e.Encode(spanContextOn(context.Background()), transfer.PlanApproved{
		TransferID:        transfer.TransferID("trf-1"),
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "policy-v3",
		OccurredAt:        encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	if !hasHeader(msgs[0].Headers, "traceparent") {
		t.Fatalf("headers = %+v, want a traceparent header (value is run-specific: presence only)", msgs[0].Headers)
	}
	if !hasHeader(msgs[0].Headers, "content-type") {
		t.Fatalf("headers = %+v, want the content-type header alongside the trace header", msgs[0].Headers)
	}
}

func TestTransferEncoderOmitsTraceparentWithoutASpan(t *testing.T) {
	installPropagator(t)
	e := NewTransferEncoder()
	msgs, err := e.Encode(context.Background(), transfer.PlanApproved{
		TransferID:        transfer.TransferID("trf-1"),
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "policy-v3",
		OccurredAt:        encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	if hasHeader(msgs[0].Headers, "traceparent") {
		t.Fatalf("headers = %+v, want NO traceparent without a live span (an all-zero one would poison a consumer)", msgs[0].Headers)
	}
}

// TestRelaySinkSendsHeadersUntouched proves the relay forwards the
// persisted headers verbatim: whatever trace context the Encode-time span
// injected survives the outbox round-trip (JSON in, JSON out) and lands on
// the broker. This is the presence-contract half of ADR 0007 slice A.
func TestRelaySinkSendsHeadersUntouched(t *testing.T) {
	installPropagator(t)
	var (
		mu   sync.Mutex
		sent []kafkago.Message
	)
	sink := RelaySink{send: func(ctx context.Context, msg kafkago.Message) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, msg)
		return nil
	}}

	headers := tracedHeaders(spanContextOn(context.Background()))
	enc := Encoded{Topic: TransferTopic, EventType: "t", Key: []byte("k"), Value: []byte("v"), Headers: headers}
	if err := sink.Send(context.Background(), enc); err != nil {
		t.Fatalf("send: %v", err)
	}

	// The outbox persists headers as JSON and rebuilds them on claim —
	// simulate that round-trip and prove the traceparent survives.
	roundTripped := roundTripHeaders(t, sent[0].Headers)
	if !hasHeader(roundTripped, "traceparent") {
		t.Fatalf("headers after an outbox JSON round-trip = %+v, want the traceparent present", roundTripped)
	}
}

// roundTripHeaders marshals headers to JSON and back, exactly the shape
// postgres.encodeOutboxHeaders/decodeOutboxHeaders impose.
func roundTripHeaders(t *testing.T, headers []kafkago.Header) []kafkago.Header {
	t.Helper()
	type stored struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}
	out := make([]stored, 0, len(headers))
	for _, h := range headers {
		out = append(out, stored{Key: h.Key, Value: h.Value})
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back []stored
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	final := make([]kafkago.Header, 0, len(back))
	for _, h := range back {
		final = append(final, kafkago.Header{Key: h.Key, Value: h.Value})
	}
	return final
}
