//go:build integration

// D27 (trace continuity across the outbox): the W3C trace context of the
// transaction that wrote an outbox row crosses the outbox to Kafka. NIP
// already stores it (outbox_events.headers, migration 0002; tracedHeaders at
// Encode, publisher.go; RelaySink forwards the headers verbatim, ADR 0007), so
// these tests pin that behaviour end to end. Real Postgres and a real Kafka
// broker, both via testcontainers (startPostgres / startKafkaBroker).
package kafka_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// The broker comes from testcontainers (startKafkaBroker); the import pins
// the fleet rule TestKafkaIntegrationTestsUseTestcontainers checks.
var _ = tckafka.Run

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// traceOutboxDB starts a real Postgres, migrates it and returns a pool.
func traceOutboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// readByKey reads topic from the beginning (fresh group) until it finds the
// message with key: the shared broker's topics also carry other tests' rows.
func readByKey(t *testing.T, ctx context.Context, brokers []string, topic, key string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("outbox-trace"),
		StartOffset: kafkago.FirstOffset, MinBytes: 1, MaxBytes: 10e6, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("read %s for key %q: %v", topic, key, err)
		}
		if string(msg.Key) == key {
			return msg
		}
	}
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// storedTraceparent returns the traceparent stored in outbox_events.headers
// of the only row for eventType ("" when the row has none).
func storedTraceparent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType string) string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_events WHERE event_type = $1`, eventType).Scan(&raw); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	// The stored shape is outbox.go's outboxHeader: {"key": string, "value": bytes}.
	var stored []struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("decode stored headers %s: %v", raw, err)
	}
	for _, h := range stored {
		if h.Key == "traceparent" {
			return string(h.Value)
		}
	}
	return ""
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case publishes
// inside a request span; the outbox row stores that span's traceparent; the
// relay (a later pass with no span of its own) sends it as a Kafka header;
// and a consumer extracting it with the global propagator lands on the
// request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	pool := traceOutboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startKafkaBroker(t)
	ensureTopic(t, brokers, outboundkafka.TransferTopic)

	transferID := fmt.Sprintf("trf-trace-%d", time.Now().UnixNano())
	outbox := postgres.NewOutboxWriter(pool, outboundkafka.NewTransferEncoder())
	reqCtx, reqSpan := tracer.Start(ctx, "POST /v1/transfers:approve", trace.WithSpanKind(trace.SpanKindServer))
	if err := outbox.Publish(reqCtx, transfer.PlanApproved{
		TransferID: transfer.TransferID(transferID), OriginSiteID: "WH1", DestinationSiteID: "WH2",
		SKU: "SKU-1", Quantity: 5, PolicyVersion: "policy-v3", OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	const approved = "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved"
	if stored := storedTraceparent(t, ctx, pool, approved); stored != wantTraceparent {
		t.Fatalf("outbox row traceparent = %q, want the request span's %q", stored, wantTraceparent)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, postgres.WithMaxAttempts(postgres.DefaultOutboxMaxAttempts))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readByKey(t, ctx, brokers, outboundkafka.TransferTopic, transferID)
	got, ok := headerValue(msg.Headers, "traceparent")
	if !ok || got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}

	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half:
// a row written with no trace context (no active span, or a row that predates
// trace capture) relays with exactly its stored key, value and headers, and
// carries no traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	pool := traceOutboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startKafkaBroker(t)
	topic := fmt.Sprintf("warehouse.network-inventory-planning.itest-no-trace-%d", time.Now().UnixNano())
	ensureTopic(t, brokers, topic)

	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, key, value)
		VALUES ($1, 'com.example.Untraced', 'transfer-untraced', '{}')
	`, topic); err != nil {
		t.Fatalf("insert untraced row: %v", err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, postgres.WithMaxAttempts(postgres.DefaultOutboxMaxAttempts))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readByKey(t, ctx, brokers, topic, "transfer-untraced")
	if string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: value=%q", msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}

// kafkaHeaderCarrier adapts kafka-go headers to the OTel text-map carrier,
// as the inbound consumers do (readOnlyHeaderCarrier in
// internal/adapters/inbound/kafka).
type kafkaHeaderCarrier []kafkago.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	v, _ := headerValue(c, key)
	return v
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
