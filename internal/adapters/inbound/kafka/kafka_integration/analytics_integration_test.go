//go:build integration

package kafka_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundhttp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// The fleet fitness test requires every Kafka-touching integration test to
// start its broker via testcontainers-go/modules/kafka (startKafkaBroker in
// planning_read_models_integration_test.go does exactly that).
var _ = tckafka.Run

func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "..", "analytics", "migrations")
}

// encodeAnalytics runs the REAL publisher-side encoder (ADR 0007), so the test
// proves the projector reads exactly what NIP writes.
func encodeAnalytics(t *testing.T, ev transfer.DomainEvent) kafkago.Message {
	t.Helper()
	out, err := outboundkafka.NewAnalyticsEncoder().Encode(context.Background(), ev)
	if err != nil || len(out) != 1 {
		t.Fatalf("encode %T: %d messages, %v", ev, len(out), err)
	}
	return kafkago.Message{Key: out[0].Key, Value: out[0].Value}
}

func rawCloudEvent(t *testing.T, eventName, subject string, data any) kafkago.Message {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: fmt.Sprintf("raw-%s-%d", eventName, time.Now().UnixNano()), Entity: "saga", EventName: eventName,
		Subject: subject, Time: time.Date(2026, 10, 5, 9, 30, 0, 0, time.UTC),
		Stream: cloudevents.StreamAnalytics, Version: 1, Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafkago.Message{Key: []byte(subject), Value: value}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec,noctx // httptest server on loopback
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s = %d %s", url, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("GET %s: %v (%s)", url, err, body)
	}
}

// TestAnalyticsProjectorAndReportsEndToEnd runs the publisher-side encoder,
// a real Kafka broker, the projector's consumer and the reports handler over
// a real analytical Postgres: duplicates, unknown types, garbage and poison
// all travel the same topic.
func TestAnalyticsProjectorAndReportsEndToEnd(t *testing.T) {
	brokers := startKafkaBroker(t)
	analyticsURL := startPostgres(t)
	if err := postgres.RunMigrations(analyticsURL, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("analytics migrations: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ensureTopic(t, brokers, outboundkafka.AnalyticsTopic)

	day := func(h, m int) time.Time { return time.Date(2026, 10, 5, h, m, 0, 0, time.UTC) }
	dwell45 := int64(45) // seconds in PROPOSED; the saga's age at the transition (60) is NOT the dwell
	advanced := encodeAnalytics(t, transfer.StateAdvanced{TransferID: "t1", From: "PROPOSED", To: "APPROVED", AgeSeconds: 60, DwellSeconds: &dwell45, OccurredAt: day(8, 1)})
	// An event published before dwell_seconds existed (age 500): counted in
	// without_dwell, excluded from the percentiles, never a zero or its age.
	legacy := rawCloudEvent(t, "TransferStateAdvanced", "t3",
		map[string]any{"transfer_id": "t3", "from": "PROPOSED", "to": "APPROVED", "age_seconds": 500})
	poison := rawCloudEvent(t, "TransferStateAdvanced", "other-subject",
		map[string]any{"transfer_id": "t9", "from": "A", "to": "X", "age_seconds": 1})
	messages := []kafkago.Message{
		encodeAnalytics(t, transfer.StateAdvanced{TransferID: "t1", To: "PROPOSED", AgeSeconds: 0, OccurredAt: day(8, 0)}),
		advanced,
		advanced, // the same CloudEvents id delivered twice
		legacy,
		{Key: []byte("junk"), Value: []byte("this is not a CloudEvent")},
		rawCloudEvent(t, "TransferTeleported", "t1", map[string]any{"transfer_id": "t1"}),
		poison,
		encodeAnalytics(t, transfer.StuckDetected{TransferID: "t1", State: "APPROVED", AgeSeconds: 4000, ThresholdSeconds: 3600, OccurredAt: day(9, 0)}),
		encodeAnalytics(t, transfer.RebalanceRunCompleted{RunID: "run-1", ProposalCount: 5, RejectedCount: 1, StaleFacts: 0, CompletedAt: day(10, 0)}),
		// The marker: once it is projected, everything before it on the
		// (single-partition) topic, duplicate and poison included, was consumed.
		encodeAnalytics(t, transfer.StateAdvanced{TransferID: "t2", To: "PROPOSED", AgeSeconds: 0, OccurredAt: day(11, 0)}),
	}
	writer := &kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Topic: outboundkafka.AnalyticsTopic, Balancer: &kafkago.Hash{},
		BatchTimeout: 10 * time.Millisecond, RequiredAcks: kafkago.RequireAll,
		// Not DefaultTransport: its process-wide metadata cache can hide a topic created a moment ago.
		Transport: &kafkago.Transport{},
	}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		t.Fatalf("produce: %v", err)
	}

	writePool, err := analyticsstore.NewPool(ctx, analyticsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer writePool.Close()
	consumer := inboundkafka.NewAnalyticsConsumer(brokers, outboundkafka.AnalyticsTopic, uniqueGroupID("nip-analytics"),
		analyticsstore.NewProjection(writePool), slog.New(slog.NewTextHandler(io.Discard, nil)))
	runCtx, stopConsumer := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	defer func() {
		stopConsumer()
		<-done
		_ = consumer.Close()
	}()

	// Six distinct valid events are projected; the duplicate, junk, unknown
	// type and poison add nothing.
	waitForProcessed(ctx, t, writePool, 6)

	readPool, err := analyticsstore.NewReadOnlyPool(ctx, analyticsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer readPool.Close()
	srv := httptest.NewServer((&inboundhttp.ReportsServer{Reader: analyticsstore.NewReader(readPool)}).Routes())
	defer srv.Close()
	rng := "?from=2026-10-05T00:00:00Z&to=2026-10-06T00:00:00Z"

	var funnel struct {
		Days []struct {
			Day, State string
			Transfers  int
		}
	}
	getJSON(t, srv.URL+"/reports/transfer-funnel"+rng, &funnel)
	if fmt.Sprint(funnel.Days) != "[{2026-10-05 APPROVED 2} {2026-10-05 PROPOSED 2}]" {
		t.Errorf("funnel = %+v; the duplicate must count once, t1 and t3 reach APPROVED, t1 and t2 reach PROPOSED", funnel.Days)
	}
	var dwell struct {
		Days []struct {
			State        string
			Transitions  int
			WithoutDwell int      `json:"without_dwell"`
			P50Dwell     *float64 `json:"p50_dwell_seconds"`
			P95Dwell     *float64 `json:"p95_dwell_seconds"`
		}
	}
	getJSON(t, srv.URL+"/reports/state-dwell"+rng, &dwell)
	if len(dwell.Days) != 1 || dwell.Days[0].State != "PROPOSED" || dwell.Days[0].Transitions != 2 || dwell.Days[0].WithoutDwell != 1 ||
		dwell.Days[0].P50Dwell == nil || *dwell.Days[0].P50Dwell != 45 || dwell.Days[0].P95Dwell == nil || *dwell.Days[0].P95Dwell != 45 {
		t.Errorf("dwell = %+v; want PROPOSED, 2 transitions, 1 without dwell, p50=p95=45 (the real dwell, not age 60 or 500)", dwell.Days)
	}
	var stuck struct {
		Days []struct {
			State      string
			Detections int
		}
		Latest []struct {
			TransferID string `json:"transfer_id"`
			State      string
		}
	}
	getJSON(t, srv.URL+"/reports/stuck-transfers"+rng, &stuck)
	if len(stuck.Days) != 1 || stuck.Days[0].Detections != 1 || len(stuck.Latest) != 1 || stuck.Latest[0].TransferID != "t1" {
		t.Errorf("stuck = %+v", stuck)
	}
	var runs struct {
		Days []struct {
			Runs, Proposals, Rejected int
			RejectionRate             float64 `json:"rejection_rate"`
		}
	}
	getJSON(t, srv.URL+"/reports/rebalance-runs"+rng, &runs)
	if len(runs.Days) != 1 || runs.Days[0].Runs != 1 || runs.Days[0].Proposals != 5 || runs.Days[0].Rejected != 1 || runs.Days[0].RejectionRate != 0.2 {
		t.Errorf("runs = %+v", runs.Days)
	}
	var fresh struct {
		AsOf       time.Time `json:"as_of"`
		LagSeconds float64   `json:"lag_seconds"`
	}
	getJSON(t, srv.URL+"/reports/freshness", &fresh)
	if !fresh.AsOf.Equal(day(11, 0)) || fresh.LagSeconds <= 0 {
		t.Errorf("freshness = %+v", fresh)
	}

	assertDeadLettered(ctx, t, brokers, poison)
}

// waitForProcessed polls analytics_processed_events until it holds want rows
// (and asserts it never exceeds it).
func waitForProcessed(ctx context.Context, t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM analytics_processed_events`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == want {
			return
		}
		if n > want || time.Now().After(deadline) {
			t.Fatalf("processed events = %d, want exactly %d", n, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// assertDeadLettered reads the DLQ topic and requires the poison message,
// byte for byte, with its error headers.
func assertDeadLettered(ctx context.Context, t *testing.T, brokers []string, poison kafkago.Message) {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: outboundkafka.AnalyticsTopic + inboundkafka.DLQSuffix,
		GroupID: uniqueGroupID("dlq-read"), MinBytes: 1, MaxBytes: 10e6,
	})
	defer func() { _ = reader.Close() }()
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	msg, err := reader.FetchMessage(readCtx)
	if err != nil {
		t.Fatalf("nothing on the DLQ topic: %v", err)
	}
	if string(msg.Value) != string(poison.Value) {
		t.Fatalf("DLQ value differs from the poison message:\n got  %s\n want %s", msg.Value, poison.Value)
	}
	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["x-dlq-source-topic"] != outboundkafka.AnalyticsTopic || headers["x-dlq-error"] == "" {
		t.Fatalf("DLQ headers = %v", headers)
	}
}
