package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// fakeCapabilities implements ports.SiteCapabilityRepository.
type fakeCapabilities struct {
	mu       sync.Mutex
	upserts  int
	failNext int
	rows     map[string]planning.SiteCapability
}

func newFakeCapabilities() *fakeCapabilities {
	return &fakeCapabilities{rows: map[string]planning.SiteCapability{}}
}

func (f *fakeCapabilities) Upsert(ctx context.Context, c planning.SiteCapability) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return false, fmt.Errorf("injected failure")
	}
	f.upserts++
	f.rows[c.Site] = c
	return true, nil
}

// fakeDemands implements ports.SiteSkuDemandRepository.
type fakeDemands struct {
	mu       sync.Mutex
	failNext int
	rows     map[string]planning.SiteSkuDemand
}

func newFakeDemands() *fakeDemands {
	return &fakeDemands{rows: map[string]planning.SiteSkuDemand{}}
}

func (f *fakeDemands) Upsert(ctx context.Context, d planning.SiteSkuDemand) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return false, fmt.Errorf("injected failure")
	}
	f.rows[d.SourceKey()] = d
	return true, nil
}

// fakePlans implements ports.PublishedCapacityPlanRepository.
type fakePlans struct {
	mu       sync.Mutex
	failNext int
	rows     map[string]planning.PublishedCapacityPlan
}

func newFakePlans() *fakePlans {
	return &fakePlans{rows: map[string]planning.PublishedCapacityPlan{}}
}

func (f *fakePlans) Upsert(ctx context.Context, p planning.PublishedCapacityPlan) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return false, fmt.Errorf("injected failure")
	}
	f.rows[p.PlanID] = p
	return true, nil
}

// fakeClaims implements ports.ProcessedEventRepository. rollback emulates
// the unit-of-work rollback of a claim whose handling failed.
type fakeClaims struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newFakeClaims() *fakeClaims { return &fakeClaims{seen: map[string]bool{}} }

func (f *fakeClaims) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := consumer + "/" + eventID
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	return true, nil
}

func (f *fakeClaims) rollback(consumer, eventID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.seen, consumer+"/"+eventID)
}

// rollbackSince un-claims every key added after the seen-map reached size
// n (caller holds f.mu).
func (f *fakeClaims) rollbackSince(n int) {
	if len(f.seen) <= n {
		return
	}
	keep := make(map[string]bool, n)
	i := 0
	for k, v := range f.seen {
		if i < n {
			keep[k] = v
			i++
		}
	}
	// Map iteration order is random; that is fine here because a failing
	// handler claims exactly ONE key inside the unit of work.
	f.seen = keep
}

// passUoW is a ports.UnitOfWork whose transaction boundary is the function
// itself: a failing fn rolls back every claim made inside it (mirroring
// pgx, where the processed-event INSERT dies with the transaction), and a
// failing upsert simply never reaches its map write.
type passUoW struct {
	claims *fakeClaims
}

func (u passUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.claims != nil {
		u.claims.mu.Lock()
		before := len(u.claims.seen)
		u.claims.mu.Unlock()
		err := fn(ctx)
		if err != nil {
			u.claims.mu.Lock()
			u.claims.rollbackSince(before)
			u.claims.mu.Unlock()
		}
		return err
	}
	return fn(ctx)
}

// encodeEvent builds a real CloudEvents 1.0 message value via this
// service's own envelope helper (never a hand-rolled shape).
func encodeEvent(t *testing.T, eventType, subject string, occurredAt time.Time, data any) []byte {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        "id-" + subject + "-" + occurredAt.Format(time.RFC3339Nano),
		Entity:    "entity",
		EventName: "Synthetic",
		Subject:   subject,
		Time:      occurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	// Overwrite type with the exact producer string under test.
	return []byte(strings.Replace(string(value),
		`"com.warehouse.wes.network-inventory-planning.entity.Synthetic"`,
		`"`+eventType+`"`, 1))
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestSiteCapabilityConsumerAppliesFact(t *testing.T) {
	repo := newFakeCapabilities()
	claims := newFakeClaims()
	c := &SiteCapabilityConsumer{Capabilities: repo, ProcessedEvents: claims, UoW: passUoW{}, Logger: quietLogger()}
	occurred := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	value := encodeEvent(t, typeSiteCapabilityChanged, "WH1", occurred, siteCapabilityChangedData{
		SiteCode: "WH1", TransferOriginEnabled: true, TransferDestinationEnabled: false, CapabilityRevision: 4,
	})
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := repo.rows["WH1"]
	if !ok {
		t.Fatal("capability fact was not applied")
	}
	if got.Revision != 4 || !got.TransferOriginEnabled || got.TransferDestinationEnabled {
		t.Fatalf("applied = %+v", got)
	}
	if !got.AsOf.Equal(occurred) {
		t.Fatalf("AsOf = %v, want the CE time %v", got.AsOf, occurred)
	}

	// Redelivery of the same event id is a no-op.
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("unexpected error on redelivery: %v", err)
	}
	if repo.upserts != 1 {
		t.Fatalf("upserts = %d, want 1 (redelivery is a no-op)", repo.upserts)
	}
}

func TestSiteCapabilityConsumerIgnoresUnknownTypeAndGarbage(t *testing.T) {
	repo := newFakeCapabilities()
	c := &SiteCapabilityConsumer{Capabilities: repo, ProcessedEvents: newFakeClaims(), UoW: passUoW{}, Logger: quietLogger()}
	occurred := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	// Another facility-layout type on the same topic.
	other := encodeEvent(t, "com.warehouse.wms.facility-layout.site.SiteRegistered", "WH1", occurred, map[string]string{"site_code": "WH1"})
	if err := c.HandleMessage(context.Background(), other); err != nil {
		t.Fatalf("unknown type must be ignored, got %v", err)
	}
	if repo.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", repo.upserts)
	}
	// Not CloudEvents at all.
	if err := c.HandleMessage(context.Background(), []byte(`{"event_type":"legacy"}`)); err != nil {
		t.Fatalf("non-CloudEvents must be skipped, got %v", err)
	}
	// Malformed payload for the known type.
	bad := encodeEvent(t, typeSiteCapabilityChanged, "WH1", occurred, map[string]string{"site_code": ""})
	if err := c.HandleMessage(context.Background(), bad); err != nil {
		t.Fatalf("domain-validation failure must be skipped, got %v", err)
	}
	if repo.upserts != 0 {
		t.Fatalf("upserts = %d, want 0", repo.upserts)
	}
}

func TestSiteCapabilityConsumerRetriesTransientFailure(t *testing.T) {
	repo := newFakeCapabilities()
	repo.failNext = 1 // the upsert fails once
	claims := newFakeClaims()
	c := &SiteCapabilityConsumer{Capabilities: repo, ProcessedEvents: claims, UoW: passUoW{}, Logger: quietLogger()}
	occurred := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	value := encodeEvent(t, typeSiteCapabilityChanged, "WH1", occurred, siteCapabilityChangedData{
		SiteCode: "WH1", TransferOriginEnabled: true, TransferDestinationEnabled: true, CapabilityRevision: 2,
	})
	if err := c.HandleMessage(context.Background(), value); err == nil {
		t.Fatal("transient failure must be returned non-nil so the loop retries")
	}
	if len(repo.rows) != 0 {
		t.Fatal("failed handling must leave no state behind")
	}
	// The claim rolled back with the unit of work: a retry of the same
	// message processes it (claims.rollback emulates the rollback).
	claims.rollback(siteCapabilityConsumerName, "id-WH1-"+occurred.Format(time.RFC3339Nano))
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("retry after transient failure: %v", err)
	}
	if _, ok := repo.rows["WH1"]; !ok {
		t.Fatal("retry must apply the fact")
	}
}

func TestSiteSkuDemandConsumerAppliesAndTombstones(t *testing.T) {
	repo := newFakeDemands()
	c := &SiteSkuDemandConsumer{Demands: repo, ProcessedEvents: newFakeClaims(), UoW: passUoW{}, Logger: quietLogger()}
	occurred := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	due := "2026-10-07T10:00:00Z"
	active := encodeEvent(t, typeSiteSkuDemandChanged, "ord-1/line/1", occurred, siteSkuDemandChangedData{
		SourceOrderID: "ord-1", LineNo: 1, SiteID: "WH1", SKU: "SKU-1", DemandedUnits: 12, DueAt: due, State: "ACTIVE", AssignmentVersion: "static-site-v1",
	})
	if err := c.HandleMessage(context.Background(), active); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d, ok := repo.rows["ord-1#1"]
	if !ok || d.DemandedUnits != 12 || d.State != planning.DemandActive {
		t.Fatalf("applied = %+v", d)
	}

	// A REMOVED fact for the same key tombstones the row.
	removed := encodeEvent(t, typeSiteSkuDemandChanged, "ord-1/line/1", occurred.Add(time.Minute), siteSkuDemandChangedData{
		SourceOrderID: "ord-1", LineNo: 1, SiteID: "WH1", SKU: "SKU-1", DemandedUnits: 0, DueAt: due, State: "REMOVED", AssignmentVersion: "static-site-v1",
	})
	if err := c.HandleMessage(context.Background(), removed); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d, ok = repo.rows["ord-1#1"]
	if !ok || d.State != planning.DemandRemoved {
		t.Fatalf("tombstoned = %+v (ok=%v)", d, ok)
	}
}

func TestCapacityPlanConsumerAppliesAndExcludesLegacy(t *testing.T) {
	repo := newFakePlans()
	c := &CapacityPlanConsumer{Plans: repo, ProcessedEvents: newFakeClaims(), UoW: passUoW{}, Logger: quietLogger()}
	occurred := time.Date(2026, 10, 6, 21, 45, 0, 0, time.UTC)

	modern := capacityPlanPublishedData{
		PlanID: "plan-1", SiteID: "WH1", WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: "2026-10-07T08:00:00Z", WindowEnd: "2026-10-07T16:00:00Z",
		AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", PublishedAt: "2026-10-06T21:45:10Z",
	}
	value := encodeEvent(t, typeCapacityPlanPublished, "plan-1", occurred, modern)
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p, ok := repo.rows["plan-1"]
	if !ok || p.SiteID != "WH1" || p.CapacityOverWindow != 8000 {
		t.Fatalf("applied = %+v", p)
	}
	if !p.PublishedAt.Equal(occurred) {
		t.Fatalf("PublishedAt = %v, want the CE time %v", p.PublishedAt, occurred)
	}

	// Legacy payload WITHOUT site_id: excluded, never inferred from
	// warehouse_id.
	legacy := modern
	legacy.PlanID = "plan-legacy"
	legacy.SiteID = ""
	legacyValue := encodeEvent(t, typeCapacityPlanPublished, "plan-legacy", occurred, legacy)
	if err := c.HandleMessage(context.Background(), legacyValue); err != nil {
		t.Fatalf("legacy plan must be skipped deterministically, got %v", err)
	}
	if _, ok := repo.rows["plan-legacy"]; ok {
		t.Fatal("legacy plan without site_id must NOT be stored")
	}
}

// fakeReader drives the run loop with a fixed message sequence.
type fakeReader struct {
	mu       sync.Mutex
	messages []kafkago.Message
	commits  []int
	closed   bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if len(r.messages) == 0 {
		r.mu.Unlock()
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	m := r.messages[0]
	r.messages = r.messages[1:]
	r.mu.Unlock()
	return m, nil
}

func (r *fakeReader) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.commits = append(r.commits, int(m.Offset))
	}
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// TestRunLoopCommitsOnlyAfterSuccess proves the at-least-once commit
// contract: a handler that fails twice then succeeds gets exactly ONE
// commit, after the success.
func TestRunLoopCommitsOnlyAfterSuccess(t *testing.T) {
	repo := newFakeCapabilities()
	repo.failNext = 2
	claims := newFakeClaims()
	reader := &fakeReader{}
	occurred := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	value := encodeEvent(t, typeSiteCapabilityChanged, "WH1", occurred, siteCapabilityChangedData{
		SiteCode: "WH1", TransferOriginEnabled: true, TransferDestinationEnabled: true, CapabilityRevision: 9,
	})
	reader.messages = []kafkago.Message{{Value: value, Partition: 0, Offset: 42}}

	var sleeps []time.Duration
	c := &SiteCapabilityConsumer{
		Reader:          reader,
		Capabilities:    repo,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		sleep: func(ctx context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		reader.mu.Lock()
		n := len(reader.commits)
		reader.mu.Unlock()
		if n == 1 {
			cancel()
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("run loop never committed the handled message")
		case <-time.After(10 * time.Millisecond):
		}
	}
	<-done
	if len(reader.commits) != 1 || reader.commits[0] != 42 {
		t.Fatalf("commits = %v, want exactly [42] after the success", reader.commits)
	}
	if len(sleeps) < 2 {
		t.Fatalf("sleeps = %v, want backoff between the two failures", sleeps)
	}
	if _, ok := repo.rows["WH1"]; !ok {
		t.Fatal("the retried message must eventually be applied")
	}
}

// --- OTel trace extraction (ADR 0007) ---------------------------------------
//
// The propagation contract on the consume side: whatever traceparent a
// producer injected into the message headers, the handler's ctx carries
// the SAME span context. Presence/parentage only — the ids themselves are
// the producer's business.

func TestConsumeLoopExtractsTraceparentIntoHandlerCtx(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

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

	var (
		gotMu   sync.Mutex
		got     trace.SpanContext
		handled bool
	)
	reader := &fakeReader{}
	// fakeReader yields its seeded messages then blocks on ctx; seed one
	// message carrying the producer's traceparent and cancel after it.
	reader.messages = []kafkago.Message{{
		Topic:     FacilityTopic,
		Partition: 0,
		Offset:    0,
		Headers: []kafkago.Header{
			{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")},
			{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	loop := consumeLoop{
		reader: reader,
		handle: func(ctx context.Context, msg kafkago.Message) error {
			gotMu.Lock()
			got = trace.SpanContextFromContext(ctx)
			handled = true
			gotMu.Unlock()
			cancel()
			return nil
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		name:   "trace extraction probe",
	}
	if err := loop.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	gotMu.Lock()
	defer gotMu.Unlock()
	if !handled {
		t.Fatal("handler never ran")
	}
	if !got.Equal(want) {
		t.Fatalf("handler span context = %+v, want the producer's %+v (extraction dropped the trace)", got, want)
	}
}

// TestConsumeLoopHandlesUntracedMessage covers the un-instrumented
// producer: no traceparent on the message, the handler still runs (with
// no span context — never an error).
func TestConsumeLoopHandlesUntracedMessage(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	reader := &fakeReader{}
	reader.messages = []kafkago.Message{{Topic: FacilityTopic, Partition: 0, Offset: 0}}

	ctx, cancel := context.WithCancel(context.Background())
	handled := false
	loop := consumeLoop{
		reader: reader,
		handle: func(ctx context.Context, msg kafkago.Message) error {
			handled = true
			if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
				t.Errorf("span context = %+v, want none (message carried no traceparent)", sc)
			}
			cancel()
			return nil
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		name:   "untraced probe",
	}
	if err := loop.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if !handled {
		t.Fatal("handler never ran")
	}
}
