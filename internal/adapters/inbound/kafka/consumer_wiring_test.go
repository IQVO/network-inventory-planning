package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// This file unit-covers the wiring the handler-level tests leave at 0%:
// the New*Consumer constructors (reader configuration, DLQ topics), Close,
// the Run loop of every consumer, the FactUseCases / ReplyUseCases
// adapter structs, and the small kafka.go helpers (readerConfig's
// synchronous-commit contract, sleepCtx, the read-only OTel header
// carrier).

var wiringNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

func readerConfigOf(t *testing.T, r Reader) kafkago.ReaderConfig {
	t.Helper()
	real, ok := r.(*kafkago.Reader)
	if !ok {
		t.Fatalf("Reader is %T, want *kafkago.Reader", r)
	}
	return real.Config()
}

// TestConstructorsWireReaderConfigAndTopics proves every constructor
// points its reader at the right brokers/topic/groupID and keeps
// CommitInterval unset (the synchronous commit the at-least-once loop
// depends on), and that Close releases the never-started reader.
func TestConstructorsWireReaderConfigAndTopics(t *testing.T) {
	brokers := []string{"b1:9092", "b2:9092"}
	cases := []struct {
		name     string
		topic    string
		build    func() Reader
		dlqTopic string
	}{
		{
			name:  "site capability",
			topic: FacilityTopic,
			build: func() Reader {
				c := NewSiteCapabilityConsumer(brokers, "grp-cap", newFakeCapabilities(), newFakeClaims(), passUoW{}, quietLogger())
				return c.Reader
			},
		},
		{
			name:  "site sku demand",
			topic: OrderTopic,
			build: func() Reader {
				c := NewSiteSkuDemandConsumer(brokers, "grp-dem", newFakeDemands(), newFakeClaims(), passUoW{}, quietLogger())
				return c.Reader
			},
		},
		{
			name:  "capacity plan",
			topic: PlanningTopic,
			build: func() Reader {
				c := NewCapacityPlanConsumer(brokers, "grp-plan", newFakePlans(), newFakeClaims(), passUoW{}, quietLogger())
				return c.Reader
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := readerConfigOf(t, tc.build())
			if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "b1:9092" || cfg.Brokers[1] != "b2:9092" {
				t.Errorf("Brokers = %v, want %v", cfg.Brokers, brokers)
			}
			if cfg.Topic != tc.topic {
				t.Errorf("Topic = %q, want %q", cfg.Topic, tc.topic)
			}
			if cfg.GroupID == "" || cfg.GroupID == "group" {
				t.Errorf("GroupID = %q, want the caller's env-configured group", cfg.GroupID)
			}
			if cfg.CommitInterval != 0 {
				t.Errorf("CommitInterval = %v, want 0 (synchronous commits after handling)", cfg.CommitInterval)
			}
		})
	}
}

func TestTransferConsumerConstructorsWireDLQTopics(t *testing.T) {
	brokers := []string{"b1:9092"}
	fact := NewTransferFactConsumer(brokers, "grp-fact", nil, newFakeClaims(), passUoW{}, quietLogger())
	if fact.DLQTopic != FulfillmentTopic+DLQSuffix {
		t.Errorf("fact DLQTopic = %q, want %q", fact.DLQTopic, FulfillmentTopic+DLQSuffix)
	}
	if cfg := readerConfigOf(t, fact.Reader); cfg.Topic != FulfillmentTopic {
		t.Errorf("fact Topic = %q, want %q", cfg.Topic, FulfillmentTopic)
	}
	if err := fact.Close(); err != nil {
		t.Errorf("fact Close on a never-started reader: %v", err)
	}

	reply := NewTransferReplyConsumer(brokers, "grp-reply", nil, newFakeClaims(), passUoW{}, quietLogger())
	if reply.DLQTopic != InventoryTopic+DLQSuffix {
		t.Errorf("reply DLQTopic = %q, want %q", reply.DLQTopic, InventoryTopic+DLQSuffix)
	}
	if cfg := readerConfigOf(t, reply.Reader); cfg.Topic != InventoryTopic {
		t.Errorf("reply Topic = %q, want %q", cfg.Topic, InventoryTopic)
	}
	if err := reply.Close(); err != nil {
		t.Errorf("reply Close on a never-started reader: %v", err)
	}
}

func TestNewAnalyticsConsumerWiresTopicAndDLQ(t *testing.T) {
	brokers := []string{"b1:9092"}
	topic := "warehouse.analytics.events"
	c := NewAnalyticsConsumer(brokers, topic, "grp-an", nil, quietLogger())
	if c.DLQTopic != topic+DLQSuffix {
		t.Errorf("DLQTopic = %q, want %q", c.DLQTopic, topic+DLQSuffix)
	}
	cfg := readerConfigOf(t, c.Reader)
	if cfg.Topic != topic || cfg.GroupID != "grp-an" {
		t.Errorf("reader config = %+v, want topic %q group grp-an", cfg, topic)
	}
}

// runUntilCommitted runs c.Run until the reader committed n offsets, then
// cancels the context and returns Run's result (a cancelled loop's
// context.Canceled is the normal shutdown, reported as nil).
func runUntilCommitted(t *testing.T, c interface {
	Run(ctx context.Context) error
}, reader *fakeReader, n int) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		reader.mu.Lock()
		commits := len(reader.commits)
		reader.mu.Unlock()
		if commits >= n {
			cancel()
			err := <-done
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("run loop never committed the handled message")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSiteSkuDemandConsumerRunAppliesAndCommits(t *testing.T) {
	repo := newFakeDemands()
	reader := &fakeReader{}
	c := &SiteSkuDemandConsumer{
		Reader: reader, Demands: repo, ProcessedEvents: newFakeClaims(),
		UoW: passUoW{}, Logger: quietLogger(),
	}
	reader.messages = []kafkago.Message{{
		Topic: OrderTopic, Partition: 0, Offset: 7,
		Value: encodeEvent(t, typeSiteSkuDemandChanged, "ord-1/line/1", wiringNow, siteSkuDemandChangedData{
			SourceOrderID: "ord-1", LineNo: 1, SiteID: "WH1", SKU: "SKU-1",
			DemandedUnits: 12, DueAt: "2026-10-07T10:00:00Z", State: "ACTIVE", AssignmentVersion: "static-site-v1",
		}),
	}}
	if err := runUntilCommitted(t, c, reader, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reader.commits) != 1 || reader.commits[0] != 7 {
		t.Fatalf("commits = %v, want [7] (committed only after the apply)", reader.commits)
	}
	if d, ok := repo.rows["ord-1#1"]; !ok || d.DemandedUnits != 12 {
		t.Fatalf("applied demand = %+v (ok=%t), want 12 units for ord-1#1", d, ok)
	}
}

func TestCapacityPlanConsumerRunAppliesAndCommits(t *testing.T) {
	repo := newFakePlans()
	reader := &fakeReader{}
	c := &CapacityPlanConsumer{
		Reader: reader, Plans: repo, ProcessedEvents: newFakeClaims(),
		UoW: passUoW{}, Logger: quietLogger(),
	}
	reader.messages = []kafkago.Message{{
		Topic: PlanningTopic, Partition: 0, Offset: 3,
		Value: encodeEvent(t, typeCapacityPlanPublished, "plan-1", wiringNow, capacityPlanPublishedData{
			PlanID: "plan-1", SiteID: "WH1", WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
			WindowStart: "2026-10-07T08:00:00Z", WindowEnd: "2026-10-07T16:00:00Z",
			AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
			BottleneckStep: "REBIN", PublishedAt: wiringNow.Format(time.RFC3339),
		}),
	}}
	if err := runUntilCommitted(t, c, reader, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reader.commits) != 1 || reader.commits[0] != 3 {
		t.Fatalf("commits = %v, want [3]", reader.commits)
	}
	if p, ok := repo.rows["plan-1"]; !ok || p.SiteID != "WH1" {
		t.Fatalf("applied plan = %+v (ok=%t)", p, ok)
	}
}

func TestTransferFactConsumerRunAppliesAndCommits(t *testing.T) {
	applier := &fakeFactApplier{}
	reader := &fakeReader{}
	c := &TransferFactConsumer{
		Reader: reader, Applier: applier, ProcessedEvents: newFakeClaims(),
		UoW: passUoW{}, Logger: quietLogger(), DLQTopic: FulfillmentTopic + DLQSuffix,
	}
	reader.messages = []kafkago.Message{{
		Topic: FulfillmentTopic, Partition: 0, Offset: 11,
		Value: encodeEvent(t, typeTransferPicked, "task-1", wiringNow, transferFactData{
			TransferRef: "trf-1", TaskID: "task-1", WorkKind: "PICK", Quantity: 10,
		}),
	}}
	if err := runUntilCommitted(t, c, reader, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reader.commits) != 1 || reader.commits[0] != 11 {
		t.Fatalf("commits = %v, want [11]", reader.commits)
	}
	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.picks) != 1 {
		t.Fatalf("picks = %d, want the picked fact applied once", len(applier.picks))
	}
}

func TestTransferReplyConsumerRunAppliesAndCommits(t *testing.T) {
	applier := &fakeReplyApplier{}
	reader := &fakeReader{}
	c := &TransferReplyConsumer{
		Reader: reader, Applier: applier, ProcessedEvents: newFakeClaims(),
		UoW: passUoW{}, Logger: quietLogger(), DLQTopic: InventoryTopic + DLQSuffix,
	}
	reader.messages = []kafkago.Message{{
		Topic: InventoryTopic, Partition: 0, Offset: 5,
		Value: encodeEvent(t, typeTransferStockAllocated, "trf-1:1", wiringNow, transferStockAllocatedData{
			TransferID: "trf-1", TransferLineID: "trf-1:1", ReservationID: "res-1",
			OriginSiteID: "WH1", SKU: "SKU-1", Quantity: 10,
			Allocations: []struct {
				StockUnitID string `json:"stock_unit_id"`
				BinID       string `json:"bin_id"`
				Quantity    int    `json:"quantity"`
			}{{StockUnitID: "su-1", BinID: "BIN-1", Quantity: 10}},
			ExpiresAt: wiringNow.Add(time.Hour).Format(time.RFC3339),
		}),
	}}
	if err := runUntilCommitted(t, c, reader, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reader.commits) != 1 || reader.commits[0] != 5 {
		t.Fatalf("commits = %v, want [5]", reader.commits)
	}
	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.allocations) != 1 {
		t.Fatalf("allocations = %d, want the allocated reply applied once", len(applier.allocations))
	}
}

// --- FactUseCases / ReplyUseCases adapters -----------------------------------
//
// The adapter structs had no unit coverage: they triple and quadruple the
// use cases into the consumer interfaces. These tests drive them over the
// REAL use cases with the memory store, so each adapter method is proven
// to call the right use case with the input it was handed.

// recordingPublisher captures every published domain event.
type recordingPublisher struct {
	events []transfer.DomainEvent
}

func (p *recordingPublisher) Publish(_ context.Context, events ...transfer.DomainEvent) error {
	p.events = append(p.events, events...)
	return nil
}

func (p *recordingPublisher) names() []string {
	out := make([]string, 0, len(p.events))
	for _, e := range p.events {
		out = append(out, e.EventName())
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// storedTransferAt rehydrates a transfer already sitting in the given
// state, exactly as the acceptance suite seeds saga state.
func storedTransferAt(t *testing.T, s *memory.TransferStore, id string, state transfer.TransferState) transfer.TransferID {
	t.Helper()
	trf := transfer.Rehydrate(transfer.Snapshot{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id,
		OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1",
		Quantity: 10, PolicyVersion: "pv-1",
		ProposalAsOf: wiringNow.Add(-time.Hour), ExpiresAt: wiringNow.Add(24 * time.Hour),
		State:     state,
		Audit:     []transfer.AuditEntry{{To: state, Event: "seeded", Reason: "seed", OccurredAt: wiringNow}},
		CreatedAt: wiringNow, UpdatedAt: wiringNow,
	})
	if _, err := s.Create(context.Background(), trf, "key-"+id); err != nil {
		t.Fatalf("Create(%s at %s): %v", id, state, err)
	}
	return trf.ID()
}

var fullRelease = usecases.WorkReleaseConfig{
	PickPathID: "pick-path", PickCPTOffset: 2 * time.Hour,
	DispatchPathID: "dispatch-path", DispatchCPTOffset: 3 * time.Hour,
}

func TestFactUseCasesAdapterDrivesTheSagaTail(t *testing.T) {
	store := memory.NewTransferStore()
	pub := &recordingPublisher{}
	facts := FactUseCases{
		Pick:       usecases.ApplyTransferPick{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}, Release: fullRelease},
		Dispatched: usecases.ApplyTransferDispatched{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}},
		Arrival:    usecases.ApplyTransferArrival{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}},
	}
	ctx := context.Background()
	id := storedTransferAt(t, store, "trf-f1", transfer.StateAllocated)

	if err := facts.ApplyPick(ctx, usecases.ApplyPickInput{
		TransferID: id, Picked: transfer.Picked{PickedQuantity: 7}, OccurredAt: wiringNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyPick: %v", err)
	}
	trf, _ := store.Load(ctx, id)
	if trf.State() != transfer.StatePicked || trf.PickedQuantity() != 7 {
		t.Fatalf("after ApplyPick = %s/%d, want PICKED with 7 picked (a short pick is recorded)", trf.State(), trf.PickedQuantity())
	}
	if names := pub.names(); !contains(names, "TransferStateAdvanced") || !contains(names, "WorkDemandReleased") {
		t.Fatalf("published = %v, want the state occurrence and the dispatch demand (carrying the picked 7)", names)
	}

	if err := facts.ApplyDispatched(ctx, usecases.FactInput{TransferID: id, OccurredAt: wiringNow.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("ApplyDispatched: %v", err)
	}
	if trf, _ = store.Load(ctx, id); trf.State() != transfer.StateInTransit {
		t.Fatalf("after ApplyDispatched = %s, want IN_TRANSIT", trf.State())
	}

	if err := facts.ApplyArrival(ctx, usecases.FactInput{TransferID: id, OccurredAt: wiringNow.Add(3 * time.Minute)}); err != nil {
		t.Fatalf("ApplyArrival: %v", err)
	}
	if trf, _ = store.Load(ctx, id); trf.State() != transfer.StateArrived {
		t.Fatalf("after ApplyArrival = %s, want ARRIVED", trf.State())
	}
}

func TestReplyUseCasesAdapterDrivesTheSaga(t *testing.T) {
	store := memory.NewTransferStore()
	pub := &recordingPublisher{}
	replies := ReplyUseCases{
		Allocate: usecases.ApplyTransferAllocation{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}, Release: fullRelease, Now: func() time.Time { return wiringNow }},
		Reject:   usecases.ApplyTransferRejection{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}},
		Staged:   usecases.ApplyTransferReceiptStaged{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}},
		Stow:     usecases.ApplyTransferStow{Transfers: store, Events: pub, UoW: memory.UnitOfWork{}},
	}
	ctx := context.Background()

	allocID := storedTransferAt(t, store, "trf-r1", transfer.StateAllocating)
	if err := replies.ApplyAllocation(ctx, usecases.ApplyAllocationInput{
		TransferID: allocID,
		Allocation: transfer.StockAllocation{
			TransferLineID: allocID.LineID(), OriginSiteID: "WH1", SKU: "SKU-1",
			ReservationID: "res-1", Quantity: 10,
			Allocations: []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-1", Quantity: 10}},
			ExpiresAt:   wiringNow.Add(time.Hour),
		},
		OccurredAt: wiringNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyAllocation: %v", err)
	}
	trf, _ := store.Load(ctx, allocID)
	if trf.State() != transfer.StateAllocated || trf.ReservationID() != "res-1" {
		t.Fatalf("after ApplyAllocation = %s/%q, want ALLOCATED under res-1", trf.State(), trf.ReservationID())
	}
	if names := pub.names(); !contains(names, "WorkDemandReleased") {
		t.Fatalf("published = %v, want the pick demand released with the allocation", names)
	}

	rejID := storedTransferAt(t, store, "trf-r2", transfer.StateAllocating)
	if err := replies.ApplyRejection(ctx, usecases.ApplyRejectionInput{
		TransferID: rejID, LineID: rejID.LineID(),
		Reason: transfer.RejectionInsufficientUsable, OccurredAt: wiringNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyRejection: %v", err)
	}
	if trf, _ = store.Load(ctx, rejID); trf.State() != transfer.StateUnfulfillable {
		t.Fatalf("after ApplyRejection = %s, want UNFULFILLABLE", trf.State())
	}

	stowID := storedTransferAt(t, store, "trf-r3", transfer.StateInTransit)
	if err := replies.ApplyReceiptStaged(ctx, usecases.ApplyReceiptStagedInput{
		TransferID: stowID, LineID: stowID.LineID(), OccurredAt: wiringNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyReceiptStaged: %v", err)
	}
	if trf, _ = store.Load(ctx, stowID); trf.State() != transfer.StateArrived {
		t.Fatalf("after ApplyReceiptStaged = %s, want ARRIVED", trf.State())
	}

	if err := replies.ApplyStowed(ctx, usecases.ApplyStowInput{
		TransferID: stowID,
		Stowed: transfer.Stowed{
			TransferLineID: stowID.LineID(), DestinationSite: "WH2", SKU: "SKU-1",
			ReceivedQuantity: 10, StowedQuantity: 10,
			Allocations: []transfer.StowAllocation{{StockUnitID: "su-1", BinID: "BIN-9", Quantity: 10}},
		},
		OccurredAt: wiringNow.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("ApplyStowed: %v", err)
	}
	if trf, _ = store.Load(ctx, stowID); trf.State() != transfer.StateReceived {
		t.Fatalf("after ApplyStowed = %s, want RECEIVED (terminal)", trf.State())
	}
}

// --- kafka.go helpers ---------------------------------------------------------

func TestReaderConfigKeepsCommitsSynchronous(t *testing.T) {
	cfg := readerConfig([]string{"b:9092"}, "warehouse.topic", "grp")
	if cfg.Topic != "warehouse.topic" || cfg.GroupID != "grp" || len(cfg.Brokers) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.CommitInterval != 0 {
		t.Fatalf("CommitInterval = %v, want 0 so CommitMessages commits synchronously", cfg.CommitInterval)
	}
}

func TestSleepCtxWaitsAndCancels(t *testing.T) {
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("sleepCtx(1ms) = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepCtx(cancelled) = %v, want context.Canceled", err)
	}
}

func TestReadOnlyHeaderCarrier(t *testing.T) {
	carrier := readOnlyHeaderCarrier{headers: []kafkago.Header{
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: "tracestate", Value: []byte("k=v")},
	}}
	// Set is a no-op: a consumer extracts, it never re-injects.
	carrier.Set("traceparent", "overwritten")
	if got := carrier.Get("traceparent"); got != "00-abc-def-01" {
		t.Fatalf("Get after Set = %q, want the original value", got)
	}
	if got := carrier.Get("missing"); got != "" {
		t.Fatalf("Get(missing) = %q, want \"\"", got)
	}
	keys := carrier.Keys()
	if len(keys) != 2 || keys[0] != "traceparent" || keys[1] != "tracestate" {
		t.Fatalf("Keys() = %v, want both header keys in order", keys)
	}
}
