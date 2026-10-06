//go:build integration

// Package kafka_integration proves the Phase-2 transfer saga end-to-end
// against REAL infrastructure: Postgres and Kafka via testcontainers. The
// approval transaction persists the saga and writes the outbox; the relay
// drains TransferPlanApproved + TransferAllocationRequested onto
// warehouse.network-inventory-planning.events; a synthetic
// inventory-storage reply drives the aggregate to ALLOCATED; a replay of
// the same reply is a no-op.
package kafka_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// tckafka is imported (not just inherited from the sibling file's helpers)
// because the fleet fitness test TestKafkaIntegrationTestsUseTestcontainers
// requires every Kafka-touching integration test to run its broker via
// testcontainers-go/modules/kafka — the shared helpers in
// planning_read_models_integration_test.go do exactly that.
var _ = tckafka.Run

var itNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// itFacts seeds the three read models so the approval's fail-closed
// validation passes: WH1 (origin, capacity 500, demand 40) → WH2
// (destination, in-window SKU-1 demand 10).
func itFacts() planning.Facts {
	due := itNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, itNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, itNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = itNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = itNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: itNow.Add(time.Hour), WindowEnd: itNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: itNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: itNow.Add(time.Hour), WindowEnd: itNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: itNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

// readOutboxEvents reads the two outbox-published messages off the
// service's own topic (fresh group, from the beginning).
func readOutboxEvents(t *testing.T, brokers []string, want int) []kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    outboundkafka.TransferTopic,
		GroupID:  uniqueGroupID("outbox-read"),
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() { _ = reader.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var msgs []kafkago.Message
	for len(msgs) < want {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("fetch outbox message %d: %v (got %d)", len(msgs), err, len(msgs))
		}
		msgs = append(msgs, msg)
		if err := reader.CommitMessages(ctx, msg); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	return msgs
}

func TestTransferSagaIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	brokers := startKafkaBroker(t)
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	uow := postgres.NewUnitOfWork(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	transfers := postgres.NewTransferRepo(pool)
	snapshots := postgres.NewSnapshotRepo(pool)
	encoder := outboundkafka.NewTransferEncoder()
	outbox := postgres.NewOutboxWriter(pool, encoder)
	sink := outboundkafka.NewRelaySink(brokers)
	defer func() { _ = sink.Close() }()
	relay := postgres.NewOutboxRelay(pool, sink)

	ensureTopic(t, brokers, outboundkafka.TransferTopic)
	ensureTopic(t, brokers, inboundkafka.InventoryTopic)

	// Seed the read models directly through the repos (the consumers are
	// Phase 1 and already covered by their own integration test).
	facts := itFacts()
	if err := uow.Do(ctx, func(ctx context.Context) error {
		for _, c := range facts.Capabilities {
			if _, err := postgres.NewSiteCapabilityRepo(pool).Upsert(ctx, c); err != nil {
				return err
			}
		}
		for _, d := range facts.Demands {
			if _, err := postgres.NewSiteSkuDemandRepo(pool).Upsert(ctx, d); err != nil {
				return err
			}
		}
		for _, p := range facts.Plans {
			if _, err := postgres.NewPublishedCapacityPlanRepo(pool).Upsert(ctx, p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed read models: %v", err)
	}

	approve := &usecases.ApproveTransfer{
		Transfers:    transfers,
		Events:       outbox,
		Snapshot:     snapshots,
		UoW:          uow,
		MaxStaleness: 10 * time.Minute,
		Now:          func() time.Time { return itNow },
	}

	// 1. Approve: state persists through ALLOCATING and the outbox holds
	//    both events, all in ONE transaction.
	result, err := approve.Execute(ctx, usecases.ApproveTransferInput{
		IdempotencyKey:    "itest-idem-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "integration rebalance",
		ProposalAsOf:      itNow.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	loaded, err := transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.State() != "ALLOCATING" {
		t.Fatalf("state after approve = %s, want ALLOCATING", loaded.State())
	}

	// A replay of the same key is the same transfer, nothing new persisted.
	replay, err := approve.Execute(ctx, usecases.ApproveTransferInput{
		IdempotencyKey:    "itest-idem-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "integration rebalance",
		ProposalAsOf:      itNow.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.TransferID != result.TransferID {
		t.Fatalf("replay = %+v", replay)
	}

	// 2. Drain the relay: TransferPlanApproved and the
	//    TransferAllocationRequested command (keyed transfer_line_id)
	//    reach warehouse.network-inventory-planning.events in order.
	published, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if published != 2 {
		t.Fatalf("published = %d, want 2", published)
	}
	msgs := readOutboxEvents(t, brokers, 2)
	var sawApproved, sawRequested bool
	for _, m := range msgs {
		var envelope struct {
			Type    string         `json:"type"`
			Subject string         `json:"subject"`
			Data    map[string]any `json:"data"`
		}
		if err := json.Unmarshal(m.Value, &envelope); err != nil {
			t.Fatalf("decode %s: %v", m.Key, err)
		}
		switch envelope.Type {
		case "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved":
			sawApproved = true
			if envelope.Data["transfer_id"] != string(result.TransferID) {
				t.Fatalf("approved payload = %+v", envelope.Data)
			}
		case "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested":
			sawRequested = true
			wantLine := string(result.TransferID) + ":1"
			if string(m.Key) != wantLine || envelope.Data["transfer_line_id"] != wantLine {
				t.Fatalf("command key/payload = %q / %+v", m.Key, envelope.Data)
			}
			if envelope.Data["origin_site_id"] != "WH1" || envelope.Data["sku"] != "SKU-1" || envelope.Data["quantity"].(float64) != 5 {
				t.Fatalf("command payload = %+v", envelope.Data)
			}
		default:
			t.Fatalf("unexpected type %q", envelope.Type)
		}
	}
	if !sawApproved || !sawRequested {
		t.Fatalf("approved=%v requested=%v", sawApproved, sawRequested)
	}
	// The relay is idempotent: a second pass has nothing to publish.
	again, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("second relay: %v", err)
	}
	if again != 0 {
		t.Fatalf("second relay published = %d, want 0", again)
	}

	// 3. Simulate inventory-storage's TransferStockAllocated reply on
	//    warehouse.inventory.events and consume it: the saga reaches
	//    ALLOCATED with the reservation persisted.
	replyAt := itNow.Add(time.Minute)
	allocatedValue := syntheticInventoryReply(t, result.TransferID, replyAt)
	produceRaw(t, brokers, inboundkafka.InventoryTopic, string(result.TransferID)+":1", allocatedValue)

	allocate := &usecases.ApplyTransferAllocation{Transfers: transfers, UoW: uow}
	reject := &usecases.ApplyTransferRejection{Transfers: transfers, UoW: uow}
	replyConsumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject}, processedEvents, uow, nil)
	runConsumerUntil(t, replyConsumer, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after reply: %v", err)
	}
	if loaded.State() != "ALLOCATED" {
		t.Fatalf("state after reply = %s, want ALLOCATED", loaded.State())
	}
	if loaded.ReservationID() != "res-itest-1" || len(loaded.Allocations()) != 1 {
		t.Fatalf("reservation = %q allocations = %+v", loaded.ReservationID(), loaded.Allocations())
	}
	audit := loaded.Audit()
	if len(audit) != 5 {
		t.Fatalf("audit entries = %d, want 5: %+v", len(audit), audit)
	}

	// 4. Replay the SAME reply (fresh consumer group, redelivered): a
	//    no-op — the transfer stays ALLOCATED with one audit entry for
	//    the allocation, deduped on the CE id.
	replayConsumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply-replay"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject}, processedEvents, uow, nil)
	runConsumerUntil(t, replayConsumer, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after replay: %v", err)
	}
	if loaded.State() != "ALLOCATED" || len(loaded.Audit()) != 5 {
		t.Fatalf("after replay: state = %s audit = %d (reply replay must be a no-op)", loaded.State(), len(loaded.Audit()))
	}
}

// syntheticInventoryReply builds inventory-storage's
// TransferStockAllocated wire shape (its own asyncapi contract, mirrored).
func syntheticInventoryReply(t *testing.T, id fmt.Stringer, occurredAt time.Time) []byte {
	t.Helper()
	payload := map[string]any{
		"specversion":     "1.0",
		"id":              "itest-reply-" + id.String(),
		"source":          "/warehouse/inventory-storage",
		"type":            "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated",
		"subject":         "res-itest-1",
		"time":            occurredAt.UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:inventory-storage:events:TransferStockAllocated:v1",
		"data": map[string]any{
			"transfer_id":      id.String(),
			"transfer_line_id": id.String() + ":1",
			"origin_site_id":   "WH1",
			"reservation_id":   "res-itest-1",
			"sku":              "SKU-1",
			"quantity":         5,
			"allocations": []map[string]any{
				{"stock_unit_id": "su-itest-1", "bin_id": "BIN-ITEST", "quantity": 5},
			},
			"expires_at": occurredAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
		},
	}
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	return value
}

// ensureMigrationsPathIsRepoRelative guards the migrations lookup when the
// runner's working directory differs.
func ensureMigrationsPathIsRepoRelative(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "outbound", "postgres", "migrations")
}
