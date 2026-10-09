//go:build integration

// Package kafka_integration proves the Phase-2 transfer saga end-to-end
// against REAL infrastructure: Postgres and Kafka via testcontainers. The
// approval transaction persists the saga and writes the outbox; the relay
// drains TransferPlanApproved + TransferAllocationRequested onto
// warehouse.network-inventory-planning.events; a synthetic
// inventory-storage reply drives the aggregate to ALLOCATED; a replay of
// the same reply is a no-op.
package kafka_integration

// The import block needs strings for the fact builders below.
import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
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
		Release: usecases.WorkReleaseConfig{
			PickPathID:        "transfer-pick-path",
			PickCPTOffset:     2 * time.Hour,
			DispatchPathID:    "transfer-dispatch-path",
			DispatchCPTOffset: 3 * time.Hour,
		},
		Now: func() time.Time { return itNow },
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

	allocate := &usecases.ApplyTransferAllocation{
		Transfers: transfers,
		Events:    outbox,
		UoW:       uow,
		Release: usecases.WorkReleaseConfig{
			PickPathID:        "transfer-pick-path",
			PickCPTOffset:     2 * time.Hour,
			DispatchPathID:    "transfer-dispatch-path",
			DispatchCPTOffset: 3 * time.Hour,
		},
		Now: func() time.Time { return itNow },
	}
	reject := &usecases.ApplyTransferRejection{Transfers: transfers, UoW: uow}
	stagedUse := &usecases.ApplyTransferReceiptStaged{Transfers: transfers, UoW: uow}
	stowUse := &usecases.ApplyTransferStow{Transfers: transfers, UoW: uow}
	replyConsumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject, Staged: *stagedUse, Stow: *stowUse}, processedEvents, uow, nil)
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

	// 4. Relay the pick WorkDemandReleased (the allocation reply released
	//    it in the SAME transaction as the ALLOCATED transition) and read
	//    it off the topic: exactly WES's consumed shape.
	published, err = relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay pick demand: %v", err)
	}
	if published != 1 {
		t.Fatalf("pick-demand relay published = %d, want 1", published)
	}
	// The reader uses a fresh group, so it replays the whole topic: read
	// all three messages and take the LAST (the pick demand).
	msgs = readOutboxEvents(t, brokers, 3)
	pickDemandMsg := msgs[len(msgs)-1]
	var pickDemandEnvelope struct {
		Type    string         `json:"type"`
		Subject string         `json:"subject"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(pickDemandMsg.Value, &pickDemandEnvelope); err != nil {
		t.Fatalf("decode pick demand: %v", err)
	}
	if pickDemandEnvelope.Type != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" {
		t.Fatalf("pick demand type = %q", pickDemandEnvelope.Type)
	}
	wantDemandID := string(result.TransferID) + ":pick"
	if string(pickDemandMsg.Key) != wantDemandID || pickDemandEnvelope.Data["demand_id"] != wantDemandID {
		t.Fatalf("pick demand key/payload = %q / %+v", msgs[0].Key, pickDemandEnvelope.Data)
	}
	if pickDemandEnvelope.Data["work_kind"] != "TRANSFER_PICK" ||
		pickDemandEnvelope.Data["transfer_ref"] != string(result.TransferID) ||
		pickDemandEnvelope.Data["path_id"] != "transfer-pick-path" ||
		pickDemandEnvelope.Data["site_id"] != "WH1" ||
		pickDemandEnvelope.Data["sku"] != "SKU-1" ||
		pickDemandEnvelope.Data["quantity"].(float64) != 5 {
		t.Fatalf("pick demand payload = %+v", pickDemandEnvelope.Data)
	}

	// 5. Replay the SAME allocation reply (fresh consumer group,
	//    redelivered): a no-op — the transfer stays ALLOCATED with one
	//    audit entry for the allocation, deduped on the CE id, and the
	//    pick demand is never re-relayed.
	replayConsumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply-replay"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject, Staged: *stagedUse, Stow: *stowUse}, processedEvents, uow, nil)
	runConsumerUntil(t, replayConsumer, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after replay: %v", err)
	}
	if loaded.State() != "ALLOCATED" || len(loaded.Audit()) != 5 {
		t.Fatalf("after replay: state = %s audit = %d (reply replay must be a no-op)", loaded.State(), len(loaded.Audit()))
	}
	if n, err := relay.RelayOnce(ctx); err != nil || n != 0 {
		t.Fatalf("replayed reply must not re-release the pick demand: n = %d err = %v", n, err)
	}

	// 6. Synthetic fulfillment-execution TransferPicked (SHORT pick: 3 of
	//    the allocated 5) on warehouse.fulfillment.events: the fact
	//    consumer drives ALLOCATED → PICKED, records picked_quantity 3,
	//    and releases the dispatch WorkDemandReleased with quantity 3.
	ensureTopic(t, brokers, inboundkafka.FulfillmentTopic)
	pickedAt := replyAt.Add(5 * time.Minute)
	produceRaw(t, brokers, inboundkafka.FulfillmentTopic, "task-itest-1", syntheticTransferFact(t, "com.warehouse.wes.fulfillment-execution.transfer.TransferPicked", "task-itest-1", result.TransferID, "TRANSFER_PICK", 3, pickedAt))

	factAllocateCfg := usecases.WorkReleaseConfig{
		PickPathID:        "transfer-pick-path",
		PickCPTOffset:     2 * time.Hour,
		DispatchPathID:    "transfer-dispatch-path",
		DispatchCPTOffset: 3 * time.Hour,
	}
	factConsumer := inboundkafka.NewTransferFactConsumer(brokers, uniqueGroupID("transfer-fact"),
		inboundkafka.FactUseCases{
			Pick:       usecases.ApplyTransferPick{Transfers: transfers, Events: outbox, UoW: uow, Release: factAllocateCfg},
			Dispatched: usecases.ApplyTransferDispatched{Transfers: transfers, UoW: uow},
			Arrival:    usecases.ApplyTransferArrival{Transfers: transfers, UoW: uow},
		}, processedEvents, uow, nil)
	runConsumerUntil(t, factConsumer, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after pick: %v", err)
	}
	if loaded.State() != "PICKED" {
		t.Fatalf("state after pick = %s, want PICKED", loaded.State())
	}
	if loaded.PickedQuantity() != 3 {
		t.Fatalf("picked quantity = %d, want 3 (short pick recorded)", loaded.PickedQuantity())
	}

	// The dispatch demand carries the PICKED quantity (3), not 5. The
	// fresh-group reader replays the whole topic: take the LAST message.
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("dispatch-demand relay: n = %d err = %v", n, err)
	}
	msgs = readOutboxEvents(t, brokers, 4)
	dispatchDemandMsg := msgs[len(msgs)-1]
	var dispatchDemandEnvelope struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(dispatchDemandMsg.Value, &dispatchDemandEnvelope); err != nil {
		t.Fatalf("decode dispatch demand: %v", err)
	}
	wantDispatchID := string(result.TransferID) + ":dispatch"
	if dispatchDemandEnvelope.Type != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" ||
		dispatchDemandEnvelope.Data["demand_id"] != wantDispatchID ||
		dispatchDemandEnvelope.Data["work_kind"] != "TRANSFER_DISPATCH" ||
		dispatchDemandEnvelope.Data["path_id"] != "transfer-dispatch-path" ||
		dispatchDemandEnvelope.Data["quantity"].(float64) != 3 {
		t.Fatalf("dispatch demand = %+v", dispatchDemandEnvelope.Data)
	}

	// 7. Dispatch → IN_TRANSIT, then the inventory destination facts drive
	//    ARRIVED and RECEIVED (terminal).
	dispatchedAt := pickedAt.Add(5 * time.Minute)
	produceRaw(t, brokers, inboundkafka.FulfillmentTopic, "task-itest-2", syntheticTransferFact(t, "com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched", "task-itest-2", result.TransferID, "TRANSFER_DISPATCH", 3, dispatchedAt))
	stagedAt := dispatchedAt.Add(5 * time.Minute)
	produceRaw(t, brokers, inboundkafka.InventoryTopic, string(result.TransferID)+":1", syntheticInventoryDestinationFact(t, "com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged", result.TransferID, stagedAt, 0))
	stowedAt := stagedAt.Add(5 * time.Minute)
	produceRaw(t, brokers, inboundkafka.InventoryTopic, string(result.TransferID)+":1", syntheticInventoryDestinationFact(t, "com.warehouse.wms.inventory-storage.stock.TransferStockStowed", result.TransferID, stowedAt, 3))

	secondFactConsumer := inboundkafka.NewTransferFactConsumer(brokers, uniqueGroupID("transfer-fact-2"),
		inboundkafka.FactUseCases{
			Pick:       usecases.ApplyTransferPick{Transfers: transfers, Events: outbox, UoW: uow, Release: factAllocateCfg},
			Dispatched: usecases.ApplyTransferDispatched{Transfers: transfers, UoW: uow},
			Arrival:    usecases.ApplyTransferArrival{Transfers: transfers, UoW: uow},
		}, processedEvents, uow, nil)
	runConsumerUntil(t, secondFactConsumer, 15*time.Second)
	finalReplyConsumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply-final"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject, Staged: *stagedUse, Stow: *stowUse}, processedEvents, uow, nil)
	runConsumerUntil(t, finalReplyConsumer, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after stow: %v", err)
	}
	if loaded.State() != "RECEIVED" {
		t.Fatalf("state after stow = %s, want RECEIVED (terminal)", loaded.State())
	}
	if len(loaded.StowAllocations()) != 1 || loaded.StowAllocations()[0].BinID != "BIN-ITEST-DEST" {
		t.Fatalf("stow allocations = %+v", loaded.StowAllocations())
	}
	fullAudit := loaded.Audit()
	wantTail := []string{"TransferPicked", "TransferDispatched", "TransferReceiptStaged", "TransferStockStowed"}
	if len(fullAudit) < len(wantTail) {
		t.Fatalf("audit = %d entries: %+v", len(fullAudit), fullAudit)
	}
	tail := fullAudit[len(fullAudit)-len(wantTail):]
	for i, want := range wantTail {
		if tail[i].Event != want {
			t.Fatalf("audit tail[%d] = %q, want %q (full: %+v)", i, tail[i].Event, want, fullAudit)
		}
	}

	// 8. Replays of every fact (fresh groups) are no-ops: the terminal
	// state and the audit trail are unchanged, nothing new is relayed.
	replayFacts := inboundkafka.NewTransferFactConsumer(brokers, uniqueGroupID("transfer-fact-replay"),
		inboundkafka.FactUseCases{
			Pick:       usecases.ApplyTransferPick{Transfers: transfers, Events: outbox, UoW: uow, Release: factAllocateCfg},
			Dispatched: usecases.ApplyTransferDispatched{Transfers: transfers, UoW: uow},
			Arrival:    usecases.ApplyTransferArrival{Transfers: transfers, UoW: uow},
		}, processedEvents, uow, nil)
	runConsumerUntil(t, replayFacts, 15*time.Second)
	replayReplies := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply-replay2"),
		inboundkafka.ReplyUseCases{Allocate: *allocate, Reject: *reject, Staged: *stagedUse, Stow: *stowUse}, processedEvents, uow, nil)
	runConsumerUntil(t, replayReplies, 15*time.Second)

	loaded, err = transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after fact replays: %v", err)
	}
	if loaded.State() != "RECEIVED" || len(loaded.Audit()) != 9 {
		t.Fatalf("after replays: state = %s audit = %d (replays must be no-ops)", loaded.State(), len(loaded.Audit()))
	}
	if n, err := relay.RelayOnce(ctx); err != nil || n != 0 {
		t.Fatalf("replays must not re-release demands: n = %d err = %v", n, err)
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

// syntheticTransferFact builds fulfillment-execution's transfer-fact wire
// shape (its TransferFactData contract, mirrored): data {transfer_ref,
// demand_id, work_unit_id, task_id, work_kind, site_id, sku, quantity},
// subject/key task_id.
func syntheticTransferFact(t *testing.T, fullType, taskID string, transferID fmt.Stringer, workKind string, quantity int, occurredAt time.Time) []byte {
	t.Helper()
	payload := map[string]any{
		"specversion":     "1.0",
		"id":              "itest-fact-" + taskID,
		"source":          "/warehouse/fulfillment-execution",
		"type":            fullType,
		"subject":         taskID,
		"time":            occurredAt.UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:fulfillment-execution:events:" + strings.Split(fullType, ".")[len(strings.Split(fullType, "."))-1] + ":v1",
		"data": map[string]any{
			"transfer_ref": transferID.String(),
			"demand_id":    transferID.String() + ":pick",
			"work_unit_id": "wu-" + taskID,
			"task_id":      taskID,
			"work_kind":    workKind,
			"site_id":      "WH1",
			"sku":          "SKU-1",
			"quantity":     quantity,
		},
	}
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal fact: %v", err)
	}
	return value
}

// syntheticInventoryDestinationFact builds inventory-storage's
// TransferReceiptStaged / TransferStockStowed wire shape: subject/key
// transfer_line_id, data per the agreed receiving contract.
func syntheticInventoryDestinationFact(t *testing.T, fullType string, transferID fmt.Stringer, occurredAt time.Time, stowedQty int) []byte {
	t.Helper()
	eventName := strings.Split(fullType, ".")[len(strings.Split(fullType, "."))-1]
	data := map[string]any{
		"transfer_id":         transferID.String(),
		"transfer_line_id":    transferID.String() + ":1",
		"destination_site_id": "WH2",
		"sku":                 "SKU-1",
		"expected_quantity":   5,
		"received_quantity":   5,
	}
	switch eventName {
	case "TransferReceiptStaged":
		data["variance"] = 0
	case "TransferStockStowed":
		data["stowed_quantity"] = stowedQty
		data["allocations"] = []map[string]any{
			{"stock_unit_id": "su-itest-1", "bin_id": "BIN-ITEST-DEST", "quantity": stowedQty},
		}
	}
	payload := map[string]any{
		"specversion":     "1.0",
		"id":              "itest-dest-" + eventName + "-" + occurredAt.Format(time.RFC3339Nano),
		"source":          "/warehouse/inventory-storage",
		"type":            fullType,
		"subject":         transferID.String() + ":1",
		"time":            occurredAt.UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:inventory-storage:events:" + eventName + ":v1",
		"data":            data,
	}
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal destination fact: %v", err)
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
