//go:build integration

package kafka_integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// The broker comes from testcontainers (startKafkaBroker); the import pins
// that for TestKafkaIntegrationTestsUseTestcontainers.
var _ = tckafka.Run

// TestTransferReplyPoisonIsDeadLetteredAndTheNextReplyApplies is D4
// against REAL Kafka + Postgres: two POISON replies (an allocation and a
// rejection addressed to a foreign transfer line) are published ahead of
// the good allocation reply on the SAME partition (same key). Before the
// fix the first poison was an unclassified error and the consumer retried
// it forever, so the good reply behind it never applied. Now each poison is
// dead-lettered raw to warehouse.inventory.events.dlq and committed past,
// and the good reply drives the saga to ALLOCATED.
func TestTransferReplyPoisonIsDeadLetteredAndTheNextReplyApplies(t *testing.T) {
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
	transfers := postgres.NewTransferRepo(pool)
	outbox := postgres.NewOutboxWriter(pool, outboundkafka.NewTransferEncoder(), outboundkafka.NewAnalyticsEncoder())
	ensureTopic(t, brokers, inboundkafka.InventoryTopic)

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

	release := usecases.WorkReleaseConfig{
		PickPathID: "transfer-pick-path", PickCPTOffset: 2 * time.Hour,
		DispatchPathID: "transfer-dispatch-path", DispatchCPTOffset: 3 * time.Hour,
	}
	approve := &usecases.ApproveTransfer{
		Transfers: transfers, Events: outbox, Snapshot: postgres.NewSnapshotRepo(pool), UoW: uow,
		MaxStaleness: 10 * time.Minute, Release: release, Now: func() time.Time { return itNow },
	}
	result, err := approve.Execute(ctx, usecases.ApproveTransferInput{
		IdempotencyKey: "itest-poison-1", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1",
		Quantity: 5, PolicyVersion: "policy-v3", OperatorReason: "poison reply test",
		ProposalAsOf: itNow.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	replyAt := itNow.Add(time.Minute)
	key := string(result.TransferID) + ":1"
	poisonAllocated := rewriteReply(t, syntheticInventoryReply(t, result.TransferID, replyAt), "itest-poison-allocated", string(result.TransferID)+":2", "")
	poisonRejected := rewriteReply(t, syntheticInventoryReply(t, result.TransferID, replyAt), "itest-poison-rejected", string(result.TransferID)+":2",
		"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected")
	produceRaw(t, brokers, inboundkafka.InventoryTopic, key, poisonAllocated)
	produceRaw(t, brokers, inboundkafka.InventoryTopic, key, poisonRejected)
	produceRaw(t, brokers, inboundkafka.InventoryTopic, key, syntheticInventoryReply(t, result.TransferID, replyAt))

	consumer := inboundkafka.NewTransferReplyConsumer(brokers, uniqueGroupID("transfer-reply-poison"),
		inboundkafka.ReplyUseCases{
			Allocate: usecases.ApplyTransferAllocation{Transfers: transfers, Events: outbox, UoW: uow, Release: release, Now: func() time.Time { return itNow }},
			Reject:   usecases.ApplyTransferRejection{Transfers: transfers, Events: outbox, UoW: uow},
			Staged:   usecases.ApplyTransferReceiptStaged{Transfers: transfers, Events: outbox, UoW: uow},
			Stow:     usecases.ApplyTransferStow{Transfers: transfers, Events: outbox, UoW: uow},
		}, postgres.NewProcessedEventRepo(pool), uow, nil)
	runConsumerUntil(t, consumer, 20*time.Second)

	loaded, err := transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.State() != transfer.StateAllocated || loaded.ReservationID() != "res-itest-1" {
		t.Fatalf("state = %s reservation = %q, want ALLOCATED behind the dead-lettered poison", loaded.State(), loaded.ReservationID())
	}

	dead := readDLQ(t, brokers, inboundkafka.InventoryTopic+inboundkafka.DLQSuffix, 2)
	for i, want := range [][]byte{poisonAllocated, poisonRejected} {
		if string(dead[i].Value) != string(want) {
			t.Fatalf("DLQ[%d] value differs from the poison message:\n got  %s\n want %s", i, dead[i].Value, want)
		}
		headers := map[string]string{}
		for _, h := range dead[i].Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["x-dlq-source-topic"] != inboundkafka.InventoryTopic || headers["x-dlq-consumer"] != "transfer-reply-consumer" || headers["x-dlq-error"] == "" {
			t.Fatalf("DLQ[%d] headers = %v", i, headers)
		}
	}
}

// rewriteReply re-ids a synthetic reply, points it at another line and,
// when newType is set, turns it into that type (a rejection carries a
// valid closed reason so only the line is wrong).
func rewriteReply(t *testing.T, value []byte, id, lineID, newType string) []byte {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(value, &envelope); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	envelope["id"] = id
	data := envelope["data"].(map[string]any)
	data["transfer_line_id"] = lineID
	if newType != "" {
		envelope["type"] = newType
		envelope["dataschema"] = "urn:warehouse:inventory-storage:events:TransferStockAllocationRejected:v1"
		data["requested_quantity"] = 5
		data["reason"] = "INSUFFICIENT_USABLE"
	}
	out, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode reply: %v", err)
	}
	return out
}

// readDLQ reads want messages off a dead-letter topic (fresh group, from
// the beginning).
func readDLQ(t *testing.T, brokers []string, topic string, want int) []kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("dlq-read"), MinBytes: 1, MaxBytes: 10e6,
	})
	defer func() { _ = reader.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []kafkago.Message
	for len(out) < want {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("read DLQ %s: %v (got %d of %d)", topic, err, len(out), want)
		}
		out = append(out, msg)
	}
	return out
}
