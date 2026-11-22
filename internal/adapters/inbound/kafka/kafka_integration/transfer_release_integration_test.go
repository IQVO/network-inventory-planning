//go:build integration

package kafka_integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// syntheticAllocationReleased builds inventory-storage's
// TransferStockAllocationReleased wire shape (ADR 0022; subject/key
// transfer_line_id) for the reservation the itest allocation reply made.
func syntheticAllocationReleased(t *testing.T, eventID string, id transfer.TransferID, occurredAt time.Time) []byte {
	t.Helper()
	payload := map[string]any{
		"specversion":     "1.0",
		"id":              eventID,
		"source":          "/warehouse/inventory-storage",
		"type":            "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased",
		"subject":         id.LineID(),
		"time":            occurredAt.UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:inventory-storage:events:TransferStockAllocationReleased:v1",
		"data": map[string]any{
			"transfer_id":      id.String(),
			"transfer_line_id": id.LineID(),
			"reservation_id":   "res-itest-1",
			"origin_site_id":   "WH1",
			"sku":              "SKU-1",
			"quantity":         5,
			"outcome":          "RELEASED",
			"reason":           "revoked for cancelled transfer",
		},
	}
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal released fact: %v", err)
	}
	return value
}

// TestCancelledAllocatedTransferSettlesOnceAndARedeliveryIsANoOp is the
// consumer-level regression for the e2e scenario "Cancelling an ALLOCATED
// transfer": against a REAL Postgres (the real CHECK constraints), the
// TransferStockAllocationReleased fact that used to fail forever with
// SQLSTATE 23514 now settles RELEASING -> CANCELLED exactly once, and both a
// redelivery of the same CloudEvent and a re-emitted fact (new event id) are
// no-ops that return nil — the consume loop never retries them.
func TestCancelledAllocatedTransferSettlesOnceAndARedeliveryIsANoOp(t *testing.T) {
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
	outbox := postgres.NewOutboxWriter(pool, outboundkafka.NewTransferEncoder())

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
		PickPathID:        "transfer-pick-path",
		PickCPTOffset:     2 * time.Hour,
		DispatchPathID:    "transfer-dispatch-path",
		DispatchCPTOffset: 3 * time.Hour,
	}
	approve := &usecases.ApproveTransfer{
		Transfers: transfers, Events: outbox, Snapshot: postgres.NewSnapshotRepo(pool), UoW: uow,
		MaxStaleness: 10 * time.Minute, Release: release, Now: func() time.Time { return itNow },
	}
	result, err := approve.Execute(ctx, usecases.ApproveTransferInput{
		IdempotencyKey: "itest-release-1", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1",
		Quantity: 5, PolicyVersion: "policy-v3", OperatorReason: "integration rebalance",
		ProposalAsOf: itNow.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	// The reply consumer, wired exactly like the composition root minus the
	// Kafka reader: HandleMessage is the unit the consume loop retries.
	consumer := &inboundkafka.TransferReplyConsumer{
		Applier: inboundkafka.ReplyUseCases{
			Allocate: usecases.ApplyTransferAllocation{Transfers: transfers, Events: outbox, UoW: uow, Release: release, Now: func() time.Time { return itNow }},
			Released: usecases.ApplyTransferAllocationReleased{Transfers: transfers, Events: outbox, UoW: uow},
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(pool),
		UoW:             uow,
		Logger:          slog.Default(),
	}

	if err := consumer.HandleMessage(ctx, syntheticInventoryReply(t, result.TransferID, itNow.Add(time.Minute))); err != nil {
		t.Fatalf("apply allocation reply: %v", err)
	}
	cancel := usecases.CancelTransfer{Transfers: transfers, Events: outbox, UoW: uow, Now: func() time.Time { return itNow.Add(2 * time.Minute) }}
	cancelled, err := cancel.Execute(ctx, usecases.CancelTransferInput{TransferID: result.TransferID, Reason: "wrong lane selected"})
	if err != nil || !cancelled.Async {
		t.Fatalf("cancel an ALLOCATED transfer: async=%v err=%v", cancelled.Async, err)
	}
	releasing, err := transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if releasing.State() != transfer.StateReleasing || releasing.ReservationID() != "res-itest-1" {
		t.Fatalf("after cancel = state %s reservation %q, want RELEASING holding res-itest-1", releasing.State(), releasing.ReservationID())
	}

	// The message that used to fail forever with SQLSTATE 23514.
	msg := syntheticAllocationReleased(t, "itest-released-1", result.TransferID, itNow.Add(3*time.Minute))
	if err := consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("the release fact must apply (was a retried-forever SQLSTATE 23514): %v", err)
	}
	settled, err := transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load settled: %v", err)
	}
	if settled.State() != transfer.StateCancelled || settled.ReservationID() != "" {
		t.Fatalf("settled = state %s reservation %q, want CANCELLED with no reservation", settled.State(), settled.ReservationID())
	}
	version, auditLen := settled.Version(), len(settled.Audit())

	// Redelivery of the SAME CloudEvent: the processed-event claim dedupes.
	if err := consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("redelivered fact must be a no-op, got %v", err)
	}
	// A re-emitted fact (new event id): an illegal transition, skipped
	// deterministically (nil), never retried.
	reemitted := syntheticAllocationReleased(t, "itest-released-2", result.TransferID, itNow.Add(4*time.Minute))
	if err := consumer.HandleMessage(ctx, reemitted); err != nil {
		t.Fatalf("re-emitted fact must be skipped, got %v", err)
	}

	after, err := transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load after redelivery: %v", err)
	}
	if after.State() != transfer.StateCancelled || after.Version() != version || len(after.Audit()) != auditLen {
		t.Fatalf("redelivery changed the transfer: state %s version %d->%d audit %d->%d",
			after.State(), version, after.Version(), auditLen, len(after.Audit()))
	}
}
