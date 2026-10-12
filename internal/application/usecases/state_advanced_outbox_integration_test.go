//go:build integration

package usecases_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// stateAdvancedType is the full CloudEvents type the outbox row carries.
const stateAdvancedType = "com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced"

// outboxTransitions reads the TransferStateAdvanced rows the REAL outbox
// writer stored for one transfer, in insertion order, as "from>to".
func outboxTransitions(t *testing.T, f *usecasesFixture, id transfer.TransferID) []string {
	t.Helper()
	rows, err := f.pool.Query(context.Background(),
		`SELECT topic, value FROM outbox_events WHERE event_type = $1 AND key = $2 ORDER BY id`, stateAdvancedType, []byte(id))
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var topic string
		var value []byte
		if err := rows.Scan(&topic, &value); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		if topic != outboundkafka.AnalyticsTopic {
			t.Fatalf("TransferStateAdvanced on topic %q, want %q", topic, outboundkafka.AnalyticsTopic)
		}
		var envelope struct {
			SpecVersion string `json:"specversion"`
			Subject     string `json:"subject"`
			Data        struct {
				TransferID string `json:"transfer_id"`
				From       string `json:"from"`
				To         string `json:"to"`
			} `json:"data"`
		}
		if err := json.Unmarshal(value, &envelope); err != nil {
			t.Fatalf("decode outbox row: %v", err)
		}
		if envelope.SpecVersion != "1.0" || envelope.Subject != string(id) || envelope.Data.TransferID != string(id) {
			t.Fatalf("outbox row is not a CloudEvents 1.0 occurrence of %s: %s", id, value)
		}
		out = append(out, envelope.Data.From+">"+envelope.Data.To)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func assertTransitions(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("TransferStateAdvanced rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TransferStateAdvanced[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

// TestUsecases_EverySagaTransitionOutboxesStateAdvanced is D3 (ADR 0007,
// asyncapi TransferStateAdvanced): EVERY saga transition — the approval
// trail, ALLOCATED, PICKED, IN_TRANSIT, ARRIVED, RECEIVED — writes exactly
// one TransferStateAdvanced row to outbox_events through the REAL outbox
// writer (both encoders, as the composition root wires it), in the same
// transaction as the state change, so downstream readers see every move.
func TestUsecases_EverySagaTransitionOutboxesStateAdvanced(t *testing.T) {
	f := newUsecasesFixture(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(f.pool)
	outbox := postgres.NewOutboxWriter(f.pool, outboundkafka.NewTransferEncoder(), outboundkafka.NewAnalyticsEncoder())
	f.approve.Events = outbox

	result, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-d3-happy"))
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	id := result.TransferID
	at := usecasesNow.Add(time.Minute)

	steps := []struct {
		name string
		run  func() error
	}{
		{"allocated", func() error {
			return usecases.ApplyTransferAllocation{Transfers: f.transfers, Events: outbox, UoW: uow, Release: usecasesReleaseConfig(), Now: func() time.Time { return at }}.
				Execute(ctx, usecases.ApplyAllocationInput{TransferID: id, OccurredAt: at, Allocation: transfer.StockAllocation{
					TransferLineID: id.LineID(), OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-d3", Quantity: 5,
					Allocations: []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 5}}, ExpiresAt: at.Add(24 * time.Hour),
				}})
		}},
		{"picked", func() error {
			return usecases.ApplyTransferPick{Transfers: f.transfers, Events: outbox, UoW: uow, Release: usecasesReleaseConfig()}.
				Execute(ctx, usecases.ApplyPickInput{TransferID: id, Picked: transfer.Picked{PickedQuantity: 5}, OccurredAt: at.Add(time.Minute)})
		}},
		{"dispatched", func() error {
			return usecases.ApplyTransferDispatched{Transfers: f.transfers, Events: outbox, UoW: uow}.
				Execute(ctx, usecases.FactInput{TransferID: id, OccurredAt: at.Add(2 * time.Minute)})
		}},
		{"receipt staged", func() error {
			return usecases.ApplyTransferReceiptStaged{Transfers: f.transfers, Events: outbox, UoW: uow}.
				Execute(ctx, usecases.ApplyReceiptStagedInput{TransferID: id, LineID: id.LineID(), OccurredAt: at.Add(3 * time.Minute)})
		}},
		{"stowed", func() error {
			return usecases.ApplyTransferStow{Transfers: f.transfers, Events: outbox, UoW: uow}.
				Execute(ctx, usecases.ApplyStowInput{TransferID: id, OccurredAt: at.Add(4 * time.Minute), Stowed: transfer.Stowed{
					TransferLineID: id.LineID(), DestinationSite: "WH2", SKU: "SKU-1", ReceivedQuantity: 5, StowedQuantity: 5,
					Allocations: []transfer.StowAllocation{{StockUnitID: "su-9", BinID: "bin-dest", Quantity: 5}},
				}})
		}},
	}
	for _, s := range steps {
		if err := s.run(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
	}

	assertTransitions(t, outboxTransitions(t, f, id), []string{
		">DRAFT", "DRAFT>PROPOSED", "PROPOSED>APPROVED", "APPROVED>ALLOCATING",
		"ALLOCATING>ALLOCATED", "ALLOCATED>PICKED", "PICKED>IN_TRANSIT", "IN_TRANSIT>ARRIVED", "ARRIVED>RECEIVED",
	})
}

// TestUsecases_RejectionOutboxesStateAdvanced covers the other terminal
// leg: ALLOCATING → UNFULFILLABLE is announced like every other move.
func TestUsecases_RejectionOutboxesStateAdvanced(t *testing.T) {
	f := newUsecasesFixture(t)
	ctx := context.Background()
	outbox := postgres.NewOutboxWriter(f.pool, outboundkafka.NewTransferEncoder(), outboundkafka.NewAnalyticsEncoder())
	f.approve.Events = outbox

	result, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-d3-reject"))
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	err = usecases.ApplyTransferRejection{Transfers: f.transfers, Events: outbox, UoW: postgres.NewUnitOfWork(f.pool)}.
		Execute(ctx, usecases.ApplyRejectionInput{TransferID: result.TransferID, LineID: result.TransferID.LineID(),
			Reason: transfer.RejectionInsufficientUsable, OccurredAt: usecasesNow.Add(time.Minute)})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}
	assertTransitions(t, outboxTransitions(t, f, result.TransferID), []string{
		">DRAFT", "DRAFT>PROPOSED", "PROPOSED>APPROVED", "APPROVED>ALLOCATING", "ALLOCATING>UNFULFILLABLE",
	})
}
