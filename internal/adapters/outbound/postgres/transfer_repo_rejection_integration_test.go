//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TestUpdateStatePersistsRejectionWithoutReservation is the regression for the
// rejection path: a transfer that never got a reservation (ALLOCATING ->
// UNFULFILLABLE) has an EMPTY reservation id. UpdateState used to write that as
// ” instead of NULL, which violates inter_warehouse_transfer_reservation_id_check
// (SQLSTATE 23514), so the reply consumer retried the rejection forever and the
// transfer stayed ALLOCATING. Found by the e2e transfer scenario.
func TestUpdateStatePersistsRejectionWithoutReservation(t *testing.T) {
	pool := startPostgres(t)
	repo := postgres.NewTransferRepo(pool)
	ctx := context.Background()

	tr, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: "trf-reject-1", IdempotencyKey: "key-trf-reject-1", OriginSiteID: "WH1", DestinationSiteID: "WH2",
		SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance",
		ProposalAsOf: base, ExpiresAt: base.Add(24 * time.Hour), Now: base,
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if _, err := tr.Approve(base.Add(time.Minute)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := tr.RequestAllocation(base.Add(2 * time.Minute)); err != nil {
		t.Fatalf("request allocation: %v", err)
	}
	if existing, err := repo.Create(ctx, tr, "key-trf-reject-1"); err != nil || existing != nil {
		t.Fatalf("create: existing=%v err=%v", existing, err)
	}

	loaded, err := repo.Load(ctx, tr.ID())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := loaded.MarkUnfulfillable(loaded.ID().LineID(), transfer.RejectionInsufficientUsable, base.Add(3*time.Minute)); err != nil {
		t.Fatalf("mark unfulfillable: %v", err)
	}
	if err := repo.UpdateState(ctx, loaded); err != nil {
		t.Fatalf("UpdateState on a rejection (no reservation) must succeed: %v", err)
	}

	after, err := repo.Load(ctx, tr.ID())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if after.State() != transfer.StateUnfulfillable {
		t.Fatalf("state = %s, want %s", after.State(), transfer.StateUnfulfillable)
	}
	if after.ReservationID() != "" {
		t.Fatalf("reservation id = %q, want empty (NULL)", after.ReservationID())
	}
}
