//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// reservationStep is one saga transition applied to a loaded aggregate.
type reservationStep struct {
	name string
	do   func(trf *transfer.InterWarehouseTransfer, at func(minutes int) time.Time) error
}

// TestEveryReservationTransitionPersistsAgainstTheRealConstraint drives
// every transition the aggregate has through the REAL repository against
// the REAL inter_warehouse_transfer_reservation_id_check (migration 0009),
// persisting after each step. It is the regression for cancelling an
// ALLOCATED transfer: the TransferStockAllocationReleased fact moved the
// transfer RELEASING -> CANCELLED but kept its reservation id, and
// UpdateState failed with SQLSTATE 23514, so the reply consumer retried the
// fact forever and the transfer stayed RELEASING instead of CANCELLED.
func TestEveryReservationTransitionPersistsAgainstTheRealConstraint(t *testing.T) {
	pool := startPostgres(t)
	repo := postgres.NewTransferRepo(pool)
	ctx := context.Background()

	released := func(outcome transfer.ReleaseOutcome) reservationStep {
		return reservationStep{"mark released " + string(outcome), func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
			return trf.MarkReleased(transfer.StockAllocationReleased{
				TransferLineID: trf.ID().LineID(),
				ReservationID:  "res-1",
				OriginSiteID:   "WH1",
				SKU:            "SKU-1",
				Quantity:       10,
				Outcome:        outcome,
				Reason:         "revoked for cancelled transfer",
			}, at(4))
		}}
	}
	approve := reservationStep{"approve", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		_, err := trf.Approve(at(1))
		return err
	}}
	request := reservationStep{"request allocation", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		_, err := trf.RequestAllocation(at(2))
		return err
	}}
	allocate := reservationStep{"mark allocated", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkAllocated(transfer.StockAllocation{
			TransferLineID: trf.ID().LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-1",
			Quantity:       10,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 10}},
			ExpiresAt:      at(3).Add(24 * time.Hour),
		}, at(3))
	}}
	reject := reservationStep{"mark unfulfillable", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkUnfulfillable(trf.ID().LineID(), transfer.RejectionInsufficientUsable, at(3))
	}}
	cancel := reservationStep{"cancel", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.Cancel("operator changed mind", at(3))
	}}
	cancelAfter := reservationStep{"cancel after allocation", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		_, err := trf.CancelAfterAllocation("operator changed mind", at(4))
		return err
	}}
	pick := reservationStep{"mark picked", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkPicked(transfer.Picked{PickedQuantity: 10}, at(4))
	}}
	dispatch := reservationStep{"mark dispatched", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkDispatched(at(5))
	}}
	arrive := reservationStep{"mark arrived", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkArrived("TransferArrived", at(6))
	}}
	stow := reservationStep{"mark stowed", func(trf *transfer.InterWarehouseTransfer, at func(int) time.Time) error {
		return trf.MarkStowed(transfer.Stowed{
			TransferLineID:   trf.ID().LineID(),
			DestinationSite:  "WH2",
			SKU:              "SKU-1",
			ReceivedQuantity: 10,
			StowedQuantity:   10,
			Allocations:      []transfer.StowAllocation{{StockUnitID: "su-9", BinID: "bin-9", Quantity: 10}},
		}, at(7))
	}}

	paths := []struct {
		name  string
		steps []reservationStep
	}{
		{"cancelled while proposed", []reservationStep{cancel}},
		{"cancelled while approved", []reservationStep{approve, cancel}},
		{"cancelled while allocating", []reservationStep{approve, request, cancel}},
		{"rejected", []reservationStep{approve, request, reject}},
		{"released cancel", []reservationStep{approve, request, allocate, cancelAfter, released(transfer.ReleaseOutcomeReleased)}},
		{"already resolved cancel", []reservationStep{approve, request, allocate, cancelAfter, released(transfer.ReleaseOutcomeAlreadyResolved)}},
		{"full delivery", []reservationStep{approve, request, allocate, pick, dispatch, arrive, stow}},
	}
	for i, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			id := transfer.TransferID(fmt.Sprintf("trf-reservation-%d", i))
			key := "key-" + string(id)
			at := func(minutes int) time.Time { return base.Add(time.Duration(minutes) * time.Minute) }

			trf, err := transfer.ProposeTransfer(transfer.ProposalInput{
				ID: id, IdempotencyKey: key, OriginSiteID: "WH1", DestinationSiteID: "WH2",
				SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance",
				ProposalAsOf: base, ExpiresAt: base.Add(24 * time.Hour), Now: base,
			})
			if err != nil {
				t.Fatalf("propose: %v", err)
			}
			if existing, err := repo.Create(ctx, trf, key); err != nil || existing != nil {
				t.Fatalf("create: existing=%v err=%v", existing, err)
			}

			for _, s := range path.steps {
				loaded, err := repo.Load(ctx, id)
				if err != nil {
					t.Fatalf("before %s: load: %v", s.name, err)
				}
				if err := s.do(loaded, at); err != nil {
					t.Fatalf("%s: %v", s.name, err)
				}
				if err := repo.UpdateState(ctx, loaded); err != nil {
					t.Fatalf("%s: UpdateState must succeed against the real CHECK constraints: %v", s.name, err)
				}
			}

			final, err := repo.Load(ctx, id)
			if err != nil {
				t.Fatalf("final load: %v", err)
			}
			var reservationIsNull bool
			if err := pool.QueryRow(ctx,
				`SELECT reservation_id IS NULL FROM inter_warehouse_transfer WHERE transfer_id = $1`, string(id),
			).Scan(&reservationIsNull); err != nil {
				t.Fatalf("read reservation_id: %v", err)
			}
			if final.State() == transfer.StateCancelled && !reservationIsNull {
				t.Fatalf("a CANCELLED transfer must persist with reservation_id NULL, got %q", final.ReservationID())
			}
			if final.State() == transfer.StateReceived && reservationIsNull {
				t.Fatal("a RECEIVED transfer keeps the reservation it consumed")
			}
		})
	}
}

// TestSettledReleaseRoundTripsAsCancelledWithoutReservation is the focused
// regression: the exact RELEASING -> CANCELLED write the failing e2e
// scenario performed ("Cancelling an ALLOCATED transfer").
func TestSettledReleaseRoundTripsAsCancelledWithoutReservation(t *testing.T) {
	pool := startPostgres(t)
	repo := postgres.NewTransferRepo(pool)
	ctx := context.Background()

	trf, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: "trf-release-1", IdempotencyKey: "key-trf-release-1", OriginSiteID: "WH1", DestinationSiteID: "WH2",
		SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance",
		ProposalAsOf: base, ExpiresAt: base.Add(24 * time.Hour), Now: base,
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if _, err := trf.Approve(base.Add(time.Minute)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := trf.RequestAllocation(base.Add(2 * time.Minute)); err != nil {
		t.Fatalf("request allocation: %v", err)
	}
	if existing, err := repo.Create(ctx, trf, "key-trf-release-1"); err != nil || existing != nil {
		t.Fatalf("create: existing=%v err=%v", existing, err)
	}

	loaded, err := repo.Load(ctx, trf.ID())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := loaded.MarkAllocated(transfer.StockAllocation{
		TransferLineID: loaded.ID().LineID(), OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-1", Quantity: 10,
		Allocations: []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 10}},
		ExpiresAt:   base.Add(24 * time.Hour),
	}, base.Add(3*time.Minute)); err != nil {
		t.Fatalf("mark allocated: %v", err)
	}
	if err := repo.UpdateState(ctx, loaded); err != nil {
		t.Fatalf("persist ALLOCATED: %v", err)
	}
	if _, err := loaded.CancelAfterAllocation("wrong lane selected", base.Add(4*time.Minute)); err != nil {
		t.Fatalf("cancel after allocation: %v", err)
	}
	if err := repo.UpdateState(ctx, loaded); err != nil {
		t.Fatalf("persist RELEASING (still holds the reservation): %v", err)
	}

	releasing, err := repo.Load(ctx, trf.ID())
	if err != nil {
		t.Fatalf("load RELEASING: %v", err)
	}
	if releasing.State() != transfer.StateReleasing || releasing.ReservationID() != "res-1" {
		t.Fatalf("RELEASING = state %s reservation %q, want RELEASING holding res-1", releasing.State(), releasing.ReservationID())
	}
	if err := releasing.MarkReleased(transfer.StockAllocationReleased{
		TransferLineID: releasing.ID().LineID(), ReservationID: "res-1", OriginSiteID: "WH1", SKU: "SKU-1", Quantity: 10,
		Outcome: transfer.ReleaseOutcomeReleased, Reason: "revoked",
	}, base.Add(5*time.Minute)); err != nil {
		t.Fatalf("mark released: %v", err)
	}
	if err := repo.UpdateState(ctx, releasing); err != nil {
		t.Fatalf("persisting the released (CANCELLED) transfer must succeed, was SQLSTATE 23514: %v", err)
	}

	after, err := repo.Load(ctx, trf.ID())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if after.State() != transfer.StateCancelled || after.ReservationID() != "" || len(after.Allocations()) != 0 {
		t.Fatalf("reloaded = state %s reservation %q allocations %+v, want CANCELLED with no hold", after.State(), after.ReservationID(), after.Allocations())
	}
	if audit := after.Audit(); len(audit) != 7 || audit[6].To != transfer.StateCancelled {
		t.Fatalf("audit = %+v, want 7 entries ending in CANCELLED", audit)
	}
}
