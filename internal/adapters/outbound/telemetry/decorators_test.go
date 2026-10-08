package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var decoratorNow = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

// fakeRepo mimics the Postgres repository's version contract: version ==
// number of persisted audit entries, and a repeated idempotency key
// returns the first aggregate.
type fakeRepo struct {
	byKey     map[string]*transfer.InterWarehouseTransfer
	updateErr error
}

func (f *fakeRepo) Create(_ context.Context, t *transfer.InterWarehouseTransfer, key string) (*transfer.InterWarehouseTransfer, error) {
	if existing, ok := f.byKey[key]; ok {
		return existing, nil
	}
	f.byKey[key] = t
	t.SetVersion(int64(len(t.Audit())))
	return nil, nil
}

func (f *fakeRepo) Load(context.Context, transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	return nil, errors.New("not used")
}

func (f *fakeRepo) UpdateState(_ context.Context, t *transfer.InterWarehouseTransfer) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	t.SetVersion(int64(len(t.Audit())))
	return nil
}

func allocatingTransfer(t *testing.T, id string) *transfer.InterWarehouseTransfer {
	t.Helper()
	trf, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id,
		OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 5,
		PolicyVersion: "policy-v1", OperatorReason: "rebalance",
		ProposalAsOf: decoratorNow.Add(-time.Minute), ExpiresAt: decoratorNow.Add(24 * time.Hour), Now: decoratorNow,
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if _, err := trf.Approve(decoratorNow); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := trf.RequestAllocation(decoratorNow); err != nil {
		t.Fatalf("request allocation: %v", err)
	}
	return trf
}

func TestMeteredRepositoryCountsEveryTransitionOnce(t *testing.T) {
	m, collect := newTestMetrics(t)
	repo := NewMeteredTransferRepository(&fakeRepo{byKey: map[string]*transfer.InterWarehouseTransfer{}}, m)
	ctx := context.Background()

	trf := allocatingTransfer(t, "trf-1")
	existing, err := repo.Create(ctx, trf, "key-trf-1")
	if err != nil || existing != nil {
		t.Fatalf("create = (%v, %v)", existing, err)
	}
	// Creation persisted DRAFT, PROPOSED, APPROVED, ALLOCATING.
	assertSeries(t, stateAdvancedCounterName, collect()[stateAdvancedCounterName], map[string]int64{
		"to=DRAFT": 1, "to=PROPOSED": 1, "to=APPROVED": 1, "to=ALLOCATING": 1,
	})

	// A later transition: only the NEW audit entry is counted.
	if err := trf.MarkUnfulfillable(trf.ID().LineID(), transfer.RejectionInsufficientUsable, decoratorNow); err != nil {
		t.Fatalf("mark unfulfillable: %v", err)
	}
	if err := repo.UpdateState(ctx, trf); err != nil {
		t.Fatalf("update: %v", err)
	}
	assertSeries(t, stateAdvancedCounterName, collect()[stateAdvancedCounterName], map[string]int64{
		"to=DRAFT": 1, "to=PROPOSED": 1, "to=APPROVED": 1, "to=ALLOCATING": 1, "to=UNFULFILLABLE": 1,
	})

	// An update that persists no new entry counts nothing.
	if err := repo.UpdateState(ctx, trf); err != nil {
		t.Fatalf("idempotent update: %v", err)
	}
	if got := collect()[stateAdvancedCounterName]["to=UNFULFILLABLE"]; got != 1 {
		t.Fatalf("UNFULFILLABLE = %d after a no-op update, want 1", got)
	}
}

func TestMeteredRepositoryIgnoresReplayAndFailure(t *testing.T) {
	m, collect := newTestMetrics(t)
	inner := &fakeRepo{byKey: map[string]*transfer.InterWarehouseTransfer{}}
	repo := NewMeteredTransferRepository(inner, m)
	ctx := context.Background()

	first := allocatingTransfer(t, "trf-1")
	if _, err := repo.Create(ctx, first, "dup"); err != nil {
		t.Fatalf("create: %v", err)
	}
	before := collect()[stateAdvancedCounterName]["to=DRAFT"]

	// Same idempotency key: the repository returns the first aggregate and
	// persists nothing, so nothing is counted.
	second := allocatingTransfer(t, "trf-2")
	existing, err := repo.Create(ctx, second, "dup")
	if err != nil || existing == nil {
		t.Fatalf("replayed create = (%v, %v), want the first transfer", existing, err)
	}
	if got := collect()[stateAdvancedCounterName]["to=DRAFT"]; got != before {
		t.Fatalf("DRAFT = %d after a replay, want %d", got, before)
	}

	// A failed update counts nothing.
	if err := first.MarkUnfulfillable(first.ID().LineID(), transfer.RejectionInsufficientUsable, decoratorNow); err != nil {
		t.Fatalf("mark unfulfillable: %v", err)
	}
	inner.updateErr = errors.New("db down")
	if err := repo.UpdateState(ctx, first); err == nil {
		t.Fatal("update must surface the inner error")
	}
	if got := collect()[stateAdvancedCounterName]["to=UNFULFILLABLE"]; got != 0 {
		t.Fatalf("UNFULFILLABLE = %d after a failed update, want 0", got)
	}
}

func TestMeteredRepositoryNilMetricsReturnsInner(t *testing.T) {
	inner := &fakeRepo{byKey: map[string]*transfer.InterWarehouseTransfer{}}
	if got := NewMeteredTransferRepository(inner, nil); got != ports.TransferRepository(inner) {
		t.Fatal("nil metrics must return the inner repository unchanged")
	}
}

type fakeSink struct{ err error }

func (f fakeSink) Send(context.Context, outboundkafka.Encoded) error { return f.err }

func TestMeteredRelaySinkCountsPublishedAndFailed(t *testing.T) {
	m, collect := newTestMetrics(t)
	ctx := context.Background()

	ok := NewMeteredRelaySink(fakeSink{}, m)
	for range 2 {
		if err := ok.Send(ctx, outboundkafka.Encoded{}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	boom := errors.New("broker unreachable")
	bad := NewMeteredRelaySink(fakeSink{err: boom}, m)
	if err := bad.Send(ctx, outboundkafka.Encoded{}); !errors.Is(err, boom) {
		t.Fatalf("failed send must return the sink error unchanged, got %v", err)
	}

	assertSeries(t, outboxRelayedCounterName, collect()[outboxRelayedCounterName], map[string]int64{
		"outcome=published": 2, "outcome=failed": 1,
	})
}

func TestMeteredRelaySinkNilMetricsReturnsInner(t *testing.T) {
	inner := fakeSink{}
	if got := NewMeteredRelaySink(inner, nil); got != RelaySink(inner) {
		t.Fatal("nil metrics must return the inner sink unchanged")
	}
}
