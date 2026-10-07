package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// fakeFactApplier records the applied facts.
type fakeFactApplier struct {
	mu         sync.Mutex
	picks      []usecases.ApplyPickInput
	dispatched []usecases.FactInput
	arrivals   []usecases.FactInput
	failPick   int
}

func (f *fakeFactApplier) ApplyPick(_ context.Context, in usecases.ApplyPickInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPick > 0 {
		f.failPick--
		return fmt.Errorf("injected transient failure")
	}
	f.picks = append(f.picks, in)
	return nil
}

func (f *fakeFactApplier) ApplyDispatched(_ context.Context, in usecases.FactInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, in)
	return nil
}

func (f *fakeFactApplier) ApplyArrival(_ context.Context, in usecases.FactInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.arrivals = append(f.arrivals, in)
	return nil
}

// unknownRefApplier simulates the repo's ErrTransferNotFound (an unknown
// transfer_ref): the consumer must WARN + commit past, never retry.
type unknownRefApplier struct{ calls int }

func (u *unknownRefApplier) ApplyPick(_ context.Context, in usecases.ApplyPickInput) error {
	u.calls++
	return transfer.ErrTransferNotFound
}

func (u *unknownRefApplier) ApplyDispatched(_ context.Context, in usecases.FactInput) error {
	u.calls++
	return transfer.ErrTransferNotFound
}

func (u *unknownRefApplier) ApplyArrival(_ context.Context, in usecases.FactInput) error {
	u.calls++
	return transfer.ErrTransferNotFound
}

func newFactConsumer(applier TransferFactApplier, claims ReplyProcessedEvents) *TransferFactConsumer {
	return &TransferFactConsumer{
		Applier:         applier,
		ProcessedEvents: claims,
		UoW:             passUoW{},
		Logger:          quietLogger(),
	}
}

var factOccurred = time.Date(2026, 10, 6, 13, 0, 0, 0, time.UTC)

func factValue(t *testing.T, eventType, taskID string, occurred time.Time, data transferFactData) []byte {
	t.Helper()
	return encodeEvent(t, eventType, taskID, occurred, data)
}

func baseFactData(transferRef string) transferFactData {
	return transferFactData{
		TransferRef: transferRef,
		DemandID:    transferRef + ":pick",
		WorkUnitID:  "wu-1",
		TaskID:      "task-1",
		WorkKind:    "TRANSFER_PICK",
		SiteID:      "WH1",
		SKU:         "SKU-1",
		Quantity:    5,
	}
}

func TestTransferFactConsumerAppliesPicked(t *testing.T) {
	applier := &fakeFactApplier{}
	c := newFactConsumer(applier, newFakeClaims())

	value := factValue(t, typeTransferPicked, "task-1", factOccurred, baseFactData("trf-a"))
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(applier.picks) != 1 {
		t.Fatalf("picks = %d", len(applier.picks))
	}
	got := applier.picks[0]
	if got.TransferID != transfer.TransferID("trf-a") || got.Picked.PickedQuantity != 5 {
		t.Fatalf("applied = %+v", got)
	}
	if !got.OccurredAt.Equal(factOccurred) {
		t.Fatalf("occurred at = %v", got.OccurredAt)
	}

	// Redelivery of the same CE id is a no-op (dedupe claim).
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if len(applier.picks) != 1 {
		t.Fatalf("picks after redelivery = %d, want 1", len(applier.picks))
	}
}

func TestTransferFactConsumerAppliesDispatchedAndArrival(t *testing.T) {
	applier := &fakeFactApplier{}
	c := newFactConsumer(applier, newFakeClaims())

	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferDispatched, "task-2", factOccurred, baseFactData("trf-a"))); err != nil {
		t.Fatalf("dispatched: %v", err)
	}
	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferArrived, "task-3", factOccurred, baseFactData("trf-a"))); err != nil {
		t.Fatalf("arrived: %v", err)
	}
	if len(applier.dispatched) != 1 || applier.dispatched[0].TransferID != transfer.TransferID("trf-a") {
		t.Fatalf("dispatched = %+v", applier.dispatched)
	}
	if len(applier.arrivals) != 1 || applier.arrivals[0].TransferID != transfer.TransferID("trf-a") {
		t.Fatalf("arrivals = %+v", applier.arrivals)
	}
}

func TestTransferFactConsumerUnknownTransferRefCommitsPast(t *testing.T) {
	// The task-required behaviour: a fact whose transfer_ref names no
	// transfer this deployment knows is WARN-logged and committed past —
	// never retried, never crashed on.
	applier := &unknownRefApplier{}
	claims := newFakeClaims()
	c := newFactConsumer(applier, claims)

	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferPicked, "task-9", factOccurred, baseFactData("trf-never-approved"))); err != nil {
		t.Fatalf("unknown transfer_ref must be skipped, got %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("calls = %d", applier.calls)
	}
	// The claim STAYED committed: a redelivery of the same event id is a
	// no-op, not a second apply attempt.
	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferPicked, "task-9", factOccurred, baseFactData("trf-never-approved"))); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("calls after redelivery = %d, want 1 (claim stays committed)", applier.calls)
	}
}

func TestTransferFactConsumerDeterministicSkips(t *testing.T) {
	applier := &fakeFactApplier{}
	c := newFactConsumer(applier, newFakeClaims())

	// Unknown type on the fulfillment topic.
	other := encodeEvent(t, "com.warehouse.wes.fulfillment-execution.task.TaskCompleted", "task-4", factOccurred, map[string]string{"task_id": "task-4"})
	if err := c.HandleMessage(context.Background(), other); err != nil {
		t.Fatalf("unknown type must be ignored: %v", err)
	}
	// Not CloudEvents.
	if err := c.HandleMessage(context.Background(), []byte(`{"event_type":"legacy"}`)); err != nil {
		t.Fatalf("non-CloudEvents must be skipped: %v", err)
	}
	// A pick fact without a positive quantity (quantity is optional on
	// the wire; the short-pick logic needs it).
	noQty := baseFactData("trf-a")
	noQty.Quantity = 0
	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferPicked, "task-5", factOccurred, noQty)); err != nil {
		t.Fatalf("quantity-less pick must be skipped: %v", err)
	}
	if len(applier.picks)+len(applier.dispatched)+len(applier.arrivals) != 0 {
		t.Fatalf("nothing must be applied: %+v", applier)
	}
}

func TestTransferFactConsumerRetriesTransientFailure(t *testing.T) {
	applier := &fakeFactApplier{failPick: 1}
	claims := newFakeClaims()
	c := newFactConsumer(applier, claims)

	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferPicked, "task-6", factOccurred, baseFactData("trf-b"))); err == nil {
		t.Fatal("transient failure must be returned non-nil so the loop retries")
	}
	if len(applier.picks) != 0 {
		t.Fatal("failed handling must apply nothing")
	}
	// The claim rolled back with the unit of work: a retry processes it.
	claims.rollback(transferFactConsumerName, "id-task-6-"+factOccurred.Format(time.RFC3339Nano))
	if err := c.HandleMessage(context.Background(), factValue(t, typeTransferPicked, "task-6", factOccurred, baseFactData("trf-b"))); err != nil {
		t.Fatalf("retry after transient failure: %v", err)
	}
	if len(applier.picks) != 1 {
		t.Fatal("retry must apply the pick")
	}
}
