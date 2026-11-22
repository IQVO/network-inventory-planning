package kafka

// Event consumer contract tests (E1-E4) for the inventory-storage edge:
// TransferReplyConsumer subscribes to inventory-storage's shared topic
// warehouse.inventory.events (InventoryTopic wired into readerConfig by
// NewTransferReplyConsumer) and acts only on the transfer-saga replies
// and facts: TransferStockAllocated, TransferStockAllocationRejected,
// TransferStockAllocationReleased (ADR 0022), TransferReceiptStaged and
// TransferStockStowed (ADR 0005). The contract is the PINNED copy of
// inventory-storage's own apis/asyncapi.yaml
// (testdata/inventory-storage.asyncapi.yaml, contracts/pinned.json; the
// pinned-contracts CI job fails when the copy drifts from
// IQVO/inventory-storage@develop).
//
// Golden fixtures are the producer spec's OWN examples, each delivered
// with a freshly minted CloudEvents id so every routed type exercises
// its own dispatch (the spec's examples are free to reuse ids across
// messages — the consumer dedupes on (consumer, id) regardless of type).

import (
	"context"
	"testing"
)

// inventoryContractConsumer builds the REAL consumer over the in-package
// recording applier double.
func inventoryContractConsumer(applier *fakeReplyApplier, claims *fakeClaims) *TransferReplyConsumer {
	return &TransferReplyConsumer{
		Applier:         applier,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
	}
}

// inventoryGoldenExample returns the pinned producer spec's own example
// for one routed type (each documents exactly one).
func inventoryGoldenExample(t *testing.T, routedType string) pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	name, ok := doc.documentedTypes(t, InventoryTopic)[routedType]
	if !ok {
		t.Fatalf("pinned inventory-storage spec does not document %s on %s", routedType, InventoryTopic)
	}
	examples := doc.goldenExamples(t, InventoryTopic, name)
	if len(examples) == 0 {
		t.Fatalf("pinned inventory-storage spec documents %s without examples", routedType)
	}
	return examples[0]
}

// inventoryRoutedTypes is the consumer's full dispatch table.
var inventoryRoutedTypes = []string{
	typeTransferStockAllocated,
	typeTransferStockAllocationRejected,
	typeTransferStockAllocationReleased,
	typeTransferReceiptStaged,
	typeTransferStockStowed,
}

// E1: the topic TransferReplyConsumer's reader subscribes to is
// documented by the producer's pinned spec, and the channel documents
// every CloudEvents type the decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_InventoryEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	documented := doc.documentedTypes(t, InventoryTopic)
	for _, routed := range inventoryRoutedTypes {
		if _, ok := documented[routed]; !ok {
			t.Errorf("pinned inventory-storage spec does not document %s on %s (documented: %v)", routed, InventoryTopic, documented)
		}
	}
}

// E2: every routed type's example (fresh id) decodes through the REAL
// cloudevents.Decode and routes through the REAL HandleKafkaMessage →
// applier path: exactly one applied reply of the routed kind carrying
// the wire's own facts, and a replay of the same bytes applies nothing
// more (claim-map size is the published-set comparison).
func TestGoldenEventsDecode_InventoryReplies(t *testing.T) {
	for _, routed := range inventoryRoutedTypes {
		ex := inventoryGoldenExample(t, routed)
		t.Run(ex.Name, func(t *testing.T) {
			golden := mintFreshID(t, ex.Payload)
			applier, claims := &fakeReplyApplier{}, newFakeClaims()
			c := inventoryContractConsumer(applier, claims)

			if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertInventoryRoutedOnce(t, golden, applier)
			if got := len(claims.seen); got != 1 {
				t.Fatalf("golden claimed %d processed-event ids, want 1", got)
			}

			if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
				t.Fatalf("replay: %v", err)
			}
			assertInventoryRoutedOnce(t, golden, applier)
			if got := len(claims.seen); got != 1 {
				t.Errorf("replayed id claimed a second processed-event row (%d total)", got)
			}
		})
	}
}

// assertInventoryRoutedOnce asserts exactly one applied reply overall
// (the routed kind's own bucket) carrying the wire's transfer ids.
func assertInventoryRoutedOnce(t *testing.T, golden map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	total := len(applier.allocations) + len(applier.rejections) + len(applier.staged) + len(applier.stowed) + len(applier.released)
	if total != 1 {
		t.Fatalf("golden routed %d replies (alloc=%d rej=%d staged=%d stowed=%d released=%d), want exactly 1",
			total, len(applier.allocations), len(applier.rejections), len(applier.staged), len(applier.stowed), len(applier.released))
	}
	data := golden["data"].(map[string]any)
	switch {
	case len(applier.allocations) == 1:
		assertAllocationFacts(t, data, applier)
	case len(applier.rejections) == 1:
		assertRejectionFacts(t, data, applier)
	case len(applier.released) == 1:
		assertReleaseFacts(t, data, applier)
	case len(applier.staged) == 1:
		assertStagedFacts(t, data, applier)
	case len(applier.stowed) == 1:
		assertStowedFacts(t, data, applier)
	}
}

func assertAllocationFacts(t *testing.T, data map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	in := applier.allocations[0]
	if string(in.TransferID) != strOf(t, data["transfer_id"]) || in.Allocation.TransferLineID != strOf(t, data["transfer_line_id"]) {
		t.Errorf("allocation recorded transfer %q line %q, wire says %q/%q", in.TransferID, in.Allocation.TransferLineID, data["transfer_id"], data["transfer_line_id"])
	}
	if in.Allocation.Quantity != int(numI64(t, data["quantity"])) || in.Allocation.ReservationID != strOf(t, data["reservation_id"]) {
		t.Errorf("allocation recorded qty %d reservation %q, wire says %d/%q", in.Allocation.Quantity, in.Allocation.ReservationID, data["quantity"], data["reservation_id"])
	}
}

func assertRejectionFacts(t *testing.T, data map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	in := applier.rejections[0]
	if string(in.TransferID) != strOf(t, data["transfer_id"]) || string(in.Reason) != strOf(t, data["reason"]) {
		t.Errorf("rejection recorded transfer %q reason %q, wire says %q/%q", in.TransferID, in.Reason, data["transfer_id"], data["reason"])
	}
}

func assertReleaseFacts(t *testing.T, data map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	in := applier.released[0]
	if string(in.TransferID) != strOf(t, data["transfer_id"]) || in.Released.TransferLineID != strOf(t, data["transfer_line_id"]) {
		t.Errorf("release recorded transfer %q line %q, wire says %q/%q", in.TransferID, in.Released.TransferLineID, data["transfer_id"], data["transfer_line_id"])
	}
}

func assertStagedFacts(t *testing.T, data map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	in := applier.staged[0]
	if string(in.TransferID) != strOf(t, data["transfer_id"]) || in.LineID != strOf(t, data["transfer_line_id"]) {
		t.Errorf("receipt staged recorded transfer %q line %q, wire says %q/%q", in.TransferID, in.LineID, data["transfer_id"], data["transfer_line_id"])
	}
}

func assertStowedFacts(t *testing.T, data map[string]any, applier *fakeReplyApplier) {
	t.Helper()
	in := applier.stowed[0]
	if string(in.TransferID) != strOf(t, data["transfer_id"]) || in.Stowed.TransferLineID != strOf(t, data["transfer_line_id"]) {
		t.Errorf("stow recorded transfer %q line %q, wire says %q/%q", in.TransferID, in.Stowed.TransferLineID, data["transfer_id"], data["transfer_line_id"])
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_Inventory(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	for _, routed := range inventoryRoutedTypes {
		ex := inventoryGoldenExample(t, routed)
		envelopeCheck(t, ex.Name, ex.Payload, "inventory-storage", false)
		name := doc.documentedTypes(t, InventoryTopic)[routed]
		required := doc.envelopeRequired(t, doc.message(t, name).Payload)
		for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
			if !required[attr] {
				t.Errorf("pinned spec's %s envelope does not require %q (required: %v)", name, attr, required)
			}
		}
	}
}

// E4: unknown ADDITIVE attributes in data must not fail decode or
// handling; an unknown event TYPE is silently skipped; a non-CloudEvents
// message is skipped-and-logged; a malformed payload is DEAD-LETTERED;
// and a reply the contract refuses (an unknown rejection reason) is
// dead-lettered (ADR 0013) — this consumer's OWN documented policy.
func TestUnknownAttributesTolerated_Inventory(t *testing.T) {
	golden := mintFreshID(t, inventoryGoldenExample(t, typeTransferStockAllocationRejected).Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	applier, claims, dlq := &fakeReplyApplier{}, newFakeClaims(), &anFakeDLQ{}
	c := inventoryContractConsumer(applier, claims)
	c.DLQ, c.DLQTopic = dlq, InventoryTopic+DLQSuffix
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(applier.rejections) != 1 {
		t.Errorf("unknown data attributes stopped the rejection from routing (%d rejections)", len(applier.rejections))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.inventory-storage.stock.StockRelocated"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(applier.rejections) != 1 || len(claims.seen) != 1 {
		t.Errorf("unknown event type was acted on (%d rejections, %d claims)", len(applier.rejections), len(claims.seen))
	}

	// Malformed payload: dead-lettered, never an error.
	malformed := clonePayload(golden)
	malformed["data"] = []any{"not", "an", "object"}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, malformed)); err != nil {
		t.Fatalf("malformed payload must be dead-lettered, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 1 {
		t.Errorf("malformed payload dead-lettered %d messages, want 1", got)
	}

	// An unknown rejection reason: a contract break — dead-lettered.
	badReason := clonePayload(golden)
	badReason["data"].(map[string]any)["reason"] = "NOT_IN_THE_VOCABULARY"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, badReason)); err != nil {
		t.Fatalf("unknown rejection reason must be dead-lettered, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 2 {
		t.Errorf("unknown rejection reason dead-lettered %d messages total, want 2", got)
	}

	// Not a CloudEvent: skipped, never an error, never dead-lettered.
	if err := c.HandleMessage(context.Background(), []byte(`{"transfer_id":"tr-1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 2 {
		t.Errorf("non-CloudEvents message dead-lettered (%d messages)", got)
	}
}
