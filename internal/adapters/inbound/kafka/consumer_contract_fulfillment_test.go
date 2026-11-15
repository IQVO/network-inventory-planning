package kafka

// Event consumer contract tests (E1-E4) for the fulfillment-execution
// edge: TransferFactConsumer subscribes to fulfillment-execution's
// shared topic warehouse.fulfillment.events (FulfillmentTopic wired into
// readerConfig by NewTransferFactConsumer) and acts only on the three
// transfer facts TransferPicked / TransferDispatched / TransferArrived
// (ADR 0005). The contract is the PINNED copy of fulfillment-execution's
// own apis/asyncapi.yaml (testdata/fulfillment-execution.asyncapi.yaml,
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/fulfillment-execution@develop).
//
// Golden fixtures are the producer spec's OWN examples. CAVEAT the
// harness must absorb: the pinned spec's three transfer-fact examples
// REUSE one CloudEvents id (and subject and time) across all three
// types. The consumer dedupes on (consumer, id) regardless of type, so
// adapted clones mint a FRESH id — and the shared-id behaviour itself is
// pinned by a dedicated test below.

import (
	"context"
	"testing"
)

// fulfillmentContractConsumer builds the REAL consumer over the
// in-package recording applier double.
func fulfillmentContractConsumer(applier *fakeFactApplier, claims *fakeClaims) *TransferFactConsumer {
	return &TransferFactConsumer{
		Applier:         applier,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
	}
}

// fulfillmentGoldenExamples returns the pinned producer spec's own
// examples for one routed transfer-fact type.
func fulfillmentGoldenExamples(t *testing.T, routedType string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	name, ok := doc.documentedTypes(t, FulfillmentTopic)[routedType]
	if !ok {
		t.Fatalf("pinned fulfillment-execution spec does not document %s on %s", routedType, FulfillmentTopic)
	}
	examples := doc.goldenExamples(t, FulfillmentTopic, name)
	if len(examples) == 0 {
		t.Fatalf("pinned fulfillment-execution spec documents %s without examples", routedType)
	}
	return examples
}

// E1: the topic TransferFactConsumer's reader subscribes to is
// documented by the producer's pinned spec, and the channel documents
// all three CloudEvents types the decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_FulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	documented := doc.documentedTypes(t, FulfillmentTopic)
	for _, routed := range []string{typeTransferPicked, typeTransferDispatched, typeTransferArrived} {
		if _, ok := documented[routed]; !ok {
			t.Errorf("pinned fulfillment-execution spec does not document %s on %s (documented: %v)", routed, FulfillmentTopic, documented)
		}
	}
}

// E2: each transfer-fact example (fresh id minted — see the file comment)
// decodes through the REAL cloudevents.Decode and routes through the
// REAL HandleKafkaMessage → applier path: exactly one applied fact of
// the routed kind carrying the wire's own transfer_ref and quantity, and
// a replay of the same bytes applies nothing more.
func TestGoldenEventsDecode_FulfillmentTransferFacts(t *testing.T) {
	for _, tc := range []struct {
		routed string
	}{
		{typeTransferPicked},
		{typeTransferDispatched},
		{typeTransferArrived},
	} {
		for _, ex := range fulfillmentGoldenExamples(t, tc.routed) {
			t.Run(ex.Name, func(t *testing.T) {
				golden := mintFreshID(t, ex.Payload)
				applier, claims := &fakeFactApplier{}, newFakeClaims()
				c := fulfillmentContractConsumer(applier, claims)

				if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
					t.Fatalf("handle golden example: %v", err)
				}
				assertFactRoutedOnce(t, tc.routed, golden, applier)
				if got := len(claims.seen); got != 1 {
					t.Fatalf("golden claimed %d processed-event ids, want 1", got)
				}

				if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
					t.Fatalf("replay: %v", err)
				}
				assertFactRoutedOnce(t, tc.routed, golden, applier)
				if got := len(claims.seen); got != 1 {
					t.Errorf("replayed id claimed a second processed-event row (%d total)", got)
				}
			})
		}
	}
}

// assertFactRoutedOnce asserts exactly one applied fact of the routed
// kind (and none of the others) carrying the wire's own facts.
func assertFactRoutedOnce(t *testing.T, routed string, golden map[string]any, applier *fakeFactApplier) {
	t.Helper()
	switch routed {
	case typeTransferPicked:
		assertPickRouted(t, golden, applier)
	case typeTransferDispatched:
		assertDispatchRouted(t, golden, applier)
	case typeTransferArrived:
		assertArrivalRouted(t, golden, applier)
	}
}

func assertPickRouted(t *testing.T, golden map[string]any, applier *fakeFactApplier) {
	t.Helper()
	if len(applier.picks) != 1 || len(applier.dispatched) != 0 || len(applier.arrivals) != 0 {
		t.Fatalf("picked golden routed %d picks/%d dispatched/%d arrivals, want 1/0/0",
			len(applier.picks), len(applier.dispatched), len(applier.arrivals))
	}
	data := golden["data"].(map[string]any)
	if got := applier.picks[0]; string(got.TransferID) != strOf(t, data["transfer_ref"]) || got.Picked.PickedQuantity != int(numI64(t, data["quantity"])) {
		t.Errorf("pick recorded %+v, wire says transfer_ref %q quantity %d", got, data["transfer_ref"], data["quantity"])
	}
}

func assertDispatchRouted(t *testing.T, golden map[string]any, applier *fakeFactApplier) {
	t.Helper()
	if len(applier.dispatched) != 1 || len(applier.picks) != 0 || len(applier.arrivals) != 0 {
		t.Fatalf("dispatched golden routed %d dispatched/%d picks/%d arrivals, want 1/0/0",
			len(applier.dispatched), len(applier.picks), len(applier.arrivals))
	}
	data := golden["data"].(map[string]any)
	if got := applier.dispatched[0]; string(got.TransferID) != strOf(t, data["transfer_ref"]) {
		t.Errorf("dispatch recorded transfer %q, wire says %q", got.TransferID, data["transfer_ref"])
	}
}

func assertArrivalRouted(t *testing.T, golden map[string]any, applier *fakeFactApplier) {
	t.Helper()
	if len(applier.arrivals) != 1 || len(applier.picks) != 0 || len(applier.dispatched) != 0 {
		t.Fatalf("arrival golden routed %d arrivals/%d picks/%d dispatched, want 1/0/0",
			len(applier.arrivals), len(applier.picks), len(applier.dispatched))
	}
	data := golden["data"].(map[string]any)
	if got := applier.arrivals[0]; string(got.TransferID) != strOf(t, data["transfer_ref"]) {
		t.Errorf("arrival recorded transfer %q, wire says %q", got.TransferID, data["transfer_ref"])
	}
}

// TestGoldenSharedIDIsDedupedAcrossTypes pins the caveat from the file
// comment: the pinned spec's three examples share ONE CloudEvents id, so
// delivering a second example AS-IS (same id, different type) hits the
// processed-event gate — the published set (claim map) stays at one
// entry and the second type is never routed. That is exactly why
// adapted clones above mint fresh ids.
func TestGoldenSharedIDIsDedupedAcrossTypes_Fulfillment(t *testing.T) {
	picked := fulfillmentGoldenExamples(t, typeTransferPicked)[0].Payload
	dispatched := fulfillmentGoldenExamples(t, typeTransferDispatched)[0].Payload
	if strOf(t, picked["id"]) != strOf(t, dispatched["id"]) {
		t.Fatalf("pinned examples no longer share a CloudEvents id (%q vs %q) — the fresh-id cloning can be retired",
			picked["id"], dispatched["id"])
	}
	applier, claims := &fakeFactApplier{}, newFakeClaims()
	c := fulfillmentContractConsumer(applier, claims)

	if err := c.HandleMessage(context.Background(), goldenJSON(t, picked)); err != nil {
		t.Fatalf("handle picked: %v", err)
	}
	if got := len(claims.seen); got != 1 {
		t.Fatalf("picked claimed %d ids, want 1", got)
	}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, dispatched)); err != nil {
		t.Fatalf("handle same-id dispatched: %v", err)
	}
	if got := len(claims.seen); got != 1 {
		t.Errorf("same-id dispatched grew the published set to %d — the id gate must dedupe across types", got)
	}
	if len(applier.dispatched) != 0 {
		t.Errorf("same-id dispatched was routed anyway (%d calls)", len(applier.dispatched))
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_Fulfillment(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	for _, routed := range []string{typeTransferPicked, typeTransferDispatched, typeTransferArrived} {
		for _, ex := range fulfillmentGoldenExamples(t, routed) {
			envelopeCheck(t, ex.Name, ex.Payload, "fulfillment-execution", false)
		}
		name := doc.documentedTypes(t, FulfillmentTopic)[routed]
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
// and a TransferPicked without a positive quantity is dead-lettered as a
// refused fact (ADR 0013) — this consumer's OWN documented policy.
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	golden := mintFreshID(t, fulfillmentGoldenExamples(t, typeTransferPicked)[0].Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	applier, claims, dlq := &fakeFactApplier{}, newFakeClaims(), &anFakeDLQ{}
	c := fulfillmentContractConsumer(applier, claims)
	c.DLQ, c.DLQTopic = dlq, FulfillmentTopic+DLQSuffix
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(applier.picks) != 1 {
		t.Errorf("unknown data attributes stopped the pick from routing (%d picks)", len(applier.picks))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(applier.picks) != 1 || len(claims.seen) != 1 {
		t.Errorf("unknown event type was acted on (%d picks, %d claims)", len(applier.picks), len(claims.seen))
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

	// A pick without a positive quantity: a refused fact — dead-lettered.
	noQuantity := clonePayload(golden)
	noQuantity["data"].(map[string]any)["quantity"] = 0
	if err := c.HandleMessage(context.Background(), goldenJSON(t, noQuantity)); err != nil {
		t.Fatalf("quantity-less pick must be dead-lettered, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 2 {
		t.Errorf("quantity-less pick dead-lettered %d messages total, want 2", got)
	}

	// Not a CloudEvent: skipped, never an error, never dead-lettered.
	if err := c.HandleMessage(context.Background(), []byte(`{"transfer_ref":"tr-1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 2 {
		t.Errorf("non-CloudEvents message dead-lettered (%d messages)", got)
	}
}
