package kafka

// Event consumer contract tests (E1-E4) for the product-master edge:
// PackLevelConsumer subscribes to product-master's shared topic
// warehouse.product-master.events (ProductMasterTopic wired into
// readerConfig by NewPackLevelConsumer) and acts only on
// com.warehouse.wms.product-master.product.ProductPackLevelDeclared and
// ...ProductPackLevelRetired. The contract is the PINNED copy of
// product-master's own apis/asyncapi.yaml
// (testdata/product-master.asyncapi.yaml, contracts/pinned.json; the
// pinned-contracts CI job fails when the copy drifts from
// IQVO/product-master@develop).
//
// Golden fixtures are the producer spec's OWN examples (caseOf12,
// superseded). E2 drives the REAL consumer over the REAL
// usecases.ApplyPackLevel use case with the in-memory PackLevelView —
// the pack_levels view (ADR 0017) is what these events exist to feed.

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

// productContractConsumer builds the REAL consumer over the REAL use
// case with the in-memory view (the reader and the DLQ writer are never
// dialed inside HandleKafkaMessage; dlq records poison sends).
func productContractConsumer(view *memory.PackLevelView, claims *fakeClaims, dlq DeadLetterWriter) *PackLevelConsumer {
	return &PackLevelConsumer{
		Applier:         usecases.ApplyPackLevel{PackLevels: view},
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		DLQ:             dlq,
		DLQTopic:        ProductMasterTopic + DLQSuffix,
	}
}

// productGoldenExamples returns the pinned producer spec's own examples
// for one routed pack-level type.
func productGoldenExamples(t *testing.T, routedType string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedProductSpec)
	name, ok := doc.documentedTypes(t, ProductMasterTopic)[routedType]
	if !ok {
		t.Fatalf("pinned product-master spec does not document %s on %s", routedType, ProductMasterTopic)
	}
	examples := doc.goldenExamples(t, ProductMasterTopic, name)
	if len(examples) == 0 {
		t.Fatalf("pinned product-master spec documents %s without examples", routedType)
	}
	return examples
}

// E1: the topic PackLevelConsumer's reader subscribes to is documented
// by the producer's pinned spec, and the channel documents BOTH
// CloudEvents types the decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_ProductMasterEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProductSpec)
	documented := doc.documentedTypes(t, ProductMasterTopic)
	for _, routed := range []string{typePackLevelDeclared, typePackLevelRetired} {
		if _, ok := documented[routed]; !ok {
			t.Errorf("pinned product-master spec does not document %s on %s (documented: %v)", routed, ProductMasterTopic, documented)
		}
	}
}

// E2: the Declared and Retired examples decode through the REAL
// cloudevents.Decode and route through the REAL HandleKafkaMessage →
// ApplyPackLevel path: exactly one pack_levels transition per event
// carrying the wire's own facts, and a replay of the same CloudEvents id
// applies nothing more (claim-map size is the published-set comparison).
func TestGoldenEventsDecode_ProductPackLevels(t *testing.T) {
	t.Run("ProductPackLevelDeclared", func(t *testing.T) {
		ex := productGoldenExamples(t, typePackLevelDeclared)[0]
		view, claims := memory.NewPackLevelView(), newFakeClaims()
		c := productContractConsumer(view, claims, nil)
		if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
			t.Fatalf("handle golden example: %v", err)
		}
		assertDeclaredMirrored(t, ex, view)
		assertReplayIsNoop(t, c, ex, claims)
	})
	t.Run("ProductPackLevelRetired", func(t *testing.T) {
		// The producer's retired example (superseded, version 4) closes the
		// level its declared example (caseOf12, version 3) opened — the two
		// examples are one wire sequence on the same SKU-1/CASE level.
		declared := productGoldenExamples(t, typePackLevelDeclared)[0]
		retired := productGoldenExamples(t, typePackLevelRetired)[0]
		view, claims := memory.NewPackLevelView(), newFakeClaims()
		c := productContractConsumer(view, claims, nil)
		if err := c.HandleMessage(context.Background(), goldenJSON(t, declared.Payload)); err != nil {
			t.Fatalf("handle declared golden example: %v", err)
		}
		if err := c.HandleMessage(context.Background(), goldenJSON(t, retired.Payload)); err != nil {
			t.Fatalf("handle retired golden example: %v", err)
		}
		assertRetiredMirrored(t, retired, view)
		assertReplayIsNoop(t, c, retired, claims)
	})
}

func assertDeclaredMirrored(t *testing.T, ex pinnedExample, view *memory.PackLevelView) {
	t.Helper()
	data := goldenData(t, ex)
	got, ok, _ := view.Get(context.Background(), strOf(t, data["sku"]), packlevel.Case)
	if !ok {
		t.Fatalf("declared example mirrored no pack_levels row for %v", data["sku"])
	}
	if got.UnitsPerPack != numI64(t, data["units_per_pack"]) || got.Version != numI64(t, data["version"]) {
		t.Errorf("mirrored units=%d version=%d, wire says %d/%d",
			got.UnitsPerPack, got.Version, data["units_per_pack"], data["version"])
	}
	wantFrom, _ := time.Parse(time.RFC3339, strOf(t, data["effective_from"]))
	if !got.EffectiveFrom.Equal(wantFrom) || got.EffectiveTo != nil {
		t.Errorf("mirrored effective window %v..%v, wire declares from %q open-ended", got.EffectiveFrom, got.EffectiveTo, data["effective_from"])
	}
}

func assertRetiredMirrored(t *testing.T, ex pinnedExample, view *memory.PackLevelView) {
	t.Helper()
	data := goldenData(t, ex)
	got, ok, _ := view.Get(context.Background(), strOf(t, data["sku"]), packlevel.Case)
	if !ok {
		t.Fatalf("retired example found no pack_levels row for %v", data["sku"])
	}
	if got.EffectiveTo == nil {
		t.Fatal("retired example did not close the pack level (EffectiveTo nil)")
	}
	wantTo, _ := time.Parse(time.RFC3339, strOf(t, data["effective_to"]))
	if !got.EffectiveTo.Equal(wantTo) || got.Version != numI64(t, data["version"]) {
		t.Errorf("mirrored retirement to=%v version=%d, wire says %q/%d", got.EffectiveTo, got.Version, data["effective_to"], data["version"])
	}
}

// assertReplayIsNoop re-delivers the same bytes and asserts the claim
// map (the published set) does not grow: the processed-event gate is the
// dedupe key, not the pack-level version gate.
func assertReplayIsNoop(t *testing.T, c *PackLevelConsumer, ex pinnedExample, claims *fakeClaims) {
	t.Helper()
	before := len(claims.seen)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := len(claims.seen); got != before {
		t.Errorf("replayed CloudEvents id claimed a second processed-event row (%d -> %d)", before, got)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_ProductMaster(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProductSpec)
	for _, routed := range []string{typePackLevelDeclared, typePackLevelRetired} {
		for _, ex := range productGoldenExamples(t, routed) {
			envelopeCheck(t, ex.Name, ex.Payload, "product-master", false)
		}
		name := doc.documentedTypes(t, ProductMasterTopic)[routed]
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
// message is skipped-and-logged; and a payload that can never be applied
// is DEAD-LETTERED — this consumer's OWN documented policy (a
// deterministic refusal is dlq'd and committed past, never retried).
func TestUnknownAttributesTolerated_ProductMaster(t *testing.T) {
	golden := clonePayload(productGoldenExamples(t, typePackLevelDeclared)[0].Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	view, claims, dlq := memory.NewPackLevelView(), newFakeClaims(), &anFakeDLQ{}
	c := productContractConsumer(view, claims, dlq)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, ok, _ := view.Get(context.Background(), "SKU-1", packlevel.Case); !ok {
		t.Error("unknown data attributes stopped the declared level from mirroring")
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.product-master.product.ProductRegistered"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(claims.seen) != 1 || len(dlq.written()) != 0 {
		t.Errorf("unknown event type was acted on or dead-lettered (%d claims, %d dlq)", len(claims.seen), len(dlq.written()))
	}

	// Malformed payload (data is not the documented object): a
	// deterministic decode failure — dead-lettered, never an error.
	malformed := clonePayload(golden)
	malformed["data"] = []any{"not", "an", "object"}
	if err := c.HandleKafkaMessage(context.Background(), kafkago.Message{Topic: ProductMasterTopic, Value: goldenJSON(t, malformed)}); err != nil {
		t.Fatalf("malformed payload must be dead-lettered, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 1 {
		t.Errorf("malformed payload dead-lettered %d messages, want 1", got)
	}

	// Not a CloudEvent: skipped, never an error, never dead-lettered.
	if err := c.HandleMessage(context.Background(), []byte(`{"sku":"SKU-1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 1 {
		t.Errorf("non-CloudEvents message dead-lettered (%d messages)", got)
	}
}
