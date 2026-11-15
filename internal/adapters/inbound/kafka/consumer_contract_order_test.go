package kafka

// Event consumer contract tests (E1-E4) for the order-management edge:
// SiteSkuDemandConsumer subscribes to order-management's shared topic
// warehouse.order-management.events (OrderTopic wired into readerConfig
// by NewSiteSkuDemandConsumer) and acts only on
// com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged.
// The contract is the PINNED copy of order-management's own
// apis/asyncapi.yaml (testdata/order-management.asyncapi.yaml,
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/order-management@develop).
//
// Golden fixtures are the producer spec's OWN examples — every
// SiteSkuDemandChanged example pinned here (order-site, static-site
// ACTIVE, static-site REMOVED) is a real wire document
// order-management documents for this channel.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// countingDemands counts the upserts the REAL consumer issued, so the
// tests can assert routing (a golden event upserts exactly its row) and
// the dedupe contract (a replayed CloudEvents id upserts nothing more).
type countingDemands struct {
	*fakeDemands
	upserts int
}

func (f *countingDemands) Upsert(ctx context.Context, d planning.SiteSkuDemand) (bool, error) {
	f.upserts++
	return f.fakeDemands.Upsert(ctx, d)
}

// orderContractConsumer builds the REAL consumer over the in-package
// doubles (the reader is never dialed inside HandleMessage).
func orderContractConsumer(demands *countingDemands, claims *fakeClaims) *SiteSkuDemandConsumer {
	return &SiteSkuDemandConsumer{
		Demands:         demands,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
	}
}

// orderGoldenExamples returns the pinned producer spec's own
// SiteSkuDemandChanged examples for the subscribed channel.
func orderGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	name, ok := doc.documentedTypes(t, OrderTopic)[typeSiteSkuDemandChanged]
	if !ok {
		t.Fatalf("pinned order-management spec does not document %s on %s", typeSiteSkuDemandChanged, OrderTopic)
	}
	examples := doc.goldenExamples(t, OrderTopic, name)
	if len(examples) == 0 {
		t.Fatal("pinned order-management spec documents SiteSkuDemandChanged without examples")
	}
	return examples
}

// E1: the topic SiteSkuDemandConsumer's reader subscribes to (the
// OrderTopic constant wired into kafka.ReaderConfig) is documented by
// the producer's pinned spec, and the channel documents the exact
// CloudEvents type the decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_OrderEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	documented := doc.documentedTypes(t, OrderTopic)
	if _, ok := documented[typeSiteSkuDemandChanged]; !ok {
		t.Errorf("pinned order-management spec does not document %s on %s (documented: %v)",
			typeSiteSkuDemandChanged, OrderTopic, documented)
	}
}

// E2: every SiteSkuDemandChanged example decodes through the REAL
// cloudevents.Decode and routes through the REAL HandleMessage →
// NewSiteSkuDemand → upsert path: exactly one demand row per event
// carrying the wire's own facts, and a replay of the same CloudEvents id
// upserts nothing more (claim-map size is the published-set comparison).
func TestGoldenEventsDecode_OrderSiteSkuDemandChanged(t *testing.T) {
	for _, ex := range orderGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			demands, claims := &countingDemands{fakeDemands: newFakeDemands()}, newFakeClaims()
			c := orderContractConsumer(demands, claims)

			if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertOrderRowRecorded(t, ex, demands)
			if got := len(claims.seen); got != 1 {
				t.Fatalf("golden claimed %d processed-event ids, want 1", got)
			}

			if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("replay: %v", err)
			}
			if demands.upserts != 1 {
				t.Errorf("replayed CloudEvents id %q upserted again (%d upserts)", strOf(t, ex.Payload["id"]), demands.upserts)
			}
			if got := len(claims.seen); got != 1 {
				t.Errorf("replayed id claimed a second processed-event row (%d total)", got)
			}
		})
	}
}

// assertOrderRowRecorded asserts the golden event produced exactly one
// demand row carrying the wire's own facts.
func assertOrderRowRecorded(t *testing.T, ex pinnedExample, demands *countingDemands) {
	t.Helper()
	if demands.upserts != 1 || len(demands.rows) != 1 {
		t.Fatalf("golden event produced %d upserts over %d rows, want exactly 1/1", demands.upserts, len(demands.rows))
	}
	data := goldenData(t, ex)
	row := demands.rows[fmt.Sprintf("%s#%d", strOf(t, data["source_order_id"]), numI64(t, data["line_no"]))]
	if row.Site != strOf(t, data["site_id"]) || row.SKU != strOf(t, data["sku"]) {
		t.Errorf("recorded row site=%q sku=%q, wire says %q/%q", row.Site, row.SKU, data["site_id"], data["sku"])
	}
	if row.DemandedUnits != int(numI64(t, data["demanded_units"])) {
		t.Errorf("recorded units %d, wire says %d", row.DemandedUnits, data["demanded_units"])
	}
	if string(row.State) != strOf(t, data["state"]) {
		t.Errorf("recorded state %q, wire says %q", row.State, data["state"])
	}
	wantDue, err := time.Parse(time.RFC3339, strOf(t, data["due_at"]))
	if err != nil || !row.DueAt.Equal(wantDue) {
		t.Errorf("recorded due-at %v, wire says %q", row.DueAt, data["due_at"])
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_Order(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	for _, ex := range orderGoldenExamples(t) {
		envelopeCheck(t, ex.Name, ex.Payload, "order-management", false)
	}
	name := doc.documentedTypes(t, OrderTopic)[typeSiteSkuDemandChanged]
	required := doc.envelopeRequired(t, doc.message(t, name).Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's SiteSkuDemandChanged envelope does not require %q (required: %v)", attr, required)
		}
	}
}

// E4: unknown ADDITIVE attributes in data must not fail decode or
// handling, an unknown event TYPE is silently skipped, and a non-
// CloudEvents message is skipped-and-logged — this consumer's OWN
// documented policy (HandleMessage returns nil for anything
// deterministic; no DLQ on this edge).
func TestUnknownAttributesTolerated_Order(t *testing.T) {
	golden := clonePayload(orderGoldenExamples(t)[0].Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	demands, claims := &countingDemands{fakeDemands: newFakeDemands()}, newFakeClaims()
	c := orderContractConsumer(demands, claims)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if demands.upserts != 1 || len(demands.rows) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d upserts over %d rows", demands.upserts, len(demands.rows))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.order-management.order.OrderAllocated"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if demands.upserts != 1 || len(claims.seen) != 1 {
		t.Errorf("unknown event type was acted on (%d upserts, %d claims)", demands.upserts, len(claims.seen))
	}

	// Not a CloudEvent: skipped, never an error.
	if err := c.HandleMessage(context.Background(), []byte(`{"source_order_id":"ord-1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if len(claims.seen) != 1 {
		t.Errorf("non-CloudEvents message claimed a processed-event row (%d total)", len(claims.seen))
	}
}
