package kafka

// Event consumer contract tests (E1-E4) for the warehouse-planning
// edge: CapacityPlanConsumer subscribes to warehouse-planning's shared
// topic warehouse.warehouse-planning.events (PlanningTopic wired into
// readerConfig by NewCapacityPlanConsumer) and acts only on
// com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished.
// The contract is the PINNED copy of warehouse-planning's own
// apis/asyncapi.yaml (testdata/warehouse-planning.asyncapi.yaml,
// contracts/pinned.json; the pinned-contracts CI job fails when the
// copy drifts from IQVO/warehouse-planning@develop).
//
// Golden fixtures are the producer spec's OWN examples (publishedPlan).

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// countingPlans counts the upserts the REAL consumer issued.
type countingPlans struct {
	*fakePlans
	upserts int
}

func (f *countingPlans) Upsert(ctx context.Context, p planning.PublishedCapacityPlan) (bool, error) {
	f.upserts++
	return f.fakePlans.Upsert(ctx, p)
}

// planningContractConsumer builds the REAL consumer over the in-package
// doubles (the reader is never dialed inside HandleMessage).
func planningContractConsumer(plans *countingPlans, claims *fakeClaims) *CapacityPlanConsumer {
	return &CapacityPlanConsumer{
		Plans:           plans,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
	}
}

// planningGoldenExamples returns the pinned producer spec's own
// CapacityPlanPublished examples for the subscribed channel.
func planningGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedPlanningSpec)
	name, ok := doc.documentedTypes(t, PlanningTopic)[typeCapacityPlanPublished]
	if !ok {
		t.Fatalf("pinned warehouse-planning spec does not document %s on %s", typeCapacityPlanPublished, PlanningTopic)
	}
	examples := doc.goldenExamples(t, PlanningTopic, name)
	if len(examples) == 0 {
		t.Fatal("pinned warehouse-planning spec documents CapacityPlanPublished without examples")
	}
	return examples
}

// E1: the topic CapacityPlanConsumer's reader subscribes to is
// documented by the producer's pinned spec, and the channel documents
// the exact CloudEvents type the decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_PlanningEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedPlanningSpec)
	documented := doc.documentedTypes(t, PlanningTopic)
	if _, ok := documented[typeCapacityPlanPublished]; !ok {
		t.Errorf("pinned warehouse-planning spec does not document %s on %s (documented: %v)",
			typeCapacityPlanPublished, PlanningTopic, documented)
	}
}

// E2: every CapacityPlanPublished example decodes through the REAL
// cloudevents.Decode and routes through the REAL HandleMessage →
// NewPublishedCapacityPlan → upsert path: exactly one plan row per
// event carrying the wire's own facts, and a replay of the same
// CloudEvents id upserts nothing more.
func TestGoldenEventsDecode_PlanningCapacityPlanPublished(t *testing.T) {
	for _, ex := range planningGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			plans, claims := &countingPlans{fakePlans: newFakePlans()}, newFakeClaims()
			c := planningContractConsumer(plans, claims)

			if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertPlanningRowRecorded(t, ex, plans)
			if got := len(claims.seen); got != 1 {
				t.Fatalf("golden claimed %d processed-event ids, want 1", got)
			}

			if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("replay: %v", err)
			}
			if plans.upserts != 1 {
				t.Errorf("replayed CloudEvents id %q upserted again (%d upserts)", strOf(t, ex.Payload["id"]), plans.upserts)
			}
			if got := len(claims.seen); got != 1 {
				t.Errorf("replayed id claimed a second processed-event row (%d total)", got)
			}
		})
	}
}

// assertPlanningRowRecorded asserts the golden event produced exactly
// one plan row carrying the wire's own facts.
func assertPlanningRowRecorded(t *testing.T, ex pinnedExample, plans *countingPlans) {
	t.Helper()
	if plans.upserts != 1 || len(plans.rows) != 1 {
		t.Fatalf("golden event produced %d upserts over %d rows, want exactly 1/1", plans.upserts, len(plans.rows))
	}
	data := goldenData(t, ex)
	row, ok := plans.rows[strOf(t, data["plan_id"])]
	if !ok {
		t.Fatalf("no plan row keyed by the wire's plan_id %v (rows: %v)", data["plan_id"], plans.rows)
	}
	if row.SiteID != strOf(t, data["site_id"]) || row.PathID != strOf(t, data["path_id"]) {
		t.Errorf("recorded row site=%q path=%q, wire says %q/%q", row.SiteID, row.PathID, data["site_id"], data["path_id"])
	}
	if row.AssignedDemand != float64(numI64(t, data["assigned_demand"])) {
		t.Errorf("recorded assigned demand %v, wire says %v", row.AssignedDemand, data["assigned_demand"])
	}
	wantStart, _ := time.Parse(time.RFC3339, strOf(t, data["window_start"]))
	if !row.WindowStart.Equal(wantStart) {
		t.Errorf("recorded window start %v, wire says %q", row.WindowStart, data["window_start"])
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_Planning(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedPlanningSpec)
	for _, ex := range planningGoldenExamples(t) {
		envelopeCheck(t, ex.Name, ex.Payload, "warehouse-planning", false)
	}
	name := doc.documentedTypes(t, PlanningTopic)[typeCapacityPlanPublished]
	required := doc.envelopeRequired(t, doc.message(t, name).Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's CapacityPlanPublished envelope does not require %q (required: %v)", attr, required)
		}
	}
}

// E4: unknown ADDITIVE attributes in data must not fail decode or
// handling, an unknown event TYPE is silently skipped, a non-CloudEvents
// message is skipped-and-logged (this consumer's OWN documented policy;
// no DLQ on this edge), and a legacy payload WITHOUT the additive
// site_id is excluded from the read model — never stored, never an
// error (the documented site_id exclusion rule).
func TestUnknownAttributesTolerated_Planning(t *testing.T) {
	golden := clonePayload(planningGoldenExamples(t)[0].Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	plans, claims := &countingPlans{fakePlans: newFakePlans()}, newFakeClaims()
	c := planningContractConsumer(plans, claims)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if plans.upserts != 1 || len(plans.rows) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d upserts over %d rows", plans.upserts, len(plans.rows))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if plans.upserts != 1 || len(claims.seen) != 1 {
		t.Errorf("unknown event type was acted on (%d upserts, %d claims)", plans.upserts, len(claims.seen))
	}

	// Legacy payload without site_id: excluded (skipped-and-logged),
	// never an error, never stored.
	legacy := clonePayload(golden)
	legacyData := legacy["data"].(map[string]any)
	delete(legacyData, "site_id")
	if err := c.HandleMessage(context.Background(), goldenJSON(t, legacy)); err != nil {
		t.Fatalf("legacy no-site_id payload must be excluded, not an error: %v", err)
	}
	if plans.upserts != 1 || len(plans.rows) != 1 {
		t.Errorf("legacy no-site_id payload must not be stored (%d upserts over %d rows)", plans.upserts, len(plans.rows))
	}

	// Not a CloudEvent: skipped, never an error.
	if err := c.HandleMessage(context.Background(), []byte(`{"plan_id":"p-1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if len(claims.seen) != 1 {
		t.Errorf("non-CloudEvents message claimed a processed-event row (%d total)", len(claims.seen))
	}
}
