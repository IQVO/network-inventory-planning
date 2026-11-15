package kafka

// Event consumer contract tests (E1-E4) for the facility-layout edge:
// SiteCapabilityConsumer subscribes to facility-layout's shared topic
// warehouse.facility.events (FacilityTopic wired into readerConfig by
// NewSiteCapabilityConsumer) and acts only on
// com.warehouse.wms.facility-layout.site.SiteCapabilityChanged. The
// contract it validates against is the PINNED copy of facility-layout's
// own apis/asyncapi.yaml (testdata/facility-layout.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/facility-layout@develop).
//
// Fixture provenance: facility-layout's spec documents
// SiteCapabilityChanged WITHOUT examples, so the E2 golden is DERIVED —
// the envelope from the pinned spec's own const schemas (type,
// dataschema, source, eventDataBase) and the payload values from this
// repo's kafka_integration fixtures (planning_read_models_integration_
// test.go's WH1/revision-4 snapshot), carrying a freshly minted
// CloudEvents id. Producer example backfill pending (named in the PR
// body); E1 covers the type in the catalog meanwhile.

import (
	"context"
	"testing"
	"time"
)

// facilityContractConsumer builds the REAL consumer over the package's
// in-memory doubles (the reader is never dialed inside HandleMessage).
func facilityContractConsumer(caps *fakeCapabilities, claims *fakeClaims) *SiteCapabilityConsumer {
	return &SiteCapabilityConsumer{
		Capabilities:    caps,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
	}
}

// facilityDerivedGolden derives the SiteCapabilityChanged golden envelope
// from the pinned spec's own schemas plus this repo's integration-fixture
// values, with a freshly minted id.
func facilityDerivedGolden(t *testing.T) map[string]any {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedFacilitySpec)
	msg := doc.message(t, "siteCapabilityChanged")
	consts := doc.payloadConsts(t, msg.Payload)
	if consts["type"] != typeSiteCapabilityChanged {
		t.Fatalf("pinned spec type const %q does not match the routed type %q", consts["type"], typeSiteCapabilityChanged)
	}
	return map[string]any{
		"specversion":     "1.0",
		"id":              "derived-network-inventory-planning-site-capability-1",
		"source":          consts["source"],
		"type":            consts["type"],
		"subject":         "WH1",
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      consts["dataschema"],
		"data": map[string]any{
			"eventName":                    "SiteCapabilityChanged",
			"eventType":                    consts["type"],
			"occurredAt":                   "2026-10-06T12:00:00Z",
			"site_code":                    "WH1",
			"transfer_origin_enabled":      true,
			"transfer_destination_enabled": false,
			"capability_revision":          4,
		},
	}
}

// E1: the topic SiteCapabilityConsumer's reader subscribes to (the
// FacilityTopic constant NewSiteCapabilityConsumer wires into
// kafka.ReaderConfig) is documented by the producer's pinned spec, and
// the channel's message list documents the exact CloudEvents type the
// consumer's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_FacilityEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFacilitySpec)
	documented := doc.documentedTypes(t, FacilityTopic)
	if _, ok := documented[typeSiteCapabilityChanged]; !ok {
		t.Errorf("pinned facility-layout spec does not document %s on %s (documented: %v)",
			typeSiteCapabilityChanged, FacilityTopic, documented)
	}
	if len(documented) < 2 {
		t.Logf("note: pinned spec documents %d type(s) on the shared topic", len(documented))
	}
}

// E2: the derived golden decodes through the REAL cloudevents.Decode and
// routes through the REAL HandleMessage → NewSiteCapability → upsert
// path: exactly one site_capability row carrying the wire's own facts,
// and a replay of the same CloudEvents id records nothing more (the
// claim map's size is the published-set comparison).
func TestGoldenEventsDecode_FacilitySiteCapabilityChanged(t *testing.T) {
	t.Logf("NOTE: facility-layout documents SiteCapabilityChanged without examples; golden derived from the pinned schemas + this repo's integration fixtures")
	golden := mintFreshID(t, facilityDerivedGolden(t))
	caps, claims := newFakeCapabilities(), newFakeClaims()
	c := facilityContractConsumer(caps, claims)

	if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
		t.Fatalf("handle derived golden: %v", err)
	}
	assertFacilityRow(t, caps)
	if got := len(claims.seen); got != 1 {
		t.Fatalf("golden claimed %d processed-event ids, want 1", got)
	}

	// Dedupe contract: the CloudEvents id is the claim key — a replay
	// changes neither the row set nor the claim set.
	if err := c.HandleMessage(context.Background(), goldenJSON(t, golden)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	assertFacilityRow(t, caps)
	if got := len(claims.seen); got != 1 {
		t.Errorf("replayed CloudEvents id claimed a second processed-event row (%d total)", got)
	}
}

func assertFacilityRow(t *testing.T, caps *fakeCapabilities) {
	t.Helper()
	row, ok := caps.rows["WH1"]
	if !ok {
		t.Fatalf("golden event recorded no site_capability row for WH1 (rows: %v)", caps.rows)
	}
	if !row.TransferOriginEnabled || row.TransferDestinationEnabled {
		t.Errorf("recorded capability flags origin=%v destination=%v, wire says true/false",
			row.TransferOriginEnabled, row.TransferDestinationEnabled)
	}
	if row.Revision != 4 {
		t.Errorf("recorded revision %d, wire says 4", row.Revision)
	}
	if !row.AsOf.Equal(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("recorded as-of %v, CloudEvents time is 2026-10-06T12:00:00Z", row.AsOf)
	}
}

// E3: the derived golden carries the fleet-mandatory CloudEvents 1.0
// envelope, and the pinned spec's envelope schema requires exactly those
// attributes.
func TestCloudEventsEnvelopeInvariants_Facility(t *testing.T) {
	envelopeCheck(t, "derived SiteCapabilityChanged", facilityDerivedGolden(t), "facility-layout", false)

	doc := loadPinnedAsyncAPI(t, pinnedFacilitySpec)
	required := doc.envelopeRequired(t, doc.message(t, "siteCapabilityChanged").Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's SiteCapabilityChanged envelope does not require %q (required: %v)", attr, required)
		}
	}
}

// E4: unknown ADDITIVE attributes in data must not fail decode or
// handling, an unknown event TYPE is silently skipped, and a message
// that is not a CloudEvent at all is skipped-and-logged — this
// consumer's OWN documented policy (HandleMessage returns nil for
// anything deterministic, including non-CloudEvents bytes; no DLQ on
// this edge).
func TestUnknownAttributesTolerated_Facility(t *testing.T) {
	golden := facilityDerivedGolden(t)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	caps, claims := newFakeCapabilities(), newFakeClaims()
	c := facilityContractConsumer(caps, claims)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	assertFacilityRow(t, caps)

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.facility-layout.site.SiteRoleChanged"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := len(claims.seen); got != 1 {
		t.Errorf("unknown event type claimed a processed-event row (%d total)", got)
	}

	// Not a CloudEvent (the retired flat envelope): skipped, never an
	// error — per this consumer's documented policy.
	if err := c.HandleMessage(context.Background(), []byte(`{"site_code":"WH1"}`)); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if got := len(claims.seen); got != 1 {
		t.Errorf("non-CloudEvents message claimed a processed-event row (%d total)", got)
	}
}
