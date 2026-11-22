package kafka

// Event consumer contract tests (E1-E4) for the analytics SELF-edge:
// this repo's own projector (cmd/nip-projector) consumes this repo's own
// dedicated analytics stream warehouse.network-inventory-planning.analytics
// (outboundkafka.AnalyticsTopic, wired into readerConfig by
// NewAnalyticsConsumer — ADR 0007), folding the three saga-health
// occurrences into the analytical model. This repo is both the producer
// and the consumer, so the contract is the SELF-pin of its own
// apis/asyncapi.yaml (testdata/network-inventory-planning.asyncapi.yaml,
// registered in contracts/pinned.json): the pin keeps the projector
// honest against the same spec its analytics publisher documents for
// sibling consumers. NOTE spec shape: the own spec is AsyncAPI 3.0 with
// channels keyed by logical name, the topic in `address`, and
// channel-level `messages` maps (no subscribe/publish operations).
//
// Fixture provenance: the own spec documents its analytics messages
// WITHOUT examples, so the E2 goldens are DERIVED — payloads from the
// self-pinned schemas' own required lists, envelopes minted through the
// repo's REAL cloudevents.New encoder (the analytics publisher's exact
// emission path, ADR 0007). Own-spec example backfill pending (named in
// the PR body); E1 covers the types in the catalog meanwhile.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// ownAnalyticsRouted mirrors analyticsKinds — the full dispatch table of
// the projector's consumer (entity "saga").
var ownAnalyticsRouted = map[string]report.Kind{
	cloudevents.Type("saga", string(report.KindStateAdvanced)):         report.KindStateAdvanced,
	cloudevents.Type("saga", string(report.KindStuckDetected)):         report.KindStuckDetected,
	cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)): report.KindRebalanceRunCompleted,
}

// ownAnalyticsMessageNames maps each routed CloudEvents type to its
// components/messages entry in the self-pinned spec.
var ownAnalyticsMessageNames = map[string]string{
	cloudevents.Type("saga", string(report.KindStateAdvanced)):         "transferStateAdvanced",
	cloudevents.Type("saga", string(report.KindStuckDetected)):         "transferStuckDetected",
	cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)): "rebalanceRunCompleted",
}

// ownAnalyticsConsumer builds the REAL analytics consumer around the
// in-package recording projection (the reader is never dialed inside
// HandleMessage).
func ownAnalyticsConsumer(proj *anRecorder, dlq DeadLetterWriter) *AnalyticsConsumer {
	return &AnalyticsConsumer{
		Projection: proj,
		DLQ:        dlq,
		DLQTopic:   kafka.AnalyticsTopic + DLQSuffix,
		Logger:     quietLogger(),
		Now:        func() time.Time { return time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC) },
	}
}

// ownAnalyticsGolden mints one analytics golden through the repo's REAL
// encoder — the analytics publisher's exact emission path — with the
// payload the self-pinned schema requires.
func ownAnalyticsGolden(t *testing.T, routed string) []byte {
	t.Helper()
	t.Logf("NOTE: own spec documents %s without examples; golden derived from the self-pinned schema, minted via the real encoder", routed)
	eventName := routed[strings.LastIndex(routed, ".")+1:]
	spec := cloudevents.Spec{
		ID: "contract-" + eventName, Entity: "saga",
		EventName: eventName, Subject: ownSubject(routed),
		Time:    time.Date(2026, 10, 5, 9, 30, 0, 0, time.UTC),
		Stream:  cloudevents.StreamAnalytics,
		Version: 1,
		Data:    ownAnalyticsData(t, routed),
	}
	value, err := cloudevents.New(spec)
	if err != nil {
		t.Fatalf("mint %s golden through the real encoder: %v", routed, err)
	}
	return value
}

// ownSubject returns the subject the schema names for each kind (the
// aggregate id: transfer_id, or run_id for rebalance runs).
func ownSubject(routed string) string {
	if routed == cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)) {
		return "rebalance-2026-10-05-0930"
	}
	return "tr-42"
}

// ownAnalyticsData builds the payload each self-pinned schema requires
// (and the consumer's fill helpers validate).
func ownAnalyticsData(t *testing.T, routed string) map[string]any {
	t.Helper()
	switch routed {
	case cloudevents.Type("saga", string(report.KindStateAdvanced)):
		return map[string]any{"transfer_id": "tr-42", "from": "ALLOCATING", "to": "ALLOCATED", "age_seconds": 90}
	case cloudevents.Type("saga", string(report.KindStuckDetected)):
		return map[string]any{"transfer_id": "tr-42", "state": "ALLOCATING", "age_seconds": 3600, "threshold_seconds": 1800}
	case cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)):
		return map[string]any{"run_id": "rebalance-2026-10-05-0930", "proposal_count": 4, "rejected_count": 1, "stale_facts": 2}
	}
	t.Fatalf("no derived payload for %q", routed)
	return nil
}

// E1: the topic the projector's reader subscribes to (the
// outboundkafka.AnalyticsTopic constant cmd/nip-projector wires into
// kafka.ReaderConfig) is documented by the repo's own pinned spec, and
// the channel's message list documents every type the consumer routes
// on.
func TestPinnedAsyncAPICoversSubscriptions_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOwnSpec)
	documented := doc.documentedTypes(t, kafka.AnalyticsTopic)
	for routed := range ownAnalyticsRouted {
		if _, ok := documented[routed]; !ok {
			t.Errorf("pinned own spec does not document %s on %s (documented: %v)", routed, kafka.AnalyticsTopic, documented)
		}
	}
}

// E2: each derived golden decodes through the REAL cloudevents.Decode
// and routes through the REAL HandleMessage → toEvent → Projection.Apply
// path: exactly one applied event per delivery carrying the wire's own
// facts. The projector is a rebuildable projection with NO processed-
// event gate (its own documented model): every redelivery applies
// exactly once more — at-least-once by design, so the dedupe assertion
// here is one-apply-per-delivery, not zero-on-replay.
func TestGoldenEventsDecode_OwnAnalytics(t *testing.T) {
	for routed, kind := range ownAnalyticsRouted {
		t.Run(ownAnalyticsMessageNames[routed], func(t *testing.T) {
			raw := ownAnalyticsGolden(t, routed)
			rec, dlq := &anRecorder{}, &anFakeDLQ{}
			c := ownAnalyticsConsumer(rec, dlq)

			if err := c.HandleMessage(context.Background(), kafkago.Message{Topic: kafka.AnalyticsTopic, Value: raw}); err != nil {
				t.Fatalf("handle derived golden: %v", err)
			}
			assertAnalyticsApplied(t, routed, kind, rec)
			if got := len(dlq.written()); got != 0 {
				t.Fatalf("healthy golden dead-lettered %d messages", got)
			}

			if err := c.HandleMessage(context.Background(), kafkago.Message{Topic: kafka.AnalyticsTopic, Value: raw}); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			if rec.calls != 2 {
				t.Errorf("at-least-once projection: two deliveries must apply twice, applied %d", rec.calls)
			}
		})
	}
}

// assertAnalyticsApplied asserts one delivery applied exactly one event
// carrying the wire's own kind, id and subject aggregate.
func assertAnalyticsApplied(t *testing.T, routed string, kind report.Kind, rec *anRecorder) {
	t.Helper()
	if len(rec.events) != 1 {
		t.Fatalf("one delivery must apply exactly one event; applied %d", len(rec.events))
	}
	applied := rec.events[0]
	if applied.Kind != kind {
		t.Errorf("applied kind %q, want %q", applied.Kind, kind)
	}
	wantID := "contract-" + routed[strings.LastIndex(routed, ".")+1:]
	if applied.EventID != wantID {
		t.Errorf("applied event id %q, minted id is %q", applied.EventID, wantID)
	}
	if routed == cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)) && applied.RunID != "rebalance-2026-10-05-0930" {
		t.Errorf("applied run id %q, wire subject is the run id", applied.RunID)
	}
	if routed != cloudevents.Type("saga", string(report.KindRebalanceRunCompleted)) && applied.TransferID != "tr-42" {
		t.Errorf("applied transfer id %q, wire subject is the transfer id", applied.TransferID)
	}
}

// E3: the real encoder's output carries the fleet-mandatory CloudEvents
// 1.0 envelope with the analytics dataschema
// (urn:warehouse:network-inventory-planning:analytics:<Event>:vN — the
// analytics twins REUSE the integration event naming), and the
// self-pinned schemas require exactly those attributes.
func TestCloudEventsEnvelopeInvariants_OwnAnalytics(t *testing.T) {
	for routed := range ownAnalyticsRouted {
		raw := ownAnalyticsGolden(t, routed)
		evt, err := cloudevents.Decode(raw)
		if err != nil {
			t.Fatalf("decode minted %s golden: %v", routed, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", routed, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() != cloudevents.Source || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time incomplete (%q/%q/%q/%v)", routed, evt.ID(), evt.Source(), evt.Subject(), evt.Time())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", routed, evt.DataMediaType(), cloudevents.DataContentType)
		}
		wantSchema := "urn:warehouse:network-inventory-planning:analytics:" + evt.Type()[strings.LastIndex(evt.Type(), ".")+1:] + ":v1"
		if evt.DataSchema() != wantSchema {
			t.Errorf("%s: dataschema %q, want %q", routed, evt.DataSchema(), wantSchema)
		}
	}
	doc := loadPinnedAsyncAPI(t, pinnedOwnSpec)
	for routed, name := range ownAnalyticsMessageNames {
		msg := doc.message(t, name)
		if c := doc.payloadConsts(t, msg.Payload)["type"]; c != routed {
			t.Errorf("self-pinned %s pins type %q, consumer routes %q", name, c, routed)
		}
		required := doc.envelopeRequired(t, msg.Payload)
		for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
			if !required[attr] {
				t.Errorf("self-pinned %s envelope does not require %q (required: %v)", name, attr, required)
			}
		}
	}
}

// E4: an unknown event TYPE on the analytics stream is logged and
// skipped (additive evolution — no apply, no DLQ); a message that is not
// a CloudEvent is skipped-and-committed with a rate-limited WARN; and a
// known type with an unusable payload is DEAD-LETTERED at once — this
// consumer's OWN documented policy (deterministic refusals are dlq'd,
// never retried).
func TestUnknownAttributesTolerated_OwnAnalytics(t *testing.T) {
	routed := cloudevents.Type("saga", string(report.KindStateAdvanced))
	rec, dlq := &anRecorder{}, &anFakeDLQ{}
	c := ownAnalyticsConsumer(rec, dlq)
	ctx := context.Background()

	// Unknown additive attributes in data: tolerated end-to-end.
	tolerant := ownAnalyticsGolden(t, routed)
	tolerant, err := withAdditiveData(tolerant)
	if err != nil {
		t.Fatalf("rewrite golden data: %v", err)
	}
	if err := c.HandleMessage(ctx, kafkago.Message{Topic: kafka.AnalyticsTopic, Value: tolerant}); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if rec.calls != 1 || len(dlq.written()) != 0 {
		t.Fatalf("unknown data attributes changed the outcome (%d applies, %d dlq)", rec.calls, len(dlq.written()))
	}

	// Unknown event TYPE: logged, ignored, committed past.
	unknownType, err := withType(tolerant, "com.warehouse.wes.network-inventory-planning.saga.FutureOccurrence")
	if err != nil {
		t.Fatalf("rewrite golden type: %v", err)
	}
	if err := c.HandleMessage(ctx, kafkago.Message{Topic: kafka.AnalyticsTopic, Value: unknownType}); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if rec.calls != 1 || len(dlq.written()) != 0 {
		t.Errorf("unknown event type applied or dead-lettered (%d applies, %d dlq)", rec.calls, len(dlq.written()))
	}

	// Known type with an unusable payload (transfer_id != subject):
	// dead-lettered at once, never an error.
	poison, err := withSubject(tolerant, "other-transfer")
	if err != nil {
		t.Fatalf("rewrite golden subject: %v", err)
	}
	if err := c.HandleMessage(ctx, kafkago.Message{Topic: kafka.AnalyticsTopic, Value: poison}); err != nil {
		t.Fatalf("poison payload must be dead-lettered, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 1 {
		t.Errorf("poison payload dead-lettered %d messages, want 1", got)
	}

	// Not a CloudEvent: skipped and committed past, never dead-lettered.
	if err := c.HandleMessage(ctx, kafkago.Message{Topic: kafka.AnalyticsTopic, Value: []byte(`{"transfer_id":"tr-42"}`)}); err != nil {
		t.Fatalf("non-CloudEvents message must be skipped, not an error: %v", err)
	}
	if got := len(dlq.written()); got != 1 || rec.calls != 1 {
		t.Errorf("non-CloudEvents message applied or dead-lettered (%d applies, %d dlq)", rec.calls, len(dlq.written()))
	}
}

// withAdditiveData deep-rewrites the minted envelope's data object with
// two unknown additive keys (scalar + nested).
func withAdditiveData(raw []byte) ([]byte, error) {
	return rewriteData(raw, func(data map[string]any) {
		data["future_additive_field"] = "unknown-to-this-consumer"
		data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	})
}

// withType rewrites the envelope's CloudEvents type.
func withType(raw []byte, typ string) ([]byte, error) {
	return rewriteEnvelope(raw, func(env map[string]any) { env["type"] = typ })
}

// withSubject rewrites the envelope's CloudEvents subject (poison: the
// data's transfer_id no longer matches it).
func withSubject(raw []byte, subject string) ([]byte, error) {
	return rewriteEnvelope(raw, func(env map[string]any) { env["subject"] = subject })
}

// rewriteEnvelope unmarshals the minted envelope, applies fn, and
// remarshals it (JSON in, JSON out — the encoder's own wire format).
func rewriteEnvelope(raw []byte, fn func(map[string]any)) ([]byte, error) {
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	fn(env)
	return json.Marshal(env)
}

// rewriteData is rewriteEnvelope scoped to the envelope's data object.
func rewriteData(raw []byte, fn func(map[string]any)) ([]byte, error) {
	return rewriteEnvelope(raw, func(env map[string]any) {
		if data, ok := env["data"].(map[string]any); ok {
			fn(data)
		}
	})
}
