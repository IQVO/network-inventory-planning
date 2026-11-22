package kafka

// Shared support for the event consumer contract tests (E1-E4), which
// validate this repo's inbound Kafka edges against PINNED copies of the
// producing side's AsyncAPI spec (contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop). The pinned copies are byte-for-byte producer files: never
// edit them — refresh = copy the producer's file over the pin.
//
// The fleet's pinned specs are NOT uniform. This harness reads the two
// shapes this repo's producers actually use (verified against each pin):
//
//   - AsyncAPI 2.6 channel-perspective (all six sibling producers): the
//     channel is keyed by the topic, and its 'subscribe:' operation (the
//     app-perspective 'publish:' is accepted too) carries the message
//     list as a single $ref or a oneOf of $refs;
//   - AsyncAPI 3.0 (this repo's own self-pinned spec): channels are
//     keyed by a logical name with an `address` holding the topic, and
//     the channel itself carries a `messages` map of $refs.
//
// The pinned spec is the MESSAGE CATALOG ONLY: what this repo actually
// subscribes to is derived from the reader-config wiring in the consumer
// files (the *Topic constants passed to readerConfig), never from the
// spec's operations.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// The pinned spec copies next to this consuming adapter.
const (
	pinnedFacilitySpec    = "testdata/facility-layout.asyncapi.yaml"
	pinnedFulfillmentSpec = "testdata/fulfillment-execution.asyncapi.yaml"
	pinnedInventorySpec   = "testdata/inventory-storage.asyncapi.yaml"
	pinnedOrderSpec       = "testdata/order-management.asyncapi.yaml"
	pinnedProductSpec     = "testdata/product-master.asyncapi.yaml"
	pinnedPlanningSpec    = "testdata/warehouse-planning.asyncapi.yaml"
	pinnedOwnSpec         = "testdata/network-inventory-planning.asyncapi.yaml"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 and 3.0 the
// contract tests read: channels (with their operations or channel-level
// message maps), and the components/messages + components/schemas the
// message $refs point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

// pinnedChannel is a 2.6 channel (keyed by topic, carrying subscribe/
// publish operations) or a 3.0 channel (keyed by logical name, carrying
// an address and a channel-level messages map).
type pinnedChannel struct {
	Address    string                      `yaml:"address"`
	Subscribe  *pinnedOperation            `yaml:"subscribe"`
	Publish    *pinnedOperation            `yaml:"publish"`
	Messages   map[string]pinnedMessageRef `yaml:"messages"`
	Operations map[string]any              `yaml:"operations"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
	Schemas  map[string]any           `yaml:"schemas"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Payload     any             `yaml:"payload"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// channel returns the pinned spec's channel for a topic, matching the
// 2.6 convention (channel key IS the topic) or the 3.0 convention
// (logical-name key with the topic in `address`).
func (d *pinnedAsyncAPI) channel(t *testing.T, topic string) pinnedChannel {
	t.Helper()
	if ch, ok := d.Channels[topic]; ok {
		return ch
	}
	for _, ch := range d.Channels {
		if ch.Address == topic {
			return ch
		}
	}
	t.Fatalf("pinned spec %s has no channel for topic %q", d.AsyncAPI, topic)
	return pinnedChannel{}
}

// channelMessageRefs returns the components/messages names a topic's
// channel documents, from a 2.6 'subscribe:'/'publish:' operation message
// (fleet channel-perspective: a producer's own topics live under
// 'subscribe:') or from a 3.0 channel-level messages map.
func (d *pinnedAsyncAPI) channelMessageRefs(t *testing.T, topic string) []string {
	t.Helper()
	ch := d.channel(t, topic)
	refs := messageRefsOf(t, ch.Subscribe)
	refs = append(refs, messageRefsOf(t, ch.Publish)...)
	for _, key := range sortedKeys(ch.Messages) {
		refs = append(refs, ch.Messages[key])
	}
	if len(refs) == 0 {
		t.Fatalf("pinned spec documents topic %q with no messages", topic)
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, refName(t, r.Ref))
	}
	return names
}

func messageRefsOf(t *testing.T, op *pinnedOperation) []pinnedMessageRef {
	t.Helper()
	if op == nil || op.Message == nil {
		return nil
	}
	var refs []pinnedMessageRef
	if op.Message.Ref != "" {
		refs = append(refs, *op.Message)
	}
	return append(refs, op.Message.OneOf...)
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// documentedTypes maps the CloudEvents `type` each message on the topic
// documents to the message's name. The type is read from the payload
// schema's const where the spec pins it; specs that type the envelope's
// `type` as a plain string (product-master, and exception-management's
// own spec) document the concrete type in each message's own example
// payload — read it from there.
func (d *pinnedAsyncAPI) documentedTypes(t *testing.T, topic string) map[string]string {
	t.Helper()
	documented := map[string]string{}
	for _, name := range d.channelMessageRefs(t, topic) {
		msg := d.message(t, name) // fails the test on a dangling $ref
		if c := d.payloadConsts(t, msg.Payload)["type"]; c != "" {
			documented[c] = name
			continue
		}
		for _, ex := range msg.Examples {
			if typ, ok := ex.Payload["type"].(string); ok {
				documented[typ] = name
				break
			}
		}
	}
	return documented
}

// goldenExamples returns the spec's own examples for one message name on
// a topic; skip-when-empty lets E2 name the type as example-backfill
// pending instead of failing on a catalogued type.
func (d *pinnedAsyncAPI) goldenExamples(t *testing.T, topic, messageName string) []pinnedExample {
	t.Helper()
	return d.message(t, messageName).Examples
}

// payloadConsts collects the const values a message payload declares for
// CloudEvents envelope attributes (type, dataschema, ...), wherever the
// spec keeps them — directly under properties, in an allOf overlay on
// the shared envelope schema, or behind a payload-level $ref into
// components/schemas.
func (d *pinnedAsyncAPI) payloadConsts(t *testing.T, payload any) map[string]string {
	t.Helper()
	consts := map[string]string{}
	d.collectPayloadConsts(t, payload, consts, map[string]bool{})
	return consts
}

func (d *pinnedAsyncAPI) collectPayloadConsts(t *testing.T, node any, consts map[string]string, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectPayloadConsts(t, schema, consts, resolving)
		return
	}
	for attr, c := range propertyConsts(v) {
		if _, seen := consts[attr]; !seen {
			consts[attr] = c
		}
	}
	for _, item := range allOfItems(v) {
		d.collectPayloadConsts(t, item, consts, resolving)
	}
}

// resolveSchemaRef resolves node's top-level $ref into
// components/schemas (cycle-guarded). ok is false when node carries no
// $ref, or the ref is already being resolved.
func (d *pinnedAsyncAPI) resolveSchemaRef(t *testing.T, node map[string]any, resolving map[string]bool) (any, bool) {
	ref, ok := node["$ref"].(string)
	if !ok {
		return nil, false
	}
	name := refName(t, ref)
	if resolving[name] {
		return nil, false
	}
	resolving[name] = true
	schema, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("pinned spec: unresolved schema $ref %q", ref)
	}
	return schema, true
}

// propertyConsts returns the const string each directly-declared property
// of node pins (empty when node pins none).
func propertyConsts(node map[string]any) map[string]string {
	consts := map[string]string{}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return consts
	}
	for attr, p := range props {
		if pm, ok := p.(map[string]any); ok {
			if c, ok := pm["const"].(string); ok {
				consts[attr] = c
			}
		}
	}
	return consts
}

// allOfItems returns node's allOf entries (nil when none).
func allOfItems(node map[string]any) []any {
	items, _ := node["allOf"].([]any)
	return items
}

// envelopeRequired returns the required attribute names declared by a
// message payload's envelope schema (resolving top-level $refs into
// components/schemas). The subset that names CloudEvents attributes is
// what E3 asserts on.
func (d *pinnedAsyncAPI) envelopeRequired(t *testing.T, payload any) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	d.collectRequired(t, payload, required, map[string]bool{})
	return required
}

func (d *pinnedAsyncAPI) collectRequired(t *testing.T, node any, required map[string]bool, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	for _, r := range requiredList(v) {
		required[r] = true
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectRequired(t, schema, required, resolving)
		return
	}
	for _, item := range allOfItems(v) {
		d.collectRequired(t, item, required, resolving)
	}
}

// requiredList returns node's required attribute names (nil when none).
func requiredList(node map[string]any) []string {
	raw, ok := node["required"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// refName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// goldenData returns the example's `data` object (yaml.v3 decodes whole
// numbers as int — coerce through numI64 when comparing).
func goldenData(t *testing.T, ex pinnedExample) map[string]any {
	t.Helper()
	data, ok := ex.Payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("golden example %s carries no data object", ex.Name)
	}
	return data
}

// numI64 coerces a YAML-decoded number (int | int64 | float64) to int64.
func numI64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		t.Fatalf("value %v (%T) is not a number", v, v)
		return 0
	}
}

// strOf coerces a YAML-decoded scalar to string.
func strOf(t *testing.T, v any) string {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("value %v (%T) is not a string", v, v)
	}
	return s
}

// mintFreshID returns the payload with a freshly minted CloudEvents id.
// Several producers' specs reuse ONE id across all of a channel's
// examples, so adapted/derived clones must mint their own before they
// can exercise anything but the dedupe path.
func mintFreshID(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	fresh := clonePayload(payload)
	fresh["id"] = uuid.NewString()
	return fresh
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}

// sortedKeys gives deterministic iteration over a message map.
func sortedKeys(m map[string]pinnedMessageRef) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// envelopeCheck asserts the fleet-mandatory CloudEvents 1.0 envelope on
// one golden payload (E3): specversion, id/source/subject/time present,
// datacontenttype, and the dataschema prefix with the type's .vN suffix.
// Some specs omit per-message contentType and inherit the document-level
// defaultContentType — datacontenttype is asserted present, not equal to
// a constant, when inherit is true.
func envelopeCheck(t *testing.T, name string, payload map[string]any, producerRepo string, inheritDCT bool) {
	t.Helper()
	if sv, _ := payload["specversion"].(string); sv != "1.0" {
		t.Errorf("%s: specversion %q, want 1.0", name, sv)
	}
	for _, attr := range []string{"id", "source", "subject", "time"} {
		if s, _ := payload[attr].(string); s == "" {
			t.Errorf("%s: envelope attribute %q must be present and non-empty", name, attr)
		}
	}
	if inheritDCT {
		if s, _ := payload["datacontenttype"].(string); s == "" {
			t.Errorf("%s: datacontenttype must be present (directly or via defaultContentType)", name)
		}
	} else if s, _ := payload["datacontenttype"].(string); s != "application/json" {
		t.Errorf("%s: datacontenttype %q, want application/json", name, s)
	}
	ds, _ := payload["dataschema"].(string)
	wantPrefix := "urn:warehouse:" + producerRepo + ":"
	if !strings.HasPrefix(ds, wantPrefix) {
		t.Errorf("%s: dataschema %q must start with %q", name, ds, wantPrefix)
	}
	if !strings.Contains(ds, ":v") {
		t.Errorf("%s: dataschema %q must carry the type's .vN suffix", name, ds)
	}
}
