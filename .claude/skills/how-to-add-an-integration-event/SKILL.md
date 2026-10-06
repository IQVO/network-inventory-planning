---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, consumer-group rules. Use when touching internal/adapters kafka or outbox code, a publisher/consumer, or apis/asyncapi*.yaml.
---

<!-- TEMPLATE NOTE (warehouse-harness-template v2): adapt every repo-specific example in this file (file paths, type names, field names) to THIS repo real code. Do not copy-paste verbatim. -->

# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once.

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment — this repo
forwards only `StockReserved`/`ReservationRevoked`; everything else is a
local concern published only to the Postgres outbox
(`internal/adapters/outbound/postgres/event_publisher.go`) for audit, not
broadcast. Before adding a new event to the Kafka publisher, confirm a
sibling context genuinely needs to react to it — check
`docs/docs/ddd/context-map.md` or the equivalent ubiquitous-language doc
for who's actually downstream.

### 2. Envelope: CloudEvents 1.0, structured mode — MANDATORY

Every message is a CloudEvents 1.0 JSON document in structured content
mode (Kafka value = `application/cloudevents+json`), with the Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`. There is no
other envelope in this fleet — no flat `event_id`/`event_type`/`occurred_at`
shape, no dual-write, no `EVENT_ENVELOPE_MODE` toggle (see
`.claude/rules/integration-events.md`; a fitness test enforces it).

```json
{
  "specversion": "1.0",
  "id": "<uuid v4, minted once, persisted with the outbox row>",
  "source": "/warehouse/<repo>",
  "type": "com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>",
  "subject": "<aggregate id>",
  "time": "<domain occurred-at, RFC3339 UTC>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:<repo>:events:<EventName>:v1",
  "data": { "the": "actual payload, business types only" }
}
```

`type` follows the platform-wide reverse-DNS convention
`com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`, all
lowercase except the final PascalCase event name — e.g.
`com.warehouse.wms.inventory-storage.reservation.ReservationRevoked`. Take
the subdomain/context segment from the subdomain table on warehouse-docs'
Event Standard page (`docs/strategic-design/event-standard-cloudevents.md`);
don't guess it. The same `type` is used on the analytics topic; only
`dataschema` changes (`…:analytics:<EventName>:v1`).

### 3. Implementation

Add the event struct to `internal/domain/<aggregate>/` (it should already
exist as a domain event the aggregate raises — publishing wires an
EXISTING domain event onto Kafka, it doesn't invent a new payload shape at
the adapter layer). In the Kafka publisher adapter:

- Encode ONLY through `internal/adapters/kafka/cloudevents` (copied from
  this template's `templates/cloudevents/cloudevents.go.tmpl`):
  ```go
  value, err := cloudevents.New(cloudevents.Spec{
      ID:        evt.ID(),            // minted once; the outbox row stores it
      Entity:    "reservation",
      EventName: "ReservationRevoked",
      Subject:   evt.ReservationID(),
      Time:      evt.OccurredAt(),
      Stream:    cloudevents.StreamEvents,
      Version:   1,
      Data:      payload,              // unchanged wire payload
  })
  msg := kafkago.Message{
      Key:     []byte(evt.ReservationID()),
      Value:   value,
      Headers: append(traceHeaders, cloudevents.ContentTypeHeader()),
  }
  ```
- Give the message a partition key that keeps ordering where it matters
  (the aggregate id) and keep the `kafkago.Hash{}` balancer
- Use `Topic` — this service's own topic constant
  (`warehouse.<context>.events`), never a sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel
  (`defaultContentType: application/cloudevents+json`, the shared
  CloudEvents envelope schema with every attribute required), with its
  exact `type` const and `dataschema`, matching the entity-grouping
  convention already there (group by aggregate, not chronologically)
- Regenerate the AsyncAPI HTML reference:
  ```bash
  cd docs && npm run gen-async-docs:all   # or gen-async-docs, check package.json
  ```
  This repo's `docs-api-drift` CI job fails the PR if the generated
  `static/asyncapi/<ctx>/` output doesn't match a fresh regen — a nullable
  field change here has bitten before.

### 5. Test

Add a golden exact-JSON unit test for the new `type` asserting every
CloudEvents attribute, the `type` string and the `content-type` header,
against a fake `Writer` (see `publisher_test.go` — never a real broker in
a unit test). If this event
now needs a `_integration_test.go` asserting real delivery, it MUST use
testcontainers (see the fitness test `TestKafkaIntegrationTestsUseTestcontainers`
in `internal/architecture/` — a skip-gated `KAFKA_BROKERS` test or a
hardcoded `localhost:9092` fails CI).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name, its exact CloudEvents `type`
strings and payload shape ONLY — never its Go types. See `internal/adapters/outbound/facilitycache/consumer.go`'s
own doc comment: "This service has no business knowing anything else
about that context beyond this topic name and the envelope/payload shapes
below." Hand-mirror the payload struct locally; do not add a Go module
dependency on the sibling repo (an architecture fitness test in most
repos in this fleet would catch that anyway for the stricter contexts —
check this repo's own `internal/architecture/` for a
`TestNoSiblingContextOutboundCalls`-style guard before assuming it's
allowed).

### 2. Decode CloudEvents only, dispatch on the full `type`

```go
evt, err := cloudevents.Decode(msg.Value)
if err != nil { // errors.Is(err, cloudevents.ErrNotCloudEvent): deterministic poison
    // existing DLQ path if this consumer has one, else:
    logger.Warn("skipping non-CloudEvents message", "topic", msg.Topic,
        "partition", msg.Partition, "offset", msg.Offset, "err", err)
    return commit(msg) // never crash, never block the partition
}
switch evt.Type() {
case "com.warehouse.wes.fulfillment-execution.task.TaskCompleted": // exact, byte-identical to the producer
    var p taskCompletedData // local mirror of the payload
    if err := evt.DataAs(&p); err != nil { /* poison: skip as above */ }
    // dedupe on evt.ID(); use evt.Time() / evt.Subject() from attributes
default:
    return commit(msg) // unknown types are ignored, not errors
}
```

Never parse a legacy flat shape as a fallback, never dispatch on a short
name or suffix match. Add a test that a legacy flat-envelope message is
rejected (skipped/DLQ'd), not parsed.

### 3. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one for your use
case is THE most common integration-event mistake in this fleet, and it
was learned from a real incident (wes-work-planning#67).

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at
a time (e.g. this service's own analytics projector). The group id is a
plain named constant (`AnalyticsConsumerGroup`), reused across restarts —
that's correct because Kafka's committed-offset resume semantics are
EXACTLY what you want: pick up where the single instance left off.

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work queue) —
see `facilitycache/consumer.go`'s `consumerGroupPrefix` +
`uniqueConsumerGroup()`. The group id MUST be unique per process instance
(hostname+PID+timestamp), NEVER a fixed shared string. Consumer group
offsets are shared infrastructure state: a brand-new process joining a
group an EARLIER instance already consumed resumes from that instance's
committed offset, so the new process gets marked "ready" with an empty
local cache having replayed nothing — a silent correctness bug, not a
crash.

**Never do this** (the actual incident): a fixed shared consumer group id
on a consumer meant to run as exactly one instance per environment. When
a local dev/test harness process joins the SAME broker's SAME group as a
live in-cluster Deployment, Kafka's rebalance protocol hands the
partition to only ONE of the two group members — the other silently
starves. Fix: make the group id env-configurable
(`KAFKA_CONSUMER_GROUP`/`<SERVICE>_CONSUMER_GROUP`), never hardcode it as
a literal string. This fleet's `internal/architecture/`
`TestKafkaConsumerGroupNeverHardcodedInline` fitness test (where present)
enforces this statically — an inline `GroupID: "literal"` fails CI.

### 4. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other
code depends on, expose a `Ready()` gate the health check consults, and
block readiness (not process startup — a transient Kafka outage
shouldn't be fatal) until the initial replay finishes. A readiness check
that only re-evaluates on a NEW message arriving deadlocks forever on an
ordinary restart where a shared/already-caught-up group never gets a new
message to trigger it — use `OffsetFetch` against the group's committed
offset, or (simpler and less bug-prone) just use the per-process-unique
group pattern above, which sidesteps the whole class of bug.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package import
```

Prove any new fitness-test-adjacent behavior actually matters by running
the specific scenario against a real broker if this repo has
testcontainers-based integration tests for the consumer/publisher touched.
