# ADR-0009: Analytics read side — a projector and read-only reports over a separate analytical database

Status: Accepted

Additive: the OLTP API, the integration topic, the analytics publisher
(ADR 0007) and its payloads are unchanged. Adds two binaries, one
analytical-database migration set, an opt-in chart block and a second
(separate) database.

## Context

Every OLTP bounded context of the fleet ships an analytics read side
("data-mesh read side"): a separate analytical Postgres fed by a
**projector** that consumes the context's own analytics topic, and a
read-only **reports** binary over that database. The reference
implementations are `warehouse-planning` (ADR 0005), `workforce-management`
and `facility-layout`; this ADR mirrors their shape and records where this
service differs.

The **publisher side already exists** (ADR 0007): the transfer use cases and
the scheduled rebalance run raise `TransferStateAdvanced`,
`TransferStuckDetected` and `RebalanceRunCompleted` through the transactional
outbox onto `warehouse.network-inventory-planning.analytics`. Nothing consumed
them, so the saga's health could be seen in a trace but never queried as a
trend. This ADR adds the consuming half and changes **nothing** about what is
published.

## Decision

### 1. What is projected

Exactly the three published event types, dispatched on the **full**
CloudEvents `type` (`com.warehouse.wes.network-inventory-planning.saga.<Event>`;
`dataschema=urn:warehouse:network-inventory-planning:analytics:<Event>:v1`):

| event | payload | fact table |
| --- | --- | --- |
| `TransferStateAdvanced` | `transfer_id`, `from` (empty for the creation entry), `to`, `age_seconds` (saga age at the transition) | `transfer_state_advances` |
| `TransferStuckDetected` | `transfer_id`, `state`, `age_seconds`, `threshold_seconds` | `transfer_stuck_detections` |
| `RebalanceRunCompleted` | `run_id`, `proposal_count`, `rejected_count`, `stale_facts` | `rebalance_run_facts` |

The model is append-only: one row per CloudEvents id, keyed by it, plus an
`analytics_processed_events` set. Instants are the CloudEvents `time`
attributes. Nothing is invented: no event outside these three is projected, and
no payload field beyond those above is read. Unknown CloudEvents types on the
topic are **skipped and logged**, never fatal, so the stream can grow new event
types without a projector release (additive evolution).

### 2. The projector (`cmd/nip-projector`)

- Consumes the analytics topic under a **fixed consumer group read from env**
  (`ANALYTICS_CONSUMER_GROUP`, default `network-inventory-planning-analytics`,
  set by the chart; distinct from every OLTP group of this service). It is an
  at-least-once consumer, not a full-replay cache: committed offsets are
  honoured, and a brand-new group starts at the earliest offset so the model can
  be rebuilt from retained history.
- **Dedupe and effect in one transaction**: `Projection.Apply` inserts the
  CloudEvents `id` into `analytics_processed_events` (`ON CONFLICT DO NOTHING`)
  and appends the fact row in ONE analytical-database transaction. A failure
  leaves nothing written and the mark unrecorded; a replay of an applied id is a
  no-op. The offset is committed only after `Apply` returned.
- **Delivery policy**:
  - not a CloudEvents 1.0 message (garbage, the retired flat envelope): skipped
    and committed past, with a **rate-limited WARN** (the first is logged, then
    at most one per minute carrying the number suppressed);
  - a valid CloudEvent of another type: logged at INFO, ignored, committed past;
  - a known type with an unusable payload (subject is not the transfer/run id,
    missing `to`/`state`/counts, negative numbers, no `time`) or one the store
    deterministically rejects (Postgres data-exception / integrity-violation
    class, e.g. a CHECK): **dead-lettered immediately** to
    `warehouse.network-inventory-planning.analytics.dlq` (raw bytes plus
    `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` headers), then
    committed past; a failed DLQ write retries the message, so poison is never
    lost;
  - a **transient** failure (database down, timeout): the same message is
    retried with capped exponential backoff and **never** dead-lettered. A
    database outage must not turn into silently missing analytics; the cost is
    that a prolonged outage blocks the partition, which the logs make visible.
- Boot: the analytical migrations and the first ping run under
  `internal/bootretry` (the first outbound dial of an Istio-injected pod is
  reset ~10s after start); Kafka is dialled lazily by the reader. Admin port
  `:8091` serves `/healthz` and `/readyz` (flipped to 503 first on shutdown);
  graceful shutdown drains, lets the in-flight message finish, then closes the
  pool.
- It writes **only** to the analytical database (`ANALYTICS_DATABASE_URL`, a
  direct DSN; `ANALYTICS_MIGRATIONS_PATH`, default `analytics/migrations`) and
  never opens the OLTP database.

### 3. The reports (`cmd/nip-reports`, `:8092`)

Read-only HTTP over the analytical database through a read-only pool
(`default_transaction_read_only=on`, 15s statement timeout). The DSN is
`ANALYTICS_READER_DATABASE_URL`, falling back to `ANALYTICS_DATABASE_URL` for
single-role local runs. `/healthz`; RFC 7807 problem+json errors in the shape of
the rest of this API (`type`, `title`, `status`, `detail`); **no auth** (fleet
rule: the ClusterIP boundary is the access control); additive OpenAPI
(`apis/openapi.yaml`, tag `reports`, per-path server `:8092`). The reports
carry no OLTP use case and make no call to any sibling.

Conventions shared with the sibling services' reports: optional `from` / `to`
(RFC 3339; both omitted = the 30 days ending now; only `to` = the 30 days before
it; only `from` = from..now; at most 366 days; `from` inclusive, `to` exclusive;
an empty, inverted or oversized range is a 400). Buckets are **UTC calendar
days**. Empty results are empty arrays, never errors or `null`. Wire fields are
snake_case, like the event payloads.

| endpoint | question | shape |
| --- | --- | --- |
| `GET /reports/transfer-funnel` | how many distinct transfers reached each state per day | `days[]`: `day`, `state` (= `to`), `transfers` |
| `GET /reports/state-dwell` | `age_seconds` p50/p95 per `from` state per day | `days[]`: `day`, `state` (= `from`), `transitions`, `p50_age_seconds`, `p95_age_seconds` |
| `GET /reports/stuck-transfers` | stuck detections per state per day, plus the latest occurrences (`?limit=` 1–100, default 20) | `days[]`: `day`, `state`, `detections`, `transfers`; `latest[]`: `transfer_id`, `state`, `age_seconds`, `threshold_seconds`, `detected_at` |
| `GET /reports/rebalance-runs` | proposals vs rejected per day | `days[]`: `day`, `runs`, `proposals`, `rejected`, `rejection_rate`, `stale_facts` |
| `GET /reports/freshness` | how far the projection is behind (fleet analytics charter §4; one endpoint for all reports, which read one projection) | `as_of` (newest applied CloudEvents time), `lag_seconds` (now − as_of, never negative); both `null` until the first event |

**What `state-dwell` is and is not.** `TransferStateAdvanced.age_seconds` is the
saga's age **since creation** at the moment of the transition (ADR 0007), not
the time spent in the state it left. The report therefore shows how far along
the saga was when it left each state; the difference between consecutive states'
percentiles approximates the time spent. A true per-state dwell would need a
payload field (`dwell_seconds`) added to the publisher; that is a deliberate
follow-up, not done here because the publisher contract is out of scope. The
`stuck-transfers` report is the real "stuck in a state" signal (its
`age_seconds` is the age in the current state).

SQL only counts, sums and takes `percentile_cont`; rejection rate, range rules,
limit rules and the percentile definition live in `internal/analytics/report`
(pure, imports no other internal package, mutation tested). One contract suite
runs against the in-memory store and against real Postgres.

### 4. Why a separate analytical database

The projection is a derived copy that can be dropped and rebuilt from the topic.
Keeping it out of the OLTP database means a heavy report cannot take OLTP
connections, the two schemas evolve independently (`analytics/migrations` is
separate from `internal/adapters/outbound/postgres/migrations`), the reports
role can be read-only on a database that holds nothing transactional, and the
projector (the only writer) holds no credentials on OLTP data. The cost is a
second role/database to provision (`warehouse-infra`); both are direct DSNs
(PgBouncer is not involved: the projector runs migrations).

### 5. Packaging

The one image builds every `cmd/*`, so the binaries ship in the same image
(`/app/nip-projector`, `/app/nip-reports`, uid 1000) with `analytics/migrations`
in `/app/analytics/migrations` and `EXPOSE 8080 8090 8091 8092`. The chart gains
an **opt-in `analytics:` block** (default `enabled: false`; a default render is
byte-identical to before): two Deployments (components `analytics-projector`,
`analytics-reports`), a ClusterIP Service for reports only, an `analytics`
Secret (or `analytics.database.existingSecret`) and per-component HPA blocks.
Render-time guards refuse `analytics.enabled` without a DSN source or without
`kafka.enabled`, instead of crash-looping. The projector pod is never given the
OLTP `DATABASE_URL`; the reports pod is given neither Kafka nor any DSN but the
reader's. `tests/test_service_selectors.py` pins that each Service selects
exactly one Deployment; `tests/test_env_wiring.py` pins the env contracts.

## Consequences

- The analytics stream becomes a consumed contract of exactly one service-owned
  consumer (`nip-projector`); the `.events` integration topic is untouched.
- `analytics_*` metrics are not exposed (the binaries have no `/metrics` and no
  OTel exporter); a dashboard would need a metrics surface first. Trace context
  already flows through the consumer loop (ADR 0007).
- Reports are only as fresh as the projector; `/reports/freshness` makes the lag
  observable. The model begins at the earliest retained offset of the topic;
  events published before ADR 0007 shipped do not exist.
- `state-dwell` carries the age-since-creation caveat above.

## Alternatives considered and rejected

- **Project into the OLTP database** (tables next to `transfers`): no isolation
  of load, schema or credentials; a report bug could hold OLTP connections.
- **Compute the reports from `transfers` and its audit trail**: it would put
  analytical queries on the OLTP database, and the audit trail has no stuck
  detections or rebalance-run outcomes in this shape.
- **Dead-letter after N attempts for every failure** (some siblings' policy):
  drops analytics for events that failed only because the database was briefly
  down.
- **A full-replay cache consumer** (unique group per process): the model is a
  database, not a per-process cache; a fixed group gives at-least-once with
  committed offsets.
- **Fan the projector out of the `.events` topic**: the integration contract
  would have to carry analytics-only occurrences, and the siblings' convention
  is a dedicated analytics topic (already chosen in ADR 0007).
- **Add `dwell_seconds` to `TransferStateAdvanced` now**: changes the published
  contract (and its asyncapi/golden tests) outside this slice; recorded as a
  follow-up above.
