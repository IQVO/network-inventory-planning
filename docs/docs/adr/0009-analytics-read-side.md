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
| `TransferStateAdvanced` | `transfer_id`, `from` (empty for the creation entry), `to`, `age_seconds` (saga age at the transition), optional `dwell_seconds` (time spent in `from`; Amendment) | `transfer_state_advances` |
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
| `GET /reports/state-dwell` | time spent in each `from` state per day (Amendment: `dwell_seconds` p50/p95) | `days[]`: `day`, `state` (= `from`), `transitions`, `without_dwell`, `p50_dwell_seconds`, `p95_dwell_seconds` |
| `GET /reports/stuck-transfers` | stuck detections per state per day, plus the latest occurrences (`?limit=` 1–100, default 20) | `days[]`: `day`, `state`, `detections`, `transfers`; `latest[]`: `transfer_id`, `state`, `age_seconds`, `threshold_seconds`, `detected_at` |
| `GET /reports/rebalance-runs` | proposals vs rejected per day | `days[]`: `day`, `runs`, `proposals`, `rejected`, `rejection_rate`, `stale_facts` |
| `GET /reports/freshness` | how far the projection is behind (fleet analytics charter §4; one endpoint for all reports, which read one projection) | `as_of` (newest applied CloudEvents time), `lag_seconds` (now − as_of, never negative); both `null` until the first event |

**What `state-dwell` is.** Since the Amendment below, the report is the true time
spent in each state, from `dwell_seconds`. The original version of this report
percentiled `age_seconds` (the saga's age since creation, ADR 0007), which was
only a proxy for how far along the saga was; that caveat is superseded. The
`stuck-transfers` report remains the real "stuck in a state right now" signal
(its `age_seconds` is the age in the current state).

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
- `state-dwell` reports true per-state dwell (see the Amendment); events
  published before `dwell_seconds` existed are counted in `without_dwell`.

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
- **Add `dwell_seconds` to `TransferStateAdvanced` now**: rejected in the
  original slice (it changes the published contract); done in the Amendment.

## Amendment: true per-state dwell (`dwell_seconds`)

Status: Accepted. Supersedes the age-since-creation caveat of the original
`state-dwell` report; the ADR number is unchanged.

- **Publisher.** `TransferStateAdvanced` gains an additive integer
  `dwell_seconds`: occurred-at minus the time of the audit entry that moved the
  transfer INTO `from`. It is derived from the aggregate's own audit trail (the
  nearest earlier entry whose `to` equals `from`), so it is exact for a
  multi-hop trail and for a single transition appended to a loaded aggregate.
  The creation entry has no earlier entry and no entry time for `from`: the
  field is **omitted** there (never sent as 0). `age_seconds` is unchanged. The
  dataschema stays `...:TransferStateAdvanced:v1` — a new optional field is
  additive within v1 and consumers must tolerate its absence (catalogue row in
  ADR 0004 updated; `apis/asyncapi.yaml` updated).
- **Projector.** Additive migration `analytics/migrations/0002_state_advance_dwell`
  adds a nullable `dwell_seconds` (`>= 0`) to `transfer_state_advances`. The
  consumer stores it when present and projects an event without it as `NULL`
  (old events replay unchanged); a negative value is dead-lettered like any
  other deterministically bad payload. Idempotency on the CloudEvents id is
  unchanged. Rolling back the migration drops only the new column.
- **Report.** `GET /reports/state-dwell` returns per UTC day and `from` state:
  `transitions` (all), `without_dwell` (those with no `dwell_seconds`),
  `p50_dwell_seconds` and `p95_dwell_seconds`. Transitions without a dwell are
  **excluded** from the percentiles and never treated as zero; the percentiles
  are `null` when every transition of the day lacks one. The report fields
  `p50_age_seconds` / `p95_age_seconds` are removed (the service is
  cluster-internal and its only consumer is this repo's tests). A state "left"
  into itself — the aggregate's DRAFT → DRAFT creation record — leaves no state
  and is no longer counted (before, it was a spurious zero-age DRAFT row).
- **Consequence.** Events published before this change stay in the model as
  `NULL`, so a day mixing old and new events shows a non-zero `without_dwell`;
  the model is not backfilled (a dwell cannot be reconstructed from
  `age_seconds` alone).
