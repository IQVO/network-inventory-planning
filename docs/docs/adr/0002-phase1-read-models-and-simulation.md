# ADR-0002: Phase-1 local read models, fail-closed planning snapshot and advisory simulation

Status: Accepted

## Context

ADR-0001 established network-inventory-planning as a bounded context that
integrates with siblings exclusively through versioned CloudEvents facts —
never synchronous calls. Phase 0 shipped the explicit-snapshot diagnostic
endpoint (`POST /v1/transfer-proposals:generate`), which requires a caller
to assemble a coherent snapshot by hand. Planning anything real needs the
facts themselves, held locally, with explicit rules for what happens when
they are missing or stale.

Three producer facts are confirmed on the producers' own contracts
(`apis/asyncapi.yaml` on origin/develop, read before any consumer code was
written — see the fleet rule against trusting a design doc's conceptual
model):

1. `com.warehouse.wms.facility-layout.site.SiteCapabilityChanged`
   (facility-layout, `warehouse.facility.events`):
   `{site_code, transfer_origin_enabled, transfer_destination_enabled,
   capability_revision}`.
2. `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged`
   (order-management, `warehouse.order-management.events`):
   `{source_order_id, line_no, site_id, sku, demanded_units, due_at,
   state, assignment_version}`.
3. `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished`
   (warehouse-planning, `warehouse.warehouse-planning.events`): the
   existing v1 payload plus the additive `site_id`.

## Decision

### Postgres persistence (DATABASE_URL)

- `DATABASE_URL` unset → the zero-config diagnostic service exactly as
  before: no read models, no consumers, `GET /v1/transfer-simulations`
  answers 503 (`read-models-unavailable`), never a fabricated snapshot.
- `DATABASE_URL` set → migrations run at startup, the pool is pinged, and a
  failure to connect REFUSES to boot (fail, never fall back — a silent
  fallback looks healthy while dropping exactly the state persistence was
  added for).
- Tables (migration 0001): `processed_events` (dedupe on `(consumer,
  event_id)`, claimed in-transaction), `site_capability` (keyed site_id,
  LWW on capability_revision), `site_sku_demand` (keyed
  source_order_id+line_no, state ACTIVE/REMOVED — a REMOVED fact
  tombstones the row), `published_capacity_plan` (keyed plan_id, LWW on
  the CloudEvents time).

### Consumers

Three inbound Kafka consumers (one per topic) follow the fleet's
at-least-once atomicity checklist:

- `FetchMessage`/`CommitMessages` (never `ReadMessage`'s auto-commit); the
  offset commits only AFTER the read-model transaction committed.
- The processed-event claim and the read-model upsert run inside ONE
  `ports.UnitOfWork` (pgtx-carried transaction); a failed handling rolls
  the claim back, so a redelivery is processed, not skipped.
- Structured CloudEvents 1.0 validation via the repo's own
  `internal/adapters/kafka/cloudevents`; dispatch on the FULL type string;
  unknown types ignored; non-CloudEvents, malformed payloads, missing
  fields and domain-validation failures are deterministic — logged and
  committed past, never retried, never crashed on.
- Transient failures (DB/tx) return non-nil and the run loop retries the
  SAME message with capped exponential backoff (200ms→5s).
- Consumer group ids come from env with NO default
  (`SITE_CAPABILITY_CONSUMER_GROUP`, `SITE_SKU_DEMAND_CONSUMER_GROUP`,
  `CAPACITY_PLAN_CONSUMER_GROUP`); unset means the consumer does not exist
  (a default would let a locally run process join the live cluster group).
- A legacy `CapacityPlanPublished` without `site_id` is EXCLUDED — the site
  is never inferred from `warehouse_id`. Exclusion is a skip-and-log, not
  an error.

### Fail-closed PlanningSnapshot

`internal/domain/planning.BuildSnapshot` refuses (returns an error, never
a degraded snapshot) when:

- any read model is entirely empty (no capabilities / no demands / no
  plans);
- any fact's watermark is older than `MaxStaleness` (default 10m, env
  `PLANNING_MAX_STALENESS`);
- a participating site is disabled as a transfer origin or destination.

A site missing ANY of the three facts is excluded from the snapshot rather
than zero-filled; if no site survives, that is an error too. Capacity
windows are `[window_start, window_end)` and demand is filtered by
`due_at` against that half-open interval. The snapshot's `AsOf` is the
OLDEST watermark of the included facts.

### Advisory-only simulation

`GET /v1/transfer-simulations` serves `SimulateTransferOptions` — a
per-site view (capability directions, window-filtered demand total,
capacity over window, headroom) with `advisory: true`. It reserves
nothing, moves nothing, promises nothing. Incomplete/stale read models
yield `503` with an RFC 7807 problem (`read-models-incomplete`). The
explicit-snapshot `POST /v1/transfer-proposals:generate` stays unchanged
as the diagnostic path.

## Consequences

- Nothing is published yet: no outbox, no produced event types. The
  planned TransferProposed/TransferPlanApproved/OriginReservationRequested
  types enter `apis/asyncapi.yaml` atomically with the code that publishes
  them (Phase 2+).
- The read models are this context's local property; the producers remain
  authoritative and can change their payloads only via new versions, which
  this contract mirrors on the next read.
- Unknown-or-stale data excludes a candidate from planning rather than
  inventing a safe-looking answer from incomplete state — the network
  inventory transfers reference's "make unknown or stale data exclude a
  candidate" rule.
- Integration confidence comes from testcontainers Postgres + Kafka
  (integration build tag), never a skip-gated external broker.
