---
id: runbook
title: Runbook
sidebar_label: Runbook
description: Deploying and operating network-inventory-planning - Deployments, probes, migrations, Kafka topics and groups, the outbox relay, the DLQ, tickers, scaling and routine procedures.
---

# Runbook

## Deployment

The service ships as one Helm chart, `charts/network-inventory-planning`
(chart name `network-inventory-planning`), and one image,
`claudioed/network-inventory-planning` (all four binaries in `/app`). Edge
routing is owned by warehouse-infra: Kong `:8000` serves the REST API under
`/api/network-inventory-planning` through the chart's disabled-by-default
`gatewayApi` / `ingress` values; the Nginx web gateway `:80` serves the console
remote under `/mfes/network-inventory-planning/`.

| Deployment (component label) | Command | Enabled by | Container port | Service | Probes |
| --- | --- | --- | --- | --- | --- |
| `api` | image entrypoint `./network-inventory-planning` | always | 8080 (`http`) | ClusterIP 80 → 8080 | startup, liveness and readiness all `GET /healthz` (startup: every 2s, 30 failures = 60s for migrations) |
| `mcp` | `/app/mcp` | `mcp.enabled` | 8090 | ClusterIP 8090 | `GET /healthz` |
| `analytics-projector` | `/app/nip-projector` | `analytics.enabled` | 8091 (`admin`) | none | startup + liveness `GET /healthz`, readiness `GET /readyz` |
| `analytics-reports` | `/app/nip-reports` | `analytics.enabled` | 8092 | ClusterIP 80 → 8092 | `GET /healthz` |
| `frontend` (`nip_mfe`) | nginx | `frontend.enabled` | 8080 | ClusterIP 80 → 8080 | `GET /healthz` |

### What the probes actually prove

- **api**: there is no `/readyz`. `GET /healthz` answers `204` as soon as the
  HTTP server listens, and the server only starts listening **after** the
  migrations applied and the pool pinged (`wire` runs before
  `ListenAndServe`). Kafka is not checked: consumers and the relay start in
  goroutines, and a broker outage shows up in logs and consumer lag, not in
  the probes.
- **mcp**: `GET /healthz` is a static `{"status":"ok"}`; like the API it only
  listens after migrations and the ping (when `DATABASE_URL` is set).
- **analytics-projector**: `/healthz` never flips; `/readyz` answers
  `{"status":"ready"}` and flips to `503 {"status":"not_ready"}` as the first
  step of graceful shutdown. The analytical migrations and the database ping
  run before the admin server starts, each retried by `internal/bootretry`
  (5 attempts, 1s doubling, about 31s) to survive the Istio first-dial reset.
- **analytics-reports**: `GET /healthz` is static; the database ping before
  listening is retried the same way.

`terminationGracePeriodSeconds` is 30; every binary drains its HTTP server for
up to 10s on SIGTERM.

## Migrations

| Database | Directory | Run by | When |
| --- | --- | --- | --- |
| OLTP | `internal/adapters/outbound/postgres/migrations` (`/app/migrations` in the image) | `network-inventory-planning` and `mcp` | At every start, before the server listens, through golang-migrate. A failure refuses the boot. |
| Analytical | `analytics/migrations` | `nip-projector` only | At every start, under boot retry. |

`MIGRATIONS_DATABASE_URL` (secret key `database.migrationsExistingSecretKey`,
optional) gives the migration step a direct connection when `DATABASE_URL`
goes through PgBouncer (ADR 0006). There is no separate migration Job: a
release that ships a migration applies it when the first new pod starts;
golang-migrate's advisory lock serialises the API and MCP pods.

## Kafka

One broker serves the whole platform (in-cluster; `localhost:9092` from the
host). Every message is a CloudEvents 1.0 event in structured mode.

### Produced

| Topic | Types (`com.warehouse.wes.network-inventory-planning.` + …) | Key | Producer |
| --- | --- | --- | --- |
| `warehouse.network-inventory-planning.events` | `transfer.TransferPlanApproved`, `transfer.TransferAllocationRequested`, `workdemand.WorkDemandReleased` | transfer id, `transfer_line_id`, `demand_id` | outbox relay in the API |
| `warehouse.network-inventory-planning.analytics` | `saga.TransferStateAdvanced`, `saga.TransferStuckDetected`, `saga.RebalanceRunCompleted` | transfer id / run id | outbox relay in the API |
| `warehouse.network-inventory-planning.analytics.dlq` | raw poison messages from the analytics topic | original key | `nip-projector` |

The relay writer sets `AllowAutoTopicCreation` (the relay is the first writer
of both own topics), `RequiredAcks: all` and `BatchSize: 1`.

### Consumed

| Consumer (`processed_events.consumer`) | Topic | Types | Group id variable | Binary |
| --- | --- | --- | --- | --- |
| `site-capability-consumer` | `warehouse.facility.events` | `com.warehouse.wms.facility-layout.site.SiteCapabilityChanged` | `SITE_CAPABILITY_CONSUMER_GROUP` | API |
| `site-sku-demand-consumer` | `warehouse.order-management.events` | `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged` | `SITE_SKU_DEMAND_CONSUMER_GROUP` | API |
| `capacity-plan-consumer` | `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | `CAPACITY_PLAN_CONSUMER_GROUP` | API |
| `transfer-reply-consumer` | `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated`, `...reservation.TransferStockAllocationRejected`, `...stock.TransferReceiptStaged`, `...stock.TransferStockStowed` | `TRANSFER_REPLY_CONSUMER_GROUP` | API |
| `transfer-fact-consumer` | `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.transfer.TransferPicked`, `...TransferDispatched`, `...TransferArrived` | `TRANSFER_FACT_CONSUMER_GROUP` | API |
| analytics consumer | `warehouse.network-inventory-planning.analytics` | the three `saga.*` types | `ANALYTICS_CONSUMER_GROUP` (default `network-inventory-planning-analytics`) | `nip-projector` |

The five API group ids have **no default** and are empty in `values.yaml`:
each is the off switch of its consumer and must be filled per environment.
Other types on a consumed topic are ignored.

### Delivery semantics

- **API consumers** (`internal/adapters/inbound/kafka/kafka.go`): at-least-once
  with an atomic effect. The `processed_events` claim and the side effect
  commit in one transaction; the offset is committed only after that. A
  deterministic problem (not a CloudEvent, malformed payload, failed domain
  validation, unknown transfer, illegal transition, refused fact) is logged
  at WARN and committed past. A transient error retries **the same message
  forever** with backoff 200ms → 5s, logging at ERROR, and blocks its
  partition. There is no DLQ for these consumers.
- **Analytics consumer**: non-CloudEvents messages are skipped (one WARN per
  minute with a suppressed count); unknown types are skipped at INFO; a known
  type with an unusable payload, or one the store deterministically rejects,
  is written to `warehouse.network-inventory-planning.analytics.dlq` with the
  headers `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` and committed
  past. A database outage is retried, never dead-lettered. A failed DLQ write
  is retried as transient.

## Outbox relay

`postgres.OutboxRelay` (`internal/adapters/outbound/postgres/outbox.go`) runs in
the API process when `KAFKA_BROKERS` and `OUTBOX_RELAY_ENABLED` are set:

- every 1s it claims up to 100 unpublished rows `ORDER BY id` with
  `FOR UPDATE SKIP LOCKED` and sends them **one at a time**;
- a sent row gets `published_at = now()`;
- a failed send increments `attempts`, stores `last_error`, commits, and ends
  the pass, so no later row overtakes it; the next tick retries it. There is
  no attempt limit in production (`maxAttempts` 0).

Without the relay, approvals and fact applications still commit and their
rows wait in `outbox_events`.

## Tickers (housekeeping)

Both run inside every API replica, only when an outbox publisher exists.

| Ticker | Variable | Default | What a tick does |
| --- | --- | --- | --- |
| Saga health check | `NIP_HEALTH_CHECK_INTERVAL`, `NIP_STUCK_THRESHOLDS` | every 5m; `ALLOCATING` 1h, `PICKED` 24h, `IN_TRANSIT` 72h, others 24h | Reads up to 500 non-terminal transfers and writes one `TransferStuckDetected` outbox row per transfer older than its state's threshold. It never changes a transfer. |
| Scheduled rebalance | `NIP_REBALANCE_SCHEDULE` | off | Builds the fail-closed snapshot, runs the planner, inserts a `rebalance_runs` row (`COMPLETED` or `FAILED` with `fail_closed_reason`) and writes a `RebalanceRunCompleted` row. It never approves anything. In this version the planner is fed no positions, policies or lanes, so every completed run records `proposal_count = 0`. |

There is no sweeper: `processed_events`, `outbox_events` (published rows),
`transfer_audit` and `rebalance_runs` grow without bound. Pruning them is a
manual operation.

## Analytics reports

`nip-reports` serves these routes (all `GET`, JSON, also in the
[API reference](../api-reference/overview.md)):

| Route | Query | Response |
| --- | --- | --- |
| `/reports/transfer-funnel` | `from`, `to` (RFC 3339, optional) | `{from, to, days:[{day, state, transfers}]}` |
| `/reports/state-dwell` | `from`, `to` | `{from, to, days:[{day, state, transitions, p50_age_seconds, p95_age_seconds}]}` |
| `/reports/stuck-transfers` | `from`, `to`, `limit` (1–100, default 20) | `{from, to, days:[{day, state, detections, transfers}], latest:[{transfer_id, state, age_seconds, threshold_seconds, detected_at}]}` |
| `/reports/rebalance-runs` | `from`, `to` | `{from, to, days:[{day, runs, proposals, rejected, rejection_rate, stale_facts}]}` |
| `/reports/freshness` | none | `{as_of, lag_seconds}` (both `null` until the first event is applied) |
| `/healthz` | none | `{"status":"ok"}` |

Ranges are half-open `[from, to)`, default to the 30 days ending now and may
not exceed 366 days; a bad range is a 400 `invalid-report-range`, a bad limit a
400 `invalid-report-limit`, a database failure a 500 `report-store-error`
(cause not echoed). Days are UTC calendar dates.

## Scaling

- **api**: `autoscaling` (HPA, 1–3 replicas, 70% CPU) is off by default. Extra
  replicas share each consumer group's partitions safely (dedupe by
  `processed_events`). Two things multiply with replicas: both tickers (each
  replica runs its own health check and rebalance, producing duplicate
  occurrences) and outbox relays (`SKIP LOCKED` gives each relay a disjoint
  batch, so strict id order across all rows only holds with one relay).
- **pgxpool**: OLTP `MaxConns` 10 per process; analytical writer and reader 5
  each. Budget `replicas × 10` (API) + `replicas × 10` (MCP) against Postgres.
- **analytics-projector**: HPA capped at 2; writes are idempotent on the
  CloudEvents id. **analytics-reports**: stateless, HPA up to 3.

## Routine procedures

| Task | Procedure |
| --- | --- |
| Re-publish an event | `UPDATE outbox_events SET published_at = NULL, attempts = 0, last_error = NULL WHERE id = <id>;` The relay resends the stored bytes with the same CloudEvents `id`, so consumers deduplicate it. |
| Unblock a relay stuck on one row | `SELECT id, topic, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` Fix the cause (topic, broker, ACLs). Every row behind the failing one waits. |
| Replay a read-model topic | Reset the group's offsets with the Kafka tools while the API is scaled to 0. Events already in `processed_events` for that consumer are skipped; delete those rows first if they must be re-applied. |
| Rebuild the analytics model | The projection can be dropped and rebuilt from the topic: point `ANALYTICS_CONSUMER_GROUP` at a new group id (it starts from the earliest offset) on an emptied analytical database, or reset the existing group's offsets. |
| Drain the analytics DLQ | Read `warehouse.network-inventory-planning.analytics.dlq`, inspect `x-dlq-error`, fix the producer or projection, and re-produce the raw value to the analytics topic. |
| Rotate database credentials | Update the Secret (`database.existingSecret` / `analytics.database.existingSecret`) and restart the Deployments: DSNs are read only at start. |
| Enable approval in a new environment | Set `kafka.enabled`, the five group ids, `config.transferPickPathId` and `config.transferDispatchPathId`; keep `config.outboxRelayEnabled: "true"`. |
| Investigate a stuck transfer | `GET /v1/transfers/{id}` (audit trail), the MCP tool `find_stuck_transfers`, or `GET /reports/stuck-transfers`. See [Troubleshooting](./troubleshooting.md). |
