---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in the network-inventory-planning code, plus every problem type the APIs return.
---

# Troubleshooting

## Symptom → cause → check → fix

| Symptom | Likely cause | Check | Fix |
| --- | --- | --- | --- |
| API pod never becomes ready; startup probe fails after 60s | A migration failed or Postgres is unreachable: the server only listens after `RunMigrations` and the ping succeed. | Pod logs: `run migrations: ...`, `open postgres pool: ...` or `ping postgres: ...`; the process exits. | Fix the DSN or the migration. Behind PgBouncer set `MIGRATIONS_DATABASE_URL` to a direct DSN (ADR 0006). |
| API exits at boot with `PLANNING_MAX_STALENESS must be a positive duration` | Unparsable or non-positive value. | `kubectl get deploy -o yaml` env. | Use a Go duration such as `10m`. |
| `nip-projector` CrashLoopBackOff with `ANALYTICS_DATABASE_URL is required` / `KAFKA_BROKERS is required` | Required variable missing. | Pod env / the analytics Secret. | Set `analytics.database.*` and `kafka.enabled`. |
| `nip-projector` or `nip-reports` exits after about 31s at boot | The database is genuinely unreachable (boot retry exhausted: 5 attempts, 1s doubling). | Log `... (after 5 attempts): ...`. | Fix the DSN or network; a single early reset is retried automatically. |
| `GET /v1/transfer-simulations` → 503 `read-models-unavailable` | The API runs without `DATABASE_URL`. | Startup log `DATABASE_URL unset`. | Configure the database. |
| `GET /v1/transfer-simulations` → 503 `read-models-incomplete` | `BuildSnapshot` refused: an empty table, a fact older than `PLANNING_MAX_STALENESS`, no site with all three facts, or a participating site disabled for a direction. | The problem `detail` names the reason (for example `stale site capability for WH1`). `SELECT site_id, as_of FROM site_capability;` and the same for `site_sku_demand` / `published_capacity_plan`. | Check the three consumers run (their group ids are set) and the upstream contexts publish; a disabled site must be re-enabled upstream (any disabled participating site refuses the whole snapshot). |
| Read models never change | The consumer's group variable is empty, `KAFKA_BROKERS` is unset, or the consumer is blocked on one message. | Boot log `consumer disabled name=...`; ERROR lines `... handling failed; retrying the same message` with `partition`/`offset`; consumer lag. | Set the group id; fix the transient cause (usually the database). Deterministic problems are skipped at WARN, so a blocked partition always has an ERROR line. |
| `POST /v1/transfers:approve` → 503 `saga-unavailable` | No `DATABASE_URL`, or no `KAFKA_BROKERS` (no outbox writer). | Boot WARN `KAFKA_BROKERS not configured`. | Configure both. |
| Approval → 503 `config-incomplete` | `TRANSFER_PICK_PATH_ID` unset or the pick CPT offset not positive (ADR 0005). | Boot WARN `work release not configured`. | Set `config.transferPickPathId`. |
| Approval → 422 `facts-incomplete` | The current snapshot refuses (stale or missing facts), a site is missing or not enabled for its direction, the destination shows no in-window demand for the SKU, or the origin capacity cannot cover its own demand plus the quantity. | `detail` names the rule. | Refresh the facts or pick another pair; the approval re-validates against current facts by design. |
| Approval → 422 `invalid-approval` | A missing field, non-positive quantity, origin = destination, missing `proposalAsOf` or empty policy version. | `detail`. | Fix the request. |
| Approval → 422 `proposal-expired` | The 24h expiry computed at approval is not after "now" (clock problem). | Server clock. | — |
| Approval → 409 `idempotency-conflict` | The `Idempotency-Key` was used before with a different origin, destination, SKU, quantity, policy version or `proposalAsOf`. | `SELECT transfer_id, idempotency_key FROM inter_warehouse_transfer WHERE idempotency_key = '<key>';` | Send a fresh key per logical approval. A replay with the same body (the operator reason is not compared) answers 200 `replayed: true`. |
| Approval → 400 `idempotency-key-required` | Header missing. | — | Send `Idempotency-Key`. |
| Approval → 503 `approval-unavailable` | Any other failure: reading the read models or writing the transaction. | API logs, Postgres health. | Retry with the same key once the database is healthy. |
| Transfer stays `ALLOCATING` | inventory-storage never replied, the relay is off, or `TRANSFER_REPLY_CONSUMER_GROUP` is empty. | `outbox_events` row for `TransferAllocationRequested` has `published_at`? Reply consumer running? `find_stuck_transfers` with `state=ALLOCATING`. | Enable the relay / consumer; check inventory-storage's allocation consumer. The health check flags it after 1h. |
| Transfer `UNFULFILLABLE` | inventory-storage rejected: `ORIGIN_SITE_UNKNOWN`, `INSUFFICIENT_USABLE` or `IDEMPOTENCY_CONFLICT`. | `rejectionReason` on `GET /v1/transfers/{id}`. | Terminal; approve a new transfer if appropriate. |
| Transfer stays `ALLOCATED` | No `TransferPicked` from fulfillment-execution: the pick `WorkDemandReleased` was not consumed by wes-work-planning, or `TRANSFER_FACT_CONSUMER_GROUP` is empty. | `outbox_events` for `WorkDemandReleased` (key `<transferId>:pick`); fact consumer boot log. | Check WES knows `TRANSFER_PICK_PATH_ID`; enable the fact consumer. |
| Transfer stays `PICKED`, no dispatch work | `TRANSFER_DISPATCH_PATH_ID` (or its offset) is not configured: the PICKED transition commits, the dispatch demand is withheld and the fact is skipped. | WARN on the fact consumer; no `<transferId>:dispatch` row in `outbox_events`. | Configure the dispatch path. There is no automatic re-release; the missing demand has to be re-issued by hand (re-publishing is not possible because the row was never written). |
| Transfer stays `IN_TRANSIT` | Neither `TransferArrived` nor inventory-storage's `TransferReceiptStaged` arrived. | Reply and fact consumers' WARN/ERROR lines. | Check the destination receiving flow. |
| A fact is ignored | It was a replay, out of order, for an unknown `transfer_ref`, or failed the aggregate's checks (wrong line, site, SKU, quantity). | WARN `skipping deterministic ...` with `transfer_id`. | Expected for replays; otherwise fix the producer's payload. |
| `warehouse.inventory.events` partition stuck behind one `TransferStockAllocated` / `TransferStockAllocationRejected` | The reply names a known transfer in `ALLOCATING` but does not match it (other line id, origin, SKU, allocation total, no reservation id or expiry). `MarkAllocated` / `MarkUnfulfillable` return a plain error that `usecases.IsDeterministic` does not recognise, so the consumer treats it as transient. | ERROR `transfer reply consumer handling failed; retrying the same message` with the mismatch in `error`. | Fix the producer's payload; to unblock, move the group's offset past the message. Known gap in the code. |
| `outbox_events` backlog grows | Relay disabled (`OUTBOX_RELAY_ENABLED` empty), broker unreachable, or one row failing (head-of-line). | `SELECT id, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` | Fix the cause of `last_error`; the next 1s tick retries. |
| Analytics DLQ grows | A saga-health event with an unusable payload, or one the projection rejects. | Message headers `x-dlq-error`, `x-dlq-source-topic`. | Fix the producer or projection, then re-produce the message. |
| Reports are stale | Projector down, lagging or blocked on a transient database error. | `GET /reports/freshness` `lag_seconds`; projector `/readyz`; consumer lag of `network-inventory-planning-analytics`. | Restore the analytical database; the projector resumes from its committed offset. |
| Funnel report never shows `IN_TRANSIT`, `ARRIVED`, `RECEIVED` or `UNFULFILLABLE` | The use cases for those transitions are wired without an event publisher in `cmd/network-inventory-planning/main.go`, so no `TransferStateAdvanced` is emitted for them. | `SELECT to_state, count(*) FROM transfer_state_advances GROUP BY 1;` | Known gap in the code; use `GET /v1/transfers` for those states. |
| `GET /v1/rebalance-runs` is empty | `NIP_REBALANCE_SCHEDULE` is unset (the default) or there is no outbox. | Boot log `scheduled rebalance disabled`. | Set the schedule. Completed runs record zero proposals in this version (the planner gets no policies or lanes). |
| `TransferStuckDetected` repeats for the same transfer | Expected: every tick re-reports every transfer still over its threshold, and every API replica runs its own ticker. | — | Deduplicate downstream by `transfer_id`. |
| Health check never fires | `NIP_HEALTH_CHECK_INTERVAL=0`, an invalid `NIP_STUCK_THRESHOLDS`, or no outbox. | Boot log `saga health check disabled` / `invalid stuck thresholds`. | Fix the value; an unknown state name disables the whole check. |
| MCP tool returns `read-side-unavailable` | `cmd/mcp` runs without `DATABASE_URL`. | MCP boot log. | Configure the database. |

There is no circuit breaker and no `412` anywhere in this service: NIP makes no
outbound HTTP calls and uses no ETags.

## Problem types

Every REST error is `application/problem+json` with
`type = https://warehouse.example/problems/<slug>`
(`internal/adapters/inbound/http/handler.go`, `transfers_read.go`,
`reports_handler.go`).

| Slug | Status | Route(s) |
| --- | --- | --- |
| `invalid-request` | 400 | `POST /v1/transfer-proposals:generate`, `POST /v1/transfers:approve` (malformed JSON or unknown field) |
| `invalid-planning-snapshot` | 422 | generate (missing `asOf`, mixed position snapshot) |
| `read-models-unavailable` | 503 | `GET /v1/transfer-simulations` |
| `read-models-incomplete` | 503 | `GET /v1/transfer-simulations` |
| `saga-unavailable` | 503 | approve |
| `idempotency-key-required` | 400 | approve |
| `idempotency-conflict` | 409 | approve |
| `config-incomplete` | 503 | approve |
| `invalid-approval` | 422 | approve |
| `facts-incomplete` | 422 | approve |
| `proposal-expired` | 422 | approve |
| `approval-unavailable` | 503 | approve |
| `rebalance-runs-unavailable` | 503 | `GET /v1/rebalance-runs` |
| `invalid-limit` | 400 | `GET /v1/rebalance-runs` (`limit` outside 1–200) |
| `read-side-unavailable` | 503 | `GET /v1/transfers`, `GET /v1/transfers/{id}` |
| `invalid-query` | 400 | `GET /v1/transfers` (unknown state, bad `limit`/`offset`) |
| `transfer-not-found` | 404 | `GET /v1/transfers/{id}` |
| `invalid-report-range` | 400 | `nip-reports` `/reports/*` |
| `invalid-report-limit` | 400 | `/reports/stuck-transfers` |
| `report-store-error` | 500 | `nip-reports` `/reports/*` |

MCP tools report the same slugs as the prefix of an `isError` result
(`invalid-query: ...`, `transfer-not-found: ...`, `read-side-unavailable: ...`,
`read-models-unavailable: ...`, `read-models-incomplete: ...`), and
`internal-error` for anything untyped.
