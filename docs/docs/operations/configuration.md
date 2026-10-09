---
id: configuration
title: Configuration
sidebar_label: Configuration
description: Every environment variable each network-inventory-planning binary reads, with defaults, meaning and the chart value that sets it.
---

# Configuration

Every binary is configured only through environment variables. The tables
below list every variable read by `cmd/*` and `internal/*` (non-test code), one
table per binary. "Chart value" names the key in
`charts/network-inventory-planning/values.yaml` that renders the variable, when
the chart renders it at all.

## `network-inventory-planning` (API, consumers, relay, tickers)

Source: `cmd/network-inventory-planning/main.go`, `internal/adapters/outbound/telemetry/telemetry.go`.

| Variable | Default | Required? | Meaning | Chart value |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API. | `config.httpAddr` |
| `DATABASE_URL` | none | no | OLTP Postgres DSN. **Unset** = diagnostic mode: no migrations, no read models, no consumers; only `/healthz` and `POST /v1/transfer-proposals:generate` work and every other route answers 503. **Set** = migrate, open the pool (`MaxConns` 10, `statement_timeout` 5s) and ping, or refuse to boot. | `database.url` / `database.existingSecret` + `existingSecretKey` |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | Directory of the OLTP `*.sql` migrations. The image sets `migrations` (`/app/migrations`). | (image `ENV`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | DSN used **only** by the golang-migrate step: a direct (non-PgBouncer) connection, because migrate's session-scoped `pg_advisory_lock` does not survive transaction pooling (ADR 0006). The runtime pool never uses it. | `database.migrationsExistingSecretKey` (secret key, `optional: true`) |
| `KAFKA_BROKERS` | none | for approval and Kafka | Comma-separated broker list. Unset: no outbox writer (so `POST /v1/transfers:approve` answers 503 `saga-unavailable`), no consumers, no saga-health check, no scheduled rebalance. | `kafka.brokers` (rendered only when `kafka.enabled`) |
| `SITE_CAPABILITY_CONSUMER_GROUP` | none | no | Consumer group of the `site-capability` consumer on `warehouse.facility.events`. Empty = that consumer does not run. | `kafka.siteCapabilityConsumerGroup` |
| `SITE_SKU_DEMAND_CONSUMER_GROUP` | none | no | Group of the `site-sku-demand` consumer on `warehouse.order-management.events`. Empty = off. | `kafka.siteSkuDemandConsumerGroup` |
| `CAPACITY_PLAN_CONSUMER_GROUP` | none | no | Group of the `capacity-plan` consumer on `warehouse.warehouse-planning.events`. Empty = off. | `kafka.capacityPlanConsumerGroup` |
| `TRANSFER_REPLY_CONSUMER_GROUP` | none | no | Group of the `transfer-reply` consumer on `warehouse.inventory.events` (allocation replies and destination facts). Empty = off, and approved transfers never leave `ALLOCATING`. | `kafka.transferReplyConsumerGroup` |
| `TRANSFER_FACT_CONSUMER_GROUP` | none | no | Group of the `transfer-fact` consumer on `warehouse.fulfillment.events`. Empty = off, and allocated transfers never leave `ALLOCATED`. | `kafka.transferFactConsumerGroup` |
| `OUTBOX_RELAY_ENABLED` | empty (off) | no | Any non-empty value starts the outbox relay (needs `KAFKA_BROKERS`). Empty: events stay as rows in `outbox_events` until a relay runs. | `config.outboxRelayEnabled` (default `"true"`) |
| `PLANNING_MAX_STALENESS` | `10m` | no | Freshness budget of every fact in the fail-closed planning snapshot (simulation, approval, scheduled rebalance). Must be a positive Go duration; anything else **fails the boot**. | `config.maxStaleness` |
| `TRANSFER_PICK_PATH_ID` | none | for approval | Process path id carried by the pick-leg `WorkDemandReleased` (ADR 0005). Unset: approval answers 503 `config-incomplete`. | `config.transferPickPathId` |
| `TRANSFER_PICK_CPT_OFFSET` | `2h` | no | CPT horizon of the pick demand (release time + offset). An invalid or non-positive value is logged at WARN and the default is used. | `config.transferPickCptOffset` |
| `TRANSFER_DISPATCH_PATH_ID` | none | for the dispatch leg | Path id of the dispatch-leg demand. Unset: a `TransferPicked` fact still moves the transfer to `PICKED`, but no dispatch demand is released (logged, committed past). | `config.transferDispatchPathId` |
| `TRANSFER_DISPATCH_CPT_OFFSET` | `3h` | no | CPT horizon of the dispatch demand. Invalid → WARN and default. | `config.transferDispatchCptOffset` |
| `NIP_HEALTH_CHECK_INTERVAL` | `5m` | no | Tick of the saga-health (stuck transfer) check (ADR 0007). `0` disables it. An unparsable or negative value disables the check with an ERROR log (the process still starts). Also off when there is no outbox (no `KAFKA_BROKERS`). | not rendered; use `extraEnv` |
| `NIP_STUCK_THRESHOLDS` | `ALLOCATING=1h,PICKED=24h,IN_TRANSIT=72h` (every other non-terminal state 24h) | no | Comma-separated `STATE=DURATION` overrides merged over the defaults. An unknown state, bad duration or non-positive value disables the check with an ERROR log. | not rendered; use `extraEnv` |
| `NIP_REBALANCE_SCHEDULE` | none (off) | no | Interval of the observe-only scheduled rebalance. Unset or `0` = off; invalid = off with an ERROR log. Needs the outbox. | not rendered; use `extraEnv` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute of traces and metrics. | image tag or `Chart.AppVersion` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint (insecure) for traces and metrics (ADR 0012). Export never blocks startup. | `otel.endpoint` (rendered when `otel.enabled`, default `otel-collector.observability.svc.cluster.local:4317`) |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `environment` |

The API process logs with Go's default `slog` logger; it does not read
`LOG_LEVEL`.

## `mcp`

Source: `cmd/mcp/main.go`.

| Variable | Default | Required? | Meaning | Chart value |
| --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address (Streamable HTTP at `/` and `/mcp`, plus `GET /healthz`). | `mcp.httpAddr` |
| `DATABASE_URL` | none | no | OLTP DSN. Unset: the server boots but every tool returns an `isError` result (`read-side-unavailable` or `read-models-unavailable`). Set: migrate, connect, ping or exit 1. | same secret as the API |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | OLTP migrations directory. | `mcp.migrationsPath` (`migrations`) |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step only. | `database.migrationsExistingSecretKey` |
| `PLANNING_MAX_STALENESS` | `10m` | no | Freshness budget used by `simulate_transfer_options`. Invalid → exit 1. | `config.maxStaleness` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning`, `error` (JSON logs on stdout). | `mcp.logLevel` |

## `nip-projector`

Source: `cmd/nip-projector/main.go` (`loadConfig`).

| Variable | Default | Required? | Meaning | Chart value |
| --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | Read-write DSN of the analytical database (direct Postgres: it also runs the analytical migrations). Missing → exit 1. | `analytics.database.projectorUrl` or `analytics.database.existingSecret` |
| `KAFKA_BROKERS` | none | **yes** | Brokers; the projector consumes `warehouse.network-inventory-planning.analytics`. Missing → exit 1. | `kafka.brokers` |
| `ANALYTICS_CONSUMER_GROUP` | `network-inventory-planning-analytics` | no | The projector's fixed consumer group. A new group id starts from the earliest offset and rebuilds the model. | `analytics.projector.consumerGroup` |
| `ANALYTICS_MIGRATIONS_PATH` | `analytics/migrations` | no | Analytical migrations directory (relative to `/app` in the image). | `analytics.migrationsPath` |
| `ADMIN_ADDR` | `:8091` | no | Admin server (`/healthz`, `/readyz`). | `analytics.projector.adminAddr` |
| `LOG_LEVEL` | `info` | no | Log level of the JSON logger. | `analytics.logLevel` |

## `nip-reports`

Source: `cmd/nip-reports/main.go` (`loadConfig`).

| Variable | Default | Required? | Meaning | Chart value |
| --- | --- | --- | --- | --- |
| `ANALYTICS_READER_DATABASE_URL` | none | one of the two | Reader DSN, ideally a read-only role. Wins over `ANALYTICS_DATABASE_URL`. | `analytics.database.reportsUrl` (falls back to `projectorUrl`) or the existing secret |
| `ANALYTICS_DATABASE_URL` | none | one of the two | Fallback DSN for single-role local runs. With neither set the process exits 1. | — |
| `HTTP_ADDR` | `:8092` | no | Listen address of the reports server. | `analytics.reports.httpAddr` |
| `LOG_LEVEL` | `info` | no | Log level. | `analytics.logLevel` |

The reports pool opens every connection with
`default_transaction_read_only = on` regardless of the role.

## Chart switches that are not environment variables

| Value | Default | Effect |
| --- | --- | --- |
| `kafka.enabled` | `false` | Renders `KAFKA_BROKERS` and the five group ids into the API Deployment. |
| `mcp.enabled` | `false` | Renders the `mcp` Deployment and Service. |
| `analytics.enabled` | `false` | Renders the `analytics-projector` and `analytics-reports` Deployments (and the reports Service); needs `kafka.enabled` and a DSN source. |
| `frontend.enabled` | `false` | Renders the `nip_mfe` console remote (nginx) Deployment and Service. |
| `autoscaling.enabled`, `analytics.projector.autoscaling.enabled`, `analytics.reports.autoscaling.enabled` | `false` | HPAs (max 3, 2 and 3 replicas). |
| `ingress.enabled` / `gatewayApi.enabled` | `false` | Edge routing of the REST API, switched on by warehouse-infra. |
| `otel.enabled` | `true` | Renders `OTEL_EXPORTER_OTLP_ENDPOINT`. |

See the [Runbook](./runbook.md) for what each Deployment runs and how it is
probed.
