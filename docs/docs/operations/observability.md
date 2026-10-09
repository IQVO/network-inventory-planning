---
id: observability
title: Observability
sidebar_label: Observability
description: Metrics, traces, logs and dashboards of network-inventory-planning, and the alerts worth having.
---

# Observability

## What exports telemetry

Only the API binary (`cmd/network-inventory-planning`) sets up OpenTelemetry
(`telemetry.Setup`, ADR 0012). It **pushes** traces and metrics over
OTLP/gRPC (insecure) to `OTEL_EXPORTER_OTLP_ENDPOINT` (default
`localhost:4317`; the chart sets
`otel-collector.observability.svc.cluster.local:4317`). There is no
`/metrics` scrape endpoint. Metrics are exported every 30s. Export never
blocks: with no Collector listening, telemetry is dropped and the service runs
normally.

`mcp`, `nip-projector` and `nip-reports` do not initialise OpenTelemetry: they
emit structured logs only.

Resource attributes on every span and metric (`internal/adapters/outbound/telemetry/telemetry.go`):

| Attribute | Value |
| --- | --- |
| `service.name` | `network-inventory-planning` |
| `service.version` | `SERVICE_VERSION` (default `dev`; the chart sets the image tag) |
| `deployment.environment.name` | `ENVIRONMENT` (default `local`) |

## Metrics

NIP defines **no custom instruments**. Everything comes from two libraries,
checked in the module cache at the versions pinned in `go.mod`:

### HTTP server (`otelhttp` v0.70.0, wrapping `Handler.Routes`)

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `http.server.request.duration` | histogram | `s` | `http.request.method`, `http.route` (the ServeMux pattern, e.g. `/v1/transfers/{id}`), `http.response.status_code`, `network.protocol.name`, `network.protocol.version`, `server.address`, `url.scheme` |
| `http.server.request.body.size` | histogram | `By` | same |
| `http.server.response.body.size` | histogram | `By` | same |

`internal/adapters/inbound/http/otel_metrics_test.go` pins that
`http.server.request.duration` carries `http.route`. In Prometheus these appear
as `http_server_request_duration_seconds_*` with `service_name` and
`http_route` labels.

### Go runtime (`contrib/instrumentation/runtime` v0.72.0, `runtime.Start`)

| Instrument | Type | Unit |
| --- | --- | --- |
| `go.memory.used` | up-down counter (attribute `go.memory.type`) | `By` |
| `go.memory.limit` | up-down counter | `By` |
| `go.memory.allocated` | counter | `By` |
| `go.memory.allocations` | counter | `{allocation}` |
| `go.memory.gc.goal` | up-down counter | `By` |
| `go.goroutine.count` | up-down counter | `{goroutine}` |
| `go.processor.limit` | up-down counter | `{thread}` |
| `go.config.gogc` | up-down counter | `%` |

### Signals that are not metrics

Saga health is published as **events**, not metrics, and reaches dashboards
through the analytics read side:

| Question | Where to look |
| --- | --- |
| How many transfers reached each state per day? | `GET /reports/transfer-funnel` |
| How long do transfers dwell in each state (p50/p95)? | `GET /reports/state-dwell` |
| Which transfers are stuck? | `GET /reports/stuck-transfers`, MCP `find_stuck_transfers` |
| Do scheduled rebalances fail closed? | `GET /reports/rebalance-runs`, `GET /v1/rebalance-runs` |
| Is the projection behind? | `GET /reports/freshness` (`lag_seconds`) |
| Is the outbox draining? | `SELECT count(*) FROM outbox_events WHERE published_at IS NULL;` |

## Traces

- **Server spans**: `otelhttp` starts one span per request, named
  `<METHOD> <route>` once the ServeMux pattern is known (for example
  `POST /v1/transfers:approve`).
- **Propagation through Kafka**: the global propagator is W3C Trace Context +
  Baggage. The encoders inject `traceparent`/`tracestate` from the active
  context into each outbox row's headers (`tracedHeaders`,
  `internal/adapters/outbound/kafka/publisher.go`), and the relay forwards
  them verbatim, so an approval's `TransferAllocationRequested` carries the
  trace of the HTTP request even though it is sent later. Every consumer
  extracts the headers into the handler context
  (`internal/adapters/inbound/kafka/kafka.go`), so events NIP writes while
  handling a consumed message carry the upstream trace.
- No spans are started for Kafka produce or consume, Postgres queries or the
  tickers; ticker-written rows carry no `traceparent`.

## Logs

The API logs with Go's default `slog` logger (text to stderr). `mcp`,
`nip-projector` and `nip-reports` log JSON to stdout at `LOG_LEVEL`.

| Message (substring) | Level | Fields | Meaning |
| --- | --- | --- | --- |
| `DATABASE_URL unset: running without read models` | INFO | `enable_with` | Diagnostic mode. |
| `KAFKA_BROKERS not configured; outbox disabled` | WARN | `enable_with` | Approval answers 503. |
| `work release not configured; POST /v1/transfers:approve answers 503` | WARN | `error`, `enable_with` | Pick path or offset missing. |
| `consumer running` / `consumer disabled` | INFO | `name`, `group_id`, `brokers` | One line per consumer at boot. |
| `outbox relay running` | INFO | `topic`, `brokers` | Relay started. |
| `saga health check running` / `disabled` | INFO / WARN / ERROR | `interval`, `thresholds_env`, `error` | Ticker state at boot. |
| `scheduled rebalance running` / `disabled` | INFO / WARN / ERROR | `interval`, `enable_with` | Ticker state at boot. |
| `<consumer> handling failed; retrying the same message` | ERROR | `error`, `partition`, `offset`, `attempt`, `retry_in` | A transient failure blocking a partition. |
| `skipping ...` (for example `skipping deterministic allocation-reply failure`) | WARN | `error`, `event_id`, `transfer_id` | A message committed past without effect. |
| `analytics: dead-lettering a message that can never be projected` | ERROR | `error`, `dlq_topic`, `partition`, `offset` | Projector DLQ write. |
| `analytics: skipping message that is not a CloudEvents 1.0 event` | WARN | `topic`, `partition`, `offset`, `suppressed_since_last_warning` | Sampled once per minute. |
| `retrying` / `succeeded after retry` | WARN / INFO | `op`, `attempt`, `in`, `err` | Boot retry of the analytics binaries. |
| `mcp tool failed with an unexpected error` | ERROR | `error` | An untyped error behind an `internal-error` tool result. |

## Dashboards

warehouse-infra provisions a Grafana dashboard for this context,
`terraform/dashboards/contexts/network-inventory-planning.json` ("Warehouse —
Network Inventory Planning"). Its panels: Kong request rate, 5xx rate and p95
latency for the context's HTTPRoute; request rate by route, 5xx rate and p95
duration from `http_server_request_duration_seconds{service_name=~"network-inventory-planning.*"}`;
goroutines (`go_goroutine_count`) and memory (`go_memory_used_bytes`); and Loki
log panels for `{app="network-inventory-planning"}` (live stream, volume by
level, errors and warnings). The "Service HTTP RED" row names the projector,
reports and MCP processes too, but only the API exports metrics today.

## Suggested alerts

| Alert | Expression / check | Why |
| --- | --- | --- |
| API 5xx ratio | `sum(rate(http_server_request_duration_seconds_count{service_name="network-inventory-planning",http_response_status_code=~"5.."}[5m])) / sum(rate(http_server_request_duration_seconds_count{service_name="network-inventory-planning"}[5m])) > 0.05` | 503s mean fail-closed read models or missing configuration. |
| Simulation refusing | 503 rate on `http_route="/v1/transfer-simulations"` above zero for 15m | Read models stale beyond `PLANNING_MAX_STALENESS`. |
| Outbox backlog | unpublished `outbox_events` older than 5 minutes | Relay off, broker down, or a head-of-line row failing. |
| Consumer lag | lag of the five NIP groups and `network-inventory-planning-analytics` (Kafka exporter) | A transient error retries the same message forever. |
| Analytics DLQ | any message on `warehouse.network-inventory-planning.analytics.dlq` | A poison saga-health event. |
| Projection freshness | `lag_seconds` from `/reports/freshness` above 15 minutes | Projector down or lagging. |
| Stuck transfers | new rows in `transfer_stuck_detections` (or `/reports/stuck-transfers`) | Allocation reply or a fulfillment fact never arrived. |
| Rebalance failing closed | consecutive `FAILED` rows in `rebalance_runs` | Chronically stale read models. |
