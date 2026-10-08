# ADR-0011: Metrics (OpenTelemetry HTTP RED + three business counters)

Status: Accepted

## Context

ADR-0007 gave this context trace *propagation* only: it set the global W3C
propagator and nothing else. No `MeterProvider` was installed, no instrument was
registered and no HTTP server middleware existed, so none of the four
deployables (`network-inventory-planning`, `-mcp`, `-reports`, `-projector`)
reported a single series. The fleet's per-context Grafana dashboards
(`warehouse-infra` `scripts/gen-context-dashboards.py`) are built from
`http_server_request_duration_seconds_*`, one Tier-2 counter row and the Go
runtime panels, so this context's dashboard would have been empty.

The fleet standard-metrics convention (order-management ADR-0009,
fulfillment-execution ADR-0019) defines the baseline. `inventory-storage` and
`warehouse-planning` are the reference implementations; this ADR mirrors them.

## Decision

### Tier 1 (identical across the fleet)

- `internal/adapters/outbound/telemetry` is copied from `warehouse-planning`:
  `Setup(ctx, serviceName, version, endpoint)` installs a `TracerProvider` and a
  `MeterProvider` (both OTLP/gRPC, non-blocking, 30 s metric export), the W3C
  propagator, and Go runtime metrics, and returns a graceful-shutdown func.
  `SetupFromEnv` reads `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`)
  and `SERVICE_VERSION`. A setup error is logged and the process continues with
  a no-op shutdown: **telemetry never blocks boot**, and an unreachable
  Collector degrades to dropped telemetry.
- Service names (the Prometheus `service_name` label): `network-inventory-planning`
  (API, `cmd/network-inventory-planning`), `network-inventory-planning-mcp`
  (`cmd/mcp`), `network-inventory-planning-reports` (`cmd/nip-reports`),
  `network-inventory-planning-projector` (`cmd/nip-projector`).
- HTTP RED: the siblings use chi + `otelchi`; this service uses the stdlib
  `http.ServeMux`, so `telemetry.HTTPMiddleware` wraps the mux with
  `otelhttp.NewHandler`. It emits the same `http.server.request.duration`
  histogram (Prometheus `http_server_request_duration_seconds_*`) with an
  `http.route` label. **The route label is the mux pattern**
  (`/v1/transfers/{id}`), read from `http.Request.Pattern`, never the raw path;
  an unmatched request (404) carries no `http.route` at all, so cardinality is
  bounded by the number of registered routes. It wraps the API, MCP and reports
  routers in their composition roots (the inbound adapters may not import an
  outbound adapter) and changes no response. The projector is admin-only
  (`/healthz`, `/readyz`): it gets `Setup` (runtime metrics, so its
  `service_name` appears) and deliberately no HTTP middleware.
- `Setup` runs before the router is built because the middleware binds to the
  global `MeterProvider` at construction.

### Tier 2: three business counters

Port `ports.TransferMetrics` (application layer, nil-able at every call site);
adapter `telemetry.TransferMetrics` (nil-receiver safe). Names follow
`<context>.<aggregate>.<verb>` with `outcome`/`to` attributes; Prometheus names
follow the generator's rule (dots to underscores, `_total` on counters).

| OTel instrument | Prometheus series | Attribute | Values |
|---|---|---|---|
| `network_inventory_planning.transfers.approved` | `network_inventory_planning_transfers_approved_total` | `outcome` | `approved`, `replayed`, `refused` |
| `network_inventory_planning.transfers.state_advanced` | `network_inventory_planning_transfers_state_advanced_total` | `to` | the `TransferState` enum (`DRAFT`, `PROPOSED`, `APPROVED`, `ALLOCATING`, `ALLOCATED`, `PICKED`, `IN_TRANSIT`, `ARRIVED`, `RECEIVED`, `UNFULFILLABLE`, `CANCELLED`) |
| `network_inventory_planning.outbox.relayed` | `network_inventory_planning_outbox_relayed_total` | `outcome` | `published`, `failed` |

- **approved** is recorded by `ApproveTransfer.Execute` (optional `Metrics`
  field): `approved` = a new transfer was created, `replayed` = an existing
  Idempotency-Key with the same payload, `refused` = an error of any class
  (invalid input, stale facts, expired proposal, idempotency conflict,
  work-release not configured, dependency failure). The outcome comes from the
  result/error *class*, never from the error text or ids.
- **state_advanced** is recorded by a `TransferRepository` decorator
  (`NewMeteredTransferRepository`): one increment per audit entry a successful
  `Create` (the four creation transitions) or `UpdateState` (the entries past
  the aggregate's version) persists; an idempotent replay persists nothing and
  counts nothing. The repository is the choke point of *every* transition, which
  matters because the analytics-event hook is not: `ApplyTransferDispatched`,
  `ApplyTransferArrival`, `ApplyTransferReceiptStaged` and `ApplyTransferStow`
  carry an unexported `events` field that no composition root can set, and
  `ApplyTransferRejection` has none, so `TransferStateAdvanced` occurrences for
  `IN_TRANSIT`, `ARRIVED`, `RECEIVED` and `UNFULFILLABLE` are not emitted in
  production today (a separate gap, not fixed here). Counting at the repository
  keeps this metric truthful regardless.
- **relayed** is recorded by a relay-sink decorator (`NewMeteredRelaySink`): one
  increment per send the outbox relay attempts. A failed row is retried on the
  next pass, so `failed` counts attempts, not distinct rows.
- Unknown values collapse to `other` inside the adapter, so a programming slip
  cannot mint a new series. **No ids, SKUs, site ids or payloads are ever labels
  or log fields.**
- Accepted imprecision: `state_advanced` and `approved` are recorded inside the
  caller's unit of work, just before commit. A later rollback (e.g. the outbox
  insert failing after the state write) over-counts that attempt. These are
  health signals, not ledgers; the `transfer_audit` table is the ledger.

### Dashboard entry (for `warehouse-infra`)

Append this to `CONTEXTS` in `scripts/gen-context-dashboards.py` and re-run the
generator (the Prometheus names are derived by the rule in that file's
docstring, not yet observed live):

```python
    {
        "key": "network-inventory-planning",
        "title": "Network Inventory Planning",
        "uid": "warehouse-network-inventory-planning",
        # OTel service.name: network-inventory-planning (api),
        # -mcp, -reports, -projector: one prefix regex covers every process.
        "service_regex": "network-inventory-planning.*",
        "kong_service_regex": "httproute\\.warehouse-systems\\.network-inventory-planning\\..*",
        "loki_app": "network-inventory-planning",
        "business_metrics": [
            (
                "network_inventory_planning_transfers_approved_total",
                "outcome",
                "POST /v1/transfers:approve outcomes: approved (new transfer), replayed (idempotent re-send) or refused (any error). A rising refused share means operators approve proposals the fail-closed facts or inputs reject.",
            ),
            (
                "network_inventory_planning_transfers_state_advanced_total",
                "to",
                "Saga transitions by the state entered. The funnel shape: a state that keeps being entered while its successor does not is where transfers are stalling.",
            ),
            (
                "network_inventory_planning_outbox_relayed_total",
                "outcome",
                "Outbox rows the relay tried to send to Kafka, by outcome (published or failed). Failed with no matching published means the broker is down and the saga is not progressing.",
            ),
        ],
    },
```

## Consequences

- The context's dashboard has data on first deploy: HTTP RED per route, three
  business counters and Go runtime panels, for all four processes.
- `go.mod` gains only the OTel packages the siblings already use, at the same
  versions (`otel`/`sdk`/`metric`/OTLP exporters v1.46.0, `contrib/.../runtime`
  v0.71.0, `otelhttp` v0.70.0); the OTel line moved from v1.44.0.
- Deployments must provide `OTEL_EXPORTER_OTLP_ENDPOINT` (the fleet Collector)
  for export to happen; without it `Setup` targets `localhost:4317` and drops
  silently. As in the sibling charts (none sets it), this comes from the
  warehouse-infra values override, not from this chart.
- The MCP server's long-lived streams are measured by `otelhttp` as ordinary
  requests (duration is stream lifetime for those); acceptable, same as the
  siblings.
