# ADR-0006: Deployable image and Helm chart

Status: Accepted

## Context

Through ADR-0005 the service had Kafka consumers, an outbox relay and a
transfer saga, but no Dockerfile, no chart and no entry in warehouse-infra.
Its sibling contexts already consume and publish its events
(`warehouse.network-inventory-planning.events`), so the saga could not run
end to end: nothing produced on the topic they read.

## Decision

- Ship a `Dockerfile` that builds every `cmd/*` binary (so a later `cmd/mcp`
  or projector cannot be merged without being in the image) and a Helm chart
  in `charts/network-inventory-planning/`, the same shape as the fleet's other
  charts. warehouse-infra owns the environment overlay; this repo owns the
  shape of its workload.
- The chart renders only the api component. MCP, frontend and analytics
  components are added by their own PRs.
- Every Kafka-dependent feature stays opt-in in the chart exactly as in the
  binary: an empty consumer-group id renders nothing, `outbox.relayEnabled`
  requires `kafka.enabled` (render-time check), and `transfer.pickPathId` has
  no default (ADR-0005: approval answers 503 until it is set).
- The binary reads `MIGRATIONS_DATABASE_URL` (falling back to `DATABASE_URL`)
  for the golang-migrate step only. golang-migrate takes a session-scoped
  `pg_advisory_lock` that PgBouncer's transaction pooling cannot honour; the
  runtime pgxpool keeps using `DATABASE_URL`.
- Probes use `GET /healthz`, the only health route the binary serves.

## Consequences

- The service can be deployed; the transfer saga can run once the rest of its
  edges exist.
- `TRANSFER_DISPATCH_PATH_ID` must name a path in WES's PathCatalogue or the
  dispatch leg stays fail-closed. That path is owned by
  process-path-management and is configured per environment, not defaulted here.
- No `/readyz`: readiness equals liveness until the binary grows a real
  readiness probe.
