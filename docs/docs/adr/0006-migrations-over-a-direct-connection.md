# ADR-0006: Migrations over a direct connection; chart routing

Status: Accepted

## Context

The service is now packaged (Dockerfile, Helm chart) and deployed behind
PgBouncer in the shared cluster. Its `DATABASE_URL` points at the pooler, which
runs in transaction-pooling mode. golang-migrate takes a session-scoped
`pg_advisory_lock` to serialise concurrent migration runs at boot; a
transaction pooler cannot honour a session lock, so migrating through it fails
(the same fault found in the other fleet services under load).
warehouse-infra already writes a direct `MIGRATIONS_DATABASE_URL` into the
service's database Secret; the binary did not read it. The chart also had no
routing templates, so the Kong route warehouse-infra configures could not be
rendered.

## Decision

- The binary resolves the DSN for the golang-migrate step from
  `MIGRATIONS_DATABASE_URL`, falling back to `DATABASE_URL`. The runtime
  pgxpool keeps using `DATABASE_URL` only.
- The chart renders `MIGRATIONS_DATABASE_URL` from the same Secret
  (`database.migrationsExistingSecretKey`, `optional: true`, so a Secret without
  the key still starts the pod).
- The chart ships opt-in `HTTPRoute` (Gateway API) and `Ingress` templates, off
  by default, in the fleet's shape; warehouse-infra enables one and supplies the
  path.

## Consequences

- Migrations run over a session-mode connection wherever a pooler fronts the
  runtime connection; local runs without the variable behave as before.
- A deployment that forgets the direct DSN still boots (fallback) but will hit
  the pooler/advisory-lock fault under concurrent starts; the chart default and
  warehouse-infra both set it.
