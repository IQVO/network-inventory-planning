# ADR-0011: godog/Gherkin acceptance tests as executable specification

Status: Accepted

## Context

By this point the service already had a deep test suite: table-driven unit
tests per aggregate and per invariant (`internal/domain`), use-case tests
against fakes (`internal/application/usecases`), `httptest` tests per handler,
Postgres/Kafka integration tests per adapter, a `gremlins` mutation pass over
the domain and an architecture fitness test. The CI workflow and the Makefile
even carried a `bdd` job and a `make bdd` target (`go test ./... -run
TestFeatures -v`) — but the repository held **no features and no godog
dependency**, so the job ran `go test` against zero matching tests and passed
vacuously.

What was missing was a **statement of behaviour in the domain's own
vocabulary**. `TestApproveEndpointConflictYields409` is precise, but it does
not say *why* reusing an idempotency key for a different request is refused,
and it cannot be read by anyone who does not read Go. That matters here
because this service's correctness is defined by rules that are easy to get
subtly wrong and are mostly about **refusing**: the planner fails closed on an
empty or stale read model (a 503 from the simulation is the *correct* answer),
an approval is validated against the *current* facts, a transfer moves only
along `DRAFT → PROPOSED → APPROVED → ALLOCATING → ALLOCATED → PICKED →
IN_TRANSIT → ARRIVED → RECEIVED` (or to `UNFULFILLABLE`), and the analytics
reports are derived only from the analytics stream.

There was also a structural gap: the existing tests each verify one layer.
Nothing drove the *whole* stack — router, DTOs, error mapping, use cases,
aggregates, the CloudEvents encoders, the analytics consumer and the report
handlers — as a black box the way a real client does.

## Decision

**We write executable specifications in Gherkin under `features/`, run them
with [godog](https://github.com/cucumber/godog) v0.16.0 (the official Cucumber
implementation for Go) in `Strict` mode, and gate them in CI as the existing
`bdd` job.** This mirrors `inventory-storage` ADR 0007.

1. **One feature file per concern**, in the ubiquitous language of
   `.claude/rules/domain-model.md`. Each file starts with a `# Derived from:`
   comment citing the `apis/openapi.yaml` operations and the domain rules it
   specifies, and is tagged `@bdd`:

   | File | Covers |
   | --- | --- |
   | `features/health.feature` | `GET /healthz` |
   | `features/proposal_generation.feature` | `POST /v1/transfer-proposals:generate` — quantity bounds, lane rules, score ordering, determinism, advisory-only, 400/422 |
   | `features/simulation.feature` | `GET /v1/transfer-simulations` — headroom view, fail-closed 503 on empty/stale/disabled facts |
   | `features/transfer_approval.feature` | `POST /v1/transfers:approve` — saga start, idempotency (replay / 409 / required header), 400/422/503, CloudEvents on the wire |
   | `features/transfer_lifecycle.feature` | the saga state machine driven by inventory-storage and fulfillment-execution facts: legal steps, illegal transitions refused, short picks, unknown transfers, audit trail |
   | `features/transfer_queries.feature` | `GET /v1/transfers`, `GET /v1/transfers/{id}` — ordering, filters, paging, 404/400/503 |
   | `features/rebalance_runs.feature` | `GET /v1/rebalance-runs` — observe-only COMPLETED/FAILED runs, newest first, limit |
   | `features/analytics_reports.feature` | `GET /reports/{transfer-funnel,state-dwell,stuck-transfers,rebalance-runs,freshness}` — the publish → project → report path, range rules, idempotent projection |

2. **True black-box acceptance tests.** Step definitions live in
   `features_test.go` (suite, world, HTTP and JSON steps) and its siblings
   `features_facts_test.go`, `features_saga_test.go`, `features_reports_test.go`
   at the repo root (`package main_test`). They wire the **real HTTP routers**
   — `inboundhttp.Handler.Routes()` for the OLTP API and
   `ReportsServer.Routes()` for `nip-reports` — to in-memory outbound adapters,
   serve them over `httptest.NewServer`, and drive them with plain `net/http`
   requests. Every outcome is asserted from the response (or from a read-back
   through the same API), never from a repository.

3. **In-memory outbound adapters.** `internal/adapters/outbound/memory`
   implements the saga store (`TransferRepository`, `TransferQuery`,
   `StuckTransferReader`), the planning read models, the rebalance run history,
   a unit of work and a settable clock with the same contracts as the Postgres
   adapters (unique idempotency key, optimistic version, per-transfer audit
   sequence, newest-first ordering). Nothing in `cmd/` wires them. The existing
   `analyticsstore.Memory` serves the reports.

4. **Inbound Kafka is the one thing REST cannot reach, so it is simulated at
   the use-case seam.** inventory-storage replies and fulfillment-execution
   facts arrive over Kafka; `Given/When` steps hand them to the same
   `ApplyTransfer*` use cases the consumers call. Outbound events go through
   the **production** `TransferEncoder`/`AnalyticsEncoder`, so scenarios assert
   the exact CloudEvents 1.0 messages the relay would publish, and the
   **production** `AnalyticsConsumer` projects them into the store the reports
   read. Planning facts are declared locally in the scenario — the suite never
   calls a sibling context, which this service must not do either (fleet rule).

5. **Fresh state per scenario**, a frozen clock (`2026-10-06T15:00:00Z`) moved
   only by steps such as `11 minutes pass`, and idempotency keys as the
   scenario's names for transfers (`{T1}` expands to the minted id). No
   sleeps, no ordering coupling.

6. **Scenarios assert the specified behaviour.** Where the implementation
   diverges from `apis/openapi.yaml` or from the documented rules, the
   divergence is reported (and fixed when trivial and safe), never encoded as
   a passing scenario.

7. **Blocking in CI** as the `bdd` job, locally `make bdd`, and as part of
   `make check` through `go test ./... -race`.

## Consequences

### Easier

- The behaviour is readable by anyone who has not opened a Go file, in the
  vocabulary of the warehouse (*fail closed*, *replayed*, *UNFULFILLABLE*,
  *stuck*), and it fails the build when it stops being true.
- The full stack is covered end to end: status codes, `problem+json` bodies,
  DTO field names, the CloudEvents envelope and the report handlers — the
  layer of bugs unit tests structurally cannot see.
- Together with [ADR 0009](./0009-analytics-read-side.md) the publish →
  project → report path is exercised as one flow without Kafka or Postgres.

### Harder

- **A second vocabulary to maintain.** A change to a route or DTO now touches
  the handler *and* the step glue. The composition in `features_test.go`
  mirrors `cmd/network-inventory-planning/main.go` (package `main` cannot be
  imported); a new use case on `Handler` must be added to both.
- **Step reuse needs discipline** (the near-duplicate-step problem). New
  scenarios should reuse the generic `the JSON field "path" is …` steps and
  the transfer-fact steps before adding new ones.
- **Overlap with the `httptest` and use-case suites.** Some assertions now
  exist in two places. Accepted deliberately: those suites are exhaustive per
  unit, these are the readable specification of the important journeys.
- **The in-memory adapters are not the Postgres adapters.** They specify
  behaviour, not persistence; the integration tests keep covering SQL.
