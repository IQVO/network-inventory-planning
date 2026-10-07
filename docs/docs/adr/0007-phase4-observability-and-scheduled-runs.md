# ADR-0007: Phase-4 observability — OTel propagation, saga-health analytics, scheduled rebalance runs

Status: Accepted

## Context

Phases 1–3 left this context with a complete transfer saga
(DRAFT→…→RECEIVED) but three operational blind spots:

1. **No trace propagation.** A trace that enters this service on a Kafka
   message dies at the consumer: nothing Extracts the W3C `traceparent`
   header, and nothing Injects one on publish, so a transfer approval
   cannot be correlated with the inventory-storage reply that completes
   it. inventory-storage already ships the fleet pattern
   (`internal/adapters/outbound/kafka/tracing.go`, the `headerCarrier`).
2. **No health signal.** A transfer that parks in ALLOCATING for a day is
   invisible: the saga is deliberately fail-closed and never
   self-recovers, so a stuck transfer waits for a human who has nothing
   to look at.
3. **No scheduled planning.** Rebalancing proposals exist only when an
   operator explicitly calls the simulation or generate endpoints; the
   network drifts between calls and nobody can answer "what would the
   planner have proposed at 03:00".

## Decision

### 1. OTel trace propagation (both directions, one shape)

- **Inbound (every consumer):** `consumeLoop.run` — the single loop all
  five consumers funnel through — Extracts the message headers into the
  handler ctx via `otel.GetTextMapPropagator()` and a read-only
  `readOnlyHeaderCarrier` before handling. Every handler, and every
  outbox write it makes, runs as a child of the producer's publish span.
- **Outbound:** `TransferEncoder.Encode` (and the new
  `AnalyticsEncoder`) inject via a writable `headerCarrier` (the same
  shape as inventory-storage's). The traceparent is injected at ENCODE
  time — inside the use case's transaction — and persists in the outbox
  row's headers, so the relay's later Kafka write carries the trace of
  the use case that raised the event even though it happens in another
  goroutine, possibly minutes later. The relay forwards headers
  verbatim.
- **No span without a span:** with no live span on ctx the propagator
  writes nothing — an un-instrumented deployment ships clean headers,
  never an all-zero traceparent a consumer would try to parent onto.
- The composition root sets the global propagator
  (`TraceContext`+`Baggage`). A full tracer provider (OTLP export) is a
  later slice; propagation itself only needs the propagator.
- **Tests assert header PRESENCE, not values** — trace/span ids are
  per-run and per-sampler; the contract is that a span-carrying ctx
  yields a `traceparent` a consumer can Extract, and a span-less ctx
  yields none.

### 2. Saga-health analytics on a dedicated topic

A new **`warehouse.network-inventory-planning.analytics`** topic carries
CloudEvents 1.0 analytics occurrences (`dataschema` stream `analytics`,
mirroring inventory-storage's `warehouse.inventory.analytics`
separation of OLTP contract from health signal):

| Type | Subject/key | Data | Raised |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced` | `transfer_id` | `{transfer_id, from, to, age_seconds}` | Every saga transition, published through the transactional outbox IN the same transaction as the transition. |
| `com.warehouse.wes.network-inventory-planning.saga.TransferStuckDetected` | `transfer_id` | `{transfer_id, state, age_seconds, threshold_seconds}` | A bounded in-process ticker detecting non-terminal transfers past their per-state threshold. |
| `com.warehouse.wes.network-inventory-planning.saga.RebalanceRunCompleted` | `run_id` | `{run_id, proposal_count, rejected_count, stale_facts}` | Each scheduled rebalance pass. |

- `TransferStateAdvanced` derives from the aggregate's audit delta
  (`StateAdvancedSince(loadedVersion)` — the same delta `UpdateState`
  persists), so an occurrence exists for every step of every transfer
  without touching the integration contract.
- **Stuck detection is observe-only.** `CheckStuckTransfers` reads a
  narrow `StuckView` projection (`ListNonTerminal`: id, state,
  updated_at — never a rehydrated aggregate), evaluates pure per-state
  thresholds, and publishes. The loop NEVER mutates saga state, cancels
  a transfer, or releases a reservation — what an operator does with the
  signal is a human decision.
- Cadence and thresholds are env-configured:
  - `NIP_HEALTH_CHECK_INTERVAL` (default `5m`; `0` disables).
  - `NIP_STUCK_THRESHOLDS` — comma-separated `STATE=DURATION` overrides
    over the defaults `ALLOCATING=1h, PICKED=24h, IN_TRANSIT=72h`
    (unlisted non-terminal states fall back to a flat 24h). An
    unparsable value or unknown state name disables the check with an
    error log — fail-closed config, never a silently-dead sensor.

### 3. Bounded scheduled rebalance runs (observe-only)

- `NIP_REBALANCE_SCHEDULE` (a duration; **no default — unset means
  off**: a planner pass on a shared fleet is a deliberate deployment
  choice) ticks `RunScheduledRebalance`.
- Each pass runs the SAME fail-closed `planning.BuildSnapshot` as the
  simulation and the SAME `transfer.Planner` as the generate endpoint,
  persists a `rebalance_runs` row (migration 0004: id, started_at,
  snapshot_as_of, proposal_count, rejected_count, outcome,
  fail_closed_reason nullable) and publishes `RebalanceRunCompleted`.
- **Observe-only: no approval, no allocation command, ever.** A proposal
  becomes a transfer only through an operator's explicit
  `POST /v1/transfers:approve`.
- A fail-closed snapshot build (stale/missing facts) is a RECORDED
  `FAILED` run carrying the refusal as `fail_closed_reason` — a
  chronically stale read model becomes visible in run history rather
  than swallowed.
- `GET /v1/rebalance-runs` (new) lists runs newest-first, `?limit=N`
  clamped to [1,200] (default 50); unconfigured persistence answers 503,
  never an empty 200 that would fake a history.

### 4. Ticker shape

Both loops share `usecases.PeriodicRunner`: first tick immediate, then
fixed interval; a failed tick is logged and retried next interval (a
failure never stops the loop); cancellation stops it cleanly. The wait
function is injectable, so the scheduler tests drive ticks
deterministically — no sleep-based tests.

## Consequences

- A trace now spans: producer service → this service's consumer →
  outbox write → relay publish → downstream consumer, via the same W3C
  headers inventory-storage already speaks.
- The analytics topic is additive: the integration topic
  (`warehouse.network-inventory-planning.events`) and its three
  published types are untouched, so no consumer of the existing contract
  changes.
- The event-catalogue fitness test now requires the three new analytics
  types in the CloudEvents ADR's catalogue (ADR-0004's table is updated
  by this ADR's addition — see the type table above).
- v1 scheduler passes record `proposal_count` from positions+planner
  with no lane/policy catalogue projected yet (a later phase wires the
  lane catalogue); a COMPLETED run with zero proposals is a legitimate
  outcome, not a failure.
