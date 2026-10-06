# ADR-0003: Phase-2 inter-warehouse transfer saga (approval, outbox, allocation replies)

Status: Accepted

## Context

Phase 1 (ADR-0002) gave this context local read models and an
advisory-only simulation. Nothing was published and no transfer could
commit. Meanwhile inventory-storage shipped its side of the reservation
slice (its PR #142): it CONSUMES our command
`com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested`
on `warehouse.network-inventory-planning.events` (data
`{transfer_id, transfer_line_id, origin_site_id, sku, quantity}`,
subject/key `transfer_line_id`) and PUBLISHES replies on
`warehouse.inventory.events`:

- `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated`
  (subject/key `reservation_id`; data includes `allocations[]` and
  `expires_at`);
- `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected`
  (subject/key `transfer_line_id`; closed reasons
  `ORIGIN_SITE_UNKNOWN|INSUFFICIENT_USABLE|IDEMPOTENCY_CONFLICT`).

The missing half is the saga itself: turning an advisory proposal into a
persisted, operator-approved, allocation-driven transfer without ever
moving physical stock ourselves, and without a distributed transaction.

## Decision

### The aggregate

`internal/domain/transfer.InterWarehouseTransfer` is a first-class
event-sourced saga aggregate with the state machine

```text
DRAFT → PROPOSED → APPROVED → ALLOCATING → ALLOCATED
                                     └─→ UNFULFILLABLE
(any pre-release state)        → CANCELLED
```

- **Cancel only pre-release**: `CANCELLED` is legal from
  DRAFT/PROPOSED/APPROVED/ALLOCATING and illegal once ALLOCATED or
  UNFULFILLABLE — after settlement the saga must not silently forget a
  hold inventory-storage is keeping; releasing is a later phase's explicit
  revocation path.
- **Immutable audit trail**: every transition appends a
  `transfer_audit` row (from_state, to_state, event, reason,
  occurred_at, per-transfer seq). Rows are insert-only; nothing ever
  updates or deletes them.
- **Transfer-level idempotency key**: `inter_warehouse_transfer.
  idempotency_key` is UNIQUE. The approval endpoint's required
  `Idempotency-Key` header becomes it: replaying the same key with the
  same payload returns the ORIGINAL transfer (`replayed: true`), never a
  second aggregate or a second event fan-out; the same key with a
  DIFFERENT payload is a 409 `idempotency-conflict`.
- Illegal transitions return a typed `IllegalTransitionError` and mutate
  nothing.

### The approval endpoint

`POST /v1/transfers:approve` (idempotency-key required, RFC 7807
problems, operator reason captured, 24h expiry in v1) takes a proposal
snapshot (origin, destination, SKU, quantity, policy version,
proposal_as_of) and validates it against the CURRENT read models through
the same fail-closed `planning.BuildSnapshot` the simulation uses, plus
`transfer.ValidateApproval` (both sites participate, directions enabled,
destination still shows in-window demand for the SKU, origin capacity
covers its own demand plus the transfer). Stale/missing/disabled facts
refuse with 422 `facts-incomplete`; unusable read models with 503 — never
a degraded approval.

On success ONE `ports.UnitOfWork` runs: PROPOSED → APPROVED → ALLOCATING
transitions, the aggregate INSERT (with its audit trail), and the two
outbox inserts. v1 is single-line: one TransferAllocationRequested per
transfer with `transfer_line_id = "<transfer_id>:1"`.

### The transactional outbox

Migration 0002 adds `outbox_events` (same shape as inventory-storage's
ADR-0017 table). `postgres.OutboxWriter` (the `TransferEventPublisher`
port) encodes each domain event via the outbound Kafka encoder
(`outboundkafka.TransferEncoder`, CloudEvents 1.0 structured mode through
this repo's own `internal/adapters/kafka/cloudevents`, CE id minted once
and persisted with the row) and inserts the rows INSIDE the approval
transaction. `postgres.OutboxRelay` is the only path to the broker: it
claims unpublished rows `FOR UPDATE SKIP LOCKED`, sends them one at a
time in id order via `outboundkafka.RelaySink`, marks each published as
it succeeds, and records attempts/last_error on failure so per-key
ordering is never violated by a later row overtaking a failed earlier
one. A request handler never sends to Kafka directly. The relay runs when
`OUTBOX_RELAY_ENABLED` is set and `KAFKA_BROKERS` configured; without
brokers the rows wait and the endpoint still works (the saga is durable,
delivery is catch-up).

### The reply consumer

`inboundkafka.TransferReplyConsumer` reads `warehouse.inventory.events`
under env-configured `TRANSFER_REPLY_CONSUMER_GROUP` (no default: unset
means the consumer does not exist). It dispatches on the FULL type
string, ignores everything else, hand-mirrors the two payloads (never
imports inventory-storage's Go types), dedupes on the CE id via
`processed_events`, and applies the reply in ONE transaction with the
claim (commit-after-settle, offset committed only after the transaction
commits — the Phase-1 at-least-once atomicity checklist):

- **Allocated** → `MarkAllocated`: state ALLOCATED with reservation_id,
  per-stock-unit allocations and expires_at persisted. The reply is
  validated against the transfer (line id, origin, SKU, quantity sum,
  reservation completeness); any mismatch is a deterministic skip.
- **Rejected** → `MarkUnfulfillable`: state UNFULFILLABLE with the
  closed reason; an unknown reason is a deterministic skip.

Transient failures (DB/tx) return non-nil and the run loop retries the
same message with capped backoff; deterministic failures log and move
on.

## Consequences

- The end-to-end invariant is testable with testcontainers
  (PG+Kafka): approve persists ALLOCATING and the outbox holds both
  events; the relay publishes them in order; a synthetic
  inventory-storage reply drives the aggregate to ALLOCATED; a reply
  replay (fresh group, same CE id) is a no-op.
- The published contract is now real: `apis/asyncapi.yaml` declares the
  two published types and the two consumed replies; inventory-storage's
  consumer already expects the exact command shape (five data fields,
  transfer_line_id key).
- The saga deliberately stops at ALLOCATED **at Phase 2**.
  Release-to-WES (work demand), pick/dispatch/arrival facts and
  destination receipt/stow correlation are Phase 3 — ADR-0005 implements
  them (the fact-driven tail PICKED → IN_TRANSIT → ARRIVED → RECEIVED
  plus the WorkDemandReleased pick/dispatch legs). A CANCELLED-after-
  allocation path still needs an explicit reservation revocation
  command.
- Approval capacity is bounded by the relay's drain rate, not the
  request path; an operator can observe stuck deliveries directly in
  `outbox_events` (attempts/last_error).
