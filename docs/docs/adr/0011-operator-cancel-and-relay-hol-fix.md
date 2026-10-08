# ADR-0011: Operator cancel (pre-release) and relay head-of-line fix

Status: Accepted

## Context

Three escapes surfaced in a live run:

1. `InterWarehouseTransfer.Cancel(reason, now)` has existed since ADR-0003
   but **nothing called it**: an operator had no way to withdraw a transfer
   that was approved by mistake or stuck in `ALLOCATING` (ADR-0007's stuck
   detector deliberately only *observes*, ADR-0008 left cancel "a separate
   decision").
2. The transactional-outbox relay claimed rows `ORDER BY id` and, on the
   first sink failure, recorded the failure for that row and **ended the
   pass**. Every later row — of *every* key and topic — waited behind one
   poison row until its attempts ran out. Observed live: one row at 243
   attempts blocked 14 rows, including `WorkDemandReleased`.
3. (Scheduled-rebalance honesty is recorded as an Amendment to ADR-0007,
   not here.)

## Decision

### 1. Operator cancel, REST only

- Use case `CancelTransfer`: load → `Cancel(reason, now)` →
  `UpdateState` → `TransferStateAdvanced` (the same `publishStateAdvanced`
  helper every other transition uses), **all in one UnitOfWork**.
- **The aggregate alone decides which states cancel.** ADR-0003/0005 are
  unchanged: `DRAFT`, `PROPOSED`, `APPROVED`, `ALLOCATING` only. `ALLOCATED`
  and the fact-driven tail (`PICKED`…`RECEIVED`), `UNFULFILLABLE` refuse
  with `IllegalTransitionError`, surfaced as `ErrTransferNotCancellable`.
  The use case does not widen this.
- **Idempotent:** cancelling an already `CANCELLED` transfer is a no-op
  success (nothing written, no second occurrence, original reason kept).
- `POST /v1/transfers/{id}:cancel`, body `{"reason": "<required>"}`:

  | Status | Problem slug | When |
  | --- | --- | --- |
  | 200 | – | cancelled (or already cancelled); body is the `TransferView` |
  | 400 | `invalid-request` | malformed body / unknown field |
  | 404 | `transfer-not-found` | unknown id |
  | 409 | `transfer-not-cancellable` | state past release |
  | 422 | `invalid-cancellation` | blank / missing reason |
  | 503 | `cancel-unavailable` | no persistence/outbox wired, or infra error |

  The mux cannot express a literal after a wildcard within one segment, so
  the route is `POST /v1/transfers/{idAction}` and the handler requires the
  `:cancel` suffix (anything else is 404).
- **No MCP write tool.** The MCP surface stays read-only (ADR-0008); the
  governance test is untouched.
- Wired in `cmd/network-inventory-planning` only when the outbox publisher
  exists (same rule as approve): without it the occurrence could not commit
  with the state change, so the endpoint answers 503 instead of cancelling
  silently without the analytics trail.

### 2. Events emitted

Only the **`TransferStateAdvanced` analytics occurrence** (ADR-0007) for
`<state> → CANCELLED`. **No integration event** is added to the events
topic or the AsyncAPI/ADR-0004 catalogue: a cancellable transfer has no
released work demand and no `ALLOCATED` reservation, so no downstream
consumer has anything to undo. If a consumer ever needs a cancel signal,
that is a new, deliberate contract (and the reservation-revocation path
ADR-0003 already defers).

Known edge, stated rather than hidden: in `ALLOCATING` the
`TransferAllocationRequested` command has already been emitted, so
inventory-storage may create the reservation concurrently with the cancel.
Its later `TransferStockAllocated` reply then hits a `CANCELLED` transfer,
is an `IllegalTransitionError`, and the reply consumer logs-and-skips it
(deterministic). The reservation is not released by this service; it ages
out via its own `expires_at`. Closing that window needs the explicit
revocation command, which remains a later phase. Cancelling from
`ALLOCATING` is the operator's call; it is what ADR-0003 permits.

### 3. Relay head-of-line fix (`OutboxRelay.RelayOnce`)

Ordering domain = **(topic, key)**. On a send failure the relay now records
`attempts`/`last_error` for that row and **continues**, skipping any *later*
row whose `(topic, key)` already failed in this pass. Rows of other keys or
topics are never held behind a poison row; rows of the *same* key are never
sent out of order. The pass commits every success and every failure record
in one transaction and returns the count published plus an
`errors.Join` of the per-row failures. `max attempts` semantics are
unchanged (rows at the limit are excluded by `claimBatch`; the next row of
that key then proceeds exactly as before). A failed row is retried on the
next pass in id order.

## Consequences

- Operators can withdraw pre-release transfers; the trail
  (`TransferCancelled` audit entry, analytics occurrence) is the same shape
  as every other transition.
- A poison outbox row no longer stalls unrelated events. Residual limit:
  `claimBatch` still takes `LIMIT batchSize` oldest rows, so ≥ `batchSize`
  simultaneously poisoned rows would still fill the batch; `WithMaxAttempts`
  is the existing bound for that.
- The exposed contract grows by one REST operation only; Kafka contracts
  and the MCP tool list are unchanged.
