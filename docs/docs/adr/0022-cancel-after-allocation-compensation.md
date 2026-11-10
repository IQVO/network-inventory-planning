# ADR-0022: Cancel-after-allocation — the transfer saga's compensation leg

Status: Accepted (2026-11-05)

Closes ADR-0003's documented limitation: *"a CANCELLED-after-allocation
path still needs an explicit reservation revocation command."* Until now a
transfer abandoned or stuck in ALLOCATED left its origin reservation
holding stock in inventory-storage until lazy expiry noticed the timeout —
hours of phantom unusable stock, and no way for the planner to close the
saga cleanly.

## Context

The saga's state machine allowed CANCELLED only from the pre-release
states (DRAFT/PROPOSED/APPROVED/ALLOCATING): once stock is ALLOCATED the
saga must not silently forget a hold another context is keeping. The
compensation was designed but never built — the transfer had no operator
cancel surface at all (the domain `Cancel` method was unreachable: no
endpoint, no consumer).

## Decision

Add the explicit revocation path as a command/reply exchange, exactly
mirroring the allocation leg (ADR 0003):

```
ALLOCATED --CancelAfterAllocation--> RELEASING --TransferStockAllocationReleased--> CANCELLED
```

- **New state RELEASING** (non-terminal; the reservation_id and
  allocation fields stay on the aggregate until the release settles, when
  ADR 0023 clears them). The stuck ticker's default
  threshold for RELEASING is 1h, the ALLOCATING class — it waits only
  for inventory-storage's reply. Migration 0009 widens the state and
  reservation_id CHECK constraints.
- **`TransferAllocationReleaseRequested`** (published COMMAND on this
  context's integration topic, subject/key `transfer_line_id`, data
  `{transfer_id, transfer_line_id, reservation_id, origin_site_id, sku,
  quantity, reason}`): raised by the aggregate's
  `CancelAfterAllocation` and outboxed in the SAME transaction as the
  ALLOCATED → RELEASING transition.
- **`TransferStockAllocationReleased`** (consumed reply on
  `warehouse.inventory.events`, subject/key `transfer_line_id`, data
  `{..., outcome, reason}` with outcome the closed set
  `RELEASED | ALREADY_RESOLVED`): settles the saga RELEASING → CANCELLED.
  Both outcomes are success — either way the hold is gone. inventory-storage
  (its ADR 0051) resolves the reservation through its existing
  RevokeReservation logic and owns a `transfer_allocation_releases`
  ledger for database-level replay safety.
- **`POST /v1/transfers/{transferId}/cancellation`** (keyed, ADR-0021):
  pre-release states cancel synchronously (200 CANCELLED); ALLOCATED
  answers 202 RELEASING; PICKED onwards and UNFULFILLABLE refuse 409
  cancel-refused — the stock is physically in motion or the saga already
  closed, and after pick a physical custody exception (never a silent
  restore) is the right tool. The cancel is idempotent on the saga
  state: RELEASING answers 202 again, CANCELLED answers 200 again; the
  Idempotency-Key middleware replays the stored answer byte for byte.
- **Fact checks**: MarkReleased refuses (deterministically, ErrFactRefused
  → dead-letter) any fact that does not match the transfer it claims to
  settle — wrong line, reservation, origin, SKU, quantity, or an outcome
  outside the closed set.

## Consequences

- The fact-driven tail (PICKED onwards) stays non-cancellable: the stock
  is physically in motion and a cancel there would need a
  damage/short-pick exception flow, which remains out of scope.
- The release exchange is idempotent end to end: NIP's cancel replays on
  the saga state, inventory-storage's release replays on its ledger, and
  the reply consumer dedupes on the CloudEvents id.
- Cross-references: inventory-storage ADR 0051 (the provider side),
  ADR 0003 (the original saga), ADR 0005 (the fact-driven tail's
  discipline), ADR 0013 (deterministic refusal handling).
