# ADR-0023: A transfer cancelled after allocation holds no reservation

Status: Accepted (2026-11-10)

Amends ADR-0022 (cancel-after-allocation compensation).

## Context

ADR-0022 added RELEASING and said the reservation id and allocation fields
"stay on the aggregate" while inventory-storage resolves the hold. It did not
say what happens to them when `TransferStockAllocationReleased` settles the
saga to CANCELLED. The implementation kept them.

Migration 0009 admits a `reservation_id` only in ALLOCATED, RELEASING, PICKED,
IN_TRANSIT, ARRIVED and RECEIVED (`inter_warehouse_transfer_reservation_id_check`).
CANCELLED is outside that set, so persisting the settled transfer failed with
SQLSTATE 23514. The reply consumer classified the database error as transient
and retried the same fact forever (ADR-0013); the transfer stayed RELEASING and
the e2e scenario "Cancelling an ALLOCATED transfer" could not pass.

## Decision

The state table for the reservation reference is the constraint's, stated as a
domain rule:

| State | Reservation id / allocations / expiry |
| --- | --- |
| DRAFT, PROPOSED, APPROVED, ALLOCATING | none yet |
| ALLOCATED, RELEASING | held |
| PICKED, IN_TRANSIT, ARRIVED, RECEIVED | held (the stock the transfer consumed) |
| UNFULFILLABLE | none (rejected before any reservation) |
| CANCELLED | none — pre-release cancels never had one; a release-settled cancel lets go of it |

`InterWarehouseTransfer.MarkReleased` clears the reservation id, the
allocations and the allocation expiry in the same step that moves RELEASING to
CANCELLED. The hold is gone either way (`RELEASED` or `ALREADY_RESOLVED`), so
the aggregate has nothing to reference. The constraint is not weakened and no
SQL workaround exists: the aggregate cannot reach the invalid combination.

The history is not lost: the ALLOCATED audit entry's reason still names the
reservation (`reservation <id>`), and the release fact's own audit entry records
the outcome.

## Consequences

- `GET /v1/transfers/{id}` of a release-cancelled transfer omits
  `reservationId`, as for any transfer that never held one.
- A redelivered or re-emitted `TransferStockAllocationReleased` for a settled
  transfer is an illegal transition and is skipped with a WARN, as before;
  the same CloudEvent id is deduped by the processed-event claim.
- Every transition is covered by a table that persists it against the real
  constraint (`transfer_repo_reservation_integration_test.go`) and by a
  domain table that asserts the invariant after each step
  (`reservation_invariant_test.go`).
- Not changed: the consumer still treats a persistence error as transient and
  retries it with capped backoff (ADR-0013 reserves dead-lettering for
  payloads the aggregate refuses). A constraint violation is a bug in this
  service, not in the message, and blocking the partition with an ERROR per
  attempt is how the e2e suite found it.
