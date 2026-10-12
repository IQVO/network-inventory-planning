# ADR-0013: Poison transfer replies and facts are dead-lettered, never retried

Status: Accepted

## Context

The transfer-reply consumer (`warehouse.inventory.events`) and the
transfer-fact consumer (`warehouse.fulfillment.events`) run the fleet's
at-least-once loop: a handler error is TRANSIENT and the same message is
retried forever with capped backoff, blocking its partition, while a
deterministic problem is logged and committed past (ADR 0003, ADR 0005).

The classification had a hole. `InterWarehouseTransfer.MarkAllocated` and
`MarkUnfulfillable` refuse a reply that does not match the transfer it names
(another `transfer_line_id`, origin, SKU, allocation total, no reservation id
or expiry, a rejection reason outside inventory-storage's closed vocabulary)
with a plain `fmt.Errorf`. `usecases.IsDeterministic` only recognised
`ErrTransferNotFound` and `IllegalTransitionError`, so such a reply was
treated as transient: one malformed reply wedged a `warehouse.inventory.events`
partition — and with it every later reply of every transfer keyed there —
until someone moved the group's offset by hand. The facts consumer had the
mirror-image gap: a refused or malformed fact was skipped with only a WARN, so
the evidence was lost.

The same bytes always fail the same way. Retrying them never helps; skipping
them silently hides a producer/consumer disagreement about a transfer.

## Decision

1. The domain names the refusals. `transfer.ErrReplyRefused` is the sentinel
   for an allocation reply the aggregate refuses; `ErrReplyLineMismatch`
   (reply for another line) and `ErrUnknownRejectionReason` (reason outside
   `ORIGIN_SITE_UNKNOWN | INSUFFICIENT_USABLE | IDEMPOTENCY_CONFLICT`) wrap it.
   Every check in `checkAllocationReply` and `MarkUnfulfillable` returns one of
   them; the existing `ErrFactRefused` keeps covering the fact checks.
2. `usecases.IsDeterministic` includes refused replies and facts.
   `usecases.IsRefusedReply` singles out the POISON subset (`ErrReplyRefused`,
   `ErrFactRefused`) from the benign skips (unknown transfer, illegal
   transition — replays and out-of-order messages).
3. Both transfer consumers dead-letter poison with the fleet's `<topic>.dlq`
   convention (already used by the analytics projector, ADR 0009, and by
   wes-work-planning / order-management for consumed sibling topics):
   `warehouse.inventory.events.dlq` and `warehouse.fulfillment.events.dlq`.
   The raw message (key, value, original headers) is written with
   `x-dlq-source-topic`, `x-dlq-consumer` (`transfer-reply-consumer` /
   `transfer-fact-consumer`, because a consumed topic's DLQ may be shared with
   sibling consumers), `x-dlq-error` and `x-dlq-failed-at`.
4. Poison is: an undecodable payload of a known type (bad JSON shape,
   unparsable `expires_at`), a rejection with an unknown reason (checked in
   the adapter before the saga is touched — the same condition as the domain
   sentinel, the same treatment), a `TransferPicked` without a positive
   quantity, and any use-case error matching `IsRefusedReply`.
5. The dead-letter write happens INSIDE the unit of work that holds the
   `processed_events` claim. A failed DLQ write rolls the claim back and is
   returned as transient, so the loop retries and the poison is never lost; a
   successful write commits the claim, and the offset moves on. A redelivery
   after commit is a dedupe no-op (no second dead letter).
6. Non-CloudEvents bytes and unknown types stay a plain skip (full-type
   dispatch; they are not addressed to this consumer). Unknown transfers and
   illegal transitions stay WARN-and-skip: they are expected (a transfer this
   deployment never approved, a replay) and must not flood the DLQ.

## Consequences

- A malformed reply can no longer block the saga: the next reply on the same
  partition applies. Proven by
  `TestTransferReplyPoisonIsDeadLetteredAndTheNextReplyApplies` (real Kafka +
  Postgres: two poison replies ahead of a good one on one partition).
- The transfer the poison named keeps its state (`ALLOCATING` for a refused
  reply); the stuck-transfer health check (ADR 0007) flags it after its
  threshold, and the DLQ holds the evidence.
- Two new topics exist once the first poison arrives (auto-created by the
  DLQ writer). They are operational sinks, not contracts: they carry other
  contexts' raw messages and are not added to `apis/asyncapi.yaml`.
- Re-driving a dead letter needs the producer fixed first and the
  `processed_events` row of that CloudEvents id deleted (the claim committed
  together with the dead letter).
