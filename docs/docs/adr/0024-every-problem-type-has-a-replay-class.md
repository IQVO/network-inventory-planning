# ADR-0024: Every problem type has a replay class

Status: Accepted (2026-11-20)

Adopts the fleet idempotency replay-class rule
(`.claude/rules/fleet/idempotency-and-outbox.md`, warehouse-harness-template
`docs/idempotency-replay-classes.md`; owner decision 2026-11-18) in this
context: a request retried under the same `Idempotency-Key` is answered from
the store only when its first answer is final for that request. No event type
changes, no consumed-contract change, no route changes; the wire slugs,
titles and statuses are unchanged — only which refusals are stored changes.

## Context

The store (migration 0008, `internal/adapters/outbound/postgres/idempotency_store.go`)
runs the handler inside ONE transaction with the key row: it claims the key
with `INSERT ... ON CONFLICT DO NOTHING`, opens `SAVEPOINT handler_run`, runs
the handler through the transaction in the request context, then stores the
answer (status, content type, location, body) and commits. Until this ADR it
stored every answer below 500 — so a refusal whose cause is pure state (the
planning facts not ingested yet, a transfer not created yet) was replayed
forever, and with no TTL or purge a stale refusal pinned the key for good.
The motivating case was exactly the fleet's: `422 facts-incomplete` (the
read models are fed by Kafka; the facts arrive minutes later), stored and
replayed although the same request would pass after the facts arrived.

## Decision

Every problem type this adapter can answer has exactly one replay class,
declared on its catalogue entry (`internal/adapters/inbound/http/problems.go`):

- **FINAL** — the answer is a function of the request alone (or of a fact
  that never changes back). Stored and replayed.
- **OUTCOME** — the command committed state and then answered an error that
  must be remembered. Stored, and the handler's writes commit with the stored
  answer (this store keeps a stored answer's writes: the savepoint is
  released, not rolled back).
- **RETRYABLE** — nothing committed and the answer depends on state that can
  change. NOT stored: the middleware marks the capture, the store rolls the
  whole transaction back exactly like its 5xx branch, the key row is dropped
  with it and a retry under the same key executes again. A different body
  under the released key is simply a new request.

A 2xx is stored; a 5xx and a panic are not (unchanged). The class travels
from `writeProblem` (the shared error writer, the only writer of problems)
through the request context to the middleware's capture, which turns it into
`contract.StoredResponse.Transient`; the middleware holds no list of slugs
and no status ranges beyond 2xx and 5xx. A refusal that declared no class (a
problem written without the catalogue) is released like RETRYABLE — the safe
fallback. The in-memory store honours the same mark, so both implementations
of `ports.IdempotencyStore` agree.

A concurrent duplicate (the loser of the key claim) waits on the unique index
until the first transaction ends: when the first request ended RETRYABLE the
row never committed, so the loser's claim succeeds and it executes itself;
when the first answer was stored the loser replays it.

### Class table

| slug | status | class | reason |
| --- | --- | --- | --- |
| `idempotency-key-required` | 400 | FINAL | the header is a function of the request (written before the store: never captured, classified for completeness) |
| `idempotency-key-invalid` | 400 | FINAL | same: malformed header, request-only |
| `idempotency-key-reused` | 409 | FINAL | the same key with a different fingerprint is a reuse for as long as the first answer is stored |
| `invalid-request` | 400 | FINAL | malformed JSON / unknown field / unreadable body: request-only |
| `invalid-if-match` | 400 | FINAL | the If-Match header is unparseable: request-only (unkeyed PUTs anyway) |
| `invalid-catalogue-entry` | 422 | FINAL | the entry fails validation of its own fields (unkeyed PUTs anyway) |
| `invalid-planning-snapshot` | 422 | FINAL | the planner validates the caller-provided snapshot: request-only |
| `invalid-approval` | 422 | FINAL | every raise site is request- or configuration-only: `approve_transfer.go` nil clock, non-positive staleness, missing key, missing proposalAsOf, and `transfer.ErrInvalidProposal` (the domain's own-face validation); the state-dependent refusals answer `facts-incomplete` instead |
| `invalid-cancel` | 422 | FINAL | empty reason / nil clock: request-only |
| `proposal-expired` | 422 | FINAL | monotone in time (an expired proposal never unexpires); today unreachable from the approval route, which mints `expiresAt = now + 24h` |
| `cancel-refused` | 409 | FINAL | the fact-driven tail (PICKED onwards) is monotone — `IllegalTransitionError` forbids going back — and UNFULFILLABLE is terminal: once refused, always refused |
| `unknown-query-parameter` | 400 | FINAL | the query string is request-only (unkeyed GETs anyway) |
| `invalid-limit` | 400 | FINAL | request-only |
| `invalid-cursor` | 400 | FINAL | the token is request-only (a well-formed foreign cursor is a store bug, not state that changes) |
| `invalid-query` | 400 | FINAL | request-only (unkeyed GET) |
| `invalid-report-range` | 400 | FINAL | request-only (reports GETs) |
| `invalid-report-limit` | 400 | FINAL | request-only |
| `below-one-case` | 422 | RETRYABLE | the origin's headroom comes from the CURRENT planning snapshot, which the Kafka consumers rebuild (roundToCases → OriginHeadroom) |
| `facts-incomplete` | 422 | RETRYABLE | the planning facts are event-fed: a site missing, stale or disabled now can participate after the next event |
| `transfer-not-found` | 404 | RETRYABLE | the transfer is created by ANOTHER request (the approval); a 404 on the command's target is FINAL only when nothing can deliver the target later, and here the approval can; the slug also answers the unkeyed read side |
| `version-mismatch` | 412 | RETRYABLE | the If-Match precondition compares against mutable catalogue state; re-fetch and retry succeeds (unkeyed PUTs anyway) |
| `concurrent-modification` | 409 | RETRYABLE | a lost optimistic-lock race: nothing committed, the retry wins (unkeyed PUTs anyway) |
| `approval-unavailable` | 503 | RETRYABLE | 5xx: never stored, a retry executes once the dependency is back |
| `cancel-unavailable` | 503 | RETRYABLE | 5xx |
| `catalogue-unavailable` | 503 | RETRYABLE | 5xx |
| `config-incomplete` | 503 | RETRYABLE | 5xx (deployment configuration missing) |
| `idempotency-unavailable` | 503 | RETRYABLE | 5xx |
| `pack-levels-unavailable` | 503 | RETRYABLE | 5xx |
| `read-models-incomplete` | 503 | RETRYABLE | 5xx |
| `read-models-unavailable` | 503 | RETRYABLE | 5xx |
| `read-side-unavailable` | 503 | RETRYABLE | 5xx |
| `rebalance-runs-unavailable` | 503 | RETRYABLE | 5xx |
| `report-store-error` | 500 | RETRYABLE | 5xx |
| `saga-unavailable` | 503 | RETRYABLE | 5xx |

No problem type is OUTCOME today: no path commits state and then refuses.
The two refusals raised inside the approval's unit of work
(`ErrIdempotencyConflict` — the application-level key check — and the
proposal-validation refusals) return as errors from the unit of work and roll
back with it, so nothing they wrote survives. The OUTCOME mechanism is real
in this store (a stored refusal's writes commit: the savepoint is released on
a 4xx, which is how a healed 23505 conflict already commits) and is proven by
a synthetic catalogue entry in the replay-class tests only.

Judgement calls (most consequential first):

1. `transfer-not-found` RETRYABLE, not FINAL. This service mints the
   transfer ids and never deletes a transfer, which reads FINAL — but the
   target arrives by a separate keyed request, and the same slug answers the
   unkeyed read side. With no TTL a wrongly-FINAL 404 would pin forever, so
   the fleet default (unsure ⇒ RETRYABLE) applies. The one-word flip is
   recorded here if the owner prefers FINAL.
2. `cancel-refused` FINAL: the state machine's tail is monotone
   (`IllegalTransitionError`), so the refusal cannot clear with time. A
   wrong FINAL would pin a refusal for good — accepted because the monotone
   claim is enforced by the domain, not by an assumption about event order.
3. `invalid-approval` FINAL re-verifies the phase-0 hint (which said
   RETRYABLE, "validated against current facts"): the facts-based validation
   answers `facts-incomplete`, while every `ErrInvalidApproval` raise site is
   request- or configuration-only (cited in the table).
4. `proposal-expired` FINAL although the approval route cannot raise it
   today (`expiresAt` is minted at `now + 24h`): classified for the day a
   caller-supplied expiry exists, by its monotone-in-time cause.

## Consequences

- A retry under a key that was refused RETRYABLE executes again: the caller
  sees a fresh answer, never the stored refusal. RELEASED means only that
  the same key may be tried again by whoever sent it; no consumer-side
  retry behaviour is added by this ADR.
- `idempotency-key-reused` no longer protects a released key: a different
  body under a key whose first request ended RETRYABLE is accepted as a new
  request (fleet rule, stated in the spec text).
- The BDD scenario that pinned the old behaviour (a replayed
  `facts-incomplete` refusal) now proves the release: the retry executes and
  succeeds once the facts arrive.
- Open point, NOT fixed here: no TTL and no purge ever deletes an
  `idempotency_keys` row (migration 0008's comment promises a sweep "by
  age"; nothing implements it). A stored FINAL answer — success or refusal —
  replays forever. The wrongly-FINAL cost of a wrong classification is
  therefore unbounded, which is why the RETRYABLE default matters more here
  than in TTL'd stores. Follow-up shape: order-sourcing #84 /
  last-mile-delivery #86.
- Open point: `docs/docs/api-reference/rest/*` and the OpenAPI descriptions
  now say which refusals replay; the pinned spec copies in consumers do not
  (description-only drift, handled by the wave's pin refresh).

## References

- Fleet rule: `.claude/rules/fleet/idempotency-and-outbox.md`; standard:
  warehouse-harness-template `docs/idempotency-replay-classes.md`.
- The tests that hold this ADR true:
  `internal/adapters/inbound/http/replay_class_catalogue_test.go` (the ADR
  table equals the catalogue),
  `internal/adapters/inbound/http/idempotency_replay_classes_integration_test.go`
  (per-class behaviour over the real Postgres store) and
  `cmd/network-inventory-planning/idempotency_replay_integration_test.go`
  (the release over the real router, Postgres and Kafka).
- Companion decisions: ADR-0021 (the fleet REST guideline adoption whose
  stored-4xx sentence this ADR refines), ADR-0003 (the saga whose approval
  and cancellation routes are the keyed writes).
