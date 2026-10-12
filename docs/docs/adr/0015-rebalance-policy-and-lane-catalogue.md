# ADR-0015: Rebalance Policy and Transfer Lane catalogues

Status: Accepted

## Context

ADR 0007 added the scheduled rebalance pass (`NIP_REBALANCE_SCHEDULE`) and
left one thing for later: "no lane/policy catalogue projected yet (a later
phase wires the lane catalogue)". The pass called the planner with an empty
`[]Policy` and `[]Lane`, so every run was a COMPLETED run with zero
proposals. The ticker existed but could never propose anything.

The planner itself (`transfer.Planner`, ADR 0001) already reasons with a
`Policy` (site + SKU: safety stock, target stock, unit priority, version) and
a `Lane` (origin, destination, lead time, unit handling cost, enabled).
`POST /v1/transfer-proposals:generate` accepts both on every call. No sibling
context owns either concept: NIP is the only bounded context that reasons
about inter-site transfer economics. So no fact exists to project from, and
the catalogue has to be NIP's own data, managed by an operator.

NIP also holds no per-site stock position, because no sibling publishes
one. The scheduled pass therefore cannot run the position-based
`Planner.Generate` that the generate endpoint runs over a caller-supplied
snapshot.

## Decision

1. **Two NIP-owned catalogues.** These are persisted by additive migration
   `0005_rebalance_catalogue`:

   | Table | Key | Holds |
   |---|---|---|
   | `rebalance_policies` | `(site_id, sku)` | `Policy` |
   | `transfer_lanes` | directed `(origin_site_id, destination_site_id)` | `Lane` |

   Each row carries a `revision`, which starts at 1 and gains 1 per accepted
   change, and an `updated_at`. The domain types are
   `transfer.RebalancePolicy` and `transfer.TransferLane`. `Validate`
   refuses an entry on its face with `ErrInvalidCatalogueEntry` in these
   cases:
   - a missing key;
   - a negative quantity or cost;
   - `TargetStock < SafetyStock`;
   - a lane to itself;
   - a revision that changes the key.

2. **REST, with the existing shapes.** The routes are:
   - `GET /v1/rebalance-policies` and `PUT /v1/rebalance-policies`
   - `GET /v1/transfer-lanes` and `PUT /v1/transfer-lanes`

   A PUT body is exactly the existing `Policy` or `Lane` schema, the same
   JSON the generate endpoint accepts, and is keyed by its own Site + SKU or
   Origin → Destination. Responses embed that shape as
   `{policy|lane, revision, updatedAt[, outcome]}`.

3. **PUT is idempotent and uses optimistic concurrency.**

   | Situation | Result |
   |---|---|
   | The PUT carries exactly the stored entry | Nothing is written; 200 `UNCHANGED` |
   | A new entry | 201 `CREATED`, revision 1 |
   | A changed entry | 200 `UPDATED`, revision + 1 |
   | `If-Match` (the `ETag`, i.e. the revision) is sent but the entry moved on or does not exist | 412 `version-mismatch` |
   | `If-Match` is omitted | The PUT applies to the current revision |
   | Two writers race on the same revision | The guarded write (`INSERT … ON CONFLICT DO NOTHING` / `UPDATE … WHERE revision = $n`) lets exactly one win; the other gets 409 `concurrent-modification` |
   | The entry is invalid | 422 `invalid-catalogue-entry` |
   | The instance has no database | 503 `catalogue-unavailable` |

   There is no DELETE. A lane is retired with `Enabled: false`. A site stops
   receiving a SKU when its policy has `TargetStock: 0`.

4. **The scheduled pass plans from the catalogues.** The new
   `Planner.PlanFromSnapshot(snapshot, policies, lanes)` reuses the
   site-level approximation that approval validation (ADR 0003) enforces,
   so every scheduled proposal is approvable as generated:
   - **Lane usability.** A lane is usable only when it is `Enabled` and
     `site_capability` lets its origin send and its destination receive.
     The fail-closed snapshot (ADR 0002) already excludes sites without
     complete facts, and it fails the whole run when a participating site
     is disabled for a direction. That makes it a FAILED run with the
     reason, the same as the simulation and the approval.
   - **Need.** The destination's need for a SKU is its in-window ACTIVE
     demand, capped by its policy's `TargetStock`.
   - **Donation.** The origin donates from its capacity headroom (capacity
     over the window minus its own in-window demand) and keeps its policy's
     `SafetyStock` back. An origin without a policy for the SKU never
     donates.
   - **Allocation.** Candidates are served greedily, highest score first,
     against shared need and headroom budgets, so one pass never promises
     the same units twice. The order is deterministic: score, then SKU,
     origin, destination.
   - **Rejections.** A candidate that fails `Proposal.Valid`, for example
     one whose handling and lead-time cost outweigh its benefit, is counted
     in the run's `rejectedCount`.
   - **Unreadable catalogue.** An unreadable catalogue is a FAILED run
     (`load rebalance catalogue: …`), never a silent zero-proposal run.

   The pass stays observe-only (ADR 0007): it records the run and publishes
   `RebalanceRunCompleted` with the counts. It never approves, never
   allocates and never releases work.

5. The generate endpoint is unchanged: it still plans over a caller-supplied
   snapshot of positions, policies and lanes.

## Consequences

- The scheduled run proposes once an operator fills the catalogues. With
  empty catalogues it behaves exactly as before: COMPLETED with zero
  proposals.
- Operators get an auditable, concurrency-safe catalogue API. A stale
  dashboard cannot overwrite a newer edit when it sends `If-Match`.
- The scheduled planner is position-blind by construction. If a sibling
  ever publishes stock positions, a later ADR can project them and switch
  the pass to the position-based planner without changing the catalogue
  API.
- New tests:

  | Test | Covers |
  |---|---|
  | domain `catalogue_test.go` | validation and revision rules |
  | domain `scheduled_planner_test.go` | capability gate, budgets, greedy sharing, determinism |
  | use-case tests | idempotent PUT, If-Match, lost race, scheduled run proposing, catalogue outage as a FAILED run |
  | HTTP handler and OpenAPI contract tests | the REST surface against the spec |
  | `postgres_integration/catalogue_integration_test.go` | a real two-writer race against Postgres 16 |
  | `features/rebalance_catalogue.feature` | BDD for the scheduled run producing proposals |

## Follow-ups

- **Proposed: `InterWarehouseTransfer.Cancel` (D7).** The aggregate's
  `Cancel` transition stays reserved. The domain allows it from DRAFT,
  PROPOSED, APPROVED and ALLOCATING, but it has no route, no use case and
  no published event. No ADR yet specifies who may cancel a transfer, or how
  compensation works. Compensation means releasing an origin reservation
  that inventory-storage may already hold for an ALLOCATING transfer whose
  reply is in flight, and withdrawing released work demand. A future ADR
  must define the cancel command, its authorization, the compensation
  messages to inventory-storage and wes-work-planning, and the analytics
  event. Until then no transfer reaches CANCELLED; a stuck transfer surfaces
  through the saga health check (ADR 0007).
- **Not planned.** A catalogue change event (for example
  `RebalancePolicyChanged`) has no consumer today and is not published.
