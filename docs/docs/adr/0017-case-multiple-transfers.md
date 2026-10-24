# ADR-0017: Case-multiple transfers

Status: Accepted (2026-10-24)

Extends [ADR-0003](./0003-transfer-saga.md) (the transfer saga) and respects the
real Postgres and Kafka runtime. Additive on the wire; two migrations
(`0006_pack_levels`, `0007_transfer_case_rounding`). Part of the mastercase /
GTIN programme (contract P11-6).

## Context

Inter-site transfers move stock in eaches, but for a SKU that has an active CASE
level the origin picks and ships sealed cases. A transfer of 17 eaches of a
12-per-case SKU cannot be shipped as asked. The planner must move whole cases
and must never lose the demand silently.

## Decision

1. A local copy of product-master's pack levels (`ProductPackLevelDeclared` /
   `ProductPackLevelRetired`) is kept in Postgres by a consumer on its own
   stable group, `PACK_LEVEL_CONSUMER_GROUP` (chart `config.packLevelConsumerGroup`).
   No live lookup of the sibling context. The domain consumes a pure
   `CaseMultiples` map built from that view.
2. For a SKU with an active CASE level of at least 2 eaches the requested
   quantity is rounded by `RoundToCases`:
   - to the NEAREST multiple of the case size, a tie going UP;
   - never fewer than one case while the demand is at least 1;
   - never more than the origin can supply: capped at floor(available / case size)
     cases. An origin below one case moves nothing: the planner counts the
     candidate as rejected and approval answers 422 `below-one-case`.
3. The demand is never silently dropped. The proposal, the approved transfer and
   `TransferPlanApproved` carry `caseMultiple`, `cases`, `requestedQtyEaches`,
   `roundedQtyEaches` and a SIGNED `remainderEaches`
   (requested minus rounded: positive = eaches not moved, negative = the rounding
   went up). `quantity` is the rounded eaches.
4. A SKU without an active CASE level is planned and published exactly as before,
   byte for byte; the new fields are absent.
5. inventory-storage's allocation command has no case field, so
   `TransferAllocationRequested` is unchanged: its quantity is simply the rounded
   eaches. The case fields live on the plan, the transfer view and
   `TransferPlanApproved` only.
6. An unreadable pack view is fail-closed: 503 `pack-levels-unavailable` on
   proposal generation and on approval.

## Consequences

- One new mandatory env var, `PACK_LEVEL_CONSUMER_GROUP`; it must be set in
  warehouse-infra helm-values before this version is deployed.
- Rounding can move more than requested (rounding up); the signed remainder makes
  that visible to the operator and to downstream consumers.
- Retiring a CASE level makes the SKU transfer in eaches again from the next plan;
  an approved transfer keeps the rounding it was approved with.
