---
id: use-cases
title: Use cases
sidebar_label: Use cases
description: Every application use case of network-inventory-planning - trigger, inputs, invariants checked and events raised.
---

# Use cases

Every use case lives in `internal/application/usecases`. Each takes its clock
as a function (`Now`) and its stores and publishers as ports; the composition
roots in `cmd/` wire them. "Outbox" means the event is written as an
`outbox_events` row in the same transaction as the state change and reaches
Kafka through the relay.

## Planning (advisory)

| Use case | Trigger | Inputs | Invariants checked | Events raised |
| --- | --- | --- | --- | --- |
| `GenerateTransferProposals` | `POST /v1/transfer-proposals:generate` | `asOf`, `positions[]`, `policies[]`, `lanes[]` (request body) | `asOf` required; every position's `AsOf` equals `asOf` (no mixed snapshot); the planner then only uses enabled directed lanes, keeps the origin's safety stock (`usable − safetyStock`), never exceeds the destination deficit (`target − (usable + confirmedInbound)`), requires origin and destination positions with the same `AsOf`, and keeps a proposal only if its score (`quantity × unitPriority − quantity × unitHandlingCost − leadTimeDays`) is positive. Output is sorted by score, then SKU, origin, destination. | none |
| `SimulateTransferOptions` | `GET /v1/transfer-simulations`; MCP `simulate_transfer_options` | none (loads the three read models) | `planning.BuildSnapshot`: no empty table, every fact within `PLANNING_MAX_STALENESS`, a site participates only with capability + demand + capacity plan, a participating site disabled for either direction refuses the whole snapshot, demand counts only when `due_at` is in the plan window `[start, end)` | none |

## The transfer saga

| Use case | Trigger | Inputs | Invariants checked | Events raised |
| --- | --- | --- | --- | --- |
| `ApproveTransfer` | `POST /v1/transfers:approve` | `Idempotency-Key` header; `originSiteId`, `destinationSiteId`, `sku`, `quantity`, `policyVersion`, `operatorReason`, `proposalAsOf` | pick-leg release configured (else 503); key and `proposalAsOf` present; the **current** snapshot builds; `transfer.ValidateApproval` (both sites participate, origin origin-enabled, destination destination-enabled, destination has in-window demand for the SKU, origin capacity ≥ its in-window demand + quantity); `ProposeTransfer` field checks (ids, sites differ, quantity > 0, policy version); expiry = approval + 24h; idempotency key unique (same body → replay, different body → 409) | outbox: `TransferPlanApproved`, `TransferAllocationRequested`, and four `TransferStateAdvanced` (`→DRAFT`, `→PROPOSED`, `→APPROVED`, `→ALLOCATING`) |
| `ApplyTransferAllocation` | `TransferStockAllocated` on `warehouse.inventory.events` | transfer id, line id, origin, SKU, reservation id, allocations, reservation expiry, CE time | state is `ALLOCATING`; reply is for line `<id>:1`, same origin and SKU, has a reservation id, at least one allocation with stock unit, bin and positive quantity, allocations total the requested quantity, has an expiry | outbox: `TransferStateAdvanced` (`ALLOCATING→ALLOCATED`), pick `WorkDemandReleased` (`<id>:pick`, `TRANSFER_PICK`, origin site, CPT = reply time + pick offset, allocated quantity) |
| `ApplyTransferRejection` | `TransferStockAllocationRejected` | transfer id, line id, reason | state is `ALLOCATING`; reason in the closed set; line id matches | none (state change only) |
| `ApplyTransferPick` | `TransferPicked` on `warehouse.fulfillment.events` | `transfer_ref`, picked `quantity` | state is `ALLOCATED`; `0 < picked ≤ quantity` (a short pick is recorded) | outbox: `TransferStateAdvanced` (`ALLOCATED→PICKED`), dispatch `WorkDemandReleased` (`<id>:dispatch`, `TRANSFER_DISPATCH`, picked quantity) unless the dispatch leg is not configured |
| `ApplyTransferDispatched` | `TransferDispatched` | `transfer_ref` | state is `PICKED` | none in production (its publisher field is not wired by `cmd/network-inventory-planning`) |
| `ApplyTransferArrival` | `TransferArrived` | `transfer_ref` | state is `IN_TRANSIT` | none in production (same reason) |
| `ApplyTransferReceiptStaged` | `TransferReceiptStaged` on `warehouse.inventory.events` | transfer id, line id | state is `IN_TRANSIT` | none in production (same reason) |
| `ApplyTransferStow` | `TransferStockStowed` | transfer id, line id, destination, SKU, quantities, stow allocations | state is `ARRIVED`; same line, destination and SKU; stowed quantity > 0; at least one stow allocation | none in production (same reason) |

`InterWarehouseTransfer.Cancel` exists in the domain (legal from `DRAFT`,
`PROPOSED`, `APPROVED`, `ALLOCATING` with a reason) but **no use case or route
calls it** today.

## Read side

| Use case | Trigger | Inputs | Invariants checked | Events raised |
| --- | --- | --- | --- | --- |
| `GetTransfer` | `GET /v1/transfers/{id}`; MCP `get_transfer` | id | id not blank; unknown id → `transfer-not-found` | none |
| `ListTransfers` | `GET /v1/transfers`; MCP `list_transfers` | `state`, `originSiteId`, `destinationSiteId` (REST) or `site` (MCP), `limit`, `offset` | state in the 11-state vocabulary (case-insensitive); `0 ≤ limit ≤ 200` (0 → 50; REST rejects an explicit `limit=0`); `offset ≥ 0` | none |
| `FindStuckTransfers` | MCP `find_stuck_transfers` | `older_than_minutes`, `state`, `limit` | threshold positive; state must be non-terminal; ordered stalest first | none |
| `ListRebalanceRuns` | `GET /v1/rebalance-runs` | `limit` | clamped to 1–200, default 50 (REST rejects out-of-range values with 400) | none |

## Scheduled (observe-only, ADR 0007)

| Use case | Trigger | Inputs | Invariants checked | Events raised |
| --- | --- | --- | --- | --- |
| `CheckStuckTransfers` | `PeriodicRunner` every `NIP_HEALTH_CHECK_INTERVAL` (default 5m) | up to 500 non-terminal transfers (id, state, `updated_at`) | age in state > the state's threshold (`NIP_STUCK_THRESHOLDS`); never mutates a transfer | outbox: one `TransferStuckDetected` per stuck transfer, every tick |
| `RunScheduledRebalance` | `PeriodicRunner` every `NIP_REBALANCE_SCHEDULE` (off by default) | the three read models | same fail-closed snapshot as the simulation; a refusal is recorded as a `FAILED` run, not an error; never approves or allocates | `rebalance_runs` row; outbox: `RebalanceRunCompleted` (`run_id = rebal-<id>-<startedAt>`) after a completed run |

`PeriodicRunner` (`periodic.go`) is the ticker loop both run in: it logs a
failed tick and keeps going.

## Read-model ingestion (adapter level)

The three planning facts are not use cases: the inbound consumers in
`internal/adapters/inbound/kafka/consumers.go` validate each payload through
the `planning` domain constructors and upsert through the repository ports,
inside the same unit of work as the `processed_events` claim:

| Consumer | Fact | Rule |
| --- | --- | --- |
| site capability | `SiteCapability` | a strictly greater `capability_revision` supersedes |
| site SKU demand | `SiteSkuDemand` | last write wins per `(source_order_id, line_no)`; `REMOVED` contributes no units |
| capacity plan | `PublishedCapacityPlan` | a newer `published_at` supersedes; a plan without `site_id` is excluded |

## Analytics projection (`nip-projector`)

`AnalyticsConsumer` folds the three `saga.*` events into the analytical
tables through `report.Projection` (`internal/adapters/outbound/analyticsstore`):
one row per event, idempotent on the CloudEvents id recorded in
`analytics_processed_events` in the same transaction. The reports binary reads
them through `report.Reader` and the pure rules in `internal/analytics/report`
(half-open ranges, 30-day default, 366-day cap, percentiles, rejection rate,
freshness).
