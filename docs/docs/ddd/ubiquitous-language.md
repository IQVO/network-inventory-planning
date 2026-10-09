---
id: ubiquitous-language
title: Ubiquitous language
sidebar_label: Ubiquitous language
description: The terms of network-inventory-planning, each tied to the type, constant or column that implements it.
---

# Ubiquitous language

Use these exact names in code, API and conversation. Each term is a type,
constant or field in `internal/domain/**`, a use case, or a migration column.

## In the model

| Term | Meaning | Where |
| --- | --- | --- |
| **Site** (`SiteID`) | One warehouse in the network. The identity is facility-layout's `site_code`; NIP keeps a string, not the site hierarchy | `transfer.SiteID`, `planning.SiteCapability.Site` |
| **Position** | The planner's point-in-time view of one SKU at one site: `Available`, `CustomerReservations`, `ConfirmedInbound`, `CommittedOutbound`, `AsOf`. Deliberately not an inventory ledger | `transfer.Position` |
| **Usable** | `Available − CustomerReservations − CommittedOutbound` | `Position.Usable()` |
| **Policy** | Versioned guardrails for a SKU at a site: `SafetyStock`, `TargetStock`, `UnitPriority` | `transfer.Policy` |
| **Deficit** | `TargetStock − (Usable + ConfirmedInbound)`, never negative | `Policy.Deficit` |
| **Lane** | An approved directed route between two sites with `LeadTime`, `UnitHandlingCost`, `Enabled` | `transfer.Lane` |
| **Proposal** | Advisory recommendation: origin, destination, SKU, quantity, policy version, position as-of, reasons, score breakdown. Reserves and moves nothing | `transfer.Proposal` |
| **ReasonCode** | `DESTINATION_BELOW_TARGET`, `ORIGIN_ABOVE_SAFETY_STOCK`, `APPROVED_LANE` | `transfer.ReasonCode` |
| **ScoreBreakdown** | `PriorityBenefit − HandlingPenalty − LeadTimePenalty`; only a positive total is a valid proposal | `transfer.ScoreBreakdown` |
| **Planner** | Deterministic service turning positions, policies and lanes into proposals | `transfer.Planner` |
| **SiteCapability** | A site's transfer capability from facility-layout: origin-enabled, destination-enabled, `capability_revision` | `planning.SiteCapability` |
| **SiteSkuDemand** | One source order line's demand at a site: units, `due_at`, state `ACTIVE` or `REMOVED`, assignment version | `planning.SiteSkuDemand` |
| **PublishedCapacityPlan** | A warehouse-planning capacity plan mirrored locally: window `[window_start, window_end)`, `capacity_over_window` | `planning.PublishedCapacityPlan` |
| **PlanningSnapshot** | The coherent, fail-closed view simulation, approval and scheduled runs use. `AsOf` is the oldest watermark it used | `planning.PlanningSnapshot`, `BuildSnapshot` |
| **Participating site** | A site with all three facts fresh within `PLANNING_MAX_STALENESS`. Others are excluded, never zero-filled | `planning.BuildSnapshot` |
| **Staleness budget** | `PLANNING_MAX_STALENESS` (default 10m): the maximum age of any fact | `cmd/network-inventory-planning/main.go` |
| **Simulation** | The advisory per-site view: demand, capacity over window, headroom (negative = short) | `usecases.SimulateTransferOptions` |
| **InterWarehouseTransfer** | The saga aggregate: an operator-approved plan to move a quantity of one SKU from origin to destination | `transfer.InterWarehouseTransfer` |
| **TransferState** | `DRAFT`, `PROPOSED`, `APPROVED`, `ALLOCATING`, `ALLOCATED`, `PICKED`, `IN_TRANSIT`, `ARRIVED`, `RECEIVED`, `UNFULFILLABLE`, `CANCELLED`; `RECEIVED`, `UNFULFILLABLE` and `CANCELLED` are terminal | `transfer.TransferState` |
| **Transfer line** (`LineID`) | The v1 single line `<transfer_id>:1`; the correlation key with inventory-storage | `TransferID.LineID()` |
| **AuditEntry** | One immutable transition: seq, from, to, event, reason, time | `transfer.AuditEntry`, `transfer_audit` |
| **Idempotency key** | The approval's required `Idempotency-Key`, unique per transfer; a replay with the same body returns the original | `inter_warehouse_transfer.idempotency_key` |
| **Approval expiry** | 24h after approval; approving at or after it is `proposal-expired` | `defaultApprovalExpiry` |
| **Allocation** | One stock unit's part of the origin reservation (`stock_unit_id`, `bin_id`, `quantity`) | `transfer.Allocation` |
| **RejectionReason** | `ORIGIN_SITE_UNKNOWN`, `INSUFFICIENT_USABLE`, `IDEMPOTENCY_CONFLICT` | `transfer.RejectionReason` |
| **WorkDemand** (`WorkDemandReleased`) | One leg released as warehouse work; `demand_id` is `<transfer_id>:pick` or `:dispatch` | `transfer.DemandReleased` |
| **WorkKind** | `TRANSFER_PICK`, `TRANSFER_DISPATCH`, `TRANSFER_ARRIVAL` (WES's enum; NIP releases the first two) | `transfer.WorkKind` |
| **CPT** | Critical pull time of a released demand: release time + the leg's offset | `TRANSFER_*_CPT_OFFSET` |
| **Work release configuration** | Path ids and CPT offsets of the two legs; deployment configuration, never request input | `usecases.WorkReleaseConfig` |
| **Picked quantity** | What the origin pick actually picked; a short pick is recorded and the dispatch carries it | `InterWarehouseTransfer.PickedQuantity` |
| **StowAllocation** | A destination stow location from `TransferStockStowed` | `transfer.StowAllocation` |
| **Stuck transfer** | A non-terminal transfer whose last transition is older than its state's threshold. A reading, never a state | `transfer.StuckCheck` |
| **StuckThresholds** | `ALLOCATING=1h`, `PICKED=24h`, `IN_TRANSIT=72h`, 24h otherwise; `NIP_STUCK_THRESHOLDS` overrides | `transfer.DefaultStuckThresholds` |
| **RebalanceRun** | One scheduled, observe-only planning pass: watermark, counts, `COMPLETED` or `FAILED` with its fail-closed reason | `transfer.RebalanceRun`, `rebalance_runs` |
| **Saga-health occurrence** | An analytics event about the saga, not a contract event: `TransferStateAdvanced`, `TransferStuckDetected`, `RebalanceRunCompleted` | `warehouse.network-inventory-planning.analytics` |
| **Outbox** | The `outbox_events` table every published event is written to, in the same transaction as its cause | `postgres.OutboxWriter` |
| **Freshness** | How far the analytics projection is behind: `as_of`, `lag_seconds` | `report.Freshness` |

## Words used with care

- A **proposal** is advice; a **transfer** exists only after approval.
- **Allocated** is inventory-storage's reservation of origin stock; it is not
  picked, shipped or received.
- **Stuck** is a reading; nothing ever moves a transfer because it is stuck.
- **Fail-closed** means refuse (503/422, a `FAILED` run), never answer with an
  empty or zero-filled result.

## Not implemented

Terms from the design plan that have no code: NetworkNode, SKUPlacementPolicy,
DemandForecast, a projected NetworkInventoryPosition read model, lane capacity
and dispatch calendars, carbon cost, auto-approval.
