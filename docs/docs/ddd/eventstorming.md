---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
description: Design-level EventStorming of network-inventory-planning - three processes, with the hotspots visible in the code.
---

# EventStorming (design level)

Notation from the ddd-crew
[EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet).
Hotspots are gaps visible in the code or ADRs, not invented ones.

## Legend

```mermaid
flowchart LR
  A([Actor]):::actor
  C[Command]:::command
  G[Aggregate]:::aggregate
  E[Domain event]:::event
  P[Policy]:::policy
  R[Read model]:::readmodel
  X[External system]:::external
  H[Hotspot]:::hotspot
  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: ddd-crew cheat-sheet colours.

## 1. Sibling facts become a fail-closed snapshot

```mermaid
flowchart LR
  FL[facility-layout]:::external --> E1[SiteCapabilityChanged]:::event
  E1 --> P1[Whenever a capability revision is newer, replace the site row]:::policy
  P1 --> R1[site_capability]:::readmodel
  OM[order-management]:::external --> E2[SiteSkuDemandChanged]:::event
  E2 --> P2[Whenever an order line changes, upsert it, REMOVED tombstones it]:::policy
  P2 --> R2[site_sku_demand]:::readmodel
  WPL[warehouse-planning]:::external --> E3[CapacityPlanPublished]:::event
  E3 --> P3[Whenever a plan with a site_id is published, keep the newest per plan]:::policy
  P3 --> R3[published_capacity_plan]:::readmodel
  R1 --> S1[BuildSnapshot, fail closed]:::policy
  R2 --> S1
  R3 --> S1
  OP([Operator]):::actor --> Q1[Read the simulation]:::command
  S1 --> Q1
  Q1 --> R4[Per-site demand, capacity and headroom]:::readmodel
  H1[Demand upserts have no ordering guard]:::hotspot -.- P2
  H2[The planner needs positions, policies and lanes that nothing projects]:::hotspot -.- R4
  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/adapters/inbound/kafka/consumers.go`, `internal/adapters/outbound/postgres/planning_repositories.go`, `internal/domain/planning/snapshot.go`, `internal/application/usecases/simulate_transfer_options.go`

## 2. Approve, reserve, release the pick

```mermaid
flowchart LR
  OP([Operator]):::actor --> C1[ApproveTransfer with Idempotency-Key]:::command
  S0[Snapshot and ValidateApproval]:::policy --> C1
  C1 --> G1[InterWarehouseTransfer]:::aggregate
  G1 --> E1[TransferPlanApproved]:::event
  G1 --> E2[TransferAllocationRequested]:::event
  E2 --> X1[inventory-storage]:::external
  X1 --> E3[TransferStockAllocated]:::event
  X1 --> E4[TransferStockAllocationRejected]:::event
  E3 --> P1[Whenever stock is allocated, release the pick leg]:::policy
  P1 --> E5[WorkDemandReleased pick]:::event
  E5 --> X2[wes-work-planning]:::external
  E4 --> P2[Whenever allocation is rejected, mark the transfer unfulfillable]:::policy
  P2 --> G1
  H1[Approval is 503 until TRANSFER_PICK_PATH_ID is set]:::hotspot -.- C1
  H2[No cancel or revocation once the reservation exists]:::hotspot -.- G1
  H3[A mismatched reply is retried forever]:::hotspot -.- E3
  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/application/usecases/approve_transfer.go`, `transfer_facts.go`, `internal/domain/transfer/saga.go`, `approval.go`, `internal/adapters/inbound/kafka/transfer_reply_consumer.go`

## 3. Floor and destination facts close the transfer

```mermaid
flowchart LR
  FE[fulfillment-execution]:::external --> E1[TransferPicked]:::event
  E1 --> P1[Whenever the pick completes, record the picked quantity and release the dispatch leg]:::policy
  P1 --> G1[InterWarehouseTransfer]:::aggregate
  P1 --> E2[WorkDemandReleased dispatch]:::event
  E2 --> X1[wes-work-planning]:::external
  FE --> E3[TransferDispatched]:::event
  E3 --> P2[Whenever dispatch completes, mark in transit]:::policy
  P2 --> G1
  INV[inventory-storage]:::external --> E4[TransferReceiptStaged]:::event
  FE --> E5[TransferArrived reserved]:::event
  E4 --> P3[Whenever the first arrival signal comes, mark arrived]:::policy
  E5 --> P3
  P3 --> G1
  INV --> E6[TransferStockStowed]:::event
  E6 --> P4[Whenever the stow is confirmed, mark received]:::policy
  P4 --> G1
  G1 --> R1[Audit trail]:::readmodel
  T1[Health ticker]:::policy --> E7[TransferStuckDetected]:::event
  H1[TransferArrived may never fire]:::hotspot -.- E5
  H2[No TransferStateAdvanced for these transitions]:::hotspot -.- R1
  H3[Missing dispatch path withholds the dispatch demand with no re-release]:::hotspot -.- P1
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/application/usecases/transfer_facts.go`, `saga_health.go`, `internal/domain/transfer/workflow.go`, `cmd/network-inventory-planning/main.go`

Hotspot H2 is precise: `PICKED` does emit `TransferStateAdvanced`, while
`IN_TRANSIT`, `ARRIVED`, `RECEIVED` (and `UNFULFILLABLE` in diagram 2) do not.

## Sticky inventory

- **Read models:** `site_capability`, `site_sku_demand`,
  `published_capacity_plan`, `processed_events`, `inter_warehouse_transfer`
  with `transfer_audit`, `rebalance_runs`, and the analytical fact tables (see
  [Entity relationship](./entity-relationship.md)).
- **Not on the board because not built:** forecasting, auto-approval, route
  optimisation, dynamic order-site assignment.
