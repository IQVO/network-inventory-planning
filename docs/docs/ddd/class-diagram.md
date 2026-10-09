---
id: class-diagram
title: Class diagrams
sidebar_label: Class diagrams
description: UML class diagrams of the network-inventory-planning domain, ports and adapters.
---

# Class diagrams

UML class diagrams of `internal/domain/**`, then the ports and adapters around
them. Only types and methods that exist in the code are drawn; plain getters
are left out. The fields of `InterWarehouseTransfer` are unexported in the
code and shown as private attributes.

## Package `transfer`: the saga aggregate

```mermaid
classDiagram
  class InterWarehouseTransfer {
    -TransferID id
    -string idempotencyKey
    -string originSiteID
    -string destinationSiteID
    -string sku
    -int quantity
    -string policyVersion
    -string operatorReason
    -TransferState state
    -string reservationID
    -int pickedQuantity
    -RejectionReason rejectionReason
    -int64 version
    +Approve(now) PlanApproved
    +RequestAllocation(now) AllocationRequested
    +MarkAllocated(StockAllocation, now) error
    +MarkUnfulfillable(lineID, reason, now) error
    +MarkPicked(Picked, now) error
    +MarkDispatched(now) error
    +MarkArrived(event, now) error
    +MarkStowed(Stowed, now) error
    +Cancel(reason, now) error
    +DispatchQuantity() int
    +StateAdvancedSince(version) StateAdvanced[]
  }
  class TransferState {
    <<enumeration>>
    DRAFT
    PROPOSED
    APPROVED
    ALLOCATING
    ALLOCATED
    PICKED
    IN_TRANSIT
    ARRIVED
    RECEIVED
    UNFULFILLABLE
    CANCELLED
  }
  class AuditEntry {
    +int64 Seq
    +TransferState From
    +TransferState To
    +string Event
    +string Reason
    +Time OccurredAt
  }
  class Allocation {
    +string StockUnitID
    +string BinID
    +int Quantity
  }
  class StowAllocation {
    +string StockUnitID
    +string BinID
    +int Quantity
  }
  class RejectionReason {
    <<enumeration>>
    ORIGIN_SITE_UNKNOWN
    INSUFFICIENT_USABLE
    IDEMPOTENCY_CONFLICT
  }
  class IllegalTransitionError
  class PlanApproved
  class AllocationRequested
  class DemandReleased {
    +string DemandID
    +WorkKind WorkKind
    +string PathID
    +string SiteID
    +Time CPT
    +int Quantity
  }
  class StateAdvanced
  InterWarehouseTransfer --> TransferState
  InterWarehouseTransfer "1" *-- "many" AuditEntry : audit
  InterWarehouseTransfer "1" *-- "many" Allocation : allocations
  InterWarehouseTransfer "1" *-- "many" StowAllocation : stowAllocations
  InterWarehouseTransfer --> RejectionReason
  InterWarehouseTransfer ..> IllegalTransitionError : returns
  InterWarehouseTransfer ..> PlanApproved : raises
  InterWarehouseTransfer ..> AllocationRequested : raises
  InterWarehouseTransfer ..> StateAdvanced : derives
```

Source: `internal/domain/transfer/saga.go`, `workflow.go`, `events.go`, `analytics.go`

`DemandReleased` is built by the use cases (`pickDemand`, `dispatchDemand`)
from a loaded transfer, not raised by the aggregate.

## Package `transfer`: planner, stuck detection, runs

```mermaid
classDiagram
  class Planner {
    +Generate(positions, policies, lanes) Proposal[]
  }
  class Position {
    +SiteID Site
    +SKU SKU
    +int Available
    +int CustomerReservations
    +int ConfirmedInbound
    +int CommittedOutbound
    +Time AsOf
    +Usable() int
  }
  class Policy {
    +string Version
    +int SafetyStock
    +int TargetStock
    +int UnitPriority
    +Deficit(Position) int
  }
  class Lane {
    +SiteID Origin
    +SiteID Destination
    +Duration LeadTime
    +int UnitHandlingCost
    +bool Enabled
    +Allows(origin, destination) bool
  }
  class Proposal {
    +SiteID Origin
    +SiteID Destination
    +int Quantity
    +string PolicyVersion
    +ReasonCode[] Reasons
    +Score() int
    +Valid() bool
  }
  class ScoreBreakdown {
    +int PriorityBenefit
    +int HandlingPenalty
    +int LeadTimePenalty
    +Total() int
  }
  class StuckCheck {
    +Evaluate(views, now) Stuck[]
  }
  class StuckThresholds {
    +ThresholdFor(state) Duration
  }
  class StuckView
  class RebalanceRun {
    +int64 ID
    +Time StartedAt
    +int ProposalCount
    +int RejectedCount
    +RebalanceOutcome Outcome
  }
  Planner ..> Position : reads
  Planner ..> Policy : reads
  Planner ..> Lane : reads
  Planner ..> Proposal : produces
  Proposal *-- ScoreBreakdown
  StuckCheck --> StuckThresholds
  StuckCheck ..> StuckView : reads
```

Source: `internal/domain/transfer/model.go`, `planner.go`, `stuck.go`, `analytics.go`

`ValidateApproval` (`approval.go`) and `ParseStuckThresholds` are package
functions, not types.

## Package `planning`

```mermaid
classDiagram
  class SiteCapability {
    +string Site
    +bool TransferOriginEnabled
    +bool TransferDestinationEnabled
    +int64 Revision
    +Time AsOf
    +Supersedes(other) bool
    +OriginAllowed() bool
    +DestinationAllowed() bool
  }
  class SiteSkuDemand {
    +string SourceOrderID
    +int LineNo
    +string Site
    +string SKU
    +int DemandedUnits
    +Time DueAt
    +DemandState State
    +string AssignmentVersion
    +Active() bool
    +DueIn(start, end) bool
  }
  class PublishedCapacityPlan {
    +string PlanID
    +string SiteID
    +string Location
    +string PathID
    +Time WindowStart
    +Time WindowEnd
    +float64 CapacityOverWindow
    +Time PublishedAt
    +Covers(dueAt) bool
    +Supersedes(other) bool
  }
  class Facts {
    +Watermark() Time
  }
  class PlanningSnapshot {
    +Time AsOf
    +Map Capabilities
    +Map DemandBySiteSKU
    +Map CapacityBySite
  }
  Facts o-- SiteCapability
  Facts o-- SiteSkuDemand
  Facts o-- PublishedCapacityPlan
  Facts ..> PlanningSnapshot : BuildSnapshot
```

Source: `internal/domain/planning/capability.go`, `demand.go`, `capacity_plan.go`, `facts.go`, `snapshot.go`

## Ports and adapters

```mermaid
classDiagram
  class TransferRepository {
    <<interface>>
    +Create(transfer, key) existing
    +Load(id) transfer
    +UpdateState(transfer)
  }
  class TransferQuery {
    <<interface>>
    +Get(id)
    +List(filter)
  }
  class TransferEventPublisher {
    <<interface>>
    +Publish(events)
  }
  class PlanningSnapshotRepository {
    <<interface>>
    +Load() Facts
  }
  class ProcessedEventRepository {
    <<interface>>
    +Claim(consumer, eventID) bool
  }
  class UnitOfWork {
    <<interface>>
    +Do(fn) error
  }
  class RebalanceRunRepository {
    <<interface>>
    +Record(run) id
    +List(limit)
  }
  class StuckTransferReader {
    <<interface>>
    +ListNonTerminal(limit)
  }
  class ApproveTransfer
  class SimulateTransferOptions
  class RunScheduledRebalance
  class CheckStuckTransfers
  class GetTransfer
  class ListTransfers
  class FindStuckTransfers
  class TransferRepo
  class TransferQueryRepo
  class OutboxWriter
  class SnapshotRepo
  class RebalanceRunRepo
  ApproveTransfer --> TransferRepository
  ApproveTransfer --> TransferEventPublisher
  ApproveTransfer --> PlanningSnapshotRepository
  ApproveTransfer --> UnitOfWork
  SimulateTransferOptions --> PlanningSnapshotRepository
  RunScheduledRebalance --> RebalanceRunRepository
  RunScheduledRebalance --> TransferEventPublisher
  CheckStuckTransfers --> StuckTransferReader
  CheckStuckTransfers --> TransferEventPublisher
  GetTransfer --> TransferQuery
  ListTransfers --> TransferQuery
  FindStuckTransfers --> TransferQuery
  TransferRepo ..|> TransferRepository
  TransferRepo ..|> StuckTransferReader
  TransferQueryRepo ..|> TransferQuery
  OutboxWriter ..|> TransferEventPublisher
  SnapshotRepo ..|> PlanningSnapshotRepository
  RebalanceRunRepo ..|> RebalanceRunRepository
```

Source: `internal/application/ports/*.go`, `internal/application/usecases/*.go`, `internal/adapters/outbound/postgres/*.go`, `cmd/network-inventory-planning/main.go`

`TransferRepo` serves both the saga repository and the stuck-transfer reader
(`wireHealthTicker` passes `postgres.NewTransferRepo(pool)` as the reader).
