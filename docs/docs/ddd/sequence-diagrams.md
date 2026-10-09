---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
description: UML sequence diagrams of the main runtime interactions of network-inventory-planning.
---

# Sequence diagrams

The main runtime interactions, from the use-case and adapter code. Replies
are shown as dashed returns or notes.

## 1. Project a sibling fact into a read model

All three read-model consumers share this shape; the example is
`SiteCapabilityChanged`.

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.facility.events
  participant C as SiteCapabilityConsumer
  participant U as UnitOfWork
  participant P as ProcessedEventRepository
  participant R as SiteCapabilityRepository
  K->>C: FetchMessage
  C->>C: cloudevents.Decode, check the full type
  Note over C: not a CloudEvent, unknown type, malformed or invalid payload is logged and committed past
  C->>U: Do(claim and upsert)
  U->>P: Claim(consumer, CloudEvents id)
  P-->>U: claimed or already processed
  U->>R: Upsert(capability), a greater revision wins
  U-->>C: commit, or roll back and return the error
  C->>K: CommitMessages only after the transaction settled
```

Source: `internal/adapters/inbound/kafka/consumers.go`, `kafka.go` (`consumeLoop`)
Omits: the retry loop (200ms doubling to 5s, no attempt limit) and the
`traceparent` extraction before handling.

## 2. Advisory simulation (REST and MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client as Console or MCP client
  participant H as Handler
  participant S as SimulateTransferOptions
  participant R as SnapshotRepo
  participant D as planning.BuildSnapshot
  Client->>H: GET /v1/transfer-simulations
  H->>S: Execute
  S->>R: Load capabilities, demands, plans
  R-->>S: facts
  S->>D: BuildSnapshot(facts, MaxStaleness, now)
  Note over D: refuses on an empty model, a stale fact, or a participating site disabled for a direction
  D-->>S: PlanningSnapshot or error
  S-->>H: per-site demand, capacity, headroom
  H-->>Client: 200, or 503 problem+json read-models-incomplete
```

Source: `internal/adapters/inbound/http/handler.go`, `internal/application/usecases/simulate_transfer_options.go`, `internal/domain/planning/snapshot.go`
Without `DATABASE_URL` the handler answers 503 `read-models-unavailable`
before the use case. The MCP tool `simulate_transfer_options` calls the same
use case.

## 3. Approve a transfer

```mermaid
sequenceDiagram
  autonumber
  actor Op as Operator
  participant H as Handler
  participant A as ApproveTransfer
  participant D as BuildSnapshot and ValidateApproval
  participant U as UnitOfWork
  participant T as TransferRepo
  participant O as OutboxWriter
  Op->>H: POST /v1/transfers:approve with Idempotency-Key
  H->>A: Execute(input)
  A->>A: check clock, staleness, work-release config, key, as-of
  Note over A: no pick path id or offset is 503 config-incomplete, nothing persisted
  A->>D: BuildSnapshot, then ValidateApproval
  Note over D: missing, stale or disabled facts are 422 facts-incomplete
  A->>U: Do
  U->>T: ProposeTransfer, Approve, RequestAllocation in memory
  U->>T: Create(transfer, idempotency key)
  T-->>U: existing transfer or none
  Note over U: same key and body replays the original, a different body is 409 idempotency-conflict
  U->>O: Publish TransferStateAdvanced x4, TransferPlanApproved, TransferAllocationRequested
  U-->>A: commit
  A-->>H: transfer in ALLOCATING
  H-->>Op: 200 transferId, state, transferLineId, expiresAt
```

Source: `internal/application/usecases/approve_transfer.go`, `internal/domain/transfer/saga.go`, `approval.go`, `internal/adapters/outbound/postgres/transfer_repo.go`, `outbox.go`
The aggregate insert and every outbox row commit together or not at all.

## 4. Outbox relay to Kafka

```mermaid
sequenceDiagram
  autonumber
  participant L as OutboxRelay
  participant DB as outbox_events
  participant S as RelaySink
  participant K as Kafka
  loop every relay tick
    L->>DB: claim unpublished rows in id order, FOR UPDATE SKIP LOCKED
    DB-->>L: rows
    L->>S: send one row with its stored headers
    S->>K: write to the row's topic
    K-->>S: ack
    L->>DB: set published_at
    Note over L: on a send failure increment attempts, store last_error and end the pass so no later row overtakes
  end
```

Source: `internal/adapters/outbound/postgres/outbox.go`, `internal/adapters/outbound/kafka/publisher.go`, `cmd/network-inventory-planning/main.go`
The relay runs when `OUTBOX_RELAY_ENABLED` is true and `KAFKA_BROKERS` is set;
production sets no attempt cap, so a failing row is retried every tick.

## 5. Apply the allocation reply and release the pick

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.inventory.events
  participant C as TransferReplyConsumer
  participant U as UnitOfWork
  participant A as ApplyTransferAllocation
  participant T as TransferRepo
  participant O as OutboxWriter
  K->>C: TransferStockAllocated
  C->>U: Do(claim and apply)
  U->>A: Execute(transfer id, allocation)
  A->>T: Load
  A->>A: MarkAllocated checks line, origin, SKU, quantity, reservation
  A->>T: UpdateState with optimistic version
  A->>O: Publish TransferStateAdvanced, WorkDemandReleased pick
  U-->>C: commit
  C->>K: CommitMessages
```

Source: `internal/application/usecases/transfer_facts.go`, `internal/adapters/inbound/kafka/transfer_reply_consumer.go`
`TransferStockAllocationRejected` follows the same shape, calls
`MarkUnfulfillable` and releases nothing.

## 6. Pick, dispatch, arrival and stow facts

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka
  participant C as Fact and reply consumers
  participant A as Apply use cases
  participant T as TransferRepo
  participant O as OutboxWriter
  K->>C: TransferPicked on warehouse.fulfillment.events
  C->>A: ApplyTransferPick
  A->>T: Load, MarkPicked, UpdateState
  A->>O: Publish TransferStateAdvanced, WorkDemandReleased dispatch with the picked quantity
  K->>C: TransferDispatched
  C->>A: ApplyTransferDispatched
  A->>T: MarkDispatched, UpdateState
  K->>C: TransferReceiptStaged on warehouse.inventory.events
  C->>A: ApplyTransferReceiptStaged
  A->>T: MarkArrived, UpdateState
  K->>C: TransferStockStowed
  C->>A: ApplyTransferStow
  A->>T: MarkStowed with the stow allocations, UpdateState
  Note over A: each fact is one transaction with its claim; unknown transfer, illegal transition or refused fact is logged and committed past
```

Source: `internal/application/usecases/transfer_facts.go`, `internal/adapters/inbound/kafka/transfer_fact_consumer.go`, `transfer_reply_consumer.go`
Omits: the reserved `TransferArrived`, which also calls `MarkArrived`.

## 7. Stuck check and scheduled rebalance

```mermaid
sequenceDiagram
  autonumber
  participant R as PeriodicRunner
  participant H as CheckStuckTransfers
  participant Rd as StuckTransferReader
  participant U as RunScheduledRebalance
  participant O as OutboxWriter
  R->>H: tick, the first one immediate
  H->>Rd: ListNonTerminal
  Rd-->>H: id, state, updated_at
  H->>H: StuckCheck.Evaluate against per-state thresholds
  H->>O: Publish TransferStuckDetected per stuck transfer
  Note over H: observe-only, no state change
  R->>U: tick of NIP_REBALANCE_SCHEDULE when configured
  U->>U: BuildSnapshot, then Planner.Generate
  Note over U: no positions, policies or lanes are passed, so a completed run proposes nothing
  U->>O: record a rebalance_runs row, publish RebalanceRunCompleted
```

Source: `internal/application/usecases/periodic.go`, `saga_health.go`, `scheduled_rebalance.go`
A failed tick is logged and retried at the next interval. A snapshot refusal
is recorded as a `FAILED` run and publishes nothing.

## 8. Analytics projection and reports

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka analytics topic
  participant P as nip-projector
  participant A as Analytical database
  participant D as DLQ topic
  actor Client as Report reader
  participant R as nip-reports
  K->>P: message
  P->>P: decode, dispatch on the full saga type
  alt not a CloudEvent
    Note over P: rate-limited WARN, committed past
  else another CloudEvents type
    Note over P: logged, ignored, committed past
  else known type with an unusable payload
    P->>D: raw bytes with x-dlq headers
  else valid
    P->>A: one transaction, mark the event id and append the fact row
  end
  P->>K: commit the offset
  Client->>R: GET /reports/transfer-funnel
  R->>A: read-only query, UTC day buckets
```

Source: `cmd/nip-projector/main.go`, `internal/adapters/inbound/kafka/analytics_consumer.go`, `internal/adapters/outbound/analyticsstore/*.go`, `cmd/nip-reports/main.go`, `internal/analytics/report/*.go`
A transient failure retries the same message and is never dead-lettered. The
DLQ topic is `warehouse.network-inventory-planning.analytics.dlq`.
