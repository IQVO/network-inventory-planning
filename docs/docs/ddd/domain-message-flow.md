---
id: domain-message-flow
title: Domain message flow
sidebar_label: Domain message flow
description: The messages that cross network-inventory-planning's boundary, as numbered domain message flows.
---

# Domain message flow

Following ddd-crew [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Arrows are prefixed `cmd:` (command), `evt:` (event) or `qry:` (query). Only
real messages appear: REST routes from the handler, MCP tools from `tools.go`
and CloudEvents types from the consumers and encoders.

## 1. Facts arrive, an operator approves, stock is reserved, the pick is released

```mermaid
sequenceDiagram
  autonumber
  participant FL as facility-layout
  participant OM as order-management
  participant WPL as warehouse-planning
  actor Op as Operator via nip_mfe
  participant NIP as network-inventory-planning
  participant INV as inventory-storage
  participant WP as wes-work-planning
  FL->>NIP: evt: SiteCapabilityChanged on warehouse.facility.events
  OM->>NIP: evt: SiteSkuDemandChanged on warehouse.order-management.events
  WPL->>NIP: evt: CapacityPlanPublished on warehouse.warehouse-planning.events
  Note over NIP: three local read models, each deduplicated on the CloudEvents id
  Op->>NIP: qry: GET /v1/transfer-simulations
  Note over NIP: per-site demand, capacity and headroom, or 503 when facts are missing or stale
  Op->>NIP: cmd: POST /v1/transfers:approve with Idempotency-Key
  Note over NIP: validated against the current snapshot, persisted as ALLOCATING, events in the outbox
  Note over NIP: TransferPlanApproved goes to the events topic, no consumer
  NIP->>INV: cmd: TransferAllocationRequested keyed by transfer_line_id
  INV->>NIP: evt: TransferStockAllocated on warehouse.inventory.events
  Note over NIP: ALLOCATING to ALLOCATED, reservation and allocations persisted
  NIP->>WP: cmd: WorkDemandReleased, demand transfer_id:pick, TRANSFER_PICK
```

Source: `internal/adapters/inbound/kafka/consumers.go`, `internal/adapters/inbound/http/handler.go`, `internal/application/usecases/approve_transfer.go`, `transfer_facts.go`, `internal/adapters/inbound/kafka/transfer_reply_consumer.go`

The simulation is per site; it does not propose quantities. Proposals come
from `POST /v1/transfer-proposals:generate` with an explicit snapshot.

## 2. The physical tail: pick, dispatch, arrival, stow

```mermaid
sequenceDiagram
  autonumber
  participant WP as wes-work-planning
  participant FE as fulfillment-execution
  participant NIP as network-inventory-planning
  participant INV as inventory-storage
  WP->>FE: pick work released (WES release path)
  FE->>NIP: evt: TransferPicked on warehouse.fulfillment.events
  Note over NIP: ALLOCATED to PICKED, picked quantity recorded even when short
  NIP->>WP: cmd: WorkDemandReleased, demand transfer_id:dispatch, picked quantity
  WP->>FE: dispatch work released
  FE->>NIP: evt: TransferDispatched
  Note over NIP: PICKED to IN_TRANSIT
  INV->>NIP: evt: TransferReceiptStaged on warehouse.inventory.events
  Note over NIP: IN_TRANSIT to ARRIVED, TransferArrived would do the same
  INV->>NIP: evt: TransferStockStowed
  Note over NIP: ARRIVED to RECEIVED, stow allocations persisted, terminal
```

Source: `internal/application/usecases/transfer_facts.go`, `internal/adapters/inbound/kafka/transfer_fact_consumer.go`, `transfer_reply_consumer.go`

The `TRANSFER_ARRIVAL` work kind exists in the vocabulary but NIP never
releases it.

## 3. Allocation is refused

```mermaid
sequenceDiagram
  autonumber
  actor Op as Operator
  participant NIP as network-inventory-planning
  participant INV as inventory-storage
  Op->>NIP: cmd: POST /v1/transfers:approve
  NIP->>INV: cmd: TransferAllocationRequested
  INV->>NIP: evt: TransferStockAllocationRejected with a closed reason
  Note over NIP: ALLOCATING to UNFULFILLABLE, no work is released
  Op->>NIP: qry: GET /v1/transfers/{id}
  Note over NIP: rejectionReason and the full audit trail
```

Source: `internal/domain/transfer/saga.go` (`MarkUnfulfillable`), `transfer_reply_consumer.go`, `internal/adapters/inbound/http/transfers_read.go`

The saga does not retry or re-plan; the operator approves a new transfer
under a new idempotency key.

## 4. Finding a stuck transfer

```mermaid
sequenceDiagram
  autonumber
  participant NIP as network-inventory-planning
  participant PROJ as nip-projector
  participant REP as nip-reports
  participant OA as warehouse-ops-agent
  actor Op as Operator
  NIP-->>PROJ: evt: TransferStuckDetected on the analytics topic
  Note over NIP: observe-only ticker, never changes a transfer
  OA->>NIP: qry: MCP find_stuck_transfers with older_than_minutes
  Op->>REP: qry: GET /reports/stuck-transfers
  Note over REP: detections per state per day plus the latest occurrences
```

Source: `internal/application/usecases/saga_health.go`, `periodic.go`, `internal/adapters/inbound/mcp/tools.go`, `internal/adapters/inbound/http/reports_handler.go`
