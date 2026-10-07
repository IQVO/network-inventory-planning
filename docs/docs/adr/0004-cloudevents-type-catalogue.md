# ADR-0004: CloudEvents type catalogue for network-inventory-planning

Status: Accepted

## Context

The fleet's event-catalogue fitness test requires every CloudEvents type
this service declares for ITSELF in `apis/asyncapi.yaml` to be listed in a
CloudEvents ADR under `docs/docs/adr/` — other contexts read that ADR to
learn what this service emits, without parsing the contract. Phase 2
(ADR-0003) introduced this context's first two published types, so the
catalogue duty starts now.

## Decision

This service emits exactly six CloudEvents 1.0 types (structured mode,
`application/cloudevents+json; charset=UTF-8`, published exclusively
through the transactional outbox). Three on the integration topic
`warehouse.network-inventory-planning.events`:

| Type | Dataschema | Subject / Kafka key | Raised when |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved` | `urn:warehouse:network-inventory-planning:events:TransferPlanApproved:v1` | `transfer_id` | An operator approved an advisory proposal into a transfer plan (POST /v1/transfers:approve; the APPROVED transition). |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested` | `urn:warehouse:network-inventory-planning:events:TransferAllocationRequested:v1` | `transfer_line_id` (`<transfer_id>:1` in v1) | The saga's command to inventory-storage to allocate origin stock (the ALLOCATING transition, same outbox transaction). |
| `com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased` | `urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1` | `demand_id` (`<transfer_id>:pick` / `:dispatch`) | The saga's command to WES to work one transfer leg (ADR 0005): the pick leg with the ALLOCATED transition, the dispatch leg with the PICKED transition — each in the same outbox transaction. Note the `workdemand` entity segment: WES's consumed contract names it, not `transfer`. |

Plus, since ADR 0007 (Phase-4 observability), three saga-health
occurrences on the analytics topic
`warehouse.network-inventory-planning.analytics` (dataschema stream
`analytics`, not `events`):

| Type | Dataschema | Subject / Kafka key | Raised when |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced` | `urn:warehouse:network-inventory-planning:analytics:TransferStateAdvanced:v1` | `transfer_id` | Every saga state transition (ADR 0007), published through the outbox in the same transaction as the transition. |
| `com.warehouse.wes.network-inventory-planning.saga.TransferStuckDetected` | `urn:warehouse:network-inventory-planning:analytics:TransferStuckDetected:v1` | `transfer_id` | The bounded health ticker finds a non-terminal transfer past its per-state threshold (ADR 0007). Observe-only. |
| `com.warehouse.wes.network-inventory-planning.saga.RebalanceRunCompleted` | `urn:warehouse:network-inventory-planning:analytics:RebalanceRunCompleted:v1` | `run_id` | A scheduled rebalance pass completes (ADR 0007). Observe-only: no approval, no allocation command. |

Everything else this service's AsyncAPI declares is CONSUMED, not
declared for itself, and therefore outside this catalogue:

- `com.warehouse.wms.facility-layout.site.SiteCapabilityChanged`
- `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged`
- `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished`
- `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated`
- `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected`
- `com.warehouse.wes.fulfillment-execution.transfer.TransferPicked`
- `com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched`
- `com.warehouse.wes.fulfillment-execution.transfer.TransferArrived`
- `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged`
- `com.warehouse.wms.inventory-storage.stock.TransferStockStowed`

## Consequences

- Adding, renaming or versioning a published type is a three-part change
  in one PR: the AsyncAPI document, the emitting code, and this catalogue
  row (the fitness test fails the build on drift).
- Consumers (today: inventory-storage's transfer-allocation consumer)
  read this catalogue for the wire contract; the authoritative payload
  shapes remain in `apis/asyncapi.yaml`.
