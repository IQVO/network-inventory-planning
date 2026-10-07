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

This service emits exactly three CloudEvents 1.0 types (all on
`warehouse.network-inventory-planning.events`, structured mode,
`application/cloudevents+json; charset=UTF-8`, published exclusively
through the transactional outbox):

| Type | Dataschema | Subject / Kafka key | Raised when |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved` | `urn:warehouse:network-inventory-planning:events:TransferPlanApproved:v1` | `transfer_id` | An operator approved an advisory proposal into a transfer plan (POST /v1/transfers:approve; the APPROVED transition). |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested` | `urn:warehouse:network-inventory-planning:events:TransferAllocationRequested:v1` | `transfer_line_id` (`<transfer_id>:1` in v1) | The saga's command to inventory-storage to allocate origin stock (the ALLOCATING transition, same outbox transaction). |
| `com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased` | `urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1` | `demand_id` (`<transfer_id>:pick` / `:dispatch`) | The saga's command to WES to work one transfer leg (ADR 0005): the pick leg with the ALLOCATED transition, the dispatch leg with the PICKED transition — each in the same outbox transaction. Note the `workdemand` entity segment: WES's consumed contract names it, not `transfer`. |

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
