---
id: domain-events
title: Domain events
sidebar_label: Domain events
description: Every event network-inventory-planning publishes or consumes - type, topic, key, payload, trigger and effect.
---

# Domain events

Every event this context publishes or consumes. All are CloudEvents 1.0 in
structured content mode (`content-type: application/cloudevents+json; charset=UTF-8`)
with `source=/warehouse/network-inventory-planning` on what it publishes. The
published catalogue is pinned by [ADR 0004](../adr/0004-cloudevents-type-catalogue.md)
and an architecture fitness test (`internal/architecture/catalogue_fitness_test.go`):
adding, renaming or versioning a published type is one change across
`apis/asyncapi.yaml`, the emitting code and that ADR.

## Published on `warehouse.network-inventory-planning.events`

All through the transactional outbox, in the same transaction as the state
change that justifies them.

| Type | Kafka key | Raised when | Consumer |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved` | `transfer_id` | an operator approved a proposal (`APPROVED`) | none in the fleet today |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested` | `transfer_line_id` (`<transfer_id>:1`) | the saga asks for origin stock (`ALLOCATING`, same transaction) | inventory-storage |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationReleaseRequested` | `transfer_line_id` (`<transfer_id>:1`) | an operator cancelled an `ALLOCATED` transfer (`ALLOCATED → RELEASING`, same transaction, ADR 0022) | inventory-storage |
| `com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased` | `demand_id` (`<transfer_id>:pick` / `:dispatch`) | pick leg with `ALLOCATED`, dispatch leg with `PICKED` | wes-work-planning |

`dataschema` is `urn:warehouse:network-inventory-planning:events:<EventName>:v1`.
The `workdemand` entity segment is the one WES's consumed contract names.

Payloads (`apis/asyncapi.yaml`, `internal/adapters/outbound/kafka/publisher.go`):

- `TransferPlanApproved`: `transfer_id`, `origin_site_id`,
  `destination_site_id`, `sku`, `quantity`, `policy_version`,
  `operator_reason` (may be empty), `proposal_as_of`.
- `TransferAllocationRequested`: exactly inventory-storage's five fields,
  `transfer_id`, `transfer_line_id`, `origin_site_id`, `sku`, `quantity`.
- `TransferAllocationReleaseRequested`: `transfer_id`, `transfer_line_id`,
  `reservation_id` (the hold being resolved), `origin_site_id`, `sku`,
  `quantity`, `reason` (the operator's cancellation reason).
- `WorkDemandReleased`: exactly WES's eight fields, `demand_id`, `work_kind`
  (`TRANSFER_PICK` or `TRANSFER_DISPATCH`), `transfer_ref`, `path_id`,
  `site_id` (the origin), `cpt` (triggering fact time + the leg's offset),
  `sku`, `quantity` (allocated for pick, picked for dispatch).

## Published on `warehouse.network-inventory-planning.analytics` (ADR 0007)

Saga-health occurrences for this context's own projector, not an integration
contract. `dataschema` is
`urn:warehouse:network-inventory-planning:analytics:<EventName>:v1`.

| Type | Key | Data | Raised when |
| --- | --- | --- | --- |
| `...saga.TransferStateAdvanced` | `transfer_id` | `transfer_id`, `from` (empty for the creation entry), `to`, `age_seconds` (since creation) | each transition of the saga, in the transition's own transaction: `ApproveTransfer` (`→DRAFT`, `→PROPOSED`, `→APPROVED`, `→ALLOCATING`), `ApplyTransferAllocation` (`→ALLOCATED`), `ApplyTransferRejection` (`→UNFULFILLABLE`), `ApplyTransferPick` (`→PICKED`), `ApplyTransferDispatched` (`→IN_TRANSIT`), `ApplyTransferArrival` / `ApplyTransferReceiptStaged` (`→ARRIVED`), `ApplyTransferStow` (`→RECEIVED`), `CancelTransfer` (`→CANCELLED`, or `→RELEASING` for an `ALLOCATED` transfer) and `ApplyTransferAllocationReleased` (`RELEASING→CANCELLED`) |
| `...saga.TransferStuckDetected` | `transfer_id` | `transfer_id`, `state`, `age_seconds` (since `updated_at`), `threshold_seconds` | each health tick, for each non-terminal transfer past its threshold |
| `...saga.RebalanceRunCompleted` | `run_id` | `run_id`, `proposal_count`, `rejected_count`, `stale_facts` | a scheduled rebalance completed (a `FAILED` run publishes nothing) |

Every one of those use cases calls `publishStateAdvanced`
(`internal/application/usecases/analytics_hook.go`), and
`cmd/network-inventory-planning/wire_sagas.go` wires the outbox publisher into
all of them, so the analytics stream has one occurrence for every step of every
transfer. Without a broker (`KAFKA_BROKERS` unset) the publisher is nil and the
occurrence is skipped, never an error.

## Not published

`TransferProposed`, `OriginReservationRequested` and any transfer-completed
integration event are not in the AsyncAPI document. A proposal is advisory and
never published. A cancel publishes an integration event only for an
`ALLOCATED` transfer (the `TransferAllocationReleaseRequested` command above);
a cancel before allocation publishes only its `TransferStateAdvanced`.

## Consumed

Thirteen types from six producers (every `receive*` operation in
`apis/asyncapi.yaml` except the three `saga.*` ones below). Decoded as
CloudEvents structured mode only, dispatched on the full `type`, deduplicated
on the CloudEvents `id` in `processed_events` in the same unit of work, and
committed only after that transaction settles.

| Type | Topic | Producer | Effect |
| --- | --- | --- | --- |
| `com.warehouse.wms.facility-layout.site.SiteCapabilityChanged` | `warehouse.facility.events` | facility-layout | upsert `site_capability`; a greater `capability_revision` wins |
| `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged` | `warehouse.order-management.events` | order-management | upsert `site_sku_demand` per `(source_order_id, line_no)`, newest CloudEvents `time` wins (an older event is a no-op, ADR 0014); `REMOVED` contributes no units |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | `warehouse.warehouse-planning.events` | warehouse-planning | upsert `published_capacity_plan` per `plan_id`, newer `published_at` wins; no `site_id` → excluded |
| `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated` | `warehouse.inventory.events` | inventory-storage | `ALLOCATING → ALLOCATED`; reservation, allocations, expiry; releases the pick demand |
| `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected` | `warehouse.inventory.events` | inventory-storage | `ALLOCATING → UNFULFILLABLE` with a closed reason; an unknown reason is logged and skipped |
| `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased` | `warehouse.inventory.events` | inventory-storage | `RELEASING → CANCELLED`; outcome `RELEASED` or `ALREADY_RESOLVED` settles it either way; clears reservation id, allocations and expiry (ADR 0022, 0023) |
| `com.warehouse.wes.fulfillment-execution.transfer.TransferPicked` | `warehouse.fulfillment.events` | fulfillment-execution | `ALLOCATED → PICKED`; picked quantity (short allowed); releases the dispatch demand |
| `com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched` | `warehouse.fulfillment.events` | fulfillment-execution | `PICKED → IN_TRANSIT` |
| `com.warehouse.wes.fulfillment-execution.transfer.TransferArrived` | `warehouse.fulfillment.events` | fulfillment-execution | `IN_TRANSIT → ARRIVED`; reserved, may never fire |
| `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged` | `warehouse.inventory.events` | inventory-storage | `IN_TRANSIT → ARRIVED` |
| `com.warehouse.wms.inventory-storage.stock.TransferStockStowed` | `warehouse.inventory.events` | inventory-storage | `ARRIVED → RECEIVED`; stow allocations persisted |
| `com.warehouse.wms.product-master.product.ProductPackLevelDeclared` | `warehouse.product-master.events` | product-master | upsert the local `pack_levels` copy used to round case SKUs to whole cases (ADR 0017); a version not greater than the stored one is a no-op |
| `com.warehouse.wms.product-master.product.ProductPackLevelRetired` | `warehouse.product-master.events` | product-master | end-date that level in the local copy at `effective_to`; a level never seen or a GTIN that is no longer the stored one is ignored, a version not greater is a no-op |

Whichever of `TransferArrived` and `TransferReceiptStaged` arrives first drives
the arrival; the other is skipped as out of order. `nip-projector` consumes the
three `saga.*` types from the analytics topic. Failure handling per consumer is
in [Ecosystem integration](../ecosystem/integration.md).
