# ADR-0005: Work-demand release and fact-driven saga transitions

Status: Accepted

## Context

ADR-0003 stopped the saga at ALLOCATED: inventory-storage held an origin
reservation, but nothing told WES to actually work the transfer, and
nothing drove the aggregate past the allocation. Three sibling contracts
now exist to close that loop:

- **WES consumes `WorkDemandReleased`** (wes-work-planning, merged #150):
  type
  `com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased`,
  dataschema `urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1`,
  subject/key `demand_id`, data `{demand_id, work_kind, transfer_ref,
  path_id, site_id, cpt, sku, quantity}` (all required), work_kind enum
  `TRANSFER_PICK|TRANSFER_DISPATCH|TRANSFER_ARRIVAL`. WES validates
  `path_id` against its own PathCatalogue and enqueues one work unit per
  demand under the deterministic id `demand_id`.
- **fulfillment-execution publishes transfer facts** (merged #162) on
  `warehouse.fulfillment.events`:
  `com.warehouse.wes.fulfillment-execution.transfer.TransferPicked|TransferDispatched|TransferArrived`,
  dataschema `urn:warehouse:fulfillment-execution:events:<Name>:v1`,
  subject/key `task_id`, data `{transfer_ref, demand_id?, work_unit_id,
  task_id, work_kind, site_id?, sku?, quantity?}`.
- **inventory-storage publishes destination facts** on
  `warehouse.inventory.events` (contract built in parallel; the strings
  below are the agreed shape):
  `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged`,
  dataschema `urn:warehouse:inventory-storage:events:TransferReceiptStaged:v1`,
  data `{transfer_id, transfer_line_id, destination_site_id, sku,
  expected_quantity, received_quantity, variance}`; and
  `com.warehouse.wms.inventory-storage.stock.TransferStockStowed`,
  dataschema `urn:warehouse:inventory-storage:events:TransferStockStowed:v1`,
  data `{transfer_id, transfer_line_id, destination_site_id, sku,
  received_quantity, stowed_quantity, allocations:[{stock_unit_id,
  bin_id, quantity}]}` — both subject/key `transfer_line_id`.

## Decision

### The extended state machine

```text
DRAFT → PROPOSED → APPROVED → ALLOCATING → ALLOCATED → PICKED → IN_TRANSIT → ARRIVED → RECEIVED
                                     └─→ UNFULFILLABLE                (terminal)
(any pre-release state)                                    → CANCELLED
```

- **ALLOCATED → PICKED** on fulfillment-execution's `TransferPicked`.
  A SHORT pick (`picked_quantity` < allocated) is RECORDED, never
  refused: `picked_quantity` persists on the aggregate and the dispatch
  demand carries it, so the planned flow equals the physical flow. Only
  an impossible quantity (≤ 0 or > allocated) refuses, as a deterministic
  `ErrFactRefused` skip.
- **PICKED → IN_TRANSIT** on `TransferDispatched`.
- **IN_TRANSIT → ARRIVED** on EITHER `TransferArrived` (the reserved,
  WES-work-driven kind — it may never fire) OR inventory-storage's
  `TransferReceiptStaged` (scan-driven receiving). Both drive the same
  transition; whichever arrives first wins and the other is a
  deterministic out-of-order skip. This is deliberate: receiving a
  shipment by scanning must not depend on a WES arrival task existing.
- **ARRIVED → RECEIVED (terminal)** on inventory-storage's
  `TransferStockStowed`, with the destination `allocations[]` persisted
  on the aggregate.
- **Cancellation stays pre-release only** (as ADR 0003): from PICKED
  onwards the stock is physically in motion; any cancel is an illegal
  transition.

### Work-demand release

Two of the three legs are released by THIS context, each through the
transactional outbox in the SAME transaction as the state change that
justifies it (a transfer can never be ALLOCATED with its pick work
unreleased, nor PICKED with its dispatch work unreleased):

| Trigger | Leg | demand_id | quantity | path_id | site_id | cpt |
| --- | --- | --- | --- | --- | --- | --- |
| `TransferStockAllocated` (the existing reply consumer, same UnitOfWork) | TRANSFER_PICK | `<transfer_id>:pick` | allocated quantity | `TRANSFER_PICK_PATH_ID` | origin | reply time + `TRANSFER_PICK_CPT_OFFSET` |
| `TransferPicked` (the new fact consumer) | TRANSFER_DISPATCH | `<transfer_id>:dispatch` | **picked** quantity | `TRANSFER_DISPATCH_PATH_ID` | origin | fact time + `TRANSFER_DISPATCH_CPT_OFFSET` |

The `TRANSFER_ARRIVAL` leg is NOT released by this context in v1:
arrival work at the destination is triggered by the receiving scan
itself (the receipt-staged path), so a released arrival demand would
race the physical truck.

`demand_id` is deterministic (`<transfer_id>:pick` / `:dispatch`), so a
WES-side replay dedupes onto the same work unit.

### Fail-closed at approve time

`TRANSFER_PICK_PATH_ID` has NO default. When it (or a valid
`TRANSFER_PICK_CPT_OFFSET`) is unset, `POST /v1/transfers:approve`
answers **503 `config-incomplete`** and persists nothing: an approval
would mint a transfer whose allocation reply has no pick demand to
release — the "allocated-but-unworked" strand. Configuration gaps are
found at approve time, never discovered on the reply path.

### The consumers

- **`warehouse.inventory.events`** (existing `TRANSFER_REPLY_CONSUMER_GROUP`
  consumer, extended): the two allocation replies (unchanged) plus the
  two destination facts.
- **`warehouse.fulfillment.events`** (new consumer, group
  `TRANSFER_FACT_CONSUMER_GROUP`): the three transfer facts.

Both follow the Phase-1/2 delivery rules: CloudEvents 1.0 structured
mode only, dispatch on the FULL type, dedupe on the CE id in
`processed_events`, claim + transition (+ outbox rows) in ONE
UnitOfWork, offset committed only after the transaction settles. A fact
whose `transfer_ref` names no known transfer is WARN-logged and
committed past — it belongs to a transfer this deployment never approved
— never retried, never crashed on.

### The arrival-kind reservation

`TransferArrived` is documented as RESERVED: scan-driven receiving may
bypass it entirely (`TransferReceiptStaged` drives the same ARRIVED
transition). The consumer still handles it so a WES that DOES work an
arrival task contributes the same transition.

## Consequences

- The published-type catalogue (ADR 0004) gains
  `com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased`
  — note the `workdemand` entity segment, WES's contract, not `transfer`.
- Migration 0003 widens the state CHECK, adds `picked_quantity` /
  `stow_allocations`, and re-names 0002's auto-named CHECK constraints
  so later migrations have stable handles.
- Short picks surface as a dispatch demand with the picked quantity; the
  delta against the reservation is inventory-storage's variance concern
  at stow time, not a saga state.
- Deployment requires `TRANSFER_PICK_PATH_ID` (and the WES PathCatalogue
  to contain that path) before approvals are accepted; the Helm values /
  warehouse-infra `local.services` entry must set it.
