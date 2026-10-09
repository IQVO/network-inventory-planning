---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
description: network-inventory-planning is a Core subdomain in the WES tier; the reasoning, and how its neighbours are classified.
---

# Subdomain classification

| Context | Classification | CloudEvents tier |
| --- | --- | --- |
| `network-inventory-planning` | **Core** | `wes` |

## Why Core

- **It is where a network decision is made.** No other context relates stock
  at one site to demand and capacity at another; that decision avoids
  stockouts and expedited freight, which is competitive advantage for a
  multi-site operator.
- **It must be reproducible.** Every proposal carries its policy version,
  reason codes (`DESTINATION_BELOW_TARGET`, `ORIGIN_ABOVE_SAFETY_STOCK`,
  `APPROVED_LANE`) and a score breakdown; approval re-validates against the
  current facts and refuses on stale ones. No off-the-shelf product would fit
  the fleet's fail-closed, event-carried facts.
- **It coordinates without owning.** The saga drives inventory-storage and the
  WES contexts through commands and facts, never by sharing their data, which
  is custom work tied to this fleet's contracts.
- **What would lower it:** if transfers were planned by a bought network
  planning tool behind an Anti-Corruption Layer, this context would shrink to
  a Supporting saga coordinator.

The fleet verdict (warehouse-docs `docs/strategic-design/subdomain-classification.md`
on `main`) is the same: Core, `wes`, "Cost reduction / service level",
"Custom-built, early".

## Tier

The CloudEvents `type` segment is `wes`
(`internal/adapters/kafka/cloudevents/cloudevents.go`, `Subdomain = "wes"`),
so every type NIP publishes starts with
`com.warehouse.wes.network-inventory-planning.`. The tier is not the
classification: it says which layer of the platform the context belongs to.

## Neighbours

Copied from the fleet table (warehouse-docs `origin/main`), not re-derived here:

| Neighbour | Relationship to NIP | Classification | Tier |
| --- | --- | --- | --- |
| `inventory-storage` | receives `TransferAllocationRequested`; sends allocation replies and destination facts | Core | `wms` |
| `wes-work-planning` | receives `WorkDemandReleased` | Core | `wes` |
| `fulfillment-execution` | sends `TransferPicked`, `TransferDispatched`, `TransferArrived` | Core | `wes` |
| `warehouse-planning` | sends `CapacityPlanPublished` | Core | `wes` |
| `order-management` | sends `SiteSkuDemandChanged` | Generic/Supporting | `wes` |
| `facility-layout` | sends `SiteCapabilityChanged` | Generic | `wms` |
| `warehouse-ops-agent` | calls the read-only MCP tools | Supporting | `wes` |
| `process-path-management` | indirect only: the configured pick and dispatch path ids must exist in its catalogue | Generic | `wes` |
