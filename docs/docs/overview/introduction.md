---
id: introduction
title: Introduction
sidebar_label: Introduction
description: What the network-inventory-planning bounded context owns, how it is classified, and where to read next.
---

# Network Inventory Planning

`network-inventory-planning` (NIP) is the bounded context that decides
**whether stock should move between warehouses** and then carries an approved
inter-warehouse transfer from the origin reservation to the destination stow.
It recommends and coordinates; it never becomes the inventory ledger and never
moves physical stock itself
([ADR 0001](../adr/0001-network-inventory-planning-boundary.md)).
`inventory-storage` stays authoritative for stock and reservations, and the
WES contexts stay authoritative for executable work.

| Fact | Value | Source |
| --- | --- | --- |
| Tier | **WES** (CloudEvents `type` segment `wes`) | `internal/adapters/kafka/cloudevents/cloudevents.go` (`Subdomain = "wes"`) |
| Subdomain class | **Core** | [Subdomain classification](../ddd/subdomain-classification.md) |
| Event `type` prefix | `com.warehouse.wes.network-inventory-planning.` | `cloudevents.Type()` |
| CloudEvents `source` | `/warehouse/network-inventory-planning` | `cloudevents.Source` |
| Own Kafka topics | `warehouse.network-inventory-planning.events` (integration) and `warehouse.network-inventory-planning.analytics` (saga health) | `internal/adapters/outbound/kafka/publisher.go`, `analytics_publisher.go` |
| Binaries | `network-inventory-planning` (API, consumers, outbox relay, tickers), `mcp`, `nip-projector`, `nip-reports` | `cmd/*/main.go` |
| Edge | Kong `:8000` under `/api/network-inventory-planning`; the console remote `nip_mfe` is served by Nginx `:80` | ADR 0010; the route itself is supplied by warehouse-infra through the chart's disabled-by-default `gatewayApi` / `ingress` values |
| Authentication | None. REST and MCP are unauthenticated fleet-wide; access control is the cluster boundary | `internal/architecture/fitness_test.go` (`TestNoAuthMiddlewareReintroduced`) |

## What it owns

- **Advisory transfer proposals.** `POST /v1/transfer-proposals:generate`
  runs the deterministic `transfer.Planner` over an explicit, coherent snapshot
  (positions, policies, lanes) supplied by the caller. A proposal keeps the
  origin's safety stock, never exceeds the destination's deficit, only uses an
  enabled directed lane and carries reason codes plus a score breakdown.
- **Three local read models** fed by Kafka: `site_capability`
  (facility-layout's `SiteCapabilityChanged`), `site_sku_demand`
  (order-management's `SiteSkuDemandChanged`) and `published_capacity_plan`
  (warehouse-planning's `CapacityPlanPublished`).
- **The fail-closed planning snapshot** (`planning.BuildSnapshot`): a site
  participates only with all three facts fresh within
  `PLANNING_MAX_STALENESS`; stale or empty inputs refuse instead of
  zero-filling. It backs the advisory simulation
  (`GET /v1/transfer-simulations`), approval validation and the scheduled
  rebalance pass.
- **The `InterWarehouseTransfer` saga** (11 states, `DRAFT` to `RECEIVED`),
  started only by an operator's `POST /v1/transfers:approve` with an
  `Idempotency-Key`. It emits `TransferAllocationRequested` to
  inventory-storage, releases pick and dispatch `WorkDemandReleased` commands
  to wes-work-planning, and follows fulfillment-execution's and
  inventory-storage's facts to the destination. Every event goes through a
  transactional outbox.
- **Saga-health observation**: a stuck-transfer check and an observe-only
  scheduled rebalance publish analytics occurrences; `nip-projector` folds them
  into a separate analytical database that `nip-reports` serves read-only.
- **A read-only MCP server** with four tools for AI clients
  (warehouse-ops-agent uses all four).

## What it does not own

- Stock quantities, reservations and stow locations (inventory-storage).
- Work units, waves and task execution (wes-work-planning,
  fulfillment-execution).
- Site capabilities, customer demand and capacity plans (facility-layout,
  order-management, warehouse-planning): NIP keeps local copies only.
- Forecasting, route optimisation and auto-approval: not built.

## Read next

| If you want to | Read |
| --- | --- |
| See the processes, ports and data stores | [Architecture](./architecture.md) |
| Run it on your machine | [Quickstart](./quickstart.md) |
| Configure a binary | [Configuration](../operations/configuration.md) |
| Deploy and operate it | [Runbook](../operations/runbook.md) |
| Watch it | [Observability](../operations/observability.md) |
| Debug a symptom | [Troubleshooting](../operations/troubleshooting.md) |
| Run or extend the tests | [Testing](../development/testing.md) |
| Know who it talks to | [Integration](../ecosystem/integration.md) |
| Call it from an AI client | [MCP tools](../mcp/tools.md) |
| Understand the model | [Use cases](../ddd/use-cases.md), [Domain events](../ddd/domain-events.md), [Ubiquitous language](../ddd/ubiquitous-language.md) |
| Read the decisions | [ADR index](../adr/about.md) |
| Call the REST API | [API reference](../api-reference/overview.md) |
