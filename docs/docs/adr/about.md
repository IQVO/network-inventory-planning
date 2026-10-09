---
id: about
title: Architecture Decision Records
sidebar_label: ADR index
description: Every architecture decision record of network-inventory-planning with its number, title and status.
---

# Architecture Decision Records

The ADRs live in `docs/docs/adr/`. Each body is immutable once accepted; a
change of mind is a new ADR that supersedes the old one. The status column is
the `Status:` line of each ADR body.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](./0001-network-inventory-planning-boundary.md) | Network Inventory Planning Boundary | Accepted |
| [0002](./0002-phase1-read-models-and-simulation.md) | Phase-1 local read models, fail-closed planning snapshot and advisory simulation | Accepted |
| [0003](./0003-transfer-saga.md) | Phase-2 inter-warehouse transfer saga (approval, outbox, allocation replies) | Accepted |
| [0004](./0004-cloudevents-type-catalogue.md) | CloudEvents type catalogue for network-inventory-planning | Accepted |
| [0005](./0005-work-demand-release-and-fact-transitions.md) | Work-demand release and fact-driven saga transitions | Accepted |
| [0006](./0006-migrations-over-a-direct-connection.md) | Migrations over a direct connection; chart routing | Accepted |
| [0007](./0007-phase4-observability-and-scheduled-runs.md) | Phase-4 observability — OTel propagation, saga-health analytics, scheduled rebalance runs | Accepted |
| [0008](./0008-transfer-read-side-and-read-only-mcp.md) | Transfer read side and a read-only MCP server | Accepted |
| [0009](./0009-analytics-read-side.md) | Analytics read side — a projector and read-only reports over a separate analytical database | Accepted |
| [0010](./0010-console-remote-nip-mfe.md) | Console remote `nip_mfe` (Module Federation) | Accepted |
| [0011](./0011-godog-bdd-acceptance-tests.md) | godog/Gherkin acceptance tests as executable specification | Accepted |
| [0012](./0012-otlp-telemetry-export.md) | OTLP trace and metric export for the API process | Accepted |

## Where each decision shows up in the code

| ADR | Code |
| --- | --- |
| 0001 | `internal/domain/transfer` (advisory planner), no inventory ledger anywhere |
| 0002 | `internal/domain/planning`, the three read-model consumers, `GET /v1/transfer-simulations` |
| 0003 | `InterWarehouseTransfer`, `ApproveTransfer`, `outbox_events` and the relay, the transfer-reply consumer |
| 0004 | `internal/adapters/kafka/cloudevents`, `internal/architecture/catalogue_fitness_test.go` |
| 0005 | `WorkDemandReleased`, `TRANSFER_*_PATH_ID` / `_CPT_OFFSET`, the transfer-fact consumer |
| 0006 | `MIGRATIONS_DATABASE_URL`, the chart's `gatewayApi` / `ingress` values |
| 0007 | trace propagation through Kafka headers, the analytics encoder, `NIP_HEALTH_CHECK_INTERVAL`, `NIP_STUCK_THRESHOLDS`, `NIP_REBALANCE_SCHEDULE`, `rebalance_runs` |
| 0008 | `GET /v1/transfers`, `GET /v1/transfers/{id}`, `cmd/mcp` |
| 0009 | `cmd/nip-projector`, `cmd/nip-reports`, `analytics/migrations` |
| 0010 | `web/` and the chart's `frontend` component |
| 0011 | `features/*.feature`, `features_*_test.go` |
| 0012 | `internal/adapters/outbound/telemetry`, `OTEL_EXPORTER_OTLP_ENDPOINT` |
