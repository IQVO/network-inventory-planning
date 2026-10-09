---
id: overview
title: API reference
sidebar_label: Overview
description: The REST surfaces of network-inventory-planning - which binary serves which route, the error model and how the reference is generated.
---

# API reference

The operation pages under this section are generated from `apis/openapi.yaml`
by `docusaurus-plugin-openapi-docs` (`npm run clean-api-docs && npm run gen-api-docs`
in `docs/`). This page maps every route to the binary that actually serves it,
read from the routers in code.

## `network-inventory-planning` (port 8080, Kong `/api/network-inventory-planning`)

Source: `internal/adapters/inbound/http/handler.go` (`Handler.Routes`).

| Method | Path | Purpose | Needs |
| --- | --- | --- | --- |
| `GET` | `/healthz` | liveness; `204 No Content` | nothing |
| `POST` | `/v1/transfer-proposals:generate` | advisory proposals from an explicit snapshot in the body (`asOf`, `positions`, `policies`, `lanes`) | nothing |
| `GET` | `/v1/transfer-simulations` | advisory per-site simulation from the local read models | `DATABASE_URL` |
| `POST` | `/v1/transfers:approve` | operator approval: starts the transfer saga; `Idempotency-Key` header required | `DATABASE_URL`, `KAFKA_BROKERS`, `TRANSFER_PICK_PATH_ID` |
| `GET` | `/v1/transfers` | list transfers, newest first; `state`, `originSiteId`, `destinationSiteId`, `limit` (1–200, default 50), `offset` | `DATABASE_URL` |
| `GET` | `/v1/transfers/{id}` | one transfer with its audit trail | `DATABASE_URL` |
| `GET` | `/v1/rebalance-runs` | scheduled-rebalance history, newest first; `limit` 1–200, default 50 | `DATABASE_URL` |

The two colon routes (`/v1/transfer-proposals:generate`,
`/v1/transfers:approve`) are Google-style custom methods; they are in the
OpenAPI spec as written.

The approval response is `{transferId, state, originSiteId, destinationSiteId,
sku, quantity, policyVersion, reservationId?, rejectionReason?, replayed,
expiresAt, transferLineId}`; a replay of the same key and body answers `200`
with `replayed: true`.

## `nip-reports` (port 8092, cluster-internal)

Source: `internal/adapters/inbound/http/reports_handler.go` (`ReportsServer.Routes`).
`GET /healthz`, `/reports/transfer-funnel`, `/reports/state-dwell`,
`/reports/stuck-transfers`, `/reports/rebalance-runs`, `/reports/freshness`.
They are in the same OpenAPI document (tag `reports`), but they are **not**
served by the API process and are not routed by Kong. Query parameters and
response shapes are listed in the [Runbook](../operations/runbook.md#analytics-reports).

## `mcp` (port 8090)

`/` and `/mcp` are mounts of the MCP Streamable HTTP handler, not REST
routes; `GET /healthz` answers `{"status":"ok"}`. See [MCP tools](../mcp/tools.md).

## `nip-projector` (port 8091, admin only)

`/healthz` (`{"status":"ok"}`) and `/readyz` (`{"status":"ready"}`, or
`503 {"status":"not_ready"}` while shutting down). Not in the OpenAPI document.

## Error model

Every error is RFC 7807 `application/problem+json` with
`type = https://warehouse.example/problems/<slug>`, `title`, `status` and
`detail`. The full slug list is on
[Troubleshooting](../operations/troubleshooting.md#problem-types).

There is no authentication on any route.
