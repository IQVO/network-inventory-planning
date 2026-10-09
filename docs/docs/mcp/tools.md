---
id: tools
title: MCP tools
sidebar_label: MCP tools
description: The read-only MCP server of network-inventory-planning - transport, every tool, its inputs and its outputs.
---

# MCP tools

`cmd/mcp` serves a Model Context Protocol server named
`network-inventory-planning-mcp` (version `1.0.0`) over **Streamable HTTP
only**, at both `/` and `/mcp` on `MCP_ADDR` (default `:8090`), plus
`GET /healthz`. It is built on the official MCP Go SDK
(`internal/adapters/inbound/mcp`). There is no authentication: the ClusterIP
boundary is the access control.

The server reuses the REST read side's use cases over the OLTP database. It
never starts the outbox relay, never dials Kafka and has **no write tool**:
approving a transfer stays an operator action on the REST API (ADR 0008).
Every tool carries the `readOnlyHint: true` annotation.

Inputs below are the JSON field names of the tool input schemas, checked
against a live `tools/list` of `cmd/mcp` built from this branch. Required
inputs are `transfer_id` (`get_transfer`) and `older_than_minutes`
(`find_stuck_transfers`); every other field is optional, and every input
schema sets `additionalProperties: false`. Each tool is annotated
`readOnlyHint: true`, `idempotentHint: false`, and publishes an output schema.

| Tool | Read-only | Inputs | Output |
| --- | --- | --- | --- |
| `get_transfer` | yes | `transfer_id` (string, required) | the transfer view plus `audit[]` of `{seq, from, to, event, cause, occurred_at}` |
| `list_transfers` | yes | `state` (one of the 11 states), `site` (matches origin **or** destination), `limit` (1–200, default 50) | `{transfers[], total}`, newest first; `total` counts matches before paging |
| `find_stuck_transfers` | yes | `older_than_minutes` (integer, required, positive), `state` (a non-terminal state), `limit` (1–200, default 50) | `{transfers[], total}`, stalest first; `RECEIVED`, `UNFULFILLABLE` and `CANCELLED` are never reported |
| `simulate_transfer_options` | yes | none | `{advisory, as_of, sites[]}` with per site `site`, `origin_enabled`, `destination_enabled`, `total_demand`, `capacity_over_window`, `capacity_headroom` (negative = short), `window_start`, `window_end` |

The transfer view has `id`, `state`, `sku`, `quantity`, `picked_quantity`
(omitted until picked), `origin_site_id`, `destination_site_id`,
`policy_version`, `reservation_id` (omitted until allocated),
`rejection_reason` (omitted unless unfulfillable), `expires_at`, `created_at`,
`updated_at` and `version`. Timestamps are UTC RFC 3339. State names are
accepted case-insensitively.

## Errors

A failing tool returns an `isError` result whose text is `<slug>: <detail>`,
with the same slugs the REST API uses:

| Slug | When |
| --- | --- |
| `invalid-query` | unknown state, terminal state for `find_stuck_transfers`, `limit` out of range, non-positive `older_than_minutes`, blank `transfer_id` |
| `transfer-not-found` | `get_transfer` with an unknown id |
| `read-side-unavailable` | the server runs without `DATABASE_URL` (transfer tools) |
| `read-models-unavailable` | the server runs without `DATABASE_URL` (`simulate_transfer_options`) |
| `read-models-incomplete` | the fail-closed snapshot refused (stale, missing or disabled facts) |
| `internal-error` | anything untyped; the cause is logged, never returned |

## Calling it

```bash
curl -s -X POST http://localhost:8090/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' -i
```

Take the `Mcp-Session-Id` response header, send
`notifications/initialized`, then `tools/list` or
`tools/call` with that header. The server's instructions text tells a model
which tool answers which question.

## Who uses it

warehouse-ops-agent's NIP MCP client
(`internal/adapters/outbound/mcpclient/network_inventory_planning.go` in that
repo) calls all four tools for its transfer watch.
