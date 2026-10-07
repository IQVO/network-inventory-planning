---
paths:
  - "internal/adapters/inbound/mcp/**"
  - "cmd/mcp/**"
---

# MCP server (inbound adapter)

One MCP server for this bounded context, an additive inbound adapter (the
fleet's "MCP servers are additive inbound adapters" decision; short rule in
`.claude/rules/fleet/no-auth-and-mcp.md`; this repo's record is
`docs/docs/adr/0008-transfer-read-side-and-read-only-mcp.md`) over the SAME use
cases the REST adapter calls.

- Code: `internal/adapters/inbound/mcp/` (tools, error mapping) and the
  composition root `cmd/mcp/` (env, adapters, router, graceful shutdown). Built
  on the official SDK `github.com/modelcontextprotocol/go-sdk`.
- Transport: **Streamable HTTP only** (no stdio, no SSE). `MCP_ADDR` (default
  `:8090`), served at **`/` and `/mcp`**; `GET /healthz` -> `200 {"status":"ok"}`.
- **No auth of any kind** (fleet-wide revert 2026-09-11): no keys, no bearer
  checks. Access control is the in-cluster ClusterIP boundary.
- Architecture: the adapter depends ONLY on `internal/application` and
  `internal/domain`; nothing depends on it (`TestMCPAdapterDependencyRule`).
- Env (`cmd/mcp`): `MCP_ADDR`, `DATABASE_URL` (unset -> every tool answers
  `read-side-unavailable`, like REST 503), `MIGRATIONS_DATABASE_URL` (direct DSN
  for the migration step only), `MIGRATIONS_PATH`, `PLANNING_MAX_STALENESS`,
  `LOG_LEVEL`. It runs the idempotent migrations on start. It starts NO outbox
  relay, NO consumer and never dials Kafka.

## Tools (4, all READ-ONLY; budget 6)

Arguments are snake_case. Timestamps are RFC3339. Failures are MCP tool errors
(`isError: true`) whose text is `<slug>: <message>`, using the REST problem
slugs (`transfer-not-found`, `invalid-query`, `read-side-unavailable`,
`read-models-unavailable`, `read-models-incomplete`); unexpected infrastructure
errors are logged and reported as a generic `internal-error`.

| Tool | Backed by | Arguments | Result |
|---|---|---|---|
| `get_transfer` | `GetTransfer` | `transfer_id` | the transfer + `audit[]` (`seq`, `from`, `to`, `event`, `cause`, `occurred_at`) |
| `list_transfers` | `ListTransfers` | optional `state`, `site` (origin OR destination), `limit` (<= 200) | `transfers[]`, `total` |
| `find_stuck_transfers` | `FindStuckTransfers` | `older_than_minutes` (> 0), optional non-terminal `state`, `limit` | `transfers[]` stalest first, `total` |
| `simulate_transfer_options` | `SimulateTransferOptions` | none | `advisory`, `as_of`, `sites[]`; fail-closed |

There is NO write tool: approving a transfer stays REST/operator-only.
`TestToolSurface` pins the exact set, naming, `ReadOnlyHint` on every tool,
descriptions and documented snake_case arguments, and rejects write verbs.

## Adding a tool

Add a typed input struct (snake_case `json` tags + `jsonschema:"..."` on every
field), a `Deps` method calling an existing use case, register it in
`registerTools` with the read-only annotation, map errors through `mapError`,
and extend `wantTools` in `TestToolSurface`. A WRITE tool needs a new ADR and an
auth-model decision first.
