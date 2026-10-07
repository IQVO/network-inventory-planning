# ADR-0007: Transfer read side and a read-only MCP server

Status: Accepted

## Context

The transfer saga (ADR-0003, ADR-0005) was write-only from the outside: the
only transfer endpoint was `POST /v1/transfers:approve`, and the persistence
port (`ports.TransferRepository`) only offered `Create` / `Load` /
`UpdateState` for the saga itself. Operators, the console and the ops-agent
could not ask "what is the status of transfer X?" or "which transfers are
stuck?" — the second question matters in practice, because the saga is driven
by replies and facts arriving over Kafka and a lost reply leaves a transfer
parked in `ALLOCATING` (or `IN_TRANSIT`) with nothing to say so.

Sibling bounded contexts expose their use cases to agents through a separate,
additive MCP server (warehouse-systems ADR-0008; reference implementation:
warehouse-planning `cmd/mcp`). This context had none.

## Decision

### Read side (hexagonal, additive)

- A NEW query port, `ports.TransferQuery` (`Get`, `List`), implemented by
  `postgres.TransferQueryRepo`. `ports.TransferRepository` is NOT widened:
  the write/rehydrate port and the read port stay separate, and the read
  adapter issues `SELECT`s only and never joins a `UnitOfWork`.
- Three use cases over it: `GetTransfer`, `ListTransfers`,
  `FindStuckTransfers`. Validation (closed state vocabulary, paging bounds,
  positive threshold) lives in the use cases, shared by both inbound
  adapters.
- "Stuck" is defined in the use-case layer, not in an adapter or a tool: a
  transfer is stuck when its state has not changed for **strictly longer**
  than the threshold (`updated_at`, which every transition sets) and it is in
  a **non-terminal** state. `RECEIVED`, `UNFULFILLABLE` and `CANCELLED` are
  terminal (`TransferState.IsTerminal`) and are never reported, however old.
  Results are ordered stalest first.
- REST: `GET /v1/transfers` (`state`, `originSiteId`, `destinationSiteId`,
  `limit` ≤ 200, `offset`; newest first; `{items,total,limit,offset}`) and
  `GET /v1/transfers/{id}` (the transfer plus its audit trail: sequence,
  from/to state, event, cause, timestamp). Errors use the existing
  `problem+json` shape: 400 for a bad query, 404 for an unknown id, 503 when
  the instance runs without `DATABASE_URL` (nil use case) or the store cannot
  be read — the detail of an infrastructure failure is not echoed.
  `apis/openapi.yaml` documents both; the AsyncAPI is untouched.

### Read-only MCP server

- A separate `cmd/mcp` binary (official MCP Go SDK, Streamable HTTP only,
  `:8090` at `/` and `/mcp`, open `GET /healthz`, unauthenticated), wired by
  the same use cases as REST through a new inbound adapter
  `internal/adapters/inbound/mcp`. It starts no outbox relay, no consumer and
  never dials Kafka; it runs the idempotent migrations on start (like the
  api) and reads `PLANNING_MAX_STALENESS` for the simulation.
- Curated, intent-level, **read-only** tools: `get_transfer`,
  `list_transfers`, `find_stuck_transfers`, `simulate_transfer_options`.
  Tool errors follow the fleet's slug-prefixed convention (`<slug>: <detail>`,
  slugs shared with the REST problem types); infrastructure errors are logged
  and reported as a generic `internal-error`.
- A governance test pins the exact tool set, the tool budget, snake_case
  naming, descriptions, argument documentation and `ReadOnlyHint=true` on
  every tool, and rejects any tool whose name starts with a write verb.
- Chart: opt-in `mcp.enabled` (default `false`) renders a second Deployment
  and Service (`component=mcp`, command `/app/mcp`); the api Deployment and
  Service pin `component=api`, so each Service selects exactly one
  Deployment (`tests/test_service_selectors.py`). The image builds both
  binaries and exposes 8080 and 8090.

### Why REST and MCP do not mutate in this phase

- Approval is an operator decision with money-shaped consequences (it
  reserves origin stock through inventory-storage). It already has a safe,
  idempotency-keyed REST endpoint; handing the same action to an agent over an
  unauthenticated MCP endpoint (auth is deliberately absent fleet-wide since
  2026-09-11) would let any in-cluster caller move stock. The MCP surface
  therefore stays read-only until an auth-model decision exists.
- There is nothing to mutate on the read side: transfers advance only through
  the saga (replies and facts). A "retry" or "cancel stuck transfer" tool is a
  separate decision (cancel is legal only before the origin reservation
  exists, and a held reservation needs the explicit revocation path of a later
  phase); this ADR gives operators the visibility to make that decision.

## Consequences

- Operators and agents can see a transfer's state and full audit trail and
  find the ones that stopped advancing, without database access.
- The read adapter's queries are plain indexed `SELECT`s (`idx_transfer_state`
  exists; no new migration). `updated_at` is not indexed: at the current table
  size the stuck query is a filtered scan; add a `(state, updated_at)` index if
  the table grows large.
- The `find_stuck_transfers` threshold is an operator-chosen number of
  minutes; there is no per-state SLA table yet. Per-state thresholds, alerting
  on stuck transfers and any write tool each need their own decision.
- The api and mcp images share one build; MCP can be enabled per environment
  without touching the api Deployment.
