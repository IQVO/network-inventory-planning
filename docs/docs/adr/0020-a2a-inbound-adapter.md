# ADR-0020: A2A inbound adapter (third driving adapter, read-only)

Status: Accepted (2026-10-29)

Mirrors the reference rollout in
[wes-work-planning ADR-0046](https://iqvo.github.io/wes-work-planning/docs/adr/0046-a2a-inbound-adapter)
and follows [ADR-0008](./0008-transfer-read-side-and-read-only-mcp.md) (the
read-only MCP adapter this one is modelled on). Respects the real Postgres and
Kafka runtime. No wire change, no migration, no new event type.

## Context

The fleet is adopting the Agent2Agent (A2A) protocol so that a bounded context
can **delegate** a goal to another context's agent. This context already
exposes its transfer read side over REST and a read-only MCP server (ADR-0008).
The two questions a delegating agent would ask of it are exactly the ones the
saga's health depends on: *where does this transfer stand and how did it get
there?* and *which transfers have stopped advancing?*

Three transports now carry three different things and must not overlap:

| Transport | Carries |
| --- | --- |
| Kafka + CloudEvents | **facts** between contexts |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
read use cases (`GetTransfer`, `FindStuckTransfers`) as the REST and MCP
adapters, built on the official `a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and
served by a new `cmd/a2a` binary (JSON-RPC 2.0 over HTTP, port **8093**, the
Agent Card at `/.well-known/agent-card.json`, the endpoint mounted at `/` and
`/a2a`, `GET /healthz`). `cmd/a2a` is a clone of `cmd/mcp`'s composition.

### The agent is read-only (write gate)

An A2A skill may change state only if the context's MCP surface already offers
that write. The MCP charter of this context is read-only (ADR-0008: approving
a transfer moves origin stock through inventory-storage and stays an operator
action on the REST API until an auth-model decision exists), so **the A2A agent
is read-only too**. Delegation never widens what the context exposes:

- no skill approves, cancels or retries a transfer;
- `Deps` carries only the two read use cases, pinned by
  `TestA2ADepsCarryNoWriteUseCase`;
- `cmd/a2a` starts no outbox relay and no consumer, never dials Kafka and
  writes no outbox row (`TestA2ABinaryIsReadOnlyWiring`; the integration test
  proves the outbox row count does not move across the skill calls).

### Two curated, goal-shaped skills, derived from the MCP tools

| Skill | Kind | Derived from | What it does |
| --- | --- | --- | --- |
| `transfer_status` | read | `get_transfer` | One transfer by `transferId`: state, sku, quantity, sites, reservation, rejection reason and the audit trail of state transitions. |
| `find_stuck_transfers` | read | `find_stuck_transfers` | Non-terminal transfers whose state has not changed for MORE than `olderThanMinutes`, stalest first, with the total before paging; optional `state` and `limit` (1 to 200, default 50). |

`list_transfers` and `simulate_transfer_options` stay MCP-only: the curation
discipline of ADR-0008 applies (goal-shaped, at most two skills, never one per
endpoint). The staleness rule lives in the use case, not in the adapter.

### Message-only, answered in-band

Both skills are transactional request/response reads, so the agent is
message-only (no Task lifecycle). Request-shape failures and domain rejections
are **answered** as RFC 7807-shaped `problem`s inside the message envelope,
never raised as JSON-RPC errors, with the **same classification as the REST
read handlers** (`writeReadProblem`) and the same problem `type` URIs:

| Status | Problem | When |
| --- | --- | --- |
| 400 | `invalid-query` | the use case refuses the query (unknown or terminal `state`) |
| 400 | `invalid-argument`, `malformed-skill-request`, `unknown-skill` | adapter-level request shape |
| 404 | `transfer-not-found` | unknown transfer id |
| 503 | `read-side-unavailable` | no store wired, or the store could not be read (the infrastructure detail is logged, never echoed) |

Reads have no 409 or 500 path: REST answers 503 for an unrecognised read error
and so does this adapter.

### Strict input, adapter-owned wording

- Unknown argument names are **rejected**, not ignored (`DisallowUnknownFields`
  plus no trailing data): a misspelled `olderThanMinute` must not silently drop
  the threshold. Go's `encoding/json` matches names case-insensitively, so only
  genuinely unknown names are refused.
- The surface takes camelCase names (`transferId`, `olderThanMinutes`). The MCP
  snake_case names are refused as unknown arguments. The shared use case words
  its refusal with the MCP name (`older_than must be positive`), which this
  surface would then refuse; so the adapter validates first (positive and at
  most ten years, `limit` 1 to 200) and owns the detail text, and a test pins
  that no answer part mentions `older_than` or `transfer_id`. The bound on
  `olderThanMinutes` also keeps the minutes-to-duration conversion from
  overflowing into a negative threshold.

### Facts still travel on Kafka

A2A never becomes a data-sync channel. Nothing here publishes; the saga's
facts keep leaving the api binary as CloudEvents through the transactional
outbox.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP (revert of 2026-09-11,
`TestNoAuthMiddlewareReintroduced`). Schemes are added to the card, not a
redesign, if that decision is ever reversed.

### Boundaries

- `internal/adapters/inbound/a2a` depends only on the application and domain
  layers, and nothing depends on it (`TestA2AAdapterDependencyRule`).
- This context is an A2A **server only**: no outbound A2A client is added
  (`TestNoOutboundA2AClient`).
- Observability: one OTel span `a2a.skill <id>` per skill run with the
  `a2a.skill` attribute (error status on a problem answer), plus the Tier-1 HTTP
  RED middleware of `cmd/mcp`.

## Consequences

- A fourth deployable (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, `component=a2a`, default **off**), exposed through Kong at
  `/api/a2a/network-inventory-planning` rewritten to `/` so the well-known card
  stays reachable under the prefix.
- `cmd/a2a` requires `DATABASE_URL` and `A2A_AGENT_URL` (an absolute http(s)
  URL; the card advertises it, so a relative or schemeless value is refused at
  boot). The chart defaults `A2A_AGENT_URL` to the in-cluster Service DNS and
  `a2a.agentUrl` overrides it. Unlike `cmd/mcp`, a missing `DATABASE_URL` is a
  boot error, not a store-less mode: the runtime rule is real Postgres only.
- It needs no Kafka and no `PLANNING_MAX_STALENESS` (there is no simulation
  skill), so the chart renders neither `KAFKA_BROKERS`, `EVENT_PUBLISHER` nor the
  consumer groups for it (asserted by the chart tests).
- Port 8093 is the fleet's A2A port (8090 is this chart's MCP server, 8091 the
  projector admin port, 8092 the reports API).
- Adding a write skill later (for example approving a transfer) needs its own
  ADR: the MCP charter and an auth-model decision come first, and the arch test
  on `Deps` is the sensor that fails until then.
