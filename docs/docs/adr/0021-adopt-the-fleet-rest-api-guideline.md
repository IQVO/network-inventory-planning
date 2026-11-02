# ADR-0021: Adopt the fleet REST API guideline

Status: Accepted (2026-11-02)

Applies the fleet-wide REST API guideline (warehouse-docs "REST API Guideline",
rules R1-R8, enforced by the Spectral ruleset in `.spectral.yaml`). Respects the
real Postgres and Kafka runtime. No event type changes, no consumed-contract
change. Breaking on the REST surface: it deploys together with the rest of the
REST cutover set.

## Context

The fleet measured divergent REST conventions: custom-method routes
(`:verb`), keyed and unkeyed writes side by side, key reuse answered 422,
two spellings of the replay header, and unpaginated lists. This context had
two `:verb` routes, one unkeyed `POST`, and five collection `GET`s without
keyset paging or with `offset` paging.

## Decision

### Routes (R1, R2)

| Old | New |
| --- | --- |
| `POST /v1/transfer-proposals:generate` | `POST /v1/transfer-proposal-runs` (job noun) |
| `POST /v1/transfers:approve` | `POST /v1/transfer-approvals` (collection-level approval, body keeps the transfer fields) |

`operationId`s are unchanged, so the MCP and A2A mappings do not move. Status
codes and bodies are unchanged. `TestRetiredRoutesAreGone` pins the old routes
to 404/405. There is no alias and no dual route.

### Idempotency (R3-R6, R8)

- Every `POST` (the only write verb besides `PUT`) sits behind one route-scoped
  middleware (`internal/adapters/inbound/http/idempotency.go`), including the
  previously unkeyed `POST /v1/transfer-proposal-runs`.
- The `Idempotency-Key` header is 1-128 printable ASCII characters: missing is
  400 `idempotency-key-required`, malformed is 400 `idempotency-key-invalid`.
- The key is global (one row per key in `idempotency_keys`, migration 0008),
  the fingerprint is method + path + SHA-256 of the body. Same key and
  fingerprint replays the stored answer with `Idempotency-Replayed: true`;
  a different fingerprint is **409** `idempotency-key-reused` (was 409
  `idempotency-conflict` on approval only).
- The key row commits in the SAME transaction as the aggregate change and its
  outbox rows. 2xx and 4xx answers are stored; a 5xx or panic rolls back and is
  never stored.
- The approval response body no longer carries `replayed`: a replay is
  byte-identical to the first answer, so the signal is the header only. The
  transfer-level idempotency key of the saga (ADR-0003) is the same
  `Idempotency-Key`.
- `PUT` (catalogue upserts, optimistic concurrency by `If-Match`) is idempotent
  by method and takes no middleware.

### Instance without a database

`DATABASE_URL` is mandatory at boot, so a store-less instance exists only when
the handler is constructed without one (tests). Rule 2.1 allows no unkeyed
`POST`, therefore such an instance **refuses every `POST` with 503
`idempotency-unavailable`** (fail closed) instead of running it unkeyed. The
side effect: proposal generation, a pure computation, is also refused there.
The BDD scenarios pin this.

### Pagination (R7)

`limit` (1-500, default 100) and `cursor` (opaque keyset cursor, base64url
JSON of last sort key and id, ordered by sort key then id) on:
`GET /v1/transfers`, `/v1/rebalance-runs`, `/v1/rebalance-policies`,
`/v1/transfer-lanes`. The answer is `{items, nextCursor}`; `nextCursor` is
absent on the last page. 400 `invalid-limit`, `invalid-cursor`, and
`unknown-query-parameter` (so a consumer still sending `offset`, `after` or
`nextToken` fails loudly). `GET /v1/transfers` lost `total`, `limit` and
`offset`. Both the Postgres and the in-memory stores implement the keyset.

Exemption: `GET /v1/transfer-simulations` is `x-pagination-exempt: view`: one
composite advisory view (per-site capacity versus demand) that merely contains
an array, with no ids to page on. The analytics `/reports/*` endpoints are
window-bounded reports served by `nip-reports` with their own spec.

The read-only MCP tool `list_transfers` and the A2A skills keep their own
tool/skill schemas (bounded `limit`, ADR-0008, ADR-0020); they read the same
use cases.

### Consumers changed

The web remote (`web/src`): new routes, `Idempotency-Key` on both `POST`s,
lists read from `items`, lanes and policies follow `nextCursor` until absent,
the transfers screen pages by cursor, and "replayed" is derived from the
`Idempotency-Replayed` response header. No other service is called by this
context over HTTP.

## Consequences

- Clients of the two renamed routes, the list envelopes and the 409 problem
  type must update in the same release.
- Every `POST` now writes one `idempotency_keys` row; retention is at least 24
  hours (a sweep by `created_at` is future work, the index exists).
- The approval and run results are replayed from the stored bytes, not
  recomputed: a replay after the saga advanced returns the first answer;
  `GET /v1/transfers/{id}` is the way to read the current state.
