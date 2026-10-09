---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
description: Build, test and run network-inventory-planning locally, seed its read models and make the first calls.
---

# Quickstart

## Prerequisites

| Tool | Why | Version source |
| --- | --- | --- |
| Go | build and test | `go.mod` (the Dockerfile builds with `golang:1.27-alpine`) |
| Docker | a local Postgres, the container image | any recent |
| `golangci-lint` | `make lint` / `make check` | pinned `v2.14.0` in `Makefile` and CI |
| `gremlins` (optional) | `make mutation` | pinned `v0.6.0` |
| `govulncheck` (optional) | `make vuln` | latest |
| `psql` (optional) | seeding the read models below | any |
| Node 20 (optional) | this docs site (`docs/`) | `.github/workflows/docs.yml` |

## Build and test

All targets live in the `Makefile` (`make help` lists them):

```bash
make build        # go build ./...
make test         # go test ./... -race  (unit + httptest + BDD, no database)
make check        # fmt-check vet build lint test   (the pre-commit gate)
make check-all    # check + coverage (90% gate) + arch-test + bdd
make integration  # go test -tags=integration ./... -race -count=1  (needs Postgres)
make image        # docker build -t claudioed/network-inventory-planning:dev .
```

There is no `make run`; run the binaries with `go run` as below.

## 1. The zero-config mode

With no environment at all the API boots without a database and serves only
the health check and the explicit-snapshot proposal generator:

```bash
go run ./cmd/network-inventory-planning
curl -i http://localhost:8080/healthz          # 204 No Content
```

```bash
curl -s -X POST http://localhost:8080/v1/transfer-proposals:generate \
  -H 'content-type: application/json' \
  --data '{
    "asOf":"2026-10-05T15:00:00Z",
    "positions":[
      {"Site":"GRU","SKU":"SKU-1","Available":140,"CustomerReservations":10,"CommittedOutbound":10,"AsOf":"2026-10-05T15:00:00Z"},
      {"Site":"REC","SKU":"SKU-1","Available":10,"ConfirmedInbound":5,"AsOf":"2026-10-05T15:00:00Z"}
    ],
    "policies":[
      {"Version":"policy-2026-10","Site":"GRU","SKU":"SKU-1","SafetyStock":50,"TargetStock":80,"UnitPriority":2},
      {"Version":"policy-2026-10","Site":"REC","SKU":"SKU-1","SafetyStock":20,"TargetStock":70,"UnitPriority":6}
    ],
    "lanes":[{"Origin":"GRU","Destination":"REC","LeadTime":86400000000000,"UnitHandlingCost":1,"Enabled":true}]
  }'
```

The snapshot types carry no JSON tags, so the field names are the Go names
(`Site`, `SKU`, `LeadTime` in nanoseconds, ...). Every position must carry the
request's `asOf`; a mixed snapshot is a 422 `invalid-planning-snapshot`.
Unknown fields are a 400 `invalid-request`. Every other route answers 503 in
this mode (for example `GET /v1/transfer-simulations` →
`read-models-unavailable`).

## 2. With a local Postgres

```bash
docker run -d --name nip-pg -p 5432:5432 \
  -e POSTGRES_USER=network-inventory-planning \
  -e POSTGRES_DB=network-inventory-planning \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres:16

export DATABASE_URL='postgres://network-inventory-planning@localhost:5432/network-inventory-planning?sslmode=disable'
go run ./cmd/network-inventory-planning
```

At startup the binary applies `internal/adapters/outbound/postgres/migrations`
(override the directory with `MIGRATIONS_PATH`, the DSN with
`MIGRATIONS_DATABASE_URL`) and refuses to boot if a migration fails.

### Seed the three read models

In a real deployment the consumers fill these tables from Kafka. Locally you
can insert fresh facts directly. Every fact must be newer than
`PLANNING_MAX_STALENESS` (default 10 minutes), so re-run the seed if you wait
longer:

```sql
-- psql "$DATABASE_URL"
INSERT INTO site_capability (site_id, transfer_origin_enabled, transfer_destination_enabled, capability_revision, as_of) VALUES
  ('WH1', true, true, 1, now()),
  ('WH2', true, true, 1, now());

INSERT INTO site_sku_demand (source_order_id, line_no, site_id, sku, demanded_units, due_at, state, assignment_version, as_of) VALUES
  ('ord-1', 1, 'WH1', 'SKU-1', 10, now() + interval '1 hour', 'ACTIVE', 'v1', now()),
  ('ord-2', 1, 'WH2', 'SKU-1', 30, now() + interval '1 hour', 'ACTIVE', 'v1', now());

INSERT INTO published_capacity_plan (plan_id, site_id, location, path_id, window_start, window_end, assigned_demand, capacity_over_window, shortage, published_at) VALUES
  ('plan-1', 'WH1', 'WH1', 'path-1', now() - interval '1 hour', now() + interval '8 hours', 10, 500, 0, now()),
  ('plan-2', 'WH2', 'WH2', 'path-1', now() - interval '1 hour', now() + interval '8 hours', 30, 100, 0, now());
```

A site participates only when it has all three facts; a demand counts only when
its `due_at` falls inside that site's plan window.

### Simulate

```bash
curl -s http://localhost:8080/v1/transfer-simulations
```

The answer is `{"advisory":true,"asOf":...,"sites":[...]}` with, per site,
`originEnabled`, `destinationEnabled`, `totalDemand`, `capacityOverWindow`,
`capacityHeadroom`, `windowStart` and `windowEnd`. Stale or missing facts give
a 503 `read-models-incomplete` with the refusal reason in `detail`.

## 3. Approving a transfer (the saga)

Approval needs the transactional outbox (so `KAFKA_BROKERS` must be set) and a
pick-leg release configuration. The fleet has one Kafka broker, reachable from
the host at `localhost:9092` through the kind cluster's external access:

```bash
export KAFKA_BROKERS=localhost:9092
export TRANSFER_PICK_PATH_ID=transfer-pick-path      # any path id WES knows
export TRANSFER_DISPATCH_PATH_ID=transfer-dispatch-path
# OUTBOX_RELAY_ENABLED=true would publish to the shared broker; leave it unset
# to keep the events as rows in outbox_events.
go run ./cmd/network-inventory-planning
```

:::caution
Do not set the `*_CONSUMER_GROUP` variables on a laptop that points at the
shared broker: a local process would join the cluster's groups and take their
partitions. They have no defaults for exactly this reason.
:::

```bash
curl -s -X POST http://localhost:8080/v1/transfers:approve \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: quickstart-1' \
  --data "{\"originSiteId\":\"WH1\",\"destinationSiteId\":\"WH2\",\"sku\":\"SKU-1\",\"quantity\":20,
           \"policyVersion\":\"policy-1\",\"operatorReason\":\"quickstart\",
           \"proposalAsOf\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
```

The response carries `transferId` (`trf-<uuid>`), `state: ALLOCATING`,
`transferLineId` (`<transferId>:1`), `expiresAt` (24 h after approval) and
`replayed: false`. Sending the same body with the same key again answers
`replayed: true`; a different body under the same key is a 409
`idempotency-conflict`.

Read it back:

```bash
curl -s 'http://localhost:8080/v1/transfers?state=ALLOCATING'
curl -s http://localhost:8080/v1/transfers/<transferId>        # with the audit trail
```

and look at what the approval queued:

```sql
SELECT id, topic, event_type, published_at FROM outbox_events ORDER BY id;
```

You will see `TransferPlanApproved` and `TransferAllocationRequested` on
`warehouse.network-inventory-planning.events` plus four
`TransferStateAdvanced` occurrences on
`warehouse.network-inventory-planning.analytics`. The saga then waits for
inventory-storage's allocation reply (see [Use cases](../ddd/use-cases.md)).

## 4. The MCP server

```bash
DATABASE_URL=$DATABASE_URL go run ./cmd/mcp      # :8090, Streamable HTTP at / and /mcp
curl -s http://localhost:8090/healthz             # {"status":"ok"}
```

Tools and inputs are listed on [MCP tools](../mcp/tools.md).

## 5. The analytics read side

The projector needs a separate database and the broker:

```bash
docker exec nip-pg createdb -U network-inventory-planning nip_analytics
export ANALYTICS_DATABASE_URL='postgres://network-inventory-planning@localhost:5432/nip_analytics?sslmode=disable'
KAFKA_BROKERS=localhost:9092 go run ./cmd/nip-projector   # admin :8091
go run ./cmd/nip-reports                                  # :8092
curl -s http://localhost:8092/reports/freshness
```

`nip-projector` joins the group `network-inventory-planning-analytics` unless
`ANALYTICS_CONSUMER_GROUP` overrides it; pick a private group id when you point
it at the shared broker.

## 6. This documentation site

```bash
cd docs
npm ci
npm run start     # http://localhost:3000/network-inventory-planning/
```

The REST reference under `docs/docs/api-reference/rest` is generated from
`apis/openapi.yaml`: `npm run clean-api-docs && npm run gen-api-docs`.
