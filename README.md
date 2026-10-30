# Network Inventory Planning

Network Inventory Planning is the bounded context for network inventory
position, replenishment policy, transfer lanes, deterministic transfer
proposals, and inter-warehouse transfer-plan orchestration — a **Core
subdomain in the WES tier** of the `warehouse-systems` fleet.

It recommends transfers; it never becomes the inventory ledger or moves
physical stock directly. Inventory Storage remains authoritative for stock
and reservations, while WES contexts remain authoritative for executable
work. It integrates with its siblings exclusively through Kafka CloudEvents
(no synchronous sibling REST/MCP calls) and is a Published Language source
on `warehouse.network-inventory-planning.events`.

## Disclaimer

This project is a domain-driven design study. It is not associated with, endorsed by or affiliated with any company, and it uses no company's code, data, logos or trademarks. All names, figures and examples are illustrative or synthetic.

---

## Documentation

The full documentation is a Docusaurus site under [`docs/`](docs/), published at
<https://iqvo.github.io/network-inventory-planning/> (build it locally with `npm ci` and
`npm run build` in `docs/`, Node 20).

Page map (source under `docs/docs/`):

| Area | Pages |
|---|---|
| Overview | [Introduction](docs/docs/overview/introduction.md), [Architecture](docs/docs/overview/architecture.md), [Quickstart](docs/docs/overview/quickstart.md) |
| Operations | [Runbook](docs/docs/operations/runbook.md), [Configuration](docs/docs/operations/configuration.md) (every env var per binary), [Observability](docs/docs/operations/observability.md), [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Development | [Testing](docs/docs/development/testing.md) |
| Domain-Driven Design | [Subdomain classification](docs/docs/ddd/subdomain-classification.md), [Use cases](docs/docs/ddd/use-cases.md), [Domain events](docs/docs/ddd/domain-events.md), [Ubiquitous language](docs/docs/ddd/ubiquitous-language.md), [Core domain chart](docs/docs/ddd/core-domain-chart.md), [Bounded context canvas](docs/docs/ddd/bounded-context-canvas.md), [Context map](docs/docs/ddd/context-map.md), [Aggregate design canvas](docs/docs/ddd/aggregate-design-canvas.md), [Domain message flow](docs/docs/ddd/domain-message-flow.md), [EventStorming](docs/docs/ddd/eventstorming.md), [Class diagrams](docs/docs/ddd/class-diagram.md), [Entity-relationship](docs/docs/ddd/entity-relationship.md), [Sequence diagrams](docs/docs/ddd/sequence-diagrams.md) |
| Ecosystem | [Integration](docs/docs/ecosystem/integration.md) |
| AI ecosystem | [MCP tools](docs/docs/mcp/tools.md), [A2A inbound adapter](docs/docs/adr/0020-a2a-inbound-adapter.md) |
| API | [API reference overview](docs/docs/api-reference/overview.md) (the REST reference is generated from [`apis/openapi.yaml`](apis/openapi.yaml)) |
| Decisions | [ADR index](docs/docs/adr/about.md) |

---

## Architecture

Hexagonal / ports & adapters. Dependencies point inward only:

```
cmd/network-inventory-planning/   the REST API (composition root): HTTP, six Kafka consumers, the saga tickers and the outbox relay in one process
cmd/mcp/                          read-only MCP server (Streamable HTTP, :8090)
cmd/a2a/                          read-only A2A agent (JSON-RPC over HTTP, Agent Card at /.well-known/agent-card.json, :8093)
cmd/nip-projector/                analytics projector — the ONLY writer of the analytical database (admin :8091)
cmd/nip-reports/                  read-only reports reader (serves /reports/*, :8092)
internal/
  domain/                         pure Go: transfer (the transfer saga), planning (positions, policies, lanes), packlevel
  application/                    ports (OUT interfaces), usecases
  adapters/
    inbound/http/                 handlers, DTOs, RFC 9457 problem+json errors, /healthz + /readyz
    inbound/kafka/                the six consumers (site-capability, site-sku-demand, capacity-plan, transfer-reply, transfer-fact, pack-level)
    inbound/mcp/  inbound/a2a/    the read-only agent surfaces
    outbound/postgres/            pgx repos, embedded golang-migrate SQL, transactional outbox (OLTP)
    outbound/kafka/               CloudEvents encoders + relay sink
    outbound/analyticsstore/      analytical-DB projection and report queries
    outbound/telemetry/           OpenTelemetry wiring (API binary)
    outbound/memory/              test doubles; never imported by cmd/** (enforced)
  architecture/                   the fitness tests
analytics/migrations/             analytical golang-migrate SQL
features/                         godog BDD (Gherkin)
web/                              the nip_mfe console remote
```

The layering rules are executable tests in `internal/architecture/`, not
conventions: `architecture_test.go` (hexagonal dependencies),
`kafka_writer_topic_fitness_test.go` / `kafka_writer_acks_fitness_test.go`,
`outbox_relay_cap_fitness_test.go`, `catalogue_fitness_test.go` (the
CloudEvents type catalogue), `events_fitness_test.go` and
`kafka_dlq_shutdown_fitness_test.go` — run by `make arch-test`.

## Guardrails

- Every input position must have exactly the request `asOf`; mixed snapshots are rejected.
- Source stock is protected by source safety stock.
- Quantity is limited to source surplus and destination deficit.
- Transfers only use enabled, directed lanes and need a positive transparent score.
- Cross-context integration uses Kafka CloudEvents 1.0; no synchronous sibling REST/MCP calls.

## Domain events

Contract: [`apis/asyncapi.yaml`](apis/asyncapi.yaml), catalogue in [ADR 0004](docs/docs/adr/0004-cloudevents-type-catalogue.md). Every message is a
CloudEvents 1.0 structured-JSON envelope. Real types, verbatim from the contract:

```
com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved
com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased
com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced
```

- **Published** to `warehouse.network-inventory-planning.events` (through the
  transactional outbox): `TransferPlanApproved`, `TransferAllocationRequested`,
  `WorkDemandReleased`, `TransferStateAdvanced`, `TransferStuckDetected`,
  `RebalanceRunCompleted` — types
  `com.warehouse.wes.network-inventory-planning.<entity>.<Event>` (`transfer`,
  `workdemand`, `saga`). The same outbox rows additionally feed
  `warehouse.network-inventory-planning.analytics` (ADR 0007).
- **Consumed** (six consumers, each on its own group id): facility-layout
  `SiteCapabilityChanged` (`warehouse.facility.events`); order-management
  `SiteSkuDemandChanged` (`warehouse.order-management.events`);
  warehouse-planning `CapacityPlanPublished`
  (`warehouse.warehouse-planning.events`); inventory-storage
  `TransferStockAllocated`, `TransferStockAllocationRejected`,
  `TransferReceiptStaged`, `TransferStockStowed` (`warehouse.inventory.events`);
  fulfillment-execution `TransferPicked`, `TransferDispatched`,
  `TransferArrived` (`warehouse.fulfillment.events`); product-master
  `ProductPackLevelDeclared`, `ProductPackLevelRetired`
  (`warehouse.product-master.events`).

## Running it

```sh
HTTP_ADDR=:8080 go run ./cmd/network-inventory-planning
curl -i http://localhost:8080/healthz
```

Generate an advisory proposal from a coherent planning snapshot:

```sh
curl -X POST http://localhost:8080/v1/transfer-proposals:generate \
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

The response is an explainable, score-ordered advisory proposal. It reserves and moves nothing: an operator approves a transfer separately (`POST /v1/transfers:approve`), and the transfer saga then drives the Inventory Storage reservation, WES work release, pick, dispatch, arrival and stow through Kafka (see [Domain message flow](docs/docs/ddd/domain-message-flow.md)).

### Configuration

The runtime rule differs from most siblings: **Postgres is required for
everything beyond diagnostics** (unset `DATABASE_URL` = diagnostic mode: only
`/healthz` and `POST /v1/transfer-proposals:generate` work, every other route
answers 503), while **Kafka is optional locally** — unset `KAFKA_BROKERS`
disables the outbox (`POST /v1/transfers:approve` answers 503
`saga-unavailable`), the consumers, the saga-health check and the scheduled
rebalance; when it is set, `PACK_LEVEL_CONSUMER_GROUP` is required with it
(ADR 0017) and `OUTBOX_RELAY_ENABLED` (any non-empty value) starts the relay.

`cmd/network-inventory-planning`:

| Env var | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address of the REST API |
| `DATABASE_URL` | *(unset = diagnostic mode)* | OLTP Postgres DSN; set ⇒ migrate, open the pool and ping, or refuse to boot |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | Directory of the OLTP `*.sql` migrations (the image sets `migrations`) |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | DSN used only by the migration step (direct connection; ADR 0006) |
| `KAFKA_BROKERS` | *(unset = Kafka off)* | Comma-separated broker list; feeds the outbox, the consumers and the saga tickers |
| `SITE_CAPABILITY_CONSUMER_GROUP` | *(unset = off)* | Group of the site-capability consumer on `warehouse.facility.events` |
| `SITE_SKU_DEMAND_CONSUMER_GROUP` | *(unset = off)* | Group of the site-sku-demand consumer on `warehouse.order-management.events` |
| `CAPACITY_PLAN_CONSUMER_GROUP` | *(unset = off)* | Group of the capacity-plan consumer on `warehouse.warehouse-planning.events` |
| `TRANSFER_REPLY_CONSUMER_GROUP` | *(unset = off)* | Group of the transfer-reply consumer on `warehouse.inventory.events` (without it approved transfers never leave `ALLOCATING`) |
| `TRANSFER_FACT_CONSUMER_GROUP` | *(unset = off)* | Group of the transfer-fact consumer on `warehouse.fulfillment.events` (without it allocated transfers never leave `ALLOCATED`) |
| `PACK_LEVEL_CONSUMER_GROUP` | **required when `KAFKA_BROKERS` is set** (ADR 0017) | Group of the pack-level consumer on `warehouse.product-master.events`; no default group id is safe |
| `OUTBOX_RELAY_ENABLED` | *(unset = relay off)* | Any non-empty value starts the outbox relay (needs `KAFKA_BROKERS`); unset, events stay as rows in `outbox_events` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | Relay attempt cap (ADR 0016); `0` = unlimited; a bad value logs WARN and uses 20 |
| `PLANNING_MAX_STALENESS` | `10m` | Freshness budget of every fact in the fail-closed snapshot; invalid ⇒ boot error |
| `TRANSFER_PICK_PATH_ID` | *(unset = approval answers 503 `config-incomplete`)* | Process path id carried by the pick-leg `WorkDemandReleased` (ADR 0005) |
| `TRANSFER_PICK_CPT_OFFSET` | `2h` | CPT horizon of the pick demand |
| `TRANSFER_DISPATCH_PATH_ID` | *(unset = no dispatch demand)* | Path id of the dispatch-leg demand; a `TransferPicked` fact still moves the transfer to `PICKED` |
| `TRANSFER_DISPATCH_CPT_OFFSET` | `3h` | CPT horizon of the dispatch demand |
| `NIP_HEALTH_CHECK_INTERVAL` | `5m` | Saga-health (stuck transfer) check tick (ADR 0007); `0` disables |
| `NIP_STUCK_THRESHOLDS` | `ALLOCATING=1h,PICKED=24h,IN_TRANSIT=72h` (other non-terminal states 24h) | `STATE=DURATION` overrides merged over the defaults |
| `NIP_REBALANCE_SCHEDULE` | *(unset = off)* | Interval of the observe-only scheduled rebalance; needs the outbox |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error`, case-insensitive |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC endpoint for traces and metrics (ADR 0012) |
| `OTEL_SERVICE_NAME` | `network-inventory-planning` | `service.name` resource attribute |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute |
| `ENVIRONMENT` | `local` | `deployment.environment.name` resource attribute |

`cmd/mcp` reads `MCP_ADDR` (`:8090`), `DATABASE_URL` (unset ⇒ every tool
returns `read-side-unavailable`), `MIGRATIONS_PATH`, `MIGRATIONS_DATABASE_URL`
(default `DATABASE_URL`), `PLANNING_MAX_STALENESS` (`10m`) and `LOG_LEVEL`.
`cmd/a2a` reads `A2A_ADDR` (`:8093`), `A2A_AGENT_URL` (**required**, absolute,
advertised on the Agent Card), `DATABASE_URL` (**required**),
`MIGRATIONS_PATH`, `MIGRATIONS_DATABASE_URL` (default `DATABASE_URL`),
`LOG_LEVEL` and `SERVICE_VERSION`. Full tables per binary, with the chart
value that renders each variable: the
[Configuration](docs/docs/operations/configuration.md) page.

### Analytics data product

An additive analytical read side built from this service's own domain events
(ADR 0007): the OLTP API fans events out (one outbox, two encoders) to BOTH
`warehouse.network-inventory-planning.events` and
`warehouse.network-inventory-planning.analytics`; the projector is the only
writer of the analytical database; the reports server is read-only.

```sh
# Projector — the ONLY analytical-DB writer (migrations in analytics/migrations)
export ANALYTICS_DATABASE_URL="postgres://nip:***@localhost:5432/nip_analytics?sslmode=disable"
KAFKA_BROKERS=localhost:9092 go run ./cmd/nip-projector    # admin /healthz,/readyz on :8091

# Reports — read-only reader (every connection pinned read-only)
ANALYTICS_READER_DATABASE_URL="$ANALYTICS_DATABASE_URL" go run ./cmd/nip-reports   # :8092

curl "http://localhost:8092/reports/transfer-funnel?from=...&to=..."
curl "http://localhost:8092/reports/state-dwell"
curl "http://localhost:8092/reports/stuck-transfers"
curl "http://localhost:8092/reports/rebalance-runs"
curl "http://localhost:8092/reports/freshness"
```

| Env var | Default | Meaning |
|---|---|---|
| `ANALYTICS_DATABASE_URL` | **required** (projector) | Read-write DSN of the analytical database |
| `KAFKA_BROKERS` | **required** (projector) | Brokers of `warehouse.network-inventory-planning.analytics` |
| `ANALYTICS_CONSUMER_GROUP` | `network-inventory-planning-analytics` | Projector consumer group (a new group replays the whole topic) |
| `ANALYTICS_MIGRATIONS_PATH` | `analytics/migrations` | Analytical migrations (projector only) |
| `ADMIN_ADDR` | `:8091` | Projector admin server (`/healthz`, `/readyz`) |
| `ANALYTICS_READER_DATABASE_URL` | falls back to `ANALYTICS_DATABASE_URL` | Reader DSN for `cmd/nip-reports`, ideally a read-only role |
| `HTTP_ADDR` (reports) | `:8092` | Reports listen address |

## Observability

Only the API binary sets up OpenTelemetry ([ADR 0012](docs/docs/adr/0012-otlp-telemetry-export.md)): it pushes traces and
metrics over OTLP/gRPC to `OTEL_EXPORTER_OTLP_ENDPOINT` (default
`localhost:4317`; export never blocks, and there is no `/metrics` scrape
endpoint). Resource attributes: `service.name`
(`OTEL_SERVICE_NAME`, default `network-inventory-planning`),
`service.version` (`SERVICE_VERSION`, default `dev`) and
`deployment.environment.name` (`ENVIRONMENT`, default `local`). It defines
**no custom instruments**: `http.server.request.duration` and the
request/response body-size histograms from `otelhttp`, plus the Go runtime
metrics. The API logs with `slog` (text to stderr; `LOG_LEVEL` sets the
level); `mcp`, `nip-projector` and `nip-reports` do not initialise OpenTelemetry
and emit structured logs only. Full instrument tables and suggested alerts:
the [Observability](docs/docs/operations/observability.md) page.

## API

Contract: [`apis/openapi.yaml`](apis/openapi.yaml). No authentication. Errors are
RFC 9457 `application/problem+json` (`type` is a slug such as `not-found`,
`validation-failed`, `saga-unavailable`, `config-incomplete`). The approval
endpoint is idempotency-keyed: `POST /v1/transfers:approve` requires a fresh
`Idempotency-Key` header per logical approval; a replayed key replays the first
response, a key reused with a different request is rejected.

| Method and path | Operation |
| --- | --- |
| `POST /v1/transfer-proposals:generate` | deterministic, explainable, score-ordered proposal from a snapshot |
| `GET /v1/transfer-simulations` | the simulation catalogue |
| `POST /v1/transfers:approve` | approve a proposal; starts the transfer saga (`Idempotency-Key` required) |
| `GET /v1/transfers`, `GET /v1/transfers/{id}` | transfer queries (state, saga trail) |
| `GET /v1/rebalance-runs` | rebalance run history |
| `GET /v1/rebalance-policies`, `PUT /v1/rebalance-policies` | read, upsert rebalance policies |
| `GET /v1/transfer-lanes`, `PUT /v1/transfer-lanes` | read, upsert transfer lanes |
| `GET /reports/transfer-funnel`, `/reports/state-dwell`, `/reports/stuck-transfers`, `/reports/rebalance-runs`, `/reports/freshness` | analytics reports (served by `cmd/nip-reports`) |
| `GET /healthz` | liveness |

## Helm chart

The chart is `charts/network-inventory-planning`: one `api` Deployment and
Service, plus four opt-in components — `mcp`, `a2a`, `analytics`
(`analytics-projector` + `analytics-reports`; needs `kafka.enabled` and a DSN
source) and `frontend` — all default off. `kafka.enabled` renders
`KAFKA_BROKERS` and the consumer group ids; `otel.enabled` (default true)
renders `OTEL_EXPORTER_OTLP_ENDPOINT`. HPAs, Ingress and the Gateway API
route are off by default.

```sh
helm template network-inventory-planning charts/network-inventory-planning -f charts/network-inventory-planning/ci/default-values.yaml
bash charts/network-inventory-planning/tests/render_test.sh
python3 charts/network-inventory-planning/tests/test_service_selectors.py
python3 charts/network-inventory-planning/tests/test_env_wiring.py
python3 charts/network-inventory-planning/tests/test_kong_config.py
```

The render tests prove, among other invariants, that every Service selects
exactly one Deployment, the env wiring matches `values.yaml`, and the Kong
route config is coherent; `ci/default-values.yaml` and
`ci/all-components-values.yaml` are the CI values files. Linted in CI with
`ct lint --charts charts/network-inventory-planning`.

### Running the MCP server in Kubernetes

`cmd/mcp` exposes the transfer read side to agents over the Model Context
Protocol: Streamable HTTP at `/` and `/mcp`, an open `GET /healthz`, no auth,
no Kafka, no writes. Four tools: `get_transfer`, `list_transfers`,
`find_stuck_transfers` and `simulate_transfer_options` (the latter honors
`PLANNING_MAX_STALENESS`); the tool registry is pinned by tests in
`internal/adapters/inbound/mcp`. Enable with `mcp.enabled=true`; the Kong
route is opt-in via `mcp.route.enabled`.

```sh
DATABASE_URL=postgres://nip:***@localhost:5432/nip?sslmode=disable go run ./cmd/mcp   # :8090
```

### Running the A2A agent in Kubernetes

`cmd/a2a` serves the same read side as the MCP server as an Agent2Agent agent
(JSON-RPC over HTTP, Agent Card at `/.well-known/agent-card.json`, endpoint at
`/` and `/a2a`, default `:8093`). It is read-only and offers two goal-shaped
skills, `transfer_status` and `find_stuck_transfers`
([ADR-0020](docs/docs/adr/0020-a2a-inbound-adapter.md)). A2A carries delegation only;
facts still leave as CloudEvents through the transactional outbox of the api binary.
Enable with `a2a.enabled=true` (`A2A_AGENT_URL` defaults to the in-cluster
Service DNS, override with `a2a.agentUrl`; `a2a.route.enabled` adds the Kong route).

```sh
DATABASE_URL=$DATABASE_URL A2A_AGENT_URL=http://localhost:8093/ go run ./cmd/a2a
curl -s http://localhost:8093/.well-known/agent-card.json
```

## Operator micro-frontend (`web/`)

`web/` is the `nip_mfe` Module Federation remote for the warehouse-console
shell: read-only screens over the transfer queries and reports above. The API
base is resolved at runtime from the shell's
`window.__WAREHOUSE_CONFIG__.apiOrigin`
(`${apiOrigin}/api/network-inventory-planning`). Enable in the cluster with
`frontend.enabled=true` (nginx pod, ClusterIP, default off), served by
warehouse-infra's web gateway at `/mfes/network-inventory-planning/`.

```sh
cd web && npm install && npm run dev     # standalone on :5192
```

## Local development / quality gate

Every CI sensor is a `make` target. `make help` lists them all; the repo also
has `make image`, `make helm-lint` and `make helm-template` for the chart.

| Command | What it runs |
| --- | --- |
| `make check` | format, vet, build, lint, unit, handler and BDD tests (the pre-push gate) |
| `make check-fast` | fmt-check, vet, arch-test |
| `make check-all` | check + coverage + arch-test + bdd |
| `make coverage` | 90% gate over `internal/domain` and `internal/application` |
| `make mutation` | gremlins on every `internal/domain/<pkg>` |
| `make arch-test` | hexagonal and harness fitness tests |
| `make bdd` | godog scenarios in `features/` |
| `make integration` | testcontainers Postgres and Kafka (needs Docker) |
| `make vuln` | govulncheck |

Git hooks are managed by [lefthook](https://github.com/evilmartians/lefthook)
(`lefthook.yml`): pre-commit runs `fmt-check` + `vet` + `lint`; pre-push runs
`make check`. Activate once per clone: `lefthook install`.

## Tests

- **Unit** — `go test ./...` (domain, use cases, handlers; `make test`).
- **BDD acceptance** — godog/Gherkin over the real HTTP API in `features/` (`analytics_reports`, `health`, `proposal_generation`, `rebalance_catalogue`, `rebalance_runs`, `simulation`, `transfer_approval`, `transfer_lifecycle`, `transfer_queries`; `make bdd`).
- **Integration** — testcontainers Postgres and Kafka (`make integration`, build-tagged, needs only Docker).
- **Coverage gate** — `make coverage`: ≥ 90% over `internal/domain` + `internal/application`.
- **Mutation** — gremlins over every `internal/domain/<pkg>` (`make mutation`; the exhaustive `mutation-full`).
- **Architecture fitness** — `internal/architecture/` (`make arch-test`).
- **Web** — lint, typecheck and tests in `web/`.

CI (`.github/workflows/ci.yml`) runs `lint`, `guide-lint`, `complexity`, `test`
(with the 90% coverage gate), `bdd`, `integration`, `mutation-fast`, `api-lint`,
`vuln`, `chart-tests`, `arch-test` and `web` on every push and PR (`helm-lint`
with `ct lint` on PRs); `trivy-scan` builds the image and scans it on PRs; the
exhaustive `mutation` and the advisory `drift` sensor run only on the weekly
schedule or manual dispatch; `docker-publish` pushes the image to GHCR on
`main` once the gate jobs are green, and `release` then tags the next version
and publishes the chart.

## Architecture Decision Records

The ADR index lives at [docs/docs/adr/about.md](docs/docs/adr/about.md) — 20
decisions, from the context boundary and the transfer saga to the CloudEvents
catalogue, the outbox relay attempt cap and the A2A inbound adapter.

## License

MIT — see [LICENSE](LICENSE).
