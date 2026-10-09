# Network Inventory Planning

Network Inventory Planning is the bounded context for network inventory position, replenishment policy, transfer lanes, deterministic transfer proposals, and inter-warehouse transfer-plan orchestration.

It recommends transfers; it never becomes the inventory ledger or moves physical stock directly. Inventory Storage remains authoritative for stock and reservations, while WES contexts remain authoritative for executable work.

## Running locally

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

## Guardrails

- Every input position must have exactly the request `asOf`; mixed snapshots are rejected.
- Source stock is protected by source safety stock.
- Quantity is limited to source surplus and destination deficit.
- Transfers only use enabled, directed lanes and need a positive transparent score.
- Cross-context integration uses Kafka CloudEvents 1.0; no synchronous sibling REST/MCP calls.

## Quality

```sh
make check-all
```

## Documentation

The full documentation is a Docusaurus site under [`docs/`](docs/), published at
<https://iqvo.github.io/network-inventory-planning/> (build it locally with `npm ci` and
`npm run build` in `docs/`, Node 20).

- Overview: [Introduction](docs/docs/overview/introduction.md), [Architecture](docs/docs/overview/architecture.md), [Quickstart](docs/docs/overview/quickstart.md)
- Operations: [Runbook](docs/docs/operations/runbook.md), [Configuration](docs/docs/operations/configuration.md), [Observability](docs/docs/operations/observability.md), [Troubleshooting](docs/docs/operations/troubleshooting.md)
- Development: [Testing](docs/docs/development/testing.md)
- Domain-Driven Design: [Subdomain classification](docs/docs/ddd/subdomain-classification.md), [Use cases](docs/docs/ddd/use-cases.md), [Domain events](docs/docs/ddd/domain-events.md), [Ubiquitous language](docs/docs/ddd/ubiquitous-language.md), [Core domain chart](docs/docs/ddd/core-domain-chart.md), [Bounded context canvas](docs/docs/ddd/bounded-context-canvas.md), [Context map](docs/docs/ddd/context-map.md), [Aggregate design canvas](docs/docs/ddd/aggregate-design-canvas.md), [Domain message flow](docs/docs/ddd/domain-message-flow.md), [EventStorming](docs/docs/ddd/eventstorming.md), [Class diagrams](docs/docs/ddd/class-diagram.md), [Entity-relationship](docs/docs/ddd/entity-relationship.md), [Sequence diagrams](docs/docs/ddd/sequence-diagrams.md)
- Ecosystem: [Integration](docs/docs/ecosystem/integration.md)
- AI ecosystem: [MCP tools](docs/docs/mcp/tools.md)
- API: [API reference overview](docs/docs/api-reference/overview.md) (the REST reference is generated from [`apis/openapi.yaml`](apis/openapi.yaml))
- Decisions: [ADR index](docs/docs/adr/about.md)
