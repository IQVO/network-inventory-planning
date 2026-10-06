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

The response is an explainable, score-ordered advisory proposal. Approval, Inventory Storage reservation, WES work creation, dispatch, receipt, and reconciliation are deliberately separate upcoming slices.

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
