# network-inventory-planning

harness-template: v4

Warehouse-systems fleet bounded context Network Inventory Planning (wes tier). Owns the network inventory position read model, replenishment policy, transfer lanes, deterministic transfer proposals and the transfer saga. It recommends transfers; it never becomes the inventory ledger: Inventory Storage stays authoritative for stock and reservations, WES contexts for executable work.
This project is a domain-driven design study, not associated with any company: never name a real company, brand or product in code, comments, ADRs, specs, tests or fixtures (README `## Disclaimer`).

## Commands
`make check` (fmt, vet, build, lint, unit + handler + BDD; the pre-push gate), `make coverage` (scope + threshold live only in the Makefile), `make mutation` (gremlins per internal/domain/<pkg>; thresholds are measured, never guessed — currently 83/88 from a full run), `make arch-test`, `make integration` (testcontainers; build tag `integration`; needs Docker), `make vuln`, `make helm-lint`. Lint: `golangci-lint run --allow-parallel-runners ./...`.
Guides: `python3 scripts/harness/repo_lint.py`, `python3 scripts/harness/guide_lint.py` must stay clean.

## Layout (hexagonal)
`internal/domain/{packlevel,planning,transfer}` pure Go; `internal/application/{ports,usecases}`;
`internal/adapters/inbound/{http (chi),kafka,mcp (read-only)}`; `internal/adapters/outbound/{postgres,kafka,analyticsstore,telemetry,memory}`;
`cmd/{network-inventory-planning,nip-projector,nip-reports,mcp}` — OLTP service, analytics projector, reports reader, read-only MCP server. The Docusaurus docs site and the ADRs live under docs/ (ADRs in docs/docs/adr/).

## Runtime rule
Real PostgreSQL and Kafka only. `DATABASE_URL` mandatory (boot error naming the variable). The six Kafka consumer groups (siteCapability, siteSkuDemand, capacityPlan, transferReply, transferFact, packLevel) are each their own off switch: an EMPTY value means that consumer does not run (no defaults on purpose — a default would let a locally run process join the live cluster's group and steal its partitions). No authentication on REST or MCP.

## REST and MCP
chi router implementing every operation of apis/openapi.yaml (transfer proposal generation/simulation/approval, rebalance runs + policies, transfer lanes, transfers list/detail, the four reports + freshness) plus /healthz, /readyz. Errors are RFC 9457 problem+json.
The MCP server (cmd/mcp) exposes one read-only tool per REST GET, bound to the same read use cases; reads add zero outbox rows.

## Events: CloudEvents 1.0 is MANDATORY
`type` = `com.warehouse.wes.network-inventory-planning.<entity>.<Event>`; envelope only via the shared cloudevents encoder; subject and Kafka key = aggregate id; transactional outbox on every publish (at-least-once). Consumed contracts (fleet siblings + network contexts) and their local read models are catalogued in apis/asyncapi.yaml. A new event: domain struct + apis/asyncapi.yaml + the ADR type-catalogue row in the SAME commit.

## Domain
Boundary and phase decisions in docs/docs/adr/0001-0005 (boundary, read models + simulation, transfer saga, CloudEvents catalogue, work-demand release); later ADRs 0006-0019 cover the analytics read side, deployables and the transfer-fact seams.

## Workflow
GitFlow: feature/* -> PR into develop, CI green before merge, never push develop or main. lefthook: pre-commit fmt/vet/lint, pre-push `make check`. Commit and push after each step.
Helm chart: charts/network-inventory-planning (mcp + frontend default OFF; chart tests in charts/network-inventory-planning/tests: render_test.sh, test_env_wiring.py, test_kong_config.py, test_service_selectors.py — run in CI by the chart-tests job).
