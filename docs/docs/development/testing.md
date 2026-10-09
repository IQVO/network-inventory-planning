---
id: testing
title: Testing
sidebar_label: Testing
description: The test pyramid of network-inventory-planning as it exists in the repo, the make targets and every CI job.
---

# Testing

## The pyramid

| Level | Where | How many | Runs with | Needs |
| --- | --- | --- | --- | --- |
| Unit | `internal/domain/**`, `internal/application/**`, `internal/analytics/report`, adapters with fakes, `cmd/*/main_test.go` | 240 `func Test...` across the module (unit, integration and fitness together) | `make test` | nothing |
| HTTP adapter (httptest) | `internal/adapters/inbound/http/*_test.go` | handler, approve, simulate, transfers read, rebalance runs, reports, OTel metric and OpenAPI-contract tests | `make test` | nothing |
| BDD acceptance (godog/Gherkin, ADR 0011) | `features/*.feature`, steps in `features_test.go`, `features_saga_test.go`, `features_facts_test.go`, `features_reports_test.go` | **8 feature files, 107 `Scenario`/`Scenario Outline` blocks, 188 executed scenarios, 1361 steps** | `make bdd` (also part of `make test`) | nothing (in-memory world) |
| Integration (testcontainers) | files tagged `//go:build integration` | 8 files | `make integration` | Docker |
| Architecture fitness | `internal/architecture` | 10 tests | `make arch-test` | nothing |
| Mutation | gremlins over `internal/domain/transfer`, `internal/analytics/report`, `internal/domain` | — | `make mutation`, `make mutation-full` | `gremlins` v0.6.0 |
| Console remote | `web/src/**/*.test.ts(x)` (vitest + Testing Library) | — | `npm test` in `web/` | Node 22 and a sibling `warehouse-ui-kit` checkout |

The BDD numbers come from a local run of `go test -run TestFeatures -count=1 -v .`
on this branch: `188 scenarios (188 passed)`, `1361 steps (1361 passed)`.

### BDD feature files

| File | Scenario blocks | Covers |
| --- | --- | --- |
| `features/analytics_reports.feature` | 22 | the five `/reports/*` routes, ranges, redelivery |
| `features/transfer_approval.feature` | 22 | approval, idempotency, fail-closed refusals, problem types |
| `features/proposal_generation.feature` | 15 | planner rules on `POST /v1/transfer-proposals:generate` |
| `features/transfer_lifecycle.feature` | 13 | allocation, rejection, pick, dispatch, arrival, stow |
| `features/transfer_queries.feature` | 12 | `GET /v1/transfers` filters, paging, 400/404/503 |
| `features/simulation.feature` | 11 | fail-closed simulation |
| `features/rebalance_runs.feature` | 10 | run history and its `limit` |
| `features/health.feature` | 2 | `/healthz` |

### Integration tests

| File | What it proves | Containers |
| --- | --- | --- |
| `internal/adapters/outbound/postgres/transfer_query_integration_test.go` | the query side (filters, ordering, audit) | `postgres:16-alpine` |
| `internal/adapters/outbound/postgres/transfer_repo_rejection_integration_test.go` | the rejection path persists | Postgres |
| `internal/adapters/outbound/postgres/postgres_integration/rebalance_integration_test.go` | `rebalance_runs` repository | Postgres |
| `internal/adapters/outbound/analyticsstore/postgres_integration_test.go` | analytical projection and report queries | `postgres:16-alpine` |
| `internal/adapters/inbound/kafka/kafka_integration/planning_read_models_integration_test.go` | the three read-model consumers end to end | `confluentinc/confluent-local:7.6.1` + Postgres |
| `internal/adapters/inbound/kafka/kafka_integration/transfer_saga_integration_test.go` | approval → outbox → relay → replies and facts → `RECEIVED` | Kafka + Postgres |
| `internal/adapters/inbound/kafka/kafka_integration/analytics_integration_test.go` | projector consumer and DLQ | Kafka + Postgres |
| `cmd/mcp/main_integration_test.go` | the MCP binary against a real database | `postgres:16-alpine` |

Kafka tests must start a broker through testcontainers: the fitness test
`TestKafkaIntegrationTestsUseTestcontainers` fails any Kafka test that gates on
`os.Getenv("KAFKA_BROKERS")` or hardcodes `localhost:9092`.

### Architecture fitness tests

| Test | Rule |
| --- | --- |
| `TestHexagonalArchitecture` | domain and application never import adapters |
| `TestMCPAdapterDependencyRule` | the MCP adapter depends only on application and domain, and nothing depends on it |
| `TestNoAuthMiddlewareReintroduced` | no auth middleware on REST or MCP |
| `TestKafkaConsumerGroupNeverHardcodedInline` | group ids come from the composition root's env |
| `TestKafkaIntegrationTestsUseTestcontainers` | see above |
| `TestNoEventEnvelopeToggleOrFlatEnvelope` | no flat-envelope fallback |
| `TestCloudEventsOnly` | every producer and consumer goes through the CloudEvents package |
| `TestReplayConsumersSetCommitInterval` | a reader with a process-unique group id (full-replay cache consumer) must set `CommitInterval`; NIP has no such reader today |
| `TestEventCatalogueMatchesContract`, `TestEventCatalogueDetector` | every published type appears in the event catalogue contract |

### Contract tests

There is no schemathesis job in this repo. The REST contract is guarded by
`TestSimulationResponseMatchesTheOpenAPISchema`
(`internal/adapters/inbound/http/openapi_contract_test.go`, compares the live
simulation response with `apis/openapi.yaml`), by Spectral linting of both
specs in CI, and on the web side by `web/src/test/contract.test.ts`.

### Mutation testing

`.gremlins.yaml` sets `workers: 1` and `timeout-coefficient: 30`. Its
`threshold.efficacy` and `threshold.mutant-coverage` still hold the template
placeholders `{{MEASURED_EFFICACY_MINUS_1}}` and
`{{MEASURED_MUTANT_COVERAGE_MINUS_1}}`, so no numeric mutation threshold is
configured today.

### Chart tests

`charts/network-inventory-planning/tests/test_env_wiring.py` and
`test_service_selectors.py` check the rendered chart's env names against the
binaries and the Service selectors; no CI job runs them.

## Make targets

| Target | Command |
| --- | --- |
| `make build` | `go build ./...` |
| `make vet` | `go vet ./...` |
| `make fmt` / `make fmt-check` | `gofmt -w .` / fail on `gofmt -l .` |
| `make lint` | `golangci-lint run ./...` (v2.14.0) |
| `make test` | `go test ./... -race` |
| `make coverage` | race tests with `-coverpkg=./internal/domain/...,./internal/application/...,./internal/analytics/...` and a **90%** gate |
| `make integration` | `go test -tags=integration ./... -race -count=1` |
| `make bdd` | `go test ./... -run TestFeatures -v` |
| `make arch-test` | `go test ./internal/architecture/... -v` |
| `make mutation` | `gremlins unleash ./internal/domain/transfer` and `./internal/analytics/report` |
| `make mutation-full` | `gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30` |
| `make vuln` | `govulncheck ./...` |
| `make check` | `fmt-check vet build lint test` |
| `make check-all` | `check coverage arch-test bdd` |
| `make check-fast` | `fmt-check vet arch-test` + tests of changed packages (agent hook) |
| `make image`, `make helm-lint`, `make helm-template` | Docker build, `ct lint`, `helm template` |
| `make guide-lint`, `make harness-test` | agent-guide and hook self-tests |

`lefthook.yml` wires the git hooks.

## CI (`.github/workflows/ci.yml`)

| Job | Runs on | What it runs |
| --- | --- | --- |
| `lint` | push, PR | golangci-lint v2.14.0 |
| `guide-lint` | push, PR (advisory, `continue-on-error`) | `scripts/harness/guide_lint.py`, `test_hook.py` |
| `complexity` | push, PR | golangci-lint with only `gocyclo, gocognit, cyclop, funlen, nestif`, plus an informational gocyclo report |
| `test` | push, PR | build, vet, race tests with coverage, 90% gate |
| `bdd` | push, PR | `go test ./... -run TestFeatures -v` |
| `integration` | push, PR | `go test -tags=integration ./... -race -count=1` (a `postgres:16` service is provisioned, but the tests start their own containers) |
| `mutation-fast` | push, PR | gremlins on `internal/domain/transfer` and `internal/analytics/report` |
| `mutation` | schedule (Monday 06:00 UTC), dispatch | gremlins on `internal/domain`; opens or closes a `harness:red` issue |
| `drift` | schedule, dispatch | deadcode, `go mod tidy -diff`, knip on `web/`, coverage-quality report |
| `api-lint` | push, PR | Spectral on `apis/openapi.yaml` and `apis/asyncapi.yaml` |
| `vuln` | push, PR | govulncheck |
| `helm-lint` | PR | `ct lint --charts charts/network-inventory-planning` |
| `arch-test` | push, PR | `go test ./internal/architecture/... -v` |
| `web` | push, PR | in `web/`: `npm ci`, lint (oxlint), `tsc -b`, vitest, build (with `warehouse-ui-kit` built first) |
| `trivy-scan` | PR | builds the image, uploads SARIF, blocks on fixable CRITICAL/HIGH |
| `docker-publish` | push to `main` | pushes `claudioed/network-inventory-planning:latest` and `:<sha>`, cosign signature, SPDX SBOM attestation |
| `release` | push to `main` after `docker-publish` | next `vX.Y.Z` tag, versioned image tag, Helm chart to `oci://ghcr.io/claudioed`, GitHub release |

Other workflows: `docs.yml` (builds this site on every PR touching `docs/**`
or `apis/**`, deploys Pages from `main`), `ai-review.yml` (advisory AI review,
needs `ANTHROPIC_API_KEY`) and `selftest.yml` (harness template self-test).

## Evals

`EVALS.md` records harness evaluations (agent runs with and without the
guides) carried out on sibling repositories; it contains no evals of this
service's behaviour.
