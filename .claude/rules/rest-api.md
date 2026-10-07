---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

- `POST /v1/transfer-proposals:generate` -> `GenerateTransferProposals`
- `GET /v1/transfer-simulations` -> `SimulateTransferOptions` (advisory, fail-closed)
- `POST /v1/transfers:approve` -> `ApproveTransfer` (idempotency-keyed saga start)
- `GET /v1/transfers` -> `ListTransfers` (read-only; `state`, `originSiteId`, `destinationSiteId`, `limit` <= 200, `offset`)
- `GET /v1/transfers/{id}` -> `GetTransfer` (read-only; the transfer plus its audit trail; 404 problem+json when unknown)
- `GET /healthz` -> process health probe

## Conventions

- The planning API is unauthenticated pending a fleet-wide replacement decision; do not reintroduce auth middleware locally.
- Commands are explicit and idempotency-ready. A generation request must carry a coherent `asOf` time, policy version, and site/SKU inputs; it never reads siblings synchronously.
- Validation errors are returned as RFC 7807 `application/problem+json` with a stable machine-readable `type`.
- The generate endpoint returns proposals only. It does not reserve, pick, ship, or mutate physical inventory.
- The read endpoints (`GET /v1/transfers*`) go through the `ports.TransferQuery` query port, never `TransferRepository`; they answer 503 problem+json when the instance has no database and never echo infrastructure error detail.
- Keep `apis/openapi.yaml` in step with the routes (CI `api-lint`: `spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn`).
