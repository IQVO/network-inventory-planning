---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

- `POST /v1/transfer-proposals:generate` -> `GenerateTransferProposals`
- `GET /healthz` -> process health probe

## Conventions

- The planning API is unauthenticated pending a fleet-wide replacement decision; do not reintroduce auth middleware locally.
- Commands are explicit and idempotency-ready. A generation request must carry a coherent `asOf` time, policy version, and site/SKU inputs; it never reads siblings synchronously.
- Validation errors are returned as RFC 7807 `application/problem+json` with a stable machine-readable `type`.
- The endpoint returns proposals only. It does not reserve, pick, ship, or mutate physical inventory.
