# ADR-0010: Console remote `nip_mfe` (Module Federation)

Status: Accepted

## Context

Operators had no screen for this context: transfers, their audit trail and the
network simulation were reachable only through REST/MCP (ADR-0007) and the
approval action only by calling `POST /v1/transfers:approve` by hand. Every
other bounded context with an operator surface ships a Vite + React **Module
Federation remote** that `warehouse-console` loads at runtime (fleet MFE
console architecture; reference: `warehouse-planning/web`, container
`capacity_mfe`). This context had none.

## Decision

### The remote (`web/`)

- A standalone Vite + React + TypeScript project in `web/`, not part of the Go
  module. Federation container **`nip_mfe`**, exposing one module `./App`
  (default export, no props, relative routes). Production Vite `base` is
  `/mfes/network-inventory-planning/`; dev/preview port **5192** (5190 is
  `capacity_mfe`, 5191 is `productmaster_mfe`).
- It is a plain browser client of **this service's own REST API only**, through
  `${apiOrigin}/api/network-inventory-planning` (`apiOrigin` from the console's
  runtime `/config.json`, published as `window.__WAREHOUSE_CONFIG__`; a
  production build throws without it). It talks to no sibling context. Kong
  serves APIs only; the remote's HTML/JS is served by its own nginx pod behind
  the Nginx web gateway, never through Kong.
- No authentication (fleet decision): the remote sends no key or bearer.
- Shared UI primitives come from `@warehouse/ui-kit` (`Card`, `DataTable`,
  `StatusPill`, `Timeline`, `KpiStat`, `FreshnessBadge`); the saga states are
  not in the kit's `StatusPill` map, so the tone is passed explicitly
  (`stateTone`) instead of changing the kit.

### Screens

| Route (relative) | Reads | Writes |
|---|---|---|
| `/` Transfers | `GET /v1/transfers` (state, originSiteId, destinationSiteId, limit/offset) | none |
| `/transfers/:id` Transfer detail | `GET /v1/transfers/{id}` | none |
| `/simulation` Network simulation | `GET /v1/transfer-simulations` | `POST /v1/transfers:approve` |
| `/rebalance-runs` Rebalance runs | `GET /v1/rebalance-runs` | none |

Behaviours that are contract, not taste:

- **Freshness is shown, not implied.** The transfers table stamps when the page
  was loaded and each row shows the age of its last state change (a
  non-terminal transfer with an old last change is stuck); the simulation shows
  its `asOf` watermark through `FreshnessBadge`.
- **Fail-closed means a visible refusal.** `GET /v1/transfer-simulations`
  answers 503 problem+json while the read models are unconfigured, incomplete
  or stale; the screen shows "Read models not ready" with the problem's own
  title and detail and never renders an empty simulation for it. "Ready but
  nothing advisable" is a distinct, non-error empty state.
- **The simulation serves proposals only.** It carries no stock positions, so
  the per-site table is *derived from the options* (units each site would send
  and receive if every option were executed), labelled as such; it is not a
  stock or headroom level. A true per-site capacity view needs an endpoint this
  service does not have.
- **Approval is idempotent from the UI.** The Idempotency-Key is minted when
  the approval form opens for one option and reused for every retry of that
  approval (a 503 or a network failure can be retried without risking a second
  transfer); a different option gets a new key. The request is the option's own
  snapshot (origin, destination, SKU, quantity, policyVersion, proposalAsOf)
  plus the optional operator reason. Problems are rendered with their title,
  detail and HTTP status: 409 (key reused for another payload), 422 (fail-closed
  validation), 503 (configuration incomplete / read models not ready). A
  `replayed: true` answer is said out loud ("showing the original transfer").
- The remote never mutates anything except through that one endpoint.

### Delivery

- `web/Dockerfile` (nginx-unprivileged, same build strategy as the sibling
  remotes: `@warehouse/ui-kit` as the named build context `uikit`, no lockfile
  install inside the image). The Go image's root `.dockerignore` now excludes
  `web/`; the frontend has its own build context.
- Chart: opt-in `frontend.enabled` (default `false`) renders a Deployment and
  a **ClusterIP** Service, `component=frontend`; **no Ingress/HTTPRoute** for it
  (warehouse-infra's Nginx web gateway routes `/mfes/network-inventory-planning/`).
  `tests/test_service_selectors.py` pins that every Service still selects
  exactly one Deployment and that the frontend is ClusterIP, off by default and
  unrouted by this chart.
- CI: a `web` job (lint, `tsc -b`, vitest, build) mirrors the siblings'; it is
  not a required check.
- `warehouse-console` registers the remote as `nip_mfe` (entry
  `/mfes/network-inventory-planning/remoteEntry.js` in production,
  `http://localhost:5192/remoteEntry.js` in dev).

## Consequences

- Operators can see and approve transfers without hand-built REST calls.
- **Kong's global CORS plugin must allow the `Idempotency-Key` request header**
  (`warehouse-infra/terraform/kong-cors.tf` currently allows only `Accept`,
  `Content-Type`, `Origin`). Until it does, the browser's preflight for
  `POST /v1/transfers:approve` is rejected and the Approve action cannot work
  from the console; the read screens are unaffected. MSW/fetch-mock tests
  cannot see this; it is a warehouse-infra change tracked outside this repo.
- `warehouse-infra` (`frontends.tf` `frontend_remotes` and the chart override
  for `frontend.enabled`) must register the repo for the gateway route and
  image side-loading before the remote is reachable in kind.
- The remote's Dependabot entry is deliberately absent: it depends on
  `file:../../warehouse-ui-kit`, outside the repo, which Dependabot cannot
  fetch.
