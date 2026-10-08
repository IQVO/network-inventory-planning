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
- **The simulation serves proposals only.** *(Wrong; superseded by the Correction below.)* It carries no stock positions, so
  the per-site table is *derived from the options* (units each site would send
  and receive if every option were executed), labelled as such; it is not a
  stock or headroom level. A true per-site capacity view needs an endpoint this
  service does not have.
- **Approval is idempotent from the UI.** *(The "one option" framing is superseded by the Correction below; the idempotency rules stand.)* The Idempotency-Key is minted when
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

## Correction

**What was wrong.** The remote was built from `apis/openapi.yaml`, which
described `GET /v1/transfer-simulations` as `{asOf, options: Proposal[]}`. The
real handler (`internal/adapters/inbound/http/handler.go`, `simulate` and
`simulateSiteDTO`) answers
`{advisory: true, asOf, sites: [{site, originEnabled, destinationEnabled,
totalDemand, capacityOverWindow, capacityHeadroom, windowStart, windowEnd}]}`:
a per-site capacity-versus-demand view with **no proposals, no routes, no SKUs
and no quantities**. The "Simulation" screen (a per-site roll-up derived from
options, plus an options table with a row-level Approve button) and every
mock/fetch fixture were written against that fiction, so against the real
service the tab showed nothing and Approve had no proposal to start from. The
earlier statement "the simulation serves proposals only" above is the inverse of
the truth: it serves per-site headroom and nothing else. (The OpenAPI spec is
corrected separately, IQVO/network-inventory-planning#22.)

**Why it happened.** The spec, not the handler, was taken as the source of truth,
and the fixtures were then generated from the same spec, so tests and screens
agreed with each other and with nothing real.

**What changed.**

- The simulation screen renders `sites` honestly: demand, capacity over the
  window, headroom (negative = short, highlighted), origin/destination-enabled
  flags, the window, freshness from `asOf`, and an "advisory, moves nothing"
  banner. Sites are ordered short first (most short first), then covered sites
  by most headroom, then by site id, mirroring warehouse-ops-agent's
  `explain_network_imbalance`. "Short" means `capacityHeadroom < 0`, as the
  service defines it; no other threshold is introduced. The fail-closed 503
  handling is unchanged.
- Approve is an **operator-driven form**, not a pick-from-options list. Proposals
  exist only as the result of `POST /v1/transfer-proposals:generate` over an
  explicit snapshot (positions, policies, lanes) that a browser operator cannot
  reasonably hand-write, so the console does not pretend to offer them. The
  operator chooses the origin (`originEnabled` sites) and the destination
  (`destinationEnabled`, different from the origin), and types the SKU, quantity
  (positive integer), policy version, optional reason and `proposalAsOf`
  (defaulting to the simulation's `asOf`, editable). No policy version is
  defaulted: the code and ADRs name no current one, so the field is required and
  empty. The guidance text points at short and donor sites but never suggests a
  quantity. The service remains the authority: it re-validates the request
  against the current fail-closed read models.
- Idempotency: the key is minted per distinct request. Retrying identical input
  reuses the key; changing any field mints a new one; a completed approval
  resets the form.
- Fixtures are generated from the real shapes, and `web/src/test/contract.test.ts`
  fails when a fixture's keys or JSON kinds diverge from the shapes pinned in
  `web/src/test/realShapes.ts`, or when those pins diverge from the `json` tags
  of the Go DTOs (the test reads `internal/adapters/inbound/http/*.go`).
- Other divergences found by checking every type against the handlers:
  problem+json carries only `type`, `title`, `status`, `detail` (no `instance`)
  with `type` = `https://warehouse.example/problems/<slug>`; the mocks used a
  different host, an `instance` field and slugs the service never emits
  (`read-models-not-ready`, `proposal-stale`, `service-unavailable`). A 422 on
  approve covers every fail-closed refusal (facts missing, stale or disabled,
  including read models that cannot be assembled, and an expired proposal), not
  only "invalid request"; the 503 lead no longer claims read models are the 503
  cause. `pickedQuantity` is omitted when zero, so absence past `PICKED` means
  "none recorded", not "not picked yet". The audit trail starts at `DRAFT`
  (`TransferDrafted`), which the fixture omitted. Approve always answers 200,
  including a replay; it never answers 201.
