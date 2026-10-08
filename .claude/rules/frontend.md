---
paths:
  - "web/**"
---

# Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: `nip-mfe`, a Vite + React + TypeScript **Module
Federation remote** consumed by the separate `warehouse-console` shell repo
(ADR-0010). It is a plain browser client of this service's own REST API:
nothing in `web/` talks to any other bounded context, and nothing in
`internal/` knows `web/` exists. Reference implementation:
`warehouse-planning/web` (`capacity_mfe`).

- Federation container `nip_mfe`. Exposes ONE module, `./App` (`src/App.tsx`,
  default export, **no props**): the shell mounts it under a route prefix and
  provides the `BrowserRouter`, the design tokens and
  `window.__WAREHOUSE_CONFIG__`. Routes are relative: `/` (transfers),
  `/transfers/:id`, `/simulation`, `/rebalance-runs`.
- Dev/preview port **5192** (strictPort). Production base
  `/mfes/network-inventory-planning/`.
- API base = `${apiOrigin}/api/network-inventory-planning` (`src/config.ts`); a
  production build throws when `apiOrigin` is missing. No auth: the remote sends
  no key or bearer. Kong serves APIs only; never route HTML/JS through it.
- Own `package.json`, build and tests; does **not** participate in `make check`.
  CI's `web` job runs `npm run lint`, `npx tsc -b`, `npm test`, `npm run build`
  after building the sibling `warehouse-ui-kit`. Locally, `web/` expects the
  kit checked out beside this repo (`file:../../warehouse-ui-kit`).

## Rules that each cost real time elsewhere in the fleet

- **`web/vite.config.ts` stays in OBJECT form** (`vitest.config.ts` does
  `mergeConfig`; a callback export kills the whole suite). The base is derived
  from `process.argv.includes("build")`.
- **Relative links must come from a layout route WITH a path** (`<Route path="/"
  element={<NipLayout/>}>`), and tests must mount the remote under the host's
  splat route (`App.test.tsx` `mountUnderHost`): a nav above `<Routes>` or in a
  pathless layout resolves `simulation` to `/<mount>/simulation/simulation`.
- **The Docker build cannot use the committed lockfile or a host
  `node_modules`** (missing linux native bindings, npm/cli#4828): see
  `web/Dockerfile` and `web/.dockerignore`. Never commit `node_modules/` or
  `dist/`.
- **Use only `@warehouse/ui-kit`** components and tokens. The saga states are not
  in `StatusPill`'s map: pass `tone` (`stateTone`) rather than hand-rolling a
  colour or editing the kit from here.
- **No Dependabot `npm` entry for `/web`** (depends on a path outside the repo).
- Tests mock `fetch` (`src/test/fetchMock.ts`: an unmocked request REJECTS).
  Every screen has loading, success, empty and error tests; an RFC 7807 failure
  must surface BOTH `title` and `detail`.

## Behaviour that is contract

- `GET /v1/transfer-simulations` 503 is "read models not ready" (fail-closed),
  never an empty simulation; "ready, nothing advisable" is a separate empty state.
- The simulation has proposals only (no stock levels): the per-site table is
  derived from the options and says so.
- Approve = `POST /v1/transfers:approve` with an `Idempotency-Key` minted when
  the form opens and REUSED for retries of the same approval; a different option
  gets a new key. 409 / 422 / 503 problems are shown with title and detail;
  `replayed: true` is stated.
- Kong's CORS plugin must allow the `Idempotency-Key` header for the console
  origin or the browser preflight for Approve fails (warehouse-infra).
