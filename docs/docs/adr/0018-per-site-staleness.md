# ADR-0018: A stale fact excludes its site, never the whole planning snapshot

Status: Accepted (2026-10-25)

Amends the "Fail-closed PlanningSnapshot" passage of
[ADR-0002](./0002-phase1-read-models-and-simulation.md). Respects the real
Postgres and Kafka runtime. No wire change beyond problem wording, no migration,
no new env var (`PLANNING_MAX_STALENESS` keeps its meaning and default).

## Context

ADR-0002 says "Unknown-or-stale data excludes a candidate from planning rather
than inventing a safe-looking answer", and `PlanningSnapshot` documents that
"anything missing, stale or direction-disabled excludes that site". The code did
the opposite for staleness and direction: `BuildSnapshot` returned an error for
the FIRST stale capability, demand or capacity-plan fact, and for any
participating site disabled as origin or destination, refusing the whole
snapshot.

The producers emit facts on change only, so in a steady cluster some fact is
always older than `MaxStaleness` (default 10m). Found live: with that default,
`POST /v1/transfers:approve` answered 422 `facts-incomplete` ("stale site
capability for ABE8 (as of 2026-10-19...)") and `GET /v1/transfer-simulations`
answered 503 `read-models-incomplete` for EVERY site pair, including pairs whose
own facts were fresh.

## Decision

1. A fact older than `MaxStaleness` EXCLUDES ITS SITE from the snapshot, exactly
   like a missing fact. It never refuses the other sites. Exactly
   `MaxStaleness` old is fresh; one nanosecond older is stale.
   - capability: the NEWEST revision of the site is judged;
   - published capacity plan: the NEWEST plan of the site is judged;
   - demand: any stale demand row (ACTIVE or REMOVED) of the site excludes it; a
     zero watermark means "never applied", not stale.
2. `BuildSnapshot` still refuses (fail-closed) when
   - a read model is entirely empty,
   - `MaxStaleness` is not positive, or
   - NO site survives. The error names every stale exclusion, site by site.
3. The snapshot carries `StaleSites` (site to the stale facts, as of when), so a
   refusal can name them. A site simply missing a fact is absent without a
   reason, as before.
4. `AsOf` stays the OLDEST watermark of the INCLUDED facts; an excluded site
   never ages it.
5. Approval is fail-closed per role. `ValidateApproval` refuses with
   `ErrFactsIncomplete` (422 `facts-incomplete`) when the origin OR the
   destination is not in the snapshot, naming the site and the stale fact:
   `origin site WH1 is excluded from the planning snapshot: stale site capability
   (as of ..., older than 10m0s)`. Stale facts of any OTHER site do not matter.
6. Simulation and scheduled proposal generation plan over the surviving sites; a
   stale site yields no candidates and no row. A scheduled pass is FAILED only
   when no site survives.
7. A site disabled as origin or as destination is no longer a snapshot-level
   error. It participates, and each direction is gated where it is used:
   `usableLanes` (origin must send, destination must receive) in the planner,
   `ValidateApproval` (`not transfer-origin enabled` / `not transfer-destination
   enabled`) on approval, and the simulation reports both flags per site.
   Disabled as origin it may still receive, and vice versa.

## Consequences

- One quiet site no longer takes down approvals and simulations for the whole
  network; its own transfers are refused with a message that says why.
- The simulation can answer 200 with fewer sites than the network has. Callers
  that need a given site must look for it in the answer; `asOf` is the oldest
  watermark among the sites shown.
- Because producers emit on change only, a site whose facts rarely change is
  still excluded once they age past `MaxStaleness`. Raising
  `PLANNING_MAX_STALENESS` or re-emitting facts periodically (a producer matter)
  remains the lever; this ADR bounds the blast radius, it does not make quiet
  facts fresh. Demand is judged per row, so one old demand line excludes its
  site until it is replaced.
- 503 `read-models-incomplete` on the simulation and 422 `facts-incomplete` on
  approval now mean "no site left" and "this origin or destination is
  excluded/incomplete" respectively; they are no longer produced by an
  unrelated site's facts.
