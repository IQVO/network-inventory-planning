# ADR-0014: Ordering guard on the site_sku_demand read model

Status: Accepted

## Context

ADR 0002 gave each Phase-1 read model a supersession rule:
`site_capability` keeps the strictly greater `capability_revision`,
`published_capacity_plan` keeps the strictly later CloudEvents `time`. The
third one, `site_sku_demand` (order-management's `SiteSkuDemandChanged`,
keyed `source_order_id + line_no`), had none: its upsert replaced the row
whatever the incoming event's age.

The `processed_events` claim deduplicates on the CloudEvents `id`, so an
exact redelivery was already harmless. It does not protect against an
OLDER fact arriving after a newer one under a different id — a producer
re-publishing a stale fact, a replay after `processed_events` was pruned, or
a consumer-group offset reset. Each of those silently rolled demand back (for
example resurrecting a `REMOVED` line as `ACTIVE`), and the planner and the
approval's fail-closed validation then reasoned on demand that no longer
existed.

`SiteSkuDemandChanged` carries no sequence number. Its CloudEvents `time` is
the moment order-management produced the fact, and the consumer already
stores it in `site_sku_demand.as_of` (it is also the freshness watermark the
snapshot checks against `PLANNING_MAX_STALENESS`).

## Decision

1. The upsert only writes when the incoming event is not older than the
   stored row: `ON CONFLICT ... DO UPDATE ... WHERE EXCLUDED.as_of >=
   site_sku_demand.as_of`. An older event leaves the row untouched and the
   repository reports `applied = false`.
2. Ties apply. Kafka delivers one key (`<order>/line/<n>`) in partition
   order, so an equal-time message read later is the later fact; refusing it
   could drop a real update produced within the same clock tick.
3. No migration: `as_of` already holds exactly the event's occurred time
   (`consumers.go` sets `demand.AsOf = e.Time()`), so a second column would
   duplicate it. The primary key `(source_order_id, line_no)` makes the
   guard a single-row comparison; no index is needed.
4. A refused (older) event is not an error: the claim commits, the consumer
   logs `skipping SiteSkuDemandChanged older than the stored demand` at INFO
   with the event id, order, line and occurred time, and the offset moves on.

## Consequences

- Redelivered or replayed older demand facts can no longer roll the read
  model back. Proven by `TestSiteSkuDemandUpsertIgnoresOlderEvents`
  (testcontainers Postgres): an older ACTIVE fact and an older REMOVED
  tombstone are no-ops, a newer and a same-instant fact apply.
- The guard trusts order-management's clock. A producer clock that jumps
  backwards would freeze a line's demand until a fact newer than the stored
  one arrives; that is the same trust ADR 0002 already places in
  warehouse-planning's `time` for capacity plans.
- Replaying the topic from the beginning into an emptied table rebuilds the
  same final state (each line's newest fact wins).
