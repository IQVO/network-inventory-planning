---
paths:
  - "cmd/nip-projector/**"
  - "cmd/nip-reports/**"
  - "analytics/**"
  - "internal/analytics/**"
  - "internal/adapters/outbound/analyticsstore/**"
  - "internal/adapters/inbound/kafka/analytics_consumer*.go"
  - "internal/adapters/inbound/http/reports_handler*.go"
---

# Analytics read side (projector + reports)

The consuming half of the saga-health analytics stream that ADR 0007 publishes.
Record: `docs/docs/adr/0009-analytics-read-side.md`. Do not change the publisher
(`internal/adapters/outbound/kafka/analytics_publisher.go`) or the `.events`
topic from here.

- `cmd/nip-projector` is the ONLY writer of the SEPARATE analytical database
  (`ANALYTICS_DATABASE_URL`, `ANALYTICS_MIGRATIONS_PATH` default
  `analytics/migrations`, `KAFKA_BROKERS`, `ANALYTICS_CONSUMER_GROUP` default
  `network-inventory-planning-analytics`, `ADMIN_ADDR` `:8091`). It never opens
  the OLTP database. `cmd/nip-reports` (`:8092`) is read-only over a read-only
  pool (`ANALYTICS_READER_DATABASE_URL`, falling back to `ANALYTICS_DATABASE_URL`).
- Dispatch on the FULL CloudEvents `type`; an unknown type is skipped and logged,
  never fatal. Dedupe on the CloudEvents `id` and the fact insert are ONE
  analytical-database transaction (`Projection.Apply`).
- Delivery policy: dead-letter ONLY deterministic poison (bad payload, subject
  mismatch, Postgres class 22/23); retry transient failures forever with capped
  backoff, never dead-letter them; skip non-CloudEvents with a sampled WARN.
- `internal/analytics/report` is pure and imports no other internal package
  (arch-test); SQL only counts/sums/`percentile_cont`, everything else lives
  there and is in `make mutation` and the coverage packages. One contract suite
  (`analyticsstore/contract_test.go`) runs against the memory store and Postgres.
- `analytics/migrations` is additive and has its own down files; never edit an
  applied migration, add the next number.
- Reports are unauthenticated by fleet decision and make no call to any sibling.
  Wire fields are snake_case; days are UTC; ranges are `[from, to)`.
- The chart block `analytics:` is opt-in; keep `tests/test_service_selectors.py`
  and `tests/test_env_wiring.py` green when touching it.
