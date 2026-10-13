# ADR-0016: Outbox relay attempt cap

Status: Accepted

Amends the per-key ordering consequence of
[ADR-0003](./0003-transfer-saga.md) ("per-key ordering is never violated by a
later row overtaking a failed earlier one"). Everything else in ADR-0003 (one
row per event and topic, written in the use case's transaction, drained by
one relay in `id` order) is unchanged.

## Context

The outbox relay (ADR-0003, `postgres.OutboxRelay`) sends rows one at a time
in `id` order and stops a pass at the first failed row, so a later row never
overtakes an earlier one. `postgres.WithMaxAttempts` already existed, but
`cmd/network-inventory-planning` never passed it, so the relay ran with the
default of 0: unlimited retries. The relay also logged nothing on a failed
send.

Unlimited retries are right for a broker outage, which ends. They are wrong
for a **poison row**, one the broker will never accept (a message over the
broker's size limit, a topic that cannot be created, a value the sink rejects
every time). Such a row fails every pass, every pass stops at it, and every
row behind it waits forever, for every key and both topics
(`warehouse.network-inventory-planning.events` and `.analytics`). The saga
stalls: no `TransferAllocationRequested`, no `WorkDemandReleased`, no
`TransferStateAdvanced` leaves the context, and nothing pages.

The fleet's ecosystem gap fix D12 settled the convention for every relay:
inventory-storage's existing attempt-cap semantics, the env var
`OUTBOX_MAX_ATTEMPTS` with default `20`, an ERROR log once a row is excluded,
and an architecture fitness test.

## Decision

1. **Every composition root that builds a relay passes an attempt cap.**
   `cmd/network-inventory-planning` (the only binary here that runs a relay;
   `cmd/mcp`, `cmd/nip-projector` and `cmd/nip-reports` run none) reads
   `OUTBOX_MAX_ATTEMPTS` and passes it as `postgres.WithMaxAttempts`. Default
   `20` (`postgres.DefaultOutboxMaxAttempts`); `0` means unlimited, the old
   behaviour. A negative or unparsable value logs a WARN and uses the default,
   so a typo cannot silently remove the cap. The chart exposes it as
   `config.outboxMaxAttempts` (default `"20"`).
2. **An excluded row stays where it is.** A row whose `attempts` has reached
   the cap is excluded from the claim (`AND attempts < $max`). It is **not**
   deleted and **not** moved: it stays in `outbox_events` with
   `published_at IS NULL`, its `attempts` and its `last_error`, for
   inspection. There is no DLQ topic: a row the broker rejects would most
   likely be rejected on a DLQ topic of the same broker too, and the table
   already is the inspectable store.
3. **The failure that reaches the cap is logged at ERROR** with `outbox_id`,
   `topic`, `event_type`, `key`, `attempts`, `max_attempts` and the send
   error, once, after the attempt is committed. Failures below the cap are
   logged at WARN. The relay takes the process logger
   (`postgres.WithRelayLogger`).
4. **Re-arming is an operator action**: after fixing the cause,
   `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` makes the row
   claimable again on the next pass (runbook, "An outbox row hit the attempt
   cap").
5. **Sensor.** The architecture fitness test `TestOutboxRelaysPassAttemptCap`
   fails when a non-test file under `cmd/` calls `NewOutboxRelay` without a
   `WithMaxAttempts(...)` argument; `TestOutboxRelayCapSensorFailsOnBadFixtures`
   proves the sensor can fail.

## Consequences

- **Per-key ordering is no longer absolute.** Once a row is excluded, later
  rows overtake it, **including rows with the same key**: a consumer can see
  a transfer's `TransferStateAdvanced` before the earlier event of that
  transfer that was excluded, or see the excluded one late after it is
  re-armed. This is the deliberate trade: the cap exists so one poison row
  cannot stop the whole relay, for every key, forever. Consumers already
  deduplicate on the CloudEvents `id` and tolerate redelivery; a re-armed row
  is a late delivery of the same `id`.
- Ordering among rows that are **not** excluded is unchanged: within a pass
  the relay still stops at the first failure, so a transient failure below
  the cap never lets a later row overtake.
- With the defaults (1 s interval, cap 20) a row that fails every send is
  excluded after roughly 20 seconds of continuous failure. A broker outage
  longer than that also excludes the rows at the head of the queue, one per
  pass, as each reaches the cap: each one is logged at ERROR and must be
  re-armed. Raise `OUTBOX_MAX_ATTEMPTS` (or set `0`) where long broker outages
  are expected and a stalled relay is preferable to manual re-arming.
- An excluded row is visible with
  `SELECT id, topic, event_type, key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= <cap>;`

## Verification

- `go test -tags=integration ./internal/adapters/outbound/postgres/postgres_integration/ -run TestOutboxRelay -count=1`:
  `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain`,
  `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows`.
- `go test ./cmd/network-inventory-planning/ -run 'TestOutboxMaxAttemptsFromEnv|TestDefaultOutboxMaxAttemptsIsTwenty'`.
- `go test ./internal/architecture/ -run OutboxRelay`.
- `python3 charts/network-inventory-planning/tests/test_env_wiring.py`
  asserts the api Deployment renders `OUTBOX_MAX_ATTEMPTS` (`"20"` by
  default, `"0"` as is).
