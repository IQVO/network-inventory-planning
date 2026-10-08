-- network-inventory-planning analytics read model (ADR 0009).
--
-- This is the ANALYTICAL database, separate from the OLTP database. It is
-- written only by cmd/nip-projector and read (read-only) by
-- cmd/nip-reports. Everything here is a projection derived from the
-- analytics event stream (warehouse.network-inventory-planning.analytics),
-- not a source of truth, and can be dropped and rebuilt from the topic.

-- Idempotency: every applied CloudEvents id is recorded here exactly once,
-- in the SAME transaction as its effect on a fact table. occurred_at is the
-- event's CloudEvents `time`.
CREATE TABLE analytics_processed_events (
    event_id    TEXT        PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per saga.TransferStateAdvanced: a single state transition.
--   from_state   the state the saga left; '' for the creation entry
--   to_state     the state it reached
--   age_seconds  seconds since the aggregate's creation at the transition
CREATE TABLE transfer_state_advances (
    event_id    TEXT        PRIMARY KEY,
    transfer_id TEXT        NOT NULL CHECK (transfer_id <> ''),
    from_state  TEXT        NOT NULL DEFAULT '',
    to_state    TEXT        NOT NULL CHECK (to_state <> ''),
    age_seconds BIGINT      NOT NULL CHECK (age_seconds >= 0),
    occurred_at TIMESTAMPTZ NOT NULL
);

-- One row per saga.TransferStuckDetected: a non-terminal transfer whose age
-- in its state exceeded the per-state threshold.
CREATE TABLE transfer_stuck_detections (
    event_id          TEXT        PRIMARY KEY,
    transfer_id       TEXT        NOT NULL CHECK (transfer_id <> ''),
    state             TEXT        NOT NULL CHECK (state <> ''),
    age_seconds       BIGINT      NOT NULL CHECK (age_seconds >= 0),
    threshold_seconds BIGINT      NOT NULL DEFAULT 0 CHECK (threshold_seconds >= 0),
    occurred_at       TIMESTAMPTZ NOT NULL
);

-- One row per saga.RebalanceRunCompleted: one scheduled rebalance pass.
CREATE TABLE rebalance_run_facts (
    event_id       TEXT        PRIMARY KEY,
    run_id         TEXT        NOT NULL CHECK (run_id <> ''),
    proposal_count INTEGER     NOT NULL CHECK (proposal_count >= 0),
    rejected_count INTEGER     NOT NULL CHECK (rejected_count >= 0),
    stale_facts    INTEGER     NOT NULL DEFAULT 0 CHECK (stale_facts >= 0),
    occurred_at    TIMESTAMPTZ NOT NULL
);

-- Every report filters on occurred_at.
CREATE INDEX idx_transfer_state_advances_occurred_at  ON transfer_state_advances (occurred_at);
CREATE INDEX idx_transfer_stuck_detections_occurred_at ON transfer_stuck_detections (occurred_at);
CREATE INDEX idx_rebalance_run_facts_occurred_at       ON rebalance_run_facts (occurred_at);
