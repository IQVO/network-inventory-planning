-- 0004_rebalance_runs.up.sql: ADR 0006 — one row per scheduled
-- (observe-only) rebalance pass. A row records what the planner saw:
-- the snapshot watermark, the proposal/rejected counts, the outcome and
-- (on a fail-closed snapshot refusal) the reason. No approval, no
-- allocation command ever derives from a row; the run history exists so
-- a chronically stale read model is visible without scraping logs.

CREATE TABLE rebalance_runs (
    id                  BIGSERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    snapshot_as_of      TIMESTAMPTZ,
    proposal_count      INTEGER NOT NULL DEFAULT 0,
    rejected_count      INTEGER NOT NULL DEFAULT 0,
    outcome             TEXT NOT NULL CHECK (outcome IN ('COMPLETED','FAILED')),
    fail_closed_reason  TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX rebalance_runs_started_at_idx ON rebalance_runs (started_at DESC);
