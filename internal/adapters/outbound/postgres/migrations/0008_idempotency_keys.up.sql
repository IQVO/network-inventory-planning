-- 0008_idempotency_keys.up.sql: fleet REST guideline section 2. One row per
-- Idempotency-Key; the key alone identifies it (global, not per route). The
-- row is written in the SAME transaction as the aggregate change and its
-- outbox rows, so a rolled-back request leaves no key behind.
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    fingerprint     TEXT NOT NULL,
    status          INTEGER NOT NULL,
    content_type    TEXT NOT NULL DEFAULT '',
    location        TEXT NOT NULL DEFAULT '',
    body            BYTEA NOT NULL DEFAULT ''::bytea,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Retention is at least 24 hours; the index serves a sweep by age.
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);
