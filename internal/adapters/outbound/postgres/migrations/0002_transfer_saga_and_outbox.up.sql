-- 0002_transfer_saga_and_outbox.up.sql: Phase-2 InterWarehouseTransfer
-- saga aggregate, its event-sourced audit trail, and the transactional
-- outbox (same shape as inventory-storage's ADR-0017 outbox_events).

-- inter_warehouse_transfer: the saga aggregate row. state is the CHECK'd
-- lifecycle enum; idempotency_key is UNIQUE — the transfer-level
-- idempotency of the approval endpoint (the same Idempotency-Key header
-- replays the FIRST transfer, never mints a second one). version drives
-- optimistic concurrency on UpdateState.
CREATE TABLE inter_warehouse_transfer (
    transfer_id       TEXT        NOT NULL PRIMARY KEY,
    idempotency_key   TEXT        NOT NULL UNIQUE,
    origin_site_id    TEXT        NOT NULL,
    destination_site_id TEXT      NOT NULL,
    sku               TEXT        NOT NULL,
    quantity          INTEGER     NOT NULL CHECK (quantity > 0),
    policy_version    TEXT        NOT NULL,
    operator_reason   TEXT        NOT NULL DEFAULT '',
    proposal_as_of    TIMESTAMPTZ NOT NULL,
    expires_at        TIMESTAMPTZ NOT NULL,
    state             TEXT        NOT NULL CHECK (state IN
                        ('DRAFT','PROPOSED','APPROVED','ALLOCATING','ALLOCATED','UNFULFILLABLE','CANCELLED')),
    reservation_id    TEXT,
    allocations       JSONB       NOT NULL DEFAULT '[]',
    allocation_expires_at TIMESTAMPTZ,
    rejection_reason  TEXT        CHECK (rejection_reason IN
                        ('ORIGIN_SITE_UNKNOWN','INSUFFICIENT_USABLE','IDEMPOTENCY_CONFLICT')),
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL,
    version           BIGINT      NOT NULL DEFAULT 1,
    CHECK (origin_site_id <> destination_site_id),
    CHECK (reservation_id IS NULL OR state IN ('ALLOCATED'))
);

CREATE INDEX idx_transfer_state ON inter_warehouse_transfer (state);
CREATE INDEX idx_transfer_origin ON inter_warehouse_transfer (origin_site_id, sku);

-- transfer_audit: the immutable event-sourced audit trail. Rows are only
-- ever INSERTed (append-only); seq is per-transfer and assigned in
-- insertion order via the serial.
CREATE TABLE transfer_audit (
    transfer_id  TEXT        NOT NULL REFERENCES inter_warehouse_transfer (transfer_id) ON DELETE CASCADE,
    seq          BIGINT      NOT NULL,
    from_state   TEXT        NOT NULL,
    to_state     TEXT        NOT NULL,
    event        TEXT        NOT NULL,
    reason       TEXT        NOT NULL DEFAULT '',
    occurred_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transfer_id, seq)
);

-- outbox_events: one already-encoded Kafka message per event x topic. The
-- approval transaction inserts the rows; the background relay drains them
-- (published_at set) in id order. Identical to inventory-storage's
-- migration 0005 so operational tooling transfers.
CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
