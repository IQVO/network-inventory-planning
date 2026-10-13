-- 0005_rebalance_catalogue.up.sql: the operator-managed Rebalance Policy
-- and Transfer Lane catalogues (ADR 0015). Additive: two new tables, no
-- change to any existing one. Both are NIP's own data (no sibling owns
-- inter-site transfer economics), written only through
-- PUT /v1/rebalance-policies and PUT /v1/transfer-lanes and read by the
-- scheduled rebalance pass.
--
-- revision is the optimistic-concurrency token: 1 on creation, +1 per
-- accepted change; every write is guarded by the revision the writer read.

CREATE TABLE rebalance_policies (
    site_id        TEXT        NOT NULL,
    sku            TEXT        NOT NULL,
    policy_version TEXT        NOT NULL CHECK (policy_version <> ''),
    safety_stock   INTEGER     NOT NULL CHECK (safety_stock >= 0),
    target_stock   INTEGER     NOT NULL CHECK (target_stock >= safety_stock),
    unit_priority  INTEGER     NOT NULL CHECK (unit_priority >= 0),
    revision       BIGINT      NOT NULL CHECK (revision >= 1),
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (site_id, sku)
);

CREATE TABLE transfer_lanes (
    origin_site_id      TEXT        NOT NULL,
    destination_site_id TEXT        NOT NULL,
    lead_time_ns        BIGINT      NOT NULL CHECK (lead_time_ns >= 0),
    unit_handling_cost  INTEGER     NOT NULL CHECK (unit_handling_cost >= 0),
    enabled             BOOLEAN     NOT NULL,
    revision            BIGINT      NOT NULL CHECK (revision >= 1),
    updated_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (origin_site_id, destination_site_id),
    CHECK (origin_site_id <> destination_site_id)
);
