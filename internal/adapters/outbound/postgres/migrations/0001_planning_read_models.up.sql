-- 0001_planning_read_models.up.sql: Phase-1 local read models for the
-- inbound Kafka consumers, plus the shared processed-events idempotency
-- guard. These tables are owned entirely by this context -- they are NOT
-- shared aggregates, just the local facts the fail-closed PlanningSnapshot
-- reads.

-- processed_events: idempotency guard shared by every inbound Kafka
-- consumer in this service. (consumer, event_id) is claimed with an
-- INSERT ... ON CONFLICT DO NOTHING in the SAME transaction as the event's
-- side effects (ports.UnitOfWork), so a rollback un-claims it; zero rows
-- affected means a previous, committed handling exists, so the caller
-- skips reprocessing it.
CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- site_capability: last-write-wins snapshot of one site's transfer
-- capability, mirrored from facility-layout's SiteCapabilityChanged
-- (keyed site_code; a strictly greater capability_revision supersedes).
CREATE TABLE site_capability (
    site_id                     TEXT        NOT NULL PRIMARY KEY,
    transfer_origin_enabled     BOOLEAN     NOT NULL,
    transfer_destination_enabled BOOLEAN    NOT NULL,
    capability_revision         BIGINT      NOT NULL,
    as_of                       TIMESTAMPTZ NOT NULL
);

-- site_sku_demand: one row per source order line, mirrored from
-- order-management's SiteSkuDemandChanged (keyed source_order_id+line_no;
-- state ACTIVE/REMOVED, a REMOVED fact tombstones the line's demand).
CREATE TABLE site_sku_demand (
    source_order_id    TEXT        NOT NULL,
    line_no            INTEGER     NOT NULL,
    site_id            TEXT        NOT NULL,
    sku                TEXT        NOT NULL,
    demanded_units     INTEGER     NOT NULL,
    due_at             TIMESTAMPTZ NOT NULL,
    state              TEXT        NOT NULL CHECK (state IN ('ACTIVE', 'REMOVED')),
    assignment_version TEXT        NOT NULL,
    as_of              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_order_id, line_no)
);

CREATE INDEX idx_site_sku_demand_site_due ON site_sku_demand (site_id, due_at);

-- published_capacity_plan: one row per plan_id, mirrored from
-- warehouse-planning's CapacityPlanPublished (last-write-wins on the
-- CloudEvents time). A legacy event without site_id is EXCLUDED by the
-- consumer -- never stored, never inferred.
CREATE TABLE published_capacity_plan (
    plan_id              TEXT             NOT NULL PRIMARY KEY,
    site_id              TEXT             NOT NULL,
    location             TEXT             NOT NULL,
    path_id              TEXT             NOT NULL,
    window_start         TIMESTAMPTZ      NOT NULL,
    window_end           TIMESTAMPTZ      NOT NULL,
    assigned_demand      DOUBLE PRECISION NOT NULL CHECK (assigned_demand >= 0),
    capacity_over_window DOUBLE PRECISION NOT NULL CHECK (capacity_over_window >= 0),
    shortage             DOUBLE PRECISION NOT NULL CHECK (shortage >= 0),
    published_at         TIMESTAMPTZ      NOT NULL,
    CHECK (window_end > window_start)
);

CREATE INDEX idx_published_capacity_plan_site_window ON published_capacity_plan (site_id, window_start);
