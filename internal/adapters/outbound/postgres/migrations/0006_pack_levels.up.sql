-- 0006_pack_levels.up.sql: the local copy of product-master's pack levels
-- (ADR 0017), one row per (sku, level). Written only by the pack-level
-- consumer from ProductPackLevelDeclared / ProductPackLevelRetired; read by
-- the planning use cases to learn a SKU's active CASE units_per_pack.
-- Additive: one new table, no change to any existing one.
--
-- version is product-master's aggregate version that last changed the level:
-- an event is applied only when its version is greater (replay / late
-- delivery change nothing). effective_to is NULL while the level is open.

CREATE TABLE pack_levels (
    sku            TEXT        NOT NULL CHECK (sku <> ''),
    level          TEXT        NOT NULL CHECK (level IN ('EACH', 'INNER', 'CASE', 'PALLET')),
    gtin           TEXT        NOT NULL CHECK (gtin <> ''),
    units_per_pack BIGINT      NOT NULL CHECK (units_per_pack >= 1),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    version        BIGINT      NOT NULL CHECK (version >= 1),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sku, level)
);
