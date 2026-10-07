-- 0003_work_demand_release.up.sql: ADR 0005 — fact-driven saga tail
-- (PICKED/IN_TRANSIT/ARRIVED/RECEIVED) and the work-demand release.
-- picked_quantity records a possibly-short pick (dispatch releases with
-- it); stow_allocations mirrors inventory-storage's TransferStockStowed
-- allocations[] once the transfer is RECEIVED.
--
-- 0002 declared its CHECK constraints inline and UNNAMED, so Postgres
-- auto-named them (<table>_check, <table>_check1, ...): they cannot be
-- dropped by a stable name. Drop every table-level check on the relation
-- and re-add each one NAMED, so later migrations have stable handles.

DO $$
DECLARE c text;
BEGIN
    FOR c IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'inter_warehouse_transfer'::regclass AND contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE inter_warehouse_transfer DROP CONSTRAINT %I', c);
    END LOOP;
END $$;

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_state_check CHECK (state IN
        ('DRAFT','PROPOSED','APPROVED','ALLOCATING','ALLOCATED',
         'PICKED','IN_TRANSIT','ARRIVED','RECEIVED',
         'UNFULFILLABLE','CANCELLED'));

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_quantity_check CHECK (quantity > 0);

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_origin_dest_check CHECK (origin_site_id <> destination_site_id);

-- The origin reservation survives the fact-driven tail (the hold exists
-- until the stock is stowed at the destination): 0002's
-- `reservation_id IS NULL OR state IN ('ALLOCATED')` widens to every
-- post-allocation state.
ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_reservation_id_check CHECK (
        reservation_id IS NULL OR state IN
        ('ALLOCATED','PICKED','IN_TRANSIT','ARRIVED','RECEIVED')
    );

ALTER TABLE inter_warehouse_transfer
    ADD COLUMN picked_quantity INTEGER,
    ADD COLUMN stow_allocations JSONB NOT NULL DEFAULT '[]';

-- A picked quantity only ever exists from PICKED onwards and never above
-- the planned quantity.
ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT transfer_picked_quantity_check CHECK (
        picked_quantity IS NULL AND state NOT IN ('PICKED','IN_TRANSIT','ARRIVED','RECEIVED')
        OR picked_quantity IS NOT NULL AND picked_quantity > 0
            AND picked_quantity <= quantity AND state IN ('PICKED','IN_TRANSIT','ARRIVED','RECEIVED')
    );
