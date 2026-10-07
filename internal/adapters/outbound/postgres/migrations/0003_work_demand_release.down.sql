ALTER TABLE inter_warehouse_transfer DROP CONSTRAINT transfer_picked_quantity_check;
ALTER TABLE inter_warehouse_transfer DROP COLUMN stow_allocations;
ALTER TABLE inter_warehouse_transfer DROP COLUMN picked_quantity;
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
        ('DRAFT','PROPOSED','APPROVED','ALLOCATING','ALLOCATED','UNFULFILLABLE','CANCELLED')),
    ADD CONSTRAINT inter_warehouse_transfer_quantity_check CHECK (quantity > 0),
    ADD CONSTRAINT inter_warehouse_transfer_origin_dest_check CHECK (origin_site_id <> destination_site_id),
    ADD CONSTRAINT inter_warehouse_transfer_reservation_id_check CHECK (reservation_id IS NULL OR state IN ('ALLOCATED'));
