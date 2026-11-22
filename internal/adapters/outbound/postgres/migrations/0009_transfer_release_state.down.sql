-- Reverse of 0009: drop RELEASING from both CHECK constraints. Any
-- transfer still RELEASING must be settled (CANCELLED) before this
-- migration can run.

ALTER TABLE inter_warehouse_transfer
    DROP CONSTRAINT inter_warehouse_transfer_state_check;

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_state_check CHECK (state IN
        ('DRAFT','PROPOSED','APPROVED','ALLOCATING','ALLOCATED',
         'PICKED','IN_TRANSIT','ARRIVED','RECEIVED',
         'UNFULFILLABLE','CANCELLED'));

ALTER TABLE inter_warehouse_transfer
    DROP CONSTRAINT inter_warehouse_transfer_reservation_id_check;

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_reservation_id_check CHECK (
        reservation_id IS NULL OR state IN
        ('ALLOCATED','PICKED','IN_TRANSIT','ARRIVED','RECEIVED')
    );
