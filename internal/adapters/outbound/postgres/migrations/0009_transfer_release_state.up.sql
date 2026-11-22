-- Cancel-after-allocation compensation (ADR 0022): the RELEASING state.
--
-- A transfer cancelled in ALLOCATED moves to RELEASING while
-- inventory-storage resolves the origin reservation, and settles to
-- CANCELLED on the TransferStockAllocationReleased fact. Both CHECK
-- constraints 0003 installed must admit the new state: the state enum
-- itself, and the reservation_id check (a RELEASING transfer still
-- carries the reservation it is releasing).

ALTER TABLE inter_warehouse_transfer
    DROP CONSTRAINT inter_warehouse_transfer_state_check;

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_state_check CHECK (state IN
        ('DRAFT','PROPOSED','APPROVED','ALLOCATING','ALLOCATED','RELEASING',
         'PICKED','IN_TRANSIT','ARRIVED','RECEIVED',
         'UNFULFILLABLE','CANCELLED'));

ALTER TABLE inter_warehouse_transfer
    DROP CONSTRAINT inter_warehouse_transfer_reservation_id_check;

ALTER TABLE inter_warehouse_transfer
    ADD CONSTRAINT inter_warehouse_transfer_reservation_id_check CHECK (
        reservation_id IS NULL OR state IN
        ('ALLOCATED','RELEASING','PICKED','IN_TRANSIT','ARRIVED','RECEIVED')
    );
