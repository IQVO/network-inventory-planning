-- 0007_transfer_case_rounding.up.sql: the case-multiple rounding report of an
-- approved transfer (ADR 0017). Additive: two nullable columns on
-- inter_warehouse_transfer, NULL for every existing row and for every SKU
-- without an active CASE level, so those transfers are exactly as before.
--
--   case_multiple         the active CASE level's units per pack (eaches)
--   requested_qty_eaches  the quantity asked for before rounding (the demand)
--
-- quantity stays the eaches the transfer moves (the rounded quantity), so
-- cases = quantity / case_multiple and remainder = requested - quantity are
-- derived, never stored twice.

ALTER TABLE inter_warehouse_transfer
    ADD COLUMN case_multiple        INTEGER,
    ADD COLUMN requested_qty_eaches INTEGER,
    ADD CONSTRAINT inter_warehouse_transfer_case_rounding_both_or_none
        CHECK ((case_multiple IS NULL) = (requested_qty_eaches IS NULL)),
    ADD CONSTRAINT inter_warehouse_transfer_case_rounding_shape
        CHECK (case_multiple IS NULL
               OR (case_multiple >= 2 AND requested_qty_eaches >= 1 AND quantity % case_multiple = 0));
