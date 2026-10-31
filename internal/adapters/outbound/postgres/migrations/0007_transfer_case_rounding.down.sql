-- 0007_transfer_case_rounding.down.sql
ALTER TABLE inter_warehouse_transfer
    DROP CONSTRAINT IF EXISTS inter_warehouse_transfer_case_rounding_shape,
    DROP CONSTRAINT IF EXISTS inter_warehouse_transfer_case_rounding_both_or_none,
    DROP COLUMN IF EXISTS requested_qty_eaches,
    DROP COLUMN IF EXISTS case_multiple;
