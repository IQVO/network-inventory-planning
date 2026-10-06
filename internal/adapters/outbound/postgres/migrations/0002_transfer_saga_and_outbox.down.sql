-- 0002_transfer_saga_and_outbox.down.sql
DROP INDEX IF EXISTS idx_outbox_events_unpublished;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS transfer_audit;
DROP INDEX IF EXISTS idx_transfer_origin;
DROP INDEX IF EXISTS idx_transfer_state;
DROP TABLE IF EXISTS inter_warehouse_transfer;
