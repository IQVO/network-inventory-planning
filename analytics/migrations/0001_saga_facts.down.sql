DROP INDEX IF EXISTS idx_rebalance_run_facts_occurred_at;
DROP INDEX IF EXISTS idx_transfer_stuck_detections_occurred_at;
DROP INDEX IF EXISTS idx_transfer_state_advances_occurred_at;
DROP TABLE IF EXISTS rebalance_run_facts;
DROP TABLE IF EXISTS transfer_stuck_detections;
DROP TABLE IF EXISTS transfer_state_advances;
DROP TABLE IF EXISTS analytics_processed_events;
