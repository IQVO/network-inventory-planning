-- 0005_rebalance_catalogue.down.sql: remove the two catalogue tables
-- 0005 created (rollback of this migration only; never run in a deploy).

DROP TABLE IF EXISTS transfer_lanes;
DROP TABLE IF EXISTS rebalance_policies;
