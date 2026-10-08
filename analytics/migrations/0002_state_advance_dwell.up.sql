-- ADR 0009 amendment: true per-state dwell time.
--
-- TransferStateAdvanced gained an additive `dwell_seconds` payload field: the
-- seconds the saga spent in the `from` state. Additive and nullable on
-- purpose: rows projected before this migration, and events from a publisher
-- that does not yet send the field (or the creation entry, whose entry time
-- is unknown), have NULL here. NULL means "unknown" and is EXCLUDED from the
-- dwell percentiles; it is never rewritten to 0.
ALTER TABLE transfer_state_advances
    ADD COLUMN dwell_seconds BIGINT CHECK (dwell_seconds IS NULL OR dwell_seconds >= 0);
