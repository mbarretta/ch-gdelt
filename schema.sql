-- Ingest-state/audit table for the GDELT ingest Cloud Run function.
-- This is the ONLY table this project creates; gdelt.events and gdelt.mentions
-- already exist on the target ClickHouse Cloud instance and must never be
-- created, altered, or dropped here.
-- Run once by hand against the target ClickHouse Cloud instance.
CREATE TABLE gdelt.ingest_log
(
    file_timestamp DateTime,
    status Enum8('success' = 1, 'missing' = 2, 'error' = 3),
    rows_export UInt32,
    rows_mentions UInt32,
    processed_at DateTime DEFAULT now(),
    message String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY file_timestamp;
