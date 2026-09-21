-- Append-only log of schema changes applied after bootstrap.sql's initial
-- setup, for objects this project owns. Each entry below must be additive
-- and non-destructive -- never DROP or otherwise remove existing data.

-- Add new entries at the bottom, each as a dated comment followed by its
-- statement(s). Prefer guards (e.g. ADD COLUMN IF NOT EXISTS) so re-running
-- this whole file against an instance that already has some entries applied
-- stays safe. Run by hand against the target ClickHouse Cloud instance,
-- after bootstrap.sql, whenever a new entry is added (see README.md).
