-- DESTRUCTIVE: drops every database object currently created by bootstrap.sql
-- and alters.sql, returning this project's schema to a clean slate.
--
-- Apply this file using the same ClickHouse connection/database context used to
-- apply bootstrap.sql. The unqualified event_count_by_actor table deliberately
-- mirrors bootstrap.sql.

-- Remove dependent objects before their targets and sources.
DROP VIEW IF EXISTS gdelt.event_count_by_actor_mv;
DROP DICTIONARY IF EXISTS gdelt.cameo_dict;

-- Remove tables created by bootstrap.sql.
DROP TABLE IF EXISTS gdelt.event_count_by_actor;
DROP TABLE IF EXISTS gdelt.ingest_log;
DROP TABLE IF EXISTS gdelt.mentions;
DROP TABLE IF EXISTS gdelt.events;

-- The dictionary uses this project-owned credential.
DROP USER IF EXISTS dict_reader;
