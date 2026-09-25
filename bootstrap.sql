-- Initial, idempotent setup for objects this project owns in the `gdelt`
-- database. Safe to re-run any number of times: every statement below is
-- guarded with IF NOT EXISTS, so re-applying this file against an
-- already-provisioned instance is a no-op.
--
-- Run once by hand against the target ClickHouse Cloud instance (see
-- README.md's deployment runbook). Schema changes made after this initial
-- setup that must not destroy existing data belong in alters.sql instead.

CREATE TABLE IF NOT EXISTS gdelt.events (
    `GLOBALEVENTID` UInt32,
    `Day` Date,
    `MonthYear` UInt32,
    `Year` UInt16,
    `FractionDate` Float32,
    `Actor1Code` String,
    `Actor1Name` String,
    `Actor1CountryCode` String,
    `Actor1KnownGroupCode` String,
    `Actor1EthnicCode` String,
    `Actor1Religion1Code` String,
    `Actor1Religion2Code` String,
    `Actor1Type1Code` String,
    `Actor1Type2Code` String,
    `Actor1Type3Code` String,
    `Actor2Code` String,
    `Actor2Name` String,
    `Actor2CountryCode` String,
    `Actor2KnownGroupCode` String,
    `Actor2EthnicCode` String,
    `Actor2Religion1Code` String,
    `Actor2Religion2Code` String,
    `Actor2Type1Code` String,
    `Actor2Type2Code` String,
    `Actor2Type3Code` String,
    `IsRootEvent` Bool,
    `EventCode` String,
    `EventBaseCode` String,
    `EventRootCode` String,
    `QuadClass` UInt8,
    `GoldsteinScale` Decimal32(1),
    `NumMentions` UInt32,
    `NumSources` UInt32,
    `NumArticles` UInt32,
    `AvgTone` Float64,
    `Actor1Geo_Type` Int8,
    `Actor1Geo_FullName` String,
    `Actor1Geo_CountryCode` String,
    `Actor1Geo_ADM1Code` String,
    `Actor1Geo_ADM2Code` String,
    `Actor1Geo_Lat` Nullable(Float64),
    `Actor1Geo_Long` Nullable(Float64),
    `Actor1Geo_FeatureID` String,
    `Actor2Geo_Type` Int8,
    `Actor2Geo_FullName` String,
    `Actor2Geo_CountryCode` String,
    `Actor2Geo_ADM1Code` String,
    `Actor2Geo_ADM2Code` String,
    `Actor2Geo_Lat` Nullable(Float64),
    `Actor2Geo_Long` Nullable(Float64),
    `Actor2Geo_FeatureID` String,
    `ActionGeo_Type` Int8,
    `ActionGeo_FullName` String,
    `ActionGeo_CountryCode` String,
    `ActionGeo_ADM1Code` String,
    `ActionGeo_ADM2Code` String,
    `ActionGeo_Lat` Nullable(Float64),
    `ActionGeo_Long` Nullable(Float64),
    `ActionGeo_FeatureID` String,
    `DATEADDED` DateTime,
    `SOURCEURL` String
)
PARTITION BY toYYYYMM(DATEADDED)
ORDER BY (toYYYYMMDD(DATEADDED));

CREATE TABLE IF NOT EXISTS gdelt.mentions (
    `GLOBALEVENTID` UInt32,
    `EventTimeDate` DateTime,
    `MentionTimeDate` DateTime,
    `MentionType` UInt8,
    `MentionSourceName` String,
    `MentionIdentifier` String,
    `SentenceID` UInt32,
    `Actor1CharOffset` Int32,
    `Actor2CharOffset` Int32,
    `ActionCharOffset` Int32,
    `InRawText` UInt8,
    `Confidence` UInt8,
    `MentionDocLen` UInt32,
    `MentionDocTone` Float64,
    `MentionDocTranslationInfo` String,
    `Extras` String
)
PARTITION BY toYYYYMM(EventTimeDate)
ORDER BY (toYYYYMMDD(EventTimeDate));

-- Ingest-state/audit table for the GDELT ingest Cloud Run function.
CREATE TABLE IF NOT EXISTS gdelt.ingest_log
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

CREATE TABLE IF NOT EXISTS gdelt.event_count_by_actor (
    `Day` Date,
    `ActorName` String,
    `Count` UInt32,
)
ENGINE = SummingMergeTree
ORDER BY (Day, ActorName);

CREATE MATERIALIZED VIEW IF NOT EXISTS gdelt.event_count_by_actor_mv TO gdelt.event_count_by_actor
    AS SELECT
        Day,
        arrayJoin([Actor1Name, Actor2Name]) as ActorName,
        count() AS Count
    FROM gdelt.events
    WHERE ActorName <> ''
    GROUP BY Day, ActorName;

-- ${DICT_READER_PASSWORD} is substituted at apply time (see README) so the
-- real password never lives in this file.
CREATE USER IF NOT EXISTS dict_reader IDENTIFIED BY '${DICT_READER_PASSWORD}';
GRANT SELECT ON gdelt.cameo TO dict_reader;

CREATE DICTIONARY IF NOT EXISTS gdelt.cameo_dict
(
    code UInt16,
    description String
)
PRIMARY KEY code
SOURCE(
    CLICKHOUSE(DB 'gdelt' TABLE 'cameo' USER 'dict_reader' PASSWORD '${DICT_READER_PASSWORD}')
)
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);
