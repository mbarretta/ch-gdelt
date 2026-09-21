"""GDELT-to-ClickHouse Cloud Run ingest function.

Polls GDELT's ``lastupdate.txt`` (published on a 15-minute cadence), computes
every 15-minute-boundary timestamp that has not yet been successfully
processed, and loads the ``export`` and ``mentions`` streams for each into an
existing ClickHouse Cloud ``gdelt`` database. GKG is explicitly out of scope.

See ``.claude/plans/ok-i-want-a-nifty-waffle.md`` for the full design
rationale (publish cadence, gap-detection strategy, idempotency scheme).
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import clickhouse_connect
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdelt_ingest")

LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
MASTERFILELIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

TIMESTAMP_RE = re.compile(r"\d{14}")
GDELT_BOUNDARY = timedelta(minutes=15)

DATABASE = "gdelt"
EVENTS_TABLE = "events"
MENTIONS_TABLE = "mentions"
INGEST_LOG_TABLE = "ingest_log"

_KIND_SUFFIXES = {
    "export": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}


# --------------------------------------------------------------------------
# Field value converters
# --------------------------------------------------------------------------

def _to_int(value):
    return int(value) if value not in (None, "") else None


def _to_float(value):
    return float(value) if value not in (None, "") else None


def _to_str(value):
    return value if value is not None else ""


def _to_date(value):
    return datetime.strptime(value, "%Y%m%d").date() if value else None


def _to_datetime(value):
    return _as_utc(datetime.strptime(value, "%Y%m%d%H%M%S")) if value else None


def _as_utc(value):
    """Normalize a datetime that represents a UTC instant to timezone-aware
    UTC, whether it arrived naive (GDELT string parsing, or a ClickHouse read
    under the client's default naive_utc tz_mode) or already aware.

    GDELT's 14-digit timestamps and every DateTime column this project reads
    or writes are always UTC; this makes that explicit so clickhouse_connect
    never has to guess -- see _write_column_binary in clickhouse_connect's
    DateTime type, which calls the naive datetime.timestamp() (interpreted
    against the *local system* timezone) unless the value is already
    timezone-aware.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# --------------------------------------------------------------------------
# Explicit static source-ordinal -> GDELT 2.0 field name mapping.
#
# Ordinals and names were verified once during development against local
# GDELT sample files (a 61-tab-delimited-column export and a 16-column
# mentions file), cross-referenced with the GDELT 2.0 Event/Mention codebook
# field layout. Those sample files are gitignored and not part of this repo,
# so this verification is not reproducible from a fresh clone; "Day" (export
# ordinal 2) and "DATEADDED"/"EventTimeDate"/"MentionTimeDate" match the
# naming used in the architecture doc and this task's acceptance criteria.
# --------------------------------------------------------------------------

EVENTS_COLUMN_SPEC = [
    (1, "GLOBALEVENTID", _to_int),
    (2, "Day", _to_date),
    (3, "MonthYear", _to_int),
    (4, "Year", _to_int),
    (5, "FractionDate", _to_float),
    (6, "Actor1Code", _to_str),
    (7, "Actor1Name", _to_str),
    (8, "Actor1CountryCode", _to_str),
    (9, "Actor1KnownGroupCode", _to_str),
    (10, "Actor1EthnicCode", _to_str),
    (11, "Actor1Religion1Code", _to_str),
    (12, "Actor1Religion2Code", _to_str),
    (13, "Actor1Type1Code", _to_str),
    (14, "Actor1Type2Code", _to_str),
    (15, "Actor1Type3Code", _to_str),
    (16, "Actor2Code", _to_str),
    (17, "Actor2Name", _to_str),
    (18, "Actor2CountryCode", _to_str),
    (19, "Actor2KnownGroupCode", _to_str),
    (20, "Actor2EthnicCode", _to_str),
    (21, "Actor2Religion1Code", _to_str),
    (22, "Actor2Religion2Code", _to_str),
    (23, "Actor2Type1Code", _to_str),
    (24, "Actor2Type2Code", _to_str),
    (25, "Actor2Type3Code", _to_str),
    (26, "IsRootEvent", _to_int),
    (27, "EventCode", _to_str),
    (28, "EventBaseCode", _to_str),
    (29, "EventRootCode", _to_str),
    (30, "QuadClass", _to_int),
    (31, "GoldsteinScale", _to_float),
    (32, "NumMentions", _to_int),
    (33, "NumSources", _to_int),
    (34, "NumArticles", _to_int),
    (35, "AvgTone", _to_float),
    (36, "Actor1Geo_Type", _to_int),
    (37, "Actor1Geo_FullName", _to_str),
    (38, "Actor1Geo_CountryCode", _to_str),
    (39, "Actor1Geo_ADM1Code", _to_str),
    (40, "Actor1Geo_ADM2Code", _to_str),
    (41, "Actor1Geo_Lat", _to_float),
    (42, "Actor1Geo_Long", _to_float),
    (43, "Actor1Geo_FeatureID", _to_str),
    (44, "Actor2Geo_Type", _to_int),
    (45, "Actor2Geo_FullName", _to_str),
    (46, "Actor2Geo_CountryCode", _to_str),
    (47, "Actor2Geo_ADM1Code", _to_str),
    (48, "Actor2Geo_ADM2Code", _to_str),
    (49, "Actor2Geo_Lat", _to_float),
    (50, "Actor2Geo_Long", _to_float),
    (51, "Actor2Geo_FeatureID", _to_str),
    (52, "ActionGeo_Type", _to_int),
    (53, "ActionGeo_FullName", _to_str),
    (54, "ActionGeo_CountryCode", _to_str),
    (55, "ActionGeo_ADM1Code", _to_str),
    (56, "ActionGeo_ADM2Code", _to_str),
    (57, "ActionGeo_Lat", _to_float),
    (58, "ActionGeo_Long", _to_float),
    (59, "ActionGeo_FeatureID", _to_str),
    (60, "DATEADDED", _to_datetime),
    (61, "SOURCEURL", _to_str),
]

MENTIONS_COLUMN_SPEC = [
    (1, "GLOBALEVENTID", _to_int),
    (2, "EventTimeDate", _to_datetime),
    (3, "MentionTimeDate", _to_datetime),
    (4, "MentionType", _to_int),
    (5, "MentionSourceName", _to_str),
    (6, "MentionIdentifier", _to_str),
    (7, "SentenceID", _to_int),
    (8, "Actor1CharOffset", _to_int),
    (9, "Actor2CharOffset", _to_int),
    (10, "ActionCharOffset", _to_int),
    (11, "InRawText", _to_int),
    (12, "Confidence", _to_int),
    (13, "MentionDocLen", _to_int),
    (14, "MentionDocTone", _to_float),
    (15, "MentionDocTranslationInfo", _to_str),
    (16, "Extras", _to_str),
]

def _assert_complete_spec(column_spec, expected_length):
    ordinals = [ordinal for ordinal, _, _ in column_spec]
    if ordinals != list(range(1, expected_length + 1)):
        raise AssertionError(
            f"column spec must cover ordinals 1..{expected_length} in order, got {ordinals}"
        )


_assert_complete_spec(EVENTS_COLUMN_SPEC, 61)
_assert_complete_spec(MENTIONS_COLUMN_SPEC, 16)


def expected_column_names(column_spec):
    return [name for _, name, _ in column_spec]


# --------------------------------------------------------------------------
# ClickHouse client
# --------------------------------------------------------------------------

def get_client():
    """Build a clickhouse-connect client from CLICKHOUSE_URL/USER/PASSWORD env vars."""
    url = os.environ["CLICKHOUSE_URL"]
    parsed = urlparse(url)
    if not parsed.hostname:
        raise RuntimeError(f"CLICKHOUSE_URL is not a valid URL: {url!r}")
    return clickhouse_connect.get_client(
        host=parsed.hostname,
        port=parsed.port,
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=parsed.scheme == "https",
        # Defense in depth: every datetime this module hands to
        # clickhouse-connect is already timezone-aware UTC (see _as_utc), but
        # pin the session's own timezone too so a naive value can never be
        # silently reinterpreted using this process's local system timezone.
        settings={"session_timezone": "UTC"},
    )


def validate_destination_columns(client, table, column_spec):
    """Confirm, by name (never positional order), that every column this spec
    expects to write actually exists in the live destination table, and
    return a {name: type} map fetched via DESCRIBE TABLE.

    Raises RuntimeError loudly if any expected destination column is missing.
    """
    describe = client.query(f"DESCRIBE TABLE {table}")
    actual = {row[0]: row[1] for row in describe.result_rows}
    missing = [name for name in expected_column_names(column_spec) if name not in actual]
    if missing:
        raise RuntimeError(f"{table} is missing expected destination column(s): {missing}")
    return actual


# --------------------------------------------------------------------------
# lastupdate.txt / masterfilelist.txt / candidate URL logic
# --------------------------------------------------------------------------

def _kind_from_url(url):
    filename = url.rsplit("/", 1)[-1]
    for kind, suffix in _KIND_SUFFIXES.items():
        if filename.endswith(suffix):
            return kind
    return None


def fetch_lastupdate(session=None):
    """GET lastupdate.txt, parse its 3 lines into per-kind (size, md5, url),
    and derive the 14-digit latest timestamp from the export URL's filename.

    Returns (files, latest_timestamp) where files is
    {"export": {"size": int, "md5": str, "url": str}, "mentions": {...}, "gkg": {...}}.
    """
    session = session or requests
    resp = session.get(LASTUPDATE_URL, timeout=30)
    resp.raise_for_status()

    files = {}
    for line in resp.text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        size, md5, url = line.split(" ", 2)
        kind = _kind_from_url(url)
        if kind is None:
            continue
        files[kind] = {"size": int(size), "md5": md5, "url": url}

    if "export" not in files:
        raise RuntimeError("lastupdate.txt did not include an export file entry")

    match = TIMESTAMP_RE.match(files["export"]["url"].rsplit("/", 1)[-1])
    if not match:
        raise RuntimeError("could not derive a 14-digit timestamp from the export URL")
    latest_timestamp = _as_utc(datetime.strptime(match.group(0), "%Y%m%d%H%M%S"))

    return files, latest_timestamp


def candidate_timestamps(last_success, latest):
    """Every 15-minute UTC boundary strictly after last_success up to and
    including latest, in order. Pure datetime arithmetic -- never fetches
    masterfilelist.txt."""
    candidates = []
    ts = last_success + GDELT_BOUNDARY
    while ts <= latest:
        candidates.append(ts)
        ts += GDELT_BOUNDARY
    return candidates


def build_url(template_url, timestamp):
    """Substitute the leading 14-digit timestamp in a known-good URL to
    construct the URL for another timestamp."""
    ts_str = timestamp.strftime("%Y%m%d%H%M%S")
    return TIMESTAMP_RE.sub(ts_str, template_url, count=1)


def lookup_in_masterfilelist(timestamp, kind, session=None):
    """Fallback only: stream masterfilelist.txt line-by-line (never loading
    the whole file into memory) to resolve the real filename/URL for this
    timestamp+kind. Returns the URL, or None if genuinely absent."""
    session = session or requests
    suffix = _KIND_SUFFIXES[kind]
    target_filename = f"{timestamp.strftime('%Y%m%d%H%M%S')}{suffix}"

    resp = session.get(MASTERFILELIST_URL, stream=True, timeout=120)
    resp.raise_for_status()
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line.endswith(target_filename):
            continue
        parts = line.split(" ", 2)
        if len(parts) == 3:
            return parts[2]
    return None


def fetch_and_parse(url, session=None):
    """Download a GDELT zip in-memory and split it into raw TSV rows.
    Returns None on 404 (caller decides whether to fall back)."""
    session = session or requests
    resp = session.get(url, timeout=60)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        member = zf.namelist()[0]
        data = zf.read(member).decode("utf-8", errors="replace")

    return [line.split("\t") for line in data.splitlines() if line.strip()]


def _fetch_with_fallback(timestamp, kind, url, session=None):
    """fetch_and_parse(url); on 404, fall back to masterfilelist.txt to
    resolve the real URL and retry once. Returns rows, or None if the file
    is genuinely unavailable."""
    rows = fetch_and_parse(url, session=session)
    if rows is not None:
        return rows

    logger.warning("Computed URL 404 for %s @ %s; falling back to masterfilelist.txt", kind, timestamp)
    resolved_url = lookup_in_masterfilelist(timestamp, kind, session=session)
    if resolved_url is None:
        return None
    return fetch_and_parse(resolved_url, session=session)


# --------------------------------------------------------------------------
# Row transform + insert
# --------------------------------------------------------------------------

def transform_row(raw_row, column_spec):
    """Map one raw TSV row to {destination_column_name: converted_value}
    using the explicit static ordinal->name mapping."""
    result = {}
    for ordinal, name, convert in column_spec:
        idx = ordinal - 1
        raw_value = raw_row[idx] if idx < len(raw_row) else ""
        result[name] = convert(raw_value)
    return result


def rows_for_insert(raw_rows, column_spec):
    """Build the explicit (column_names, data) pair for a clickhouse-connect
    insert -- never a bare positional insert."""
    names = expected_column_names(column_spec)
    data = [[transform_row(raw_row, column_spec)[name] for name in names] for raw_row in raw_rows]
    return names, data


def _dedup_token(table, timestamp):
    """Deterministic insert_deduplication_token for one (table, timestamp)
    pair. Retrying a timestamp whose insert into this table already landed
    -- e.g. because a later step in the same attempt failed -- resends this
    same token, so ClickHouse recognizes the duplicate and skips it instead
    of writing the rows twice. This is the pipeline's whole idempotency
    story: no DELETE, no mutation, no wait-for-completion polling."""
    return f"{table}:{timestamp:%Y%m%d%H%M%S}"


# --------------------------------------------------------------------------
# Per-timestamp orchestration
# --------------------------------------------------------------------------

def _write_ingest_log(client, timestamp, status, rows_export, rows_mentions, message=""):
    client.insert(
        f"{DATABASE}.{INGEST_LOG_TABLE}",
        [[timestamp, status, rows_export, rows_mentions, message]],
        column_names=["file_timestamp", "status", "rows_export", "rows_mentions", "message"],
    )


def process_timestamp(client, timestamp, export_url, mentions_url, session=None):
    """Orchestrate one timestamp end to end: fetch (with masterfilelist
    fallback), transform, insert, and log.

    Idempotency comes entirely from ClickHouse's own insert deduplication
    (see _dedup_token) -- no DELETE, no mutation, no wait-for-completion
    polling. Retrying a timestamp whose events insert already landed (e.g.
    because the mentions insert failed afterward) resends the identical
    token and ClickHouse skips the duplicate.

    Never raises for expected outcomes (missing/error) -- always returns a
    result dict and always writes exactly one gdelt.ingest_log row for a
    processed timestamp, so a bad file never blocks the rest of a catch-up
    batch in the same invocation.
    """
    export_rows = _fetch_with_fallback(timestamp, "export", export_url, session=session)
    if export_rows is None:
        _write_ingest_log(client, timestamp, "missing", 0, 0, message="export file not found for this timestamp")
        return {"status": "missing", "timestamp": timestamp}

    mentions_rows = _fetch_with_fallback(timestamp, "mentions", mentions_url, session=session)
    if mentions_rows is None:
        _write_ingest_log(client, timestamp, "missing", 0, 0, message="mentions file not found for this timestamp")
        return {"status": "missing", "timestamp": timestamp}

    try:
        validate_destination_columns(client, f"{DATABASE}.{EVENTS_TABLE}", EVENTS_COLUMN_SPEC)
        validate_destination_columns(client, f"{DATABASE}.{MENTIONS_TABLE}", MENTIONS_COLUMN_SPEC)

        event_names, event_data = rows_for_insert(export_rows, EVENTS_COLUMN_SPEC)
        client.insert(
            f"{DATABASE}.{EVENTS_TABLE}",
            event_data,
            column_names=event_names,
            settings={"insert_deduplication_token": _dedup_token(EVENTS_TABLE, timestamp)},
        )

        mention_names, mention_data = rows_for_insert(mentions_rows, MENTIONS_COLUMN_SPEC)
        client.insert(
            f"{DATABASE}.{MENTIONS_TABLE}",
            mention_data,
            column_names=mention_names,
            settings={"insert_deduplication_token": _dedup_token(MENTIONS_TABLE, timestamp)},
        )
    except Exception as exc:
        logger.exception("Failed to process timestamp %s", timestamp)
        _write_ingest_log(client, timestamp, "error", 0, 0, message=str(exc)[:500])
        return {"status": "error", "timestamp": timestamp, "error": str(exc)}

    _write_ingest_log(client, timestamp, "success", len(event_data), len(mention_data))
    return {
        "status": "success",
        "timestamp": timestamp,
        "rows_export": len(event_data),
        "rows_mentions": len(mention_data),
    }


# --------------------------------------------------------------------------
# main() orchestration
# --------------------------------------------------------------------------

def _ingest_log_state(client):
    """Return (earliest_ever_or_None, {success_timestamps}). earliest is None
    only for a genuinely empty ingest_log (never run before) -- distinct from
    a log that has rows but no successes yet."""
    row = client.query(f"SELECT count(), min(file_timestamp) FROM {DATABASE}.{INGEST_LOG_TABLE}").result_rows[0]
    total, earliest = row[0], row[1]
    earliest = _as_utc(earliest) if total else None

    success_rows = client.query(
        f"SELECT file_timestamp FROM {DATABASE}.{INGEST_LOG_TABLE} WHERE status = 'success'"
    ).result_rows
    success_timestamps = {_as_utc(r[0]) for r in success_rows}
    return earliest, success_timestamps


def _candidates_to_process(earliest, success_timestamps, latest):
    """Cold start (earliest is None): seed only the current latest timestamp.
    Otherwise: every boundary timestamp from earliest through latest that
    does not yet have a status='success' row -- covering genuine gaps and
    previously failed/missing attempts uniformly, so a failed timestamp is
    never stranded behind a later success."""
    if earliest is None:
        return [latest]
    all_candidates = candidate_timestamps(earliest - GDELT_BOUNDARY, latest)
    return [ts for ts in all_candidates if ts not in success_timestamps]


def main(request):
    """functions-framework HTTP entry point."""
    try:
        client = get_client()
        files, latest = fetch_lastupdate()
        earliest, success_timestamps = _ingest_log_state(client)
        candidates = _candidates_to_process(earliest, success_timestamps, latest)

        results = []
        for ts in candidates:
            export_url = files["export"]["url"] if ts == latest else build_url(files["export"]["url"], ts)
            mentions_url = files["mentions"]["url"] if ts == latest else build_url(files["mentions"]["url"], ts)
            results.append(process_timestamp(client, ts, export_url, mentions_url))

        summary = {
            "latest": latest.strftime("%Y%m%d%H%M%S"),
            "processed": [
                {"timestamp": r["timestamp"].strftime("%Y%m%d%H%M%S"), "status": r["status"]}
                for r in results
            ],
        }
        return json.dumps(summary), 200, {"Content-Type": "application/json"}
    except Exception:
        logger.exception("gdelt-ingest invocation failed")
        return json.dumps({"error": "internal error"}), 500, {"Content-Type": "application/json"}
