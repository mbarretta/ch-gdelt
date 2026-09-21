"""Unit tests for main.py: parsing, transform, gap-detection, and end-to-end
orchestration -- all against mocked HTTP responses and a stub ClickHouse
client. No real network or ClickHouse calls are made anywhere in this file.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone

import pytest
import requests

import main

BOUNDARY = main.GDELT_BOUNDARY


def _ts(s):
    return datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Shared test doubles
# ---------------------------------------------------------------------------

class Result:
    """Mimics the subset of clickhouse-connect's QueryResult main.py touches."""

    def __init__(self, result_rows):
        self.result_rows = result_rows


class FakeResponse:
    """A minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, text="", content=b"", lines=None):
        self.status_code = status_code
        self.text = text
        self.content = content
        self._lines = lines or []

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def iter_lines(self):
        return iter(self._lines)


class StreamOnlyResponse(FakeResponse):
    """A response whose .text/.content must never be touched -- only
    iter_lines() -- so a test can catch a masterfilelist fetch that
    accidentally buffers the whole file into memory instead of streaming it."""

    @property
    def text(self):  # noqa: D401 - property override
        raise AssertionError("masterfilelist response must be streamed via iter_lines(), not .text")

    @text.setter
    def text(self, value):
        pass

    @property
    def content(self):
        raise AssertionError("masterfilelist response must be streamed via iter_lines(), not .content")

    @content.setter
    def content(self, value):
        pass


class FakeSession:
    """Stands in for the `requests` module: only .get(url, timeout=, stream=)
    is used anywhere in main.py."""

    def __init__(self):
        self.responses = {}
        self.calls = []

    def set_response(self, url, response):
        self.responses[url] = response

    def get(self, url, timeout=None, stream=False):
        self.calls.append({"url": url, "timeout": timeout, "stream": stream})
        return self.responses.get(url, FakeResponse(status_code=404))

    def urls_called(self):
        return [c["url"] for c in self.calls]


class StubClickHouseClient:
    """Stands in for clickhouse_connect's client: only .query/.command/.insert
    are used anywhere in main.py."""

    def __init__(self, ingest_log_rows=None):
        # each row: (file_timestamp, status, rows_export, rows_mentions, message)
        self.ingest_log_rows = list(ingest_log_rows or [])
        self.events_inserts = []
        self.mentions_inserts = []
        self.call_log = []

        self.fail_delete_submit_for = set()  # {"events", "mentions"}
        self.mutation_poll_counts = {}  # table -> polls remaining before is_done
        self.fail_insert_once_for = set()  # {"events", "mentions"}

    def _table_kind(self, table):
        if table.endswith(".events"):
            return "events"
        if table.endswith(".mentions"):
            return "mentions"
        if table.endswith(".ingest_log"):
            return "ingest_log"
        raise AssertionError(f"unexpected table: {table}")

    def query(self, sql, parameters=None):
        if sql.strip().startswith("DESCRIBE TABLE"):
            table = sql.split("DESCRIBE TABLE", 1)[1].strip()
            kind = self._table_kind(table)
            spec = main.EVENTS_COLUMN_SPEC if kind == "events" else main.MENTIONS_COLUMN_SPEC
            return Result([[name, "String"] for name in main.expected_column_names(spec)])

        if "system.mutations" in sql:
            table = parameters["table"]
            remaining = self.mutation_poll_counts.get(table, 0)
            pending = 1 if remaining > 0 else 0
            if remaining > 0:
                self.mutation_poll_counts[table] = remaining - 1
            self.call_log.append(f"poll:{table}:{pending}")
            return Result([[pending]])

        if "count(), min(file_timestamp)" in sql:
            total = len(self.ingest_log_rows)
            earliest = min((r[0] for r in self.ingest_log_rows), default=None) if total else None
            return Result([[total, earliest]])

        if "status = 'success'" in sql:
            rows = [[r[0]] for r in self.ingest_log_rows if r[1] == "success"]
            return Result(rows)

        raise AssertionError(f"unexpected query: {sql}")

    def command(self, sql, parameters=None):
        # sql looks like: "ALTER TABLE gdelt.events DELETE WHERE ..."
        table = sql.split("ALTER TABLE", 1)[1].split("DELETE", 1)[0].strip()
        table_kind = self._table_kind(table)
        if table_kind in self.fail_delete_submit_for:
            raise RuntimeError(f"simulated failure submitting delete against {table_kind}")
        self.call_log.append(f"delete_submit:{table_kind}")

    def insert(self, table, data, column_names):
        kind = self._table_kind(table)
        if kind == "ingest_log":
            for row in data:
                self.ingest_log_rows.append(tuple(row))
            self.call_log.append("insert:ingest_log")
            return

        if kind in self.fail_insert_once_for:
            self.fail_insert_once_for.discard(kind)
            raise RuntimeError(f"simulated failure inserting into {kind}")

        target = self.events_inserts if kind == "events" else self.mentions_inserts
        target.append((list(column_names), [list(row) for row in data]))
        self.call_log.append(f"insert:{kind}")


def _row(spec_len, overrides):
    row = [""] * spec_len
    for idx0, value in overrides.items():
        row[idx0] = value
    return row


def _export_row(ts):
    # ordinal 1 = GLOBALEVENTID (idx0), ordinal 60 = DATEADDED (idx59)
    return _row(61, {0: "1", 59: ts.strftime("%Y%m%d%H%M%S")})


def _mentions_row(ts):
    # ordinal 1 = GLOBALEVENTID (idx0), ordinal 3 = MentionTimeDate (idx2)
    return _row(16, {0: "1", 2: ts.strftime("%Y%m%d%H%M%S")})


def _zip_bytes(filename, rows):
    buf = io.BytesIO()
    body = "\n".join("\t".join(row) for row in rows)
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, body)
    return buf.getvalue()


def _export_zip(ts):
    ts_str = ts.strftime("%Y%m%d%H%M%S")
    return _zip_bytes(f"{ts_str}.export.CSV", [_export_row(ts)])


def _mentions_zip(ts):
    ts_str = ts.strftime("%Y%m%d%H%M%S")
    return _zip_bytes(f"{ts_str}.mentions.CSV", [_mentions_row(ts)])


def _export_url(ts):
    return f"https://data.gdeltproject.org/gdeltv2/{ts.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"


def _mentions_url(ts):
    return f"https://data.gdeltproject.org/gdeltv2/{ts.strftime('%Y%m%d%H%M%S')}.mentions.CSV.zip"


def _gkg_url(ts):
    return f"https://data.gdeltproject.org/gdeltv2/{ts.strftime('%Y%m%d%H%M%S')}.gkg.csv.zip"


def _lastupdate_text(ts):
    ts_str = ts.strftime("%Y%m%d%H%M%S")
    return (
        f"77722 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa {_export_url(ts)}\n"
        f"115547 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb {_mentions_url(ts)}\n"
        f"5041361 cccccccccccccccccccccccccccccccc {_gkg_url(ts)}\n"
    )


def _register_timestamp(session, ts):
    """Serve valid export+mentions zips for this timestamp's canonical URLs."""
    session.set_response(_export_url(ts), FakeResponse(content=_export_zip(ts)))
    session.set_response(_mentions_url(ts), FakeResponse(content=_mentions_zip(ts)))


def _set_latest(session, ts):
    session.set_response(main.LASTUPDATE_URL, FakeResponse(text=_lastupdate_text(ts)))


# ---------------------------------------------------------------------------
# ac2: date/timestamp parsing
# ---------------------------------------------------------------------------

def test_to_date_parses_yyyymmdd():
    assert main._to_date("20260918") == datetime(2026, 9, 18).date()


def test_to_date_empty_is_none():
    assert main._to_date("") is None


def test_to_datetime_parses_yyyymmddhhmmss():
    result = main._to_datetime("20260918143000")
    assert result == datetime(2026, 9, 18, 14, 30, 0, tzinfo=timezone.utc)
    assert result.tzinfo is not None


def test_to_datetime_empty_is_none():
    assert main._to_datetime("") is None


def test_transform_row_converts_named_export_date_fields():
    raw = _export_row(_ts("20260918143000"))
    result = main.transform_row(raw, main.EVENTS_COLUMN_SPEC)
    assert result["Day"] is None  # blank in our fixture row
    assert result["DATEADDED"] == datetime(2026, 9, 18, 14, 30, 0, tzinfo=timezone.utc)
    assert result["DATEADDED"].tzinfo is not None


def test_transform_row_converts_named_mentions_date_fields():
    raw = _mentions_row(_ts("20260918143000"))
    result = main.transform_row(raw, main.MENTIONS_COLUMN_SPEC)
    assert result["MentionTimeDate"] == datetime(2026, 9, 18, 14, 30, 0, tzinfo=timezone.utc)
    assert result["MentionTimeDate"].tzinfo is not None
    assert result["EventTimeDate"] is None  # blank in our fixture row


# ---------------------------------------------------------------------------
# ac1/ac3: get_client() pins a UTC session timezone as defense in depth
# ---------------------------------------------------------------------------

def test_get_client_pins_utc_session_timezone(monkeypatch):
    captured = {}

    def fake_get_client(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(main.clickhouse_connect, "get_client", fake_get_client)
    monkeypatch.setenv("CLICKHOUSE_URL", "https://example.com:8443")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")

    main.get_client()

    assert captured["settings"] == {"session_timezone": "UTC"}


# ---------------------------------------------------------------------------
# ac2: candidate_timestamps boundary generation
# ---------------------------------------------------------------------------

def test_candidate_timestamps_zero_gap_returns_nothing():
    t0 = _ts("20260918143000")
    assert main.candidate_timestamps(t0, t0) == []


def test_candidate_timestamps_single_gap():
    t0 = _ts("20260918143000")
    t1 = t0 + BOUNDARY
    assert main.candidate_timestamps(t0, t1) == [t1]


def test_candidate_timestamps_multi_gap_in_order():
    t0 = _ts("20260918143000")
    latest = t0 + 3 * BOUNDARY
    expected = [t0 + BOUNDARY, t0 + 2 * BOUNDARY, t0 + 3 * BOUNDARY]
    assert main.candidate_timestamps(t0, latest) == expected


# ---------------------------------------------------------------------------
# ac1: every GDELT-derived timestamp is UTC-aware end to end, never naive --
# a naive value gets silently shifted by clickhouse_connect's local-timezone
# interpretation on a non-UTC host (see _as_utc's docstring).
# ---------------------------------------------------------------------------

def test_to_datetime_is_utc_aware():
    assert main._to_datetime("20260918143000").tzinfo == timezone.utc


def test_fetch_lastupdate_latest_is_utc_aware():
    ts = _ts("20260918144500")
    session = FakeSession()
    _set_latest(session, ts)

    _files, latest = main.fetch_lastupdate(session=session)

    assert latest.tzinfo == timezone.utc


def test_candidate_timestamps_output_is_utc_aware():
    t0 = _ts("20260918143000")
    latest = t0 + BOUNDARY
    for ts in main.candidate_timestamps(t0, latest):
        assert ts.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# ac2: _ingest_log_state normalizes ClickHouse reads to UTC-aware, even when
# the client (its default tz_mode) hands back naive datetimes -- so
# candidate_timestamps/_candidates_to_process never compares naive against
# aware.
# ---------------------------------------------------------------------------

def test_ingest_log_state_normalizes_naive_reads_to_utc_aware():
    naive_ts = datetime(2026, 9, 18, 14, 30, 0)  # simulates a naive-utc read
    client = StubClickHouseClient(ingest_log_rows=[(naive_ts, "success", 1, 1, "")])

    earliest, success_timestamps = main._ingest_log_state(client)

    assert earliest.tzinfo == timezone.utc
    assert earliest == naive_ts.replace(tzinfo=timezone.utc)
    assert all(ts.tzinfo == timezone.utc for ts in success_timestamps)

    latest = _ts("20260918150000")  # aware; must compare cleanly against `earliest`
    candidates = main._candidates_to_process(earliest, success_timestamps, latest)
    assert all(ts.tzinfo == timezone.utc for ts in candidates)


# ---------------------------------------------------------------------------
# ac2: build_url substitution
# ---------------------------------------------------------------------------

def test_build_url_substitutes_leading_timestamp():
    template = _export_url(_ts("20260918143000"))
    new_ts = _ts("20260918144500")
    assert main.build_url(template, new_ts) == _export_url(new_ts)


def test_build_url_only_substitutes_the_leading_timestamp():
    # A URL whose filename happens to also contain 14 digits elsewhere should
    # only have its leading (first) occurrence substituted.
    template = "https://data.gdeltproject.org/gdeltv2/20260918143000.export.CSV.zip"
    new_ts = _ts("20260101000000")
    result = main.build_url(template, new_ts)
    assert result == "https://data.gdeltproject.org/gdeltv2/20260101000000.export.CSV.zip"


# ---------------------------------------------------------------------------
# ac2: 404-triggers-masterfilelist-fallback branch
# ---------------------------------------------------------------------------

def test_fetch_and_parse_returns_none_on_404():
    session = FakeSession()
    session.set_response("https://example.com/missing.zip", FakeResponse(status_code=404))
    assert main.fetch_and_parse("https://example.com/missing.zip", session=session) is None


def test_404_falls_back_to_masterfilelist_streaming():
    ts = _ts("20260918144500")
    computed_url = _export_url(ts)
    # A different path than the computed URL, but ending in the exact
    # filename masterfilelist.txt must resolve for this timestamp+kind, to
    # prove the fallback's resolved URL (not the original 404 URL) is used.
    resolved_url = f"https://data.gdeltproject.org/gdeltv2/archive/{ts.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"

    session = FakeSession()
    session.set_response(computed_url, FakeResponse(status_code=404))
    master_line = f"77722 deadbeef {resolved_url}".encode("utf-8")
    session.set_response(
        main.MASTERFILELIST_URL,
        StreamOnlyResponse(status_code=200, lines=[master_line]),
    )
    session.set_response(resolved_url, FakeResponse(content=_export_zip(ts)))

    rows = main._fetch_with_fallback(ts, "export", computed_url, session=session)

    assert rows is not None
    assert len(rows) == 1
    master_call = [c for c in session.calls if c["url"] == main.MASTERFILELIST_URL][0]
    assert master_call["stream"] is True


def test_masterfilelist_only_fetched_on_404_never_routinely():
    ts = _ts("20260918144500")
    session = FakeSession()
    _register_timestamp(session, ts)

    rows = main._fetch_with_fallback(ts, "export", _export_url(ts), session=session)

    assert rows is not None
    assert main.MASTERFILELIST_URL not in session.urls_called()


def test_masterfilelist_miss_returns_none():
    ts = _ts("20260918144500")
    session = FakeSession()
    session.set_response(_export_url(ts), FakeResponse(status_code=404))
    session.set_response(main.MASTERFILELIST_URL, StreamOnlyResponse(status_code=200, lines=[]))

    rows = main._fetch_with_fallback(ts, "export", _export_url(ts), session=session)

    assert rows is None


# ---------------------------------------------------------------------------
# ac4: idempotency delete waits for confirmed completion
# ---------------------------------------------------------------------------

def test_delete_existing_rows_waits_for_multiple_polls_then_succeeds():
    client = StubClickHouseClient()
    client.mutation_poll_counts["events"] = 2  # pending, pending, then done

    ok = main.delete_existing_rows(
        client, "gdelt", "events", "DATEADDED", _ts("20260918143000"),
        timeout_s=5, poll_interval_s=0,
    )

    assert ok is True
    assert client.call_log == ["delete_submit:events", "poll:events:1", "poll:events:1", "poll:events:0"]


def test_delete_existing_rows_times_out_returns_false():
    client = StubClickHouseClient()
    client.mutation_poll_counts["events"] = 10_000  # never reaches is_done in time

    ok = main.delete_existing_rows(
        client, "gdelt", "events", "DATEADDED", _ts("20260918143000"),
        timeout_s=0.05, poll_interval_s=0.01,
    )

    assert ok is False


def test_delete_existing_rows_returns_false_when_submit_fails():
    client = StubClickHouseClient()
    client.fail_delete_submit_for.add("events")

    ok = main.delete_existing_rows(
        client, "gdelt", "events", "DATEADDED", _ts("20260918143000"),
        timeout_s=5, poll_interval_s=0,
    )

    assert ok is False
    assert client.call_log == []  # never got past the failed submit


# ---------------------------------------------------------------------------
# ac4: process_timestamp delete-then-insert ordering and failure handling
# ---------------------------------------------------------------------------

def test_process_timestamp_waits_for_both_deletes_before_either_insert():
    ts = _ts("20260918143000")
    session = FakeSession()
    _register_timestamp(session, ts)
    client = StubClickHouseClient()
    client.mutation_poll_counts["events"] = 1
    client.mutation_poll_counts["mentions"] = 1

    result = main.process_timestamp(client, ts, _export_url(ts), _mentions_url(ts), session=session)

    assert result["status"] == "success"
    last_mentions_done = max(i for i, c in enumerate(client.call_log) if c == "poll:mentions:0")
    first_insert = min(i for i, c in enumerate(client.call_log) if c.startswith("insert:"))
    assert last_mentions_done < first_insert


def test_process_timestamp_aborts_without_insert_when_delete_fails():
    ts = _ts("20260918143000")
    session = FakeSession()
    _register_timestamp(session, ts)
    client = StubClickHouseClient()
    client.fail_delete_submit_for.add("events")

    result = main.process_timestamp(client, ts, _export_url(ts), _mentions_url(ts), session=session)

    assert result["status"] == "error"
    assert client.events_inserts == []
    assert client.mentions_inserts == []
    assert [r for r in client.ingest_log_rows if r[1] == "success"] == []
    assert client.ingest_log_rows[0][1] == "error"


def test_process_timestamp_aborts_without_insert_when_delete_times_out(monkeypatch):
    # Same abort contract as a failed submit, but reached via the timeout
    # path inside delete_existing_rows -- exercised through process_timestamp
    # itself, not just the lower-level helper. process_timestamp calls
    # delete_existing_rows with its real (120s/1.0s) defaults, so wrap it
    # with tiny timeout/poll-interval values instead of waiting them out.
    real_delete_existing_rows = main.delete_existing_rows

    def fast_delete_existing_rows(client, database, table, column, timestamp, **_ignored):
        return real_delete_existing_rows(
            client, database, table, column, timestamp, timeout_s=0.02, poll_interval_s=0.005,
        )

    monkeypatch.setattr(main, "delete_existing_rows", fast_delete_existing_rows)

    ts = _ts("20260918143000")
    session = FakeSession()
    _register_timestamp(session, ts)
    client = StubClickHouseClient()
    client.mutation_poll_counts["events"] = 10_000  # never reports is_done in time

    result = main.process_timestamp(client, ts, _export_url(ts), _mentions_url(ts), session=session)

    assert result["status"] == "error"
    assert client.events_inserts == []
    assert client.mentions_inserts == []
    assert [r for r in client.ingest_log_rows if r[1] == "success"] == []


def test_process_timestamp_failure_between_inserts_leaves_no_success_row():
    ts = _ts("20260918143000")
    session = FakeSession()
    _register_timestamp(session, ts)
    client = StubClickHouseClient()
    client.fail_insert_once_for.add("mentions")

    result = main.process_timestamp(client, ts, _export_url(ts), _mentions_url(ts), session=session)

    assert result["status"] == "error"
    assert len(client.events_inserts) == 1  # export insert did happen
    assert client.mentions_inserts == []
    statuses = [r[1] for r in client.ingest_log_rows]
    assert "success" not in statuses
    assert statuses == ["error"]  # eligible for a clean retry


# ---------------------------------------------------------------------------
# ac3: main(request) end-to-end orchestration
# ---------------------------------------------------------------------------

@pytest.fixture
def wired_main(monkeypatch):
    """Monkeypatch main's module-level get_client()/requests so main(request)
    runs entirely against test doubles -- no real network or ClickHouse."""

    def _wire(client):
        session = FakeSession()
        monkeypatch.setattr(main, "get_client", lambda: client)
        monkeypatch.setattr(main, "requests", session)
        return session

    return _wire


def test_main_cold_start_processes_only_latest(wired_main):
    latest = _ts("20260918143000")
    client = StubClickHouseClient(ingest_log_rows=[])
    session = wired_main(client)
    _set_latest(session, latest)
    _register_timestamp(session, latest)

    body, status, _headers = main.main(None)

    assert status == 200
    assert len(client.events_inserts) == 1
    assert len(client.mentions_inserts) == 1
    assert [r[0] for r in client.ingest_log_rows] == [latest]
    assert client.ingest_log_rows[0][1] == "success"
    assert main.MASTERFILELIST_URL not in session.urls_called()


def test_main_multi_gap_catch_up_processes_in_order(wired_main):
    t0 = _ts("20260918140000")
    latest = t0 + 3 * BOUNDARY
    client = StubClickHouseClient(ingest_log_rows=[(t0, "success", 1, 1, "")])
    session = wired_main(client)
    _set_latest(session, latest)
    for ts in (t0 + BOUNDARY, t0 + 2 * BOUNDARY, latest):
        _register_timestamp(session, ts)

    body, status, _headers = main.main(None)

    assert status == 200
    processed_ts = [r[0] for r in client.ingest_log_rows if r[1] == "success"]
    assert processed_ts == [t0, t0 + BOUNDARY, t0 + 2 * BOUNDARY, latest]
    assert len(client.events_inserts) == 3
    assert len(client.mentions_inserts) == 3


def test_main_noop_when_nothing_unresolved_and_nothing_new(wired_main):
    t0 = _ts("20260918143000")
    client = StubClickHouseClient(ingest_log_rows=[(t0, "success", 1, 1, "")])
    session = wired_main(client)
    _set_latest(session, t0)  # nothing new published since last success

    body, status, _headers = main.main(None)

    assert status == 200
    assert client.events_inserts == []
    assert client.mentions_inserts == []
    assert len(client.ingest_log_rows) == 1  # unchanged
    assert _export_url(t0) not in session.urls_called()


def test_main_recovers_stranded_timestamp_alongside_new_latest(wired_main):
    ts_a = _ts("20260918143000")
    ts_b = ts_a + BOUNDARY

    client = StubClickHouseClient(ingest_log_rows=[])
    session = wired_main(client)

    # Run 1: latest == A, but the mentions insert fails after export succeeds
    # -> A is logged 'error', never 'success'.
    client.fail_insert_once_for.add("mentions")
    _set_latest(session, ts_a)
    _register_timestamp(session, ts_a)

    body1, status1, _ = main.main(None)
    assert status1 == 200
    assert [r[1] for r in client.ingest_log_rows] == ["error"]

    # lastupdate.txt advances to B; next invocation must repair A too.
    _set_latest(session, ts_b)
    _register_timestamp(session, ts_b)

    body2, status2, _ = main.main(None)
    assert status2 == 200

    statuses_in_order = [(r[0], r[1]) for r in client.ingest_log_rows]
    assert statuses_in_order == [
        (ts_a, "error"),
        (ts_a, "success"),
        (ts_b, "success"),
    ]
    # A is never left stranded behind B's success: A's success is logged
    # no later than B's.
    a_success_idx = statuses_in_order.index((ts_a, "success"))
    b_success_idx = statuses_in_order.index((ts_b, "success"))
    assert a_success_idx < b_success_idx
