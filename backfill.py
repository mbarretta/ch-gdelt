"""Bulk-load a historical date range of GDELT data into ClickHouse.

Discovers files via GDELT's masterfilelist.txt -- the authoritative
enumeration of every file GDELT has ever published -- batches timestamps by
UTC calendar day, and processes multiple days concurrently (sized to the
machine's CPU/available memory, capped at MAX_WORKERS regardless). Each day
is processed with main.py's existing, unchanged per-timestamp
fetch/transform/insert pipeline, so idempotency (insert dedup tokens) and
gdelt.ingest_log bookkeeping behave identically to the live Cloud Function.

Usage:
    python backfill.py                                    # 2026-01-01 through today
    python backfill.py --start-date 2026-03-01 --end-date 2026-03-31
    python backfill.py --dry-run                           # show the plan, touch nothing
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import psutil
import requests

import main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdelt_backfill")

DEFAULT_MEM_PER_WORKER_MB = 300
MAX_WORKERS = 8


# --------------------------------------------------------------------------
# masterfilelist.txt discovery
# --------------------------------------------------------------------------

def _iter_master_list_lines(session=None):
    session = session or requests
    resp = session.get(main.MASTERFILELIST_URL, stream=True, timeout=120)
    resp.raise_for_status()
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        yield raw_line.decode("utf-8", errors="replace").strip()


def _parse_master_list_line(line):
    """Return (timestamp, kind, url) for one masterfilelist.txt line ("size
    md5 url"), or None if the line is malformed or not an export/mentions
    file (GKG is out of scope, matching main.py)."""
    parts = line.split(" ", 2)
    if len(parts) != 3:
        return None
    url = parts[2]
    kind = main._kind_from_url(url)
    if kind not in ("export", "mentions"):
        return None
    filename = url.rsplit("/", 1)[-1]
    match = main.TIMESTAMP_RE.match(filename)
    if not match:
        return None
    timestamp = main._as_utc(datetime.strptime(match.group(0), "%Y%m%d%H%M%S"))
    return timestamp, kind, url


def load_master_list_urls(start_date, end_date, session=None):
    """Stream masterfilelist.txt once and collect
    {timestamp: {"export": url, "mentions": url}} for every timestamp whose
    UTC date falls in [start_date, end_date] (inclusive).

    Filters on the filename's date prefix as a cheap string comparison
    before doing any regex/datetime parsing, since the file lists every
    GDELT file published since 2015 and streaming it is the expensive part.
    """
    urls_by_timestamp = defaultdict(dict)
    start_prefix = start_date.strftime("%Y%m%d")
    end_prefix = end_date.strftime("%Y%m%d")

    for line in _iter_master_list_lines(session=session):
        filename_prefix = line.rsplit("/", 1)[-1][:8]
        if not (start_prefix <= filename_prefix <= end_prefix):
            continue
        parsed = _parse_master_list_line(line)
        if parsed is None:
            continue
        timestamp, kind, url = parsed
        urls_by_timestamp[timestamp][kind] = url

    return dict(urls_by_timestamp)


def batch_by_day(urls_by_timestamp):
    """Group {timestamp: {"export": ..., "mentions": ...}} into
    {day: {timestamp: {...}}}."""
    by_day = defaultdict(dict)
    for timestamp, urls in urls_by_timestamp.items():
        by_day[timestamp.date()][timestamp] = urls
    return dict(by_day)


# --------------------------------------------------------------------------
# Resume support: skip timestamps already marked success
# --------------------------------------------------------------------------

def existing_successes(client, start_date, end_date):
    """Every gdelt.ingest_log timestamp already status='success' within
    [start_date, end_date], so a re-run only processes what's left."""
    result = client.query(
        f"SELECT file_timestamp FROM {main.DATABASE}.{main.INGEST_LOG_TABLE} "
        "WHERE status = 'success' AND file_timestamp >= %(start)s AND file_timestamp < %(end)s",
        parameters={"start": start_date, "end": end_date + timedelta(days=1)},
    )
    return {main._as_utc(row[0]) for row in result.result_rows}


# --------------------------------------------------------------------------
# Concurrency sizing
# --------------------------------------------------------------------------

def pick_worker_count(day_count, override=None, mem_per_worker_mb=DEFAULT_MEM_PER_WORKER_MB):
    """CPU- and memory-derived concurrency, always capped at MAX_WORKERS and
    at day_count (no point running more workers than there is work), unless
    the caller passes an explicit override."""
    if override is not None:
        return max(1, override)
    if day_count == 0:
        return 1
    cpu_count = os.cpu_count() or 1
    available_mb = psutil.virtual_memory().available / (1024 * 1024)
    memory_budget = max(1, int(available_mb // mem_per_worker_mb))
    return max(1, min(cpu_count, memory_budget, MAX_WORKERS, day_count))


# --------------------------------------------------------------------------
# Per-day worker (module-level for ProcessPoolExecutor picklability)
# --------------------------------------------------------------------------

def _process_day(day, day_urls):
    """Process one UTC calendar day's worth of timestamps sequentially,
    reusing main.py's process_timestamp unchanged. Runs in its own process
    with its own ClickHouse client and HTTP session."""
    client = main.get_client()
    session = requests.Session()
    counts = {"success": 0, "missing": 0, "error": 0}

    for timestamp in sorted(day_urls):
        urls = day_urls[timestamp]
        export_url = urls.get("export")
        mentions_url = urls.get("mentions")

        if export_url is None or mentions_url is None:
            missing_kind = "export" if export_url is None else "mentions"
            main._write_ingest_log(
                client, timestamp, "missing", 0, 0,
                message=f"{missing_kind} file not listed in masterfilelist.txt for this timestamp",
            )
            counts["missing"] += 1
            continue

        result = main.process_timestamp(client, timestamp, export_url, mentions_url, session=session)
        counts[result["status"]] += 1

    return {"date": day.isoformat(), **counts}


# --------------------------------------------------------------------------
# CLI orchestration
# --------------------------------------------------------------------------

def _parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", type=_parse_date, default=date(2026, 1, 1))
    parser.add_argument("--end-date", type=_parse_date, default=None, help="default: today (UTC)")
    parser.add_argument("--workers", type=int, default=None, help="override the auto-detected concurrency")
    parser.add_argument("--mem-per-worker-mb", type=int, default=DEFAULT_MEM_PER_WORKER_MB)
    parser.add_argument("--dry-run", action="store_true", help="print the plan; touch nothing")
    return parser.parse_args(argv)


def run(args):
    end_date = args.end_date or datetime.now(timezone.utc).date()
    if end_date < args.start_date:
        raise SystemExit(f"--end-date {end_date} is before --start-date {args.start_date}")

    logger.info("Loading masterfilelist.txt for %s through %s ...", args.start_date, end_date)
    urls_by_timestamp = load_master_list_urls(args.start_date, end_date)
    by_day = batch_by_day(urls_by_timestamp)
    logger.info("masterfilelist.txt has %d day(s) with data in range", len(by_day))

    client = main.get_client()
    already_done = existing_successes(client, args.start_date, end_date)
    client.close()

    pending_by_day = {}
    for day, day_urls in by_day.items():
        remaining = {ts: urls for ts, urls in day_urls.items() if ts not in already_done}
        if remaining:
            pending_by_day[day] = remaining

    skipped = len(by_day) - len(pending_by_day)
    if skipped:
        logger.info("%d day(s) already fully loaded; skipping", skipped)

    worker_count = pick_worker_count(
        len(pending_by_day), override=args.workers, mem_per_worker_mb=args.mem_per_worker_mb
    )
    logger.info("Processing %d day(s) with %d concurrent worker(s)", len(pending_by_day), worker_count)

    if args.dry_run:
        for day in sorted(pending_by_day):
            logger.info("[dry-run] %s: %d timestamp(s) pending", day, len(pending_by_day[day]))
        return

    totals = {"success": 0, "missing": 0, "error": 0}
    failed_days = []
    with ProcessPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(_process_day, day, day_urls): day
            for day, day_urls in pending_by_day.items()
        }
        for future in as_completed(futures):
            day = futures[future]
            try:
                summary = future.result()
            except Exception:
                logger.exception("Day %s failed", day)
                failed_days.append(day.isoformat())
                continue

            for key in totals:
                totals[key] += summary[key]
            logger.info(
                "%s done: %d success, %d missing, %d error",
                summary["date"], summary["success"], summary["missing"], summary["error"],
            )

    logger.info(
        "Totals: %d success, %d missing, %d error", totals["success"], totals["missing"], totals["error"]
    )
    if failed_days:
        logger.warning("These days raised an exception and should be re-run: %s", ", ".join(sorted(failed_days)))


def main_cli(argv=None):
    run(parse_args(argv))


if __name__ == "__main__":
    main_cli()
