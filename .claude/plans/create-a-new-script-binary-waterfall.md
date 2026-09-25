# Bulk-load 2026 from the GDELT master file list

## Context

`main.py` is a Cloud Function that polls `lastupdate.txt` and processes new 15-minute GDELT timestamps one at a time, sequentially, in a single invocation. That works for the live trickle of new data but is the wrong tool for loading a large historical range: there's no CLI, no parallelism anywhere in the codebase, and the Cloud Run function is capped at a 3600s timeout per invocation (already a known constraint per the `_candidates_to_process` catch-up logic and the README's timeout-budget note). Loading all of 2026 sequentially, one 15-minute file at a time, would take far too long and risks getting killed mid-run.

The goal is a standalone local script, `backfill.py`, that bulk-loads a historical date range (defaulting to 2026-01-01 through today) by reading GDELT's `masterfilelist.txt` — the authoritative enumeration of every file GDELT has ever published — grouping the matching timestamps into one batch per calendar day, and running multiple days concurrently, sized to the machine's CPU and available memory (capped at 8 concurrent batches regardless, to stay polite to GDELT's public file server and ClickHouse Cloud).

It reuses `main.py`'s existing, already-tested per-timestamp pipeline (`process_timestamp`, `get_client`, dedup-token insert, `gdelt.ingest_log` writes) rather than re-implementing fetch/parse/insert logic — the backfill script is purely about **discovery** (master list → day batches) and **orchestration** (parallel day workers), not about re-deriving how a single file gets ingested.

## Design

### 1. Master list discovery (`_load_master_list_for_range`)

- Stream `masterfilelist.txt` once with `requests.get(..., stream=True)` and `resp.iter_lines()` (same style as `main.py:312-323`'s `lookup_in_masterfilelist`, which already proves this file is safe to stream without loading it whole).
- Each line is `size md5 url` (same format `fetch_lastupdate` already parses at `main.py:268`). Reuse `main.py._kind_from_url(url)` to classify `export` / `mentions` / `gkg`, and skip `gkg` (out of scope, matching `main.py`'s own docstring).
- Extract the leading 14-digit timestamp from the filename with `main.TIMESTAMP_RE`, cheaply check the first 4 characters against the requested year(s) before doing any datetime parsing, and only keep lines inside `[start_date, end_date]`.
- Build `{timestamp: {"export": url, "mentions": url}}`. A timestamp is only usable if both kinds are present; anything with just one kind gets logged as `missing` directly (see step 4) rather than guessing the other URL.

### 2. Day batching

- Group the collected timestamps by UTC calendar day into `{date: sorted [timestamp, ...]}`.
- Query `gdelt.ingest_log` once up front for existing `status='success'` timestamps already in the target range (same shape as `_ingest_log_state`, `main.py:464-476`, but scoped with a `WHERE file_timestamp BETWEEN ...` instead of a full-table scan) and filter those out of every day's list. This makes the script resumable: an interrupted or re-run backfill only reprocesses what didn't already succeed, on top of the insert-dedup-token idempotency `process_timestamp` already guarantees for the export/mentions inserts themselves.
- A day with zero remaining timestamps after filtering is skipped entirely (already fully loaded).

### 3. Concurrency sizing (`_pick_worker_count`)

- CPU: `os.cpu_count()`.
- Memory: `psutil.virtual_memory().available` (new dependency — add `psutil` to `requirements.txt`; not currently present, but this script runs locally, not in the Cloud Function).
- Assume a conservative fixed per-worker memory budget (e.g. 300 MB — one day-batch holds at most a couple of in-flight zip/TSV payloads at a time, never the whole day) and compute `available_memory // per_worker_bytes`.
- `worker_count = max(1, min(cpu_count, memory_budget, 8))` — the hard ceiling of 8 always applies regardless of hardware. `--workers` lets the user override the computed value outright; `--mem-per-worker-mb` lets them adjust the memory assumption.

### 4. Per-day worker (`_process_day`, module-level function for picklability)

Run under `concurrent.futures.ProcessPoolExecutor` (one process per concurrent day — processes, not threads, since each needs its own ClickHouse client/connection and `requests.Session`, and the work is I/O-bound but `clickhouse_connect` clients aren't meant to be shared across threads/processes).

Each worker, given one day's `[(timestamp, export_url, mentions_url), ...]`:
1. `client = main.get_client()`, `session = requests.Session()` — created fresh inside the worker, never passed from the parent.
2. For timestamps where both URLs were found in the master list: call `main.process_timestamp(client, ts, export_url, mentions_url, session=session)` unchanged — it already handles fetch, transform, insert-with-dedup-token, and the `gdelt.ingest_log` write, and never raises for expected outcomes.
3. For timestamps missing one kind (from step 1): write the `missing` log row directly via `main._write_ingest_log(...)`, mirroring what `process_timestamp` itself does for a 404, without fabricating a URL.
4. Return a per-day summary dict: `{"date": ..., "success": n, "missing": n, "error": n}`.

### 5. Parent orchestration (`main()` / CLI entry point)

- `argparse` with `--start-date` (default `2026-01-01`), `--end-date` (default: today, UTC), `--workers` (override), `--mem-per-worker-mb` (default 300), `--dry-run` (print the computed day batches and worker count, touch nothing).
- Submit one `_process_day` call per day to the pool, collect results as they complete (`as_completed`), log progress per day as it finishes (`logging`, same `logger` naming convention as `main.py`), and catch exceptions from individual day futures so one bad day doesn't abort the rest — collect failed days into a final "these days need a re-run" summary instead.
- Print a final aggregate summary (total success/missing/error rows, list of any failed days) to stdout.

### Files touched

- **New**: `/Users/michael.barretta/workspace/github/mbarretta/ch-gdelt/backfill.py` — the whole script described above, `import main` for the reused pipeline pieces (`get_client`, `process_timestamp`, `_write_ingest_log`, `_kind_from_url`, `TIMESTAMP_RE`, `DATABASE`, `EVENTS_TABLE`, `MENTIONS_TABLE`, `MASTERFILELIST_URL`).
- **Edit**: `/Users/michael.barretta/workspace/github/mbarretta/ch-gdelt/requirements.txt` — add `psutil` (pinned to current latest, matching the existing pinning convention).
- No changes to `main.py`, `bootstrap.sql`, or the Cloud Function itself — this is purely a new, separate local tool.

## Verification

1. `--dry-run` first: `python backfill.py --dry-run` and confirm the printed day count (~day count from Jan 1 through today), computed worker count, and that it matches expectations (e.g. no days silently dropped, no more than 8 workers).
2. Run against a small real slice before the full year: `python backfill.py --start-date 2026-01-01 --end-date 2026-01-03` and check `SELECT status, count() FROM gdelt.ingest_log WHERE file_timestamp BETWEEN '2026-01-01' AND '2026-01-04' GROUP BY status` in ClickHouse — expect mostly `success`, verify `events`/`mentions` row counts look sane (`SELECT count() FROM gdelt.events WHERE Day BETWEEN ...`).
3. Re-run the exact same 3-day command a second time and confirm via `ingest_log` that no new rows were added for timestamps that already succeeded (resumability) and that `gdelt.events`/`gdelt.mentions` row counts are unchanged (dedup-token idempotency holding under the new caller too).
4. Kill the process mid-run (Ctrl-C) partway through a larger range, then re-run the same command and confirm it picks up only the unfinished days/timestamps rather than reprocessing everything.
5. Once the small-scale runs check out, run the full default range (`python backfill.py`) and monitor the printed per-day summaries and final aggregate for any days landing in the failed-days list.
