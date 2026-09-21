# GDELT → ClickHouse Cloud Run Ingest Function

## Context

GDELT publishes fresh `export` (events), `mentions`, and `gkg` CSV zips every 15 minutes, always pointed to by `https://data.gdeltproject.org/gdeltv2/lastupdate.txt`. We want a standing pipeline that keeps a ClickHouse Cloud instance continuously up to date with the `export` and `mentions` streams (GKG is out of scope — confirmed skip), with no manual intervention.

The two wrinkles that make this more than "curl + insert":
1. **Date format**: GDELT's `Day`/`DATEADDED`/`EventTimeDate`/`MentionTimeDate` fields are `YYYYMMDD`/`YYYYMMDDHHMMSS` strings, not something ClickHouse's `DateTime`/`Date` parses natively from a TSV.
2. **Cadence mismatch + gap safety**: polling every 10 minutes against a 15-minute publish cycle means occasionally two files publish inside one poll window. A stateless "just grab lastupdate.txt" approach will silently skip files over time. We're closing that gap by tracking the last-processed timestamp in ClickHouse itself, and detecting a gap by comparing the current `lastupdate.txt` timestamp against the last one we processed.

Verified against the sample files already sitting in this directory (not from memory): `export.CSV` has 61 tab-delimited columns, `mentions.CSV` has 16. Column layout matches the standard GDELT 2.0 codebook — `Day` is col 2, `DATEADDED` is col 60 in export; `EventTimeDate`/`MentionTimeDate` are cols 2/3 in mentions.

**Existing infrastructure — corrected from initial draft**: the `gdelt` database on the target ClickHouse Cloud instance already has `events` and `mentions` tables — we do **not** create or own those, only insert into them. The only table this project creates is a small ingest-state/log table, and since it already lives in the `gdelt` database, it does not need a `gdelt_` prefix (`ingest_log`, not `gdelt_ingest_log`). Everything else about this project (the function code, GCP resources) is from-scratch — the directory is otherwise empty. Default GCP project is `ch-solution-architects`.

**Gap detection without the master file list**: GDELT publishes on fixed, documented 15-minute UTC boundaries (`:00`/`:15`/`:30`/`:45`) with a deterministic filename pattern (`<YYYYMMDDHHMMSS>.export.CSV.zip` etc.) — so in the overwhelming majority of cases we can compute the exact missed timestamp(s)/filenames directly, with no need to fetch GDELT's `masterfilelist.txt` (which lists every file since 2015 and is large). We only fall back to streaming and grepping `masterfilelist.txt` on the rare occasion a computed URL turns out to be wrong (404), meaning GDELT deviated from the boundary convention for that gap.

## Architecture

```
Cloud Scheduler (every 10 min, OIDC-authenticated)
        │  POST
        ▼
Cloud Run function "gdelt-ingest" (Python 3.12, gen2, region us-east1)
        │
        ├─ 1. GET lastupdate.txt → parse latest timestamp + URLs
        ├─ 2. Query ClickHouse: SELECT max(file_timestamp) FROM ingest_log WHERE status='success'
        ├─ 3. Build list of candidate 15-min-boundary timestamps between (last_processed, latest]
        ├─ 4. For each candidate timestamp:
        │       - build the candidate URL by substituting the timestamp into the lastupdate.txt URL template
        │       - download export.CSV.zip + mentions.CSV.zip in-memory
        │       - on 404 (boundary guess was wrong): stream masterfilelist.txt, grep for that
        │         timestamp's real filenames, retry; if still not found, log status='missing' and move on
        │       - unzip, split TSV rows, convert date fields to real Python datetime/date objects,
        │         convert numeric fields
        │       - bulk insert into gdelt.events / gdelt.mentions via clickhouse-connect
        │       - write a row to gdelt.ingest_log (status success/missing/error)
        └─ 5. Return 200 with a small JSON summary (timestamps processed, row counts)
```

ClickHouse password comes from **Secret Manager**, injected as an env var via `--set-secrets` on deploy — never touches a local `.env` in production. A local `.env` (gitignored) is only used for `functions-framework`-based local testing.

## GCP resources

1. **Secret**: `gdelt-clickhouse-password`
   ```
   printf '%s' 'REPLACE_ME' | gcloud secrets create gdelt-clickhouse-password --data-file=- --project=ch-solution-architects
   ```
2. **Service account** for the function's runtime identity (`gdelt-ingest-sa`) with `roles/secretmanager.secretAccessor`.
3. **Cloud Run function** (gen2, no Dockerfile needed — buildpacks handle Python):
   ```
   gcloud functions deploy gdelt-ingest \
     --gen2 --runtime=python312 --region=us-east1 \
     --source=. --entry-point=main --trigger-http \
     --no-allow-unauthenticated \
     --service-account=gdelt-ingest-sa@ch-solution-architects.iam.gserviceaccount.com \
     --set-secrets=CLICKHOUSE_PASSWORD=gdelt-clickhouse-password:latest \
     --set-env-vars=CLICKHOUSE_URL=https://nh7hftas75.us-east1.gcp.clickhouse.cloud:8443,CLICKHOUSE_USER=default \
     --timeout=300s --memory=512Mi
   ```
4. **Cloud Scheduler job**, invoking with OIDC as a dedicated invoker SA granted `roles/run.invoker` on the function:
   ```
   gcloud scheduler jobs create http gdelt-ingest-trigger \
     --schedule="*/10 * * * *" --uri=<function-url> --http-method=POST \
     --oidc-service-account-email=gdelt-scheduler-sa@ch-solution-architects.iam.gserviceaccount.com
   ```

## ClickHouse schema

`gdelt.events` and `gdelt.mentions` **already exist** — this project only inserts into them, never creates or alters them. Before writing `COLUMN_SPECS` (below), run `DESCRIBE TABLE gdelt.events` and `DESCRIBE TABLE gdelt.mentions` against the real instance to confirm actual column names/order/types — don't assume they exactly mirror the stock GDELT codebook casing. This is the first implementation step, not an assumption baked into this plan.

The **only** table this project creates (`schema.sql`, run once by hand) is the ingest-state/audit table — no `gdelt_` prefix needed since it already lives in the `gdelt` database:
- `gdelt.ingest_log` — `file_timestamp DateTime, status Enum8('success'=1,'missing'=2,'error'=3), rows_export UInt32, rows_mentions UInt32, processed_at DateTime DEFAULT now(), message String DEFAULT ''`, `ENGINE = MergeTree ORDER BY file_timestamp`. This is what makes catch-up possible — `max(file_timestamp) WHERE status='success'` is the resume point.

No dedup engine needed on `events`/`mentions`: because we only ever process a given `file_timestamp` once (gated by `ingest_log`), a plain `insert()` is sufficient — no `ReplacingMergeTree` complexity required, and we're not touching those tables' engine/schema regardless.

## Function code layout

- `main.py` — `functions-framework` HTTP entry point `main(request)`. Contains:
  - `COLUMN_SPECS` — two lists of `(name, ch_type, python_convert_fn)` tuples for events/mentions, filled in from the real `DESCRIBE TABLE` output (see schema section above) and used to transform each parsed row before insert. Date/numeric conversions live here (`datetime.strptime(v, "%Y%m%d%H%M%S")`, `datetime.strptime(v, "%Y%m%d").date()`, `float(v) if v else None`, etc.) — passing native Python `datetime`/`date`/`float` objects to `clickhouse-connect` sidesteps the string-parsing problem entirely, rather than fighting ClickHouse-side date functions.
  - `fetch_lastupdate()` — GET `lastupdate.txt`, parse the 3 lines, return `{kind: (size, md5, url, timestamp)}`.
  - `candidate_timestamps(last_success, latest)` — generate the 15-minute-boundary timestamps strictly after `last_success` up to and including `latest`.
  - `build_url(template_url, timestamp)` — regex-substitute the leading 14-digit timestamp in a known-good URL to construct the URL for another timestamp — the "programmatic" path that avoids `masterfilelist.txt` in the common case.
  - `lookup_in_masterfilelist(timestamp)` — **fallback only**, called when a `build_url` guess 404s. Streams `masterfilelist.txt` (`requests` with `stream=True`, line-by-line) grepping for that timestamp's actual filenames rather than loading the whole (large, multi-year) file into memory; returns the real URLs or `None` if genuinely absent.
  - `fetch_and_parse(url)` — download zip in-memory (`requests` + `io.BytesIO` + `zipfile`), split TSV lines, return raw rows (or `None` on 404).
  - `process_timestamp(client, ts, export_url, mentions_url)` — orchestrates one timestamp: fetch both files (falling back to `lookup_in_masterfilelist` on 404), transform rows, `client.insert(...)` into `gdelt.events`/`gdelt.mentions`, write the `gdelt.ingest_log` row, return counts.
  - `main(request)` — ties it together, catches all exceptions and returns HTTP 500 (so Cloud Scheduler's retry policy kicks in) vs. 200 on success.
- `requirements.txt` — `functions-framework`, `clickhouse-connect`, `requests`, `python-dotenv` (local-only).
- `schema.sql` — the single `CREATE TABLE gdelt.ingest_log` statement, run manually once against ClickHouse Cloud.
- `.env.example` / `.env` (gitignored) — `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` for local `functions-framework` runs only.
- `.gitignore` — `.env`, `*.zip`, `*.CSV`, `__pycache__/`, `venv/`.

## Verification

0. **Schema introspection**: `DESCRIBE TABLE gdelt.events` / `gdelt.mentions` against the real instance; confirm `COLUMN_SPECS` names/order/types match before writing any insert logic.
1. **Local dry run**: `functions-framework --target=main --debug`, `curl -X POST localhost:8080`, confirm it fetches the latest files, inserts into ClickHouse (point `.env` at ClickHouse Cloud), and check `SELECT count() FROM gdelt.events` / `gdelt.mentions` increases by the expected row counts (cross-check against the row counts in the downloaded/extracted sample zips already in this directory).
2. **Catch-up test (programmatic path)**: manually `INSERT` a `gdelt.ingest_log` row with `file_timestamp` set ~45 minutes in the past and `status='success'`, re-invoke, confirm it processes *multiple* missed timestamps in one run via `build_url` (not just the latest) and logs each one — without ever fetching `masterfilelist.txt`.
3. **Masterfilelist fallback test**: force a `build_url` guess to be wrong (e.g. temporarily corrupt one candidate timestamp) and confirm `lookup_in_masterfilelist` is invoked, correctly resolves the real filename, and processing succeeds — then confirm a genuinely-nonexistent timestamp still cleanly logs `status='missing'` rather than crashing.
4. **Deploy + end-to-end**: deploy per the commands above, `gcloud scheduler jobs run gdelt-ingest-trigger`, tail Cloud Run logs (`gcloud run services logs read gdelt-ingest --region=us-east1`), confirm success and new rows in ClickHouse.
5. **Idempotency check**: manually re-invoke the function twice in a row with no new GDELT data published in between; confirm the second run is a no-op (no duplicate rows, `gdelt.ingest_log` unchanged).
