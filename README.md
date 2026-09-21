# GDELT-to-ClickHouse Cloud Run ingest function

Polls GDELT's `lastupdate.txt` every 10 minutes, downloads the `export` and `mentions` CSV zips for any 15-minute boundary timestamp not yet marked `success` in `gdelt.ingest_log`, and inserts the transformed rows into the existing `gdelt.events` / `gdelt.mentions` tables. See `.claude/plans/ok-i-want-a-nifty-waffle.md` for the full design rationale.

This README does not provision anything itself. Every `gcloud` command below is a manual step a human runs once to stand up the live GCP resources; nothing in this repo executes them for you.

## Local dry run

Use this to exercise the function against a real ClickHouse instance without deploying anything.

```bash
cp .env.example .env
# edit .env: set CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

pip install -r requirements.txt
functions-framework --target=main --debug
```

In another terminal:

```bash
curl -X POST http://localhost:8080
```

Read the JSON response and the function's log output to confirm which timestamps were processed.

Idempotency comes from ClickHouse's own insert deduplication, not from anything `main.py` locks or coordinates itself: every insert into `gdelt.events`/`gdelt.mentions` carries an explicit `insert_deduplication_token` keyed on `(table, timestamp)` (see `_dedup_token` in `main.py`). Retrying a timestamp whose events insert already landed — e.g. because the mentions insert failed afterward — resends the identical token, and ClickHouse recognizes and skips the duplicate instead of writing the rows twice. There's no `DELETE`, no mutation, and nothing to poll for completion.

Running this dry run against the same ClickHouse destination while the deployed Cloud Run service is also active is still not recommended as routine practice (redundant work, noisier `gdelt.ingest_log`), but it's no longer a correctness hazard the way a delete-then-insert scheme would be: two writers racing to process the same timestamp just mean one insert lands and the identically-tokened other gets deduped.

## Deployment runbook

The commands below use placeholder values — replace `PROJECT_ID` with your GCP project ID and adjust the secret/service-account/function names if you want different ones. All commands assume `REGION=us-east1`.

### 1. Create the ClickHouse password secret

```bash
PROJECT_ID=your-gcp-project
REGION=us-east1

printf '%s' 'REPLACE_WITH_REAL_CLICKHOUSE_PASSWORD' | \
  gcloud secrets create gdelt-clickhouse-password \
    --project="$PROJECT_ID" \
    --replication-policy=automatic \
    --data-file=-
```

### 2. Create the runtime and scheduler-invoker service accounts

```bash
gcloud iam service-accounts create gdelt-ingest-sa \
  --project="$PROJECT_ID" \
  --display-name="gdelt-ingest Cloud Run runtime identity"

gcloud iam service-accounts create gdelt-scheduler-sa \
  --project="$PROJECT_ID" \
  --display-name="gdelt-ingest Cloud Scheduler invoker identity"

# Runtime SA needs to read the secret at startup.
gcloud secrets add-iam-policy-binding gdelt-clickhouse-password \
  --project="$PROJECT_ID" \
  --member="serviceAccount:gdelt-ingest-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

The `roles/run.invoker` grant on the scheduler SA happens in step 3, once the function exists and its resource name is known.

### 3. Deploy the gen2 Cloud Run function

```bash
gcloud functions deploy gdelt-ingest \
  --project="$PROJECT_ID" \
  --gen2 \
  --runtime=python312 \
  --region="$REGION" \
  --source=. \
  --entry-point=main \
  --trigger-http \
  --no-allow-unauthenticated \
  --max-instances=1 \
  --concurrency=1 \
  --timeout=3600s \
  --memory=512Mi \
  --service-account="gdelt-ingest-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="CLICKHOUSE_PASSWORD=gdelt-clickhouse-password:latest" \
  --set-env-vars="CLICKHOUSE_URL=https://REPLACE_ME.us-east1.gcp.clickhouse.cloud:8443,CLICKHOUSE_USER=REPLACE_ME"
```

`--max-instances=1 --concurrency=1` avoids redundant work (two invocations racing to process the same backlog) rather than guarding correctness — the insert-deduplication-token scheme above (see the local dry run section) is already safe under concurrent writers.

`--timeout=3600s` (the gen2 maximum) is deliberately set high: `main.py` processes every unresolved timestamp sequentially in one invocation, and each timestamp's own latency is now just its GDELT fetch plus its two inserts (no mutation-wait). A short outage produces one or two candidates and finishes in well under a minute; an outage or scheduler gap long enough to accumulate many unresolved timestamps in one run could still exceed even the 3600s ceiling, in which case the invocation is killed mid-catch-up. That is safe — the idempotent design means a killed invocation just leaves the remaining timestamps unresolved for the next scheduled run to pick up — but it does mean catch-up after a long outage may take several consecutive invocations rather than one.

Grant the scheduler SA permission to invoke the now-existing function:

```bash
gcloud functions add-invoker-policy-binding gdelt-ingest \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --gen2 \
  --member="serviceAccount:gdelt-scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

### 4. Apply bootstrap.sql once by hand, then alters.sql as it grows

Schema management here is two-phase. `bootstrap.sql` creates every object this project owns (`gdelt.ingest_log`, `event_count_by_actor`, its materialized view, the `cameo_dict` dictionary, the `dict_reader` user) with `IF NOT EXISTS` guards, so it is safe to re-run. `alters.sql` is an append-only log of later, non-destructive schema changes to those same objects — add new entries at its bottom over time rather than editing `bootstrap.sql`.

Both files contain a `${DICT_READER_PASSWORD}` placeholder for the `dict_reader` user instead of a literal password, so they must be substituted at apply time rather than passed directly as `--queries-file`. Use `envsubst` (from `gettext`; `brew install gettext` on macOS) to expand the placeholder and pipe the result into `clickhouse-client` on stdin:

```bash
DICT_READER_PASSWORD='REPLACE_WITH_REAL_PASSWORD' \
  envsubst < bootstrap.sql | \
  clickhouse-client \
    --host REPLACE_ME.us-east1.gcp.clickhouse.cloud \
    --secure --port 9440 \
    --user REPLACE_ME --password
```

Whenever a new entry is appended to `alters.sql`, apply it the same way, once, by hand:

```bash
DICT_READER_PASSWORD='REPLACE_WITH_REAL_PASSWORD' \
  envsubst < alters.sql | \
  clickhouse-client \
    --host REPLACE_ME.us-east1.gcp.clickhouse.cloud \
    --secure --port 9440 \
    --user REPLACE_ME --password
```

Never commit a real value for `DICT_READER_PASSWORD` — set it only in the shell environment for these commands.

### 5. Create the Cloud Scheduler job

```bash
FUNCTION_URL=$(gcloud functions describe gdelt-ingest \
  --project="$PROJECT_ID" --region="$REGION" --gen2 \
  --format='value(serviceConfig.uri)')

gcloud scheduler jobs create http gdelt-ingest-trigger \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --schedule="*/10 * * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --oidc-service-account-email="gdelt-scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --oidc-token-audience="$FUNCTION_URL"
```

Cloud Scheduler now calls the function every 10 minutes, authenticated via OIDC as `gdelt-scheduler-sa`, which the function's `--no-allow-unauthenticated` deployment accepts because of the `run.invoker` binding granted in step 3.
