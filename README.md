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

**Do not run this dry run against the same ClickHouse destination while the deployed Cloud Run service could also be active.** The per-timestamp idempotency scheme (delete existing rows for a timestamp, wait for the delete to be confirmed complete, then insert) assumes exactly one writer is ever processing at a time. In production that single-writer guarantee comes entirely from the deployment configuration below (`--max-instances=1 --concurrency=1`), not from any locking in the code itself. A local dry run is a second, unsynchronized writer against the same tables — running it concurrently with the live scheduled function can interleave a local delete/insert with a Cloud Run delete/insert and duplicate or drop rows for the same timestamp.

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

`--max-instances=1 --concurrency=1` is not a cost/scaling knob here — it is the mechanism that guarantees overlapping invocations can never run at the same time. The delete-then-insert idempotency scheme in `main.py` (task 2 `ac9`) is only safe under a single writer; see the warning in the local dry run section above.

`--timeout=3600s` (the gen2 maximum) is deliberately set high: `main.py` processes every unresolved timestamp sequentially in one invocation, and each timestamp can block for up to `DEFAULT_MUTATION_TIMEOUT_S * 2` (240s) waiting for its two idempotency deletes to confirm, before its inserts even start. A short outage produces one or two candidates and finishes in well under a minute; an outage or scheduler gap long enough to accumulate many unresolved timestamps in one run could still exceed even the 3600s ceiling, in which case the invocation is killed mid-catch-up. That is safe — the idempotent design means a killed invocation just leaves the remaining timestamps unresolved for the next scheduled run to pick up — but it does mean catch-up after a long outage may take several consecutive invocations rather than one.

Grant the scheduler SA permission to invoke the now-existing function:

```bash
gcloud functions add-invoker-policy-binding gdelt-ingest \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --gen2 \
  --member="serviceAccount:gdelt-scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

### 4. Apply schema.sql once by hand

`gdelt.ingest_log` is the only table this project creates (`gdelt.events` / `gdelt.mentions` already exist and must never be created, altered, or dropped by this project). Apply it once, by hand, against the target ClickHouse Cloud instance — for example with the `clickhouse-client` CLI or the ClickHouse Cloud SQL console:

```bash
clickhouse-client \
  --host REPLACE_ME.us-east1.gcp.clickhouse.cloud \
  --secure --port 9440 \
  --user REPLACE_ME --password \
  --queries-file schema.sql
```

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
