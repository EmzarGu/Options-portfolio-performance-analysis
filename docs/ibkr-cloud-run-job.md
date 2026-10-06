# IBKR Cloud Run Job

Job name:

```text
ibkr-flex-import
```

This job runs in `options-performance-dashboard` / `europe-west6` using a
backend container image that includes `portfolio_backend.ibkr.import_job`.

The prepared entrypoint is:

```bash
python -m portfolio_backend.ibkr.import_job
```

## Runtime Configuration

Environment and secrets:

```text
IBKR_FLEX_TOKEN     Secret Manager: ibkr-flex-token:latest
IBKR_FLEX_QUERY_ID  Secret Manager: ibkr-flex-query-id:latest
IBKR_RAW_BUCKET     options-portfolio-ibkr-raw-595990983720
```

Default scheduled behavior is coverage-aware. When `IBKR_IMPORT_INCEPTION_DATE`
is set and no explicit `IBKR_IMPORT_FROM` / `IBKR_IMPORT_TO` range is supplied,
the job inspects successful `ibkr_import_runs`, imports missing coverage from
inception through yesterday, and re-imports a recent overlap window to catch
late IBKR corrections.

```text
IBKR_IMPORT_INCEPTION_DATE=2022-11-01
IBKR_IMPORT_RECENT_OVERLAP_DAYS=14
IBKR_IMPORT_TO_OFFSET_DAYS=1
```

With those settings, a job running on `2026-05-09` targets coverage from
`2022-11-01` through `2026-05-08`. If Firestore is empty, it splits the full
range into IBKR-safe chunks of 365 days or less. If history is already covered,
it re-imports only the last 14 days ending `2026-05-08`.

The auto planner isolates the newest target calendar day into a one-day chunk.
If IBKR returns `1003: Statement is not available` for only that trailing day,
the job marks the chunk `deferred`, imports all available earlier chunks, and
exits successfully with `status=succeeded_with_deferred`. This avoids failing a
daily run just because IBKR has not published the newest statement yet.

Progress is written as newline-delimited JSON in Cloud Run logs:

```text
auto_plan
chunk_started
chunk_succeeded
chunk_deferred
chunk_failed
auto_summary
```

Each imported chunk has its own import run, raw GCS object, Firestore write
counts, and refresh audit record. A deferred trailing chunk has a refresh audit
record with `status=deferred`. Non-deferred failed chunks are logged with their
date range and error, and the job exits failed after reporting the summary.

The production job timeout is 1,800 seconds. The historical backfill has already
been loaded, so normal scheduled runs should only import missing coverage plus
the recent overlap window. If Firestore is reset and a full multi-year backfill
is needed again, temporarily increase the timeout before running the reset
backfill.

## Preparing the dashboard after import

Since 23 September, the production job also prepares dated Market Data selections
after dashboard preparation. `DECISION_LAB_PROVIDER=marketdata` enables this path;
`MARKETDATA_API_TOKEN` references Secret Manager `marketdata-api-token:1`.
Option preparation shares a 90-credit ceiling and a distributed lease with the
web service, and has a 90-second budget. Its result appears under
`dashboard_warmup.option_data`; a partial or failed option update leaves the
portfolio import valid. See the [release record](decision-lab-marketdata-production-2026-09-23.md).

Production enables `IBKR_IMPORT_WARM_DASHBOARD=1`,
`PIPELINE_SNAPSHOT_STORE=firestore`, and `PRICE_HISTORY_STORE=firestore`.
After all import chunks finish successfully (including an explicitly deferred
trailing statement), the job prepares the default dashboard context and verifies
that its base accounting snapshot can be read from Firestore. Failed imports
do not start preparation. Both the 07:15 and 19:45 Europe/Zurich schedules use
this same job; no separate warmer or additional scheduled service is required.

Preparation uses the same previous US trading day, source partition, import
marker and snapshot schema as interactive requests. It stores only the existing
base snapshot; the browser continues to fetch current prices on refresh and
continues to validate the latest import marker. Historical dates and custom
requests are still built on demand. Scalar historical-price parsing has been
replaced with batch conversion without changing price selection or calculations.

The `dashboard_warmup` JSON log includes status, snapshot ID and stage timings.
Persistence failure logs `severity=ERROR` and exits the job with code 1, while
the successfully imported records and their success audit remain intact. The
dashboard retains its normal on-demand rebuild fallback. Retry preparation
without contacting IBKR or changing the import marker with:

```bash
gcloud run jobs execute ibkr-flex-import \
  --project=options-performance-dashboard --region=europe-west6 \
  --args=-m,portfolio_backend.ibkr.import_job,--warm-only --wait
```

For other invocations preparation is opt-in via `--warm-dashboard` or the
environment flag. Disable it with `IBKR_IMPORT_WARM_DASHBOARD=0` if needed;
normal interactive refresh remains available.

Manual date overrides are available and take precedence over the rolling window:

```bash
python -m portfolio_backend.ibkr.import_job --from 2026-05-01 --to 2026-05-09
```

or:

```text
IBKR_IMPORT_FROM=YYYY-MM-DD
IBKR_IMPORT_TO=YYYY-MM-DD
```

For old rolling-window behavior without coverage diagnosis, set
`IBKR_IMPORT_LAST_DAYS`.

## Service Account

The prepared create script uses the existing Cloud Run runtime service account:

```text
595990983720-compute@developer.gserviceaccount.com
```

Required access:

```text
Secret Manager accessor on ibkr-flex-token
Secret Manager accessor on ibkr-flex-query-id
Storage object read/write on gs://options-portfolio-ibkr-raw-595990983720
Firestore read/write via roles/datastore.user
Logging writer
```

A narrower import service account is preferable before long-term scheduling, but
the current runtime service account already has the required least-scope bucket
and secret grants plus Firestore access.

## Storage Writes

Cloud Storage raw XML:

```text
gs://options-portfolio-ibkr-raw-595990983720/ibkr/flex/activity/query-<query_id>/run_date=YYYY-MM-DD/run_id=<run_id>/activity.xml
```

Firestore:

```text
ibkr_import_runs/{run_id}
ibkr_raw_rows/{raw_row_id}
ibkr_transactions/{transaction_id}
refresh_runs/{run_id}
```

The job stores raw XML in Cloud Storage, not Firestore. Firestore receives import
metadata, relevant raw XML element attributes, normalized Trade/OptionEAE
transactions, and an audit entry.

The job entrypoint uses the shared `IbkrImportService`, so local imports,
Cloud Run imports, and Firestore-shaped documents use the same raw-row and
transaction document schema.

## Deployment

Do not schedule this until the code is merged and the backend image has been
rebuilt from the canonical GitHub source. A manual validation image has already
proved the job runtime path, IAM, GCS writes, Firestore writes, and dedupe.

Create or update command:

```bash
IBKR_IMPORT_INCEPTION_DATE=2022-11-01 \
infra/cloud-run-jobs/create_ibkr_flex_import_job.sh
```

`IBKR_IMPORT_INCEPTION_DATE` is required by the helper script so the historical
baseline is explicit deployment configuration, not a hidden code default.

To pin a specific image:

```bash
IBKR_IMPORT_INCEPTION_DATE=2022-11-01 \
IMAGE_URI=europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/options-roi-mobile-api:<commit-sha> \
infra/cloud-run-jobs/create_ibkr_flex_import_job.sh
```

Run manually after creation:

```bash
gcloud run jobs execute ibkr-flex-import \
  --project=options-performance-dashboard \
  --region=europe-west6
```

Run a one-off explicit range, for example:

```bash
gcloud run jobs execute ibkr-flex-import \
  --project=options-performance-dashboard \
  --region=europe-west6 \
  --args=-m,portfolio_backend.ibkr.import_job,--from,2026-05-01,--to,2026-05-08
```

## Current Production Validation

Current job:

```text
Job: ibkr-flex-import
Region: europe-west6
Runtime service account: 595990983720-compute@developer.gserviceaccount.com
Scheduler: ibkr-flex-import-daily, 45 19 * * *, Europe/Zurich
Flex Query ID: 1504277
Max retries: 0
```

The importer skips standalone weekend-only gaps for Activity Flex statements,
spaces Flex `SendRequest` calls to stay inside IBKR token pacing limits, and
backs off once if IBKR returns rate-limit error `1018`.

Production uses lean Activity Flex Query `1504277`, containing only:

```text
Trades
Option Exercises, Assignments and Expirations
Cash Transactions
```

Manual validation of the lean query completed on `2026-05-10`:

| Metric | Value |
|---|---:|
| Execution | `ibkr-flex-import-hlhdn` |
| Status | `succeeded_with_deferred` |
| Succeeded chunks | 4 |
| Deferred chunks | 1 |
| Deferred date | `2026-05-09` |
| `ibkr_raw_rows` inserted | 109 |
| `ibkr_raw_rows` updated | 3,293 |
| `ibkr_transactions` inserted | 0 |
| `ibkr_transactions` updated | 2,156 |

Current Firestore state after old-query cleanup:

| Collection | Count |
|---|---:|
| `ibkr_import_runs` | 4 |
| `ibkr_raw_rows` | 3,402 |
| `ibkr_transactions` | 2,156 |

Old raw XML and Firestore rows for query `1503002` were deleted after the lean
query was validated.
