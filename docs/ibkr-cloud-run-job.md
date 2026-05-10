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

The prepared job timeout is 7,200 seconds so a reset or empty Firestore database
can complete a multi-year backfill in one run while still emitting per-chunk
progress.

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

## Manual Cloud Validation

Manual validation completed on `2026-05-09`. No scheduler was enabled.

Validated image:

```text
europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/ibkr-flex-import:manual-20260509T203310Z
```

Created job:

```text
Job: ibkr-flex-import
Region: europe-west6
Runtime service account: 595990983720-compute@developer.gserviceaccount.com
```

The first attempted range, `2026-05-01` through `2026-05-09`, failed with IBKR
error `1003: Statement is not available`. This was an IBKR report-availability
failure, not a Google Cloud IAM/runtime failure.

The second range, `2026-05-01` through `2026-05-08`, succeeded:

| Metric | Value |
|---|---:|
| Execution | `ibkr-flex-import-4v7k4` |
| XML bytes | 309,967 |
| `ibkr_raw_rows` inserted | 119 |
| `ibkr_transactions` inserted | 16 |

Raw XML object:

```text
gs://options-portfolio-ibkr-raw-595990983720/ibkr/flex/activity/query-1503002/run_date=2026-05-09/run_id=ibkr-flex-1503002-20260509T203751Z-20260501-20260508/activity.xml
```

A dedupe rerun wrote a second raw archive object and produced `0` new rows:

| Metric | Value |
|---|---:|
| Execution | `ibkr-flex-import-zqp7g` |
| `ibkr_raw_rows` inserted | 0 |
| `ibkr_raw_rows` updated | 119 |
| `ibkr_transactions` inserted | 0 |
| `ibkr_transactions` updated | 16 |

Final Firestore counts after manual validation:

| Collection | Count |
|---|---:|
| `ibkr_import_runs` | 2 |
| `ibkr_raw_rows` | 119 |
| `ibkr_transactions` | 16 |
| `refresh_runs` | 18 |

The validation confirms:

- Secret access works for `ibkr-flex-token` and `ibkr-flex-query-id`.
- Bucket writes work for `options-portfolio-ibkr-raw-595990983720`.
- Firestore writes work for import runs, raw rows, normalized transactions, and
  refresh audit entries.
- Dedupe is idempotent across repeated imports of the same IBKR date range.
