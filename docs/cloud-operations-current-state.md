# Cloud Operations Current State

Last reviewed: 2026-05-12.

## Production Services

- Project: `options-performance-dashboard`
- Region: `europe-west6`
- Mobile API service: `options-roi-mobile-api`
- Web dashboard service: `options-roi-web`
- IBKR import job: `ibkr-flex-import`
- IBKR import scheduler: `ibkr-flex-import-daily`, `30 7 * * *`,
  `Europe/Zurich`
- IBKR import job retries: `0`, so IBKR token pacing errors are not amplified
  by immediate Cloud Run retries.
- Manual IBKR import trigger:
  - Web dashboard Diagnostics tab: `Retry IBKR import`
  - Mobile API: `POST /v1/mobile/import`
  - The action starts the Cloud Run Job asynchronously. After it finishes,
    regular `Refresh data` / `POST /v1/mobile/refresh` reloads the new import
    marker and current prices.

There is no separate Cloud Run Streamlit service. The old
`options-roi-streamlit` service was deleted after the FastAPI web dashboard
became the production web UI.

Production mobile and web reads use:

```text
OPTIONS_DATA_SOURCE=ibkr
IBKR_REPORT_SOURCE=firestore
IBKR_RAW_BUCKET=options-portfolio-ibkr-raw-595990983720
```

Current IBKR Activity Flex Query ID: `1504277`. This is the lean production
query containing only `Trades`, `Option Exercises, Assignments and Expirations`,
and `Cash Transactions`.

The import planner treats standalone weekend-only Activity Flex gaps as
non-importable, and the Flex client spaces `SendRequest` calls to stay within
IBKR pacing limits.

If IBKR has not published the trailing business-day statement yet, the import
job records a deferred run. Production issue payloads surface unresolved
deferred/failed import attempts as actionable `import` warnings until a later
successful import covers the same date. This keeps the dashboards honest while
avoiding repeated automatic IBKR import attempts throughout the day.

## Persistent Storage

- Firestore Native `(default)`, location `europe-west6`
- Raw IBKR XML bucket: `gs://options-portfolio-ibkr-raw-595990983720`
- Build/source staging buckets:
  - `gs://run-sources-options-performance-dashboard-europe-west6`
  - `gs://options-performance-dashboard_cloudbuild`

The raw IBKR bucket keeps current raw XML objects and has object versioning
enabled. Noncurrent object versions are lifecycle-cleaned after 30 days.
Build/source staging buckets are lifecycle-cleaned after 7 days.

Firestore is not just a raw-data store. Production refresh also uses
`pipeline_snapshots` to persist the computed base accounting pipeline by IBKR
import marker, `as_of` date, and normalized source partition. Mobile and web
Cloud Run instances restore this shared snapshot before fetching current
prices. A full rebuild is expected only when the IBKR import marker changes,
the snapshot is missing/corrupt, or the snapshot schema is intentionally bumped.
Manual refreshes also reuse the latest successful refreshed context when it is
recent and tied to the same IBKR import marker. This avoids duplicate web/iOS
price refresh work when both clients refresh within the shared freshness window
(`SHARED_REFRESH_REUSE_SECONDS`, default 300 seconds).

## Artifact Registry

Container images are stored in:

```text
europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy
```

Cleanup policy:

- delete untagged images older than 6 hours;
- delete tagged images older than 3 days;
- keep the 10 most recent versions.

Before tightening retention further, confirm active Cloud Run services and jobs
do not still reference older image tags.

## Development Workflow

Cloud Run deployments have a fixed latency floor because Cloud Build still has
to build, push, and roll a revision. The repository is configured to keep the
Cloud Build context small via `.gcloudignore` and `.dockerignore`. The build
also reuses the previous `latest` image as a Docker layer cache so unchanged
Python dependencies are not reinstalled on every code-only deploy. For web UI
iteration, prefer local browser testing and deploy to Cloud Run only at stable
checkpoints.

The production build trigger reads the repository `cloudbuild.yaml`. It creates
one shared image and deploys it to both `options-roi-mobile-api` and
`options-roi-web`, so a `main` deployment cannot accidentally leave the web
dashboard on an older backend commit.
