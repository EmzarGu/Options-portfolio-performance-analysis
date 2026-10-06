# Cloud Operations Current State

Live web/mobile/import-job checks: 2026-10-01; scheduler checks: 2026-09-27. Storage/retention descriptions below
remain from the 2026-05-12 operational record and were not revalidated in this review.

## Latest completed-chain accounting release — 2026-10-01

Web `options-roi-web-00128-5hd` and mobile
`options-roi-mobile-api-00179-w9g` are Ready, each serving 100% of
its service traffic. Both and the existing import job use immutable digest
`sha256:5baddbbbb67256e1a0e009fdd514473c8bb1cc84a7a786f19a21caecbd6a581e`. All 565 tests passed locally and in the production container;
health endpoints returned HTTP 200. There remains one production dashboard.

Confirmed rolls carry all prior net cash/fees until the strategy quantity ends;
open balances are never realized. Schema-8 warm-up `ibkr-flex-import-j42t9`
succeeded in 28.58 seconds. The independently verified production totals are
2025 options [private reconciliation amount] 2025 total [private reconciliation amount] 2026 total [private reconciliation amount] August
[private reconciliation amount] and September [private reconciliation amount] November's open strategy balance is [private reconciliation amount]
Stock P&L, dividends and capital denominators are unchanged.

Web screens and authenticated mobile payloads match all recalculated totals.
Both Decision Lab views show candidates using 74 September-30 contracts and
80/80 ready selections. Provider quota stayed at 80 used/20 remaining; browser
error logs are empty. Schedules are unchanged. See
[completed-chain rule and deployment evidence](completed-roll-chain-accounting-2026-10-01.md).

## Previous contract-recognition release — 2026-10-01

Web `options-roi-web-00127-4tl` and mobile `options-roi-mobile-api-00178-pcj`
are Ready, each receiving 100% of its service traffic. Both services and
`ibkr-flex-import` use the same immutable image digest
`sha256:32cdd783a06aca6eab5471c17c4ca200da8dfc90d43cf521d9e587443305f01b`.
All 559 tests passed locally and in the production container. Both health
endpoints returned HTTP 200. No additional dashboard service was created.

Option rolls now realize only the closed contract's result; replacement premium
remains open until that contract closes, expires or is assigned. Schema-7
snapshot preparation `ibkr-flex-import-qvtld` succeeded. September 2026 shows
[private reconciliation amount] options P&L and [private reconciliation amount] total realized P&L; [private reconciliation amount] November premium
remains open. Nineteen historical months are restated. Assignment stock
attribution and dividend cashflows are unchanged.

Both live Decision Lab views show alternatives, 79 stored contracts dated
September 30 and 85/85 selections ready. Provider quota stayed at 80 credits
used throughout verification. Schedules and provider policy were not changed.
See [accounting correction, validation and rollback](realized-pnl-correction-2026-10-01.md).

## Previous web and import-job release — 2026-09-30

Web `options-roi-web-00126-tgx` is Ready with 100% of dashboard traffic. Web and
`ibkr-flex-import` use `lab-snapshot-fix-20260930-174752`. Decision Lab retains a
recent complete saved quote date while newer selections are incomplete and
reports download progress separately from field coverage. All 556 tests passed
locally and in the production container.

Both Lab views show 73 contracts dated September 29, 78/78 selections ready and
24 comparison rows. Warm-only execution `ibkr-flex-import-7v54t` succeeded without
additional provider credit usage. No schedules or accounting data were changed.
See [refresh continuity, validation and rollback](decision-lab-refresh-continuity-2026-09-30.md).

## Previous web and import-job release — 2026-09-27

Web `options-roi-web-00125-fts` is Ready with 100% of dashboard traffic. Web and
`ibkr-flex-import` share image `lab-calculation-fix-20260927-175940`. This release
corrects covered-call roll-down proceeds and omitted unbooked premium in exercise
scenarios, and clarifies comparison labels. All 547 tests passed locally and in
the production container. Both browser views display the corrected results;
health is OK and the checked Lab requests took 0.98–1.54 seconds.

Warm-only execution `ibkr-flex-import-fdlvd` succeeded, reading 67 stored contracts
with zero missing selections and no provider credits spent. Quotes remain dated
24 September; no newer data was fetched by these checks. Both existing import
schedules remain enabled and unchanged. One dashboard revision serves traffic.
See [release, verification and rollback details](decision-lab-calculation-fix-2026-09-27.md).

## Previous web and import-job release — 2026-09-23

Web `options-roi-web-00124-c6x` is Ready and receives 100% of dashboard traffic.
Both it and `ibkr-flex-import` use `marketdata-20260923-202818`. All 536 tests passed
in the final production container; `/health` returned HTTP 200. Market Data is now
enabled for dated Decision Lab comparisons, backed by shared snapshots and a
90-credit application ceiling. The existing 07:15 and 19:45 Europe/Zurich schedules
are enabled and unchanged. No additional production dashboard was created.

Warm-only execution `ibkr-flex-import-485m9` verified the new backend preparation
path on the initial integration image; the final image adds the main dashboard's
quote-date and credit labels and updates docstrings. The job saved 62 contracts,
stopping at the 90-credit ceiling with three AU selections pending. Browser checks
on the final release confirmed the quote date, pending count and dated-estimate
labels. Mobile and historical enrichment services were not redeployed.
See [release evidence and operating limits](decision-lab-marketdata-production-2026-09-23.md).

## Previous web and import-job release — 2026-09-20 08:25 UTC

Web `options-roi-web-00122-mr7` is Ready with 100% of web traffic. The web and
existing IBKR import job now use `morning-refresh-20260920-082304`, after **483
tests passed** in the production image. The job prepares and verifies the default
dashboard snapshot after import. Execution `ibkr-flex-import-jwncq` succeeded;
the first browser refresh reused its snapshot in **4.11 seconds**, versus
22.20 seconds earlier that morning. The following refresh took 0.78 seconds.
See the [release and performance evidence](morning-refresh-performance-2026-09-20.md).
The mobile API and historical options job were not redeployed by this release.
Both existing IBKR schedules remain enabled at 07:15 and 19:45 Europe/Zurich.

## Production release update — 2026-09-19 19:00 UTC

The [authorized refactor release](production-deployment-2026-09-19.md) supersedes
the web/mobile/import-job image observations in the earlier review below. Web
`options-roi-web-00119-wnr` and mobile `options-roi-mobile-api-00177-kqd` are Ready,
each receiving 100% of traffic, on the same tested image. The IBKR import job was
updated to that image without starting an execution. Both public health endpoints
returned HTTP 200. All 457 tests passed in the production container before release.
The historical option-market job and scheduler configuration were not changed.

## Earlier review snapshot — 2026-09-19

| Component | Observed state |
| --- | --- |
| Web service | Ready; 100% traffic to `options-roi-web-00118-hnn`; image tag `9cdc5a31c68a3880244eb245c4c3e830c0f103bf`; `/health` HTTP 200. |
| Mobile API | Ready; 100% traffic to `options-roi-mobile-api-00176-w8q`; ready transition July 30; image digest `sha256:aa1ddd9e7883ee3a84ad811291498fc0c0263d1482a49d0290935cfdc8b68a5c`. Equivalence to the web build was not established. |
| IBKR import job | Image tag matches web; retries 0, timeout 1,800 seconds. Execution `ibkr-flex-import-plrjm` completed successfully September 19 at 17:45:28 UTC. Job success alone does not prove statement completeness. |
| IBKR schedulers | Morning `15 7 * * *` and evening `45 19 * * *`, both enabled, `Europe/Zurich`. |
| Historical options scheduler | `option-market-history-import-daily`, `15 8 * * 1-5`, enabled, `Europe/Zurich`. |
| Historical options job | `option-market-history-import`, retries 3, timeout 600 seconds; still configured with image tag `cycle-signal-20260530T102728Z`. Execution `option-market-history-import-x6zmc` has `Completed=False`: the image was not found. |

The historical job's summary reported `EXECUTION_PENDING`; its specific execution
condition identifies the missing-image failure. Do not treat the summary as proof
that an import is actively running. No job was started, stopped, or reconfigured
during this review.

This historical import failure occurs before any provider request. Separately,
the CuteMarkets website/API connection probes failed. Web configuration includes
a CuteMarkets Secret Manager reference, but that does not verify the key or service.
See [provider status](option-market-validation.md) and
[the refactoring/repair proposal](refactoring-review-2026-09-19.md).

## Production Services

- Project: `options-performance-dashboard`
- Region: `europe-west6`
- Mobile API service: `options-roi-mobile-api`
- Web dashboard service: `options-roi-web`
- IBKR import job: `ibkr-flex-import`
- IBKR import schedulers:
  - `ibkr-flex-import-morning`, `15 7 * * *`, `Europe/Zurich`
  - `ibkr-flex-import-daily`, `45 19 * * *`, `Europe/Zurich`
  The morning run makes the prior business-day Flex statement available before
  normal app use when IBKR has published it. The evening run remains a catch-up
  for late statement publication and recent-row corrections.
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
successful import covers the same date. If the Cloud Run job cannot start far
enough to write a failed import record, the apps still flag stale data when the
latest successful import `to_date` is older than `IBKR_IMPORT_STALE_DAYS`
(default 3). This keeps the dashboards honest while avoiding repeated automatic
IBKR import attempts throughout the day.

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
Cloud Run instances restore this shared base snapshot before fetching current
prices. A full rebuild is expected only when the IBKR import marker changes,
the snapshot is missing/corrupt, or the snapshot schema is intentionally bumped.
Full price-refreshed context snapshots are intentionally not reused; fresh reads
should never hide a stale IBKR import marker.

The morning-refresh change on 2026-09-20 adds post-import preparation of that
base snapshot to the same scheduled import job. See the
[import runbook](ibkr-cloud-run-job.md#preparing-the-dashboard-after-import)
for configuration, verification and retry behavior, and the
[performance record](morning-refresh-performance-2026-09-20.md) for measurements.

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

The repository `cloudbuild.yaml` creates one shared image and sequentially updates
`options-roi-mobile-api`, `options-roi-web`, and `ibkr-flex-import`. These updates
are not atomic; partial deployment failures or separate deployments can leave
targets on different builds. Verify each target after deployment. This review
did not revalidate the live build-trigger configuration.

The file does not update `option-market-history-import`. That omission leaves
its configured image independent of normal backend releases, as demonstrated by
the missing May image above. Decide whether to retain and repair this job when
agreeing the provider migration. Repair must include image availability and
end-to-end execution checks, not only a successful service deployment.

There is no test step in the current build configuration. Both build ignore files
exclude `tests/`, so a future test gate must deliberately supply test sources in
addition to running them against the selected runtime.
