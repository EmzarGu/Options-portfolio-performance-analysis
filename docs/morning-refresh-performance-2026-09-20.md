# Morning dashboard refresh performance — 2026-09-20

## Production diagnosis

On web revision `options-roi-web-00121-wqq`, the first refresh at 10:08 Zurich
took 22.20 seconds; the next refresh at 10:11 took 2.03 seconds. Subsequent
dashboard data reads took about 0.45–0.48 seconds.

The morning IBKR import had completed at 07:15. Snapshot identity includes its
completion marker, the requested accounting date and the selected partition.
The new import therefore required a new base snapshot, built by the first web
request. The default IBKR accounting date was September 18 (the previous US
market session), not September 20; the weekend itself did not advance that date.

Within the first request, historical-price loading/preparation took 11.41
seconds for 27 tickers. All 27 were Firestore cache hits, with zero historical
downloads. Benchmark history also used three cached series. Current-price
fetching took 0.73 seconds and source checking took 1.23 seconds. These stage
measurements are nested within context/pipeline timings and must not be summed
with their enclosing totals. One minimum Cloud Run instance was already enabled.

## Changes

- Convert a year of historical dates and prices in one batch instead of calling
  pandas separately for every row. Preserve invalid-row filtering, date
  normalization, numeric conversion, ordering, duplicate handling and float output.
- Enable dashboard preparation at the end of the existing import job, after all
  import chunks complete. Reuse the normal shared context builder and its build
  lease, import marker checks, previous trading day and snapshot schema.
- Verify the saved snapshot by reading it back before reporting success.
  Preparation failures emit a structured error and fail the job without changing
  successful import records. `--warm-only` retries preparation without importing.
- Preserve interactive current-price refresh and on-demand rebuild fallback.
  No financial formulas, displayed features, data providers or schedules change.

Both existing daily schedules (07:15 and 19:45 Europe/Zurich) use the updated
import job. See the [operational runbook](ibkr-cloud-run-job.md#preparing-the-dashboard-after-import).

## Validation

- Local suite: **483 passed**.
- New regressions cover mixed/invalid date rows, duplicates, empty history,
  cross-process snapshot reuse, invalidation after a new import, preparation
  failure, deferred statements, import failure and preparation-only retries.
- Replay of a saved production portfolio report and historical prices produced
  identical serialized base-pipeline fields before and after the parser change.
  Benchmark returns were stubbed identically in both replay runs; this replay
  does not measure network latency or external-provider behavior.
- Local history preparation: **1,638.32 ms → 74.38 ms**; local base pipeline:
  **2,172.25 ms → 607.27 ms**. Production measurements are recorded separately below.

## Release verification

Cloud Build `bb98b480-42a7-4877-be4c-7633e355a468` completed successfully at
08:25:06 UTC. All **483 tests passed** in the production container (one existing
warning). Image tag `morning-refresh-20260920-082304` has digest
`sha256:f34dcd7a59d7928e24bbf8b7b3a318ca3f4f4b74cdb9ce0e368b186578451bee`.

The release overlays only the three changed backend modules on the retained
production image. Web revision `options-roi-web-00122-mr7` is Ready and receives
100% of web traffic. The existing `ibkr-flex-import` job uses the same image with
preparation and both Firestore stores explicitly enabled. No second dashboard,
new scheduler, dependency upgrade or mobile API deployment was introduced.

Manual execution `ibkr-flex-import-jwncq` exercised the normal import followed
by a genuinely missing snapshot build. Both import chunks succeeded and the job
completed successfully at 08:26:09 UTC. Preparation verified persisted snapshot
`ibkr_flex:2026-09-18:4ed0749b40c9d49cd193cc36c5954ba2` before browser testing.

| Measurement | Before | After |
| --- | ---: | ---: |
| Historical-price loading/preparation, 27 cached tickers | 11.41 s in first web refresh | 0.90 s in background job |
| Base pipeline build | 16.43 s in first web refresh | 4.45 s in background job |
| First browser refresh after import | 22.20 s | 4.11 s |
| Following browser refresh | 2.03 s | 0.78 s |

These are observed request/stage timings, not latency guarantees. The new first
request was also the first tested on the new web revision: source checking took
2.00 seconds and current-price fetching took 1.96 seconds. Its snapshot lookup
took 0.11 seconds and `pipeline_snapshot_hit=1`; no pipeline rebuild ran in the
request. The next refresh occurred within the existing source-marker cache
window, so its 0.78-second time should not be assumed after that cache expires.
The following dashboard data request took 0.47 seconds.

The entire visible dashboard matched its pre-release DOM snapshot after removing
only the price-update timestamp. All 11 holdings were priced, actionable issues
remained zero, and the health endpoint returned HTTP 200. Next morning's exact
latency has not been measured; today's check exercised the same import-to-first-
refresh sequence used by the scheduled morning run.

Rollback: restore web revision `options-roi-web-00121-wqq` and restore the import
job image `refactor-20260919-185738-31cc83b8` (same Artifact Registry repository).
Alternatively disable background preparation using `IBKR_IMPORT_WARM_DASHBOARD=0`.
The snapshot format and financial data schema remain unchanged.
