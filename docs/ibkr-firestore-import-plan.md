# IBKR Firestore Import Plan

This plan replaces the Google Sheet as the manually maintained transaction
source with an automated IBKR Flex Web Service import backed by Firestore.

## Current State

- The app currently reads option rows from a Google Sheet export in
  `data_sources.py`.
- Streamlit and the mobile FastAPI service both flow through the shared
  portfolio pipeline.
- The pipeline expects a normalized options DataFrame with these core columns:
  `trans_date`, `ticker`, `type`, `action`, `expiration`, `strike`, `qty`,
  `amount`, `commission`, `total_pnl`, `assigned_flag`, `comment`, and
  `source_sheet`.
- Firestore already exists in project `options-performance-dashboard` and is
  used by deployed infrastructure.

## IBKR Source

- Flex Query ID: `1503002`
- Query name: `Options DB Import - Activity`
- Format: XML
- Period: `Last 365 Calendar Days`
- Raw bucket: `options-portfolio-ibkr-raw-595990983720`
- Runtime secrets:
  - `IBKR_FLEX_TOKEN` from Secret Manager secret `ibkr-flex-token`
  - `IBKR_FLEX_QUERY_ID` from Secret Manager secret `ibkr-flex-query-id`
  - `IBKR_RAW_BUCKET` set to the raw bucket name

The first successful pull returned a 10.6 MB XML report for `2025-05-09` to
`2026-05-08`. Key returned sections were:

| Section | Rows |
|---|---:|
| `Trade` | 630 |
| `OptionEAE` | 167 |
| `CashTransaction` | 437 |
| `OpenPosition` | 43 |
| `SecurityInfo` | 226 |
| `PriorPeriodPosition` | 10,076 |
| `ConversionRate` | 12,167 |
| `TierInterestDetail` | 545 |
| `EquitySummaryByReportDateInBase` | 262 |

## Historical Backfill

IBKR Flex Web Service supports date override parameters on `SendRequest`:

```text
fd=YYYYMMDD
td=YYYYMMDD
```

Each override request is limited to 365 calendar days, so historical import
should split the account history into chunks. IBKR sometimes rejects broad
windows with `1003: Statement is not available` even when smaller windows inside
that period are available, so the importer should recursively split rejected
windows until it reaches a small minimum window.

For the current archive window on 2026-05-09, a simple 365-day plan is:

| Chunk | From | To | Days |
|---:|---|---|---:|
| 1 | 2022-01-01 | 2022-12-31 | 365 |
| 2 | 2023-01-01 | 2023-12-31 | 365 |
| 3 | 2024-01-01 | 2024-12-30 | 365 |
| 4 | 2024-12-31 | 2025-12-30 | 365 |
| 5 | 2025-12-31 | 2026-05-09 | 130 |

Local dry-run:

```bash
.venv/bin/python scripts/ibkr_backfill.py --from 2022-01-01 --to 2026-05-09 --dry-run
```

Local fetch into ignored files:

```bash
.venv/bin/python scripts/ibkr_backfill.py --from 2022-05-09 --to 2026-05-08 --max-days 180 --min-split-days 1
```

If a large chunk returns `1003: Statement is not available`, the local script now
splits the failed range automatically:

```bash
.venv/bin/python scripts/ibkr_backfill.py --from 2025-06-24 --to 2025-12-20 --max-days 180 --min-split-days 1
```

Default output path:

```text
tmp/ibkr_backfill/query-1503002/YYYYMMDD-YYYYMMDD.xml
```

The local four-year backfill from `2022-05-09` through `2026-05-08` was
retrievable from IBKR, with two isolated unavailable non-trading calendar days:
`2022-12-31` and `2025-12-20`. The successful local XML set currently contains
22 files. After dedupe across overlapping exports, relevant unique row counts
are:

| Section | Unique rows |
|---|---:|
| `Trade` | 2,028 |
| `OptionEAE` | 370 |
| `CashTransaction` | 1,349 |
| `OpenPosition` | 773 |
| `SecurityInfo` | 563 |

The production importer should use the same chunking and split-on-unavailable
logic, but write raw XML to Cloud Storage instead of local disk and write import
metadata to Firestore. Daily refresh can use the latest rolling window and rely
on dedupe to upsert only new records.

## Local Backend Source Switch

The mobile API now has an opt-in IBKR source path. The default remains Google
Sheets.

Local IBKR mode:

```bash
OPTIONS_DATA_SOURCE=ibkr \
IBKR_FLEX_REPORT_DIR=tmp/ibkr_backfill/query-1503002 \
uvicorn mobile_api:app --reload
```

If `IBKR_FLEX_REPORT_DIR` is omitted, the loader falls back to
`tmp/ibkr_backfill/query-${IBKR_FLEX_QUERY_ID}` after reading `.env`.

Persisted local JSON mode:

```bash
OPTIONS_DATA_SOURCE=ibkr \
IBKR_REPORT_SOURCE=local_json \
IBKR_IMPORT_JSON_DIR=tmp/ibkr_import/firestore_sim \
uvicorn mobile_api:app --reload
```

Future Firestore mode:

```bash
OPTIONS_DATA_SOURCE=ibkr \
IBKR_REPORT_SOURCE=firestore \
uvicorn mobile_api:app --reload
```

In IBKR mode:

- `/v1/mobile/*` uses the IBKR Flex pipeline.
- Available source sheet is reported as `IBKR Flex`.
- Old iOS requests that still send `Options 2024/2025/2026` are normalized to
  `["IBKR Flex"]` before cache keys and payloads are built.
- Mobile config reports `supports_selected_sheets = false` for IBKR mode.
- Raw XML is loaded and deduped locally by section/natural key.
- Stock and dividends are wheel-scoped from IBKR rows; unrelated portfolio
  stock activity remains excluded.

This is a bridge for local validation. Production should replace the local XML
loader with Cloud Storage/Firestore reads while preserving the same
`IbkrFlexReport -> build_ibkr_pipeline -> mobile payloads` interface.

## Storage Model

Firestore should not store the full raw XML blob because Firestore documents
are limited to 1 MiB. Store raw reports in Cloud Storage and store metadata,
parsed rows, and normalized transactions in Firestore.

### Cloud Storage

Bucket:

```text
gs://options-portfolio-ibkr-raw-595990983720
```

Object naming:

```text
ibkr/flex/activity/query-1503002/run_date=YYYY-MM-DD/run_id=<run_id>/activity.xml
```

Optional derivative objects:

```text
ibkr/flex/activity/query-1503002/run_date=YYYY-MM-DD/run_id=<run_id>/section-counts.json
ibkr/flex/activity/query-1503002/run_date=YYYY-MM-DD/run_id=<run_id>/parse-summary.json
```

### Firestore Collections

`ibkr_import_runs/{run_id}`

Tracks each import attempt.

Suggested fields:

```text
run_id
query_id
query_name
source = "ibkr_flex"
report_type = "activity"
status = "running" | "succeeded" | "failed"
started_at
finished_at
from_date
to_date
period
raw_bucket
raw_object
xml_bytes
section_counts
inserted_raw_rows
updated_raw_rows
inserted_transactions
updated_transactions
skipped_duplicates
error_code
error_message
```

`ibkr_raw_rows/{raw_row_id}`

Stores one parsed XML element per relevant section. This is the audit layer.

Suggested fields:

```text
raw_row_id
run_id
query_id
section
source_row_hash
natural_key
account_id
report_date
trade_date
date_time
conid
symbol
underlying_symbol
transaction_id
trade_id
ib_exec_id
raw
created_at
updated_at
```

The `raw` field stores the XML attributes as a map. If a row ever approaches
Firestore document limits, split rarely used attributes into
`ibkr_raw_row_payloads/{raw_row_id}` or keep only the Cloud Storage pointer.

`ibkr_transactions/{transaction_id}`

Stores deduplicated normalized activity rows used by the app and future
analytics.

Suggested fields:

```text
transaction_id
source = "ibkr_flex"
source_section
raw_row_id
account_id
currency
asset_category
sub_category
symbol
underlying_symbol
description
conid
trade_id
ib_exec_id
ib_order_id
ib_transaction_id
related_trade_id
related_transaction_id
trade_date
date_time
settle_date
expiry
strike
put_call
buy_sell
transaction_type
open_close_indicator
quantity
multiplier
trade_price
trade_money
proceeds
ib_commission
taxes
net_cash
cost_basis
realized_pnl
mtm_pnl
notes
dedupe_key
created_at
updated_at
```

`ibkr_positions/{position_id}`

Stores current position snapshots from each import.

Suggested key:

```text
<account_id>|<report_date>|<conid>|<expiry>|<strike>|<put_call>
```

`portfolio_source_snapshots/{snapshot_id}`

Materialized source health and app-facing snapshot metadata. This can coexist
with existing `source_snapshots`.

Suggested fields:

```text
snapshot_id
source = "ibkr_firestore"
as_of
latest_import_run_id
latest_raw_object
from_date
to_date
trade_count
option_eae_count
cash_transaction_count
open_position_count
issues
created_at
```

## Dedupe Rules

The importer should be idempotent. Re-reading the last 365 days is expected.

Build a deterministic dedupe key per section:

1. `Trade`:
   - Prefer `accountId|tradeID|transactionID|ibExecID` when present.
   - If `ibExecID` is missing, use `accountId|tradeID|transactionID`.
   - Fallback to a canonical row hash over stable trade fields.
2. `OptionEAE`:
   - Prefer `accountId|tradeID|date|transactionType|conid|quantity`.
   - Fallback to canonical row hash.
3. `CashTransaction`:
   - Prefer `accountId|transactionID|actionID|dateTime|type`.
   - Fallback to canonical row hash.
4. `OpenPosition`:
   - Snapshot key, not a permanent transaction key:
     `accountId|reportDate|conid|expiry|strike|putCall`.

Canonical row hash:

```text
sha256(section + "\n" + sorted_json(attributes_without_run_specific_fields))
```

Exclude run-specific fields from the hash if IBKR provides any values that can
change between identical exports.

## App Adapter

Add a Firestore-backed source adapter that produces the current pipeline input
shape. Do not initially change the calculation pipeline.

The first local comparison found that IBKR is a richer source than the current
Google Sheet. The sheet path mostly loads option rows and derives `StockTxn`
records from assigned option flags. IBKR returns explicit stock trades and
stock-side `OptionEAE` rows. Therefore, the final cutover should not rely only
on the sheet-shaped option adapter. It also needs an explicit stock transaction
adapter or a pipeline extension that can consume IBKR stock activity directly.

Mapping from `Trade` to current option row:

| Current column | IBKR source |
|---|---|
| `trans_date` | `tradeDate` or date part of `dateTime` |
| `ticker` | `underlyingSymbol` for options, else `symbol` |
| `type` | `putCall` mapped to `Put` or `Call` |
| `action` | `buySell` mapped to `Buy` or `Sell` |
| `expiration` | `expiry` |
| `strike` | `strike` |
| `qty` | absolute `quantity` |
| `amount` | `proceeds` or `netCash` before commission handling is finalized |
| `commission` | `ibCommission` |
| `total_pnl` | current-compatible signed cash value; validate against sheet |
| `assigned_flag` | derived from matching `OptionEAE` assignment/exercise rows |
| `comment` | compact IBKR audit note |
| `source_sheet` | `IBKR Flex <year>` or `IBKR Flex` |

The first implementation should keep a comparison mode:

```text
Google Sheet normalized rows
IBKR Firestore normalized rows
diff by ticker / expiry / strike / action / date / qty / cash amount
```

Only switch the default source after the comparison is acceptable.

## Import Lifecycle

1. Start run document with `status = "running"`.
2. Call IBKR `SendRequest`.
3. Poll `GetStatement` until XML is ready.
4. Save XML to Cloud Storage.
5. Parse XML sections and section counts.
6. Upsert raw row documents by `raw_row_id`.
7. Upsert normalized transaction documents by `transaction_id`.
8. Write position snapshot documents.
9. Write source snapshot metadata.
10. Finish run document with counts and `status = "succeeded"`.

On failure:

```text
status = "failed"
error_code
error_message
finished_at
```

Do not delete prior successful data on a failed run.

## Importer Boundary Implemented

The repo now has a testable importer boundary:

```text
IBKR XML bytes
  -> IbkrImportService
  -> RawReportStore
  -> ImportRecordStore
```

Implemented stores:

- `LocalRawReportStore`: writes raw XML under `tmp/ibkr_import/raw`.
- `LocalJsonImportStore`: writes Firestore-shaped JSON documents under
  `tmp/ibkr_import/firestore_sim`.
- `GcsRawReportStore`: optional production raw XML writer using
  `IBKR_RAW_BUCKET`.
- `FirestoreImportStore`: optional production writer for `ibkr_import_runs`,
  `ibkr_raw_rows`, and `ibkr_transactions`.
- `LocalJsonFlexReportRepository`: rebuilds an `IbkrFlexReport` from local
  Firestore-shaped JSON raw-row documents.
- `FirestoreFlexReportRepository`: rebuilds an `IbkrFlexReport` from
  Firestore `ibkr_raw_rows`.

CLI:

```bash
# Parse/summarize without writing.
.venv/bin/python scripts/ibkr_import_once.py \
  --xml-path tests/fixtures/ibkr_flex_sample.xml \
  --dry-run

# Local write simulation.
.venv/bin/python scripts/ibkr_import_once.py \
  --xml-dir tmp/ibkr_backfill/query-1503002 \
  --output-dir tmp/ibkr_import

# Production-oriented writer, once Cloud Run Job is ready.
.venv/bin/python scripts/ibkr_import_once.py \
  --from 2026-05-01 \
  --to 2026-05-09 \
  --store gcp

# Compare direct XML mode with persisted local JSON mode.
.venv/bin/python scripts/ibkr_compare_report_sources.py \
  --as-of 2026-05-08 \
  --xml-dir tmp/ibkr_backfill/query-1503002 \
  --json-dir tmp/ibkr_import/firestore_sim
```

The GCP path imports Google libraries only when `--store gcp` is used. Runtime
dependencies are listed in `requirements.txt`:

```text
google-cloud-firestore
google-cloud-storage
```

## Implementation Stages

1. Local parser and schema inspection. Done.
   - Input: `/tmp/ibkr-flex-1503002.xml`.
   - Output: section counts and normalized row previews with sensitive values
     redacted in logs.
2. Sheet-shaped option adapter and comparison mode. Done.
   - Map IBKR option `Trade` rows to the current DataFrame contract.
   - Exclude long option legs by default because the current pipeline models
     short option lots (`SELL/O` opens, `BUY/C` closes).
   - Compare adapter output and pipeline output against Google Sheet output.
3. Historical backfill mode. Done locally.
   - Fetch date ranges with `fd`/`td` overrides.
   - Split unavailable broad windows recursively.
   - Persist exact failed calendar windows in import metadata instead of
     masking them.
4. Explicit stock activity design. Done for wheel scope.
   - Map IBKR `Trade` rows with `assetCategory=STK`.
   - Map stock-side `OptionEAE` rows created by assignment/exercise.
   - Decide whether to extend `build_base_pipeline` to accept explicit
     `StockTxn` rows or create a new transaction-source layer before pipeline
     calculation.
5. Local Firestore emulator or dry-run mode. Done with JSON-backed local store.
   - Validate dedupe keys and normalized transaction mapping.
6. Firestore writer. Implemented and manually validated through Cloud Run Job.
   - Write `ibkr_import_runs`, `ibkr_raw_rows`, and `ibkr_transactions`.
   - `ibkr_positions` remains pending.
7. Cloud Storage raw writer. Implemented and manually validated through Cloud
   Run Job.
   - Save XML before parsing so the raw report is retained.
8. Source adapter. Partially done.
   - Local XML adapter feeds the IBKR pipeline and mobile API.
   - Persisted raw-row repositories can rebuild an `IbkrFlexReport`.
   - Mobile API still defaults to local XML in IBKR mode until Firestore import
     is deployed and validated.
9. Comparison command. Done locally.
   - Compare Google Sheet rows to IBKR-derived rows for overlapping periods.
10. App source switch. Done locally.
   - Add an environment-driven source selection:
     `OPTIONS_DATA_SOURCE=google_sheets|ibkr`.
11. Mobile and Streamlit validation. Mobile local path done; Streamlit source
    switch still pending.
   - Run existing tests and mobile smoke tests against both data sources.
12. Scheduled import. Job manually validated; scheduler pending.
   - Cloud Run Job `ibkr-flex-import` succeeded for `2026-05-01` through
     `2026-05-08`.
   - Re-importing the same range inserted no duplicate rows and only updated
     the existing Firestore documents.
   - Scheduled runs should use `IBKR_IMPORT_LAST_DAYS=14` and
     `IBKR_IMPORT_TO_OFFSET_DAYS=1` so they stop at yesterday's completed IBKR
     statement.

## Cloud Run Job Validation

Manual validation completed on `2026-05-09` with no scheduler enabled.

The current local backend image was built as:

```text
europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/ibkr-flex-import:manual-20260509T203310Z
```

The Cloud Run Job `ibkr-flex-import` was created in `europe-west6` using runtime
service account:

```text
595990983720-compute@developer.gserviceaccount.com
```

Validation range `2026-05-01` through `2026-05-09` failed with IBKR error
`1003: Statement is not available`, which confirms that same-day statements may
not be ready. Validation range `2026-05-01` through `2026-05-08` succeeded.

Successful first run:

| Metric | Value |
|---|---:|
| Execution | `ibkr-flex-import-4v7k4` |
| Raw XML bytes | 309,967 |
| `ibkr_raw_rows` inserted | 119 |
| `ibkr_transactions` inserted | 16 |

Dedupe rerun:

| Metric | Value |
|---|---:|
| Execution | `ibkr-flex-import-zqp7g` |
| `ibkr_raw_rows` inserted | 0 |
| `ibkr_raw_rows` updated | 119 |
| `ibkr_transactions` inserted | 0 |
| `ibkr_transactions` updated | 16 |

The validation confirms Cloud Run runtime IAM, Secret Manager access, GCS raw
archive writes, Firestore writes, and idempotent dedupe behavior. The remaining
production step is scheduler setup after the IBKR source branch is merged and a
canonical backend image is built from GitHub `main`.

## Cutover Criteria

- Latest import succeeds repeatedly without duplicate growth.
- IBKR-derived option rows reconcile against the Google Sheet for the overlap
  period within known manual-data differences.
- Open option shorts, assigned holdings, monthly performance, yearly
  performance, and mobile API payloads remain stable.
- Source freshness endpoints identify IBKR import time, report period, and any
  missing sections.

## Open Questions

- Whether to import exactly four years from IBKR or also keep older Google Sheet
  rows as a separate non-authoritative reference.
- Whether option expiration without explicit `OptionEAE` rows should be inferred
  from open lots, as the current pipeline does.
- Whether dividends should continue coming from yfinance in the first version or
  switch to `CashTransaction` dividend rows immediately.
- Whether Cloud Run should expose a protected manual refresh endpoint or use a
  separate Cloud Run job for imports.
- Whether the existing pipeline should be extended to accept explicit stock
  transactions, or whether a higher-level transaction model should build both
  option lots and stock lots before performance calculation.
