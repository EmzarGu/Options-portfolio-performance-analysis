# IBKR Validation Report

This report captures the current local validation of the IBKR Flex import path
against the existing Streamlit/Google-Sheet source.

Scope:

- Useful comparison period: `2024-01-01` through `2026-05-08`.
- Current Streamlit source tabs: `Options 2024`, `Options 2025`, `Options 2026`.
- IBKR source: local Flex backfill XML in `tmp/ibkr_backfill/query-1503002`.
- IBKR comparison mode: wheel-scoped native performance logic.

The compatibility adapter remains available as an audit bridge, but the current
validation uses the IBKR-native wheel logic: puts can start wheel exposure;
calls count only while prior put-assignment stock inventory is held; stock P&L
and dividends are limited to assignment-derived holding periods.

## Automated Test Coverage

Focused IBKR tests:

```bash
.venv/bin/python -m pytest tests/test_ibkr_flex_import.py tests/test_ibkr_accounting_cases.py
```

Result:

```text
38 passed
```

Full repo test suite:

```bash
.venv/bin/python -m pytest
```

Result:

```text
252 passed
```

## IBKR Case Coverage

| Case | Current validation |
|---|---|
| Short put expires worthless | Covered by IBKR case test; books premium and creates no stock transaction. |
| Short call expires worthless | Covered by wheel eligibility tests; calls without prior put-assignment stock inventory are excluded. |
| Short put assigned | Covered by IBKR case test; assignment evidence sets `assigned_flag` and creates stock buy in compatibility mode. |
| Short call assigned | Covered by IBKR case test; assignment evidence creates stock sell only against assignment-derived inventory in native mode. |
| Partial buy-to-close | Covered by IBKR case test; realized close event is booked and remaining short lot stays open. |
| Roll old option into new option | Covered by IBKR case test; old leg closes, new leg opens independently. |
| Protective long option open/close | Covered by IBKR case test; excluded from short-strategy compatibility adapter. |
| Manual stock trade | Covered by IBKR case test; preserved as normalized IBKR transaction but excluded from option adapter. |
| Cash transaction / dividend row | Dedupe key covered; native dividend logic includes only rows eligible during assignment-derived holding periods. |
| Overlapping Flex reports | Covered by combined-report test; duplicate rows are removed by section-level dedupe. |
| Partially covered call execution | Covered by IBKR case test; pipeline dataframe receives the prorated quantity and cash, not the full raw trade row. |
| Overlapping covered calls | Covered by IBKR case test; assignment-derived shares are allocated to included open calls and cannot be reused by another overlapping call. |
| Empty persisted Firestore/local-json report | Covered by IBKR import test; loaders fail loudly instead of returning a valid empty portfolio. |

## Backfill Coverage

Local backfill covers `2022-05-09` through `2026-05-08`, but the comparison
below starts at `2024-01-01` because earlier data is noisy and not useful for
the current dashboard reconciliation.

Useful-period IBKR option adapter summary:

| Metric | Value |
|---|---:|
| Option adapter rows | 602 |
| Assigned option rows | 66 |
| Date min | 2024-01-17 |
| Date max | 2026-05-06 |
| Sell rows | 316 |
| Buy rows | 286 |

## Streamlit Comparison

Commands:

```bash
.venv/bin/python scripts/ibkr_compare_pipeline.py --xml-dir tmp/ibkr_backfill/query-1503002 --since 2024-01-01 --as-of 2024-12-31
.venv/bin/python scripts/ibkr_compare_pipeline.py --xml-dir tmp/ibkr_backfill/query-1503002 --since 2024-01-01 --as-of 2025-12-31
.venv/bin/python scripts/ibkr_compare_pipeline.py --xml-dir tmp/ibkr_backfill/query-1503002 --since 2024-01-01 --as-of 2026-05-08
```

All three comparison runs completed with `issues_count = 0` for the
IBKR option adapter.

| As of | Source | Input rows | Open options | Option cashflow P&L for year | Close-only lifecycle P&L for year | Stock P&L for year | Dividends for year | Total realized P&L for year |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2024-12-31 | Streamlit sheet | 65 | 0 | 12,670.46 | 12,670.46 | -3,150.00 | 90.00 | 9,610.46 |
| 2024-12-31 | IBKR cashflow | 132 | 0 | 12,698.35 | 12,698.35 | 0.00 | 0.00 | 12,698.35 |
| 2025-12-31 | Streamlit sheet | 200 | 18 | 24,771.73 | 24,771.73 | 2,600.00 | 616.60 | 27,988.33 |
| 2025-12-31 | IBKR cashflow | 315 | 20 | 29,561.96 | 12,663.78 | 0.00 | 0.00 | 29,561.96 |
| 2026-05-08 | Streamlit sheet | 249 | 15 | 18,213.80 | 18,213.80 | 3,900.00 | 87.00 | 22,200.80 |
| 2026-05-08 | IBKR cashflow | 155 | 20 | 20,684.44 | 16,201.53 | 0.00 | 0.00 | 20,684.44 |

## Interpretation

The earlier caveat about missing stock/dividend support explains only total P&L
differences. It does not explain option P&L differences. Stock transactions
should not change option P&L.

The 2024 option-only comparison is very close: IBKR option P&L differs from the
sheet option P&L by about `27.89`. That is a good sign for the basic option
trade mapping in simple sell/expire/assign cases.

The apparent 2025 option P&L gap was caused by using close-only lifecycle P&L
from the existing sheet-shaped pipeline as if it were dashboard strategy P&L.
That was the wrong comparison for IBKR rolls.

For rolled covered calls, IBKR records the old option close debit and the new
option opening credit separately. If only the old close is counted in the
current year and the new credit is deferred until the future expiry, the year can
show an artificial large option loss even when the actual roll cash movement was
small.

Corrected 2025 cashflow view:

| 2025 ticker | IBKR sell/open credits | IBKR buy/close debits | IBKR net option cashflow |
|---|---:|---:|---:|
| `GOOGL` | 11,798.95 | -10,648.51 | 1,150.44 |
| `AAPL` | 12,725.37 | -11,599.22 | 1,126.15 |

The whole 2025 IBKR short-option cashflow is `29,561.96`, versus the sheet's
option P&L of `24,771.73`. This is a plausible reconciliation gap for a manually
simplified sheet, not a nonsensical options loss.

For these roll chains, the current sheet often records only simplified sell rows
and comments such as `Moved from ...`, or stores a manually netted adjustment on
a later-dated/future-expiring row. The current pipeline then treats those rows as
open short option lots and realizes them at expiry or assignment. IBKR records
the accounting facts directly:

```text
old short call buy-to-close debit -> realized option loss now
new short call sell-to-open credit -> new open option lot
```

Example `GOOGL` 2025:

| Date | IBKR event | Realized option P&L |
|---|---|---:|
| 2025-07-03 | Close Jul 180 call | -145.48 |
| 2025-07-30 | Close Aug 190 call | -477.41 |
| 2025-08-25 | Close Sep 195 call | -690.41 |
| 2025-11-17 | Close Dec 210 call | -6,237.40 |

The same pattern appears in `AAPL`, including call close losses on `2025-04-14`,
`2025-07-03`, `2025-08-08`, and `2025-11-17`.

This is not a stock P&L issue. It is a transaction semantics issue: IBKR records
all execution-level cash movements, while the sheet is a manually compressed
strategy log.

The first sheet-compatible adapter was intentionally option-shaped. The local
IBKR backend path now passes explicit stock transactions into the shared
pipeline from stock-side `OptionEAE` rows and uses IBKR `CashTransaction` rows
for wheel-scoped dividends.

## Latest Backend Smoke

Local command:

```bash
.venv/bin/python - <<'PY'
from datetime import date
from portfolio_backend.ibkr.repository import load_local_flex_report
from portfolio_backend.ibkr.pipeline import build_ibkr_pipeline

report = load_local_flex_report()

def fetch_price_history(tickers, start, end):
    return {}, [], {"requested": len(tickers), "fetched": 0}

def align_benchmarks(tickers, idx):
    return {}

state = build_ibkr_pipeline(
    report,
    as_of=date(2026, 5, 8),
    include_unrealized_current_year=True,
    fetch_price_history_fn=fetch_price_history,
    align_benchmarks_monthly_fn=align_benchmarks,
)
print(len(state.df_opts), len(state.open_options), len(state.stock_txns), len(state.realized_sales), len(state.div_df))
print(state.yearly.loc[state.yearly["year"].isin([2024, 2025, 2026]), [
    "year", "realized_options_pnl", "realized_stock_pnl", "dividends", "total_realized_pnl"
]])
PY
```

Result:

| Metric | Value |
|---|---:|
| Wheel option rows | 582 |
| Open option rows | 20 |
| Wheel stock transactions | 74 |
| Realized stock sale records | 32 |
| Wheel dividend cash rows | 52 |

| Year | Realized option P&L | Realized stock P&L | Net dividends | Total realized P&L |
|---:|---:|---:|---:|---:|
| 2024 | 13,312.90 | -900.00 | 118.15 | 12,531.06 |
| 2025 | 12,067.08 | 2,600.00 | 524.13 | 15,191.21 |
| 2026 YTD | 28,620.76 | 3,900.00 | 22.95 | 32,543.72 |

The current shared-pipeline view is lifecycle-based for option realization,
with open option premium projected separately in mobile monthly payloads. The
cashflow-native comparison remains useful for reconciliation, but the backend
serving path now follows the existing app's realized/open split.

## Persisted Source Smoke

The full local XML backfill was imported into the Firestore-shaped local JSON
store:

```bash
.venv/bin/python scripts/ibkr_import_once.py \
  --xml-dir tmp/ibkr_backfill/query-1503002 \
  --output-dir tmp/ibkr_import_full
```

Result:

| Metric | Value |
|---|---:|
| XML chunks imported | 22 |
| Unique raw rows inserted | 5,083 |
| Raw row updates from overlapping chunks | 2,611 |
| Unique normalized transactions inserted | 2,398 |
| Transaction updates from overlapping chunks | 1,014 |

Then direct XML mode was compared against persisted local JSON mode:

```bash
.venv/bin/python scripts/ibkr_compare_report_sources.py \
  --as-of 2026-05-08 \
  --xml-dir tmp/ibkr_backfill/query-1503002 \
  --json-dir tmp/ibkr_import_full/firestore_sim
```

Result:

| Payload | Match |
|---|---|
| Dashboard | Yes |
| Positions | Yes |
| Open option shorts | Yes |
| Tickers | Yes |
| Monthly performance | Yes |
| Yearly performance | Yes |
| Issues | Yes |

Persisted and direct XML state counts both produced:

| State field | Count |
|---|---:|
| `df_opts` | 582 |
| `open_options` | 20 |
| `stock_txns` | 74 |
| `realized_sales` | 32 |
| `div_df` | 52 |
| `issues` | 97 |

## Cloud Run Job Smoke

Manual Cloud Run Job validation completed on `2026-05-09`. No scheduler was
enabled.

The first explicit range, `2026-05-01` through `2026-05-09`, failed with IBKR
error `1003: Statement is not available`. Retrying with `2026-05-01` through
`2026-05-08` succeeded, confirming that scheduled imports should stop at
yesterday's completed statement.

Successful run:

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

This validates Secret Manager access, GCS raw XML writes, Firestore import
writes, refresh audit writes, and idempotent dedupe in the actual Cloud Run Job
environment.

## Issue Review

Using the normal backend historical price fetcher, IBKR persisted mode returned
`99` mobile issues:

| Category | Count | Interpretation |
|---|---:|---|
| Wheel-scope parse/strategy warnings | 97 | Expected IBKR scoping decisions, not decode/runtime failures. |
| Capital history detail rows | 2 | Early 2022 CROX/ASAN price-history edge coverage. |

The 97 strategy warnings break down as:

| Type | Count | Notes |
|---|---:|---|
| Excluded non-wheel call executions | 73 | Calls excluded because no prior put-assignment inventory was held. |
| Prorated call executions | 19 | Call quantity exceeded wheel-held shares; only covered wheel-held portion included. |
| Ignored assigned-call stock sales | 4 | Assigned-call stock sale ignored because no assignment-derived inventory existed. |
| Capital-history summary in shared issue list | 1 | Shared pipeline summary, not an IBKR parse issue. |

For the useful `2024-2026` validation window:

- Non-wheel call exclusions are limited to `ABR` and `IIPR`.
- The only ignored assigned-call stock sale in the useful window is `ABR` on
  `2025-08-14`.
- All prorated call execution warnings are from `2022-2023`, outside the useful
  dashboard reconciliation period.

This matches the agreed wheel rule: covered calls without a prior assigned put
are not wheel trades and should remain excluded.

## Next Validation Target

The next validation target is not another sheet-shaped adapter. It is to review
the wheel-scoped exclusions and then persist the accepted model:

1. Confirm that covered calls without prior put-assignment inventory should stay
   excluded from wheel performance.
2. Review `ibkr_option_cashflows_excluded.csv` for non-wheel calls such as
   `ABR` and `IIPR`.
3. Keep close-only lifecycle P&L as an audit view, clearly labeled.
4. Persist raw IBKR rows plus normalized wheel performance records in Firestore.
5. Re-run the 2024/2025/2026 comparison from persisted data rather than local
   XML files.

## IBKR-Native Performance Logic

Implemented separately from the current Streamlit pipeline:

```text
realized_strategy_cash_pnl =
    option_cashflow_pnl
  + stock_realized_pnl
  + dividends_net
```

Definitions:

- `option_cashflow_pnl`: IBKR option sell/open credits plus buy/close debits,
  including commissions, by execution date. Short puts are included; short calls
  are included only while prior put-assignment stock inventory is held.
- `stock_realized_pnl`: wheel-scoped stock P&L only. Assigned-put stock buys
  are matched FIFO to assigned-call stock sells from IBKR `OptionEAE` stock-side
  rows. Unrelated IBKR stock trades are excluded.
- `dividends_net`: wheel-scoped IBKR cash rows for `Dividends`,
  `Payment In Lieu Of Dividends`, `Withholding Tax`, and `871(m) Withholding`,
  included only while assignment-derived shares are held.
- Interest and account fees are tracked separately and excluded from the core
  strategy cash P&L.

Comparison command:

```bash
.venv/bin/python scripts/ibkr_compare_performance.py --since 2024-01-01 --as-of 2026-05-08
```

| Year | Source | Option P&L | Stock realized P&L | Net dividends | Realized strategy cash P&L |
|---|---|---:|---:|---:|---:|
| 2024 | Current sheet | 12,670.46 | -3,150.00 | 90.00 | 9,610.46 |
| 2024 | IBKR-native | 12,428.84 | -900.00 | 118.15 | 11,646.99 |
| 2025 | Current sheet | 24,771.73 | 2,600.00 | 616.60 | 27,988.33 |
| 2025 | IBKR-native | 28,672.78 | 2,600.00 | 524.13 | 31,796.91 |
| 2026 YTD | Current sheet | 18,213.80 | 3,900.00 | 87.00 | 22,200.80 |
| 2026 YTD | IBKR-native | 20,301.27 | 3,900.00 | 22.95 | 24,224.22 |

The option P&L differences are now plausible:

| Year | IBKR option cashflow - sheet option P&L | Main reasons |
|---|---:|---|
| 2024 | -241.62 | ABR covered-call cash is now excluded because there was no prior put assignment. Other basic option mapping is very close. |
| 2025 | 3,901.05 | Mostly year-boundary option cash and execution-level roll differences, after excluding non-wheel covered calls such as ABR/IIPR. |
| 2026 YTD | 2,087.47 | Mostly timing/bucket differences for rolls and still-open positions. Do not classify ZM as missing from the sheet: sheet mode has an active `ZM` 99 put position, while IBKR represents the same exposure as a 2026-05-06 roll from 82.5 puts into three 99 puts. Current branch total ZM option P&L is `594.563932`, matching the sheet's `594.56` apart from rounding. |

The stock and dividend rows are now scoped to the wheel strategy. The prior
whole-portfolio stock/dividend view was intentionally removed because it mixed in
IBKR holdings unrelated to options.

IBKR wheel stock realized contributors:

| Year | Main contributors |
|---|---|
| 2024 | `AAPL` +2,250.00, `GOOGL` +1,250.00, `CROX` -3,500.00, `ASAN` -900.00 |
| 2025 | `WING` +2,000.00, `NEM` +1,400.00, `GLW` +1,000.00, `RCAT` +200.00, `CROX` -2,000.00 |
| 2026 YTD | `GOOGL` +3,500.00, `IBKR` +900.00, `RCAT` +250.00, `AAPL` -750.00; other matched assignment exits at roughly flat strike-to-strike P&L |

IBKR wheel net dividend/payment-in-lieu contributors:

| Year | Main contributors |
|---|---|
| 2024 | `AAPL` 84.15, `GOOGL` 34.00 |
| 2025 | `NLR` 269.12, `WING` 73.97, `GOOGL` 70.54, `AAPL` 66.30, `NEM` 21.25, `IBKR` 20.40 |
| 2026 YTD | `AAPL` 22.10, `GOOGL` 4.05, `NVDA` 0.85 |
