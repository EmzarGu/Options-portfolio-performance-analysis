# Streamlit Sheet vs IBKR Reconciliation

- Generated: `2026-05-10` local run
- As-of date used: `2026-05-10`
- Sheet source: `Options 2024`, `Options 2025`, `Options 2026`
- IBKR source: `IBKR Flex` from `tmp/ibkr_import/firestore_sim`
- Shared current-price overlay: `17/17` tickers priced at `14:38:32`
- Branch/head: `8b31838`
- Full backend tests: `280 passed, 11 warnings`

## Executive Summary

- Suspected backend-bug blockers found by reconciliation: **0**
- Material differences still requiring acceptance/reclassification if exact sheet parity is required: **156**
- Sheet actionable issues: **0**; IBKR actionable issues after historical-warning reclassification: **0**
- Sheet audit notes: **0**; IBKR audit notes after historical-warning reclassification: **405**

No production switch or deploy was performed. CSV artifacts are under `tmp/streamlit_ibkr_reconciliation/`.

## P&L-Focused Differences

| Surface | Row | Sheet | IBKR | IBKR - Sheet | Classification | Explanation |
|---|---|---:|---:|---:|---|---|
| Dashboard | YTD realized P&L | $22,200.80 | $25,909.27 | $3,708.47 | needs_user_decision | Material source delta not tied to a known named reconciliation case; review before expecting exact sheet parity. |
| Dashboard | Current option unrealized | $6,523.09 | $3,492.70 | $-3,030.39 | needs_user_decision | Material source delta not tied to a known named reconciliation case; review before expecting exact sheet parity. |
| Dashboard | YTD total P&L | $36,027.89 | $36,705.97 | $678.08 | needs_user_decision | Material source delta not tied to a known named reconciliation case; review before expecting exact sheet parity. |
| Monthly | April 2026 options P&L | $3,716.60 | $5,542.66 | $1,826.06 | sheet_periodization_difference | April 2026 option P&L delta is driven mainly by CCJ/NVDA lifecycle-date inclusion and AAPL prior-roll realization. |
| Monthly | May 2026 realized options P&L | $0.00 | $703.33 | $703.33 | known_contract_semantics_difference | May includes current-month IBKR lifecycle realized P&L plus projection semantics for open expiring premium. |
| Monthly | May 2026 total realized P&L | $0.00 | $703.33 | $703.33 | known_contract_semantics_difference | May includes current-month IBKR lifecycle realized P&L plus projection semantics for open expiring premium. |
| Per ticker | CCJ 2026 options P&L | $504.60 | $1,788.98 | $1,284.38 | sheet_periodization_difference | CCJ April 2026 difference is lifecycle-date/roll periodization; IBKR events are valid covered-call wheel events, not over-inclusion. |
| Per ticker | NVDA 2026 options P&L | $641.88 | $855.99 | $214.11 | sheet_periodization_difference | NVDA April 2026 difference is lifecycle-date/roll periodization; IBKR events are valid covered-call wheel events, not over-inclusion. |
| Per ticker | AAPL 2026 options P&L | $-320.20 | $164.60 | $484.80 | sheet_periodization_difference | AAPL April assignment option P&L is already realized by IBKR in prior roll events; sheet defers it to April assignment. |
| Per ticker | FTNT 2026 options P&L | $85.98 | $599.25 | $513.27 | expected_ibkr_more_accurate | FTNT covered-call roll chain is now attached to assignment-derived shares; IBKR caps assigned holding at the covered strike. |
| Per ticker | CROX 2026 options P&L | $200.97 | $550.42 | $349.45 | needs_user_decision | Material source delta not tied to a known named reconciliation case; review before expecting exact sheet parity. |
| Per ticker | GOOGL 2026 options P&L | $150.40 | $410.20 | $259.80 | needs_user_decision | Material source delta not tied to a known named reconciliation case; review before expecting exact sheet parity. |

## Largest Capital-Base Differences

These dominate raw dollar deltas but are expected because IBKR reconstructs capital from broker data while sheet mode uses the selected spreadsheet rows.

| Surface | Key | Metric | Sheet | IBKR | IBKR - Sheet |
|---|---:|---|---:|---:|---:|
| monthly | 2024-01-31 | peak_capital | $21,000.00 | $70,843.37 | $49,843.37 |
| monthly | 2024-07-31 | peak_capital | $125,162.00 | $76,427.47 | $-48,734.53 |
| monthly | 2024-01-31 | avg_capital | $14,700.00 | $61,341.78 | $46,641.78 |
| yearly | 2024.0 | peak_capital | $125,162.00 | $83,427.72 | $-41,734.28 |
| monthly | 2025-07-31 | peak_capital | $204,564.36 | $166,064.36 | $-38,500.00 |
| monthly | 2025-04-30 | peak_capital | $160,222.53 | $124,222.53 | $-36,000.00 |
| monthly | 2025-07-31 | avg_capital | $178,448.44 | $143,174.24 | $-35,274.19 |
| monthly | 2025-04-30 | avg_capital | $147,554.09 | $112,784.09 | $-34,770.00 |

## Known Case Confirmations

- FTNT: IBKR now has open `Call` strike `95.0` expiring `2026-09-18` with qty `1`.
- FTNT assigned holding: `covered_shares=100`, `covered_strike=95.0`, `unrealized_pnl=$-250.00`.
- CCJ/NVDA April 2026: classified as `sheet_periodization_difference`; prior reconciliation showed valid assigned-put inventory and valid covered-call lifecycle events.
- AAPL April 2026: classified as `sheet_periodization_difference`; IBKR realized the economics in prior roll events rather than on April assignment date.
- ZM/monthly premium semantics: IBKR additive incremental premium `$3,492.70`, roll-adjusted display premium `$4,087.26`, projected month P&L `$4,196.03`.
- SPY remains excluded from IBKR wheel totals: `True`.
- ABR remains excluded from IBKR wheel totals: `True`.

## Issues / Source Health

- Sheet mode has no actionable issues or audit notes in this run.
- IBKR mode originally surfaced 6 actionable historical warnings in this local dataset: two ASAN prorated-call warnings and four early ASAN/CROX unmatched-buy warnings from 2022-05. After review, these exact historical rows are classified as wheel audit/info notes, not current data-health warnings.
- IBKR mode has 405 audit notes after that reclassification; these are expected wheel-classification exclusions and do not count as actionable.

## Surface Coverage

- Dashboard/YTD metrics: `dashboard_comparison.csv`
- Yearly table and unrealized-adjusted yearly table: `yearly_comparison.csv`, `yearly_unrealized_adjusted_comparison.csv`
- Monthly table/charts and projection fields: `monthly_comparison.csv`, `monthly_projection_comparison.csv`, `current_month_projection_comparison.csv`
- Per-ticker realized and realized+unrealized totals: `per_ticker_yearly_comparison.csv`, `per_ticker_totals_comparison.csv`
- Positions assigned holdings and open option shorts: `positions_inventory_comparison.csv`, `open_options_comparison.csv`
- Logs/source health: `issues_summary.csv`, `issues_rows.csv`, `source_row_counts.csv`, `source_health_comparison.csv`

## Blockers Before Switching Streamlit

- No suspected backend accounting blocker was found by this reconciliation.
- No actionable issue-banner blocker remains after reclassifying the 6 known historical local-dataset warnings as audit/info.
- Product/UI work remains: once Streamlit uses IBKR, sheet selector and sheet-specific copy should be hidden or relabeled as `Data Source`, with `IBKR Flex` as the only logical source.
- User acceptance remains needed for non-known per-ticker/source deltas such as CROX, GOOGL, ASAN totals if exact sheet parity is expected; otherwise they are part of moving broker data to source-of-truth.

## Notes

This report compares backend state used by Streamlit, not screenshot pixels. It intentionally uses one shared current-price fetch for both sources, so position/unrealized deltas are source/accounting deltas rather than price timing deltas.
