# Mobile Backend DTO Builder Checklist

Use this as the backend implementation checklist after the Streamlit refactor stabilizes. The goal is to keep pure DTO builders that emit the exact shapes in `docs/mobile-api-contract.md` independently from HTTP routes.

## Recommended Order

1. Keep `portfolio_backend/mobile_payloads.py` focused on pure builder functions only.
2. Keep `PipelineState` as the single calculation source of truth.
3. Add fixture tests that compare builder output to the mobile contract examples.
4. Keep routes as thin wrappers around DTO builders.
5. Keep `portfolio_backend/serializers.py` generic payloads separate from mobile contract payloads.

## Shared Builders

- `build_mobile_request(as_of, include_unrealized, selected_sheets)`: echo normalized request inputs.
- `build_data_freshness(state, selected_sheets, available_sheets, source_metadata)`: shared across read endpoints.
- `json_safe(value)`: reuse or wrap `portfolio_backend.serializers.json_safe`; never emit `NaN`, `NaT`, `pd.NA`, or formatted currency strings.
- `source_sheets`: always populated with selected sheets and row counts from `state.sheet_counts`; mark selected-but-missing sheets as `status: "missing"`.
- Stable IDs:
  - option: `optlot:{ticker}:{option_type}:{strike}:{expiration}:{opened}:{lot_sequence}`
  - inventory: `inventory:{ticker}:{buy_date}:{source}:{lot_sequence}`
  - ticker: `ticker:{ticker}`
  - ticker history: `year:{YYYY}:ticker:{ticker}`
  - month: `month:{YYYY-MM-DD}`
  - year: `year:{YYYY}`

## Endpoint Builder Map

### Dashboard

Builder: `build_mobile_dashboard(state, request, target_return=0.015, target_floor=None)`

Sources:
- `snapshot`: `state.yearly`, `state.yearly_with_unreal`, `state.total_unreal`, `state.option_unreal`, `state.stock_unreal`, `state.unrealized_blocked`, `state.capital_history_affected_years`.
- `monthly_target`: current month row from `state.monthly_cycles`, denominator from `avg_capital`, current return from RoAC, target P&L from `target_return * avg_capital`.
- `monthly_target_band`: shared lower/upper monthly target settings loaded by the route layer and passed through to the DTO.
- `open_option_short_preview`: first 3-5 rows from `build_open_option_short_rows`, sorted by moneyness risk, then projected to the smaller dashboard preview DTO.
- `issue_summary`: derived from typed issue rows plus price/parse counts.

Do not reuse `serialize_snapshot` directly as the response shape; it is close but not contract-compatible.
Do not return the full open-short row shape in dashboard previews; omit fields that only belong to `/open-option-shorts`, such as `notional_at_strike`, `accounting_open_premium`, `strategy_premium_collected`, and `missing_price`.

### Open Option Shorts

Builder: `build_open_option_short_rows(state, sort="moneyness_risk", limit=None)`

Sources:
- Start from `state.open_options` enriched by `build_open_option_shorts_frame(state.open_options, state.stock_prices or {})`.
- Required additions: stable `id`, `moneyness`, `moneyness_band`, `notional_at_strike`, `accounting_open_premium`, `realized_premium_already_booked`, `strategy_premium_collected`, `missing_price`, `risk_label`.
- Moneyness formula:
  - put: `(strike - current_price) / strike`
  - call: `(current_price - strike) / strike`
  - positive means in the money for both.
- Backend owns band assignment; Swift owns colors.

Tests should cover puts, calls, missing price, strike zero/null, same ticker/strike/expiration duplicate lots, and sort stability.

### Positions

Builder: `build_mobile_positions(state, request)`

Sources:
- `inventory`: `state.inv_df` filtered through `build_assigned_holdings_frame`.
- `open_option_shorts`: `build_open_option_short_rows(state)`.

Required cleanup:
- Inventory rows need deterministic lot sequencing independent of dataframe display order.
- `current_price` and `unrealized_pnl` are nullable when price is missing.
- `covered_shares` and `covered_strike` should be computed server-side when possible; `covered_strike` may remain null.

### Tickers

Builder: `build_mobile_tickers(state, request, year=None, include_history=False)`

Sources:
- Realized values: `state.per_ticker_totals` and/or `state.per_ticker`.
- Unrealized values: `state.advanced_unreal`, `state.inv_df`, `state.open_options`, `state.stock_prices`.
- Risk labels: derive from open shorts, inventory concentration, missing prices, and large unrealized losers.
- History rows: `per_ticker_yearly_from_realized(...)` output when `include_history=true`.
- History row IDs are mandatory and must use `year:{YYYY}:ticker:{ticker}`, not the top-level yearly-performance `year:{YYYY}` format.

Avoid leaking Streamlit table columns or display labels. Return ticker summaries designed for list/detail screens.

### Monthly Performance

Builder: `build_mobile_monthly_performance(state, request, target_return=0.015, target_floor=None, range="ytd")`

Sources:
- `months`: `state.monthly_cycles`.
- Current month: row matching `state.as_of` month end.
- Coverage flags: `state.first_incomplete_return_month`, `state.last_complete_return_month`, `state.return_series_truncated`, `state.capital_history_incomplete`.

Rules:
- `target_basis` defaults to `avg_capital`.
- `return_metric` defaults to `return_roac`.
- `target_return` and `target_floor` come from shared settings unless explicitly overridden by request query parameters.
- `target_pnl` and `remaining_pnl` are null if `avg_capital` is unavailable.
- Status values: `beat`, `miss`, `below_target`, `on_track`, `unavailable`.

### Yearly Performance

Builder: `build_mobile_yearly_performance(state, request)`

Sources:
- Use `state.yearly_with_unreal` when `include_unrealized=true`, otherwise `state.yearly`.
- Capital suppression: `state.capital_history_affected_years`.
- Unrealized current-year total should be null if blocked.

Map backend columns explicitly to contract names. Do not rely on dataframe key normalization to guess API names.

### Issues And Data Health

Builder: `build_mobile_issues(state, request)`

Sources:
- Raw issue strings: `state.issues`.
- Current price errors: `state.price_errors`, `state.price_summary`.
- Historical price errors: `state.historical_price_errors`, `state.historical_price_summary`.
- Dividend coverage: `state.dividend_summary`, `state.dividend_coverage_complete`, `state.dividend_errors`, `state.dividend_failed_tickers`, `state.dividend_attempted_tickers`.
- Capital coverage: `state.capital_history_incomplete`, `state.capital_history_coverage_issues`.

Required transformation:
- Convert strings to typed issue DTOs with `id`, `category`, `severity`, `message`, `tickers`, and `action`.
- Keep `message` human-readable, but make `category` and `action` machine-stable.

### Config

Builder: `build_mobile_config(available_sheets, prefs, source_metadata)`

Sources:
- Sheet list: `list_option_sheets(...)`.
- Defaults: saved prefs plus configured `SHEETS`.
- Source metadata: local Excel/Drive download metadata currently rendered in Streamlit data status.

Rules:
- `missing_default_sheets` is a real health signal, not an empty placeholder.
- `as_of_default` should be the backend-normalized default date.

### Refresh

Service boundary: `refresh_mobile_data(request)`

Rules:
- Refresh is the only endpoint allowed to mutate cache/freshness state.
- The first backend slice refreshes the pipeline and live price overlay together.
- Separate `prices`, `data`, and `all` scopes are deferred until there is a concrete
  client need.
- Partial success returns normal refresh response with `refresh.status: "partial"`
  and freshness details.

## Test Checklist

- Decode/compare every endpoint against the JSON fixtures used by Swift.
- `source_sheets` includes loaded and missing selected sheets.
- `force_price_refresh` does not exist on read endpoints.
- Option moneyness and banding match the contract.
- Stable IDs survive sorting/filtering and duplicate lots.
- Missing prices serialize as null plus `missing_price: true`.
- Monthly target status and remaining P&L match 1.5% target rules.
- Error envelope covers invalid dates, unknown sheets, no selected sheets, backend unavailable, and price provider unavailable.

## Route Layer Notes

After DTO builders pass tests, add HTTP routes as thin wrappers:

- parse and validate query/body
- build or fetch `PipelineState`
- call one DTO builder
- return JSON

The route layer should not contain portfolio math, dataframe massaging, moneyness logic, or Streamlit compatibility behavior.
