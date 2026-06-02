# Mobile API Contract

Status: implemented backend contract for the current mobile API iteration. This document defines mobile-friendly JSON endpoints for the iOS Options Monitor app. It does not require changes to portfolio calculations.

## Design Goals

- Make `Open option shorts sorted by moneyness` and `monthly profitability vs 1.5% target` first-class API resources.
- Keep Streamlit UI concepts out of the API. The API should expose portfolio facts, not tabs, dataframe display labels, Streamlit cache keys, or styled table details.
- Use raw machine values. Currency values are numbers in USD. Percentages are decimal fractions, e.g. `0.015` means `1.5%`.
- Return dates as ISO `YYYY-MM-DD`. Return timestamps as ISO 8601 with timezone when available.
- Preserve enough freshness metadata for mobile to show whether prices/data are current.

## Common Request Parameters

All read endpoints accept:

| Name | Type | Required | Rule |
| --- | --- | --- | --- |
| `as_of` | string date | no | Defaults to server today. Backend must cap future dates to today, matching current pipeline behavior. |
| `include_unrealized` | boolean | no | Defaults to `false`. Mirrors the current unrealized-adjusted toggle. |
| `selected_sheets` | repeated string | no | Repeat the query parameter, e.g. `selected_sheets=Options%202024&selected_sheets=Options%202025`. Defaults to saved/server defaults. |

Read endpoints must not trigger an explicit price/data refresh. The server may reuse an in-memory request context for matching read calls so multiple mobile screens do not rebuild the same portfolio repeatedly. Use `POST /v1/mobile/refresh` for an explicit price refresh, data rebuild, or both.

Example:

```http
GET /v1/mobile/dashboard?as_of=2026-05-03&include_unrealized=true&selected_sheets=Options%202024&selected_sheets=Options%202025&selected_sheets=Options%202026
```

## Common Response Envelope

All successful responses should include:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  }
}
```

Nullability rules:

- `pipeline_built_at`, `prices_updated_at`, `source_downloaded_at`, `source_modified_at` may be `null` when not known.
- Missing numeric metrics should be `null`, not `NaN`, `"n/a"`, or omitted.
- Empty collections should be `[]`.
- Suppressed metrics should include a reason in `warnings` or `issues`, and the numeric field should be `null`.
- `data_freshness.source_sheets` must always be populated for the requested/selected sheets. It must not be `[]` unless no sheets were requested and no defaults exist.

## Stable Row IDs

Every row consumed by SwiftUI lists must have a deterministic, collision-resistant `id`.

Rules:

- Option-short row ID format: `optlot:{ticker}:{option_type}:{strike}:{expiration}:{opened}:{lot_sequence}`.
- Inventory row ID format: `inventory:{ticker}:{buy_date_or_unknown}:{source}:{lot_sequence}`.
  `inventory` is grouped by ticker for the main app surfaces; when multiple
  assignment lots exist for the same ticker, `buy_date` is the latest
  assignment date, `source="stock_group"`, and
  `lot_count`/`first_buy_date`/`latest_buy_date` describe the underlying lots.
- Ticker row ID format: `ticker:{ticker}`.
- Monthly row ID format: `month:{YYYY-MM-DD}`.
- Yearly row ID format: `year:{YYYY}`.
- `lot_sequence` is a zero-based integer assigned after sorting source lots by the same deterministic keys used by the backend. It is mandatory even when the first version appears unique.
- IDs must not depend on current list sort order, display index, client filtering, or localized display text.

## Endpoint 1: Dashboard

`GET /v1/mobile/dashboard`

Purpose: first screen payload. It should be fast enough for app launch and contain the highest-priority monitoring data.

Query parameters: common parameters.

Response:

Premium fields in `items` use explicit accounting names:

- `accounting_open_premium`: open option premium not already recognized in realized P&L. This is the only open-short premium value that may feed projected P&L.
- `realized_premium_already_booked`: strategy premium already recognized in realized P&L, usually from prior roll accounting.
- `strategy_premium_collected`: total strategy premium context for the open lot: `accounting_open_premium + realized_premium_already_booked`.

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "snapshot": {
    "currency": "USD",
    "year": 2026,
    "ytd_total_pnl": 81240.0,
    "ytd_realized_pnl": 56380.0,
    "current_unrealized_pnl": 24860.0,
    "current_option_unrealized_pnl": 9200.0,
    "current_option_premium_unrealized_pnl": 11000.0,
    "current_stock_unrealized_pnl": 15660.0,
    "current_put_assignment_unrealized_pnl": -1800.0,
    "itm_put_cash_required": 18000.0,
    "itm_put_market_value": 16200.0,
    "itm_put_contracts": 2,
    "itm_put_shares": 200,
    "available_cash": null,
    "ytd_annualized_twr": 0.184,
    "unrealized_adjusted": true,
    "unrealized_blocked": false
  },
  "monthly_target": {
    "month": "2026-05-31",
    "target_basis": "avg_capital",
    "target_return": 0.015,
    "current_return": 0.006,
    "current_return_metric": "return_roac",
    "current_pnl": 3210.0,
    "target_pnl": 7890.0,
    "remaining_pnl": 4680.0,
    "status": "below_target",
    "realized_month_pnl": 3210.0,
    "realized_options_pnl": 3210.0,
    "realized_stock_pnl": 0.0,
    "open_expiring_option_premium": 5200.0,
    "open_expiring_incremental_premium": 5200.0,
    "includes_open_premium": true,
    "projection_basis": "realized_plus_open_premium",
    "projected_month_pnl": 8410.0,
    "projected_return_roac": 0.016,
    "projected_return_ropc": 0.0131,
    "projected_remaining_pnl": 0.0,
    "monthly_target_status": "beat",
    "days_remaining": 18
  },
  "open_option_short_preview": [
    {
      "id": "optlot:META:Call:510.0:2026-05-24:2026-04-19:0",
      "ticker": "META",
      "option_type": "Call",
      "strike": 510.0,
      "current_price": 516.2,
      "moneyness": 0.012,
      "moneyness_band": "in_the_money",
      "quantity": 1,
      "expiration": "2026-05-24",
      "days_to_expiration": 21,
      "opened": "2026-04-19",
      "open_price": 6.05,
      "covered_status": "covered",
      "risk_label": "In the money"
    },
    {
      "id": "optlot:TSLA:Put:180.0:2026-05-17:2026-04-18:0",
      "ticker": "TSLA",
      "option_type": "Put",
      "strike": 180.0,
      "current_price": 183.42,
      "moneyness": -0.019,
      "moneyness_band": "near",
      "quantity": 1,
      "expiration": "2026-05-17",
      "days_to_expiration": 14,
      "opened": "2026-04-18",
      "open_price": 5.8,
      "covered_status": "cash_secured",
      "risk_label": "Near strike"
    }
  ],
  "issue_summary": {
    "severity": "warning",
    "total_count": 4,
    "price_issue_count": 1,
    "parse_issue_count": 3,
    "top_messages": [
      "Price coverage incomplete: Stocks priced: 16/17",
      "Mixed-leg option rows need review"
    ]
  }
}
```

Fields:

- `snapshot.ytd_total_pnl`: null if `include_unrealized=true` and unrealized snapshot is blocked.
- `snapshot.current_unrealized_pnl`, `current_option_unrealized_pnl`, `current_stock_unrealized_pnl`, `current_put_assignment_unrealized_pnl`: null if blocked by missing required prices.
- `snapshot.current_option_unrealized_pnl`: includes open short option premium and the assignment gap for open ITM puts. The assignment gap is `(current_price - strike) * contracts * 100`, so it is negative when assignment would create an immediate stock loss.
- `snapshot.current_option_premium_unrealized_pnl`: open short option premium before subtracting the ITM put assignment gap. This is a display/reconciliation subcomponent of `current_option_unrealized_pnl`, not an additional amount to add to total unrealized.
- `snapshot.current_stock_unrealized_pnl`: actual held-stock unrealized P&L only. Open ITM put assignment exposure is excluded because the shares are not owned yet.
- `snapshot.itm_put_cash_required`: cash required to take assignment of currently ITM open puts at strike. `itm_put_market_value` is the current market value of those shares, and the difference is represented in `current_put_assignment_unrealized_pnl`.
- `snapshot.available_cash`: reserved for an IBKR available-cash import. It is `null` until the import stores account cash balances.
- `monthly_target.target_basis`: enum. Initial value is `avg_capital`, matching RoAC. If the product later supports RoPC target tracking, add `peak_capital` explicitly rather than changing semantics.
- `monthly_target.current_return_metric`: enum. Initial value is `return_roac`.
- `monthly_target.current_*`, `realized_*`, and `status`: realized-only values.
- `monthly_target.open_expiring_option_premium`: alias for `open_expiring_incremental_premium`. It is the additive open premium that is safe to add to realized P&L without double-counting same-expiration roll credits already recognized in realized roll economics.
- `monthly_target.projected_month_pnl`: canonical active-cycle projection and must match `monthly_target.cycle_projection.projected_cycle_pnl`.
- `monthly_target.cycle_projection`: the canonical active-cycle object used by web and mobile. It includes the active cycle label/month, expiries, open ticker/contract counts, realized cycle P&L, additive open premium, ITM put assignment P&L for puts expiring in the cycle, ITM covered-call stock P&L for calls expiring in the cycle, projected cycle P&L, target/remaining values, put exposure, ITM put signal, and covered-call upside signal. Broad held-stock unrealized P&L is reported separately and is not added to cycle target P&L unless the open ITM call would realize that stock sale in the cycle.
- `monthly_target.monthly_target_status`: target status based on `projected_return_roac`, not realized return.
- `monthly_target.*`: null for return/P&L fields if the required monthly capital denominator or source value is unavailable.
- `open_option_short_preview`: sorted by moneyness risk, limited to 3-5 rows.

## Endpoint 2: Open Option Shorts

`GET /v1/mobile/open-option-shorts`

Purpose: primary monitoring table replacement for the Streamlit `Open option shorts` table.

Moneyness formula:

- Put: `(strike - current_price) / strike`
- Call: `(current_price - strike) / strike`
- Positive means in-the-money/riskier for both puts and calls.
- Negative means out-of-the-money for both puts and calls.
- `moneyness_band` must be assigned by the backend using this signed value.

Query parameters:

| Name | Type | Required | Rule |
| --- | --- | --- | --- |
| common parameters | | | |
| `sort` | string | no | Default `moneyness_risk`. Other allowed values: `expiration`, `ticker`, `moneyness_pct`. |
| `limit` | integer | no | Optional. Omit for all rows. |

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "moneyness_legend": [
    { "band": "in_the_money", "label": "ITM", "min_exclusive": 0.0, "max_inclusive": null, "severity": "critical" },
    { "band": "at_strike", "label": "-1% to 0%", "min_inclusive": -0.01, "max_inclusive": 0.0, "severity": "high" },
    { "band": "near", "label": "-5% to -1%", "min_inclusive": -0.05, "max_exclusive": -0.01, "severity": "medium" },
    { "band": "ok", "label": "-10% to -5%", "min_inclusive": -0.10, "max_exclusive": -0.05, "severity": "low" },
    { "band": "clear", "label": "< -10%", "min_inclusive": null, "max_exclusive": -0.10, "severity": "info" }
  ],
  "items": [
    {
      "id": "optlot:META:Call:510.0:2026-05-24:2026-04-19:0",
      "ticker": "META",
      "option_type": "Call",
      "strike": 510.0,
      "current_price": 516.2,
      "moneyness": 0.012,
      "moneyness_band": "in_the_money",
      "quantity": 1,
      "expiration": "2026-05-24",
      "days_to_expiration": 21,
      "opened": "2026-04-19",
      "open_price": 6.05,
      "strategy_open_price": 6.05,
      "notional_at_strike": 51000.0,
      "accounting_open_premium": 605.0,
      "realized_premium_already_booked": 0.0,
      "strategy_premium_collected": 605.0,
      "covered_status": "covered",
      "risk_label": "In the money",
      "missing_price": false
    },
    {
      "id": "optlot:AMD:Put:130.0:2026-05-10:2026-04-12:0",
      "ticker": "AMD",
      "option_type": "Put",
      "strike": 130.0,
      "current_price": 146.18,
      "moneyness": -0.124,
      "moneyness_band": "clear",
      "quantity": 2,
      "expiration": "2026-05-10",
      "days_to_expiration": 7,
      "opened": "2026-04-12",
      "open_price": 1.72,
      "strategy_open_price": 1.72,
      "notional_at_strike": 26000.0,
      "accounting_open_premium": 344.0,
      "realized_premium_already_booked": 0.0,
      "strategy_premium_collected": 344.0,
      "covered_status": "cash_secured",
      "risk_label": "Expires this week",
      "missing_price": false
    }
  ],
  "future_months": [
    {
      "id": "month:2026-06-30",
      "month": "2026-06-30",
      "open_option_count": 3,
      "open_expiring_option_premium": 2600.0,
      "open_expiring_incremental_premium": 2600.0,
      "projected_month_pnl": 2600.0,
      "projected_return_roac": null,
      "projected_return_ropc": null,
      "target_pnl": null,
      "projected_remaining_pnl": null,
      "includes_open_premium": true,
      "projection_basis": "realized_plus_open_premium"
    }
  ]
}
```

Nullability:

- `id` is mandatory and must follow the stable row ID rules above.
- `current_price`, `moneyness`, and `moneyness_band` are `null` when the ticker price is missing. `missing_price` must be `true`.
- `covered_status` may be `null` until backend explicitly computes covered/uncovered status.
- `days_to_expiration` is `null` when expiration is missing/unparseable.

Backend source today:

- `state.open_options`
- `state.stock_prices`
- `build_open_options_positions_frame`
- Streamlit moneyness color logic in `_highlight_short_option_price`

Recommended backend shape change:

- Move moneyness calculation and band assignment out of Streamlit into a reusable backend function returning this row shape.

## Endpoint 3: Positions

`GET /v1/mobile/positions`

Purpose: current assigned stock inventory plus open option shorts in one mobile-friendly response.

Query parameters:

| Name | Type | Required | Rule |
| --- | --- | --- | --- |
| common parameters | | | |
| `include_open_options` | boolean | no | Default `true`. |
| `include_inventory` | boolean | no | Default `true`. |

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "inventory": [
    {
      "id": "inventory:TSLA:2025-12-20:stock_group:0",
      "ticker": "TSLA",
      "buy_date": "2025-12-20",
      "first_buy_date": "2025-11-15",
      "latest_buy_date": "2025-12-20",
      "lot_count": 2,
      "shares": 200,
      "cost_per_share": 221.8,
      "current_price": 183.42,
      "covered_shares": 0,
      "covered_strike": null,
      "covered_strike_mixed": false,
      "unrealized_pnl": -7676.0,
      "source": "stock_group",
      "missing_price": false
    },
    {
      "id": "inventory:AAPL:2025-09-03:stock_lot:0",
      "ticker": "AAPL",
      "buy_date": "2025-09-03",
      "first_buy_date": "2025-09-03",
      "latest_buy_date": "2025-09-03",
      "lot_count": 1,
      "shares": 100,
      "cost_per_share": 176.4,
      "current_price": 192.31,
      "covered_shares": 100,
      "covered_strike": 205.0,
      "covered_strike_mixed": false,
      "unrealized_pnl": 1591.0,
      "source": "stock_lot",
      "missing_price": false
    }
  ],
  "open_option_shorts": [
    {
      "id": "optlot:TSLA:Put:180.0:2026-05-17:2026-04-18:0",
      "ticker": "TSLA",
      "option_type": "Put",
      "strike": 180.0,
      "current_price": 183.42,
      "moneyness": -0.019,
      "moneyness_band": "near",
      "quantity": 1,
      "expiration": "2026-05-17",
      "days_to_expiration": 14,
      "opened": "2026-04-18",
      "open_price": 5.8,
      "notional_at_strike": 18000.0,
      "accounting_open_premium": 580.0,
      "realized_premium_already_booked": 0.0,
      "strategy_premium_collected": 580.0,
      "covered_status": "cash_secured",
      "risk_label": "Near strike",
      "missing_price": false
    }
  ]
}
```

Nullability:

- Inventory and option `id` fields are mandatory and must follow the stable row ID rules above.
- Inventory `current_price` and `unrealized_pnl` are `null` when price is missing.
- `covered_strike` is `null` when no covered call maps to the stock lot.
- `source` should be a backend enum. Current values from `inv_df` include `stock_lot` and `put_gap`. `put_gap` is a synthetic ITM put assignment-risk row and must not be treated as owned stock.

Backend source today:

- `state.inv_df`
- `state.open_options`
- `state.stock_prices`

Recommended backend shape change:

- Replace `inv_df` dataframe rows with typed `InventoryPosition` DTOs before API serialization.
- Add stable IDs because dataframes do not provide row identity.

## Endpoint 4: Per-Ticker P&L

`GET /v1/mobile/tickers`

Purpose: mobile ticker list and ticker detail source. This replaces the Streamlit `Per ticker` tab and parts of `Positions`.

Query parameters:

| Name | Type | Required | Rule |
| --- | --- | --- | --- |
| common parameters | | | |
| `year` | integer | no | If supplied, include that year’s realized breakdown. |
| `include_history` | boolean | no | Default `false`. If true, include yearly rows per ticker. |

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "items": [
    {
      "id": "ticker:NVDA",
      "ticker": "NVDA",
      "current_price": 924.74,
      "realized_options_pnl": 12480.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "combined_realized_pnl": 12480.0,
      "unrealized_pnl": 18354.0,
      "current_option_premium_unrealized_pnl": 1120.0,
      "current_put_assignment_unrealized_pnl": 0.0,
      "current_option_unrealized_pnl": 1120.0,
      "current_stock_unrealized_pnl": 17234.0,
      "total_pnl": 30834.0,
      "open_option_count": 1,
      "inventory_share_count": 100,
      "risk_labels": ["High notional exposure"],
      "history": [
        {
          "id": "year:2026:ticker:NVDA",
          "year": 2026,
          "realized_options_pnl": 12480.0,
          "realized_stock_pnl": 0.0,
          "combined_realized_pnl": 12480.0
        }
      ]
    },
    {
      "id": "ticker:TSLA",
      "ticker": "TSLA",
      "current_price": 183.42,
      "realized_options_pnl": 9140.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "combined_realized_pnl": 9140.0,
      "unrealized_pnl": -7676.0,
      "current_option_premium_unrealized_pnl": 900.0,
      "current_put_assignment_unrealized_pnl": 0.0,
      "current_option_unrealized_pnl": 900.0,
      "current_stock_unrealized_pnl": -8576.0,
      "total_pnl": 1464.0,
      "open_option_count": 1,
      "inventory_share_count": 200,
      "risk_labels": ["Near strike", "Largest unrealized loser"],
      "history": []
    }
  ]
}
```

Nullability:

- Ticker `id` is mandatory and must follow the stable row ID rules above.
- History row `id` is mandatory when `history` is populated. Use `year:{YYYY}:ticker:{ticker}`.
- `unrealized_pnl` and `total_pnl` are `null` when unrealized snapshot is blocked.
- `current_option_premium_unrealized_pnl`, `current_put_assignment_unrealized_pnl`, `current_option_unrealized_pnl`, and `current_stock_unrealized_pnl` are `null` when unrealized snapshot is blocked.
- `current_option_premium_unrealized_pnl` is open short option premium by ticker. It is not realized P&L.
- `current_put_assignment_unrealized_pnl` is the open ITM put assignment gap by ticker. It is negative when assignment would immediately create a stock loss.
- `current_option_unrealized_pnl` is `current_option_premium_unrealized_pnl + current_put_assignment_unrealized_pnl`.
- `current_stock_unrealized_pnl` is actual held-stock unrealized P&L by ticker. It excludes open put assignment exposure because those shares are not owned yet.
- For real pipeline data, `unrealized_pnl` is the ticker total unrealized snapshot: `current_option_unrealized_pnl + current_stock_unrealized_pnl`.
- `current_price` is `null` when missing.
- `dividends` is cumulative realized dividend cash for the ticker in the same optional `year` scope as the realized P&L fields. It is always present and is `0.0` when none.
- `history` is `[]` unless `include_history=true`.

Backend source today:

- `state.per_ticker`
- `state.per_ticker_totals`
- `state.advanced_unreal`
- `state.stock_prices`
- `state.open_options`
- `state.inv_df`

Recommended backend shape change:

- Build one backend `TickerSummary` DTO that joins realized, unrealized, inventory, open options, and risk flags.

## Endpoint 5: Monthly Performance

`GET /v1/mobile/performance/monthly`

Purpose: target tracking for monthly profitability. The iOS app target is 1.5% or above.

Query parameters:

| Name | Type | Required | Rule |
| --- | --- | --- | --- |
| common parameters | | | |
| `target_return` | number | no | Default `0.015`. |
| `range` | string | no | Default `ytd`. Allowed: `3m`, `6m`, `ytd`, `1y`, `since_inception`. |

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "target_return": 0.015,
  "target_basis": "avg_capital",
  "return_metric": "return_roac",
  "current_month": {
    "id": "month:2026-05-31",
    "month": "2026-05-31",
    "return_roac": 0.006,
    "return_ropc": 0.005,
    "total_realized_pnl": 3210.0,
    "realized_month_pnl": 3210.0,
    "realized_options_pnl": 3210.0,
    "realized_stock_pnl": 0.0,
    "open_expiring_option_premium": 5200.0,
    "open_expiring_incremental_premium": 5200.0,
    "projected_month_pnl": 8410.0,
    "projected_return_roac": 0.016,
    "projected_return_ropc": 0.0131,
    "target_pnl": 7890.0,
    "remaining_pnl": 4680.0,
    "projected_remaining_pnl": 0.0,
    "avg_capital": 526000.0,
    "peak_capital": 642000.0,
    "status": "below_target",
    "monthly_target_status": "beat",
    "days_remaining": 18
  },
  "months": [
    {
      "id": "month:2026-01-31",
      "month": "2026-01-31",
      "realized_options_pnl": 11840.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "total_realized_pnl": 11840.0,
      "avg_capital": 493333.0,
      "peak_capital": 615000.0,
      "return_roac": 0.024,
      "return_ropc": 0.019,
      "target_return": 0.015,
      "status": "beat",
      "realized_month_pnl": 11840.0,
      "open_expiring_option_premium": 0.0,
      "open_expiring_incremental_premium": 0.0,
      "includes_open_premium": false,
      "projection_basis": "realized_only",
      "projected_month_pnl": 11840.0,
      "projected_return_roac": 0.024,
      "projected_return_ropc": 0.019,
      "target_pnl": 7400.0,
      "projected_remaining_pnl": 0.0,
      "monthly_target_status": "beat"
    },
    {
      "id": "month:2026-03-31",
      "month": "2026-03-31",
      "realized_options_pnl": -3920.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "total_realized_pnl": -3920.0,
      "avg_capital": 490000.0,
      "peak_capital": 610000.0,
      "return_roac": -0.008,
      "return_ropc": -0.006,
      "target_return": 0.015,
      "status": "miss",
      "realized_month_pnl": -3920.0,
      "open_expiring_option_premium": 2500.0,
      "open_expiring_incremental_premium": 2500.0,
      "includes_open_premium": true,
      "projection_basis": "realized_plus_open_premium",
      "projected_month_pnl": -1420.0,
      "projected_return_roac": -0.0029,
      "projected_return_ropc": -0.0023,
      "target_pnl": 7350.0,
      "projected_remaining_pnl": 8770.0,
      "monthly_target_status": "miss"
    }
  ]
}
```

Nullability:

- Month row `id` is mandatory and must follow the stable row ID rules above.
- `return_roac` and `return_ropc` are `null` if capital coverage is incomplete for the month.
- `total_realized_pnl`, `realized_month_pnl`, `return_roac`, `return_ropc`, `remaining_pnl`, and `status` are realized-only.
- `open_expiring_incremental_premium` is assigned by option expiration month for still-open short options and is safe to add to realized P&L without double-counting. For same-expiration rolls, replacement premium can be netted into the realized roll event, so this incremental field may be zero even while a rolled replacement remains open.
- `open_expiring_option_premium` is an alias of `open_expiring_incremental_premium`.
- `includes_open_premium` is `true` when projected values include non-zero open option premium for that expiration month.
- `projection_basis` allowed values: `realized_only`, `realized_plus_open_premium`.
- `projected_month_pnl` for the active/current month is the canonical active-cycle projected P&L and matches `active_cycle.projected_cycle_pnl`. Closed historical months remain realized-only. Future rows use their own `cycle_projection.projected_cycle_pnl` when open option exposure exists.
- `active_cycle` contains the canonical active-cycle projection for the current managed option cycle.
- `cycle_projection` on `current_month`, active month rows, and `future_months` contains the same projection shape for that expiration month. Current and future open months both use cycle-scoped option lots, linked stock inventory, and prices through the shared accounting projection. Projection P&L is `realized_cycle_pnl + open_premium_collected + itm_put_unrealized_loss + itm_call_stock_pnl`; OTM call stock unrealized P&L stays outside this sum.
- `projected_return_roac`, `projected_remaining_pnl`, and `monthly_target_status` are derived from the same projected value shown in `projected_month_pnl`.
- `target_pnl`, `remaining_pnl`, and `projected_remaining_pnl` are `null` if `avg_capital` is unavailable.
- `future_months` contains future expiration months for currently open short options. Future rows include `cycle_projection` so web and mobile can display the same open-premium projection, target P&L, remaining P&L, and projected RoAC when the latest monthly capital denominator is available.
- `future_months.open_option_count` counts current open short option rows/lots expiring in that month.
- `status` allowed values: `beat`, `miss`, `below_target`, `on_track`, `unavailable`.
- `monthly_target_status` uses the same allowed values as `status`, but is based on projected RoAC.
- `target_basis` allowed values: `avg_capital`, `peak_capital`. Initial mobile target uses `avg_capital`.
- `return_metric` allowed values: `return_roac`, `return_ropc`. Initial mobile target uses `return_roac`.

Backend source today:

- `state.monthly_cycles`
- `state.open_options` for expiration-month incremental open short premium projection.
- `state.lots` for roll-adjusted open short premium display where source accounting provides roll metadata.
- `state.monthly_returns_covered`
- `state.first_incomplete_return_month`
- `state.last_complete_return_month`
- `state.return_series_truncated`

Implementation notes:

- Realized monthly values remain separate from projected values.
- Projected target status must not be labelled as realized status.

## Endpoint 6: Yearly Performance

`GET /v1/mobile/performance/yearly`

Purpose: compact annual overview for secondary mobile analytics.

Query parameters: common parameters.

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "years": [
    {
      "id": "year:2024",
      "year": 2024,
      "realized_options_pnl": 47200.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "total_realized_pnl": 47200.0,
      "total_pnl_including_unrealized": null,
      "avg_capital": 301000.0,
      "peak_capital": 415000.0,
      "roac_year": 0.157,
      "ropc_year": 0.114,
      "annualized_roac": 0.157,
      "annualized_ropc": 0.114,
      "annualized_twr": 0.153,
      "annualized_twr_active": 0.176,
      "annualized_twr_unrealized_adjusted": null,
      "metrics_available": true,
      "suppression_reason": null
    },
    {
      "id": "year:2026",
      "year": 2026,
      "realized_options_pnl": 56380.0,
      "realized_stock_pnl": 0.0,
      "dividends": 0.0,
      "total_realized_pnl": 56380.0,
      "total_pnl_including_unrealized": 81240.0,
      "avg_capital": 526000.0,
      "peak_capital": 642000.0,
      "roac_year": 0.107,
      "ropc_year": 0.088,
      "annualized_roac": 0.277,
      "annualized_ropc": 0.224,
      "annualized_twr": 0.184,
      "annualized_twr_active": 0.203,
      "annualized_twr_unrealized_adjusted": 0.241,
      "metrics_available": true,
      "suppression_reason": null
    }
  ]
}
```

Nullability:

- Year row `id` is mandatory and must follow the stable row ID rules above.
- `total_pnl_including_unrealized` is `null` unless `include_unrealized=true` and the current year is being adjusted.
- Annualized/return metrics are `null` if suppressed by capital history gaps.
- `suppression_reason` is `null` when metrics are available.

Backend source today:

- `state.yearly`
- `state.yearly_with_unreal`
- `state.capital_history_affected_years`

## Endpoint 7: Issues And Data Health

`GET /v1/mobile/issues`

Purpose: mobile replacement for `Logs / data issues`, suitable for a badge/health screen.

Query parameters: common parameters.

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:54:27+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 16,
      "missing_tickers": ["XYZ"]
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "summary": {
    "severity": "warning",
    "total_count": 4,
    "info_count": 0,
    "unrealized_blocked": false,
    "capital_history_incomplete": false,
    "dividend_coverage_complete": true
  },
  "issues": [
    {
      "id": "price-coverage-stocks",
      "category": "price",
      "severity": "warning",
      "message": "Price coverage incomplete: Stocks priced: 16/17",
      "tickers": ["XYZ"],
      "action": "refresh_prices"
    },
    {
      "id": "mixed-leg-parse-1",
      "category": "parse",
      "severity": "warning",
      "message": "Mixed-leg option row for TSLA on 2026-04-12 has ambiguous short leg.",
      "tickers": ["TSLA"],
      "action": "fix_workbook_row"
    },
    {
      "id": "wheel-warning-1",
      "category": "wheel_warning",
      "severity": "warning",
      "message": "Prorated ABC call execution on 2026-01-20 to 100 wheel-held shares out of 200 required shares.",
      "tickers": ["ABC"],
      "action": "review_source_data"
    }
  ],
  "audit_summary": {
    "total_count": 241,
    "by_category": { "wheel_audit": 241 },
    "by_severity": { "info": 241 }
  },
  "audit_notes": [
    {
      "id": "wheel-audit-1",
      "category": "wheel_audit",
      "severity": "info",
      "message": "Excluded ABC call execution on 2026-01-20 because no prior put-assignment stock inventory was held.",
      "tickers": ["ABC"],
      "action": null
    }
  ],
  "coverage": {
    "current_prices": {
      "requested": 17,
      "fetched": 16,
      "missing_tickers": ["XYZ"],
      "errors": ["XYZ: no price returned"]
    },
    "historical_prices": {
      "requested": 12,
      "fetched": 12,
      "errors": []
    },
    "dividends": {
      "attempted_tickers": 8,
      "failed_tickers": 0,
      "errors": []
    }
  }
}
```

Nullability:

- `tickers` is `[]` if the issue is not ticker-specific.
- `action` may be `null` when no obvious user action exists.

Backend source today:

- `state.issues`
- `state.price_errors`
- `state.historical_price_errors`
- `state.price_summary`
- `state.historical_price_summary`
- `state.dividend_summary`
- `state.capital_history_coverage_issues`
- `state.sheet_counts`

Recommended backend shape change:

- Convert plain issue strings into typed issue DTOs at the backend boundary. Keep original message for display, but add category/severity/action/tickers.
- `issues` contains only actionable warning/error/problem rows. Expected IBKR wheel exclusions use `category: "wheel_audit"` and `severity: "info"` and are returned under `audit_notes` with compact counts in `audit_summary`. They are not counted in `summary.total_count` and do not make the source unhealthy.
- IBKR warnings that need review use categories such as `wheel_warning` for partially included/prorated wheel lots and `missing_basis` for closes without a known open lot.

## Endpoint 8: Configuration

`GET /v1/mobile/config`

Purpose: load available sheets and default settings for Settings screen.

Query parameters: none.

Response:

```json
{
  "available_sheets": ["Options 2022", "Options 2023", "Options 2024", "Options 2025"],
  "default_selected_sheets": ["Options 2024", "Options 2025", "Options 2026"],
  "missing_default_sheets": ["Options 2026"],
  "include_unrealized_default": true,
  "as_of_default": "2026-05-03",
  "source": {
    "kind": "local_excel",
    "name": "latest_download.xlsx",
    "downloaded_at": "2026-05-03T20:53:59+02:00",
    "modified_at": null
  },
  "capabilities": {
    "supports_price_refresh": true,
    "supports_data_rebuild": true,
    "supports_selected_sheets": true,
    "supports_as_of": true
  }
}
```

Nullability:

- Source timestamps may be `null`.
- `missing_default_sheets` is `[]` when all defaults are available.

Backend source today:

- `list_option_sheets`
- `load_prefs`
- `SHEETS`
- `_download_excel` / `_render_data_status`

## Endpoint 9: Refresh

`POST /v1/mobile/refresh`

Purpose: explicit mobile refresh action. This should be separate from read endpoints because it mutates server-side cache/freshness state.

Request:

Use the same common request parameters as the read endpoints:

Fields:

- `as_of`: optional date query parameter.
- `include_unrealized`: optional boolean query parameter.
- `selected_sheets`: optional repeated query parameter.
- `cache_bust`: optional integer query parameter. If omitted, the server generates one.

In IBKR mode, refresh first checks whether the latest successful import marker
changed. If not, the server restores the persisted base pipeline from Firestore
`pipeline_snapshots` and refreshes current prices only. The refreshed state is
then written back to Firestore as the latest refreshed context for the matching
request, so follow-up read endpoints are not dependent on process-local memory
or the Cloud Run instance that handled the refresh. If the import marker changed
or the snapshot is missing/corrupt, the server rebuilds the full accounting
pipeline, stores a new base snapshot, stores the latest refreshed context, and
returns the full endpoint reload list.

Response:

```json
{
  "request": {
    "as_of": "2026-05-03",
    "include_unrealized": true,
    "selected_sheets": ["Options 2024", "Options 2025", "Options 2026"]
  },
  "data_freshness": {
    "pipeline_built_at": "2026-05-03T20:54:12+02:00",
    "prices_updated_at": "2026-05-03T20:59:02+02:00",
    "source_downloaded_at": "2026-05-03T20:53:59+02:00",
    "source_modified_at": null,
    "price_coverage": {
      "stocks_requested": 17,
      "stocks_fetched": 17,
      "missing_tickers": []
    },
    "source_sheets": [
      { "name": "Options 2024", "status": "loaded", "rows": 184 },
      { "name": "Options 2025", "status": "loaded", "rows": 219 },
      { "name": "Options 2026", "status": "missing", "rows": 0 }
    ]
  },
  "refresh": {
    "status": "partial",
    "scope": "prices_only",
    "pipeline_refreshed": false,
    "prices_refreshed": true,
    "cache_bust": 1777924201,
    "missing_price_count": 0,
    "missing_sheet_count": 1,
    "reload_endpoints": [
      "/v1/mobile/dashboard",
      "/v1/mobile/positions",
      "/v1/mobile/open-option-shorts",
      "/v1/mobile/tickers",
      "/v1/mobile/performance/monthly",
      "/v1/mobile/performance/yearly",
      "/v1/mobile/issues"
    ]
  }
}
```

Nullability:

- `refresh.status` is `refreshed` when all selected sheets and prices are available.
- `refresh.status` is `partial` when selected sheets or prices are missing; inspect
  `data_freshness`, `missing_price_count`, and `missing_sheet_count`.
- `refresh.reload_endpoints` tells the client which read endpoints to reload after
  refresh succeeds.
- `refresh.pipeline_refreshed=false` means the server reused a persisted base
  pipeline and only refreshed current prices.
- `refresh.pipeline_snapshot_id`, when present, identifies the Firestore-backed
  base pipeline used for the refresh. Clients should display it only in
  diagnostics.
- After refresh succeeds, the server persists the refreshed request context as
  the latest Firestore-backed state for matching read calls. Clients do not need
  to append `cache_bust` unless they are deliberately debugging a specific
  rebuild token.

## Endpoint 10: Trigger IBKR Import

`POST /v1/mobile/import`

Purpose: explicit user-triggered IBKR Flex import. This is separate from
`POST /v1/mobile/refresh`: import asks IBKR for newly published statement rows;
refresh reloads stored rows and current prices.

Response:

```json
{
  "import": {
    "status": "started",
    "job_name": "ibkr-flex-import",
    "region": "europe-west6",
    "project_id": "options-performance-dashboard",
    "operation_name": "projects/.../operations/...",
    "started_at": "2026-05-16T19:00:00+02:00",
    "message": "IBKR import job started. Refresh data after the job finishes to load newly imported rows."
  },
  "reload_endpoints": [
    "/v1/mobile/issues",
    "/v1/mobile/dashboard",
    "/v1/mobile/positions",
    "/v1/mobile/open-option-shorts",
    "/v1/mobile/tickers",
    "/v1/mobile/performance/monthly",
    "/v1/mobile/performance/yearly"
  ]
}
```

Client behavior:

- Show this as a Settings/diagnostics action, not as the normal price refresh.
- Disable the button while the request is in flight.
- On success, show that the import was started. The Cloud Run Job runs
  asynchronously, so the client should not assume new rows are available
  immediately.
- After a short delay or after the user taps normal refresh, call
  `POST /v1/mobile/refresh` and reload the listed endpoints.
- If the job starts but IBKR still has not published the statement, the next
  `/v1/mobile/issues` payload should continue showing an actionable `import`
  warning.

## Error Contract

Non-2xx responses should use one consistent JSON shape:

```json
{
  "error": {
    "code": "no_selected_sheets",
    "message": "No selected option sheets are available.",
    "details": {
      "selected_sheets": ["Options 2026"],
      "available_sheets": ["Options 2022", "Options 2023", "Options 2024", "Options 2025"],
      "missing_sheets": ["Options 2026"]
    },
    "request_id": "req_20260503_205902_abc123"
  }
}
```

Sheet availability rule:

- If some selected sheets are available and some are missing, return `200` with `data_freshness.source_sheets` marking missing sheets as `status: "missing"` and include a warning issue.
- If no selected/default sheets are available, return `422 no_selected_sheets`.

Recommended HTTP mappings:

| HTTP | `error.code` | When |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed query/body, unsupported enum, invalid boolean. |
| `400` | `invalid_as_of` | Date cannot be parsed. |
| `422` | `unknown_sheet` | A sheet parameter is invalid and cannot be matched to an option sheet name. |
| `422` | `no_selected_sheets` | Selected sheets resolved to an empty list. |
| `503` | `backend_unavailable` | Workbook/source cannot be loaded. |
| `503` | `price_provider_unavailable` | Live or historical price provider failed globally. |
| `207` or `200` with `status: "partial"` | `partial_refresh` | Refresh completed with partial failures. Prefer normal refresh response for partial refresh, not this error envelope. |

Error nullability:

- `details` may be `{}` but must be present.
- `request_id` may be `null` in local development, but production should populate it.

## Streamlit Concepts That Should Not Leak Into The API

- Tab names: `Yearly`, `Monthly cycles`, `Per ticker`, `Positions`, `Config`, `Logs / data issues`, `Methodology`.
- Streamlit session state keys: `selected_sheets`, `include_unrealized`, `pipeline_reload_token`, `price_refresh_token`, `price_session_id`.
- Streamlit cache behavior or cache keys.
- Display-only labels such as `YTD Realized P&L (w/ div)` or `Current unrealized snapshot`.
- Styled dataframe output, Pandas Styler color rules, `use_container_width`, and column renames like `Moneyness %`.
- Markdown methodology text.
- Button names and UI layout concepts such as columns, captions, tabs, and warnings.

## Backend Data Shape Changes That Would Make This Cleaner

1. Add DTO builders next to the portfolio backend:
   - `build_mobile_dashboard(state, request)`
   - `build_open_option_short_rows(state)`
   - `build_inventory_rows(state)`
   - `build_ticker_summaries(state)`
   - `build_monthly_target_rows(state, target_return)`
   - `build_issue_rows(state)`

2. Move moneyness and band logic out of Streamlit:
   - Current calculation lives in `build_open_options_positions_frame`.
   - Current color bands live in `_highlight_short_option_price`.
   - API should return `moneyness_band` and `risk_label`; the client decides colors.

3. Replace raw issue strings with typed issue objects:
   - Keep `message`.
   - Add `category`, `severity`, `tickers`, and `action`.

4. Give all mobile rows stable IDs:
   - Option row ID must be `optlot:{ticker}:{option_type}:{strike}:{expiration}:{opened}:{lot_sequence}`.
   - Inventory row ID must be `inventory:{ticker}:{buy_date_or_unknown}:{source}:{lot_sequence}`.
   - Ticker row ID must be `ticker:{ticker}`.
   - Monthly row ID must be `month:{YYYY-MM-DD}`.
   - Yearly row ID must be `year:{YYYY}`.
   - `lot_sequence` is mandatory and assigned by deterministic backend sort, not by current client-visible order.

5. Standardize null handling:
   - Convert Pandas `NaN`, `NaT`, and `pd.NA` to JSON `null`.
   - Never return formatted currency strings from API.

6. Separate freshness from content:
   - Every endpoint should include `data_freshness`.
   - Refresh metadata should be generated by backend service code, not Streamlit UI code.

7. Treat workbook sheet availability as data health:
   - If `Options 2026` is selected but absent from `latest_download.xlsx`, return it as `status: "missing"` under `source_sheets` and include a warning issue.

## Implementation Notes

- Start by keeping the existing pipeline as the single source of truth: `build_base_pipeline`, `apply_live_price_overlay`, and `apply_unrealized_adjusted_display`.
- Keep mobile DTO builders separate from HTTP routes. This keeps API work testable without starting a server.
- Add focused tests for:
  - moneyness band assignment.
  - missing current price serialization.
  - monthly target status.
  - `NaN` to `null` conversion.
  - selected sheet missing/loaded statuses.
  - typed issue conversion.
