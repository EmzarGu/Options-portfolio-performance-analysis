from __future__ import annotations

import math
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

import mobile_api
from portfolio_backend.charts import build_benchmark_growth_chart_data, build_options_cycle_chart_data
from portfolio_backend.mobile_api_service import (
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_positions_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)
from portfolio_backend.mobile_payloads import build_mobile_snapshot, build_yearly_performance_rows
from portfolio_backend.performance import expectancies


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _frame_records(df: Any, *, index_name: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    frame = df.copy()
    if index_name is not None:
        frame = frame.reset_index().rename(columns={frame.reset_index().columns[0]: index_name})
    if limit is not None:
        frame = frame.head(limit)
    return _json_safe(frame.to_dict(orient="records"))


def _series_records(series: Any, value_name: str) -> List[Dict[str, Any]]:
    if series is None or getattr(series, "empty", True):
        return []
    frame = series.rename(value_name).reset_index()
    first_col = frame.columns[0]
    frame = frame.rename(columns={first_col: "month"})
    return _json_safe(frame.to_dict(orient="records"))


def _benchmark_growth_by_range(state: Any) -> Dict[str, List[Dict[str, Any]]]:
    ranges = ["3M", "6M", "YTD", "1Y", "Since inception"]
    return {
        range_choice: _frame_records(
            build_benchmark_growth_chart_data(
                state.monthly_returns_covered,
                state.aligned_bench_returns,
                range_choice,
                state.as_of,
            )
        )
        for range_choice in ranges
    }


def _stock_price_records(stock_prices: Any) -> List[Dict[str, Any]]:
    if not stock_prices:
        return []
    rows = [{"ticker": str(ticker), "price": price} for ticker, price in stock_prices.items()]
    rows.sort(key=lambda row: row["ticker"])
    return _json_safe(rows)


def _advanced_unreal_records(advanced_unreal: Any) -> List[Dict[str, Any]]:
    if advanced_unreal is None or getattr(advanced_unreal, "empty", True):
        return []
    if isinstance(advanced_unreal, pd.Series):
        frame = advanced_unreal.rename("unrealized_pnl").reset_index()
        first_col = frame.columns[0]
        frame = frame.rename(columns={first_col: "ticker"})
        return _json_safe(frame.to_dict(orient="records"))
    return _frame_records(advanced_unreal)


def _expectancy_by_year_records(state: Any) -> List[Dict[str, Any]]:
    years = set()
    for event in getattr(state, "realized_option_events", []) or []:
        years.add(pd.to_datetime(event.date).year)
    for sale in getattr(state, "realized_sales", []) or []:
        years.add(pd.to_datetime(sale.date).year)
    if getattr(state, "monthly_cycles", None) is not None and not state.monthly_cycles.empty:
        monthly = state.monthly_cycles.copy()
        monthly.index = pd.to_datetime(monthly.index, errors="coerce")
        years.update(monthly.index.dropna().year.tolist())
    for chain in getattr(state, "chain_outcomes", []) or []:
        if getattr(chain, "end", None) is not None:
            years.add(pd.to_datetime(chain.end).year)

    rows: List[Dict[str, Any]] = []
    for year in sorted(years):
        option_events = [
            event
            for event in getattr(state, "realized_option_events", []) or []
            if pd.to_datetime(event.date).year == year
        ]
        sales = [
            sale
            for sale in getattr(state, "realized_sales", []) or []
            if pd.to_datetime(sale.date).year == year
        ]
        monthly_summary = getattr(state, "monthly_cycles", pd.DataFrame())
        if monthly_summary is not None and not monthly_summary.empty:
            monthly_summary = monthly_summary.copy()
            monthly_summary.index = pd.to_datetime(monthly_summary.index, errors="coerce")
            monthly_summary = monthly_summary[monthly_summary.index.year == year]
        chains = [
            chain
            for chain in getattr(state, "chain_outcomes", []) or []
            if getattr(chain, "end", None) is not None and pd.to_datetime(chain.end).year == year
        ]
        frame = expectancies(option_events, sales, monthly_summary, chains)
        if frame is not None and not frame.empty:
            frame.insert(0, "Year", year)
            rows.extend(_frame_records(frame))
    return rows


def get_web_context(*, as_of: Optional[date], include_unrealized: bool, force_rebuild: bool = False):
    # The browser dashboard is the IBKR-first production surface. Apply these
    # defaults lazily so importing this module cannot alter Streamlit/mobile
    # tests that intentionally rely on sheet mode defaults.
    os.environ.setdefault("OPTIONS_DATA_SOURCE", "ibkr")
    os.environ.setdefault("IBKR_REPORT_SOURCE", "firestore")
    if force_rebuild:
        context, cache_bust, _refresh_metadata = mobile_api._smart_refresh_context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=None,
            cache_bust=None,
        )
        return context, cache_bust
    cache_bust = None
    context = mobile_api._context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=None,
        cache_bust=cache_bust,
        force_rebuild=force_rebuild,
    )
    return context, cache_bust


def build_dashboard_data(
    *,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    target_return: Optional[float] = None,
    default_target_return: float = 0.015,
) -> Dict[str, Any]:
    # The web UI can switch realized/unrealized presentation without a server
    # round trip, so build the full unrealized-capable context once and include
    # both presentation views in the payload.
    context, _ = get_web_context(as_of=as_of, include_unrealized=True)
    state = context.state
    monthly_target_return = default_target_return if target_return is None else target_return
    dashboard = build_mobile_dashboard_payload(context, target_return=monthly_target_return)
    positions = build_mobile_positions_payload(context)
    open_shorts = build_mobile_open_option_shorts_payload(context, sort="moneyness_risk", limit=None)
    tickers = build_mobile_tickers_payload(context, include_history=False)
    monthly = build_mobile_monthly_payload(
        context,
        target_return=monthly_target_return,
        monthly_range="since_inception",
    )
    yearly = build_mobile_yearly_payload(context)
    issues = build_mobile_issues_payload(context)

    benchmark_growth_by_range = _benchmark_growth_by_range(state)
    expectancy = expectancies(
        getattr(state, "realized_option_events", []),
        getattr(state, "realized_sales", []),
        state.monthly_cycles,
        getattr(state, "chain_outcomes", []),
    )
    expectancy_by_year = _expectancy_by_year_records(state)

    return _json_safe(
        {
            "app": {
                "revision": os.getenv("K_REVISION", "local"),
                "restart_ts": os.getenv("WEB_RESTART_TS", ""),
            },
            "web": {
                "include_unrealized": bool(include_unrealized),
                "target_return": float(monthly_target_return),
            },
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "source": {
                "label": "IBKR Flex",
                "kind": "ibkr_flex",
                "row_count": int(len(getattr(state, "df_opts", []))),
                "sheet_counts": _frame_records(getattr(state, "sheet_counts", None)),
            },
            "dashboard": dashboard,
            "positions": positions,
            "open_shorts": open_shorts,
            "tickers": tickers,
            "monthly": monthly,
            "yearly": yearly,
            "views": {
                "snapshots": {
                    "with_unrealized": build_mobile_snapshot(state, True),
                    "realized_only": build_mobile_snapshot(state, False),
                },
                "yearly": {
                    "with_unrealized": build_yearly_performance_rows(state, include_unrealized=True),
                    "realized_only": build_yearly_performance_rows(state, include_unrealized=False),
                },
            },
            "issues": issues,
            "tables": {
                "monthly_cycles": _frame_records(state.monthly_cycles, index_name="month"),
                "yearly_realized": _frame_records(state.yearly),
                "yearly_with_unrealized": _frame_records(state.yearly_with_unreal),
                "per_ticker_yearly": _frame_records(state.per_ticker),
                "per_ticker_totals": _frame_records(state.per_ticker_totals),
                "benchmark_metrics": _frame_records(state.benchmark_metrics),
                "expectancy": _frame_records(expectancy),
                "expectancy_by_year": expectancy_by_year,
                "inventory": _frame_records(state.inv_df),
                "open_options": _frame_records(state.open_options),
                "options_cycle_pnl": _frame_records(build_options_cycle_chart_data(state.monthly_cycles)),
                "stock_prices": _stock_price_records(getattr(state, "stock_prices", {}) or {}),
                "unrealized_by_ticker": _advanced_unreal_records(getattr(state, "advanced_unreal", None)),
                "capital_daily_tail": _frame_records(
                    getattr(state, "capital_daily", pd.DataFrame()).tail(30),
                    index_name="date",
                ),
                "dividends": _frame_records(getattr(state, "div_df", pd.DataFrame())),
            },
            "charts": {
                "benchmark_growth": benchmark_growth_by_range.get("YTD", []),
                "benchmark_growth_by_range": benchmark_growth_by_range,
                "monthly_returns": _series_records(state.monthly_returns_covered, "return"),
                "monthly_returns_unrealized_adjusted": _series_records(
                    state.monthly_returns_unrealized_adjusted,
                    "return",
                ),
            },
            "reconciliation_notes": [
                {
                    "case": "FTNT",
                    "status": "Matched",
                    "detail": "Open 95C expiring 2026-09-18 caps 100 assigned shares.",
                },
                {
                    "case": "CCJ/NVDA April",
                    "status": "Expected IBKR periodization",
                    "detail": "IBKR recognizes valid wheel roll/close economics on lifecycle dates.",
                },
                {
                    "case": "AAPL April",
                    "status": "Expected IBKR periodization",
                    "detail": "Assignment economics were recognized through prior roll events, not duplicated in April.",
                },
                {
                    "case": "SPY/ABR",
                    "status": "Excluded",
                    "detail": "Excluded from wheel P&L by accounting rules.",
                },
                {
                    "case": "ZM monthly premium",
                    "status": "Matched semantics",
                    "detail": "Incremental projection premium is separate from roll-adjusted open-chain premium.",
                },
            ],
        }
    )

def dashboard_shell_data(*, include_unrealized: bool, target_return: float) -> Dict[str, Any]:
    return {
        "loading": True,
        "app": {
            "revision": os.getenv("K_REVISION", "local"),
            "restart_ts": os.getenv("WEB_RESTART_TS", ""),
        },
        "web": {
            "include_unrealized": bool(include_unrealized),
            "target_return": float(target_return),
        },
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": {
            "label": "IBKR Flex",
            "kind": "ibkr_flex",
            "row_count": None,
            "sheet_counts": [],
        },
        "dashboard": {
            "request": {"as_of": None, "include_unrealized": bool(include_unrealized), "selected_sheets": ["IBKR Flex"]},
            "data_freshness": {"price_coverage": {"priced_count": 0, "required_count": 0, "missing_count": 0}},
            "snapshot": {},
            "monthly_target": {},
            "open_option_short_preview": [],
            "issue_summary": {"severity": "loading", "total_count": 0},
        },
        "positions": {"inventory": [], "open_option_shorts": []},
        "open_shorts": {"items": []},
        "tickers": {"items": []},
        "monthly": {"target_return": float(target_return), "current_month": {}, "months": [], "future_months": []},
        "yearly": {"years": []},
        "views": {"snapshots": {"with_unrealized": {}, "realized_only": {}}, "yearly": {"with_unrealized": [], "realized_only": []}},
        "issues": {
            "summary": {"severity": "loading", "total_count": 0},
            "issues": [],
            "audit_summary": {"total_count": 0},
            "audit_notes": [],
            "coverage": {},
        },
        "tables": {
            "monthly_cycles": [],
            "yearly_realized": [],
            "yearly_with_unrealized": [],
            "per_ticker_yearly": [],
            "per_ticker_totals": [],
            "benchmark_metrics": [],
            "expectancy": [],
            "expectancy_by_year": [],
            "inventory": [],
            "open_options": [],
            "options_cycle_pnl": [],
            "stock_prices": [],
            "unrealized_by_ticker": [],
            "capital_daily_tail": [],
            "dividends": [],
        },
        "charts": {
            "benchmark_growth": [],
            "benchmark_growth_by_range": {},
            "monthly_returns": [],
            "monthly_returns_unrealized_adjusted": [],
        },
        "reconciliation_notes": [],
    }

