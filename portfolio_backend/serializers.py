from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from portfolio_backend.models import PipelineState
from portfolio_backend.tables import build_assigned_holdings_frame, build_open_option_shorts_frame


def json_safe(value: Any) -> Any:
    """Convert pandas/numpy values into JSON-safe plain Python values."""
    if value is None:
        return None
    if value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date().isoformat() if value.time() == datetime.min.time() else value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    return value


def _key(name: Any) -> str:
    text = str(name).strip().lower()
    text = text.replace("%", "pct").replace("&", "and")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def dataframe_records(df: pd.DataFrame, normalize_keys: bool = True) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    records = []
    for raw in df.to_dict(orient="records"):
        row = {}
        for key, value in raw.items():
            out_key = _key(key) if normalize_keys else str(key)
            row[out_key] = json_safe(value)
        records.append(row)
    return records


def series_records(series: pd.Series, *, date_key: str, value_key: str) -> List[Dict[str, Any]]:
    if series is None or series.empty:
        return []
    records = []
    for index, value in series.items():
        records.append({date_key: json_safe(index), value_key: json_safe(value)})
    return records


def _current_year_row(df: pd.DataFrame, as_of_year: int) -> Dict[str, Any]:
    if df is None or df.empty or "year" not in df.columns:
        return {}
    rows = df.loc[df["year"] == as_of_year]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def serialize_snapshot(state: PipelineState, include_unrealized_current_year: bool) -> Dict[str, Any]:
    as_of_year = int(pd.to_datetime(state.as_of).year)
    yearly = state.yearly_with_unreal if include_unrealized_current_year else state.yearly
    ytd_row = _current_year_row(yearly, as_of_year)
    realized_total = float(ytd_row.get("total_realized_pnl", 0.0) or 0.0)
    ytd_total = (
        None
        if include_unrealized_current_year and state.unrealized_blocked
        else realized_total + (float(state.total_unreal) if include_unrealized_current_year else 0.0)
    )
    twr_field = (
        "annualized_return_twr_unrealized_adjusted"
        if include_unrealized_current_year
        else "annualized_return_twr"
    )
    twr = ytd_row.get(twr_field)
    if (include_unrealized_current_year and state.unrealized_blocked) or as_of_year in set(state.capital_history_affected_years):
        twr = None

    return {
        "as_of": json_safe(state.as_of),
        "year": as_of_year,
        "include_unrealized_current_year": bool(include_unrealized_current_year),
        "ytd_total_pnl": json_safe(ytd_total),
        "ytd_realized_pnl": json_safe(realized_total),
        "ytd_annualized_twr": json_safe(twr),
        "unrealized": {
            "complete": not bool(state.unrealized_blocked),
            "total": json_safe(state.total_unreal),
            "options": json_safe(state.option_unreal),
            "stock": json_safe(state.stock_unreal),
            "missing_required_price_tickers": json_safe(state.missing_required_price_tickers),
        },
        "prices": {
            "updated_at": json_safe(state.price_updated_at),
            "stocks_requested": json_safe(state.price_summary.get("stocks_requested", 0)),
            "stocks_fetched": json_safe(state.price_summary.get("stocks_fetched", 0)),
        },
        "issue_count": len(state.issues) + len(state.price_errors),
    }


def serialize_positions(state: PipelineState) -> Dict[str, List[Dict[str, Any]]]:
    inventory = build_assigned_holdings_frame(state.inv_df)
    open_options = build_open_option_shorts_frame(state.open_options, state.stock_prices or {})

    return {
        "assigned_holdings": dataframe_records(inventory),
        "open_option_shorts": dataframe_records(open_options),
    }


def serialize_yearly(state: PipelineState, include_unrealized_current_year: bool) -> List[Dict[str, Any]]:
    yearly = state.yearly_with_unreal if include_unrealized_current_year else state.yearly
    return dataframe_records(yearly)


def serialize_monthly(state: PipelineState, include_unrealized_current_year: bool) -> Dict[str, Any]:
    monthly_returns = (
        state.monthly_returns_unrealized_adjusted
        if include_unrealized_current_year
        else state.monthly_returns_covered
    )
    monthly_cycles = state.monthly_cycles.copy() if state.monthly_cycles is not None else pd.DataFrame()
    if not monthly_cycles.empty:
        monthly_cycles.index.name = "month"
    return {
        "cycles": dataframe_records(monthly_cycles.reset_index()),
        "returns": series_records(monthly_returns, date_key="month", value_key="return"),
        "covered_returns": series_records(state.monthly_returns_covered, date_key="month", value_key="return"),
        "unrealized_adjusted_returns": series_records(
            state.monthly_returns_unrealized_adjusted,
            date_key="month",
            value_key="return",
        ),
        "active_returns": series_records(state.monthly_returns_active, date_key="month", value_key="return"),
    }


def serialize_per_ticker(state: PipelineState) -> List[Dict[str, Any]]:
    return dataframe_records(state.per_ticker_totals)


def serialize_issues(state: PipelineState) -> Dict[str, Any]:
    return {
        "issues": json_safe(state.issues),
        "price_errors": json_safe(state.price_errors),
        "historical_price_errors": json_safe(state.historical_price_errors),
        "dividend_errors": json_safe(state.dividend_errors),
        "capital_history_incomplete": bool(state.capital_history_incomplete),
        "capital_history_coverage_issues": json_safe(state.capital_history_coverage_issues),
        "return_series_truncated": bool(state.return_series_truncated),
        "first_incomplete_return_month": json_safe(state.first_incomplete_return_month),
        "last_complete_return_month": json_safe(state.last_complete_return_month),
    }


def serialize_metadata(state: PipelineState) -> Dict[str, Any]:
    return {
        "as_of": json_safe(state.as_of),
        "price_updated_at": json_safe(state.price_updated_at),
        "price_summary": json_safe(state.price_summary),
        "historical_price_summary": json_safe(state.historical_price_summary),
        "dividend_summary": json_safe(state.dividend_summary),
        "dividend_coverage_complete": bool(state.dividend_coverage_complete),
        "sheet_counts": dataframe_records(state.sheet_counts),
    }


def serialize_portfolio_state(
    state: PipelineState,
    include_unrealized_current_year: bool,
) -> Dict[str, Any]:
    return {
        "snapshot": serialize_snapshot(state, include_unrealized_current_year),
        "positions": serialize_positions(state),
        "yearly": serialize_yearly(state, include_unrealized_current_year),
        "monthly": serialize_monthly(state, include_unrealized_current_year),
        "per_ticker": serialize_per_ticker(state),
        "issues": serialize_issues(state),
        "metadata": serialize_metadata(state),
    }
