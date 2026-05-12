from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Hashable, Iterable, List, Optional

import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.issue_classification import classify_backend_issue
from portfolio_backend.serializers import json_safe
from portfolio_backend.tables import build_assigned_holdings_frame, build_open_option_shorts_frame


MONEYNESS_BANDS = [
    {
        "band": "in_the_money",
        "label": "ITM",
        "min_exclusive": 0.0,
        "max_inclusive": None,
        "severity": "critical",
    },
    {
        "band": "at_strike",
        "label": "-1% to 0%",
        "min_inclusive": -0.01,
        "max_inclusive": 0.0,
        "severity": "high",
    },
    {
        "band": "near",
        "label": "-5% to -1%",
        "min_inclusive": -0.05,
        "max_exclusive": -0.01,
        "severity": "medium",
    },
    {
        "band": "ok",
        "label": "-10% to -5%",
        "min_inclusive": -0.10,
        "max_exclusive": -0.05,
        "severity": "low",
    },
    {
        "band": "clear",
        "label": "< -10%",
        "min_inclusive": None,
        "max_exclusive": -0.10,
        "severity": "info",
    },
]

_BAND_RISK_ORDER = {
    "in_the_money": 0,
    "at_strike": 1,
    "near": 2,
    "ok": 3,
    "clear": 4,
    None: 5,
}


def build_mobile_request(
    as_of: date | datetime | pd.Timestamp | str,
    include_unrealized: bool,
    selected_sheets: Iterable[str],
) -> Dict[str, Any]:
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    return {
        "as_of": None if pd.isna(as_of_ts) else json_safe(as_of_ts),
        "include_unrealized": bool(include_unrealized),
        "selected_sheets": [str(sheet) for sheet in selected_sheets or []],
    }


def build_data_freshness(
    state,
    selected_sheets: Iterable[str],
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    """Build the shared mobile freshness envelope.

    Timestamp coverage is intentionally explicit here. Streamlit currently stores
    some freshness values as display strings, so callers should pass ISO
    timestamps when they have them instead of relying on UI state.
    """
    source_metadata = source_metadata or {}
    sheet_counts = getattr(state, "sheet_counts", pd.DataFrame())
    row_counts: Dict[str, int] = {}
    if sheet_counts is not None and not sheet_counts.empty:
        for row in sheet_counts.to_dict(orient="records"):
            sheet = row.get("source_sheet")
            if sheet is not None:
                row_counts[str(sheet)] = int(row.get("rows") or 0)

    available = {str(sheet) for sheet in available_sheets} if available_sheets is not None else set(row_counts)
    source_sheets = []
    for sheet in [str(sheet) for sheet in selected_sheets or []]:
        loaded = sheet in row_counts or sheet in available
        source_sheets.append(
            {
                "name": sheet,
                "status": "loaded" if loaded else "missing",
                "rows": int(row_counts.get(sheet, 0)),
            }
        )

    price_summary = getattr(state, "price_summary", {}) or {}
    missing_tickers = list(getattr(state, "missing_required_price_tickers", []) or [])

    return {
        "pipeline_built_at": json_safe(pipeline_built_at or source_metadata.get("pipeline_built_at")),
        "prices_updated_at": json_safe(
            prices_updated_at
            or source_metadata.get("prices_updated_at")
            or source_metadata.get("price_updated_at")
        ),
        "source_downloaded_at": json_safe(source_metadata.get("source_downloaded_at") or source_metadata.get("downloaded_at")),
        "source_modified_at": json_safe(source_metadata.get("source_modified_at") or source_metadata.get("modified_at")),
        "price_coverage": {
            "stocks_requested": json_safe(price_summary.get("stocks_requested", 0)),
            "stocks_fetched": json_safe(price_summary.get("stocks_fetched", 0)),
            "missing_tickers": json_safe(missing_tickers),
        },
        "source_sheets": source_sheets,
    }


def _request_value(request: Dict[str, Any], key: str, default: Any = None) -> Any:
    return request.get(key, default) if isinstance(request, dict) else getattr(request, key, default)


def _current_year_row(df: pd.DataFrame, as_of_year: int) -> Dict[str, Any]:
    if df is None or df.empty or "year" not in df.columns:
        return {}
    rows = df.loc[df["year"] == as_of_year]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _put_assignment_risk_from_inventory(state) -> Dict[str, Any]:
    inventory = getattr(state, "inv_df", pd.DataFrame())
    if inventory is None or inventory.empty or "source" not in inventory.columns:
        return {
            "unrealized_pnl": _number(getattr(state, "put_assignment_unreal", None)) or 0.0,
            "cash_required": _number(getattr(state, "itm_put_cash_required", None)) or 0.0,
            "market_value": _number(getattr(state, "itm_put_market_value", None)) or 0.0,
            "contracts": int(_number(getattr(state, "itm_put_contracts", None)) or 0),
            "shares": int(_number(getattr(state, "itm_put_shares", None)) or 0),
            "available_cash": _number(getattr(state, "available_cash", None)),
        }

    put_rows = inventory.loc[inventory["source"].eq("put_gap")].copy()
    if put_rows.empty:
        return {
            "unrealized_pnl": _number(getattr(state, "put_assignment_unreal", None)) or 0.0,
            "cash_required": _number(getattr(state, "itm_put_cash_required", None)) or 0.0,
            "market_value": _number(getattr(state, "itm_put_market_value", None)) or 0.0,
            "contracts": int(_number(getattr(state, "itm_put_contracts", None)) or 0),
            "shares": int(_number(getattr(state, "itm_put_shares", None)) or 0),
            "available_cash": _number(getattr(state, "available_cash", None)),
        }

    shares = pd.to_numeric(put_rows.get("shares"), errors="coerce").fillna(0.0)
    strike = pd.to_numeric(put_rows.get("cost_per_share"), errors="coerce").fillna(0.0)
    current = pd.to_numeric(put_rows.get("current_price"), errors="coerce").fillna(0.0)
    unreal = pd.to_numeric(put_rows.get("unrealized_pnl"), errors="coerce").fillna(0.0)
    total_shares = int(shares.sum())
    return {
        "unrealized_pnl": float(unreal.sum()),
        "cash_required": float((shares * strike).sum()),
        "market_value": float((shares * current).sum()),
        "contracts": int(round(total_shares / CONTRACT_MULTIPLIER)) if total_shares else 0,
        "shares": total_shares,
        "available_cash": _number(getattr(state, "available_cash", None)),
    }


def build_mobile_snapshot(state, include_unrealized: bool) -> Dict[str, Any]:
    as_of_ts = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    as_of_year = int(as_of_ts.year) if not pd.isna(as_of_ts) else None
    yearly = getattr(state, "yearly_with_unreal", pd.DataFrame()) if include_unrealized else getattr(state, "yearly", pd.DataFrame())
    ytd_row = _current_year_row(yearly, as_of_year) if as_of_year is not None else {}
    realized_total = _number(ytd_row.get("total_realized_pnl")) or 0.0
    unrealized_blocked = bool(getattr(state, "unrealized_blocked", False))
    total_unreal = _number(getattr(state, "total_unreal", None))
    option_unreal = _number(getattr(state, "option_unreal", None))
    stock_unreal = _number(getattr(state, "stock_unreal", None))
    put_assignment_risk = _put_assignment_risk_from_inventory(state)

    if include_unrealized and unrealized_blocked:
        ytd_total = None
        current_unreal = None
        current_option_unreal = None
        current_stock_unreal = None
        current_put_assignment_unreal = None
    else:
        ytd_total = realized_total + (total_unreal if include_unrealized and total_unreal is not None else 0.0)
        current_unreal = total_unreal
        current_option_unreal = option_unreal
        current_stock_unreal = stock_unreal
        current_put_assignment_unreal = put_assignment_risk["unrealized_pnl"]

    twr_field = "annualized_return_twr_unrealized_adjusted" if include_unrealized else "annualized_return_twr"
    ytd_twr = _number(ytd_row.get(twr_field))
    if (include_unrealized and unrealized_blocked) or (
        as_of_year is not None and as_of_year in set(getattr(state, "capital_history_affected_years", []) or [])
    ):
        ytd_twr = None

    return {
        "currency": "USD",
        "year": as_of_year,
        "ytd_total_pnl": json_safe(ytd_total),
        "ytd_realized_pnl": json_safe(realized_total),
        "current_unrealized_pnl": json_safe(current_unreal),
        "current_option_unrealized_pnl": json_safe(current_option_unreal),
        "current_stock_unrealized_pnl": json_safe(current_stock_unreal),
        "current_put_assignment_unrealized_pnl": json_safe(current_put_assignment_unreal),
        "itm_put_cash_required": json_safe(put_assignment_risk["cash_required"]),
        "itm_put_market_value": json_safe(put_assignment_risk["market_value"]),
        "itm_put_contracts": json_safe(put_assignment_risk["contracts"]),
        "itm_put_shares": json_safe(put_assignment_risk["shares"]),
        "available_cash": json_safe(put_assignment_risk["available_cash"]),
        "ytd_annualized_twr": json_safe(ytd_twr),
        "unrealized_adjusted": bool(include_unrealized),
        "unrealized_blocked": unrealized_blocked,
    }


def _current_month_row(monthly_cycles: pd.DataFrame, as_of: Any) -> tuple[Optional[pd.Timestamp], Dict[str, Any]]:
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_ts):
        return None, {}
    month_end = as_of_ts.to_period("M").to_timestamp("M")
    if monthly_cycles is None or monthly_cycles.empty:
        return month_end, {}
    monthly = monthly_cycles.copy()
    monthly.index = pd.to_datetime(monthly.index, errors="coerce")
    monthly = monthly[monthly.index.notna()]
    if month_end not in monthly.index:
        return month_end, {}
    return month_end, monthly.loc[month_end].to_dict()


def _monthly_target_status(current_return: Optional[float], target_return: float, month_end: Optional[pd.Timestamp], as_of: Any) -> str:
    if current_return is None or month_end is None:
        return "unavailable"
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_ts) or month_end.normalize() <= as_of_ts.normalize():
        return "beat" if current_return >= target_return else "miss"
    return "beat" if current_return >= target_return else "below_target"


def _sum_open_expiring_premium_from_frame(
    open_options: pd.DataFrame,
    month_end: pd.Timestamp,
    *,
    price_column: str,
) -> float:
    required_columns = {"expiration", price_column, "qty"}
    if price_column == "roll_adjusted_open_price":
        required_columns = {"expiration", "qty"}
    if not required_columns.issubset(open_options.columns):
        return 0.0

    options = open_options.copy()
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    options = options.loc[options["expiration"].notna()]
    if options.empty:
        return 0.0

    expiring = options.loc[options["expiration"].dt.to_period("M") == month_end.to_period("M")]
    if expiring.empty:
        return 0.0

    if price_column == "roll_adjusted_open_price":
        if "roll_adjusted_open_price" in expiring.columns:
            open_prices = pd.to_numeric(expiring["roll_adjusted_open_price"], errors="coerce")
        else:
            open_prices = pd.Series(pd.NA, index=expiring.index, dtype="Float64")
        if "open_price" in expiring.columns:
            open_prices = open_prices.fillna(pd.to_numeric(expiring["open_price"], errors="coerce"))
    else:
        open_prices = pd.to_numeric(expiring[price_column], errors="coerce")
    quantities = pd.to_numeric(expiring["qty"], errors="coerce").abs()
    premium = (open_prices * quantities * CONTRACT_MULTIPLIER).dropna()
    return float(premium.sum()) if not premium.empty else 0.0


def _open_expiring_incremental_premium(state, month_end: Optional[pd.Timestamp]) -> Optional[float]:
    if month_end is None:
        return None
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty:
        return 0.0
    return _sum_open_expiring_premium_from_frame(open_options, month_end, price_column="open_price")


def _open_expiring_roll_adjusted_premium(state, month_end: Optional[pd.Timestamp]) -> Optional[float]:
    if month_end is None:
        return None
    lots = getattr(state, "lots", None)
    if lots is not None:
        total = 0.0
        found = False
        for lot in lots:
            if getattr(lot, "close_date", None) is not None:
                continue
            expiration = pd.to_datetime(getattr(lot, "expiration", None), errors="coerce")
            if pd.isna(expiration) or expiration.to_period("M") != month_end.to_period("M"):
                continue
            qty = abs(_number(getattr(lot, "qty", None)) or 0.0)
            roll_price = getattr(lot, "roll_adjusted_open_price", None)
            price = _number(roll_price if roll_price is not None else getattr(lot, "open_price", None))
            if price is None:
                continue
            total += price * qty * CONTRACT_MULTIPLIER
            found = True
        if found:
            return float(total)

    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty:
        return 0.0
    price_column = "roll_adjusted_open_price" if "roll_adjusted_open_price" in open_options.columns else "open_price"
    return _sum_open_expiring_premium_from_frame(open_options, month_end, price_column=price_column)


def _monthly_projection_values(
    state,
    row: Dict[str, Any],
    month_end: Optional[pd.Timestamp],
    target_return: float,
) -> Dict[str, Optional[float]]:
    avg_capital = _number(row.get("avg_capital"))
    peak_capital = _number(row.get("peak_capital"))
    realized_month_pnl = _number(row.get("total_realized_pnl"))
    open_expiring_incremental_premium = _open_expiring_incremental_premium(state, month_end)
    open_expiring_roll_adjusted_premium = _open_expiring_roll_adjusted_premium(state, month_end)
    open_expiring_option_premium = open_expiring_incremental_premium

    projected_month_pnl = None
    if realized_month_pnl is not None and open_expiring_incremental_premium is not None:
        projected_month_pnl = realized_month_pnl + open_expiring_incremental_premium

    projected_return_roac = None
    if projected_month_pnl is not None and avg_capital not in (None, 0):
        projected_return_roac = projected_month_pnl / avg_capital

    projected_return_ropc = None
    if projected_month_pnl is not None and peak_capital not in (None, 0):
        projected_return_ropc = projected_month_pnl / peak_capital

    target_pnl = avg_capital * target_return if avg_capital is not None else None
    projected_remaining_pnl = None
    if target_pnl is not None and projected_month_pnl is not None:
        projected_remaining_pnl = max(target_pnl - projected_month_pnl, 0.0)
    includes_open_premium = bool((open_expiring_incremental_premium or 0.0) > 0.0)

    return {
        "realized_month_pnl": realized_month_pnl,
        "open_expiring_option_premium": open_expiring_option_premium,
        "open_expiring_incremental_premium": open_expiring_incremental_premium,
        "open_expiring_roll_adjusted_premium": open_expiring_roll_adjusted_premium,
        "includes_open_premium": includes_open_premium,
        "projection_basis": "realized_plus_open_premium" if includes_open_premium else "realized_only",
        "projected_month_pnl": projected_month_pnl,
        "projected_return_roac": projected_return_roac,
        "projected_return_ropc": projected_return_ropc,
        "target_pnl": target_pnl,
        "projected_remaining_pnl": projected_remaining_pnl,
    }


def build_monthly_target(state, *, target_return: float = 0.015) -> Dict[str, Any]:
    month_end, row = _current_month_row(getattr(state, "monthly_cycles", pd.DataFrame()), getattr(state, "as_of", None))
    avg_capital = _number(row.get("avg_capital"))
    current_return = _number(row.get("roac"))
    current_pnl = _number(row.get("total_realized_pnl"))
    projection = _monthly_projection_values(state, row, month_end, target_return)
    target_pnl = avg_capital * target_return if avg_capital is not None else None
    remaining_pnl = None
    if target_pnl is not None and current_pnl is not None:
        remaining_pnl = max(target_pnl - current_pnl, 0.0)

    as_of_ts = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    days_remaining = None
    if month_end is not None and not pd.isna(as_of_ts):
        days_remaining = max(int((month_end.normalize() - as_of_ts.normalize()).days), 0)

    return {
        "month": json_safe(month_end),
        "target_basis": "avg_capital",
        "target_return": json_safe(float(target_return)),
        "current_return": json_safe(current_return),
        "current_return_metric": "return_roac",
        "current_pnl": json_safe(current_pnl),
        "target_pnl": json_safe(target_pnl),
        "remaining_pnl": json_safe(remaining_pnl),
        "status": _monthly_target_status(current_return, target_return, month_end, getattr(state, "as_of", None)),
        "realized_month_pnl": json_safe(projection["realized_month_pnl"]),
        "realized_options_pnl": json_safe(_number(row.get("realized_options_pnl"))),
        "realized_stock_pnl": json_safe(_number(row.get("realized_stock_pnl"))),
        "open_expiring_option_premium": json_safe(projection["open_expiring_option_premium"]),
        "open_expiring_incremental_premium": json_safe(projection["open_expiring_incremental_premium"]),
        "open_expiring_roll_adjusted_premium": json_safe(projection["open_expiring_roll_adjusted_premium"]),
        "projected_month_pnl": json_safe(projection["projected_month_pnl"]),
        "projected_return_roac": json_safe(projection["projected_return_roac"]),
        "projected_return_ropc": json_safe(projection["projected_return_ropc"]),
        "projected_remaining_pnl": json_safe(projection["projected_remaining_pnl"]),
        "monthly_target_status": _monthly_target_status(
            projection["projected_return_roac"],
            target_return,
            month_end,
            getattr(state, "as_of", None),
        ),
        "includes_open_premium": bool(projection["includes_open_premium"]),
        "projection_basis": projection["projection_basis"],
        "days_remaining": json_safe(days_remaining),
    }


def _month_range_start(as_of: Any, monthly_range: str) -> Optional[pd.Timestamp]:
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if monthly_range == "since_inception" or pd.isna(as_of_ts):
        return None
    month_end = as_of_ts.to_period("M").to_timestamp("M")
    if monthly_range == "3m":
        return (month_end.to_period("M") - 2).to_timestamp("M")
    if monthly_range == "6m":
        return (month_end.to_period("M") - 5).to_timestamp("M")
    if monthly_range == "ytd":
        return pd.Timestamp(year=int(as_of_ts.year), month=1, day=1).to_period("M").to_timestamp("M")
    if monthly_range == "1y":
        return (month_end.to_period("M") - 11).to_timestamp("M")
    raise ValueError(f"Unsupported monthly range: {monthly_range}")


def _monthly_cycles_frame(state, monthly_range: str) -> pd.DataFrame:
    monthly = getattr(state, "monthly_cycles", pd.DataFrame())
    if monthly is None or monthly.empty:
        return pd.DataFrame()
    out = monthly.copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out.loc[out.index.notna()]
    start = _month_range_start(getattr(state, "as_of", None), monthly_range)
    if start is not None:
        out = out.loc[out.index >= start]
    as_of_ts = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    if not pd.isna(as_of_ts):
        out = out.loc[out.index <= as_of_ts.to_period("M").to_timestamp("M")]
    return out.sort_index()


def _month_status(row: Dict[str, Any], month_end: Optional[pd.Timestamp], target_return: float, as_of: Any) -> str:
    return _monthly_target_status(_number(row.get("roac")), target_return, month_end, as_of)


def _mobile_month_row(
    row: Dict[str, Any],
    month_end: pd.Timestamp,
    target_return: float,
    as_of: Any,
    state=None,
    *,
    include_target_detail: bool = False,
) -> Dict[str, Any]:
    avg_capital = _number(row.get("avg_capital"))
    total_realized_pnl = _number(row.get("total_realized_pnl"))
    projection = _monthly_projection_values(state, row, month_end, target_return) if state is not None else {}
    target_pnl = avg_capital * target_return if avg_capital is not None else None
    remaining_pnl = None
    if target_pnl is not None and total_realized_pnl is not None:
        remaining_pnl = max(target_pnl - total_realized_pnl, 0.0)

    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    days_remaining = None
    if not pd.isna(as_of_ts):
        days_remaining = max(int((month_end.normalize() - as_of_ts.normalize()).days), 0)

    out = {
        "id": f"month:{json_safe(month_end)}",
        "month": json_safe(month_end),
        "realized_options_pnl": json_safe(_number(row.get("realized_options_pnl"))),
        "realized_stock_pnl": json_safe(_number(row.get("realized_stock_pnl"))),
        "dividends": json_safe(_number(row.get("dividends"))),
        "total_realized_pnl": json_safe(total_realized_pnl),
        "avg_capital": json_safe(avg_capital),
        "peak_capital": json_safe(_number(row.get("peak_capital"))),
        "return_roac": json_safe(_number(row.get("roac"))),
        "return_ropc": json_safe(_number(row.get("ropc"))),
        "target_return": json_safe(float(target_return)),
        "status": _month_status(row, month_end, target_return, as_of),
    }
    if state is not None:
        out["realized_month_pnl"] = json_safe(projection["realized_month_pnl"])
        out["open_expiring_option_premium"] = json_safe(projection["open_expiring_option_premium"])
        out["open_expiring_incremental_premium"] = json_safe(projection["open_expiring_incremental_premium"])
        out["open_expiring_roll_adjusted_premium"] = json_safe(projection["open_expiring_roll_adjusted_premium"])
        out["includes_open_premium"] = bool(projection["includes_open_premium"])
        out["projection_basis"] = projection["projection_basis"]
        out["projected_month_pnl"] = json_safe(projection["projected_month_pnl"])
        out["projected_return_roac"] = json_safe(projection["projected_return_roac"])
        out["projected_return_ropc"] = json_safe(projection["projected_return_ropc"])
        out["target_pnl"] = json_safe(projection["target_pnl"])
        out["projected_remaining_pnl"] = json_safe(projection["projected_remaining_pnl"])
        out["monthly_target_status"] = _monthly_target_status(
            projection["projected_return_roac"],
            target_return,
            month_end,
            as_of,
        )
    if include_target_detail:
        out["target_pnl"] = json_safe(target_pnl)
        out["remaining_pnl"] = json_safe(remaining_pnl)
        out["days_remaining"] = json_safe(days_remaining)
    return out


def build_monthly_performance_rows(
    state,
    *,
    target_return: float = 0.015,
    monthly_range: str = "ytd",
) -> List[Dict[str, Any]]:
    monthly = _monthly_cycles_frame(state, monthly_range)
    if monthly.empty:
        return []
    rows = []
    for month_end, row in monthly.iterrows():
        rows.append(_mobile_month_row(row.to_dict(), month_end, target_return, getattr(state, "as_of", None), state))
    return rows


def _open_option_counts_by_expiration_month(state) -> Dict[pd.Timestamp, int]:
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty or "expiration" not in open_options.columns:
        return {}
    options = open_options.copy()
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    options = options.loc[options["expiration"].notna()]
    if options.empty:
        return {}
    options["month"] = options["expiration"].dt.to_period("M").dt.to_timestamp("M")
    return {month: int(count) for month, count in options.groupby("month").size().items()}


def build_future_monthly_performance_rows(
    state,
    *,
    target_return: float = 0.015,
) -> List[Dict[str, Any]]:
    as_of_ts = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    if pd.isna(as_of_ts):
        return []
    current_month = as_of_ts.to_period("M").to_timestamp("M")
    counts_by_month = _open_option_counts_by_expiration_month(state)
    rows = []
    for month_end in sorted(month for month in counts_by_month if month > current_month):
        row = {
            "total_realized_pnl": 0.0,
            "realized_options_pnl": 0.0,
            "realized_stock_pnl": 0.0,
            "dividends": 0.0,
            "avg_capital": None,
            "peak_capital": None,
            "roac": None,
            "ropc": None,
        }
        projection = _monthly_projection_values(state, row, month_end, target_return)
        rows.append(
            {
                "id": f"month:{json_safe(month_end)}",
                "month": json_safe(month_end),
                "open_option_count": int(counts_by_month.get(month_end, 0)),
                "open_expiring_option_premium": json_safe(projection["open_expiring_option_premium"]),
                "open_expiring_incremental_premium": json_safe(projection["open_expiring_incremental_premium"]),
                "open_expiring_roll_adjusted_premium": json_safe(projection["open_expiring_roll_adjusted_premium"]),
                "projected_month_pnl": json_safe(projection["projected_month_pnl"]),
                "projected_return_roac": None,
                "projected_return_ropc": None,
                "target_pnl": None,
                "projected_remaining_pnl": None,
                "includes_open_premium": bool(projection["includes_open_premium"]),
                "projection_basis": projection["projection_basis"],
            }
        )
    return rows


def build_current_month_performance(
    state,
    *,
    target_return: float = 0.015,
) -> Dict[str, Any]:
    month_end, row = _current_month_row(getattr(state, "monthly_cycles", pd.DataFrame()), getattr(state, "as_of", None))
    if month_end is None:
        return {
            "id": None,
            "month": None,
            "return_roac": None,
            "return_ropc": None,
            "total_realized_pnl": None,
            "realized_month_pnl": None,
            "realized_options_pnl": None,
            "realized_stock_pnl": None,
            "open_expiring_option_premium": None,
            "open_expiring_incremental_premium": None,
            "open_expiring_roll_adjusted_premium": None,
            "includes_open_premium": False,
            "projection_basis": "realized_only",
            "projected_month_pnl": None,
            "projected_return_roac": None,
            "projected_return_ropc": None,
            "target_pnl": None,
            "remaining_pnl": None,
            "projected_remaining_pnl": None,
            "avg_capital": None,
            "peak_capital": None,
            "status": "unavailable",
            "monthly_target_status": "unavailable",
            "days_remaining": None,
        }
    month_row = _mobile_month_row(row, month_end, target_return, getattr(state, "as_of", None), state, include_target_detail=True)
    return {
        "id": month_row["id"],
        "month": month_row["month"],
        "return_roac": month_row["return_roac"],
        "return_ropc": month_row["return_ropc"],
        "total_realized_pnl": month_row["total_realized_pnl"],
        "realized_month_pnl": month_row["realized_month_pnl"],
        "realized_options_pnl": month_row["realized_options_pnl"],
        "realized_stock_pnl": month_row["realized_stock_pnl"],
        "open_expiring_option_premium": month_row["open_expiring_option_premium"],
        "open_expiring_incremental_premium": month_row["open_expiring_incremental_premium"],
        "open_expiring_roll_adjusted_premium": month_row["open_expiring_roll_adjusted_premium"],
        "includes_open_premium": month_row["includes_open_premium"],
        "projection_basis": month_row["projection_basis"],
        "projected_month_pnl": month_row["projected_month_pnl"],
        "projected_return_roac": month_row["projected_return_roac"],
        "projected_return_ropc": month_row["projected_return_ropc"],
        "target_pnl": month_row["target_pnl"],
        "remaining_pnl": month_row["remaining_pnl"],
        "projected_remaining_pnl": month_row["projected_remaining_pnl"],
        "avg_capital": month_row["avg_capital"],
        "peak_capital": month_row["peak_capital"],
        "status": month_row["status"],
        "monthly_target_status": month_row["monthly_target_status"],
        "days_remaining": month_row["days_remaining"],
    }


def build_mobile_monthly_performance(
    state,
    request: Dict[str, Any],
    *,
    target_return: float = 0.015,
    monthly_range: str = "ytd",
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "target_return": json_safe(float(target_return)),
        "target_basis": "avg_capital",
        "return_metric": "return_roac",
        "current_month": build_current_month_performance(state, target_return=target_return),
        "months": build_monthly_performance_rows(
            state,
            target_return=target_return,
            monthly_range=monthly_range,
        ),
        "future_months": build_future_monthly_performance_rows(state, target_return=target_return),
    }


def _yearly_frame(state, include_unrealized: bool) -> pd.DataFrame:
    yearly = getattr(state, "yearly_with_unreal", pd.DataFrame()) if include_unrealized else getattr(state, "yearly", pd.DataFrame())
    if yearly is None or yearly.empty:
        return pd.DataFrame()
    out = yearly.copy()
    if "year" not in out.columns:
        return pd.DataFrame()
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out = out.loc[out["year"].notna()]
    return out.sort_values("year")


def build_yearly_performance_rows(state, *, include_unrealized: bool = False) -> List[Dict[str, Any]]:
    yearly = _yearly_frame(state, include_unrealized)
    if yearly.empty:
        return []

    as_of_ts = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    as_of_year = int(as_of_ts.year) if not pd.isna(as_of_ts) else None
    affected_years = {int(year) for year in getattr(state, "capital_history_affected_years", []) or []}
    unrealized_blocked = bool(getattr(state, "unrealized_blocked", False))
    rows = []
    for raw in yearly.to_dict(orient="records"):
        year = int(raw["year"])
        metrics_suppressed = year in affected_years
        current_year_adjusted = include_unrealized and year == as_of_year and not unrealized_blocked
        total_pnl_including_unrealized = _number(raw.get("total_pnl_incl_unreal")) if current_year_adjusted else None

        def metric(name: str) -> Any:
            return None if metrics_suppressed else _number(raw.get(name))

        rows.append(
            {
                "id": f"year:{year}",
                "year": year,
                "realized_options_pnl": json_safe(_number(raw.get("realized_options_pnl"))),
                "realized_stock_pnl": json_safe(_number(raw.get("realized_stock_pnl"))),
                "dividends": json_safe(_number(raw.get("dividends"))),
                "total_realized_pnl": json_safe(_number(raw.get("total_realized_pnl"))),
                "total_pnl_including_unrealized": json_safe(total_pnl_including_unrealized),
                "avg_capital": json_safe(_number(raw.get("avg_capital"))),
                "peak_capital": json_safe(_number(raw.get("peak_capital"))),
                "roac_year": json_safe(metric("roac_year")),
                "ropc_year": json_safe(metric("ropc_year")),
                "annualized_roac": json_safe(metric("ann_roac")),
                "annualized_ropc": json_safe(metric("ann_ropc")),
                "annualized_twr": json_safe(metric("annualized_return_twr")),
                "annualized_twr_active": json_safe(metric("annualized_return_twr_active")),
                "annualized_twr_unrealized_adjusted": json_safe(
                    metric("annualized_return_twr_unrealized_adjusted") if current_year_adjusted else None
                ),
                "metrics_available": not metrics_suppressed,
                "suppression_reason": "capital_history_incomplete" if metrics_suppressed else None,
            }
        )
    return rows


def build_mobile_yearly_performance(
    state,
    request: Dict[str, Any],
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "years": build_yearly_performance_rows(state, include_unrealized=include_unrealized),
    }


def build_issue_summary(state) -> Dict[str, Any]:
    issues = list(getattr(state, "issues", []) or [])
    price_errors = list(getattr(state, "price_errors", []) or [])
    actionable_issues = [message for message in issues if classify_backend_issue(message).actionable]
    audit_issue_count = len(issues) - len(actionable_issues)
    total_count = len(actionable_issues) + len(price_errors)
    severity = "ok" if total_count == 0 else "warning"
    if bool(getattr(state, "unrealized_blocked", False)):
        severity = "warning"
    return {
        "severity": severity,
        "total_count": total_count,
        "price_issue_count": len(price_errors),
        "parse_issue_count": len(actionable_issues),
        "audit_issue_count": audit_issue_count,
        "top_messages": json_safe([*price_errors, *actionable_issues][:3]),
    }


def _dashboard_preview_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "ticker": row["ticker"],
        "option_type": row["option_type"],
        "strike": row["strike"],
        "current_price": row["current_price"],
        "moneyness": row["moneyness"],
        "moneyness_band": row["moneyness_band"],
        "quantity": row["quantity"],
        "expiration": row["expiration"],
        "days_to_expiration": row["days_to_expiration"],
        "opened": row["opened"],
        "open_price": row["open_price"],
        "covered_status": row["covered_status"],
        "risk_label": row["risk_label"],
    }


def build_mobile_dashboard(
    state,
    request: Dict[str, Any],
    *,
    target_return: float = 0.015,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
    open_option_preview_limit: int = 5,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    mobile_request = build_mobile_request(as_of, include_unrealized, selected_sheets)
    freshness = build_data_freshness(
        state,
        selected_sheets,
        available_sheets=available_sheets,
        source_metadata=source_metadata,
        pipeline_built_at=pipeline_built_at,
        prices_updated_at=prices_updated_at,
    )
    preview = [
        _dashboard_preview_row(row)
        for row in build_open_option_short_rows(state, sort="moneyness_risk", limit=open_option_preview_limit)
    ]

    return {
        "request": mobile_request,
        "data_freshness": freshness,
        "snapshot": build_mobile_snapshot(state, include_unrealized),
        "monthly_target": build_monthly_target(state, target_return=target_return),
        "open_option_short_preview": preview,
        "issue_summary": build_issue_summary(state),
    }


def _id_part(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    safe = json_safe(value)
    return str(safe)


def _inventory_sequences(inventory: pd.DataFrame) -> Dict[Hashable, int]:
    sort_cols = ["ticker", "buy_date", "source", "cost_per_share", "shares"]
    sortable = inventory.copy()
    sortable["_source_index"] = sortable.index
    for col in sort_cols:
        if col not in sortable.columns:
            sortable[col] = None
    sortable = sortable.sort_values(sort_cols + ["_source_index"], kind="mergesort", na_position="last")
    sequence_by_index: Dict[Hashable, int] = {}
    counters: Dict[tuple, int] = defaultdict(int)
    for _, row in sortable.iterrows():
        key = (
            str(row.get("ticker") or ""),
            _date_string(row.get("buy_date")),
            str(row.get("source") or ""),
        )
        sequence_by_index[row["_source_index"]] = counters[key]
        counters[key] += 1
    return sequence_by_index


def build_inventory_rows(state) -> List[Dict[str, Any]]:
    inventory = build_assigned_holdings_frame(getattr(state, "inv_df", pd.DataFrame()))
    if inventory is None or inventory.empty:
        return []

    sequences = _inventory_sequences(inventory)
    stock_prices = getattr(state, "stock_prices", {}) or {}
    rows = []
    for source_index, row in inventory.iterrows():
        ticker = str(row.get("ticker") or "").upper().strip()
        source = str(row.get("source") or "stock_lot")
        buy_date = _date_string(row.get("buy_date"))
        shares = _int_number(row.get("shares"))
        cost_per_share = _number(row.get("cost_per_share"))
        current_price = _number(row.get("current_price"))
        if current_price is None and ticker in stock_prices:
            current_price = _number(stock_prices.get(ticker))
        missing_price = current_price is None

        unrealized_pnl = _number(row.get("unrealized_pnl"))
        if missing_price:
            unrealized_pnl = None
        elif unrealized_pnl is None and shares is not None and cost_per_share is not None and current_price is not None:
            unrealized_pnl = shares * (current_price - cost_per_share)

        sequence = sequences.get(source_index, 0)
        rows.append(
            {
                "id": f"inventory:{ticker}:{_id_part(buy_date)}:{source}:{sequence}",
                "ticker": ticker,
                "buy_date": buy_date,
                "shares": json_safe(shares),
                "cost_per_share": json_safe(cost_per_share),
                "current_price": json_safe(current_price),
                "covered_shares": json_safe(_int_number(row.get("covered_shares")) or 0),
                "covered_strike": json_safe(_number(row.get("covered_strike"))),
                "unrealized_pnl": json_safe(unrealized_pnl),
                "source": source,
                "missing_price": missing_price,
            }
        )

    rows.sort(key=lambda item: (item["ticker"], item["buy_date"] or "", item["source"], item["id"]))
    return rows


def build_mobile_positions(
    state,
    request: Dict[str, Any],
    *,
    include_open_options: bool = True,
    include_inventory: bool = True,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "inventory": build_inventory_rows(state) if include_inventory else [],
        "open_option_shorts": build_open_option_short_rows(state) if include_open_options else [],
    }


def build_mobile_open_option_shorts(
    state,
    request: Dict[str, Any],
    *,
    sort: str = "moneyness_risk",
    limit: Optional[int] = None,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "moneyness_legend": json_safe(MONEYNESS_BANDS),
        "items": build_open_option_short_rows(state, sort=sort, limit=limit),
    }


def _realized_by_ticker(state, year: Optional[int] = None) -> Dict[str, Dict[str, float]]:
    realized = getattr(state, "per_ticker", pd.DataFrame())
    if realized is None or realized.empty:
        return {}
    realized = realized.copy()
    if year is not None and "year" in realized.columns:
        realized = realized.loc[pd.to_numeric(realized["year"], errors="coerce") == int(year)]
    if realized.empty or "ticker" not in realized.columns:
        return {}

    cols = ["options_pnl", "stock_realized_pnl", "combined_realized"]
    for col in cols:
        if col not in realized.columns:
            realized[col] = 0.0
        realized[col] = pd.to_numeric(realized[col], errors="coerce").fillna(0.0)
    grouped = realized.groupby("ticker", as_index=False)[cols].sum()
    return {
        str(row["ticker"]).upper().strip(): {
            "realized_options_pnl": float(row["options_pnl"]),
            "realized_stock_pnl": float(row["stock_realized_pnl"]),
            "combined_realized_pnl": float(row["combined_realized"]),
        }
        for row in grouped.to_dict(orient="records")
    }


def _dividends_by_ticker(state, year: Optional[int] = None) -> Dict[str, float]:
    div_df = getattr(state, "div_df", pd.DataFrame())
    as_of = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    if div_df is None or div_df.empty or "ticker" not in div_df.columns or "cash" not in div_df.columns:
        return {}

    dividends = div_df.copy()
    date_col = "pay_date" if "pay_date" in dividends.columns else "ex_date" if "ex_date" in dividends.columns else None
    if date_col is not None:
        dividends[date_col] = pd.to_datetime(dividends[date_col], errors="coerce")
        dividends = dividends.loc[dividends[date_col].notna()]
        if pd.notna(as_of):
            dividends = dividends.loc[dividends[date_col] <= as_of]
        if year is not None:
            dividends = dividends.loc[dividends[date_col].dt.year == int(year)]

    if dividends.empty:
        return {}
    dividends["ticker"] = dividends["ticker"].astype(str).str.upper().str.strip()
    dividends["cash"] = pd.to_numeric(dividends["cash"], errors="coerce").fillna(0.0)
    grouped = dividends.loc[dividends["ticker"].ne("")].groupby("ticker")["cash"].sum()
    return {str(ticker): float(value) for ticker, value in grouped.items()}


def _totals_by_ticker(state) -> Dict[str, Dict[str, float]]:
    totals = getattr(state, "per_ticker_totals", pd.DataFrame())
    if totals is None or totals.empty or "ticker" not in totals.columns:
        return {}
    records = {}
    for raw in totals.to_dict(orient="records"):
        ticker = str(raw.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        records[ticker] = {
            "realized_options_pnl": _number(raw.get("options_pnl")) or 0.0,
            "realized_stock_pnl": _number(raw.get("stock_realized_pnl")) or 0.0,
            "combined_realized_pnl": _number(raw.get("combined_realized")) or 0.0,
            "unrealized_pnl": _number(raw.get("unrealized_pnl")) or 0.0,
            "total_pnl": _number(raw.get("total_pnl")) or 0.0,
        }
    return records


def _history_by_ticker(state, year: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
    realized = getattr(state, "per_ticker", pd.DataFrame())
    if realized is None or realized.empty or "ticker" not in realized.columns:
        return {}
    realized = realized.copy()
    if year is not None and "year" in realized.columns:
        realized = realized.loc[pd.to_numeric(realized["year"], errors="coerce") == int(year)]
    if realized.empty:
        return {}

    cols = ["options_pnl", "stock_realized_pnl", "combined_realized"]
    for col in cols:
        if col not in realized.columns:
            realized[col] = 0.0
        realized[col] = pd.to_numeric(realized[col], errors="coerce").fillna(0.0)
    realized["year"] = pd.to_numeric(realized.get("year"), errors="coerce")
    realized = realized.loc[realized["year"].notna()]
    grouped = realized.groupby(["year", "ticker"], as_index=False)[cols].sum()

    history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in grouped.sort_values(["ticker", "year"]).to_dict(orient="records"):
        ticker = str(row["ticker"]).upper().strip()
        row_year = int(row["year"])
        history[ticker].append(
            {
                "id": f"year:{row_year}:ticker:{ticker}",
                "year": row_year,
                "realized_options_pnl": json_safe(float(row["options_pnl"])),
                "realized_stock_pnl": json_safe(float(row["stock_realized_pnl"])),
                "combined_realized_pnl": json_safe(float(row["combined_realized"])),
            }
        )
    return history


def _ticker_counts(state) -> tuple[Dict[str, int], Dict[str, int]]:
    open_counts: Dict[str, int] = defaultdict(int)
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is not None and not open_options.empty and "ticker" in open_options.columns:
        for ticker, count in open_options["ticker"].astype(str).str.upper().str.strip().value_counts().items():
            if ticker:
                open_counts[ticker] = int(count)

    inventory_shares: Dict[str, int] = defaultdict(int)
    inventory = build_assigned_holdings_frame(getattr(state, "inv_df", pd.DataFrame()))
    if inventory is not None and not inventory.empty and {"ticker", "shares"}.issubset(inventory.columns):
        shares = inventory.copy()
        shares["shares"] = pd.to_numeric(shares["shares"], errors="coerce").fillna(0)
        for ticker, total in shares.groupby(shares["ticker"].astype(str).str.upper().str.strip())["shares"].sum().items():
            if ticker:
                inventory_shares[ticker] = int(total)
    return dict(open_counts), dict(inventory_shares)


def _ticker_risk_labels(
    ticker: str,
    *,
    open_rows: List[Dict[str, Any]],
    missing_tickers: set[str],
    unrealized_pnl: Optional[float],
    largest_unrealized_loser: Optional[str],
) -> List[str]:
    labels = []
    if ticker in missing_tickers:
        labels.append("Missing price")

    ticker_open_rows = [row for row in open_rows if row["ticker"] == ticker]
    if any(row["moneyness_band"] == "in_the_money" for row in ticker_open_rows):
        labels.append("In the money")
    elif any(row["moneyness_band"] == "at_strike" for row in ticker_open_rows):
        labels.append("At strike")
    elif any(row["moneyness_band"] == "near" for row in ticker_open_rows):
        labels.append("Near strike")

    notional = sum(float(row["notional_at_strike"] or 0.0) for row in ticker_open_rows)
    if notional >= 100000:
        labels.append("High notional exposure")

    if ticker == largest_unrealized_loser and unrealized_pnl is not None and unrealized_pnl < 0:
        labels.append("Largest unrealized loser")

    return labels


def build_ticker_summary_rows(
    state,
    *,
    year: Optional[int] = None,
    include_history: bool = False,
) -> List[Dict[str, Any]]:
    totals_by_ticker = _totals_by_ticker(state)
    realized_by_ticker = _realized_by_ticker(state, year) if year is not None else {}
    dividends_by_ticker = _dividends_by_ticker(state, year)
    history_by_ticker = _history_by_ticker(state, year) if include_history else {}
    open_counts, inventory_shares = _ticker_counts(state)
    open_rows = build_open_option_short_rows(state, sort="ticker")
    stock_prices = getattr(state, "stock_prices", {}) or {}
    missing_tickers = {str(ticker).upper().strip() for ticker in getattr(state, "missing_required_price_tickers", []) or []}
    unrealized_blocked = bool(getattr(state, "unrealized_blocked", False))

    tickers = set(totals_by_ticker) | set(realized_by_ticker) | set(open_counts) | set(inventory_shares)
    tickers.update(str(ticker).upper().strip() for ticker in stock_prices if str(ticker).strip())
    tickers.update(missing_tickers)

    largest_unrealized_loser = None
    if not unrealized_blocked and totals_by_ticker:
        loser_candidates = [
            (ticker, values.get("unrealized_pnl"))
            for ticker, values in totals_by_ticker.items()
            if values.get("unrealized_pnl") is not None
        ]
        if loser_candidates:
            largest_unrealized_loser = min(loser_candidates, key=lambda item: (item[1], item[0]))[0]

    rows = []
    for ticker in sorted(tickers):
        if not ticker:
            continue
        totals = totals_by_ticker.get(ticker, {})
        realized = realized_by_ticker.get(ticker, totals)
        combined_realized = _number(realized.get("combined_realized_pnl")) or 0.0
        unrealized_pnl = None if unrealized_blocked else _number(totals.get("unrealized_pnl"))
        total_pnl = None if unrealized_blocked else _number(totals.get("total_pnl"))
        if total_pnl is None and not unrealized_blocked and unrealized_pnl is not None:
            total_pnl = combined_realized + unrealized_pnl

        current_price = _number(stock_prices.get(ticker))
        rows.append(
            {
                "id": f"ticker:{ticker}",
                "ticker": ticker,
                "current_price": json_safe(current_price),
                "realized_options_pnl": json_safe(_number(realized.get("realized_options_pnl")) or 0.0),
                "realized_stock_pnl": json_safe(_number(realized.get("realized_stock_pnl")) or 0.0),
                "dividends": json_safe(dividends_by_ticker.get(ticker, 0.0)),
                "combined_realized_pnl": json_safe(combined_realized),
                "unrealized_pnl": json_safe(unrealized_pnl),
                "total_pnl": json_safe(total_pnl),
                "open_option_count": int(open_counts.get(ticker, 0)),
                "inventory_share_count": int(inventory_shares.get(ticker, 0)),
                "risk_labels": _ticker_risk_labels(
                    ticker,
                    open_rows=open_rows,
                    missing_tickers=missing_tickers,
                    unrealized_pnl=unrealized_pnl,
                    largest_unrealized_loser=largest_unrealized_loser,
                ),
                "history": history_by_ticker.get(ticker, []) if include_history else [],
            }
        )

    rows.sort(
        key=lambda item: (
            item["total_pnl"] is None,
            -(item["total_pnl"] if item["total_pnl"] is not None else item["combined_realized_pnl"]),
            item["ticker"],
        )
    )
    return rows


def build_mobile_tickers(
    state,
    request: Dict[str, Any],
    *,
    year: Optional[int] = None,
    include_history: bool = False,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "items": build_ticker_summary_rows(state, year=year, include_history=include_history),
    }


_TICKER_PATTERN = re.compile(r"\b[A-Z][A-Z0-9.]{0,7}\b")
_NON_TICKER_TOKENS = {
    "API",
    "ETF",
    "HTTP",
    "ITM",
    "NAN",
    "NONE",
    "PCT",
    "USD",
    "YTD",
}


def _extract_tickers_from_message(message: str) -> List[str]:
    text = str(message or "").upper()
    tickers = []
    prefix_match = re.match(r"\s*([A-Z][A-Z0-9.]{0,7})\s*:", text)
    candidates = [prefix_match.group(1)] if prefix_match else []
    candidates.extend(re.findall(r"\bFOR\s+([A-Z][A-Z0-9.]{0,7})\b", text))
    for label in ("TICKER", "TICKERS"):
        for group in re.findall(rf"\b{label}\s*:\s*([A-Z0-9.,\s]+)", text):
            candidates.extend(_TICKER_PATTERN.findall(group))
    for match in candidates:
        if match in _NON_TICKER_TOKENS:
            continue
        if match not in tickers:
            tickers.append(match)
    return tickers


def _issue_row(
    issue_id: str,
    *,
    category: str,
    severity: str,
    message: str,
    action: Optional[str],
    tickers: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    explicit_tickers = [str(ticker).upper().strip() for ticker in tickers or [] if str(ticker).strip()]
    inferred_tickers = _extract_tickers_from_message(message)
    combined = []
    for ticker in [*explicit_tickers, *inferred_tickers]:
        if ticker and ticker not in combined:
            combined.append(ticker)
    return {
        "id": issue_id,
        "category": category,
        "severity": severity,
        "message": str(message),
        "tickers": combined,
        "action": action,
    }


def build_mobile_issue_rows(state) -> List[Dict[str, Any]]:
    rows = []
    price_summary = getattr(state, "price_summary", {}) or {}
    missing_tickers = [str(ticker).upper().strip() for ticker in getattr(state, "missing_required_price_tickers", []) or []]
    requested = _int_number(price_summary.get("stocks_requested")) or _int_number(price_summary.get("requested")) or 0
    fetched = _int_number(price_summary.get("stocks_fetched")) or _int_number(price_summary.get("fetched")) or 0
    if requested and fetched < requested:
        rows.append(
            _issue_row(
                "price-coverage-stocks",
                category="price",
                severity="warning",
                message=f"Price coverage incomplete: Stocks priced: {fetched}/{requested}",
                tickers=missing_tickers,
                action="refresh_prices",
            )
        )

    for idx, message in enumerate(getattr(state, "price_errors", []) or [], start=1):
        rows.append(
            _issue_row(
                f"current-price-{idx}",
                category="price",
                severity="warning",
                message=message,
                action="refresh_prices",
            )
        )

    for idx, message in enumerate(getattr(state, "issues", []) or [], start=1):
        classification = classify_backend_issue(message)
        issue_id_prefix = "wheel-audit" if classification.category == "wheel_audit" else classification.category.replace("_", "-")
        rows.append(
            _issue_row(
                f"{issue_id_prefix}-{idx}",
                category=classification.category,
                severity=classification.severity,
                message=message,
                action=classification.action,
            )
        )

    for idx, message in enumerate(getattr(state, "historical_price_errors", []) or [], start=1):
        rows.append(
            _issue_row(
                f"historical-price-{idx}",
                category="historical_price",
                severity="warning",
                message=message,
                action="refresh_data",
            )
        )

    for idx, message in enumerate(getattr(state, "dividend_errors", []) or [], start=1):
        rows.append(
            _issue_row(
                f"dividend-{idx}",
                category="dividend",
                severity="warning",
                message=message,
                action="refresh_data",
            )
        )

    for idx, item in enumerate(getattr(state, "capital_history_coverage_issues", []) or [], start=1):
        if isinstance(item, dict):
            ticker = item.get("ticker")
            detail = item.get("reason") or item.get("message") or item
            message = f"Capital history coverage incomplete for {ticker}: {detail}" if ticker else str(detail)
            tickers = [ticker] if ticker else []
        else:
            message = str(item)
            tickers = []
        rows.append(
            _issue_row(
                f"capital-history-{idx}",
                category="capital_history",
                severity="warning",
                message=message,
                tickers=tickers,
                action="refresh_data",
            )
        )

    return rows


def build_mobile_issue_summary(state, issue_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if any(row["severity"] == "error" for row in issue_rows):
        severity = "error"
    elif any(row["severity"] == "warning" for row in issue_rows) or bool(getattr(state, "unrealized_blocked", False)):
        severity = "warning"
    else:
        severity = "ok"
    actionable_count = sum(1 for row in issue_rows if row["severity"] in {"warning", "error"})
    info_count = sum(1 for row in issue_rows if row["severity"] == "info")
    return {
        "severity": severity,
        "total_count": actionable_count,
        "info_count": info_count,
        "unrealized_blocked": bool(getattr(state, "unrealized_blocked", False)),
        "capital_history_incomplete": bool(getattr(state, "capital_history_incomplete", False)),
        "dividend_coverage_complete": bool(getattr(state, "dividend_coverage_complete", True)),
    }


def build_mobile_coverage(state) -> Dict[str, Any]:
    price_summary = getattr(state, "price_summary", {}) or {}
    historical_summary = getattr(state, "historical_price_summary", {}) or {}
    dividend_summary = getattr(state, "dividend_summary", {}) or {}
    return {
        "current_prices": {
            "requested": json_safe(price_summary.get("stocks_requested", price_summary.get("requested", 0))),
            "fetched": json_safe(price_summary.get("stocks_fetched", price_summary.get("fetched", 0))),
            "missing_tickers": json_safe(list(getattr(state, "missing_required_price_tickers", []) or [])),
            "errors": json_safe(list(getattr(state, "price_errors", []) or [])),
        },
        "historical_prices": {
            "requested": json_safe(historical_summary.get("requested", historical_summary.get("stocks_requested", 0))),
            "fetched": json_safe(historical_summary.get("fetched", historical_summary.get("stocks_fetched", 0))),
            "errors": json_safe(list(getattr(state, "historical_price_errors", []) or [])),
        },
        "dividends": {
            "attempted_tickers": json_safe(dividend_summary.get("attempted", dividend_summary.get("attempted_tickers", 0))),
            "failed_tickers": json_safe(dividend_summary.get("failed", dividend_summary.get("failed_tickers", 0))),
            "errors": json_safe(list(getattr(state, "dividend_errors", []) or [])),
        },
    }


def build_mobile_audit_summary(audit_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_category: Dict[str, int] = {}
    by_severity: Dict[str, int] = {}
    for row in audit_rows:
        category = str(row.get("category") or "unknown")
        severity = str(row.get("severity") or "info")
        by_category[category] = by_category.get(category, 0) + 1
        by_severity[severity] = by_severity.get(severity, 0) + 1
    return {
        "total_count": len(audit_rows),
        "by_category": by_category,
        "by_severity": by_severity,
    }


def build_mobile_issues(
    state,
    request: Dict[str, Any],
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    issue_rows = build_mobile_issue_rows(state)
    actionable_rows = [row for row in issue_rows if row.get("severity") in {"warning", "error"}]
    audit_rows = [row for row in issue_rows if row.get("severity") == "info"]
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": build_data_freshness(
            state,
            selected_sheets,
            available_sheets=available_sheets,
            source_metadata=source_metadata,
            pipeline_built_at=pipeline_built_at,
            prices_updated_at=prices_updated_at,
        ),
        "summary": build_mobile_issue_summary(state, issue_rows),
        "issues": actionable_rows,
        "audit_summary": build_mobile_audit_summary(audit_rows),
        "audit_notes": audit_rows,
        "coverage": build_mobile_coverage(state),
    }


def build_mobile_refresh(
    state,
    request: Dict[str, Any],
    *,
    cache_bust: Optional[int] = None,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    pipeline_built_at: Optional[Any] = None,
    prices_updated_at: Optional[Any] = None,
) -> Dict[str, Any]:
    selected_sheets = _request_value(request, "selected_sheets", [])
    include_unrealized = bool(_request_value(request, "include_unrealized", False))
    as_of = _request_value(request, "as_of", getattr(state, "as_of", None))
    freshness = build_data_freshness(
        state,
        selected_sheets,
        available_sheets=available_sheets,
        source_metadata=source_metadata,
        pipeline_built_at=pipeline_built_at,
        prices_updated_at=prices_updated_at,
    )
    missing_tickers = freshness["price_coverage"]["missing_tickers"]
    missing_sheets = [sheet["name"] for sheet in freshness["source_sheets"] if sheet["status"] == "missing"]
    status = "partial" if missing_tickers or missing_sheets else "refreshed"
    return {
        "request": build_mobile_request(as_of, include_unrealized, selected_sheets),
        "data_freshness": freshness,
        "refresh": {
            "status": status,
            "pipeline_refreshed": True,
            "prices_refreshed": freshness["prices_updated_at"] is not None,
            "cache_bust": json_safe(cache_bust),
            "missing_price_count": len(missing_tickers),
            "missing_sheet_count": len(missing_sheets),
            "reload_endpoints": [
                "/v1/mobile/dashboard",
                "/v1/mobile/positions",
                "/v1/mobile/open-option-shorts",
                "/v1/mobile/tickers",
                "/v1/mobile/performance/monthly",
                "/v1/mobile/performance/yearly",
                "/v1/mobile/issues",
            ],
        },
    }


def build_mobile_config(
    available_sheets: Iterable[str],
    prefs: Optional[Dict[str, Any]] = None,
    *,
    default_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    as_of_default: Optional[Any] = None,
    source_kind: str = "local_excel",
    source_name: Optional[str] = None,
    supports_selected_sheets: bool = True,
) -> Dict[str, Any]:
    prefs = prefs or {}
    source_metadata = source_metadata or {}
    available = [str(sheet) for sheet in available_sheets or []]
    configured_defaults = [str(sheet) for sheet in (default_sheets or [])]
    selected = [str(sheet) for sheet in (prefs.get("selected_sheets") or configured_defaults or available) if str(sheet)]
    missing_default_sheets = [sheet for sheet in selected if sheet not in set(available)]
    include_unrealized_default = bool(prefs.get("include_unrealized", True))
    as_of_ts = pd.to_datetime(as_of_default or prefs.get("as_of"), errors="coerce")
    return {
        "available_sheets": available,
        "default_selected_sheets": selected,
        "missing_default_sheets": missing_default_sheets,
        "include_unrealized_default": include_unrealized_default,
        "as_of_default": None if pd.isna(as_of_ts) else json_safe(as_of_ts),
        "source": {
            "kind": source_metadata.get("kind") or source_kind,
            "name": source_metadata.get("name") or source_name,
            "downloaded_at": json_safe(source_metadata.get("source_downloaded_at") or source_metadata.get("downloaded_at")),
            "modified_at": json_safe(source_metadata.get("source_modified_at") or source_metadata.get("modified_at")),
        },
        "capabilities": {
            "supports_price_refresh": True,
            "supports_data_rebuild": True,
            "supports_selected_sheets": bool(supports_selected_sheets),
            "supports_as_of": True,
        },
    }


def moneyness_band(moneyness: Optional[float]) -> Optional[str]:
    if moneyness is None or pd.isna(moneyness):
        return None
    value = float(moneyness)
    if value > 0:
        return "in_the_money"
    if -0.01 <= value <= 0:
        return "at_strike"
    if -0.05 <= value < -0.01:
        return "near"
    if -0.10 <= value < -0.05:
        return "ok"
    return "clear"


def risk_label_for_moneyness(moneyness: Optional[float], missing_price: bool) -> str:
    if missing_price:
        return "Missing price"
    band = moneyness_band(moneyness)
    return {
        "in_the_money": "In the money",
        "at_strike": "At strike",
        "near": "Near strike",
        "ok": "OK",
        "clear": "Clear",
    }.get(band, "Moneyness unavailable")


def _date_string(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    safe = json_safe(value)
    return safe if isinstance(safe, str) else None


def _number(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return json_safe(value)


def _int_number(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _days_to_expiration(expiration: Any, as_of: Any) -> Optional[int]:
    exp_ts = pd.to_datetime(expiration, errors="coerce")
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(exp_ts) or pd.isna(as_of_ts):
        return None
    return int((exp_ts.normalize() - as_of_ts.normalize()).days)


def _covered_status(row: pd.Series, inventory: pd.DataFrame) -> Optional[str]:
    option_type = str(row.get("type") or "")
    if option_type == "Put":
        return "cash_secured"
    if option_type != "Call":
        return None

    if inventory is None or inventory.empty or "ticker" not in inventory.columns or "shares" not in inventory.columns:
        return None
    ticker = str(row.get("ticker") or "")
    quantity = abs(_int_number(row.get("qty")) or 0)
    shares = pd.to_numeric(inventory.loc[inventory["ticker"].astype(str) == ticker, "shares"], errors="coerce").fillna(0).sum()
    return "covered" if shares >= quantity * CONTRACT_MULTIPLIER else "uncovered"


def _lot_sequences(open_options: pd.DataFrame) -> Dict[Hashable, int]:
    sort_cols = ["ticker", "type", "strike", "expiration", "trans_date", "open_price", "qty"]
    sortable = open_options.copy()
    sortable["_source_index"] = sortable.index
    for col in sort_cols:
        if col not in sortable.columns:
            sortable[col] = None
    sortable = sortable.sort_values(sort_cols + ["_source_index"], kind="mergesort", na_position="last")
    sequence_by_index: Dict[Hashable, int] = {}
    counters: Dict[tuple, int] = defaultdict(int)
    for _, row in sortable.iterrows():
        key = (
            str(row.get("ticker") or ""),
            str(row.get("type") or ""),
            _number(row.get("strike")),
            _date_string(row.get("expiration")),
            _date_string(row.get("trans_date")),
        )
        sequence_by_index[row["_source_index"]] = counters[key]
        counters[key] += 1
    return sequence_by_index


def build_open_option_short_rows(state, *, sort: str = "moneyness_risk", limit: Optional[int] = None) -> List[Dict[str, Any]]:
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty:
        return []

    enriched = build_open_option_shorts_frame(open_options, getattr(state, "stock_prices", {}) or {})
    sequences = _lot_sequences(open_options)
    inventory = getattr(state, "inv_df", pd.DataFrame())
    as_of = getattr(state, "as_of", None)

    rows = []
    for source_index, row in enriched.iterrows():
        ticker = str(row.get("ticker") or "").upper().strip()
        option_type = str(row.get("type") or "")
        strike = _number(row.get("strike"))
        quantity = _int_number(row.get("qty"))
        expiration = _date_string(row.get("expiration"))
        opened = _date_string(row.get("trans_date"))
        open_price = _number(row.get("open_price"))
        current_price = _number(row.get("current_price"))
        moneyness = _number(row.get("moneyness_pct"))
        missing_price = current_price is None
        band = moneyness_band(moneyness)
        sequence = sequences.get(source_index, 0)

        notional_at_strike = None
        if strike is not None and quantity is not None:
            notional_at_strike = abs(quantity) * strike * CONTRACT_MULTIPLIER
        premium_collected = None
        if open_price is not None and quantity is not None:
            premium_collected = abs(quantity) * open_price * CONTRACT_MULTIPLIER

        rows.append(
            {
                "id": f"optlot:{ticker}:{option_type}:{strike}:{expiration}:{opened}:{sequence}",
                "ticker": ticker,
                "option_type": option_type,
                "strike": json_safe(strike),
                "current_price": json_safe(current_price),
                "moneyness": json_safe(moneyness),
                "moneyness_band": band,
                "quantity": json_safe(quantity),
                "expiration": expiration,
                "days_to_expiration": json_safe(_days_to_expiration(row.get("expiration"), as_of)),
                "opened": opened,
                "open_price": json_safe(open_price),
                "notional_at_strike": json_safe(notional_at_strike),
                "premium_collected": json_safe(premium_collected),
                "covered_status": _covered_status(row, inventory),
                "risk_label": risk_label_for_moneyness(moneyness, missing_price),
                "missing_price": missing_price,
            }
        )

    if sort == "moneyness_risk":
        rows.sort(
            key=lambda item: (
                _BAND_RISK_ORDER.get(item["moneyness_band"], 5),
                -(item["moneyness"] if item["moneyness"] is not None else -999.0),
                item["expiration"] or "",
                item["ticker"],
                item["id"],
            )
        )
    elif sort == "expiration":
        rows.sort(key=lambda item: (item["expiration"] or "9999-12-31", item["ticker"], item["id"]))
    elif sort == "ticker":
        rows.sort(key=lambda item: (item["ticker"], item["expiration"] or "", item["id"]))
    elif sort == "moneyness_pct":
        rows.sort(key=lambda item: (item["moneyness"] is None, item["moneyness"] or 0.0, item["id"]))
    else:
        raise ValueError(f"Unsupported open option short sort: {sort}")

    return rows[:limit] if limit is not None else rows
