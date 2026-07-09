from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.performance import calculate_option_cycle_unrealized_components
from portfolio_backend.serializers import json_safe


def number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(numeric) else numeric


def month_end(value: Any) -> Optional[pd.Timestamp]:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.to_period("M").to_timestamp("M")


def monthly_target_status(
    current_return: Optional[float],
    target_return: float,
    projection_month: Optional[pd.Timestamp],
    as_of: Any,
) -> str:
    if current_return is None or projection_month is None:
        return "unavailable"
    as_of_timestamp = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_timestamp) or projection_month.normalize() <= as_of_timestamp.normalize():
        return "beat" if current_return >= target_return else "miss"
    return "beat" if current_return >= target_return else "below_target"


def current_calendar_month_row(
    monthly_cycles: pd.DataFrame,
    as_of: Any,
) -> tuple[Optional[pd.Timestamp], Dict[str, Any]]:
    as_of_timestamp = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_timestamp):
        return None, {}
    projection_month = as_of_timestamp.to_period("M").to_timestamp("M")
    if monthly_cycles is None or monthly_cycles.empty:
        return projection_month, {}
    monthly = monthly_cycles.copy()
    monthly.index = pd.to_datetime(monthly.index, errors="coerce")
    monthly = monthly.loc[monthly.index.notna()]
    if projection_month not in monthly.index:
        return projection_month, {}
    return projection_month, monthly.loc[projection_month].to_dict()


def sum_open_premium_for_month(
    open_options: pd.DataFrame,
    projection_month: pd.Timestamp,
    *,
    price_column: str = "open_price",
) -> float:
    required_columns = {"expiration", "qty", price_column}
    if not required_columns.issubset(open_options.columns):
        return 0.0

    options = open_options.copy()
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    options = options.loc[options["expiration"].notna()]
    expiring = options.loc[
        options["expiration"].dt.to_period("M") == projection_month.to_period("M")
    ]
    if expiring.empty:
        return 0.0

    prices = pd.to_numeric(expiring[price_column], errors="coerce")
    quantities = pd.to_numeric(expiring["qty"], errors="coerce").abs()
    premiums = (prices * quantities * CONTRACT_MULTIPLIER).dropna()
    return float(premiums.sum()) if not premiums.empty else 0.0


def open_incremental_premium(state: Any, projection_month: Optional[pd.Timestamp]) -> Optional[float]:
    if projection_month is None:
        return None
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty:
        return 0.0
    return sum_open_premium_for_month(open_options, projection_month)


def monthly_projection_values(
    state: Any,
    row: Dict[str, Any],
    projection_month: Optional[pd.Timestamp],
    target_return: float,
) -> Dict[str, Any]:
    avg_capital = number(row.get("avg_capital"))
    peak_capital = number(row.get("peak_capital"))
    realized_month_pnl = number(row.get("total_realized_pnl"))
    as_of_timestamp = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    as_of_month = None if pd.isna(as_of_timestamp) else as_of_timestamp.to_period("M").to_timestamp("M")
    month_is_closed = (
        projection_month is not None
        and as_of_month is not None
        and projection_month < as_of_month
    )
    open_premium = 0.0 if month_is_closed else open_incremental_premium(state, projection_month)

    projected_month_pnl = None
    if realized_month_pnl is not None and open_premium is not None:
        projected_month_pnl = realized_month_pnl + open_premium

    projected_return_roac = (
        projected_month_pnl / avg_capital
        if projected_month_pnl is not None and avg_capital not in (None, 0)
        else None
    )
    projected_return_ropc = (
        projected_month_pnl / peak_capital
        if projected_month_pnl is not None and peak_capital not in (None, 0)
        else None
    )
    target_pnl = avg_capital * target_return if avg_capital is not None else None
    projected_remaining_pnl = (
        max(target_pnl - projected_month_pnl, 0.0)
        if target_pnl is not None and projected_month_pnl is not None
        else None
    )
    includes_open_premium = bool((open_premium or 0.0) > 0.0)

    return {
        "realized_month_pnl": realized_month_pnl,
        "open_premium_collected": open_premium,
        "includes_open_premium": includes_open_premium,
        "projection_basis": "realized_plus_open_premium" if includes_open_premium else "realized_only",
        "projected_month_pnl": projected_month_pnl,
        "projected_return_roac": projected_return_roac,
        "projected_return_ropc": projected_return_ropc,
        "target_pnl": target_pnl,
        "projected_remaining_pnl": projected_remaining_pnl,
    }


def latest_monthly_capital(state: Any, field: str) -> Optional[float]:
    monthly = getattr(state, "monthly_cycles", pd.DataFrame())
    if monthly is None or monthly.empty or field not in monthly.columns:
        return None
    frame = monthly.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[frame.index.notna()].sort_index()
    values = pd.to_numeric(frame[field], errors="coerce").dropna()
    return None if values.empty else float(values.iloc[-1])


def monthly_row_for_projection(state: Any, projection_month: pd.Timestamp) -> Dict[str, Any]:
    monthly = getattr(state, "monthly_cycles", pd.DataFrame())
    if monthly is not None and not monthly.empty:
        frame = monthly.copy()
        frame.index = pd.to_datetime(frame.index, errors="coerce")
        frame = frame.loc[frame.index.notna()]
        matched = frame.loc[frame.index.to_period("M") == projection_month.to_period("M")]
        if not matched.empty:
            return matched.iloc[-1].to_dict()

    avg_capital = latest_monthly_capital(state, "avg_capital")
    peak_capital = latest_monthly_capital(state, "peak_capital")
    return {
        "total_realized_pnl": 0.0,
        "realized_options_pnl": 0.0,
        "realized_stock_pnl": 0.0,
        "dividends": 0.0,
        "avg_capital": avg_capital,
        "peak_capital": peak_capital or avg_capital,
        "roac": None,
        "ropc": None,
    }


def open_options_for_month(state: Any, projection_month: Optional[pd.Timestamp]) -> pd.DataFrame:
    open_options = getattr(state, "open_options", pd.DataFrame())
    if (
        projection_month is None
        or open_options is None
        or open_options.empty
        or "expiration" not in open_options.columns
    ):
        return pd.DataFrame()
    options = open_options.copy()
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    return options.loc[
        options["expiration"].notna()
        & (options["expiration"].dt.to_period("M") == projection_month.to_period("M"))
    ]


def active_cycle_month_end(state: Any) -> Optional[pd.Timestamp]:
    as_of_timestamp = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    calendar_month = None if pd.isna(as_of_timestamp) else as_of_timestamp.to_period("M").to_timestamp("M")
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty or "expiration" not in open_options.columns:
        return calendar_month

    expiries = pd.to_datetime(open_options["expiration"], errors="coerce").dropna()
    if expiries.empty:
        return calendar_month
    future_expiries = expiries
    if not pd.isna(as_of_timestamp):
        future_expiries = expiries.loc[expiries >= as_of_timestamp.normalize()]
    selected = future_expiries.min() if not future_expiries.empty else expiries.max()
    return selected.to_period("M").to_timestamp("M")


def option_month_counts(options: pd.DataFrame) -> tuple[int, int]:
    if options is None or options.empty:
        return 0, 0
    option_count = int(len(options))
    if "ticker" not in options.columns:
        return option_count, option_count
    ticker_count = int(
        options["ticker"].astype(str).str.upper().str.strip().replace("", pd.NA).dropna().nunique()
    )
    return ticker_count, option_count


def option_month_expiries(options: pd.DataFrame) -> list[pd.Timestamp]:
    if options is None or options.empty or "expiration" not in options.columns:
        return []
    expiries = pd.to_datetime(options["expiration"], errors="coerce").dropna()
    return sorted({pd.Timestamp(expiry).normalize() for expiry in expiries})


def option_month_dte_range(
    expiries: list[pd.Timestamp],
    as_of: Any,
) -> tuple[Optional[int], Optional[int]]:
    as_of_timestamp = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_timestamp) or not expiries:
        return None, None
    dtes = [max(int((expiry.normalize() - as_of_timestamp.normalize()).days), 0) for expiry in expiries]
    return min(dtes), max(dtes)


def put_assignment_exposure(
    options: pd.DataFrame,
    stock_prices: Dict[str, Any],
    *,
    only_itm: bool = False,
) -> float:
    if options is None or options.empty or not {"ticker", "type", "strike", "qty"}.issubset(options.columns):
        return 0.0
    total = 0.0
    for _, row in options.iterrows():
        if str(row.get("type") or "").lower().strip() != "put":
            continue
        strike = number(row.get("strike"))
        quantity = abs(number(row.get("qty")) or 0.0)
        if strike is None or quantity == 0:
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        current = number(stock_prices.get(ticker))
        if only_itm and (current is None or current >= strike):
            continue
        total += strike * quantity * CONTRACT_MULTIPLIER
    return float(total)


def covered_call_upside_foregone(options: pd.DataFrame, stock_prices: Dict[str, Any]) -> float:
    if options is None or options.empty or not {"ticker", "type", "strike", "qty"}.issubset(options.columns):
        return 0.0
    total = 0.0
    for _, row in options.iterrows():
        if str(row.get("type") or "").lower().strip() != "call":
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        strike = number(row.get("strike"))
        current = number(stock_prices.get(ticker))
        quantity = abs(number(row.get("qty")) or 0.0)
        if strike is None or current is None or quantity == 0:
            continue
        total += min(strike - current, 0.0) * quantity * CONTRACT_MULTIPLIER
    return float(total)


def build_cycle_projection(
    state: Any,
    projection_month: Optional[pd.Timestamp],
    *,
    target_return: float,
    target_floor: Optional[float] = None,
) -> Dict[str, Any]:
    if projection_month is None:
        return {}

    projection_month = projection_month.to_period("M").to_timestamp("M")
    row = monthly_row_for_projection(state, projection_month)
    projection = monthly_projection_values(state, row, projection_month, target_return)
    options = open_options_for_month(state, projection_month)
    ticker_count, contract_count = option_month_counts(options)
    expiries = option_month_expiries(options)
    min_dte, max_dte = option_month_dte_range(expiries, getattr(state, "as_of", None))
    stock_prices = getattr(state, "stock_prices", {}) or {}
    cycle_unrealized = calculate_option_cycle_unrealized_components(
        options,
        getattr(state, "inv_df", pd.DataFrame()),
        stock_prices,
    )

    stock_unrealized_pnl = None
    if not bool(getattr(state, "unrealized_blocked", False)):
        stock_unrealized_pnl = cycle_unrealized["stock_unrealized"]
    itm_put_assignment_pnl = cycle_unrealized["put_gap"]
    itm_call_stock_pnl = cycle_unrealized.get("itm_call_stock_pnl", 0.0)

    projected_cycle_pnl = projection["projected_month_pnl"]
    if projected_cycle_pnl is not None and itm_put_assignment_pnl is not None:
        projected_cycle_pnl += itm_put_assignment_pnl
    if projected_cycle_pnl is not None and stock_unrealized_pnl is not None:
        projected_cycle_pnl += stock_unrealized_pnl

    target_base = (
        projection["target_pnl"] / target_return
        if projection["target_pnl"] is not None and target_return
        else None
    )
    peak_capital = number(row.get("peak_capital"))
    projected_return_roac = (
        projected_cycle_pnl / target_base
        if projected_cycle_pnl is not None and target_base not in (None, 0)
        else None
    )
    projected_return_ropc = (
        projected_cycle_pnl / peak_capital
        if projected_cycle_pnl is not None and peak_capital not in (None, 0)
        else None
    )
    target_pnl = target_base * target_return if target_base is not None else None
    remaining_to_target = (
        max(target_pnl - projected_cycle_pnl, 0.0)
        if target_pnl is not None and projected_cycle_pnl is not None
        else None
    )

    portfolio_options = getattr(state, "open_options", pd.DataFrame())
    return {
        "cycle": projection_month.strftime("%Y-%m"),
        "month": json_safe(projection_month),
        "cycle_label": projection_month.strftime("%B %Y"),
        "expiry_dates": [expiry.date().isoformat() for expiry in expiries],
        "min_dte": json_safe(min_dte),
        "max_dte": json_safe(max_dte),
        "open_ticker_count": ticker_count,
        "open_contract_count": contract_count,
        "realized_cycle_pnl": json_safe(projection["realized_month_pnl"]),
        "open_premium_collected": json_safe(projection["open_premium_collected"]),
        "stock_unrealized_pnl": json_safe(stock_unrealized_pnl),
        "itm_call_stock_pnl": json_safe(itm_call_stock_pnl),
        "projected_cycle_pnl": json_safe(projected_cycle_pnl),
        "projected_month_pnl": json_safe(projected_cycle_pnl),
        "target_return": json_safe(float(target_return)),
        "target_floor": json_safe(float(target_floor)) if target_floor is not None else None,
        "target_base": json_safe(target_base),
        "target_basis": "avg_capital" if target_base is not None else None,
        "target_pnl": json_safe(target_pnl),
        "remaining_to_target": json_safe(remaining_to_target),
        "projected_remaining_pnl": json_safe(remaining_to_target),
        "projected_return_roac": json_safe(projected_return_roac),
        "projected_return_ropc": json_safe(projected_return_ropc),
        "monthly_target_status": monthly_target_status(
            projected_return_roac,
            target_return,
            projection_month,
            getattr(state, "as_of", None),
        ),
        "projection_basis": projection["projection_basis"],
        "includes_open_premium": bool(projection["includes_open_premium"]),
        "includes_stock_unrealized": stock_unrealized_pnl is not None,
        "portfolio_put_exposure": json_safe(put_assignment_exposure(portfolio_options, stock_prices)),
        "portfolio_itm_put_exposure": json_safe(
            put_assignment_exposure(portfolio_options, stock_prices, only_itm=True)
        ),
        "cycle_put_exposure": json_safe(put_assignment_exposure(options, stock_prices)),
        "cycle_itm_put_exposure": json_safe(put_assignment_exposure(options, stock_prices, only_itm=True)),
        "itm_put_unrealized_loss": json_safe(itm_put_assignment_pnl),
        "covered_call_upside_foregone": json_safe(covered_call_upside_foregone(options, stock_prices)),
    }


def build_active_cycle_projection(
    state: Any,
    *,
    target_return: float = 0.015,
    target_floor: Optional[float] = None,
) -> Dict[str, Any]:
    return build_cycle_projection(
        state,
        active_cycle_month_end(state),
        target_return=target_return,
        target_floor=target_floor,
    )


def open_option_counts_by_expiration_month(state: Any) -> Dict[pd.Timestamp, int]:
    open_options = getattr(state, "open_options", pd.DataFrame())
    if open_options is None or open_options.empty or "expiration" not in open_options.columns:
        return {}
    options = open_options.copy()
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    options = options.loc[options["expiration"].notna()]
    if options.empty:
        return {}
    options["month"] = options["expiration"].dt.to_period("M").dt.to_timestamp("M")
    return {projection_month: int(count) for projection_month, count in options.groupby("month").size().items()}


def build_future_cycle_projections(
    state: Any,
    *,
    target_return: float = 0.015,
    target_floor: Optional[float] = None,
) -> list[Dict[str, Any]]:
    as_of_timestamp = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    if pd.isna(as_of_timestamp):
        return []
    current_month = as_of_timestamp.to_period("M").to_timestamp("M")
    return [
        build_cycle_projection(
            state,
            projection_month,
            target_return=target_return,
            target_floor=target_floor,
        )
        for projection_month in sorted(
            month for month in open_option_counts_by_expiration_month(state) if month > current_month
        )
    ]


def build_monthly_target(
    state: Any,
    *,
    target_return: float = 0.015,
    target_floor: Optional[float] = None,
) -> Dict[str, Any]:
    calendar_month, calendar_row = current_calendar_month_row(
        getattr(state, "monthly_cycles", pd.DataFrame()),
        getattr(state, "as_of", None),
    )
    active_cycle = build_active_cycle_projection(
        state,
        target_return=target_return,
        target_floor=target_floor,
    )
    active_month = month_end(active_cycle.get("month")) if active_cycle else None
    projection_month = active_month or calendar_month
    row = monthly_row_for_projection(state, projection_month) if projection_month is not None else calendar_row
    avg_capital = number(row.get("avg_capital"))
    current_return = number(row.get("roac"))
    current_pnl = number(row.get("total_realized_pnl"))
    projection = monthly_projection_values(state, row, projection_month, target_return)
    target_pnl = avg_capital * target_return if avg_capital is not None else None
    remaining_pnl = (
        max(target_pnl - current_pnl, 0.0)
        if target_pnl is not None and current_pnl is not None
        else None
    )

    as_of_timestamp = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    days_remaining = None
    if projection_month is not None and not pd.isna(as_of_timestamp):
        days_remaining = max(int((projection_month.normalize() - as_of_timestamp.normalize()).days), 0)

    active_projected_pnl = active_cycle.get("projected_cycle_pnl") if active_cycle else None
    active_projected_return = active_cycle.get("projected_return_roac") if active_cycle else None
    active_projected_ropc = active_cycle.get("projected_return_ropc") if active_cycle else None
    active_target_pnl = active_cycle.get("target_pnl") if active_cycle else None
    active_remaining = active_cycle.get("remaining_to_target") if active_cycle else None

    return {
        "month": json_safe(projection_month),
        "target_basis": "avg_capital",
        "target_return": json_safe(float(target_return)),
        "current_return": json_safe(current_return),
        "current_return_metric": "return_roac",
        "current_pnl": json_safe(current_pnl),
        "target_pnl": json_safe(active_target_pnl if active_target_pnl is not None else target_pnl),
        "remaining_pnl": json_safe(remaining_pnl),
        "status": monthly_target_status(
            current_return,
            target_return,
            projection_month,
            getattr(state, "as_of", None),
        ),
        "realized_month_pnl": json_safe(projection["realized_month_pnl"]),
        "realized_options_pnl": json_safe(number(row.get("realized_options_pnl"))),
        "realized_stock_pnl": json_safe(number(row.get("realized_stock_pnl"))),
        "open_premium_collected": json_safe(projection["open_premium_collected"]),
        "projected_month_pnl": json_safe(
            active_projected_pnl if active_projected_pnl is not None else projection["projected_month_pnl"]
        ),
        "projected_return_roac": json_safe(
            active_projected_return if active_projected_return is not None else projection["projected_return_roac"]
        ),
        "projected_return_ropc": json_safe(
            active_projected_ropc if active_projected_ropc is not None else projection["projected_return_ropc"]
        ),
        "projected_remaining_pnl": json_safe(
            active_remaining if active_remaining is not None else projection["projected_remaining_pnl"]
        ),
        "monthly_target_status": monthly_target_status(
            number(active_projected_return)
            if active_projected_return is not None
            else projection["projected_return_roac"],
            target_return,
            active_month if active_cycle else projection_month,
            getattr(state, "as_of", None),
        ),
        "includes_open_premium": bool(projection["includes_open_premium"]),
        "projection_basis": projection["projection_basis"],
        "cycle_projection": active_cycle,
        "days_remaining": json_safe(days_remaining),
    }
