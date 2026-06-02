from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional

import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.serializers import json_safe


@dataclass(frozen=True)
class CycleProjection:
    cycle: Optional[str]
    cycle_label: str
    expiry_dates: list[str]
    min_dte: Optional[float]
    max_dte: Optional[float]
    open_ticker_count: int
    open_contract_count: int
    realized_cycle_pnl: float
    open_premium_collected: float
    stock_unrealized_pnl: Optional[float]
    projected_cycle_pnl: float
    target_return: float
    target_floor: float
    target_base: Optional[float]
    target_basis: Optional[str]
    target_pnl: Optional[float]
    remaining_to_target: Optional[float]
    projected_return_roac: Optional[float]
    portfolio_put_exposure: float
    portfolio_itm_put_exposure: float
    cycle_put_exposure: float
    cycle_itm_put_exposure: float
    near_strike_put_exposure: float
    itm_put_unrealized_loss: float
    covered_call_upside_foregone: float
    source: str = "canonical_cycle_projection"

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle": self.cycle,
            "cycle_label": self.cycle_label,
            "expiry_dates": self.expiry_dates,
            "min_dte": json_safe(self.min_dte),
            "max_dte": json_safe(self.max_dte),
            "open_ticker_count": self.open_ticker_count,
            "open_contract_count": self.open_contract_count,
            "realized_cycle_pnl": json_safe(self.realized_cycle_pnl),
            "premium_component": json_safe(self.open_premium_collected),
            "open_premium_collected": json_safe(self.open_premium_collected),
            "stock_unrealized_pnl": json_safe(self.stock_unrealized_pnl),
            "itm_put_unrealized_loss": json_safe(self.itm_put_unrealized_loss),
            "covered_call_upside_foregone": json_safe(self.covered_call_upside_foregone),
            "projected_pnl": json_safe(self.projected_cycle_pnl),
            "projected_cycle_pnl": json_safe(self.projected_cycle_pnl),
            "target_return": json_safe(self.target_return),
            "target_floor": json_safe(self.target_floor),
            "target_base": json_safe(self.target_base),
            "target_basis": self.target_basis,
            "target_pnl": json_safe(self.target_pnl),
            "remaining_to_target": json_safe(self.remaining_to_target),
            "projected_return_roac": json_safe(self.projected_return_roac),
            "portfolio_put_exposure": json_safe(self.portfolio_put_exposure),
            "portfolio_itm_put_exposure": json_safe(self.portfolio_itm_put_exposure),
            "cycle_put_exposure": json_safe(self.cycle_put_exposure),
            "cycle_itm_put_exposure": json_safe(self.cycle_itm_put_exposure),
            "near_strike_put_exposure": json_safe(self.near_strike_put_exposure),
            "source": self.source,
        }


def number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def month_label(year_month: Optional[tuple[int, int]]) -> str:
    if not year_month:
        return "Active cycle"
    return pd.Timestamp(year=year_month[0], month=year_month[1], day=1).strftime("%B %Y")


def build_state_cycle_projection(
    state: Any,
    *,
    year_month: Optional[tuple[int, int]] = None,
    target_return: float = 0.015,
    target_floor: float = 0.01,
    include_stock_unrealized: bool = False,
) -> CycleProjection:
    as_of = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    open_rows = _state_open_option_rows(state)
    dated_rows = [(row, pd.to_datetime(row.get("expiration"), errors="coerce")) for row in open_rows]
    dated_rows = [(row, expiry) for row, expiry in dated_rows if not pd.isna(expiry)]
    if year_month is None:
        future_rows = [(row, expiry) for row, expiry in dated_rows if pd.isna(as_of) or expiry.normalize() >= as_of.normalize()]
        if not future_rows:
            future_rows = dated_rows
        year_month = min(((expiry.year, expiry.month) for _row, expiry in future_rows), default=None)

    cycle_rows = [
        row
        for row, expiry in dated_rows
        if year_month is not None and (expiry.year, expiry.month) == year_month
    ]
    return _build_projection(
        cycle_rows=cycle_rows,
        portfolio_rows=open_rows,
        year_month=year_month,
        monthly_rows=_state_monthly_rows(state),
        target_return=target_return,
        target_floor=target_floor,
        target_pnl_hint=None,
        stock_unrealized_pnl=_state_stock_unrealized(state) if include_stock_unrealized else None,
        source="state_cycle_projection",
    )


def build_state_future_cycle_projections(
    state: Any,
    *,
    target_return: float = 0.015,
    target_floor: float = 0.01,
    include_current: bool = False,
) -> list[dict[str, Any]]:
    as_of = pd.to_datetime(getattr(state, "as_of", None), errors="coerce")
    current_month = None if pd.isna(as_of) else (as_of.year, as_of.month)
    months = sorted(
        {
            (expiry.year, expiry.month)
            for _row, expiry in [
                (row, pd.to_datetime(row.get("expiration"), errors="coerce"))
            for row in _state_open_option_rows(state)
            ]
            if not pd.isna(expiry)
            and (
                current_month is None
                or (expiry.year, expiry.month) > current_month
                or (include_current and (expiry.year, expiry.month) == current_month)
            )
        }
    )
    return [
        build_state_cycle_projection(
            state,
            year_month=month,
            target_return=target_return,
            target_floor=target_floor,
            include_stock_unrealized=month == current_month,
        ).to_dict()
        for month in months
    ]


def build_payload_cycle_projection(payload: dict[str, Any]) -> dict[str, Any]:
    open_rows = list((payload.get("positions") or {}).get("open_option_shorts") or [])
    today = _payload_as_of_date(payload)
    dated_rows = [(row, pd.to_datetime(row.get("expiration") or row.get("expiry"), errors="coerce")) for row in open_rows]
    dated_rows = [(row, expiry) for row, expiry in dated_rows if not pd.isna(expiry)]
    future_rows = [(row, expiry) for row, expiry in dated_rows if expiry.date() >= today]
    if not future_rows:
        future_rows = dated_rows
    year_month = min(((expiry.year, expiry.month) for _row, expiry in future_rows), default=None)
    cycle_rows = [
        row
        for row, expiry in future_rows
        if year_month is not None and (expiry.year, expiry.month) == year_month
    ]
    monthly_target = (payload.get("dashboard") or {}).get("monthly_target") or {}
    target_return = number(monthly_target.get("target_return")) or number((payload.get("web") or {}).get("target_return")) or 0.02
    target_floor = number((payload.get("web") or {}).get("target_floor")) or number(monthly_target.get("target_floor")) or 0.01
    snapshot = (payload.get("dashboard") or {}).get("snapshot") or {}
    projection = _build_projection(
        cycle_rows=cycle_rows,
        portfolio_rows=open_rows,
        year_month=year_month,
        monthly_rows=_payload_monthly_rows(payload),
        target_return=target_return,
        target_floor=target_floor,
        target_pnl_hint=number(monthly_target.get("target_pnl")),
        stock_unrealized_pnl=number(snapshot.get("current_stock_unrealized_pnl")) or 0.0,
        source="payload_cycle_projection",
    )
    return projection.to_dict()


def _build_projection(
    *,
    cycle_rows: list[dict[str, Any]],
    portfolio_rows: list[dict[str, Any]],
    year_month: Optional[tuple[int, int]],
    monthly_rows: list[dict[str, Any]],
    target_return: float,
    target_floor: float,
    target_pnl_hint: Optional[float],
    stock_unrealized_pnl: Optional[float],
    source: str,
) -> CycleProjection:
    cycle_month_row = _month_row(monthly_rows, year_month)
    realized_cycle_pnl = _first_number(
        [
            cycle_month_row.get("realized_month_pnl") if cycle_month_row else None,
            cycle_month_row.get("total_realized_pnl") if cycle_month_row else None,
        ]
    )
    if realized_cycle_pnl is None:
        realized_cycle_pnl = 0.0

    premium = _first_number(
        [
            cycle_month_row.get("open_premium_collected") if cycle_month_row else None,
            cycle_month_row.get("open_expiring_incremental_premium") if cycle_month_row else None,
        ]
    )
    if premium is None:
        use_effective_premium = not (cycle_month_row and realized_cycle_pnl)
        premium = sum(_open_option_premium(row, effective=use_effective_premium) for row in cycle_rows)

    projected_hint = number(cycle_month_row.get("projected_month_pnl")) if cycle_month_row else None
    if projected_hint is None:
        projected_pnl = realized_cycle_pnl + premium
    else:
        projected_pnl = projected_hint
    if stock_unrealized_pnl is not None:
        projected_pnl += stock_unrealized_pnl

    target_base = _target_base(monthly_rows, cycle_month_row, target_pnl_hint, target_return)
    target_pnl = target_base * target_return if target_base is not None else target_pnl_hint
    projected_return = projected_pnl / target_base if target_base not in (None, 0) else None
    remaining = max(target_pnl - projected_pnl, 0.0) if target_pnl is not None else None

    cycle_puts = [row for row in cycle_rows if _option_type(row) == "put"]
    portfolio_puts = [row for row in portfolio_rows if _option_type(row) == "put"]
    expiries = sorted(
        {
            expiry.date().isoformat()
            for expiry in [pd.to_datetime(row.get("expiration") or row.get("expiry"), errors="coerce") for row in cycle_rows]
            if not pd.isna(expiry)
        }
    )
    dtes = [number(row.get("days_to_expiration") or row.get("dte")) for row in cycle_rows]
    dtes = [dte for dte in dtes if dte is not None]
    return CycleProjection(
        cycle=f"{year_month[0]:04d}-{year_month[1]:02d}" if year_month else None,
        cycle_label=month_label(year_month),
        expiry_dates=expiries,
        min_dte=min(dtes) if dtes else None,
        max_dte=max(dtes) if dtes else None,
        open_ticker_count=len({str(row.get("ticker") or row.get("symbol") or "").upper() for row in cycle_rows if row.get("ticker") or row.get("symbol")}),
        open_contract_count=int(sum(abs(number(row.get("quantity") if row.get("quantity") is not None else row.get("qty")) or 0) for row in cycle_rows)),
        realized_cycle_pnl=float(realized_cycle_pnl),
        open_premium_collected=float(premium),
        stock_unrealized_pnl=stock_unrealized_pnl,
        projected_cycle_pnl=float(projected_pnl),
        target_return=float(target_return),
        target_floor=float(target_floor),
        target_base=target_base,
        target_basis="avg_capital" if target_base is not None else None,
        target_pnl=target_pnl,
        remaining_to_target=remaining,
        projected_return_roac=projected_return,
        portfolio_put_exposure=_put_exposure(portfolio_puts),
        portfolio_itm_put_exposure=_put_exposure([row for row in portfolio_puts if _put_assignment_gap(row) < 0]),
        cycle_put_exposure=_put_exposure(cycle_puts),
        cycle_itm_put_exposure=_put_exposure([row for row in cycle_puts if _put_assignment_gap(row) < 0]),
        near_strike_put_exposure=_put_exposure([row for row in cycle_puts if _is_near_strike_put(row)]),
        itm_put_unrealized_loss=sum(gap for gap in (_put_assignment_gap(row) for row in cycle_puts) if gap < 0),
        covered_call_upside_foregone=sum(gap for gap in (_call_upside_foregone(row) for row in cycle_rows if _option_type(row) == "call") if gap < 0),
        source=source,
    )


def _state_open_option_rows(state: Any) -> list[dict[str, Any]]:
    options = getattr(state, "open_options", pd.DataFrame())
    if options is None or getattr(options, "empty", True):
        return []
    rows = []
    for row in options.to_dict(orient="records"):
        ticker = str(row.get("ticker") or "").upper()
        row["current_price"] = number((getattr(state, "stock_prices", {}) or {}).get(ticker))
        rows.append(row)
    return rows


def _state_monthly_rows(state: Any) -> list[dict[str, Any]]:
    monthly = getattr(state, "monthly_cycles", pd.DataFrame())
    if monthly is None or getattr(monthly, "empty", True):
        return []
    frame = monthly.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[frame.index.notna()]
    out = []
    for month, row in frame.sort_index().iterrows():
        values = row.to_dict()
        values["month"] = month
        out.append(values)
    return out


def _state_stock_unrealized(state: Any) -> float:
    if bool(getattr(state, "unrealized_blocked", False)):
        return 0.0
    return number(getattr(state, "stock_unreal", None)) or 0.0


def _payload_monthly_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    monthly = payload.get("monthly") or {}
    return list(monthly.get("future_months") or []) + list(monthly.get("months") or [])


def _payload_as_of_date(payload: dict[str, Any]):
    raw = ((payload.get("dashboard") or {}).get("request") or {}).get("as_of") or payload.get("generated_at")
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return pd.Timestamp.today().date()
    return parsed.date()


def _month_row(rows: Iterable[dict[str, Any]], year_month: Optional[tuple[int, int]]) -> dict[str, Any]:
    if not year_month:
        return {}
    for row in rows:
        parsed = pd.to_datetime(row.get("month") or str(row.get("id") or "").replace("month:", ""), errors="coerce")
        if not pd.isna(parsed) and (parsed.year, parsed.month) == year_month:
            return row
    return {}


def _target_base(
    rows: list[dict[str, Any]],
    cycle_month_row: dict[str, Any],
    target_pnl_hint: Optional[float],
    target_return: float,
) -> Optional[float]:
    direct = _first_number([
        cycle_month_row.get("avg_capital") if cycle_month_row else None,
        cycle_month_row.get("average_capital") if cycle_month_row else None,
    ])
    if direct is not None:
        return direct
    if target_pnl_hint is not None and target_return:
        return target_pnl_hint / target_return
    dated = []
    for row in rows:
        parsed = pd.to_datetime(row.get("month") or str(row.get("id") or "").replace("month:", ""), errors="coerce")
        capital = _first_number([row.get("avg_capital"), row.get("average_capital"), row.get("peak_capital")])
        if not pd.isna(parsed) and capital is not None:
            dated.append((parsed, capital))
    return max(dated, key=lambda item: item[0])[1] if dated else None


def _first_number(values: Iterable[Any]) -> Optional[float]:
    for value in values:
        parsed = number(value)
        if parsed is not None:
            return parsed
    return None


def _option_type(row: dict[str, Any]) -> str:
    return str(row.get("option_type") or row.get("type") or row.get("put_call") or "").lower()


def _open_option_premium(row: dict[str, Any], *, effective: bool = True) -> float:
    explicit_values = []
    if effective:
        explicit_values.extend([row.get("display_premium_collected"), row.get("roll_adjusted_premium_collected")])
    explicit_values.append(row.get("premium_collected"))
    explicit = _first_number(explicit_values)
    if explicit is not None:
        return explicit
    price_values = []
    if effective:
        price_values.append(row.get("roll_adjusted_open_price"))
    price_values.extend([row.get("open_price"), row.get("trade_price")])
    price = _first_number(price_values)
    qty = abs(number(row.get("quantity") if row.get("quantity") is not None else row.get("qty")) or 0.0)
    return (price or 0.0) * qty * CONTRACT_MULTIPLIER


def _current_price(row: dict[str, Any]) -> Optional[float]:
    return _first_number([row.get("current_price"), row.get("underlying_price"), row.get("price")])


def _put_assignment_gap(row: dict[str, Any]) -> float:
    if _option_type(row) != "put":
        return 0.0
    strike = number(row.get("strike"))
    current = _current_price(row)
    qty = abs(number(row.get("quantity") if row.get("quantity") is not None else row.get("qty")) or 0.0)
    if strike is None or current is None or qty == 0:
        return 0.0
    return min(current - strike, 0.0) * qty * CONTRACT_MULTIPLIER


def _call_upside_foregone(row: dict[str, Any]) -> float:
    if _option_type(row) != "call":
        return 0.0
    strike = number(row.get("strike"))
    current = _current_price(row)
    qty = abs(number(row.get("quantity") if row.get("quantity") is not None else row.get("qty")) or 0.0)
    if strike is None or current is None or qty == 0:
        return 0.0
    return -max(current - strike, 0.0) * qty * CONTRACT_MULTIPLIER


def _put_exposure(rows: list[dict[str, Any]]) -> float:
    total = 0.0
    for row in rows:
        if _option_type(row) != "put":
            continue
        strike = number(row.get("strike"))
        qty = abs(number(row.get("quantity") if row.get("quantity") is not None else row.get("qty")) or 0.0)
        if strike is not None:
            total += strike * qty * CONTRACT_MULTIPLIER
    return total


def _is_near_strike_put(row: dict[str, Any]) -> bool:
    if _put_assignment_gap(row) < 0:
        return False
    strike = number(row.get("strike"))
    current = _current_price(row)
    if strike in (None, 0) or current is None:
        return False
    return abs((current - strike) / strike) <= 0.05
