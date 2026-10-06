from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Optional


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric else None


def _date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except (TypeError, ValueError):
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) is not None:
            return mapping[key]
    return None


def _items(payload: dict[str, Any], *paths: tuple[str, ...]) -> list[dict[str, Any]]:
    for path in paths:
        current: Any = payload
        for part in path:
            current = current.get(part) if isinstance(current, dict) else None
        if isinstance(current, list):
            return [row for row in current if isinstance(row, dict)]
    return []


def _action_group(row: dict[str, Any]) -> str:
    priority = str(row.get("priority") or "").lower()
    category = str(row.get("category") or "")
    dte = _number(row.get("dte"))
    if priority == "high":
        return "now"
    if dte is not None and dte <= 10 and category in {
        "Reduce assignment risk",
        "Monitor assignment risk",
        "Roll to improve recovery",
        "Evaluate exit vs roll",
        "Accept / monitor exit",
    }:
        return "now"
    if priority == "medium":
        return "plan"
    return "monitor"


def _action_rows(decision_lab: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for source in decision_lab.get("ticker_situations") or []:
        if not isinstance(source, dict):
            continue
        rows.append(
            {
                "ticker": source.get("ticker"),
                "group": _action_group(source),
                "priority": source.get("priority"),
                "category": source.get("category"),
                "objective": source.get("objective"),
                "recommendation": source.get("recommendation"),
                "expiry": source.get("expiry"),
                "dte": _number(source.get("dte")),
                "realized_pnl": _number(source.get("realized_pnl")),
                "unrealized_pnl": _number(source.get("unrealized_pnl")),
                "total_pnl": _number(source.get("total_pnl")),
                "signal_label": source.get("signal_label"),
                "signal_value": _number(source.get("signal_value")),
                "current_price": _number(source.get("current_price")),
            }
        )
    order = {"now": 0, "plan": 1, "monitor": 2}
    rows.sort(
        key=lambda row: (
            order.get(str(row.get("group")), 9),
            row.get("dte") if row.get("dte") is not None else 9999,
            _number(row.get("signal_value")) or 0,
            str(row.get("ticker") or ""),
        )
    )
    return rows


def _risk_rows(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _items(
        dashboard,
        ("open_shorts", "items"),
        ("positions", "open_option_shorts"),
        ("dashboard", "open_option_short_preview"),
    )
    output = []
    for row in rows:
        option_type = str(_first(row, "option_type", "type", "put_call") or "").title()
        ticker = str(row.get("ticker") or "").upper()
        strike = _number(row.get("strike"))
        quantity = abs(_number(_first(row, "quantity", "qty")) or 0)
        current = _number(_first(row, "current_price", "underlying_price"))
        moneyness = _number(row.get("moneyness"))
        if moneyness is not None and abs(moneyness) <= 2:
            moneyness *= 100
        if not ticker or strike is None or quantity <= 0:
            continue
        notional = strike * quantity * 100
        output.append(
            {
                "ticker": ticker,
                "type": option_type,
                "strike": strike,
                "expiry": str(_first(row, "expiration", "expiry") or "")[:10],
                "dte": _number(_first(row, "days_to_expiration", "dte")),
                "current": current,
                "moneyness": moneyness,
                "quantity": quantity,
                "notional": notional,
                "open_premium": _number(_first(row, "accounting_open_premium", "open_premium")),
                "projected_pnl": _number(row.get("projected_pnl")),
                "backing": row.get("backing"),
            }
        )
    return output


def _expiry_ladder(risk_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "put_exposure": 0.0,
            "itm_put_exposure": 0.0,
            "call_notional": 0.0,
            "open_premium": 0.0,
            "tickers": set(),
        }
    )
    for row in risk_rows:
        expiry = str(row.get("expiry") or "")
        month = expiry[:7] if len(expiry) >= 7 else "Unknown"
        bucket = grouped[month]
        bucket["tickers"].add(row.get("ticker"))
        bucket["open_premium"] += _number(row.get("open_premium")) or 0.0
        if row.get("type") == "Put":
            bucket["put_exposure"] += _number(row.get("notional")) or 0.0
            if (_number(row.get("moneyness")) or 0) > 0:
                bucket["itm_put_exposure"] += _number(row.get("notional")) or 0.0
        elif row.get("type") == "Call":
            bucket["call_notional"] += _number(row.get("notional")) or 0.0
    return [
        {
            "month": month,
            "ticker_count": len(values.pop("tickers")),
            **values,
        }
        for month, values in sorted(grouped.items())
    ]


def _recovery_rows(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    inventory = _items(dashboard, ("positions", "inventory"), ("tables", "inventory"))
    tickers = {
        str(row.get("ticker") or "").upper(): row
        for row in _items(dashboard, ("tickers", "items"), ("tables", "per_ticker_totals"))
    }
    as_of = _date(((dashboard.get("dashboard") or {}).get("request") or {}).get("as_of")) or date.today()
    rows = []
    for row in inventory:
        ticker = str(row.get("ticker") or "").upper()
        shares = abs(_number(row.get("shares")) or 0)
        cost = _number(_first(row, "cost_per_share", "avg_cost"))
        current = _number(row.get("current_price"))
        if not ticker or not shares or cost in (None, 0) or current is None:
            continue
        covered = abs(_number(row.get("covered_shares")) or 0)
        covered_strike = _number(row.get("covered_strike"))
        assigned = _date(
            _first(row, "assignment_date", "last_assigned", "buy_date", "latest_buy_date", "open_date")
        )
        age_days = max((as_of - assigned).days, 0) if assigned else None
        status = "uncovered"
        if covered > 0:
            status = "capped" if covered_strike is not None and current > covered_strike else "covered"
        metrics = tickers.get(ticker) or {}
        rows.append(
            {
                "ticker": ticker,
                "shares": shares,
                "cost": cost,
                "current": current,
                "recovery_pct": (current / cost - 1) * 100,
                "age_days": age_days,
                "capital_tied": cost * shares,
                "unrealized_pnl": _number(row.get("unrealized_pnl")),
                "total_pnl": _number(_first(metrics, "total_pnl", "total_p&l")),
                "covered_shares": covered,
                "covered_strike": covered_strike,
                "status": status,
            }
        )
    return rows


def _candidate_rows(decision_lab: dict[str, Any]) -> list[dict[str, Any]]:
    situations = {
        str(row.get("ticker") or "").upper(): row
        for row in decision_lab.get("ticker_situations") or []
        if isinstance(row, dict) and row.get("ticker")
    }
    groups = []
    for group in decision_lab.get("recommendation_candidates") or []:
        if not isinstance(group, dict):
            continue
        candidates = []
        for row in (group.get("candidates") or [])[:3]:
            candidates.append(
                {
                    "action": row.get("action"),
                    "strike": _number(row.get("strike")),
                    "expiry": row.get("expiry"),
                    "dte": _number(row.get("dte")),
                    "net_credit": _number(_first(row, "roll_net_credit", "premium")),
                    "expected_value": _number(row.get("expected_value")),
                    "ev_vs_current": _number(row.get("expected_value_vs_current")),
                    "exercise_probability": _number(row.get("exercise_probability")),
                    "delta": _number(row.get("delta")),
                    "liquidity": row.get("liquidity"),
                    "tradeability": row.get("tradeability"),
                    "score": _number(row.get("score")),
                    "is_current": bool(row.get("is_current_position")),
                    "reason": row.get("score_reason") or row.get("explanation"),
                }
            )
        ticker = str(group.get("ticker") or "").upper()
        current_state = dict(group.get("current_state") or {})
        situation = situations.get(ticker) or {}
        for key in ("realized_pnl", "unrealized_pnl", "total_pnl", "current_price", "cost_basis"):
            if current_state.get(key) is None and situation.get(key) is not None:
                current_state[key] = situation.get(key)
        groups.append(
            {
                "ticker": ticker,
                "category": group.get("category"),
                "objective": group.get("objective"),
                "current_state": current_state,
                "status": group.get("candidate_status") or {},
                "candidates": candidates,
            }
        )
    groups.sort(key=lambda row: (0 if row["candidates"] else 1, str(row.get("ticker") or "")))
    return groups


def _yearly_rows(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _items(dashboard, ("yearly", "years"), ("tables", "yearly_realized"))
    output = []
    for row in rows:
        year = _first(row, "year", "Year")
        if year is None:
            continue
        output.append(
            {
                "year": str(year),
                "options": _number(_first(row, "options_pnl", "realized_options_pnl")) or 0.0,
                "stock": _number(_first(row, "stock_pnl", "realized_stock_pnl")) or 0.0,
                "dividends": _number(row.get("dividends")) or 0.0,
                "realized": _number(_first(row, "realized", "total_realized_pnl", "combined_realized_pnl")),
                "roac": _number(row.get("roac")),
            }
        )
    return output


def _monthly_rows(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _items(dashboard, ("monthly", "months"), ("tables", "monthly_cycles"))
    output = []
    for row in rows:
        month = str(row.get("month") or "")[:10]
        if not month:
            continue
        output.append(
            {
                "month": month,
                "realized": _number(_first(row, "total_realized_pnl", "total_realized")),
                "projected": _number(_first(row, "projected_month_pnl", "projected_cycle_pnl")),
                "roac": _number(row.get("roac")),
                "projected_roac": _number(_first(row, "projected_return_roac", "projected_roac")),
            }
        )
    return output


def build_visual_prototype_data(
    dashboard: dict[str, Any],
    decision_lab: dict[str, Any],
    assignment_quality: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    risk = _risk_rows(dashboard)
    option_status = ((decision_lab.get("option_market_data") or {}).get("status") or {})
    active_cycle = (dashboard.get("monthly") or {}).get("active_cycle") or {}
    return {
        "generated_at": dashboard.get("generated_at"),
        "as_of": ((dashboard.get("dashboard") or {}).get("request") or {}).get("as_of"),
        "target_return": (dashboard.get("web") or {}).get("target_return"),
        "target_floor": (dashboard.get("web") or {}).get("target_floor"),
        "prices_updated_at": ((dashboard.get("dashboard") or {}).get("data_freshness") or {}).get(
            "prices_updated_at"
        ),
        "option_status": option_status,
        "active_cycle": active_cycle,
        "actions": _action_rows(decision_lab),
        "risk_map": risk,
        "expiry_ladder": _expiry_ladder(risk),
        "recovery_map": _recovery_rows(dashboard),
        "candidate_groups": _candidate_rows(decision_lab),
        "yearly": _yearly_rows(dashboard),
        "monthly": _monthly_rows(dashboard),
        "assignment_quality": assignment_quality or {},
    }
