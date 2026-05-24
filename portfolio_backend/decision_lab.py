from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from typing import Any, Optional


RISK_BUCKETS = [
    ("<=15%", None, 0.15),
    ("15-20%", 0.15, 0.20),
    ("20-25%", 0.20, 0.25),
    ("25-30%", 0.25, 0.30),
    (">30%", 0.30, None),
]


def build_decision_lab_data(
    dashboard_payload: dict[str, Any],
    *,
    probability_matches: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    probability_matches = probability_matches or []
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": dashboard_payload.get("source", {}),
        "data_freshness": (dashboard_payload.get("dashboard") or {}).get("data_freshness", {}),
        "summary": _summary(dashboard_payload, probability_matches),
        "action_queue": _action_queue(dashboard_payload),
        "monthly_decision": _monthly_decision(dashboard_payload),
        "strike_selection": _strike_selection(probability_matches),
        "ticker_scorecard": _ticker_scorecard(dashboard_payload),
        "open_positions": _open_positions(dashboard_payload),
        "performance_insights": _performance_insights(dashboard_payload),
        "coverage_notes": _coverage_notes(dashboard_payload, probability_matches),
    }


def _summary(payload: dict[str, Any], probability_matches: list[dict[str, Any]]) -> dict[str, Any]:
    dashboard = payload.get("dashboard") or {}
    snapshot = dashboard.get("snapshot") or {}
    issues = dashboard.get("issue_summary") or {}
    matched = [match for match in probability_matches if match.get("matched")]
    return {
        "ytd_total_pnl": _num(snapshot.get("ytd_total_pnl")),
        "current_unrealized_pnl": _num(snapshot.get("current_unrealized_pnl")),
        "actionable_issue_count": int(_num(issues.get("total_count")) or 0),
        "open_short_count": len(_open_short_rows(payload)),
        "assigned_holding_count": len(_inventory_rows(payload)),
        "probability_match_count": len(matched),
        "probability_trade_count": len(probability_matches),
        "probability_coverage_rate": _ratio(len(matched), len(probability_matches)),
    }


def _action_queue(payload: dict[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    issues = ((payload.get("issues") or {}).get("issues") or [])[:8]
    for issue in issues:
        actions.append(
            {
                "priority": "high",
                "ticker": "",
                "reason": issue.get("message") or issue.get("category") or "Actionable data issue",
                "impact": None,
                "expiry": "",
                "dte": None,
                "suggested_action": "review",
                "source": "data health",
            }
        )

    for row in _open_short_rows(payload):
        ticker = str(row.get("ticker") or "")
        option_type = str(row.get("option_type") or "").lower()
        dte = _num(row.get("days_to_expiration"))
        projected = _open_short_projected_pnl(row)
        moneyness = _num(row.get("moneyness"))
        intrinsic_gap = _open_short_intrinsic_gap(row)
        if option_type.startswith("put") and intrinsic_gap < 0:
            actions.append(
                {
                    "priority": "high" if (dte is not None and dte <= 14) else "medium",
                    "ticker": ticker,
                    "reason": "ITM put assignment risk",
                    "impact": projected,
                    "expiry": row.get("expiration"),
                    "dte": dte,
                    "suggested_action": "roll or close",
                    "source": "open puts",
                }
            )
        elif option_type.startswith("put") and moneyness is not None and abs(moneyness) <= 0.05:
            actions.append(
                {
                    "priority": "medium",
                    "ticker": ticker,
                    "reason": "Near-strike put",
                    "impact": projected,
                    "expiry": row.get("expiration"),
                    "dte": dte,
                    "suggested_action": "monitor",
                    "source": "open puts",
                }
            )
        elif option_type.startswith("call") and intrinsic_gap < 0:
            actions.append(
                {
                    "priority": "medium",
                    "ticker": ticker,
                    "reason": "Covered call caps upside",
                    "impact": projected,
                    "expiry": row.get("expiration"),
                    "dte": dte,
                    "suggested_action": "review roll",
                    "source": "open calls",
                }
            )

    for row in _inventory_rows(payload):
        unrealized = _num(row.get("unrealized_pnl"))
        covered = _num(row.get("covered_shares")) or 0
        shares = _num(row.get("shares")) or 0
        if unrealized is not None and unrealized < 0 and covered <= 0:
            actions.append(
                {
                    "priority": "medium",
                    "ticker": row.get("ticker"),
                    "reason": "Assigned holding below cost with no covered call",
                    "impact": unrealized,
                    "expiry": "",
                    "dte": None,
                    "suggested_action": "sell covered call",
                    "source": "assigned holdings",
                }
            )
        elif covered > 0 and shares > 0 and covered >= shares:
            actions.append(
                {
                    "priority": "low",
                    "ticker": row.get("ticker"),
                    "reason": "Covered holding is capped",
                    "impact": unrealized,
                    "expiry": "",
                    "dte": None,
                    "suggested_action": "monitor cap",
                    "source": "assigned holdings",
                }
            )

    for row in (payload.get("tickers") or {}).get("items") or []:
        options = _num(row.get("realized_options_pnl")) or 0
        total = _num(row.get("total_pnl")) or 0
        if options > 0 and total < 0:
            actions.append(
                {
                    "priority": "medium",
                    "ticker": row.get("ticker"),
                    "reason": "Positive premiums but negative total P&L",
                    "impact": total,
                    "expiry": "",
                    "dte": None,
                    "suggested_action": "avoid adding",
                    "source": "ticker quality",
                }
            )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    actions.sort(key=lambda item: (priority_order.get(str(item["priority"]), 9), _num(item.get("impact")) or 0))
    return actions[:24]


def _monthly_decision(payload: dict[str, Any]) -> dict[str, Any]:
    monthly = (payload.get("dashboard") or {}).get("monthly_target") or {}
    snapshot = (payload.get("dashboard") or {}).get("snapshot") or {}
    realized = _num(monthly.get("realized_month_pnl")) or 0
    open_net = _num(monthly.get("open_expiring_option_unrealized_pnl"))
    if open_net is None:
        premium = _num(monthly.get("open_expiring_incremental_premium")) or 0
        gap = _num(monthly.get("open_expiring_intrinsic_value_gap")) or 0
        open_net = premium + gap
    projected = _num(monthly.get("risk_adjusted_projected_month_pnl"))
    if projected is None:
        projected = realized + (open_net or 0)
    target_pnl = _num(monthly.get("target_pnl"))
    remaining = max((target_pnl or 0) - (projected or 0), 0) if target_pnl is not None else None
    return {
        "month": monthly.get("month"),
        "realized_pnl": realized,
        "open_option_net": open_net,
        "projected_pnl": projected,
        "target_pnl": target_pnl,
        "remaining_to_target": remaining,
        "projected_return_roac": _num(monthly.get("risk_adjusted_projected_return_roac"))
        or _num(monthly.get("projected_return_roac")),
        "target_return": _num(monthly.get("target_return")) or _num((payload.get("web") or {}).get("target_return")),
        "premium_component": _num(monthly.get("open_expiring_incremental_premium")),
        "intrinsic_gap": _num(monthly.get("open_expiring_intrinsic_value_gap")),
        "itm_put_cash_required": _num(snapshot.get("itm_put_cash_required")),
        "itm_put_contracts": _num(snapshot.get("itm_put_contracts")),
    }


def _strike_selection(probability_matches: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [match for match in probability_matches if match.get("matched")]
    bucket_rows = []
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for match in matched:
        trade = match.get("trade") or {}
        risk = _num(match.get("assignment_risk_proxy"))
        if risk is None:
            risk = _num(trade.get("assignment_risk_proxy"))
        if risk is None:
            continue
        year = _year(trade.get("trade_date"))
        if year:
            by_year[year].append(match)
        buckets[_risk_bucket(risk)].append(match)

    for label, _lower, _upper in RISK_BUCKETS:
        rows = buckets.get(label, [])
        premiums = [_opening_premium(match) for match in rows]
        capital = [_trade_capital(match) for match in rows]
        premium_per_capital = [
            premium / cap for premium, cap in zip(premiums, capital) if premium is not None and cap and cap > 0
        ]
        bucket_rows.append(
            {
                "bucket": label,
                "count": len(rows),
                "avg_assignment_risk_proxy": _avg([_assignment_risk(match) for match in rows]),
                "avg_profit_probability": _avg([_profit_probability(match) for match in rows]),
                "total_opening_premium": _sum(premiums),
                "avg_opening_premium": _avg(premiums),
                "avg_premium_to_capital": _avg(premium_per_capital),
                "top_tickers": _top_tickers(rows),
            }
        )

    by_year_rows = []
    for year, rows in sorted(by_year.items()):
        by_year_rows.append(
            {
                "year": year,
                "count": len(rows),
                "avg_profit_probability": _avg([_profit_probability(match) for match in rows]),
                "avg_assignment_risk_proxy": _avg([_assignment_risk(match) for match in rows]),
                "avg_opening_premium": _avg([_opening_premium(match) for match in rows]),
            }
        )

    return {
        "coverage": {
            "matched_count": len(matched),
            "trade_count": len(probability_matches),
            "coverage_rate": _ratio(len(matched), len(probability_matches)),
        },
        "bucket_summary": bucket_rows,
        "year_summary": by_year_rows,
        "note": "Historical Google Sheet probability is treated as a probability-of-profit proxy. This prototype uses opening premium and risk coverage only; lifecycle P&L attribution by risk bucket is the next join.",
    }


def _ticker_scorecard(payload: dict[str, Any]) -> list[dict[str, Any]]:
    open_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _open_short_rows(payload):
        open_by_ticker[str(row.get("ticker") or "").upper()].append(row)
    inventory_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _inventory_rows(payload):
        inventory_by_ticker[str(row.get("ticker") or "").upper()].append(row)

    rows = []
    for row in (payload.get("tickers") or {}).get("items") or []:
        ticker = str(row.get("ticker") or "").upper()
        total = _num(row.get("total_pnl")) or 0
        options = _num(row.get("realized_options_pnl")) or 0
        stock = _num(row.get("realized_stock_pnl")) or 0
        unreal = _num(row.get("unrealized_pnl")) or 0
        open_rows = open_by_ticker.get(ticker, [])
        inv_rows = inventory_by_ticker.get(ticker, [])
        itm_open = sum(1 for item in open_rows if _open_short_intrinsic_gap(item) < 0)
        score = 50
        score += min(max(total / 1000, -25), 25)
        score += min(max(options / 1000, -15), 15)
        score += min(max(unreal / 1000, -25), 15)
        score -= itm_open * 8
        score -= len(inv_rows) * 2 if unreal < 0 else 0
        score = round(max(0, min(100, score)))
        if score >= 72:
            status = "Preferred"
        elif score >= 55:
            status = "Watch"
        elif score >= 40:
            status = "Review"
        else:
            status = "Avoid"
        rows.append(
            {
                "ticker": ticker,
                "score": score,
                "status": status,
                "total_pnl": total,
                "realized_options_pnl": options,
                "realized_stock_pnl": stock,
                "unrealized_pnl": unreal,
                "open_options": len(open_rows),
                "itm_open_options": itm_open,
                "assigned_lots": len(inv_rows),
                "capital_tied": _sum([_num(item.get("shares")) * _num(item.get("cost_per_share")) for item in inv_rows]),
            }
        )
    rows.sort(key=lambda item: (item["score"], item["total_pnl"]), reverse=True)
    return rows


def _open_positions(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    open_rows = []
    for row in _open_short_rows(payload):
        projected = _open_short_projected_pnl(row)
        open_rows.append(
            {
                **row,
                "projected_pnl": projected,
                "intrinsic_gap": _open_short_intrinsic_gap(row),
                "cash_required_if_assigned": _cash_required_if_assigned(row),
                "decision": _open_short_decision(row),
            }
        )
    open_rows.sort(key=lambda row: (_num(row.get("projected_pnl")) or 0, _num(row.get("days_to_expiration")) or 9999))

    holdings = []
    for row in _inventory_rows(payload):
        shares = _num(row.get("shares")) or 0
        covered = _num(row.get("covered_shares")) or 0
        holdings.append(
            {
                **row,
                "uncovered_shares": max(shares - covered, 0),
                "capital_tied": (_num(row.get("cost_per_share")) or 0) * shares,
                "decision": _holding_decision(row),
            }
        )
    holdings.sort(key=lambda row: _num(row.get("unrealized_pnl")) or 0)
    return {"open_shorts": open_rows, "assigned_holdings": holdings}


def _performance_insights(payload: dict[str, Any]) -> dict[str, Any]:
    yearly = ((payload.get("yearly") or {}).get("years") or (payload.get("views") or {}).get("yearly", {}).get("with_unrealized") or [])
    monthly_rows = (payload.get("monthly") or {}).get("months") or []
    ticker_rows = (payload.get("tickers") or {}).get("items") or []
    negative_months = [row for row in monthly_rows if (_num(row.get("total_realized_pnl")) or 0) < 0]
    return {
        "yearly": yearly,
        "negative_month_count": len(negative_months),
        "largest_negative_months": sorted(
            [
                {
                    "month": row.get("month"),
                    "total_realized_pnl": _num(row.get("total_realized_pnl")),
                    "options_pnl": _num(row.get("realized_options_pnl")),
                    "stock_pnl": _num(row.get("realized_stock_pnl")),
                }
                for row in negative_months
            ],
            key=lambda row: row["total_realized_pnl"] or 0,
        )[:5],
        "worst_tickers": sorted(
            [
                {
                    "ticker": row.get("ticker"),
                    "total_pnl": _num(row.get("total_pnl")),
                    "options_pnl": _num(row.get("realized_options_pnl")),
                    "unrealized_pnl": _num(row.get("unrealized_pnl")),
                }
                for row in ticker_rows
                if (_num(row.get("total_pnl")) or 0) < 0
            ],
            key=lambda row: row["total_pnl"] or 0,
        )[:8],
    }


def _coverage_notes(payload: dict[str, Any], probability_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    notes = []
    coverage = ((payload.get("dashboard") or {}).get("data_freshness") or {}).get("price_coverage") or {}
    missing = int(_num(coverage.get("missing_count")) or 0)
    if missing:
        notes.append({"severity": "warning", "message": f"{missing} required price(s) missing from current snapshot."})
    matched = sum(1 for match in probability_matches if match.get("matched"))
    if probability_matches:
        notes.append(
            {
                "severity": "info",
                "message": f"Historical probability coverage: {matched}/{len(probability_matches)} IBKR short-option opening trades.",
            }
        )
    else:
        notes.append({"severity": "info", "message": "Historical probability import is not available in this environment."})
    return notes


def _open_short_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        ((payload.get("positions") or {}).get("open_option_shorts"))
        or ((payload.get("open_shorts") or {}).get("items"))
        or []
    )


def _inventory_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return ((payload.get("positions") or {}).get("inventory")) or ((payload.get("tables") or {}).get("inventory")) or []


def _open_short_projected_pnl(row: dict[str, Any]) -> Optional[float]:
    premium = _num(row.get("display_premium_collected"))
    if premium is None:
        premium = _num(row.get("roll_adjusted_premium_collected"))
    if premium is None:
        premium = _num(row.get("premium_collected")) or 0
    gap = _open_short_intrinsic_gap(row)
    return premium + gap


def _open_short_intrinsic_gap(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or current is None or not qty:
        return 0.0
    option_type = str(row.get("option_type") or "").lower()
    if option_type.startswith("call"):
        return -max(current - strike, 0) * 100 * qty
    return -max(strike - current, 0) * 100 * qty


def _cash_required_if_assigned(row: dict[str, Any]) -> Optional[float]:
    option_type = str(row.get("option_type") or "").lower()
    if not option_type.startswith("put"):
        return None
    strike = _num(row.get("strike"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or not qty:
        return None
    return strike * 100 * qty


def _open_short_decision(row: dict[str, Any]) -> str:
    option_type = str(row.get("option_type") or "").lower()
    dte = _num(row.get("days_to_expiration"))
    gap = _open_short_intrinsic_gap(row)
    if gap < 0 and option_type.startswith("put"):
        return "roll/close review" if dte is not None and dte <= 21 else "monitor assignment risk"
    if gap < 0 and option_type.startswith("call"):
        return "review cap/roll"
    moneyness = _num(row.get("moneyness"))
    if moneyness is not None and abs(moneyness) <= 0.05:
        return "monitor"
    return "no action"


def _holding_decision(row: dict[str, Any]) -> str:
    unrealized = _num(row.get("unrealized_pnl")) or 0
    covered = _num(row.get("covered_shares")) or 0
    shares = _num(row.get("shares")) or 0
    if unrealized < 0 and covered <= 0:
        return "consider covered call"
    if covered > 0 and covered >= shares:
        return "covered; monitor cap"
    if unrealized > 0 and covered <= 0:
        return "consider call if willing to sell"
    return "monitor"


def _risk_bucket(value: float) -> str:
    for label, lower, upper in RISK_BUCKETS:
        if (lower is None or value > lower) and (upper is None or value <= upper):
            return label
    return ">30%"


def _assignment_risk(match: dict[str, Any]) -> Optional[float]:
    trade = match.get("trade") or {}
    return _num(match.get("assignment_risk_proxy")) or _num(trade.get("assignment_risk_proxy"))


def _profit_probability(match: dict[str, Any]) -> Optional[float]:
    trade = match.get("trade") or {}
    return _num(match.get("profit_probability")) or _num(trade.get("profit_probability"))


def _opening_premium(match: dict[str, Any]) -> Optional[float]:
    trade = match.get("trade") or {}
    cash = _num(trade.get("net_cash"))
    if cash is not None:
        return abs(cash)
    qty = abs(_num(trade.get("qty")) or 0)
    price = abs(_num(trade.get("trade_price")) or 0)
    return qty * price * 100 if qty and price else None


def _trade_capital(match: dict[str, Any]) -> Optional[float]:
    trade = match.get("trade") or {}
    qty = abs(_num(trade.get("qty")) or 0)
    strike = _num(trade.get("strike"))
    if not qty or strike is None:
        return None
    return qty * strike * 100


def _top_tickers(rows: list[dict[str, Any]]) -> str:
    tickers = Counter(str((row.get("trade") or {}).get("ticker") or "").upper() for row in rows)
    return ", ".join([ticker for ticker, _count in tickers.most_common(4) if ticker])


def _year(value: Any) -> Optional[int]:
    if isinstance(value, date):
        return value.year
    try:
        return datetime.fromisoformat(str(value)[:10]).year
    except Exception:
        return None


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _sum(values: list[Optional[float]]) -> float:
    return sum(value for value in values if value is not None)


def _avg(values: list[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None
