from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from portfolio_backend.decision_lab_candidates import apply_option_market_candidates, recommendation_candidates
from portfolio_backend.option_market.history import historical_enrichment_to_probability_match


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
    historical_enrichments: Optional[list[dict[str, Any]]] = None,
    option_market_data: Optional[dict[str, Any]] = None,
    option_market_loader: Optional[
        Callable[[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], dict[str, Any]], dict[str, Any]]
    ] = None,
) -> dict[str, Any]:
    probability_matches = _combine_historical_matches(probability_matches or [], historical_enrichments or [])
    probability_matches = _enrich_probability_matches(dashboard_payload, probability_matches)
    as_of = _as_of_date(dashboard_payload)
    ticker_situations = _ticker_situations(dashboard_payload)
    active_cycle = _active_cycle(dashboard_payload)
    candidate_groups = recommendation_candidates(ticker_situations, as_of=as_of)
    if option_market_loader is not None:
        option_market_data = option_market_loader(ticker_situations, active_cycle, candidate_groups, dashboard_payload)
    recommendation_rows = apply_option_market_candidates(candidate_groups, option_market_data or {}, as_of=as_of)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": dashboard_payload.get("source", {}),
        "data_freshness": (dashboard_payload.get("dashboard") or {}).get("data_freshness", {}),
        "summary": _summary(dashboard_payload, probability_matches, ticker_situations, active_cycle, option_market_data or {}),
        "ticker_situations": [_public_situation(situation) for situation in ticker_situations],
        "active_cycle": active_cycle,
        "option_market_data": _public_option_market_data(option_market_data or {}),
        "recommendation_candidates": recommendation_rows,
        "strike_quality": _strike_quality(probability_matches),
        "coverage_notes": _coverage_notes(dashboard_payload, probability_matches, option_market_data or {}),
    }


def _summary(
    payload: dict[str, Any],
    probability_matches: list[dict[str, Any]],
    ticker_situations: list[dict[str, Any]],
    active_cycle: dict[str, Any],
    option_market_data: dict[str, Any],
) -> dict[str, Any]:
    dashboard = payload.get("dashboard") or {}
    snapshot = dashboard.get("snapshot") or {}
    issues = dashboard.get("issue_summary") or {}
    matched = [match for match in probability_matches if _assignment_risk(match) is not None]
    provider_rows = [match for match in probability_matches if match.get("provider")]
    provider_contract_rows = [match for match in provider_rows if match.get("historical_provider_contract_matched")]
    provider_price_rows = [
        match
        for match in provider_rows
        if _num(match.get("option_close")) is not None or _num(match.get("option_vwap")) is not None
    ]
    return {
        "ytd_total_pnl": _num(snapshot.get("ytd_total_pnl")),
        "current_unrealized_pnl": _num(snapshot.get("current_unrealized_pnl")),
        "actionable_issue_count": int(_num(issues.get("total_count")) or 0),
        "action_item_count": len(ticker_situations),
        "open_short_count": len(_open_short_rows(payload)),
        "assigned_holding_count": len(_inventory_rows(payload)),
        "active_cycle": active_cycle.get("cycle_label"),
        "total_open_put_exposure": active_cycle.get("portfolio_put_exposure"),
        "total_itm_put_exposure": active_cycle.get("portfolio_itm_put_exposure"),
        "probability_match_count": len(matched),
        "probability_trade_count": len(probability_matches),
        "probability_coverage_rate": _ratio(len(matched), len(probability_matches)),
        "historical_provider_trade_count": len(provider_rows),
        "historical_provider_contract_match_count": len(provider_contract_rows),
        "historical_provider_option_price_count": len(provider_price_rows),
        "option_contract_count": (option_market_data.get("status") or {}).get("contract_count"),
        "option_last_fetched_at": (option_market_data.get("status") or {}).get("last_fetched_at"),
        "option_candidate_source": (option_market_data.get("status") or {}).get("source"),
    }


def _public_situation(situation: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in situation.items() if not str(key).startswith("_")}


def _public_option_market_data(option_market_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": option_market_data.get("status") or {},
        "universe": option_market_data.get("universe") or {},
    }


def _realized_pnl_from_metrics(metrics: dict[str, Any]) -> float:
    option = _num(metrics.get("realized_options_pnl"))
    stock = _num(metrics.get("realized_stock_pnl"))
    dividends = _num(metrics.get("dividends"))
    if option is not None or stock is not None or dividends is not None:
        return (option or 0.0) + (stock or 0.0) + (dividends or 0.0)
    return _num(metrics.get("combined_realized_pnl")) or 0.0


def _total_pnl_from_metrics(metrics: dict[str, Any]) -> float:
    total = _num(metrics.get("total_pnl"))
    unrealized = _num(metrics.get("unrealized_pnl"))
    dividends = _num(metrics.get("dividends")) or 0.0
    if total is not None:
        if dividends and unrealized is not None:
            option = _num(metrics.get("realized_options_pnl"))
            stock = _num(metrics.get("realized_stock_pnl"))
            combined = _num(metrics.get("combined_realized_pnl"))
            option_stock = (option or 0.0) + (stock or 0.0)
            combined_includes_dividends = (
                combined is not None
                and (option is not None or stock is not None)
                and abs(combined - (option_stock + dividends)) < 0.01
            )
            realized_ex_dividends = combined if combined is not None else option_stock
            if not combined_includes_dividends and abs(total - (realized_ex_dividends + unrealized)) < 0.01:
                return total + dividends
        return total
    if unrealized is not None:
        return _realized_pnl_from_metrics(metrics) + unrealized
    return 0.0


def _ticker_situations(payload: dict[str, Any]) -> list[dict[str, Any]]:
    open_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _open_short_rows(payload):
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            open_by_ticker[ticker].append(row)

    inventory_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _inventory_rows(payload):
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            inventory_by_ticker[ticker].append(row)

    ticker_metrics = {
        str(row.get("ticker") or "").upper(): row
        for row in (payload.get("tickers") or {}).get("items") or []
        if row.get("ticker")
    }

    tickers = sorted(set(open_by_ticker) | set(inventory_by_ticker) | set(ticker_metrics))
    situations: list[dict[str, Any]] = []
    for ticker in tickers:
        open_rows = open_by_ticker.get(ticker, [])
        inv_rows = inventory_by_ticker.get(ticker, [])
        metrics = ticker_metrics.get(ticker, {})
        situation = _classify_ticker_situation(ticker, open_rows, inv_rows, metrics)
        if situation["category"] != "No action":
            situations.append(situation)

    priority_order = {"high": 0, "medium": 1, "low": 2}
    category_order = {
        "Reduce assignment risk": 0,
        "Monitor assignment risk": 1,
        "Recover with covered call": 2,
        "Roll to improve recovery": 3,
        "Evaluate exit vs roll": 4,
        "Harvest unused put risk": 5,
        "Accept / monitor exit": 6,
    }
    situations.sort(
        key=lambda item: (
            priority_order.get(str(item.get("priority")), 9),
            category_order.get(str(item.get("category")), 9),
            _num(item.get("signal_value")) or 0,
            str(item.get("ticker") or ""),
        )
    )
    return situations


def _classify_ticker_situation(
    ticker: str,
    open_rows: list[dict[str, Any]],
    inv_rows: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    open_puts = [row for row in open_rows if _option_type(row).startswith("put")]
    open_calls = [row for row in open_rows if _option_type(row).startswith("call")]
    itm_puts = [row for row in open_puts if _open_short_assignment_gap(row) < 0]
    near_puts = [
        row
        for row in open_puts
        if _open_short_assignment_gap(row) >= 0 and (_num(row.get("moneyness")) is not None and abs(_num(row.get("moneyness")) or 0) <= 0.05)
    ]
    deeply_otm_puts = [
        row
        for row in open_puts
        if _open_short_put_otm_distance(row) > 0
        and _open_short_put_otm_ratio(row) >= 0.25
    ]
    capped_calls = [row for row in open_calls if _covered_call_upside_foregone(row) < 0]
    uncovered_underwater = [
        row
        for row in inv_rows
        if (_num(row.get("unrealized_pnl")) or 0) < 0 and (_num(row.get("covered_shares")) or 0) <= 0
    ]
    covered_holdings = [
        row
        for row in inv_rows
        if (_num(row.get("covered_shares")) or 0) > 0 and (_num(row.get("shares")) or 0) > 0
    ]
    total_pnl = _total_pnl_from_metrics(metrics)
    options_pnl = _num(metrics.get("realized_options_pnl")) or 0
    unrealized_pnl = _num(metrics.get("unrealized_pnl")) or 0
    realized_pnl = _realized_pnl_from_metrics(metrics)

    if itm_puts:
        primary = min(itm_puts, key=lambda row: _num(row.get("days_to_expiration")) or 9999)
        return _situation(
            ticker=ticker,
            priority="high",
            category="Reduce assignment risk",
            objective="Reduce near-term put assignment risk",
            reason="Open put is in the money",
            signal_label="ITM put unrealized loss",
            signal_value=sum(_open_short_assignment_gap(row) for row in itm_puts),
            expiry=primary.get("expiration"),
            dte=_num(primary.get("days_to_expiration")),
            recommendation="Compare close vs roll down/out",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"{len(itm_puts)} ITM put(s)", f"{_money(_put_exposure(itm_puts))} ITM exposure"],
        )

    if uncovered_underwater:
        worst = min(uncovered_underwater, key=lambda row: _num(row.get("unrealized_pnl")) or 0)
        return _situation(
            ticker=ticker,
            priority="medium",
            category="Recover with covered call",
            objective="Recover income without forcing a bad exit",
            reason="Assigned holding below cost and uncovered",
            signal_label="Assigned holding drag",
            signal_value=_num(worst.get("unrealized_pnl")),
            expiry=None,
            dte=None,
            recommendation="Find covered-call candidate",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"{len(uncovered_underwater)} uncovered underwater lot(s)"],
        )

    if capped_calls or covered_holdings:
        call = min(capped_calls or open_calls, key=lambda row: _num(row.get("days_to_expiration")) or 9999, default={})
        holding_unrealized = _sum([_num(row.get("unrealized_pnl")) for row in covered_holdings or inv_rows])
        category = "Evaluate exit vs roll"
        objective = "Maximize lifecycle outcome while managing upside cap"
        recommendation = "Compare accept exit vs roll up/out"
        if total_pnl > 0 and holding_unrealized >= 0:
            category = "Accept / monitor exit"
            objective = "Exit cleanly unless rolling preserves attractive upside"
            recommendation = "Accept exit unless roll is clearly better"
        elif capped_calls and holding_unrealized < 0:
            category = "Roll to improve recovery"
            objective = "Improve recovery without extending weak risk"
            recommendation = "Evaluate roll up/out"
        return _situation(
            ticker=ticker,
            priority="medium" if capped_calls else "low",
            category=category,
            objective=objective,
            reason="Covered holding/call creates exit-or-roll decision",
            signal_label="Covered-call upside foregone" if capped_calls else "Holding unrealized P&L",
            signal_value=sum(_covered_call_upside_foregone(row) for row in capped_calls) if capped_calls else holding_unrealized,
            expiry=call.get("expiration"),
            dte=_num(call.get("days_to_expiration")),
            recommendation=recommendation,
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[
                f"{len(open_calls)} covered call(s)" if open_calls else "covered shares",
                f"ticker total {_money(total_pnl)}",
            ],
        )

    if near_puts:
        primary = min(near_puts, key=lambda row: _num(row.get("days_to_expiration")) or 9999)
        return _situation(
            ticker=ticker,
            priority="medium",
            category="Monitor assignment risk",
            objective="Avoid surprise assignment while preserving premium",
            reason="Open put is near strike",
            signal_label="Near-strike put gap",
            signal_value=sum(_open_short_assignment_gap(row) for row in near_puts),
            expiry=primary.get("expiration"),
            dte=_num(primary.get("days_to_expiration")),
            recommendation="Monitor or pre-plan roll",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"{len(near_puts)} near-strike put(s)"],
        )

    if deeply_otm_puts:
        primary = min(deeply_otm_puts, key=lambda row: _num(row.get("days_to_expiration")) or 9999)
        return _situation(
            ticker=ticker,
            priority="medium",
            category="Harvest unused put risk",
            objective="Consider rolling put strike up for more credit if risk remains acceptable",
            reason="Open put is far out of the money",
            signal_label="Put distance above strike",
            signal_value=_open_short_put_otm_distance(primary),
            expiry=primary.get("expiration"),
            dte=_num(primary.get("days_to_expiration")),
            recommendation="Compare keep vs roll strike up",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"{len(deeply_otm_puts)} far OTM put(s)"],
        )

    if options_pnl > 0 and total_pnl < 0 and (open_rows or inv_rows):
        return _situation(
            ticker=ticker,
            priority="medium",
            category="Pause new entries",
            objective="Avoid adding risk while recovery is active",
            reason="Positive premium but negative total lifecycle P&L",
            signal_label="Current unrealized drag",
            signal_value=unrealized_pnl,
            expiry=None,
            dte=None,
            recommendation="Review before adding exposure",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"realized {_money(realized_pnl)}", f"unrealized {_money(unrealized_pnl)}"],
        )

    return _situation(
        ticker=ticker,
        priority="none",
        category="No action",
        objective="No current decision required",
        reason="No active decision signal",
        signal_label="Current unrealized",
        signal_value=unrealized_pnl,
        expiry=None,
        dte=None,
        recommendation="No action",
        source="ticker-level",
        open_rows=open_rows,
        inventory_rows=inv_rows,
        metrics=metrics,
        supporting_signals=[],
    )


def _situation(
    *,
    ticker: str,
    priority: str,
    category: str,
    objective: str,
    reason: str,
    signal_label: str,
    signal_value: Optional[float],
    expiry: Any,
    dte: Optional[float],
    recommendation: str,
    source: str,
    open_rows: list[dict[str, Any]],
    inventory_rows: list[dict[str, Any]],
    metrics: dict[str, Any],
    supporting_signals: list[str],
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "priority": priority,
        "category": category,
        "objective": objective,
        "reason": reason,
        "signal_label": signal_label,
        "signal_value": signal_value,
        "open_risk_drag": signal_value,
        "expiry": expiry,
        "dte": dte,
        "recommendation": recommendation,
        "source": source,
        "open_contract_count": len(open_rows),
        "assigned_lot_count": len(inventory_rows),
        "realized_pnl": _realized_pnl_from_metrics(metrics),
        "total_pnl": _total_pnl_from_metrics(metrics),
        "unrealized_pnl": _num(metrics.get("unrealized_pnl")) or 0,
        "current_price": _first_num(
            [metrics.get("current_price")]
            + [row.get("current_price") for row in inventory_rows]
            + [row.get("current_price") for row in open_rows]
        ),
        "cost_basis": _avg([_num(row.get("cost_per_share")) for row in inventory_rows]),
        "supporting_signals": supporting_signals,
        "current_state": _current_state(ticker, open_rows, inventory_rows, metrics),
        "_open_rows": open_rows,
        "_inventory_rows": inventory_rows,
    }


def _current_state(
    ticker: str,
    open_rows: list[dict[str, Any]],
    inventory_rows: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    assigned_shares = _sum([abs(_num(row.get("shares")) or 0) for row in inventory_rows])
    assigned_dates = [
        parsed.isoformat()
        for parsed in [
            _parse_date(
                row.get("assignment_date")
                or row.get("assigned_date")
                or row.get("buy_date")
                or row.get("first_buy_date")
                or row.get("latest_buy_date")
                or row.get("open_date")
                or row.get("date")
                or row.get("lot_date")
            )
            for row in inventory_rows
        ]
        if parsed is not None
    ]
    option_bits = []
    for row in open_rows[:4]:
        option_bits.append(
            {
                "type": str(row.get("option_type") or row.get("put_call") or ""),
                "strike": _num(row.get("strike")),
                "expiry": row.get("expiration"),
                "dte": _num(row.get("days_to_expiration")),
                "quantity": _num(row.get("quantity")),
                "accounting_open_premium": _open_short_accounting_premium(row),
                "realized_premium_already_booked": _open_short_booked_premium(row),
                "strategy_premium_collected": _open_short_strategy_premium(row),
            }
        )
    return {
        "ticker": ticker,
        "current_price": _first_num(
            [metrics.get("current_price")]
            + [row.get("current_price") for row in inventory_rows]
            + [row.get("current_price") for row in open_rows]
        ),
        "assigned_shares": assigned_shares,
        "assignment_date": max(assigned_dates) if assigned_dates else None,
        "cost_basis": _avg([_num(row.get("cost_per_share")) for row in inventory_rows]),
        "realized_pnl": _realized_pnl_from_metrics(metrics),
        "current_unrealized": _num(metrics.get("unrealized_pnl")) or 0,
        "ticker_total": _total_pnl_from_metrics(metrics),
        "open_contracts": int(_sum([abs(_num(row.get("quantity")) or 0) for row in open_rows])),
        "open_options": option_bits,
    }


def _active_cycle(payload: dict[str, Any]) -> dict[str, Any]:
    canonical = ((payload.get("monthly") or {}).get("active_cycle") or {})
    if not canonical:
        canonical = ((payload.get("dashboard") or {}).get("monthly_target") or {}).get("cycle_projection") or {}
    if canonical:
        out = dict(canonical)
        target_return = _num(out.get("target_return")) or _num((payload.get("web") or {}).get("target_return")) or 0.02
        target_floor = _num((payload.get("web") or {}).get("target_floor")) or _num(out.get("target_floor")) or 0.01
        out["projected_pnl"] = _num(out.get("projected_cycle_pnl"))
        out["target_return"] = target_return
        out["target_floor"] = target_floor
        out["remaining_to_target"] = _num(out.get("remaining_to_target"))
        out["projected_return_roac"] = _num(out.get("projected_return_roac"))
        return out

    open_rows = _open_short_rows(payload)
    today = _as_of_date(payload)
    dated_rows = [(row, _parse_date(row.get("expiration"))) for row in open_rows]
    future_rows = [(row, expiry) for row, expiry in dated_rows if expiry is not None and expiry >= today]
    if not future_rows:
        future_rows = [(row, expiry) for row, expiry in dated_rows if expiry is not None]
    if future_rows:
        active_year_month = min((expiry.year, expiry.month) for _row, expiry in future_rows)
        cycle_rows = [
            row
            for row, expiry in future_rows
            if expiry is not None and (expiry.year, expiry.month) == active_year_month
        ]
        expiries = sorted({expiry for _row, expiry in future_rows if expiry and (expiry.year, expiry.month) == active_year_month})
    else:
        cycle_rows = []
        expiries = []
        active_year_month = None

    monthly_target = (payload.get("dashboard") or {}).get("monthly_target") or {}
    cycle_month_row = _active_cycle_month_row(payload, active_year_month)
    target_return = _num(monthly_target.get("target_return")) or _num((payload.get("web") or {}).get("target_return")) or 0.02
    target_floor = _num((payload.get("web") or {}).get("target_floor")) or _num(monthly_target.get("target_floor")) or 0.01
    portfolio_puts = [row for row in open_rows if _option_type(row).startswith("put")]
    cycle_puts = [row for row in cycle_rows if _option_type(row).startswith("put")]
    portfolio_put_exposure = _put_exposure(portfolio_puts)
    cycle_put_exposure = _put_exposure(cycle_puts)
    open_premium_collected = _first_num(
        [
            cycle_month_row.get("open_expiring_incremental_premium") if cycle_month_row else None,
            cycle_month_row.get("open_expiring_option_premium") if cycle_month_row else None,
        ]
    )
    if open_premium_collected is None:
        open_premium_collected = _sum([_open_short_accounting_premium(row) for row in cycle_rows])
    realized_cycle_pnl = _first_num(
        [
            cycle_month_row.get("realized_month_pnl") if cycle_month_row else None,
            cycle_month_row.get("total_realized_pnl") if cycle_month_row else None,
        ]
    )
    if realized_cycle_pnl is None:
        realized_cycle_pnl = 0.0
    snapshot = (payload.get("dashboard") or {}).get("snapshot") or {}
    stock_unrealized_pnl = _num(snapshot.get("current_stock_unrealized_pnl")) or 0.0
    itm_put_unrealized_loss = _sum(
        [gap for gap in [_open_short_assignment_gap(row) for row in cycle_puts] if gap < 0]
    )
    itm_call_stock_pnl = _itm_call_assignment_stock_pnl(cycle_rows, _inventory_rows(payload))
    projected_pnl = _num(cycle_month_row.get("projected_month_pnl")) if cycle_month_row else None
    if projected_pnl is None:
        projected_pnl = realized_cycle_pnl + open_premium_collected
    projected_pnl = projected_pnl + itm_put_unrealized_loss + itm_call_stock_pnl
    covered_call_upside_foregone = _sum(
        [
            gap
            for gap in [_covered_call_upside_foregone(row) for row in cycle_rows if _option_type(row).startswith("call")]
            if gap < 0
        ]
    )
    target_base = _first_num(
        [
            cycle_month_row.get("avg_capital") if cycle_month_row else None,
            _avg_capital_from_target(monthly_target),
            _latest_monthly_capital(payload),
        ]
    )
    if target_base:
        target_pnl = target_base * target_return
    elif cycle_month_row:
        target_pnl = _num(cycle_month_row.get("target_pnl"))
    else:
        target_pnl = _num(monthly_target.get("target_pnl"))
    return {
        "cycle": f"{active_year_month[0]:04d}-{active_year_month[1]:02d}" if active_year_month else None,
        "cycle_label": _cycle_label(active_year_month),
        "expiry_dates": [expiry.isoformat() for expiry in expiries],
        "min_dte": min([_num(row.get("days_to_expiration")) for row in cycle_rows if _num(row.get("days_to_expiration")) is not None], default=None),
        "max_dte": max([_num(row.get("days_to_expiration")) for row in cycle_rows if _num(row.get("days_to_expiration")) is not None], default=None),
        "open_ticker_count": len({str(row.get("ticker") or "").upper() for row in cycle_rows if row.get("ticker")}),
        "open_contract_count": int(_sum([abs(_num(row.get("quantity")) or 0) for row in cycle_rows])),
        "realized_cycle_pnl": realized_cycle_pnl,
        "open_premium_collected": open_premium_collected,
        "stock_unrealized_pnl": stock_unrealized_pnl,
        "itm_call_stock_pnl": itm_call_stock_pnl,
        "itm_put_unrealized_loss": itm_put_unrealized_loss,
        "covered_call_upside_foregone": covered_call_upside_foregone,
        "projected_pnl": projected_pnl,
        "target_return": target_return,
        "target_floor": target_floor,
        "target_base": target_base,
        "target_basis": "avg_capital" if target_base else None,
        "target_pnl": target_pnl,
        "remaining_to_target": max((target_pnl or 0) - projected_pnl, 0) if target_pnl is not None else None,
        "projected_return_roac": projected_pnl / target_base if target_base else None,
        "portfolio_put_exposure": portfolio_put_exposure,
        "portfolio_itm_put_exposure": _put_exposure([row for row in portfolio_puts if _open_short_assignment_gap(row) < 0]),
        "cycle_put_exposure": cycle_put_exposure,
        "cycle_itm_put_exposure": _put_exposure([row for row in cycle_puts if _open_short_assignment_gap(row) < 0]),
        "near_strike_put_exposure": _put_exposure(
            [
                row
                for row in cycle_puts
                if _open_short_assignment_gap(row) >= 0
                and _num(row.get("moneyness")) is not None
                and abs(_num(row.get("moneyness")) or 0) <= 0.05
            ]
        ),
    }


def _combine_historical_matches(
    probability_matches: list[dict[str, Any]],
    historical_enrichments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not historical_enrichments:
        return probability_matches
    combined: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for doc in historical_enrichments:
        match = historical_enrichment_to_probability_match(doc)
        trade_id = str((match.get("trade") or {}).get("trade_id") or match.get("match_id") or "")
        if not trade_id:
            continue
        combined[trade_id] = match
        order.append(trade_id)

    for match in probability_matches:
        trade = match.get("trade") if isinstance(match.get("trade"), dict) else {}
        trade_id = str(trade.get("trade_id") or match.get("trade_id") or match.get("match_id") or "")
        if not trade_id or trade_id not in combined:
            key = trade_id or str(match.get("match_id") or len(order))
            combined[key] = dict(match)
            order.append(key)
            continue
        existing = combined[trade_id]
        existing_trade = existing.get("trade") if isinstance(existing.get("trade"), dict) else {}
        if existing_trade.get("profit_probability") is None and trade.get("profit_probability") is not None:
            existing_trade = {**existing_trade, "profit_probability": trade.get("profit_probability")}
            probability = _num(trade.get("profit_probability"))
            if probability is not None:
                existing_trade["assignment_risk_proxy"] = 1.0 - probability
            existing["trade"] = existing_trade
            existing["profit_probability"] = existing_trade.get("profit_probability")
            existing["assignment_risk_proxy"] = existing_trade.get("assignment_risk_proxy")
        if match.get("probability_row_id") is not None:
            existing["probability_row_id"] = match.get("probability_row_id")
        existing["sheet_probability_matched"] = bool(match.get("matched"))
        existing["matched"] = bool(existing.get("provider_contract_matched") or match.get("matched"))

    return [combined[key] for key in order if key in combined]


def _enrich_probability_matches(payload: dict[str, Any], matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not matches:
        return []
    ticker_metrics = {
        str(row.get("ticker") or "").upper(): row
        for row in (payload.get("tickers") or {}).get("items") or []
        if row.get("ticker")
    }
    inventory_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _inventory_rows(payload):
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            inventory_by_ticker[ticker].append(row)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    enriched = [dict(match) for match in matches]
    for match in enriched:
        if not match.get("matched"):
            continue
        ticker = str((match.get("trade") or {}).get("ticker") or match.get("ticker") or "").upper()
        if ticker:
            grouped[ticker].append(match)

    for ticker, rows in grouped.items():
        metrics = ticker_metrics.get(ticker, {})
        weights = [_opening_premium(row) or _trade_capital(row) or 1.0 for row in rows]
        weight_total = sum(weights) or float(len(rows) or 1)
        put_rows = [row for row in rows if _match_put_call(row).startswith("put")]
        put_weights = [_opening_premium(row) or _trade_capital(row) or 1.0 for row in put_rows]
        put_weight_total = sum(put_weights) or float(len(put_rows) or 1)

        realized_options = _metric_present(metrics, "realized_options_pnl")
        realized_stock = _metric_present(metrics, "realized_stock_pnl", "stock_pnl")
        dividends = _metric_present(metrics, "dividends", "dividend_pnl")
        ticker_unrealized = _metric_present(metrics, "unrealized_pnl")
        assigned_unrealized = _sum_or_none([_num(row.get("unrealized_pnl")) for row in inventory_by_ticker.get(ticker, [])])
        if assigned_unrealized is None:
            assigned_unrealized = ticker_unrealized

        for row, weight in zip(rows, weights):
            share = weight / weight_total if weight_total else 0
            put_share = 0.0
            if row in put_rows:
                put_weight = _opening_premium(row) or _trade_capital(row) or 1.0
                put_share = put_weight / put_weight_total if put_weight_total else 0
            option_pnl = realized_options * share if realized_options is not None else None
            stock_pnl = realized_stock * put_share if realized_stock is not None and put_share else None
            dividend_pnl = dividends * put_share if dividends is not None and put_share else None
            unrealized_pnl = assigned_unrealized * put_share if assigned_unrealized is not None and put_share else None
            row["option_pnl"] = option_pnl
            row["stock_pnl"] = stock_pnl
            row["dividends"] = dividend_pnl
            row["unrealized_pnl"] = unrealized_pnl
            row["lifecycle_pnl"] = _sum_or_none([option_pnl, stock_pnl, dividend_pnl, unrealized_pnl])
            row["attribution"] = {
                "option": option_pnl is not None,
                "stock": stock_pnl is not None,
                "dividends": dividend_pnl is not None,
                "unrealized": unrealized_pnl is not None,
                "full_lifecycle": option_pnl is not None
                and (not _match_put_call(row).startswith("put") or unrealized_pnl is not None or stock_pnl is not None),
                "method": "ticker_weighted_best_effort",
            }
    return enriched


def _strike_quality(probability_matches: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [match for match in probability_matches if match.get("matched")]
    puts = [match for match in matched if _match_put_call(match).startswith("put")]
    calls = [match for match in matched if _match_put_call(match).startswith("call")]
    attributed = [match for match in matched if (match.get("attribution") or {}).get("option")]
    stock_attributed = [match for match in matched if (match.get("attribution") or {}).get("stock")]
    full_attributed = [match for match in matched if (match.get("attribution") or {}).get("full_lifecycle")]
    provider_rows = [match for match in probability_matches if match.get("provider")]
    provider_contract_rows = [match for match in provider_rows if match.get("historical_provider_contract_matched")]
    provider_price_rows = [
        match
        for match in provider_rows
        if _num(match.get("option_close")) is not None or _num(match.get("option_vwap")) is not None
    ]
    risk_rows = [match for match in probability_matches if _assignment_risk(match) is not None]
    return {
        "coverage": {
            "matched_count": len(matched),
            "trade_count": len(probability_matches),
            "coverage_rate": _ratio(len(matched), len(probability_matches)),
            "risk_proxy_count": len(risk_rows),
            "risk_proxy_rate": _ratio(len(risk_rows), len(probability_matches)),
            "historical_provider_trade_count": len(provider_rows),
            "historical_provider_contract_match_count": len(provider_contract_rows),
            "historical_provider_contract_match_rate": _ratio(len(provider_contract_rows), len(provider_rows)),
            "historical_provider_option_price_count": len(provider_price_rows),
            "historical_provider_option_price_rate": _ratio(len(provider_price_rows), len(provider_rows)),
            "option_lifecycle_attributed_count": len(attributed),
            "stock_outcome_attributed_count": len(stock_attributed),
            "full_lifecycle_attributed_count": len(full_attributed),
            "option_lifecycle_attribution_rate": _ratio(len(attributed), len(matched)),
            "stock_outcome_attribution_rate": _ratio(len(stock_attributed), len(matched)),
            "full_lifecycle_attribution_rate": _ratio(len(full_attributed), len(matched)),
        },
        "put_entry_quality": {
            "title": "Put Entry Quality",
            "bucket_summary": _bucket_lifecycle_summary(puts, side="put"),
        },
        "call_exit_quality": {
            "title": "Call / Exit Quality",
            "bucket_summary": _bucket_lifecycle_summary(calls, side="call"),
        },
    }


def _bucket_lifecycle_summary(rows: list[dict[str, Any]], *, side: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for match in rows:
        risk = _assignment_risk(match)
        if risk is not None:
            buckets[_risk_bucket(risk)].append(match)

    out = []
    for label, _lower, _upper in RISK_BUCKETS:
        bucket_rows = buckets.get(label, [])
        premiums = [_opening_premium(match) for match in bucket_rows]
        capital = [_trade_capital(match) for match in bucket_rows]
        total_premium = _sum(premiums)
        total_capital = _sum(capital)
        avg_risk = _avg([_assignment_risk(match) for match in bucket_rows]) or 0
        option_pnl = _sum_or_none([_match_metric(match, "option_pnl", "realized_options_pnl") for match in bucket_rows])
        stock_pnl = _sum_or_none([_match_metric(match, "stock_pnl", "realized_stock_pnl", "stock_realized_pnl") for match in bucket_rows])
        dividends = _sum_or_none([_match_metric(match, "dividends", "dividend_pnl", "dividends_net") for match in bucket_rows])
        unrealized_drag = _sum_or_none([_match_metric(match, "unrealized_pnl", "current_unrealized_pnl", "open_unrealized_pnl") for match in bucket_rows])
        lifecycle_values = [_match_metric(match, "total_pnl", "lifecycle_pnl", "combined_pnl") for match in bucket_rows]
        total_lifecycle = _sum_or_none(lifecycle_values)
        attributed_count = sum(1 for match in bucket_rows if (match.get("attribution") or {}).get("option"))
        full_lifecycle_count = sum(1 for match in bucket_rows if (match.get("attribution") or {}).get("full_lifecycle"))
        out.append(
            {
                "bucket": label,
                "count": len(bucket_rows),
                "avg_profit_probability": _avg([_profit_probability(match) for match in bucket_rows]),
                "avg_assignment_risk_proxy": _avg([_assignment_risk(match) for match in bucket_rows]),
                "opening_premium": total_premium,
                "option_pnl": option_pnl,
                "stock_pnl": stock_pnl,
                "dividends": dividends,
                "unrealized_drag": unrealized_drag,
                "lifecycle_pnl": total_lifecycle,
                "pnl_per_capital": total_lifecycle / total_capital if total_lifecycle is not None and total_capital else None,
                "assignment_rate": avg_risk if bucket_rows else None,
                "attributed_count": attributed_count,
                "full_lifecycle_count": full_lifecycle_count,
                "attribution_rate": _ratio(attributed_count, len(bucket_rows)),
                "full_lifecycle_attribution_rate": _ratio(full_lifecycle_count, len(bucket_rows)),
                "top_tickers": _top_tickers(bucket_rows),
            }
        )
    return out


def _coverage_notes(
    payload: dict[str, Any],
    probability_matches: list[dict[str, Any]],
    option_market_data: dict[str, Any],
) -> list[dict[str, Any]]:
    notes = []
    coverage = ((payload.get("dashboard") or {}).get("data_freshness") or {}).get("price_coverage") or {}
    missing = int(_num(coverage.get("missing_count")) or 0)
    requested = int(_num(coverage.get("stocks_requested")) or 0)
    fetched = int(_num(coverage.get("stocks_fetched")) or 0)
    if requested:
        notes.append({"severity": "status", "message": f"Price coverage: {fetched}/{requested} tickers."})
    if missing:
        notes.append({"severity": "status", "message": f"Missing prices: {missing}."})
    matched = sum(1 for match in probability_matches if match.get("matched"))
    risk_proxy = sum(1 for match in probability_matches if _assignment_risk(match) is not None)
    provider_rows = [match for match in probability_matches if match.get("provider")]
    provider_contract_rows = [match for match in provider_rows if match.get("historical_provider_contract_matched")]
    provider_price_rows = [
        match
        for match in provider_rows
        if _num(match.get("option_close")) is not None or _num(match.get("option_vwap")) is not None
    ]
    if provider_rows:
        notes.append(
            {
                "severity": "status",
                "message": (
                    "Historical option facts: "
                    f"{len(provider_rows)} trades, {len(provider_contract_rows)} contract matches, "
                    f"{len(provider_price_rows)} option price observations."
                ),
            }
        )
    if probability_matches:
        attributed = sum(1 for match in probability_matches if (match.get("attribution") or {}).get("option"))
        stock_attributed = sum(1 for match in probability_matches if (match.get("attribution") or {}).get("stock"))
        full_attributed = sum(1 for match in probability_matches if (match.get("attribution") or {}).get("full_lifecycle"))
        notes.append(
            {
                "severity": "status",
                "message": f"Historical risk proxy: {risk_proxy}/{len(probability_matches)} short-option opening trades.",
            }
        )
        notes.append(
            {
                "severity": "status",
                "message": f"Lifecycle attribution: option {attributed}/{matched}, stock {stock_attributed}/{matched}, full {full_attributed}/{matched}.",
            }
        )
    else:
        notes.append({"severity": "status", "message": "Historical risk proxy: unavailable."})
    option_status = option_market_data.get("status") or {}
    if option_status:
        source = option_status.get("source") or "none"
        provider = option_status.get("provider") or "n/a"
        contract_count = option_status.get("contract_count") or 0
        last_fetched = option_status.get("last_fetched_at") or "not fetched"
        quote_coverage = _ratio(
            int(_num(option_status.get("quote_coverage_count")) or 0),
            int(_num(option_status.get("contract_count")) or 0),
        )
        greek_coverage = _ratio(
            int(_num(option_status.get("greek_coverage_count")) or 0),
            int(_num(option_status.get("contract_count")) or 0),
        )
        notes.append(
            {
                "severity": "status",
                "message": f"Option data: {provider}, {source}, {contract_count} contracts, last fetched {last_fetched}.",
            }
        )
        notes.append(
            {
                "severity": "status",
                "message": f"Option coverage: quotes {fmt_pct_for_note(quote_coverage)}, greeks {fmt_pct_for_note(greek_coverage)}.",
            }
        )
    else:
        notes.append({"severity": "status", "message": "Option data: no stored provider data loaded."})
    return notes


def _open_short_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        ((payload.get("positions") or {}).get("open_option_shorts"))
        or ((payload.get("open_shorts") or {}).get("items"))
        or []
    )


def _inventory_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return ((payload.get("positions") or {}).get("inventory")) or ((payload.get("tables") or {}).get("inventory")) or []


def _option_type(row: dict[str, Any]) -> str:
    return str(row.get("option_type") or row.get("put_call") or "").lower()


def _open_short_accounting_premium(row: dict[str, Any]) -> float:
    premium = _num(row.get("accounting_open_premium"))
    if premium is not None:
        return premium
    open_price = _num(row.get("open_price"))
    quantity = abs(_num(row.get("quantity")) or _num(row.get("qty")) or 0)
    if open_price is None or not quantity:
        return 0.0
    return open_price * quantity * 100


def _open_short_strategy_premium(row: dict[str, Any]) -> float:
    premium = _num(row.get("strategy_premium_collected"))
    if premium is not None:
        return premium
    strategy_open_price = _num(row.get("strategy_open_price"))
    quantity = abs(_num(row.get("quantity")) or _num(row.get("qty")) or 0)
    if strategy_open_price is not None and quantity:
        return strategy_open_price * quantity * 100
    return _open_short_accounting_premium(row)


def _open_short_booked_premium(row: dict[str, Any]) -> float:
    premium = _num(row.get("realized_premium_already_booked"))
    if premium is not None:
        return premium
    return _open_short_strategy_premium(row) - _open_short_accounting_premium(row)


def _open_short_assignment_gap(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or current is None or not qty or not _option_type(row).startswith("put"):
        return 0.0
    return -max(strike - current, 0) * 100 * qty


def _open_short_put_otm_distance(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or current is None or not qty or not _option_type(row).startswith("put"):
        return 0.0
    return max(current - strike, 0) * 100 * qty


def _open_short_put_otm_ratio(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    if strike is None or strike <= 0 or current is None or not _option_type(row).startswith("put"):
        return 0.0
    return max(current - strike, 0) / strike


def _covered_call_upside_foregone(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or current is None or not qty or not _option_type(row).startswith("call"):
        return 0.0
    return -max(current - strike, 0) * 100 * qty


def _itm_call_assignment_stock_pnl(option_rows: list[dict[str, Any]], inventory_rows: list[dict[str, Any]]) -> float:
    inventory_by_ticker: dict[str, list[dict[str, float]]] = {}
    for row in inventory_rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        shares = _num(row.get("shares"))
        cost = _num(row.get("cost_per_share"))
        if not ticker or shares is None or shares <= 0 or cost is None:
            continue
        inventory_by_ticker.setdefault(ticker, []).append({"shares": float(shares), "cost": float(cost)})

    total = 0.0
    call_rows = sorted(
        [row for row in option_rows if _option_type(row).startswith("call")],
        key=lambda row: (_num(row.get("strike")) or 0.0),
    )
    for row in call_rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        strike = _num(row.get("strike"))
        current = _num(row.get("current_price"))
        qty = abs(_num(row.get("quantity")) or 0)
        if not ticker or strike is None or current is None or current <= strike or not qty:
            continue
        shares_needed = qty * 100
        queue = inventory_by_ticker.get(ticker, [])
        while shares_needed > 0 and queue:
            lot = queue[0]
            use = min(shares_needed, lot["shares"])
            total += (strike - lot["cost"]) * use
            lot["shares"] -= use
            shares_needed -= use
            if lot["shares"] <= 0:
                queue.pop(0)
    return float(total)


def _put_exposure(rows: list[dict[str, Any]]) -> float:
    return _sum([_cash_required_if_assigned(row) for row in rows])


def _cash_required_if_assigned(row: dict[str, Any]) -> Optional[float]:
    if not _option_type(row).startswith("put"):
        return None
    strike = _num(row.get("strike"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or not qty:
        return None
    return strike * 100 * qty


def _active_cycle_month_row(payload: dict[str, Any], year_month: Optional[tuple[int, int]]) -> dict[str, Any]:
    if not year_month:
        return {}
    candidates = []
    monthly = payload.get("monthly") or {}
    candidates.extend(monthly.get("future_months") or [])
    candidates.extend(monthly.get("months") or [])
    for row in candidates:
        month = _parse_date(row.get("month") or str(row.get("id") or "").replace("month:", ""))
        if month and (month.year, month.month) == year_month:
            return row
    return {}


def _avg_capital_from_target(monthly_target: dict[str, Any]) -> Optional[float]:
    target_pnl = _num(monthly_target.get("target_pnl"))
    target_return = _num(monthly_target.get("target_return"))
    if target_pnl is None or not target_return:
        return None
    return target_pnl / target_return


def _latest_monthly_capital(payload: dict[str, Any]) -> Optional[float]:
    dated_rows = []
    for row in (payload.get("monthly") or {}).get("months") or []:
        month = _parse_date(row.get("month") or str(row.get("id") or "").replace("month:", ""))
        capital = _first_num([row.get("avg_capital"), row.get("peak_capital")])
        if month and capital is not None:
            dated_rows.append((month, capital))
    if not dated_rows:
        return None
    return max(dated_rows, key=lambda item: item[0])[1]


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


def _match_metric(match: dict[str, Any], *names: str) -> Optional[float]:
    trade = match.get("trade") or {}
    for name in names:
        value = _num(match.get(name))
        if value is not None:
            return value
        value = _num(trade.get(name))
        if value is not None:
            return value
    return None


def _metric_present(row: dict[str, Any], *names: str) -> Optional[float]:
    for name in names:
        if name in row:
            return _num(row.get(name))
    return None


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


def _match_put_call(match: dict[str, Any]) -> str:
    trade = match.get("trade") or {}
    return str(match.get("put_call") or trade.get("put_call") or "").lower()


def _top_tickers(rows: list[dict[str, Any]]) -> str:
    tickers = Counter(str((row.get("trade") or {}).get("ticker") or "").upper() for row in rows)
    return ", ".join([ticker for ticker, _count in tickers.most_common(4) if ticker])


def _as_of_date(payload: dict[str, Any]) -> date:
    request = ((payload.get("dashboard") or {}).get("request") or {})
    parsed = _parse_date(request.get("as_of"))
    return parsed or date.today()


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _cycle_label(year_month: Optional[tuple[int, int]]) -> str:
    if not year_month:
        return "No active cycle"
    return date(year_month[0], year_month[1], 1).strftime("%B %Y")


def _standard_monthly_expiries(count: int) -> list[str]:
    today = date.today()
    expiries: list[str] = []
    year = today.year
    month = today.month
    while len(expiries) < count and len(expiries) < 12:
        expiry = _third_friday(year, month)
        expiry = _adjust_us_option_expiry_holiday(expiry)
        if expiry >= today + timedelta(days=7):
            expiries.append(expiry.isoformat())
        month += 1
        if month > 12:
            month = 1
            year += 1
    return expiries


def _third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    days_to_friday = (4 - first.weekday()) % 7
    return first + timedelta(days=days_to_friday + 14)


def _adjust_us_option_expiry_holiday(value: date) -> date:
    # Juneteenth is a US market holiday; listed monthly equity options expire on the previous trading day.
    if value.month == 6 and value.day == 19:
        return value - timedelta(days=1)
    return value


def _first_num(values: list[Any]) -> Optional[float]:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
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


def _sum_or_none(values: list[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    return sum(clean) if clean else None


def _avg(values: list[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None


def _money(value: Optional[float], digits: int = 0) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.{digits}f}"


def fmt_pct_for_note(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"
