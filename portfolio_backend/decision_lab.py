from __future__ import annotations

import hashlib
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
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
    ticker_situations = _ticker_situations(dashboard_payload)
    active_cycle = _active_cycle(dashboard_payload)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": dashboard_payload.get("source", {}),
        "data_freshness": (dashboard_payload.get("dashboard") or {}).get("data_freshness", {}),
        "summary": _summary(dashboard_payload, probability_matches, ticker_situations, active_cycle),
        "ticker_situations": ticker_situations,
        "active_cycle": active_cycle,
        "recommendation_candidates": _recommendation_candidates(ticker_situations),
        "strike_quality": _strike_quality(probability_matches),
        "coverage_notes": _coverage_notes(dashboard_payload, probability_matches),
    }


def _summary(
    payload: dict[str, Any],
    probability_matches: list[dict[str, Any]],
    ticker_situations: list[dict[str, Any]],
    active_cycle: dict[str, Any],
) -> dict[str, Any]:
    dashboard = payload.get("dashboard") or {}
    snapshot = dashboard.get("snapshot") or {}
    issues = dashboard.get("issue_summary") or {}
    matched = [match for match in probability_matches if match.get("matched")]
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
    }


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
    situations.sort(
        key=lambda item: (
            priority_order.get(str(item.get("priority")), 9),
            _num(item.get("impact")) or 0,
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
    itm_puts = [row for row in open_puts if _open_short_intrinsic_gap(row) < 0]
    near_puts = [
        row
        for row in open_puts
        if _open_short_intrinsic_gap(row) >= 0 and (_num(row.get("moneyness")) is not None and abs(_num(row.get("moneyness")) or 0) <= 0.05)
    ]
    capped_calls = [row for row in open_calls if _open_short_intrinsic_gap(row) < 0]
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
    total_pnl = _num(metrics.get("total_pnl")) or 0
    options_pnl = _num(metrics.get("realized_options_pnl")) or 0
    unrealized_pnl = _num(metrics.get("unrealized_pnl")) or 0
    realized_pnl = (_num(metrics.get("combined_realized_pnl")) or 0) or (
        (_num(metrics.get("realized_options_pnl")) or 0)
        + (_num(metrics.get("realized_stock_pnl")) or 0)
        + (_num(metrics.get("dividends")) or 0)
    )

    if itm_puts:
        primary = min(itm_puts, key=lambda row: _num(row.get("days_to_expiration")) or 9999)
        return _situation(
            ticker=ticker,
            priority="high",
            category="Reduce assignment risk",
            objective="Reduce near-term put assignment risk",
            reason="Open put is in the money",
            impact=sum(_open_short_projected_pnl(row) or 0 for row in itm_puts),
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
            impact=_num(worst.get("unrealized_pnl")),
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
            impact=sum(_open_short_projected_pnl(row) or 0 for row in capped_calls) if capped_calls else holding_unrealized,
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
            impact=sum(_open_short_projected_pnl(row) or 0 for row in near_puts),
            expiry=primary.get("expiration"),
            dte=_num(primary.get("days_to_expiration")),
            recommendation="Monitor or pre-plan roll",
            source="ticker-level",
            open_rows=open_rows,
            inventory_rows=inv_rows,
            metrics=metrics,
            supporting_signals=[f"{len(near_puts)} near-strike put(s)"],
        )

    if options_pnl > 0 and total_pnl < 0 and (open_rows or inv_rows):
        return _situation(
            ticker=ticker,
            priority="medium",
            category="Pause new entries",
            objective="Avoid adding risk while recovery is active",
            reason="Positive premium but negative total lifecycle P&L",
            impact=total_pnl,
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
        impact=total_pnl,
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
    impact: Optional[float],
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
        "impact": impact,
        "expiry": expiry,
        "dte": dte,
        "recommendation": recommendation,
        "source": source,
        "open_contract_count": len(open_rows),
        "assigned_lot_count": len(inventory_rows),
        "realized_pnl": (
            (_num(metrics.get("combined_realized_pnl")) or 0)
            or (_num(metrics.get("realized_options_pnl")) or 0)
            + (_num(metrics.get("realized_stock_pnl")) or 0)
            + (_num(metrics.get("dividends")) or 0)
        ),
        "total_pnl": _num(metrics.get("total_pnl")) or 0,
        "unrealized_pnl": _num(metrics.get("unrealized_pnl")) or 0,
        "current_price": _first_num(
            [metrics.get("current_price")]
            + [row.get("current_price") for row in inventory_rows]
            + [row.get("current_price") for row in open_rows]
        ),
        "cost_basis": _avg([_num(row.get("cost_per_share")) for row in inventory_rows]),
        "supporting_signals": supporting_signals,
        "_open_rows": open_rows,
        "_inventory_rows": inventory_rows,
    }


def _active_cycle(payload: dict[str, Any]) -> dict[str, Any]:
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
    target_return = _num(monthly_target.get("target_return")) or _num((payload.get("web") or {}).get("target_return")) or 0.02
    portfolio_puts = [row for row in open_rows if _option_type(row).startswith("put")]
    cycle_puts = [row for row in cycle_rows if _option_type(row).startswith("put")]
    portfolio_put_exposure = _put_exposure(portfolio_puts)
    cycle_put_exposure = _put_exposure(cycle_puts)
    projected_pnl = _sum([_open_short_projected_pnl(row) for row in cycle_rows])
    target_base = cycle_put_exposure or portfolio_put_exposure or _num(monthly_target.get("target_pnl")) or 0
    target_pnl = target_base * target_return if target_base else _num(monthly_target.get("target_pnl"))
    return {
        "cycle": f"{active_year_month[0]:04d}-{active_year_month[1]:02d}" if active_year_month else None,
        "cycle_label": _cycle_label(active_year_month),
        "expiry_dates": [expiry.isoformat() for expiry in expiries],
        "min_dte": min([_num(row.get("days_to_expiration")) for row in cycle_rows if _num(row.get("days_to_expiration")) is not None], default=None),
        "max_dte": max([_num(row.get("days_to_expiration")) for row in cycle_rows if _num(row.get("days_to_expiration")) is not None], default=None),
        "open_contract_count": int(_sum([abs(_num(row.get("quantity")) or 0) for row in cycle_rows])),
        "open_option_net": projected_pnl,
        "premium_component": _sum([_open_short_premium(row) for row in cycle_rows]),
        "intrinsic_gap": _sum([_open_short_intrinsic_gap(row) for row in cycle_rows]),
        "projected_pnl": projected_pnl,
        "target_return": target_return,
        "target_pnl": target_pnl,
        "remaining_to_target": max((target_pnl or 0) - projected_pnl, 0) if target_pnl is not None else None,
        "projected_return_roac": projected_pnl / target_base if target_base else None,
        "portfolio_put_exposure": portfolio_put_exposure,
        "portfolio_itm_put_exposure": _put_exposure([row for row in portfolio_puts if _open_short_intrinsic_gap(row) < 0]),
        "cycle_put_exposure": cycle_put_exposure,
        "cycle_itm_put_exposure": _put_exposure([row for row in cycle_puts if _open_short_intrinsic_gap(row) < 0]),
        "near_strike_put_exposure": _put_exposure(
            [
                row
                for row in cycle_puts
                if _open_short_intrinsic_gap(row) >= 0
                and _num(row.get("moneyness")) is not None
                and abs(_num(row.get("moneyness")) or 0) <= 0.05
            ]
        ),
    }


def _recommendation_candidates(ticker_situations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for situation in ticker_situations:
        candidates = _simulated_candidates_for_situation(situation)
        if not candidates:
            continue
        rows.append(
            {
                "ticker": situation["ticker"],
                "category": situation["category"],
                "objective": situation["objective"],
                "disclaimer": "Prototype only: simulated option-chain data; not a trading recommendation.",
                "recommended": candidates[0],
                "alternatives": candidates[1:3],
            }
        )
    return rows


def _simulated_candidates_for_situation(situation: dict[str, Any]) -> list[dict[str, Any]]:
    category = str(situation.get("category") or "")
    if category == "Recover with covered call":
        return _covered_call_candidates(situation, objective="Recover income")
    if category in {"Evaluate exit vs roll", "Roll to improve recovery", "Accept / monitor exit"}:
        return _roll_or_exit_candidates(situation)
    if category in {"Reduce assignment risk", "Monitor assignment risk"}:
        return _put_risk_candidates(situation)
    if category == "Pause new entries":
        return [_candidate("Pause new puts", None, None, 0, 0, None, None, "n/a", 78, "Active recovery state; do not add exposure until exit/recovery improves.")]
    return []


def _covered_call_candidates(situation: dict[str, Any], *, objective: str) -> list[dict[str, Any]]:
    ticker = str(situation.get("ticker") or "")
    current = _num(situation.get("current_price")) or _num(situation.get("cost_basis")) or 50
    cost = _num(situation.get("cost_basis")) or current
    base_dtes = [24, 38, 52]
    strikes = [_round_strike(max(cost, current * 1.03)), _round_strike(max(cost * 1.05, current * 1.06)), _round_strike(max(cost * 1.1, current * 1.1))]
    candidates = []
    for idx, (dte, strike) in enumerate(zip(base_dtes, strikes)):
        premium = _sim_premium(ticker, strike, current, dte, "call")
        exit_pnl = (strike - cost) * 100 + premium
        score = 58 + min(max(exit_pnl / 120, -18), 26) + (8 if strike >= cost else -15) - idx * 3
        candidates.append(
            _candidate(
                "Sell covered call",
                strike,
                _future_expiry(dte),
                dte,
                premium,
                _sim_delta(ticker, strike, current, "call"),
                _sim_iv(ticker),
                _sim_liquidity(ticker, idx),
                score,
                f"{objective}: strike {'above' if strike >= cost else 'below'} cost basis; estimated called-away P&L {_money(exit_pnl)}.",
            )
        )
    return sorted(candidates, key=lambda row: row["score"], reverse=True)


def _roll_or_exit_candidates(situation: dict[str, Any]) -> list[dict[str, Any]]:
    ticker = str(situation.get("ticker") or "")
    current = _num(situation.get("current_price")) or _num(situation.get("cost_basis")) or 50
    cost = _num(situation.get("cost_basis")) or current
    open_calls = [row for row in situation.get("_open_rows", []) if _option_type(row).startswith("call")]
    current_strike = _num(open_calls[0].get("strike")) if open_calls else None
    exit_strike = current_strike or _round_strike(max(cost, current))
    exit_pnl = (exit_strike - cost) * 100 + _sum([_open_short_premium(row) for row in open_calls])
    candidates = [
        _candidate(
            "Accept / monitor exit",
            exit_strike,
            open_calls[0].get("expiration") if open_calls else None,
            _num(open_calls[0].get("days_to_expiration")) if open_calls else None,
            _sum([_open_short_premium(row) for row in open_calls]),
            None,
            None,
            "current",
            64 + min(max(exit_pnl / 150, -18), 18),
            f"Current cap exits near {_money(exit_pnl)} estimated lifecycle result; avoids extending recovery.",
        )
    ]
    for idx, dte in enumerate([31, 45]):
        strike = _round_strike(max(current * (1.05 + idx * 0.04), cost * (1.02 + idx * 0.04), (current_strike or current) * 1.05))
        premium = _sim_premium(ticker, strike, current, dte, "call")
        unlocked = max(strike - (current_strike or current), 0) * 100
        score = 60 + min(unlocked / 90, 22) + min(premium / 60, 10) - idx * 4
        candidates.append(
            _candidate(
                "Roll up/out",
                strike,
                _future_expiry(dte),
                dte,
                premium,
                _sim_delta(ticker, strike, current, "call"),
                _sim_iv(ticker),
                _sim_liquidity(ticker, idx),
                score,
                f"Preserves about {_money(unlocked)} more upside before costs; compare against added time.",
            )
        )
    return sorted(candidates, key=lambda row: row["score"], reverse=True)


def _put_risk_candidates(situation: dict[str, Any]) -> list[dict[str, Any]]:
    ticker = str(situation.get("ticker") or "")
    puts = [row for row in situation.get("_open_rows", []) if _option_type(row).startswith("put")]
    current = _num(situation.get("current_price")) or 50
    strike = _num(puts[0].get("strike")) if puts else current
    candidates = [
        _candidate(
            "Close put risk",
            strike,
            puts[0].get("expiration") if puts else None,
            _num(puts[0].get("days_to_expiration")) if puts else None,
            -abs(_open_short_intrinsic_gap(puts[0])) if puts else 0,
            None,
            None,
            "current",
            62,
            "Removes assignment exposure; useful if cash risk is the priority.",
        )
    ]
    for idx, dte in enumerate([31, 45]):
        new_strike = _round_strike((strike or current) * (0.94 - idx * 0.03))
        credit = _sim_premium(ticker, new_strike, current, dte, "put")
        score = 60 + idx * 3 + max((strike or current) - new_strike, 0)
        candidates.append(
            _candidate(
                "Roll down/out",
                new_strike,
                _future_expiry(dte),
                dte,
                credit,
                _sim_delta(ticker, new_strike, current, "put"),
                _sim_iv(ticker),
                _sim_liquidity(ticker, idx),
                score,
                f"Reduces assignment strike by {_money((strike or current) - new_strike, 2)} while keeping premium opportunity.",
            )
        )
    return sorted(candidates, key=lambda row: row["score"], reverse=True)


def _candidate(
    action: str,
    strike: Optional[float],
    expiry: Any,
    dte: Optional[float],
    premium: Optional[float],
    delta: Optional[float],
    iv: Optional[float],
    liquidity: str,
    score: float,
    explanation: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "strike": strike,
        "expiry": expiry,
        "dte": dte,
        "premium": premium,
        "delta": delta,
        "iv": iv,
        "liquidity": liquidity,
        "score": round(max(0, min(100, score)), 1),
        "explanation": explanation,
        "is_simulated": True,
    }


def _strike_quality(probability_matches: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [match for match in probability_matches if match.get("matched")]
    puts = [match for match in matched if _match_put_call(match).startswith("put")]
    calls = [match for match in matched if _match_put_call(match).startswith("call")]
    return {
        "coverage": {
            "matched_count": len(matched),
            "trade_count": len(probability_matches),
            "coverage_rate": _ratio(len(matched), len(probability_matches)),
        },
        "put_entry_quality": {
            "title": "Put Entry Quality",
            "estimated": True,
            "bucket_summary": _bucket_lifecycle_summary(puts, side="put"),
        },
        "call_exit_quality": {
            "title": "Covered Call / Exit Quality",
            "estimated": True,
            "bucket_summary": _call_quality_summary(calls),
        },
        "note": "Estimated lifecycle attribution is prototype-only and must be verified with lot-level matching before production use.",
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
        stock_pnl = -total_capital * max(avg_risk - 0.22, 0) * 0.035 if side == "put" else 0
        dividends = total_capital * 0.0015 if side == "put" and bucket_rows else 0
        unrealized_drag = -total_capital * max(avg_risk - 0.25, 0) * 0.025 if side == "put" else 0
        total_lifecycle = total_premium + stock_pnl + dividends + unrealized_drag
        out.append(
            {
                "bucket": label,
                "count": len(bucket_rows),
                "avg_profit_probability": _avg([_profit_probability(match) for match in bucket_rows]),
                "avg_assignment_risk_proxy": _avg([_assignment_risk(match) for match in bucket_rows]),
                "opening_premium": total_premium,
                "stock_pnl_estimated": stock_pnl,
                "dividends_estimated": dividends,
                "unrealized_drag_estimated": unrealized_drag,
                "lifecycle_pnl_estimated": total_lifecycle,
                "pnl_per_capital_estimated": total_lifecycle / total_capital if total_capital else None,
                "assignment_rate_estimated": avg_risk if bucket_rows else None,
                "top_tickers": _top_tickers(bucket_rows),
            }
        )
    return out


def _call_quality_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "Low-delta income": [],
        "Balanced recovery": [],
        "Aggressive exit/cap": [],
    }
    for match in rows:
        risk = _assignment_risk(match) or 0
        if risk <= 0.20:
            groups["Low-delta income"].append(match)
        elif risk <= 0.35:
            groups["Balanced recovery"].append(match)
        else:
            groups["Aggressive exit/cap"].append(match)
    out = []
    for label, bucket_rows in groups.items():
        premium = _sum([_opening_premium(match) for match in bucket_rows])
        capital = _sum([_trade_capital(match) for match in bucket_rows])
        avg_risk = _avg([_assignment_risk(match) for match in bucket_rows])
        capped_upside = -(capital * max((avg_risk or 0) - 0.25, 0) * 0.015)
        exit_pnl = premium + capped_upside
        out.append(
            {
                "bucket": label,
                "count": len(bucket_rows),
                "avg_assignment_risk_proxy": avg_risk,
                "opening_premium": premium,
                "capped_upside_estimated": capped_upside,
                "exit_pnl_estimated": exit_pnl,
                "pnl_per_capital_estimated": exit_pnl / capital if capital else None,
                "roll_usefulness": "higher" if label == "Aggressive exit/cap" else "medium" if label == "Balanced recovery" else "low",
                "top_tickers": _top_tickers(bucket_rows),
            }
        )
    return out


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
    notes.append(
        {
            "severity": "warning",
            "message": "Recommendation candidates use simulated option-chain data in this prototype and are not trading recommendations.",
        }
    )
    notes.append(
        {
            "severity": "warning",
            "message": "Lifecycle attribution by risk bucket is estimated until verified with lot-level matching tests.",
        }
    )
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


def _open_short_projected_pnl(row: dict[str, Any]) -> Optional[float]:
    premium = _open_short_premium(row)
    gap = _open_short_intrinsic_gap(row)
    return premium + gap


def _open_short_premium(row: dict[str, Any]) -> float:
    premium = _num(row.get("display_premium_collected"))
    if premium is None:
        premium = _num(row.get("roll_adjusted_premium_collected"))
    if premium is None:
        premium = _num(row.get("premium_collected"))
    return premium or 0


def _open_short_intrinsic_gap(row: dict[str, Any]) -> float:
    strike = _num(row.get("strike"))
    current = _num(row.get("current_price"))
    qty = abs(_num(row.get("quantity")) or 0)
    if strike is None or current is None or not qty:
        return 0.0
    option_type = _option_type(row)
    if option_type.startswith("call"):
        return -max(current - strike, 0) * 100 * qty
    return -max(strike - current, 0) * 100 * qty


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


def _future_expiry(dte: int) -> str:
    return (date.today() + timedelta(days=dte)).isoformat()


def _round_strike(value: float) -> float:
    if value >= 100:
        step = 5
    elif value >= 50:
        step = 2.5
    else:
        step = 1
    return round(round(value / step) * step, 2)


def _stable_unit(ticker: str, salt: str = "") -> float:
    digest = hashlib.sha256(f"{ticker}:{salt}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _sim_premium(ticker: str, strike: float, current: float, dte: float, option_type: str) -> float:
    distance = abs(strike - current) / max(current, 1)
    iv = _sim_iv(ticker) or 0.35
    base = current * iv * math.sqrt(max(dte, 1) / 365) * max(0.08, 0.22 - distance)
    if option_type == "put":
        base *= 1.08
    return round(max(base * 100, 5), 2)


def _sim_delta(ticker: str, strike: float, current: float, option_type: str) -> float:
    distance = (strike - current) / max(current, 1)
    base = 0.28 - abs(distance) * 1.4 + _stable_unit(ticker, "delta") * 0.08
    value = max(0.08, min(0.62, base))
    return round(-value if option_type == "put" else value, 2)


def _sim_iv(ticker: str) -> float:
    return round(0.24 + _stable_unit(ticker, "iv") * 0.32, 2)


def _sim_liquidity(ticker: str, idx: int) -> str:
    value = _stable_unit(ticker, f"liq-{idx}")
    if value > 0.72:
        return "weak"
    if value > 0.38:
        return "fair"
    return "good"


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


def _avg(values: list[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None


def _money(value: Optional[float], digits: int = 0) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.{digits}f}"
