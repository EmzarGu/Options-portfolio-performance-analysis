from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional


ESTIMATED_OPTION_COMMISSION = 1.0
MIN_ACTIONABLE_NET_PREMIUM = 25.0
MAX_ACTIONABLE_SPREAD_RATIO = 0.35
MAX_LOW_PREMIUM_ABS_SPREAD = 0.10
MIN_INDICATIVE_OPEN_INTEREST = 20
MIN_INDICATIVE_VOLUME = 5
MIN_PUT_ROLL_UP_CREDIT = 75.0
MIN_CALL_ROLL_CREDIT = 25.0
MIN_ACTIONABLE_NET_PREMIUM_PER_CONTRACT = 25.0
MAX_PUT_ROLL_DELTA = 0.30
MIN_CALL_ROLL_DELTA = 0.08
MIN_RECOVERY_CALL_DELTA = 0.12
MAX_RECOVERY_CALL_DELTA = 0.45
MAX_RECOVERY_CALL_STRIKE_ABOVE_BASIS = 0.15
MAX_COVERED_CALL_DELTA = 0.65
MAX_INDICATIVE_PRICE_AGE_DAYS = 7


def recommendation_candidates(ticker_situations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for situation in ticker_situations:
        contract_requests = _contract_requests_for_situation(situation)
        if not contract_requests:
            continue
        rows.append(
            {
                "ticker": situation["ticker"],
                "category": situation["category"],
                "objective": situation["objective"],
                "current_state": situation.get("current_state") or {},
                "contract_requests": contract_requests,
                "recommended": None,
                "alternatives": [],
                "candidates": [],
            }
        )
    return rows


def _contract_requests_for_situation(situation: dict[str, Any]) -> list[dict[str, Any]]:
    category = str(situation.get("category") or "")
    open_rows = situation.get("_open_rows") or []
    if category in {"Recover with covered call", "Evaluate exit vs roll", "Roll to improve recovery", "Accept / monitor exit"}:
        put_call = "CALL"
    elif category in {"Reduce assignment risk", "Monitor assignment risk", "Harvest unused put risk"}:
        put_call = "PUT"
    else:
        return []

    expiries: list[str] = []
    for row in open_rows:
        if _option_type(row).startswith(put_call.lower()):
            expiry = str(row.get("expiration") or "")[:10]
            if expiry:
                expiries.append(expiry)

    if category == "Recover with covered call":
        expiries.extend(_standard_monthly_expiries(2))
    elif put_call == "CALL":
        expiries.extend(_standard_monthly_expiries(2))
    elif category == "Harvest unused put risk":
        for row in open_rows:
            if _option_type(row).startswith("put"):
                expiry = str(row.get("expiration") or "")[:10]
                if expiry:
                    expiries.append(expiry)
        expiries.extend(_standard_monthly_expiries(2))
    else:
        expiries.extend(_standard_monthly_expiries(2))

    deduped = []
    seen = set()
    for expiry in expiries:
        parsed = _parse_date(expiry)
        if parsed is None:
            continue
        value = parsed.isoformat()
        key = (value, put_call)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"expiry": value, "put_call": put_call})
    return deduped


def apply_option_market_candidates(
    candidate_groups: list[dict[str, Any]],
    option_market_data: dict[str, Any],
) -> list[dict[str, Any]]:
    contracts = _contracts_by_key(option_market_data.get("contracts") or [])
    status = option_market_data.get("status") or {}
    rows = []
    for group in candidate_groups:
        ticker = str(group.get("ticker") or "").upper()
        converted, candidate_status = _real_contract_candidates(ticker, group, contracts, status)
        rows.append(
            {
                **group,
                "recommended": converted[0] if converted else None,
                "alternatives": converted[1:3],
                "candidates": converted,
                "candidate_status": candidate_status,
            }
        )
    return rows


def _real_contract_candidates(
    ticker: str,
    group: dict[str, Any],
    contracts: dict[tuple[str, str], list[dict[str, Any]]],
    status: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    category = str(group.get("category") or "")
    put_call = "PUT" if category in {"Reduce assignment risk", "Monitor assignment risk", "Harvest unused put risk"} else "CALL"
    rows = contracts.get((ticker, put_call), [])
    if not rows:
        return [], {
            "status": "missing_contracts",
            "message": f"No stored {put_call.lower()} contracts for {ticker}.",
            "raw_contract_count": 0,
            "eligible_contract_count": 0,
        }
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_options = [row for row in state.get("open_options") or [] if str(row.get("type") or "").upper().startswith(put_call)]
    current_expiry = str(open_options[0].get("expiry") or "")[:10] if open_options else ""
    current_strike = _num(open_options[0].get("strike")) if open_options else None
    candidates = []
    rejection_counts: Counter[str] = Counter()

    if put_call == "CALL" and category in {"Evaluate exit vs roll", "Roll to improve recovery", "Accept / monitor exit"}:
        current_contract = _matching_contract(rows, current_expiry, current_strike)
        baseline = _covered_call_baseline(current_contract, group, status)
        if baseline is not None:
            candidates.append(baseline)
        roll_pool = [
            row
            for row in rows
            if _num(row.get("strike")) is not None
            and (not current_expiry or str(row.get("expiry") or "")[:10] >= current_expiry)
            and _contract_is_actionable(row, group, _roll_call_action(row, current_expiry, current_strike), rejection_counts, status)
        ]
        roll_candidates = [
            _covered_call_roll_candidate(row, current_contract, group, status)
            for row in roll_pool
        ]
        candidates.extend([row for row in roll_candidates if row is not None])
    elif category in {"Reduce assignment risk", "Monitor assignment risk"}:
        current_contract = _matching_contract(rows, current_expiry, current_strike)
        baseline = _short_put_baseline(current_contract, group, status)
        if baseline is not None:
            candidates.append(baseline)
        roll_pool = [
            row
            for row in rows
            if _num(row.get("strike")) is not None
            and current_strike is not None
            and (_num(row.get("strike")) or 0) < current_strike
            and (not current_expiry or str(row.get("expiry") or "")[:10] >= current_expiry)
            and _contract_is_actionable(row, group, "Roll put down/out", rejection_counts, status)
        ]
        candidates.extend(
            row
            for row in [_short_put_risk_reduction_candidate(row, current_contract, group, status) for row in roll_pool]
            if row is not None
        )
    elif category == "Harvest unused put risk":
        current_contract = _matching_contract(rows, current_expiry, current_strike)
        baseline = _short_put_baseline(current_contract, group, status)
        if baseline is not None:
            candidates.append(baseline)
        roll_pool = [
            row
            for row in rows
            if _num(row.get("strike")) is not None
            and current_strike is not None
            and (_num(row.get("strike")) or 0) > current_strike
            and (not current_expiry or str(row.get("expiry") or "")[:10] == current_expiry)
            and _contract_is_actionable(row, group, "Roll put up", rejection_counts, status)
        ]
        candidates.extend(
            row
            for row in [_short_put_roll_candidate(row, current_contract, group, status) for row in roll_pool]
            if row is not None
        )
    else:
        action = "Sell covered call" if put_call == "CALL" else "Roll down/out"
        filtered = _entry_contract_pool(rows, group, put_call)
        actionable = [row for row in filtered if _contract_is_actionable(row, group, action, rejection_counts, status)]
        candidates = [_candidate_from_live_contract(action, row, group, status) for row in actionable]
        candidates = sorted(candidates, key=lambda row: row["score"], reverse=True)

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any, Any]] = set()
    for candidate in sorted(candidates, key=lambda row: row["score"], reverse=True):
        key = (candidate.get("action"), candidate.get("expiry"), candidate.get("strike"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)

    current_rows = [row for row in deduped if row.get("is_current_position")]
    proposal_rows = [row for row in deduped if not row.get("is_current_position")]
    if current_rows:
        selected = current_rows[:1] + proposal_rows[:2]
    else:
        selected = proposal_rows[:3]
    return selected, _candidate_status(rows, selected, rejection_counts, status)


def _entry_contract_pool(rows: list[dict[str, Any]], group: dict[str, Any], put_call: str) -> list[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    current = _num(state.get("current_price"))
    cost = _num(state.get("cost_basis"))
    if put_call == "CALL":
        floor = max([value for value in [current, cost] if value is not None], default=0)
        eligible = [row for row in rows if (_num(row.get("strike")) or 0) >= floor]
        return eligible or rows
    return rows


def _option_price(contract: Optional[dict[str, Any]]) -> Optional[float]:
    if not contract:
        return None
    bid = _num(contract.get("bid"))
    ask = _num(contract.get("ask"))
    mark = _num(contract.get("mark"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2
    return mark


def _option_credit(contract: Optional[dict[str, Any]]) -> Optional[float]:
    if not contract:
        return None
    bid = _num(contract.get("bid"))
    mark = _num(contract.get("mark"))
    if bid is not None and bid > 0:
        return bid
    return mark


def _option_close_cost(contract: Optional[dict[str, Any]]) -> Optional[float]:
    if not contract:
        return None
    ask = _num(contract.get("ask"))
    mark = _num(contract.get("mark"))
    if ask is not None and ask > 0:
        return ask
    return mark


def _roll_call_action(contract: dict[str, Any], current_expiry: str, current_strike: Optional[float]) -> str:
    strike = _num(contract.get("strike"))
    expiry = str(contract.get("expiry") or "")[:10]
    if current_strike is not None and strike is not None and abs(strike - current_strike) < 0.001 and expiry > current_expiry:
        return "Roll out same strike"
    if current_strike is not None and strike is not None and strike < current_strike:
        return "Roll down/out" if expiry > current_expiry else "Roll down same expiry"
    if current_strike is not None and strike is not None and strike > current_strike:
        return "Roll up/out" if expiry > current_expiry else "Roll up same expiry"
    return "Accept / monitor exit"


def _probability_from_delta(delta: Optional[float]) -> float:
    if delta is None:
        return 0.0
    return max(0.0, min(abs(delta), 0.95))


def _covered_call_lifecycle_ev(
    state: dict[str, Any],
    *,
    strike: Optional[float],
    option_net: float,
    exercise_probability: Optional[float],
    contract_qty: int,
) -> Optional[float]:
    outcomes = _covered_call_lifecycle_outcomes(
        state,
        strike=strike,
        option_net=option_net,
        contract_qty=contract_qty,
    )
    if outcomes is None:
        return None
    probability = _probability_from_delta(exercise_probability)
    return probability * outcomes["exercise_result"] + (1 - probability) * outcomes["no_exercise_result"]


def _covered_call_lifecycle_outcomes(
    state: dict[str, Any],
    *,
    strike: Optional[float],
    option_net: float,
    contract_qty: int,
) -> Optional[dict[str, float]]:
    current_unrealized = _num(state.get("current_unrealized"))
    cost = _num(state.get("cost_basis"))
    realized = _num(state.get("realized_pnl")) or 0.0
    if strike is None or cost is None or current_unrealized is None:
        return None
    shares = max(contract_qty, 1) * 100
    return {
        "no_exercise_result": realized + current_unrealized + option_net,
        "exercise_result": realized + option_net + (strike - cost) * shares,
    }


def _covered_call_baseline_ev(
    state: dict[str, Any],
    current_contract: Optional[dict[str, Any]],
    open_option: dict[str, Any],
    contract_qty: int,
) -> Optional[float]:
    current_strike = _num(open_option.get("strike"))
    current_delta = abs(_num(current_contract.get("delta")) or 0) if current_contract and current_contract.get("delta") is not None else 0.0
    if current_strike is None:
        return _covered_call_lifecycle_ev(
            state,
            strike=_num(state.get("current_price")),
            option_net=0.0,
            exercise_probability=0.0,
            contract_qty=contract_qty,
        )
    return _covered_call_lifecycle_ev(
        state,
        strike=current_strike,
        option_net=0.0,
        exercise_probability=current_delta,
        contract_qty=contract_qty,
    )


def _put_roll_ev(
    *,
    net_credit: float,
    strike: Optional[float],
    current_price: Optional[float],
    delta: Optional[float],
    contract_qty: int,
) -> Optional[float]:
    if strike is None or current_price is None:
        return None
    assignment_probability = _probability_from_delta(delta)
    immediate_assignment_gap = -max(strike - current_price, 0) * 100 * max(contract_qty, 1)
    return net_credit + assignment_probability * immediate_assignment_gap


def _put_roll_outcomes(
    *,
    net_credit: float,
    strike: Optional[float],
    current_price: Optional[float],
    contract_qty: int,
) -> Optional[dict[str, float]]:
    if strike is None or current_price is None:
        return None
    immediate_assignment_gap = -max(strike - current_price, 0) * 100 * max(contract_qty, 1)
    return {
        "no_exercise_result": net_credit,
        "exercise_result": net_credit + immediate_assignment_gap,
    }


def _candidate_ev_score(expected_value_vs_current: Optional[float], liquidity: str, dte: Optional[int]) -> float:
    if expected_value_vs_current is None:
        base = 45.0
    else:
        base = 50.0 + max(min(expected_value_vs_current / 50.0, 35.0), -35.0)
    base += {"good": 6, "fair": 3, "indicative-good": 2, "indicative": 0, "weak": -6, "current": 0}.get(liquidity, 0)
    if dte is not None and dte > 45:
        base -= min((dte - 45) / 7, 6)
    return round(max(0, min(100, base)), 1)


def _covered_call_baseline(
    current_contract: Optional[dict[str, Any]],
    group: dict[str, Any],
    status: dict[str, Any],
) -> Optional[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_option = (state.get("open_options") or [{}])[0]
    current_strike = _num(open_option.get("strike"))
    expiry = str(open_option.get("expiry") or "")[:10]
    cost = _num(state.get("cost_basis"))
    close_cost = _option_close_cost(current_contract)
    strategy_premium = _num(open_option.get("strategy_premium_collected")) or 0
    contract_qty = _option_contract_count(open_option, state)
    if current_strike is None or cost is None:
        return None
    exit_pnl = (current_strike - cost) * 100 * contract_qty + strategy_premium
    capped_upside = _capped_upside(current_strike, _num(state.get("current_price")), contract_qty)
    delta = abs(_num(current_contract.get("delta")) or 0) if current_contract and current_contract.get("delta") is not None else None
    outcomes = _covered_call_lifecycle_outcomes(
        state,
        strike=current_strike,
        option_net=0.0,
        contract_qty=contract_qty,
    )
    expected_value = _covered_call_lifecycle_ev(
        state,
        strike=current_strike,
        option_net=0.0,
        exercise_probability=delta,
        contract_qty=contract_qty,
    )
    score = 70.0
    if exit_pnl > 0:
        score += min(exit_pnl / 500, 12)
    return {
        "action": "Accept / monitor exit",
        "strike": current_strike,
        "expiry": expiry,
        "dte": _days_until(expiry) if expiry else open_option.get("dte"),
        "premium": strategy_premium,
        "delta": delta,
        "iv": _num(current_contract.get("volatility")) if current_contract and current_contract.get("volatility") is not None else None,
        "liquidity": _contract_liquidity(current_contract) if current_contract else "current",
        "tradeability": "current",
        "score": round(min(score, 100), 1),
        "explanation": f"Accept current exit at {_money(current_strike, 2)}.",
        "exit_pnl": exit_pnl,
        "upside_left": 0,
        "upside_foregone": capped_upside,
        "roll_close_cost": close_cost * 100 * contract_qty if close_cost is not None else None,
        "roll_new_credit": None,
        "roll_net_credit": None,
        "incremental_exit_pnl": 0,
        "expected_value": expected_value,
        "expected_value_vs_current": 0,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": _probability_from_delta(delta),
        "contract_count": contract_qty,
        "is_current_position": True,
        "score_reason": f"Current call baseline; expected value {_money(expected_value)}.",
        "provider": current_contract.get("provider") or status.get("provider") if current_contract else status.get("provider"),
        "price_source": _price_source(current_contract) if current_contract else "open-position",
        "quote_coverage": bool(current_contract and current_contract.get("bid") is not None and current_contract.get("ask") is not None),
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": current_contract.get("contract_symbol") if current_contract else None,
    }


def _covered_call_roll_candidate(
    new_contract: dict[str, Any],
    current_contract: Optional[dict[str, Any]],
    group: dict[str, Any],
    status: dict[str, Any],
) -> Optional[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_option = (state.get("open_options") or [{}])[0]
    current_strike = _num(open_option.get("strike"))
    current_expiry = str(open_option.get("expiry") or "")[:10]
    current_price = _num(state.get("current_price")) or _num(new_contract.get("underlying_price"))
    cost = _num(state.get("cost_basis"))
    close_cost = _option_close_cost(current_contract)
    new_credit = _option_credit(new_contract)
    new_strike = _num(new_contract.get("strike"))
    new_expiry = str(new_contract.get("expiry") or "")[:10]
    contract_qty = _option_contract_count(open_option, state)
    if current_strike is None or cost is None or close_cost is None or new_credit is None or new_strike is None:
        return None
    if not _contract_price_usable_for_roll(current_contract, status) or not _contract_price_usable_for_roll(new_contract, status):
        return None
    if abs(new_strike - current_strike) < 0.001 and new_expiry <= current_expiry:
        return None
    if _same_expiry(new_expiry, current_expiry) and _call_price_monotonicity_violated(
        current_strike=current_strike,
        current_price=close_cost,
        new_strike=new_strike,
        new_price=new_credit,
    ):
        return None
    net_credit = (new_credit - close_cost) * 100 * contract_qty
    extra_upside = max(new_strike - current_strike, 0) * 100 * contract_qty
    baseline_exit = (current_strike - cost) * 100 * contract_qty + (_num(open_option.get("strategy_premium_collected")) or 0)
    exit_pnl = baseline_exit + extra_upside + net_credit
    incremental_exit = exit_pnl - baseline_exit
    dte_added = max((_days_until(new_expiry) or 0) - (_days_until(current_expiry) or 0), 0)
    delta = abs(_num(new_contract.get("delta")) or 0) if new_contract.get("delta") is not None else None
    action = _roll_call_action(new_contract, current_expiry, current_strike)
    if action != "Roll out same strike" and current_price is not None and new_strike < current_price:
        return None
    baseline_ev = _covered_call_baseline_ev(state, current_contract, open_option, contract_qty)
    candidate_ev = _covered_call_lifecycle_ev(
        state,
        strike=new_strike,
        option_net=net_credit,
        exercise_probability=delta,
        contract_qty=contract_qty,
    )
    outcomes = _covered_call_lifecycle_outcomes(
        state,
        strike=new_strike,
        option_net=net_credit,
        contract_qty=contract_qty,
    )
    expected_value_vs_current = (
        candidate_ev - baseline_ev if candidate_ev is not None and baseline_ev is not None else None
    )

    if action in {"Roll up same expiry", "Roll up/out", "Roll down same expiry", "Roll down/out"} and (
        delta is None or delta < MIN_CALL_ROLL_DELTA
    ):
        return None
    if action == "Roll up same expiry":
        if current_price is None or current_price <= current_strike:
            return None
    if net_credit < 0:
        if action in {"Roll up same expiry", "Roll down same expiry"}:
            return None
        if expected_value_vs_current is None or expected_value_vs_current <= 0:
            return None
        if action != "Roll down/out" and abs(net_credit) > min(250.0, extra_upside * 0.10):
            return None
        if action != "Roll down/out" and incremental_exit < 250:
            return None
    elif action == "Roll out same strike" and net_credit < MIN_CALL_ROLL_CREDIT:
        return None
    if delta is not None and delta > MAX_COVERED_CALL_DELTA and action != "Roll out same strike":
        return None

    score = _candidate_ev_score(expected_value_vs_current, _contract_liquidity(new_contract), _days_until(new_expiry))
    return {
        "action": action,
        "strike": new_strike,
        "expiry": new_expiry,
        "dte": _days_until(new_expiry),
        "premium": new_credit * 100 * contract_qty,
        "delta": delta,
        "iv": _num(new_contract.get("volatility")) if new_contract.get("volatility") is not None else None,
        "liquidity": _contract_liquidity(new_contract),
        "tradeability": "quote-backed" if new_contract.get("bid") is not None and new_contract.get("ask") is not None else "indicative",
        "score": round(max(0, min(100, score)), 1),
        "explanation": f"Net roll {_money(net_credit)}; incremental exit {_money(incremental_exit)}.",
        "exit_pnl": exit_pnl,
        "upside_left": extra_upside,
        "upside_foregone": _capped_upside(new_strike, current_price, contract_qty),
        "roll_close_cost": close_cost * 100 * contract_qty,
        "roll_new_credit": new_credit * 100 * contract_qty,
        "roll_net_credit": net_credit,
        "incremental_exit_pnl": incremental_exit,
        "expected_value": candidate_ev,
        "expected_value_vs_current": expected_value_vs_current,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": _probability_from_delta(delta),
        "contract_count": contract_qty,
        "score_reason": f"{action}: EV vs current {_money(expected_value_vs_current)}, net {_money(net_credit)}, {dte_added} DTE added.",
        "provider": new_contract.get("provider") or status.get("provider"),
        "price_source": _price_source(new_contract),
        "quote_coverage": new_contract.get("bid") is not None and new_contract.get("ask") is not None,
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": new_contract.get("contract_symbol"),
    }


def _short_put_baseline(
    current_contract: Optional[dict[str, Any]],
    group: dict[str, Any],
    status: dict[str, Any],
) -> Optional[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_option = (state.get("open_options") or [{}])[0]
    strike = _num(open_option.get("strike"))
    expiry = str(open_option.get("expiry") or "")[:10]
    strategy_premium = _num(open_option.get("strategy_premium_collected")) or 0
    contract_qty = _option_contract_count(open_option, state)
    current_price = _num(state.get("current_price")) or _num(current_contract.get("underlying_price")) if current_contract else _num(state.get("current_price"))
    delta = abs(_num(current_contract.get("delta")) or 0) if current_contract and current_contract.get("delta") is not None else None
    expected_value = _put_roll_ev(
        net_credit=0.0,
        strike=strike,
        current_price=current_price,
        delta=delta,
        contract_qty=contract_qty,
    )
    outcomes = _put_roll_outcomes(
        net_credit=0.0,
        strike=strike,
        current_price=current_price,
        contract_qty=contract_qty,
    )
    if strike is None:
        return None
    return {
        "action": "Keep current put",
        "strike": strike,
        "expiry": expiry,
        "dte": _days_until(expiry) if expiry else open_option.get("dte"),
        "premium": strategy_premium,
        "delta": delta,
        "iv": _num(current_contract.get("volatility")) if current_contract and current_contract.get("volatility") is not None else None,
        "liquidity": _contract_liquidity(current_contract) if current_contract else "current",
        "tradeability": "current",
        "score": 65.0,
        "explanation": "Keep current short put.",
        "exit_pnl": strategy_premium,
        "upside_left": None,
        "upside_foregone": 0,
        "roll_close_cost": _option_close_cost(current_contract) * 100 * contract_qty if _option_close_cost(current_contract) is not None else None,
        "roll_new_credit": None,
        "roll_net_credit": None,
        "incremental_exit_pnl": 0,
        "expected_value": expected_value,
        "expected_value_vs_current": 0,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": _probability_from_delta(delta),
        "contract_count": contract_qty,
        "is_current_position": True,
        "score_reason": "Baseline current put.",
        "provider": current_contract.get("provider") or status.get("provider") if current_contract else status.get("provider"),
        "price_source": _price_source(current_contract) if current_contract else "open-position",
        "quote_coverage": bool(current_contract and current_contract.get("bid") is not None and current_contract.get("ask") is not None),
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": current_contract.get("contract_symbol") if current_contract else None,
    }


def _short_put_roll_candidate(
    new_contract: dict[str, Any],
    current_contract: Optional[dict[str, Any]],
    group: dict[str, Any],
    status: dict[str, Any],
) -> Optional[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_option = (state.get("open_options") or [{}])[0]
    current_strike = _num(open_option.get("strike"))
    close_cost = _option_close_cost(current_contract)
    new_credit = _option_credit(new_contract)
    new_strike = _num(new_contract.get("strike"))
    current_price = _num(state.get("current_price")) or _num(new_contract.get("underlying_price"))
    contract_qty = _option_contract_count(open_option, state)
    if current_strike is None or close_cost is None or new_credit is None or new_strike is None or current_price is None:
        return None
    if not _contract_price_usable_for_roll(current_contract, status) or not _contract_price_usable_for_roll(new_contract, status):
        return None
    net_credit = (new_credit - close_cost) * 100 * contract_qty
    if net_credit < MIN_PUT_ROLL_UP_CREDIT:
        return None
    delta = abs(_num(new_contract.get("delta")) or 0) if new_contract.get("delta") is not None else None
    if delta is None or delta > MAX_PUT_ROLL_DELTA:
        return None
    if new_strike >= current_price:
        return None
    added_assignment_exposure = max(new_strike - current_strike, 0) * 100 * contract_qty
    baseline_ev = _put_roll_ev(
        net_credit=0.0,
        strike=current_strike,
        current_price=current_price,
        delta=abs(_num(current_contract.get("delta")) or 0) if current_contract and current_contract.get("delta") is not None else None,
        contract_qty=contract_qty,
    )
    expected_value = _put_roll_ev(
        net_credit=net_credit,
        strike=new_strike,
        current_price=current_price,
        delta=delta,
        contract_qty=contract_qty,
    )
    outcomes = _put_roll_outcomes(
        net_credit=net_credit,
        strike=new_strike,
        current_price=current_price,
        contract_qty=contract_qty,
    )
    expected_value_vs_current = expected_value - baseline_ev if expected_value is not None and baseline_ev is not None else None
    if expected_value_vs_current is None or expected_value_vs_current <= 0:
        return None
    score = _candidate_ev_score(expected_value_vs_current, _contract_liquidity(new_contract), _days_until(new_contract.get("expiry")))
    return {
        "action": "Roll put up",
        "strike": new_strike,
        "expiry": str(new_contract.get("expiry") or "")[:10],
        "dte": _days_until(new_contract.get("expiry")),
        "premium": new_credit * 100 * contract_qty,
        "delta": delta,
        "iv": _num(new_contract.get("volatility")) if new_contract.get("volatility") is not None else None,
        "liquidity": _contract_liquidity(new_contract),
        "tradeability": "quote-backed" if new_contract.get("bid") is not None and new_contract.get("ask") is not None else "indicative",
        "score": round(max(0, min(100, score)), 1),
        "explanation": f"Net roll credit {_money(net_credit)} while strike remains below current.",
        "exit_pnl": net_credit,
        "upside_left": None,
        "upside_foregone": 0,
        "roll_close_cost": close_cost * 100 * contract_qty,
        "roll_new_credit": new_credit * 100 * contract_qty,
        "roll_net_credit": net_credit,
        "incremental_exit_pnl": net_credit,
        "expected_value": expected_value,
        "expected_value_vs_current": expected_value_vs_current,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": _probability_from_delta(delta),
        "added_assignment_exposure": added_assignment_exposure,
        "contract_count": contract_qty,
        "score_reason": f"Roll put up: EV vs current {_money(expected_value_vs_current)}, net {_money(net_credit)}, delta {delta:.2f}.",
        "provider": new_contract.get("provider") or status.get("provider"),
        "price_source": _price_source(new_contract),
        "quote_coverage": new_contract.get("bid") is not None and new_contract.get("ask") is not None,
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": new_contract.get("contract_symbol"),
    }


def _short_put_risk_reduction_candidate(
    new_contract: dict[str, Any],
    current_contract: Optional[dict[str, Any]],
    group: dict[str, Any],
    status: dict[str, Any],
) -> Optional[dict[str, Any]]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    open_option = (state.get("open_options") or [{}])[0]
    current_strike = _num(open_option.get("strike"))
    close_cost = _option_close_cost(current_contract)
    new_credit = _option_credit(new_contract)
    new_strike = _num(new_contract.get("strike"))
    current_price = _num(state.get("current_price")) or _num(new_contract.get("underlying_price"))
    contract_qty = _option_contract_count(open_option, state)
    if current_strike is None or close_cost is None or new_credit is None or new_strike is None or current_price is None:
        return None
    if new_strike >= current_strike:
        return None
    if not _contract_price_usable_for_roll(current_contract, status) or not _contract_price_usable_for_roll(new_contract, status):
        return None
    current_expiry = str(open_option.get("expiry") or "")[:10]
    new_expiry = str(new_contract.get("expiry") or "")[:10]
    if _same_expiry(new_expiry, current_expiry) and _put_price_monotonicity_violated(
        current_strike=current_strike,
        current_price=close_cost,
        new_strike=new_strike,
        new_price=new_credit,
    ):
        return None
    delta = abs(_num(new_contract.get("delta")) or 0) if new_contract.get("delta") is not None else None
    current_delta = abs(_num(current_contract.get("delta")) or 0) if current_contract and current_contract.get("delta") is not None else None
    if delta is None:
        return None
    if current_delta is not None and delta >= current_delta:
        return None
    if delta > MAX_PUT_ROLL_DELTA:
        return None

    net_credit = (new_credit - close_cost) * 100 * contract_qty
    assignment_risk_reduction = max(current_strike - new_strike, 0) * 100 * contract_qty
    if net_credit < 0 and abs(net_credit) > min(250.0, assignment_risk_reduction * 0.12):
        return None

    dte_added = max((_days_until(new_contract.get("expiry")) or 0) - (_days_until(open_option.get("expiry")) or 0), 0)
    out_of_money_gap = max(current_price - new_strike, 0) * 100 * contract_qty
    baseline_ev = _put_roll_ev(
        net_credit=0.0,
        strike=current_strike,
        current_price=current_price,
        delta=current_delta,
        contract_qty=contract_qty,
    )
    expected_value = _put_roll_ev(
        net_credit=net_credit,
        strike=new_strike,
        current_price=current_price,
        delta=delta,
        contract_qty=contract_qty,
    )
    outcomes = _put_roll_outcomes(
        net_credit=net_credit,
        strike=new_strike,
        current_price=current_price,
        contract_qty=contract_qty,
    )
    expected_value_vs_current = expected_value - baseline_ev if expected_value is not None and baseline_ev is not None else None
    if expected_value_vs_current is None or expected_value_vs_current <= 0:
        return None
    score = _candidate_ev_score(expected_value_vs_current, _contract_liquidity(new_contract), _days_until(new_contract.get("expiry")))
    return {
        "action": "Roll put down/out",
        "strike": new_strike,
        "expiry": str(new_contract.get("expiry") or "")[:10],
        "dte": _days_until(new_contract.get("expiry")),
        "premium": new_credit * 100 * contract_qty,
        "delta": delta,
        "iv": _num(new_contract.get("volatility")) if new_contract.get("volatility") is not None else None,
        "liquidity": _contract_liquidity(new_contract),
        "tradeability": "quote-backed" if new_contract.get("bid") is not None and new_contract.get("ask") is not None else "indicative",
        "score": round(max(0, min(100, score)), 1),
        "explanation": f"Moves put strike down by {_money(assignment_risk_reduction)} while net roll is {_money(net_credit)}.",
        "exit_pnl": net_credit,
        "upside_left": None,
        "upside_foregone": 0,
        "roll_close_cost": close_cost * 100 * contract_qty,
        "roll_new_credit": new_credit * 100 * contract_qty,
        "roll_net_credit": net_credit,
        "incremental_exit_pnl": net_credit,
        "expected_value": expected_value,
        "expected_value_vs_current": expected_value_vs_current,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": _probability_from_delta(delta),
        "assignment_risk_reduction": assignment_risk_reduction,
        "contract_count": contract_qty,
        "score_reason": f"Roll put down/out: EV vs current {_money(expected_value_vs_current)}, net {_money(net_credit)}, assignment exposure reduced {_money(assignment_risk_reduction)}.",
        "provider": new_contract.get("provider") or status.get("provider"),
        "price_source": _price_source(new_contract),
        "quote_coverage": new_contract.get("bid") is not None and new_contract.get("ask") is not None,
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": new_contract.get("contract_symbol"),
    }


def _price_source(contract: Optional[dict[str, Any]]) -> str:
    if not contract:
        return "n/a"
    raw = contract.get("raw") if isinstance(contract.get("raw"), dict) else {}
    return str(raw.get("price_source") or ("quote_bid_ask_mid" if contract.get("bid") is not None and contract.get("ask") is not None else "provider_mark"))


def _same_expiry(left: Any, right: Any) -> bool:
    return bool(str(left or "")[:10] and str(left or "")[:10] == str(right or "")[:10])


def _contract_price_usable_for_roll(contract: Optional[dict[str, Any]], status: dict[str, Any]) -> bool:
    if not contract:
        return False
    bid = _num(contract.get("bid"))
    ask = _num(contract.get("ask"))
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return True
    return _indicative_price_is_fresh(contract, status)


def _indicative_price_is_fresh(contract: dict[str, Any], status: dict[str, Any]) -> bool:
    raw = contract.get("raw") if isinstance(contract.get("raw"), dict) else {}
    price_source = str(raw.get("price_source") or "").lower()
    if price_source not in {"day_close", "day_vwap"}:
        return True
    source = raw.get("source") if isinstance(raw.get("source"), dict) else {}
    day = source.get("day") if isinstance(source.get("day"), dict) else {}
    updated_at = _timestamp_date(day.get("last_updated"))
    if updated_at is None:
        return False
    reference_date = _reference_fetch_date(status) or date.today()
    return updated_at >= reference_date - timedelta(days=MAX_INDICATIVE_PRICE_AGE_DAYS)


def _reference_fetch_date(status: dict[str, Any]) -> Optional[date]:
    value = status.get("last_fetched_at") or status.get("fetched_at")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None


def _timestamp_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    if numeric > 10_000_000_000_000:
        seconds = numeric / 1_000_000_000
    elif numeric > 10_000_000_000:
        seconds = numeric / 1_000
    else:
        seconds = numeric
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).date()
    except (OverflowError, OSError, ValueError):
        return None


def _call_price_monotonicity_violated(
    *,
    current_strike: float,
    current_price: float,
    new_strike: float,
    new_price: float,
) -> bool:
    tolerance = 0.01
    if new_strike > current_strike and new_price > current_price + tolerance:
        return True
    if new_strike < current_strike and new_price < current_price - tolerance:
        return True
    return False


def _put_price_monotonicity_violated(
    *,
    current_strike: float,
    current_price: float,
    new_strike: float,
    new_price: float,
) -> bool:
    tolerance = 0.01
    if new_strike < current_strike and new_price > current_price + tolerance:
        return True
    if new_strike > current_strike and new_price < current_price - tolerance:
        return True
    return False


def _contract_is_actionable(
    contract: dict[str, Any],
    group: dict[str, Any],
    action: str,
    rejection_counts: Counter[str],
    status: dict[str, Any],
) -> bool:
    bid = _num(contract.get("bid"))
    ask = _num(contract.get("ask"))
    mark = _num(contract.get("mark"))
    quoted = bid is not None or ask is not None
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    contract_qty = _candidate_contract_count(action, state)
    commission = ESTIMATED_OPTION_COMMISSION * max(contract_qty, 1)

    if quoted:
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            rejection_counts["zero bid/ask"] += 1
            return False
        if ask < bid:
            rejection_counts["invalid spread"] += 1
            return False
        spread = ask - bid
        mid = (bid + ask) / 2
        if (mid < 0.25 and spread > MAX_LOW_PREMIUM_ABS_SPREAD) or (mid >= 0.25 and spread / mid > MAX_ACTIONABLE_SPREAD_RATIO):
            rejection_counts["wide spread"] += 1
            return False
        premium = bid * 100 * contract_qty
    else:
        if mark is None or mark <= 0:
            rejection_counts["missing price"] += 1
            return False
        if not _indicative_price_is_fresh(contract, status):
            rejection_counts["stale indicative price"] += 1
            return False
        oi = _num(contract.get("open_interest")) or 0
        volume = _num(contract.get("volume")) or 0
        if oi < MIN_INDICATIVE_OPEN_INTEREST and volume < MIN_INDICATIVE_VOLUME:
            rejection_counts["low liquidity"] += 1
            return False
        premium = mark * 100 * contract_qty

    if premium - commission < MIN_ACTIONABLE_NET_PREMIUM:
        rejection_counts["premium too small"] += 1
        return False
    if premium / max(contract_qty, 1) - ESTIMATED_OPTION_COMMISSION < MIN_ACTIONABLE_NET_PREMIUM_PER_CONTRACT:
        rejection_counts["premium too small"] += 1
        return False

    delta = abs(_num(contract.get("delta")) or 0) if contract.get("delta") is not None else None
    if delta is None:
        rejection_counts["missing delta"] += 1
        return False
    if delta <= 0 or delta >= 0.95:
        rejection_counts["implausible delta"] += 1
        return False

    dte = _days_until(contract.get("expiry"))
    if dte is None or dte < 7 or dte > 75:
        rejection_counts["outside DTE window"] += 1
        return False

    put_call = str(contract.get("put_call") or "").upper()
    if put_call == "CALL":
        state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
        current = _num(state.get("current_price")) or _num(contract.get("underlying_price"))
        cost = _num(state.get("cost_basis"))
        strike = _num(contract.get("strike"))
        if action == "Sell covered call" and strike is not None and current is not None and strike < current:
            rejection_counts["call strike below current"] += 1
            return False
        if action == "Sell covered call" and strike is not None and cost is not None and strike < cost:
            rejection_counts["call strike below basis"] += 1
            return False
        if action == "Sell covered call" and strike is not None:
            anchor = max([value for value in [cost, current] if value is not None], default=0)
            if anchor and strike > anchor * (1 + MAX_RECOVERY_CALL_STRIKE_ABOVE_BASIS):
                rejection_counts["call strike too far OTM"] += 1
                return False
            if delta is not None and delta < MIN_RECOVERY_CALL_DELTA:
                rejection_counts["delta too low"] += 1
                return False
            if delta is not None and delta > MAX_RECOVERY_CALL_DELTA:
                rejection_counts["delta too high"] += 1
                return False

    return True


def _candidate_status(
    raw_contracts: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    rejection_counts: Counter[str],
    status: dict[str, Any],
) -> dict[str, Any]:
    if candidates:
        all_indicative = all(str(candidate.get("tradeability") or "") == "indicative" for candidate in candidates)
        return {
            "status": "indicative" if all_indicative else "eligible",
            "message": f"{len(candidates)} {'indicative' if all_indicative else 'actionable'} contract(s) found.",
            "raw_contract_count": len(raw_contracts),
            "eligible_contract_count": len(candidates),
            "provider": status.get("provider"),
            "last_fetched_at": status.get("last_fetched_at"),
        }
    reason = _rejection_summary(rejection_counts)
    return {
        "status": "no_actionable_contracts",
        "message": f"No actionable contracts found{': ' + reason if reason else ''}.",
        "raw_contract_count": len(raw_contracts),
        "eligible_contract_count": 0,
        "rejection_counts": dict(rejection_counts),
        "provider": status.get("provider"),
        "last_fetched_at": status.get("last_fetched_at"),
    }


def _rejection_summary(rejection_counts: Counter[str]) -> str:
    if not rejection_counts:
        return ""
    return ", ".join(f"{reason} ({count})" for reason, count in rejection_counts.most_common(3))


def _matching_contract(rows: list[dict[str, Any]], expiry: str, strike: Optional[float]) -> Optional[dict[str, Any]]:
    if not expiry or strike is None:
        return None
    exact = [
        row
        for row in rows
        if str(row.get("expiry") or "")[:10] == expiry
        and abs((_num(row.get("strike")) or 0) - strike) < 0.001
    ]
    return exact[0] if exact else None


def _candidate_from_live_contract(action: str, contract: dict[str, Any], group: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    current = _num(state.get("current_price")) or _num(contract.get("underlying_price"))
    cost = _num(state.get("cost_basis")) or current
    strike = _num(contract.get("strike"))
    contract_qty = _candidate_contract_count(action, state)
    bid = _num(contract.get("bid"))
    mark = _num(contract.get("mark"))
    premium = bid * 100 * contract_qty if bid is not None and bid > 0 else (mark * 100 * contract_qty if mark is not None else None)
    put_call = str(contract.get("put_call") or "").upper()
    expiry = str(contract.get("expiry") or "")[:10]
    delta = abs(_num(contract.get("delta")) or 0) if contract.get("delta") is not None else None
    liquidity = _contract_liquidity(contract)
    raw = contract.get("raw") if isinstance(contract.get("raw"), dict) else {}
    quote_coverage = contract.get("bid") is not None and contract.get("ask") is not None
    tradeability = "quote-backed" if quote_coverage else "indicative"
    price_source = raw.get("price_source") or ("quote_bid" if quote_coverage else "provider_mark")

    exit_pnl = premium
    upside_left = None
    upside_foregone = None
    expected_value = None
    expected_value_vs_current = None
    exercise_probability = _probability_from_delta(delta)
    outcomes = None
    if put_call == "CALL" and strike is not None and current is not None and cost is not None:
        exit_pnl = (strike - cost) * 100 * contract_qty + (premium or 0)
        upside_left = max(strike - current, 0) * 100 * contract_qty
        upside_foregone = _capped_upside(strike, current, contract_qty)
        outcomes = _covered_call_lifecycle_outcomes(
            state,
            strike=strike,
            option_net=premium or 0.0,
            contract_qty=contract_qty,
        )
        baseline_ev = _covered_call_lifecycle_ev(
            state,
            strike=current,
            option_net=0.0,
            exercise_probability=0.0,
            contract_qty=contract_qty,
        )
        expected_value = _covered_call_lifecycle_ev(
            state,
            strike=strike,
            option_net=premium or 0.0,
            exercise_probability=delta,
            contract_qty=contract_qty,
        )
        expected_value_vs_current = (
            expected_value - baseline_ev if expected_value is not None and baseline_ev is not None else None
        )
    elif put_call == "PUT" and strike is not None:
        outcomes = _put_roll_outcomes(
            net_credit=premium or 0.0,
            strike=strike,
            current_price=current,
            contract_qty=contract_qty,
        )
        expected_value = _put_roll_ev(
            net_credit=premium or 0.0,
            strike=strike,
            current_price=current,
            delta=delta,
            contract_qty=contract_qty,
        )
        expected_value_vs_current = expected_value

    score = _candidate_ev_score(expected_value_vs_current, liquidity, _days_until(expiry) if expiry else None)
    return {
        "action": action,
        "strike": strike,
        "expiry": expiry,
        "dte": _days_until(expiry) if expiry else None,
        "premium": premium,
        "delta": delta,
        "iv": _num(contract.get("volatility")) if contract.get("volatility") is not None else None,
        "liquidity": liquidity,
        "tradeability": tradeability,
        "score": score,
        "explanation": _live_candidate_explanation(action, exit_pnl, upside_left),
        "exit_pnl": exit_pnl,
        "upside_left": upside_left,
        "upside_foregone": upside_foregone,
        "expected_value": expected_value,
        "expected_value_vs_current": expected_value_vs_current,
        "exercise_result": outcomes.get("exercise_result") if outcomes else None,
        "no_exercise_result": outcomes.get("no_exercise_result") if outcomes else None,
        "exercise_probability": exercise_probability,
        "score_reason": f"{action}: EV vs current {_money(expected_value_vs_current)}, {_live_candidate_score_reason(action, liquidity, delta, premium, upside_left)}",
        "provider": contract.get("provider") or status.get("provider"),
        "price_source": price_source,
        "quote_coverage": quote_coverage,
        "fetch_timestamp": status.get("last_fetched_at"),
        "contract_symbol": contract.get("contract_symbol"),
        "contract_count": contract_qty,
    }


def _live_candidate_explanation(action: str, exit_pnl: Optional[float], upside_left: Optional[float]) -> str:
    parts = [action]
    if exit_pnl is not None:
        parts.append(f"exit economics {_money(exit_pnl)}")
    if upside_left is not None:
        parts.append(f"upside left {_money(upside_left)}")
    return "; ".join(parts)


def _live_candidate_score_reason(
    action: str,
    liquidity: str,
    delta: Optional[float],
    premium: Optional[float],
    upside_left: Optional[float],
) -> str:
    details = [f"{liquidity} liquidity"]
    if delta is not None:
        details.append(f"{delta:.2f} delta")
    if premium is not None:
        details.append(f"{_money(premium)} premium")
    if upside_left is not None:
        details.append(f"{_money(upside_left)} upside left")
    return f"{action}: " + ", ".join(details)


def _contracts_by_key(contracts: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for contract in contracts:
        ticker = str(contract.get("ticker") or "").upper()
        put_call = str(contract.get("put_call") or "").upper()
        if ticker and put_call:
            grouped[(ticker, put_call)].append(contract)
    for rows in grouped.values():
        rows.sort(key=lambda row: (str(row.get("expiry") or ""), _num(row.get("strike")) or 0))
    return grouped


def _contract_liquidity(contract: dict[str, Any]) -> str:
    oi = _num(contract.get("open_interest")) or 0
    volume = _num(contract.get("volume")) or 0
    bid = _num(contract.get("bid"))
    ask = _num(contract.get("ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        if oi >= 100 or volume >= 20:
            return "indicative-good"
        if oi >= MIN_INDICATIVE_OPEN_INTEREST or volume >= MIN_INDICATIVE_VOLUME:
            return "indicative"
        return "weak"
    spread_ratio = (ask - bid) / ((ask + bid) / 2)
    if spread_ratio > MAX_ACTIONABLE_SPREAD_RATIO:
        return "weak"
    if spread_ratio <= 0.12 and (oi >= 100 or volume >= 20):
        return "good"
    if oi >= 100 or volume >= 20:
        return "fair"
    if oi >= 20 or volume >= 5:
        return "fair"
    return "weak"


def _option_contract_count(open_option: dict[str, Any], state: dict[str, Any]) -> int:
    qty = abs(_num(open_option.get("quantity")) or 0)
    if qty:
        return max(1, int(round(qty)))
    assigned_shares = abs(_num(state.get("assigned_shares")) or 0)
    if assigned_shares:
        return max(1, int(assigned_shares // 100))
    return 1


def _candidate_contract_count(action: str, state: dict[str, Any]) -> int:
    open_options = state.get("open_options") if isinstance(state.get("open_options"), list) else []
    if open_options:
        return _option_contract_count(open_options[0], state)
    if "covered call" in str(action).lower():
        assigned_shares = abs(_num(state.get("assigned_shares")) or 0)
        if assigned_shares:
            return max(1, int(assigned_shares // 100))
    return 1


def _capped_upside(strike: Optional[float], current_price: Optional[float], contract_qty: int) -> float:
    if strike is None or current_price is None:
        return 0.0
    return -max(current_price - strike, 0) * 100 * max(contract_qty, 1)


def _days_until(value: Any) -> Optional[int]:
    parsed = _parse_date(value)
    if parsed is None:
        return None
    return max((parsed - date.today()).days, 0)




def _option_type(row: dict[str, Any]) -> str:
    return str(row.get("option_type") or row.get("type") or row.get("put_call") or "").strip().lower()


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
    if value.month == 6 and value.day == 19:
        return value - timedelta(days=1)
    return value


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
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


def _money(value: Optional[float], digits: int = 0) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.{digits}f}"
