"""Decision cards: a compact comparison view over the Decision Lab payload.

The Decision Lab already classifies each ticker (``ticker_situations``) and
prices keep/roll candidates (``recommendation_candidates``). This module only
reshapes that output for the Today and Decision cards tabs: it groups tickers
by urgency, lays the options for one ticker side by side, and checks each
option against the owner's entry rules. It does not price contracts, filter
candidates or change any accounting value.
"""
from __future__ import annotations

from typing import Any, Optional

from portfolio_backend.app_settings import normalize_decision_rules

CONTRACT_MULTIPLIER = 100

# Categories where an expiry within ten days makes the row urgent. Mirrors the
# grouping used by the local visual prototype.
URGENT_SOON_CATEGORIES = {
    "Reduce assignment risk",
    "Monitor assignment risk",
    "Roll to improve recovery",
    "Evaluate exit vs roll",
    "Accept / monitor exit",
}
PUT_CATEGORIES = {"Reduce assignment risk", "Monitor assignment risk", "Harvest unused put risk"}
SPREAD_OK_LIQUIDITY = {"good", "fair"}
GROUP_ORDER = {"now": 0, "plan": 1, "monitor": 2}
GROUP_LABELS = {"now": "Act now", "plan": "Plan this week", "monitor": "Monitor"}


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def action_group(situation: dict[str, Any]) -> str:
    """Return ``now``, ``plan`` or ``monitor`` for one ticker situation."""
    priority = str(situation.get("priority") or "").lower()
    category = str(situation.get("category") or "")
    dte = _num(situation.get("dte"))
    if priority == "high":
        return "now"
    if dte is not None and dte <= 10 and category in URGENT_SOON_CATEGORIES:
        return "now"
    if priority == "medium":
        return "plan"
    return "monitor"


def _contract_count(candidate: dict[str, Any], state: dict[str, Any]) -> float:
    count = _num(candidate.get("contract_count"))
    if count:
        return abs(count)
    options = state.get("open_options") or []
    if options:
        quantity = _num(options[0].get("quantity"))
        if quantity:
            return abs(quantity)
    return 1.0


def _check(rule: str, status: str, text: str) -> dict[str, str]:
    return {"rule": rule, "status": status, "text": text}


def _rule_checks(candidate: dict[str, Any], *, is_put: bool, rules: dict[str, Any]) -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []
    dte = _num(candidate.get("dte"))
    dte_min, dte_max = rules["dte_min"], rules["dte_max"]
    dte_label = f"{dte_min:.0f}-{dte_max:.0f} DTE"
    if dte is None:
        checks.append(_check("dte", "unknown", f"{dte_label}: no expiry"))
    elif dte_min <= dte <= dte_max:
        checks.append(_check("dte", "ok", dte_label))
    else:
        checks.append(_check("dte", "fail", f"{dte:.0f} DTE, rule {dte_label}"))

    if is_put:
        delta = _num(candidate.get("delta"))
        low, high = rules["put_delta_min"], rules["put_delta_max"]
        delta_label = f"delta {low:.2f}-{high:.2f}"
        if delta is None:
            checks.append(_check("delta", "unknown", f"{delta_label}: no quote"))
        elif low <= abs(delta) <= high:
            checks.append(_check("delta", "ok", delta_label))
        elif abs(abs(delta) - (high if abs(delta) > high else low)) <= 0.05:
            checks.append(_check("delta", "near", f"delta {abs(delta):.2f}, rule {low:.2f}-{high:.2f}"))
        else:
            checks.append(_check("delta", "fail", f"delta {abs(delta):.2f}, rule {low:.2f}-{high:.2f}"))

    liquidity = str(candidate.get("liquidity") or "")
    if candidate.get("wide_spread"):
        checks.append(_check("spread", "fail", "wide bid/ask spread"))
    elif liquidity in SPREAD_OK_LIQUIDITY:
        checks.append(_check("spread", "ok", "spread within 35% of mid"))
    else:
        checks.append(_check("spread", "unknown", "spread not confirmed"))
    return checks


def _option_label(candidate: dict[str, Any], put_call: str) -> str:
    strike = _num(candidate.get("strike"))
    expiry = str(candidate.get("expiry") or "")[:10]
    leg = "P" if put_call == "PUT" else "C"
    strike_text = f"{strike:g}{leg}" if strike is not None else leg
    return f"{strike_text} {expiry}".strip()


def _keep_option(candidate: dict[str, Any], state: dict[str, Any], put_call: str) -> dict[str, Any]:
    is_put = put_call == "PUT"
    qty = _contract_count(candidate, state)
    strike = _num(candidate.get("strike"))
    return {
        "kind": "keep",
        "title": "Keep, accept assignment" if is_put else "Keep, accept call-away",
        "contract": _option_label(candidate, put_call),
        "action": candidate.get("action"),
        "headline": {"label": "chain net credit so far", "value": _num(candidate.get("premium")), "format": "money"},
        "metrics": [
            {"label": "Cash needed at strike" if is_put else "Shares called at strike", "value": strike * CONTRACT_MULTIPLIER * qty if strike is not None and is_put else strike, "format": "money" if is_put else "price"},
            {"label": "If assigned at today's price" if is_put else "Ticker total if called away", "value": _num(candidate.get("exercise_result")), "format": "money"},
            {"label": "If it expires" if is_put else "Ticker total if not called", "value": _num(candidate.get("no_exercise_result")), "format": "money"},
            {"label": "Delta", "value": _num(candidate.get("delta")), "format": "delta"},
            {"label": "Days left", "value": _num(candidate.get("dte")), "format": "days"},
        ],
        "checks": [],
        "score": _num(candidate.get("score")),
        "reason": candidate.get("score_reason"),
        "top": False,
    }


def _close_option(candidate: dict[str, Any], state: dict[str, Any], put_call: str) -> Optional[dict[str, Any]]:
    close_cost = _num(candidate.get("roll_close_cost"))
    premium = _num(candidate.get("premium"))
    if close_cost is None or premium is None:
        return None
    is_put = put_call == "PUT"
    qty = _contract_count(candidate, state)
    strike = _num(candidate.get("strike"))
    return {
        "kind": "close",
        "title": "Close now",
        "contract": f"buy back {_option_label(candidate, put_call)}",
        "action": "Close",
        # Strategy accounting: a final close without replacement realizes the
        # whole carried chain balance less the buyback cost.
        "headline": {"label": "chain result realized", "value": premium - close_cost, "format": "money"},
        "metrics": [
            {"label": "Cost to close", "value": -close_cost, "format": "money"},
            {"label": "Capital freed" if is_put else "Shares left uncovered", "value": strike * CONTRACT_MULTIPLIER * qty if strike is not None and is_put else qty * CONTRACT_MULTIPLIER, "format": "money" if is_put else "shares"},
            {"label": "New risk", "value": "none", "format": "text"},
        ],
        "checks": [],
        "score": None,
        "reason": "Buys back the open leg at the quoted ask.",
        "top": False,
    }


def _proposal_option(candidate: dict[str, Any], put_call: str, rules: dict[str, Any], *, has_open_leg: bool) -> dict[str, Any]:
    is_put = put_call == "PUT"
    net = _num(candidate.get("roll_net_credit"))
    if has_open_leg and net is not None:
        headline = {"label": "net credit after closing", "value": net, "format": "money"}
    else:
        headline = {"label": "premium received", "value": _num(candidate.get("premium")), "format": "money"}
    metrics = [
        {"label": "Delta", "value": _num(candidate.get("delta")), "format": "delta"},
        {"label": "Days to expiry", "value": _num(candidate.get("dte")), "format": "days"},
    ]
    if is_put:
        metrics += [
            {"label": "If assigned at today's price", "value": _num(candidate.get("exercise_result")), "format": "money"},
            {"label": "If it expires", "value": _num(candidate.get("no_exercise_result")), "format": "money"},
        ]
    else:
        metrics += [
            {"label": "Upside room added", "value": _num(candidate.get("upside_left")), "format": "money"},
            {"label": "Ticker total if called away", "value": _num(candidate.get("exercise_result")), "format": "money"},
            {"label": "Ticker total if not called", "value": _num(candidate.get("no_exercise_result")), "format": "money"},
        ]
    if has_open_leg:
        metrics.append({"label": "Better or worse than keeping", "value": _num(candidate.get("expected_value_vs_current")), "format": "money"})
    return {
        "kind": "proposal",
        "title": str(candidate.get("action") or "Alternative"),
        "contract": _option_label(candidate, put_call),
        "action": candidate.get("action"),
        "headline": headline,
        "metrics": metrics,
        "checks": _rule_checks(candidate, is_put=is_put, rules=rules),
        "score": _num(candidate.get("score")),
        "reason": candidate.get("score_reason"),
        "tradeability": candidate.get("tradeability"),
        "wide_spread": bool(candidate.get("wide_spread")),
        "top": False,
    }


def _verdict(proposals: list[dict[str, Any]], raw_proposals: list[dict[str, Any]], status: dict[str, Any], has_open_leg: bool) -> dict[str, Any]:
    if not proposals:
        message = str(status.get("message") or "No priced alternative yet. Fetch option data to compare.")
        near = status.get("near_miss_message")
        return {"tone": "neutral", "text": f"{message} {near}".strip() if near else message}
    top_raw = raw_proposals[0]
    vs_current = _num(top_raw.get("expected_value_vs_current"))
    if has_open_leg and vs_current is not None and vs_current <= 0:
        return {"tone": "neutral", "text": "No alternative scores better than keeping the current position."}
    proposals[0]["top"] = True
    text = f"Top-ranked alternative: {proposals[0]['title']} {proposals[0]['contract']}."
    failing = [check["text"] for check in proposals[0]["checks"] if check["status"] in {"fail", "near"}]
    if failing:
        text += " Outside your rules on " + "; ".join(failing) + "."
    return {"tone": "positive", "text": text}


def build_decision_card(group: dict[str, Any], situation: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    category = str(group.get("option_category") or group.get("category") or situation.get("category") or "")
    put_call = "PUT" if category in PUT_CATEGORIES else "CALL"
    state = group.get("current_state") if isinstance(group.get("current_state"), dict) else {}
    candidates = [row for row in group.get("candidates") or [] if isinstance(row, dict)]
    current = next((row for row in candidates if row.get("is_current_position")), None)
    raw_proposals = [row for row in candidates if not row.get("is_current_position")]
    has_open_leg = current is not None

    options: list[dict[str, Any]] = []
    if current is not None:
        options.append(_keep_option(current, state, put_call))
        close = _close_option(current, state, put_call)
        if close is not None:
            options.append(close)
    proposals = [_proposal_option(row, put_call, rules, has_open_leg=has_open_leg) for row in raw_proposals]
    verdict = _verdict(proposals, raw_proposals, group.get("candidate_status") or {}, has_open_leg)
    options.extend(proposals)

    return {
        "ticker": group.get("ticker") or situation.get("ticker"),
        "category": situation.get("category") or group.get("category"),
        "objective": situation.get("objective") or group.get("objective"),
        "put_call": put_call,
        "current_price": _num(state.get("current_price")),
        "options": options[:4],
        "verdict": verdict,
        "quote_date": next((row.get("quote_date") for row in candidates if row.get("quote_date")), None),
        "candidate_status": group.get("candidate_status") or {},
    }


def with_decision_cards(decision_lab: dict[str, Any], rules: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Return a copy of a Decision Lab payload with ``action_queue`` and ``decision_cards``."""
    if not isinstance(decision_lab, dict) or decision_lab.get("error") or decision_lab.get("deferred"):
        return decision_lab
    rules = normalize_decision_rules(rules)
    groups = {
        str(row.get("ticker") or "").upper(): row
        for row in decision_lab.get("recommendation_candidates") or []
        if isinstance(row, dict)
    }
    queue = []
    cards = []
    for index, situation in enumerate(decision_lab.get("ticker_situations") or []):
        if not isinstance(situation, dict):
            continue
        ticker = str(situation.get("ticker") or "").upper()
        group_key = action_group(situation)
        queue.append({**situation, "group": group_key, "group_label": GROUP_LABELS[group_key], "has_card": ticker in groups, "_order": index})
        if ticker in groups:
            card = build_decision_card(groups[ticker], situation, rules)
            cards.append({**card, "group": group_key, "group_label": GROUP_LABELS[group_key], "priority": situation.get("priority"), "_order": index})
    queue.sort(key=lambda row: (GROUP_ORDER[row["group"]], row["_order"]))
    cards.sort(key=lambda row: (GROUP_ORDER[row["group"]], row["_order"]))
    for row in queue + cards:
        row.pop("_order", None)
    return {
        **decision_lab,
        "decision_rules": {key: rules[key] for key in ("annual_target_return", "put_delta_min", "put_delta_max", "dte_min", "dte_max")},
        "action_queue": queue,
        "decision_cards": cards,
    }
