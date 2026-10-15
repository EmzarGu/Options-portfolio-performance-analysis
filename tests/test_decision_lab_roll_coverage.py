from __future__ import annotations

from datetime import date

import pytest

from portfolio_backend.decision_lab import build_decision_lab_data
from portfolio_backend.decision_lab_candidates import recommendation_candidates
from portfolio_backend.option_market.marketdata import Selection
from portfolio_backend.option_market.marketdata_local import plan_requests


def _situation(category, option_type, expiry, strike=100.0):
    return {
        "ticker": "XYZ",
        "category": category,
        "objective": "",
        "current_state": {
            "current_price": 104.0,
            "cost_basis": 95.0,
            "open_options": [{"type": option_type, "strike": strike, "expiry": expiry, "quantity": 1}],
        },
        "_open_rows": [{"option_type": option_type, "expiration": expiry, "strike": strike}],
    }


def _expiries(group):
    return [row["expiry"] for row in group["contract_requests"]]


def test_roll_requests_include_month_after_open_leg_and_skip_earlier_months():
    situation = _situation("Accept / monitor exit", "Call", "2026-11-20")

    group = recommendation_candidates([situation], as_of=date(2026, 10, 6))[0]

    # Two monthlies from 6 Oct are Oct 16 and Nov 20; neither allows a roll out.
    assert _expiries(group) == ["2026-11-20", "2026-12-18"]


def test_harvest_requests_stay_on_open_expiry():
    situation = _situation("Harvest unused put risk", "Put", "2026-11-20", strike=70.0)

    group = recommendation_candidates([situation], as_of=date(2026, 10, 6))[0]

    assert _expiries(group) == ["2026-11-20"]


def test_later_month_too_far_out_is_not_requested():
    situation = _situation("Reduce assignment risk", "Put", "2026-12-18")

    group = recommendation_candidates([situation], as_of=date(2026, 10, 6))[0]

    assert "2027-01-15" not in _expiries(group)


def test_open_strike_is_priced_in_later_months_for_roll_out():
    situation = _situation("Accept / monitor exit", "Call", "2026-11-20")
    groups = recommendation_candidates([situation], as_of=date(2026, 10, 6))

    _, plan = plan_requests(groups, as_of=date(2026, 10, 6))
    strike_by_expiry = {(request.expiry.isoformat(), selection) for request, selection in plan if selection.strike is not None}

    assert ("2026-11-20", Selection(strike=100.0)) in strike_by_expiry
    assert ("2026-12-18", Selection(strike=100.0)) in strike_by_expiry


def _itm_put_payload():
    return {
        "dashboard": {"request": {"as_of": "2026-10-06"}, "snapshot": {}, "issue_summary": {"total_count": 0}},
        "positions": {
            "inventory": [],
            "open_option_shorts": [
                {
                    "ticker": "XYZ",
                    "option_type": "Put",
                    "strike": 50.0,
                    "expiration": "2026-10-16",
                    "days_to_expiration": 10,
                    "quantity": -1,
                    "current_price": 48.0,
                    "moneyness": -0.04,
                    "accounting_open_premium": 120.0,
                    "strategy_premium_collected": 120.0,
                }
            ],
        },
        "tickers": {"items": [{"ticker": "XYZ", "total_pnl": 100.0, "realized_options_pnl": 300.0, "unrealized_pnl": -200.0}]},
        "monthly": {"months": []},
        "issues": {"issues": []},
    }


def _put(expiry, strike, bid, ask, delta):
    return {
        "provider": "marketdata", "ticker": "XYZ", "expiry": expiry, "put_call": "PUT", "strike": strike,
        "bid": bid, "ask": ask, "mark": (bid + ask) / 2, "underlying_price": 48.0, "delta": delta,
        "open_interest": 500, "volume": 50, "raw": {"price_source": "quote_midpoint"},
    }


def test_itm_put_can_roll_out_same_strike_for_credit():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [
            _put("2026-10-16", 50.0, 2.30, 2.40, -0.70),
            _put("2026-11-20", 50.0, 3.40, 3.55, -0.58),
        ],
    }

    data = build_decision_lab_data(_itm_put_payload(), option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]
    roll = next(row for row in group["candidates"] if row["action"] == "Roll put out same strike")

    assert group["category"] == "Reduce assignment risk"
    assert roll["strike"] == pytest.approx(50.0)
    assert roll["expiry"] == "2026-11-20"
    assert roll["roll_net_credit"] == pytest.approx((3.40 - 2.40) * 100)
    assert roll["expected_value_vs_current"] > 0
    assert "near_miss_message" not in group["candidate_status"]


def test_same_strike_put_roll_needs_real_credit():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [
            _put("2026-10-16", 50.0, 2.30, 2.40, -0.70),
            _put("2026-11-20", 50.0, 2.50, 2.60, -0.58),
        ],
    }

    data = build_decision_lab_data(_itm_put_payload(), option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]

    assert [row["action"] for row in group["candidates"]] == ["Keep current put"]
    status = group["candidate_status"]
    assert status["near_miss"]["strike"] == pytest.approx(50.0)
    assert status["near_miss"]["reason"] == "roll does not beat keeping the current position"
    assert status["near_miss_message"].startswith("No alternative passed the filters. Nearest: 2026-11-20 $50.00 put")


def _underwater_payload():
    payload = _itm_put_payload()
    payload["positions"]["open_option_shorts"] = []
    payload["positions"]["inventory"] = [
        {"ticker": "XYZ", "shares": 100, "cost_per_share": 80.0, "current_price": 60.0, "unrealized_pnl": -2000.0}
    ]
    return payload


def _call(strike, bid, ask, delta):
    return {"provider": "marketdata", "ticker": "XYZ", "expiry": "2026-11-20", "put_call": "CALL", "strike": strike,
            "bid": bid, "ask": ask, "underlying_price": 60.0, "delta": delta, "open_interest": 500, "volume": 50,
            "raw": {"price_source": "quote_midpoint"}}


def test_underwater_holding_still_gets_covered_calls_below_basis():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [_call(65.0, 1.10, 1.20, 0.27), _call(80.0, 0.05, 0.10, 0.03), _call(55.0, 6.00, 6.20, 0.80)],
    }

    data = build_decision_lab_data(_underwater_payload(), option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]

    assert group["category"] == "Recover with covered call"
    assert [(row["action"], row["strike"]) for row in group["candidates"]] == [("Sell covered call below basis", 65.0)]
    offered = group["recommended"]
    assert offered["exit_pnl"] == pytest.approx((65.0 - 80.0) * 100 + 110.0)
    assert "sold below basis" in offered["explanation"]
    assert "near_miss_message" not in group["candidate_status"]


def test_recovery_call_without_any_usable_call_explains_nearest_contract():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [_call(80.0, 0.05, 0.10, 0.03), _call(70.0, 0.40, 0.90, 0.10)],
    }

    data = build_decision_lab_data(_underwater_payload(), option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]
    status = group["candidate_status"]

    assert group["candidates"] == []
    assert status["near_miss"]["strike"] == pytest.approx(70.0)
    assert "Nearest: 2026-11-20 $70.00 call, delta 0.10" in status["near_miss_message"]


def test_pause_row_with_open_put_still_gets_roll_options():
    payload = _itm_put_payload()
    payload["positions"]["open_option_shorts"][0].update(
        {"strike": 45.0, "current_price": 50.0, "moneyness": 0.10, "expiration": "2026-11-20", "days_to_expiration": 45}
    )
    payload["tickers"]["items"] = [{"ticker": "XYZ", "total_pnl": -500.0, "realized_options_pnl": 300.0, "unrealized_pnl": -800.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [
            {**_put("2026-11-20", 45.0, 0.90, 1.00, -0.22), "underlying_price": 50.0},
            {**_put("2026-12-18", 45.0, 1.60, 1.70, -0.25), "underlying_price": 50.0},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    situation = data["ticker_situations"][0]
    group = data["recommendation_candidates"][0]

    assert situation["category"] == "Pause new entries"
    assert group["category"] == "Pause new entries"
    assert [row["expiry"] for row in group["contract_requests"]] == ["2026-11-20", "2026-12-18"]
    actions = [row["action"] for row in group["candidates"]]
    assert actions[0] == "Keep current put"
    assert "Roll put out same strike" in actions


def test_selection_plan_fits_daily_credit_budget_in_priority_order():
    situations = [
        {**_situation("Accept / monitor exit", "Call", "2026-11-20"), "ticker": f"T{chr(65 + index)}"}
        for index in range(20)
    ]
    groups = recommendation_candidates(situations, as_of=date(2026, 10, 6))

    _, full = plan_requests(groups, as_of=date(2026, 10, 6), budget=10_000)
    _, plan = plan_requests(groups, as_of=date(2026, 10, 6))

    assert len(full) > 85
    assert len(plan) == 85
    kept = {(request.ticker, request.expiry.isoformat(), selection) for request, selection in plan}
    for index in range(20):
        ticker = f"T{chr(65 + index)}"
        # Every queue row keeps its open leg, the roll-out strike and the 0.30 delta.
        assert (ticker, "2026-11-20", Selection(strike=100.0)) in kept
        assert (ticker, "2026-12-18", Selection(strike=100.0)) in kept
        assert (ticker, "2026-12-18", Selection(delta=0.30)) in kept
    # Extras are trimmed from the lowest-priority rows first.
    assert ("TA", "2026-11-20", Selection(strike=95.0)) in kept
    assert ("TT", "2026-12-18", Selection(strike=95.0)) not in kept
    assert not any(selection == Selection(delta=0.15) for _, _, selection in kept)


def test_plan_under_budget_is_unchanged():
    situation = _situation("Accept / monitor exit", "Call", "2026-11-20")
    groups = recommendation_candidates([situation], as_of=date(2026, 10, 6))

    _, plan = plan_requests(groups, as_of=date(2026, 10, 6))

    assert len(plan) == 2 * 4  # open strike, 0.30, basis and 0.15 in both months
