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


def test_recovery_call_without_candidates_explains_nearest_contract():
    payload = _itm_put_payload()
    payload["positions"]["open_option_shorts"] = []
    payload["positions"]["inventory"] = [
        {"ticker": "XYZ", "shares": 100, "cost_per_share": 80.0, "current_price": 60.0, "unrealized_pnl": -2000.0}
    ]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-06T12:00:00+00:00"},
        "contracts": [
            {"provider": "marketdata", "ticker": "XYZ", "expiry": "2026-11-20", "put_call": "CALL", "strike": 65.0,
             "bid": 1.10, "ask": 1.20, "underlying_price": 60.0, "delta": 0.27, "open_interest": 500, "volume": 50,
             "raw": {"price_source": "quote_midpoint"}},
            {"provider": "marketdata", "ticker": "XYZ", "expiry": "2026-11-20", "put_call": "CALL", "strike": 80.0,
             "bid": 0.05, "ask": 0.10, "underlying_price": 60.0, "delta": 0.03, "open_interest": 500, "volume": 50,
             "raw": {"price_source": "quote_midpoint"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]
    status = group["candidate_status"]

    assert group["category"] == "Recover with covered call"
    assert group["candidates"] == []
    assert status["near_miss"]["strike"] == pytest.approx(65.0)
    assert status["near_miss"]["reason"] == "call strike below current price or basis"
    assert "Nearest: 2026-11-20 $65.00 call, delta 0.27" in status["near_miss_message"]
