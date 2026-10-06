from datetime import date

import pytest

from portfolio_backend.decision_lab import build_decision_lab_data
from portfolio_backend.decision_lab_candidates import (
    _covered_call_lifecycle_outcomes,
    _covered_call_roll_candidate,
)


def _contract(ticker, strike, expiry, bid, ask, delta=0.25):
    return {
        "provider": "marketdata", "ticker": ticker, "put_call": "CALL",
        "strike": strike, "expiry": expiry, "bid": bid, "ask": ask,
        "mark": (bid + ask) / 2, "delta": delta, "open_interest": 100,
        "volume": 20, "raw": {"price_source": "quote_midpoint"},
    }


@pytest.mark.parametrize(
    "ticker,old,new,qty,spot,ask,bid,expiry,credit,proceeds_change",
    [
        ("GLW", 190, 180, 1, 156.74, 1.12, 6.60, "2026-11-20", 548, -452),
        ("CCJ", 105, 100, 2, 88.18, 0.61, 2.78, "2026-11-20", 434, -566),
        ("FLEX", 130, 120, 2, 115.98, 1.70, 3.50, "2026-10-16", 360, -1640),
        ("SAME", 100, 100, 2, 90, 1, 2, "2026-11-20", 200, 200),
        ("UP", 100, 110, 2, 90, 1, 2, "2026-11-20", 200, 2200),
    ],
)
def test_roll_exit_includes_signed_strike_change(
    ticker, old, new, qty, spot, ask, bid, expiry, credit, proceeds_change,
):
    state = {
        "cost_basis": old, "current_price": spot, "realized_pnl": 500,
        "current_unrealized": (spot - old) * qty * 100 + 250,
        "open_options": [{
            "strike": old, "expiry": "2026-10-16", "quantity": qty,
            "accounting_open_premium": 250, "strategy_premium_collected": 350,
            "realized_premium_already_booked": 100,
        }],
    }
    row = _covered_call_roll_candidate(
        _contract(ticker, new, expiry, bid, bid + 0.1),
        _contract(ticker, old, "2026-10-16", max(ask - 0.1, 0), ask),
        {"current_state": state}, {"provider": "marketdata"}, date(2026, 9, 27),
    )
    assert row is not None
    assert row["roll_net_credit"] == pytest.approx(credit)
    assert row["incremental_exit_pnl"] == pytest.approx(proceeds_change)
    assert row["exit_pnl"] == pytest.approx(350 + proceeds_change)
    assert row["exercise_result"] == pytest.approx(750 + proceeds_change)
    assert row["price_source"] == "close: quote_ask; sell: quote_bid"
    # New-leg credit must use bid, and closing debit must use ask, not midpoint.
    assert row["roll_close_cost"] == pytest.approx(ask * 100 * qty)
    assert row["roll_new_credit"] == pytest.approx(bid * 100 * qty)


@pytest.mark.parametrize(
    "option,premium",
    [
        ({"accounting_open_premium": 250, "strategy_premium_collected": 350,
          "realized_premium_already_booked": 100}, 250),
        ({"accounting_open_premium": 0, "strategy_premium_collected": 350,
          "realized_premium_already_booked": 350}, 0),
        ({"strategy_premium_collected": 350, "realized_premium_already_booked": 100}, 250),
        ({"accounting_open_premium": -50}, -50),
        (None, 0),
    ],
)
def test_exercise_adds_only_unbooked_premium_once(option, premium):
    row = _covered_call_lifecycle_outcomes(
        {"cost_basis": 100, "realized_pnl": 600, "current_unrealized": 900},
        strike=110, option_net=200, contract_qty=2, open_option=option,
    )
    assert row["exercise_result"] == pytest.approx(2800 + premium)
    assert row["no_exercise_result"] == 1700


def test_shop_baseline_and_roll_flow_through_dashboard_payload():
    realized = 343.43852 + 217.9588986
    premium = 248.94426
    payload = {
        "dashboard": {"request": {"as_of": "2026-09-27"}},
        "positions": {
            "inventory": [{"ticker": "SHOP", "shares": 100, "cost_per_share": 140,
                           "current_price": 142.27, "covered_shares": 100}],
            "open_option_shorts": [{
                "ticker": "SHOP", "option_type": "Call", "strike": 140,
                "expiration": "2026-10-16", "quantity": -1,
                "days_to_expiration": 19, "accounting_open_premium": premium,
                "strategy_premium_collected": premium,
            }],
        },
        "tickers": {"items": [{"ticker": "SHOP", "realized_options_pnl": realized,
                               "unrealized_pnl": premium, "total_pnl": realized + premium}]},
    }
    data = build_decision_lab_data(payload, option_market_data={
        "status": {"provider": "marketdata", "quote_date": "2026-09-24"},
        "contracts": [
            _contract("SHOP", 140, "2026-10-16", 6, 6.2, 0.55),
            _contract("SHOP", 140, "2026-11-20", 8, 8.2, 0.58),
        ],
    })
    rows = data["recommendation_candidates"][0]["candidates"]
    baseline = next(r for r in rows if r.get("is_current_position"))
    roll = next(r for r in rows if not r.get("is_current_position"))
    assert baseline["exercise_result"] == pytest.approx(810.3416786)
    assert baseline["no_exercise_result"] == pytest.approx(810.3416786)
    assert baseline["expected_value"] == pytest.approx(810.3416786)
    assert roll["exercise_result"] == pytest.approx(990.3416786)
    assert roll["expected_value_vs_current"] == pytest.approx(180)
    assert all(r["tradeability"] == "dated estimate" for r in rows)
    assert all(r["quote_date"] == "2026-09-24" for r in rows)
