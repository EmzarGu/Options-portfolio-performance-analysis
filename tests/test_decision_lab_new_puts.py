from __future__ import annotations

from datetime import date

import pytest

from portfolio_backend.decision_lab import build_decision_lab_data, new_put_watchlist
from portfolio_backend.decision_lab_candidates import NEW_PUT_CATEGORY, recommendation_candidates
from portfolio_backend.option_market.marketdata import Selection
from portfolio_backend.option_market.marketdata_local import plan_requests


def _payload():
    return {
        "dashboard": {"request": {"as_of": "2026-10-15"}, "snapshot": {}, "issue_summary": {"total_count": 0}},
        "positions": {
            "inventory": [],
            "open_option_shorts": [
                {
                    "ticker": "HELD",
                    "option_type": "Put",
                    "strike": 40.0,
                    "expiration": "2026-11-20",
                    "days_to_expiration": 36,
                    "quantity": -1,
                    "current_price": 50.0,
                    "moneyness": 0.2,
                }
            ],
        },
        "tickers": {"items": []},
        "monthly": {"months": []},
        "issues": {"issues": []},
    }


def _put(strike, bid, ask, delta, expiry="2026-11-20", ticker="NEWCO"):
    return {
        "provider": "marketdata", "ticker": ticker, "expiry": expiry, "put_call": "PUT", "strike": strike,
        "bid": bid, "ask": ask, "mark": (bid + ask) / 2, "underlying_price": 100.0, "delta": delta,
        "open_interest": 800, "volume": 120, "raw": {"price_source": "quote_midpoint"},
    }


def test_watchlist_parsing_dedupes_validates_and_caps():
    raw = "newco, NEWCO; bad$, brk.b " + " ".join(f"T{chr(65 + i)}" for i in range(20))

    tickers = new_put_watchlist(raw)

    assert tickers[:2] == ["NEWCO", "BRK.B"]
    assert len(tickers) == 10


def test_watchlist_skips_tickers_already_in_the_wheel():
    data = build_decision_lab_data(_payload(), put_watchlist=["NEWCO", "HELD"])

    new_rows = [row for row in data["recommendation_candidates"] if row["category"] == NEW_PUT_CATEGORY]
    assert [row["ticker"] for row in new_rows] == ["NEWCO"]
    assert all(row["category"] != NEW_PUT_CATEGORY for row in data["ticker_situations"])


def test_no_watchlist_means_no_new_put_rows(monkeypatch):
    monkeypatch.delenv("DECISION_LAB_PUT_WATCHLIST", raising=False)

    data = build_decision_lab_data(_payload())

    assert all(row["category"] != NEW_PUT_CATEGORY for row in data["recommendation_candidates"])


def test_new_put_requests_one_monthly_near_35_dte_with_wheel_deltas():
    situation = {"ticker": "NEWCO", "category": NEW_PUT_CATEGORY, "objective": "", "current_state": {"open_options": []}}

    groups = recommendation_candidates([situation], as_of=date(2026, 10, 15))
    _, plan = plan_requests(groups, as_of=date(2026, 10, 15))

    assert groups[0]["contract_requests"] == [{"expiry": "2026-11-20", "put_call": "PUT"}]
    assert [selection for _, selection in plan] == [Selection(delta=0.15), Selection(delta=0.2), Selection(delta=0.25)]


def test_new_put_candidates_rank_by_monthly_return_within_wheel_rules():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-15T12:00:00+00:00"},
        "contracts": [
            _put(92.0, 1.80, 1.90, -0.25),
            _put(90.0, 1.30, 1.38, -0.20),
            _put(87.0, 0.80, 0.86, -0.15),
            _put(80.0, 0.20, 0.24, -0.06),
            _put(95.0, 2.60, 2.70, -0.33),
        ],
    }

    data = build_decision_lab_data(_payload(), option_market_data=option_market_data, put_watchlist=["NEWCO"])
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "NEWCO")

    strikes = [row["strike"] for row in group["candidates"]]
    assert sorted(strikes) == [87.0, 90.0, 92.0]
    best = group["recommended"]
    assert best["action"] == "Sell cash-secured put"
    assert best["collateral"] == pytest.approx(best["strike"] * 100)
    expected = (best["premium"] - 1.0) / best["collateral"] * 30 / 36
    assert best["monthly_return"] == pytest.approx(expected)
    rejections = group["candidate_status"]["rejection_counts"]
    assert rejections["delta too high"] == 1
    assert rejections["premium too small"] + rejections.get("delta too low", 0) == 1


def test_new_put_rejects_expiry_outside_20_to_45_dte():
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-10-15T12:00:00+00:00"},
        "contracts": [_put(90.0, 2.10, 2.20, -0.20, expiry="2026-12-18")],
    }

    data = build_decision_lab_data(_payload(), option_market_data=option_market_data, put_watchlist=["NEWCO"])
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "NEWCO")

    assert group["candidates"] == []
    assert group["candidate_status"]["rejection_counts"] == {"outside 20-45 DTE": 1}
