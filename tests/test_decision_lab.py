from __future__ import annotations

from portfolio_backend.decision_lab import build_decision_lab_data


def _base_payload():
    return {
        "dashboard": {
            "request": {"as_of": "2026-05-25"},
            "snapshot": {},
            "monthly_target": {"target_return": 0.02},
            "issue_summary": {"total_count": 0},
        },
        "positions": {"inventory": [], "open_option_shorts": []},
        "tickers": {"items": []},
        "monthly": {"months": []},
        "issues": {"issues": []},
    }


def test_active_assignment_recovery_is_review_not_avoid():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {
            "ticker": "FUTU",
            "shares": 100,
            "cost_per_share": 160.0,
            "current_price": 105.31,
            "unrealized_pnl": -5269.0,
            "covered_shares": 0,
        }
    ]
    payload["tickers"]["items"] = [
        {
            "ticker": "FUTU",
            "realized_options_pnl": 200.0,
            "realized_stock_pnl": 0.0,
            "unrealized_pnl": -5469.0,
            "total_pnl": -5269.0,
        }
    ]

    data = build_decision_lab_data(payload)

    action = next(item for item in data["ticker_situations"] if item["ticker"] == "FUTU")
    assert action["category"] == "Recover with covered call"
    assert action["recommendation"] == "Find covered-call candidate"


def test_capped_holding_and_call_create_one_ticker_situation():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {
            "ticker": "YETI",
            "shares": 100,
            "cost_per_share": 42.5,
            "current_price": 46.15,
            "unrealized_pnl": 250.0,
            "covered_shares": 100,
        }
    ]
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "YETI",
            "option_type": "Call",
            "strike": 45.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 46.15,
            "display_premium_collected": 121.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "YETI", "total_pnl": 250.0, "realized_options_pnl": 395.0, "unrealized_pnl": 250.0}]

    data = build_decision_lab_data(payload)

    yeti_rows = [item for item in data["ticker_situations"] if item["ticker"] == "YETI"]
    assert len(yeti_rows) == 1
    assert yeti_rows[0]["category"] == "Accept / monitor exit"
    assert yeti_rows[0]["recommendation"] == "Accept exit unless roll is clearly better"


def test_active_cycle_promotes_next_open_expiry_and_splits_exposure():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "BEN",
            "option_type": "Put",
            "strike": 29.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 31.0,
            "display_premium_collected": 230.0,
        },
        {
            "ticker": "ATI",
            "option_type": "Put",
            "strike": 145.0,
            "expiration": "2026-07-17",
            "days_to_expiration": 53,
            "quantity": -1,
            "current_price": 140.0,
            "display_premium_collected": 286.0,
        },
    ]

    data = build_decision_lab_data(payload)

    assert data["active_cycle"]["cycle"] == "2026-06"
    assert data["active_cycle"]["cycle_put_exposure"] == 2900.0
    assert data["active_cycle"]["portfolio_put_exposure"] == 17400.0
    assert data["active_cycle"]["portfolio_itm_put_exposure"] == 14500.0


def test_simulated_candidates_are_deterministic_and_marked_simulated():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "ASAN", "shares": 100, "cost_per_share": 17.5, "current_price": 6.51, "unrealized_pnl": -1099.0}
    ]
    payload["tickers"]["items"] = [{"ticker": "ASAN", "total_pnl": 2145.0, "realized_options_pnl": 6840.0, "unrealized_pnl": -4395.0}]

    first = build_decision_lab_data(payload)
    second = build_decision_lab_data(payload)

    candidate = first["recommendation_candidates"][0]["recommended"]
    assert candidate == second["recommendation_candidates"][0]["recommended"]
    assert candidate["is_simulated"] is True
    assert {"action", "strike", "expiry", "dte", "premium", "iv", "liquidity", "score", "explanation"} <= set(candidate)


def test_strike_quality_splits_puts_and_calls_and_marks_estimated():
    payload = _base_payload()
    matches = [
        {
            "matched": True,
            "assignment_risk_proxy": 0.22,
            "profit_probability": 0.78,
            "trade": {"ticker": "AAA", "put_call": "Put", "strike": 95, "qty": 1, "net_cash": 100},
        },
        {
            "matched": True,
            "assignment_risk_proxy": 0.35,
            "profit_probability": 0.65,
            "trade": {"ticker": "BBB", "put_call": "Call", "strike": 110, "qty": 1, "net_cash": 120},
        },
    ]

    data = build_decision_lab_data(payload, probability_matches=matches)

    assert data["strike_quality"]["coverage"]["matched_count"] == 2
    assert data["strike_quality"]["put_entry_quality"]["estimated"] is True
    assert data["strike_quality"]["call_exit_quality"]["estimated"] is True
    assert sum(row["count"] for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"]) == 1
    assert sum(row["count"] for row in data["strike_quality"]["call_exit_quality"]["bucket_summary"]) == 1
