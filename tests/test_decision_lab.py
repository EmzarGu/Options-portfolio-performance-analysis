from __future__ import annotations

import pytest

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
            "accounting_open_premium": 121.0,
            "strategy_premium_collected": 121.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "YETI", "total_pnl": 250.0, "realized_options_pnl": 395.0, "unrealized_pnl": 250.0}]

    data = build_decision_lab_data(payload)

    yeti_rows = [item for item in data["ticker_situations"] if item["ticker"] == "YETI"]
    assert len(yeti_rows) == 1
    assert yeti_rows[0]["category"] == "Accept / monitor exit"
    assert yeti_rows[0]["recommendation"] == "Accept exit unless roll is clearly better"
    assert yeti_rows[0]["total_pnl"] == pytest.approx(250.0)
    assert yeti_rows[0]["signal_label"] == "Covered-call upside foregone"
    assert yeti_rows[0]["signal_value"] == pytest.approx(-115.0)
    assert yeti_rows[0]["open_risk_drag"] == pytest.approx(-115.0)

    candidate = next(item for item in data["recommendation_candidates"] if item["ticker"] == "YETI")
    assert candidate["candidates"] == []
    assert candidate["current_state"]["assigned_shares"] == pytest.approx(100.0)
    assert candidate["current_state"]["cost_basis"] == pytest.approx(42.5)


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
            "accounting_open_premium": 230.0,
            "strategy_premium_collected": 230.0,
        },
        {
            "ticker": "ATI",
            "option_type": "Put",
            "strike": 145.0,
            "expiration": "2026-07-17",
            "days_to_expiration": 53,
            "quantity": -1,
            "current_price": 140.0,
            "accounting_open_premium": 286.0,
            "strategy_premium_collected": 286.0,
        },
    ]

    data = build_decision_lab_data(payload)

    assert data["active_cycle"]["cycle"] == "2026-06"
    assert data["active_cycle"]["cycle_put_exposure"] == 2900.0
    assert data["active_cycle"]["portfolio_put_exposure"] == 17400.0
    assert data["active_cycle"]["portfolio_itm_put_exposure"] == 14500.0


def test_active_cycle_uses_dashboard_month_target_basis_not_put_exposure():
    payload = _base_payload()
    payload["dashboard"]["monthly_target"] = {"target_return": 0.02, "target_pnl": 6000.0}
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "CROX",
            "option_type": "Call",
            "strike": 105.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 110.0,
            "accounting_open_premium": 100.0,
            "strategy_premium_collected": 100.0,
        },
        {
            "ticker": "BEN",
            "option_type": "Put",
            "strike": 29.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 31.0,
            "accounting_open_premium": 200.0,
            "strategy_premium_collected": 200.0,
        },
    ]
    payload["monthly"]["future_months"] = [
        {
            "month": "2026-06-30",
            "projected_month_pnl": 300.0,
            "open_expiring_incremental_premium": 300.0,
        }
    ]

    data = build_decision_lab_data(payload)
    cycle = data["active_cycle"]

    assert cycle["projected_pnl"] == pytest.approx(300.0)
    assert "open_option_net" not in cycle
    assert cycle["open_premium_collected"] == pytest.approx(300.0)
    assert cycle["itm_put_unrealized_loss"] == pytest.approx(0.0)
    assert cycle["covered_call_upside_foregone"] == pytest.approx(-500.0)
    assert cycle["target_base"] == pytest.approx(300000.0)
    assert cycle["target_pnl"] == pytest.approx(6000.0)
    assert cycle["projected_return_roac"] == pytest.approx(0.001)
    assert cycle["cycle_put_exposure"] == pytest.approx(2900.0)
    assert cycle["open_ticker_count"] == 2


def test_active_cycle_uses_latest_monthly_capital_when_current_cycle_has_no_row():
    payload = _base_payload()
    payload["dashboard"]["request"] = {"as_of": "2026-06-01"}
    payload["dashboard"]["monthly_target"] = {"target_return": 0.015}
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "BEN",
            "option_type": "Put",
            "strike": 29.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 17,
            "quantity": -1,
            "current_price": 31.0,
            "accounting_open_premium": 200.0,
            "strategy_premium_collected": 200.0,
        },
    ]
    payload["monthly"]["months"] = [
        {"month": "2026-04-30", "avg_capital": 250000.0},
        {"month": "2026-05-31", "avg_capital": 300000.0},
    ]

    cycle = build_decision_lab_data(payload)["active_cycle"]

    assert cycle["cycle"] == "2026-06"
    assert cycle["target_base"] == pytest.approx(300000.0)
    assert cycle["target_pnl"] == pytest.approx(4500.0)
    assert cycle["projected_return_roac"] == pytest.approx(200.0 / 300000.0)


def test_active_cycle_excludes_broad_stock_unrealized_from_projected_pnl():
    payload = _base_payload()
    payload["dashboard"]["snapshot"] = {"current_stock_unrealized_pnl": 150.0}
    payload["dashboard"]["monthly_target"] = {"target_return": 0.02, "target_pnl": 6000.0}
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "BEN",
            "option_type": "Put",
            "strike": 29.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 31.0,
            "accounting_open_premium": 200.0,
            "strategy_premium_collected": 200.0,
        },
    ]

    cycle = build_decision_lab_data(payload)["active_cycle"]

    assert cycle["open_premium_collected"] == pytest.approx(200.0)
    assert cycle["stock_unrealized_pnl"] == pytest.approx(150.0)
    assert cycle["projected_pnl"] == pytest.approx(200.0)


def test_active_cycle_adds_only_itm_option_assignment_components_to_projected_pnl():
    payload = _base_payload()
    payload["dashboard"]["monthly_target"] = {"target_return": 0.02, "target_pnl": 6000.0}
    payload["positions"]["inventory"] = [
        {"ticker": "CALL", "shares": 100, "cost_per_share": 45.0},
        {"ticker": "OTMC", "shares": 100, "cost_per_share": 30.0},
    ]
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "PUT",
            "option_type": "Put",
            "strike": 100.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 80.0,
            "accounting_open_premium": 200.0,
            "strategy_premium_collected": 200.0,
        },
        {
            "ticker": "CALL",
            "option_type": "Call",
            "strike": 50.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 60.0,
            "accounting_open_premium": 100.0,
            "strategy_premium_collected": 100.0,
        },
        {
            "ticker": "OTMC",
            "option_type": "Call",
            "strike": 70.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 24,
            "quantity": -1,
            "current_price": 60.0,
            "accounting_open_premium": 50.0,
            "strategy_premium_collected": 50.0,
        },
    ]

    cycle = build_decision_lab_data(payload)["active_cycle"]

    assert cycle["open_premium_collected"] == pytest.approx(350.0)
    assert cycle["itm_put_unrealized_loss"] == pytest.approx(-2000.0)
    assert cycle["itm_call_stock_pnl"] == pytest.approx(500.0)
    assert cycle["projected_pnl"] == pytest.approx(-1150.0)


def test_missing_option_data_does_not_emit_provider_candidates():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "ASAN", "shares": 100, "cost_per_share": 17.5, "current_price": 6.51, "unrealized_pnl": -1099.0}
    ]
    payload["tickers"]["items"] = [{"ticker": "ASAN", "total_pnl": 2145.0, "realized_options_pnl": 6840.0, "unrealized_pnl": -4395.0}]

    first = build_decision_lab_data(payload)
    second = build_decision_lab_data(payload)

    assert first["recommendation_candidates"] == second["recommendation_candidates"]
    assert first["recommendation_candidates"][0]["recommended"] is None
    assert first["recommendation_candidates"][0]["candidates"] == []


def test_decision_lab_realized_and_total_include_dividends_when_components_present():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "NLR", "shares": 100, "cost_per_share": 150.0, "current_price": 132.55, "unrealized_pnl": -1745.0}
    ]
    payload["tickers"]["items"] = [
        {
            "ticker": "NLR",
            "total_pnl": -1000.0,
            "realized_options_pnl": 745.0,
            "realized_stock_pnl": 0.0,
            "dividends": 269.0,
            "combined_realized_pnl": 745.0,
            "unrealized_pnl": -1745.0,
        }
    ]

    data = build_decision_lab_data(payload)
    situation = next(row for row in data["ticker_situations"] if row["ticker"] == "NLR")
    current_state = data["recommendation_candidates"][0]["current_state"]

    assert situation["realized_pnl"] == pytest.approx(1014.0)
    assert situation["total_pnl"] == pytest.approx(-731.0)
    assert current_state["realized_pnl"] == pytest.approx(1014.0)
    assert current_state["ticker_total"] == pytest.approx(-731.0)


def test_stored_option_contracts_create_provider_candidate_inputs():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "ASAN", "shares": 100, "cost_per_share": 17.5, "current_price": 6.51, "unrealized_pnl": -1099.0}
    ]
    payload["tickers"]["items"] = [{"ticker": "ASAN", "total_pnl": 2145.0, "realized_options_pnl": 6840.0, "unrealized_pnl": -4395.0}]
    option_market_data = {
        "status": {
            "provider": "cutemarkets",
            "source": "stored",
            "last_fetched_at": "2026-05-25T12:00:00+00:00",
            "contract_count": 1,
            "quote_coverage_count": 1,
            "greek_coverage_count": 1,
        },
        "contracts": [
            {
                "provider": "cutemarkets",
                "request_id": "req-1",
                "ticker": "ASAN",
                "trade_date": "2026-05-25",
                "expiry": "2026-06-18",
                "put_call": "CALL",
                "strike": 18.0,
                "bid": 0.34,
                "ask": 0.36,
                "mark": 0.35,
                "underlying_price": 6.51,
                "delta": 0.14,
                "volatility": 0.44,
                "open_interest": 120,
                "volume": 15,
                "contract_symbol": "O:ASAN260618C00018000",
                "raw": {"price_source": "quote_midpoint"},
            }
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    candidate = data["recommendation_candidates"][0]["recommended"]

    assert candidate["provider"] == "cutemarkets"
    assert candidate["price_source"] == "quote_midpoint"
    assert candidate["premium"] == pytest.approx(34.0)
    assert candidate["delta"] == pytest.approx(0.14)
    assert data["option_market_data"]["status"]["contract_count"] == 1


def test_unquoted_option_contracts_do_not_create_recommendations():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "ASAN", "shares": 100, "cost_per_share": 17.5, "current_price": 6.51, "unrealized_pnl": -1099.0}
    ]
    payload["tickers"]["items"] = [{"ticker": "ASAN", "total_pnl": 2145.0, "realized_options_pnl": 6840.0, "unrealized_pnl": -4395.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-25T12:00:00+00:00"},
        "contracts": [
            {
                "provider": "cutemarkets",
                "ticker": "ASAN",
                "expiry": "2026-06-18",
                "put_call": "CALL",
                "strike": 20.0,
                "bid": None,
                "ask": None,
                "mark": 0.05,
                "underlying_price": 6.51,
                "delta": 0.08,
                "open_interest": 500,
                "volume": 20,
                "raw": {"price_source": "fmv"},
            }
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]

    assert group["recommended"] is None
    assert group["candidates"] == []
    assert group["candidate_status"]["status"] == "no_actionable_contracts"
    assert group["candidate_status"]["rejection_counts"]["premium too small"] == 1


def test_unquoted_delayed_contracts_can_create_indicative_recommendations():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "NLR", "shares": 100, "cost_per_share": 150.0, "current_price": 131.82, "unrealized_pnl": -1818.0}
    ]
    payload["tickers"]["items"] = [{"ticker": "NLR", "total_pnl": -1073.0, "realized_options_pnl": 745.0, "unrealized_pnl": -1818.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-25T12:00:00+00:00"},
        "contracts": [
            {
                "provider": "cutemarkets",
                "ticker": "NLR",
                "expiry": "2026-07-02",
                "put_call": "CALL",
                "strike": 150.0,
                "bid": None,
                "ask": None,
                "mark": 1.55,
                "underlying_price": 131.82,
                "delta": 0.24,
                "open_interest": 100,
                "volume": 20,
                "raw": {"price_source": "fmv"},
            }
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = data["recommendation_candidates"][0]
    candidate = group["recommended"]

    assert group["candidate_status"]["status"] == "indicative"
    assert candidate["tradeability"] == "indicative"
    assert candidate["quote_coverage"] is False
    assert candidate["premium"] == pytest.approx(155.0)


def test_recovery_covered_call_prefers_near_basis_not_far_otm_strikes():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {
            "ticker": "NLR",
            "shares": 100,
            "cost_per_share": 150.0,
            "current_price": 132.0,
            "unrealized_pnl": -1800.0,
            "covered_shares": 0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "NLR", "total_pnl": -1000.0, "realized_options_pnl": 745.0, "unrealized_pnl": -1800.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-27T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "NLR", "expiry": "2026-06-18", "put_call": "CALL", "strike": 150.0, "mark": 1.55, "underlying_price": 132.0, "delta": 0.24, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "NLR", "expiry": "2026-06-18", "put_call": "CALL", "strike": 185.0, "mark": 1.49, "underlying_price": 132.0, "delta": 0.09, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "NLR", "expiry": "2026-06-18", "put_call": "CALL", "strike": 195.0, "mark": 4.50, "underlying_price": 132.0, "delta": 0.09, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "NLR")

    assert group["recommended"]["strike"] == pytest.approx(150.0)
    assert all(row["strike"] <= 172.5 for row in group["candidates"])
    assert {row["strike"] for row in group["candidates"]} == {150.0}


def test_covered_call_rolls_are_scored_as_packages_not_new_leg_only():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "CROX", "shares": 100, "cost_per_share": 85.0, "current_price": 115.0, "unrealized_pnl": 3000.0, "covered_shares": 100}
    ]
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "CROX",
            "option_type": "Call",
            "strike": 105.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 23,
            "quantity": -1,
            "current_price": 115.0,
            "accounting_open_premium": 38.0,
            "strategy_premium_collected": 38.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "CROX", "total_pnl": 7049.0, "realized_options_pnl": 5049.0, "unrealized_pnl": 2000.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-25T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "CROX", "expiry": "2026-06-18", "put_call": "CALL", "strike": 105.0, "mark": 13.0, "underlying_price": 115.0, "delta": 0.8, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "CROX", "expiry": "2026-06-18", "put_call": "CALL", "strike": 125.0, "mark": 2.0, "underlying_price": 115.0, "delta": 0.25, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "CROX", "expiry": "2026-07-10", "put_call": "CALL", "strike": 105.0, "mark": 15.0, "underlying_price": 115.0, "delta": 0.72, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "CROX")
    actions = [row["action"] for row in group["candidates"]]

    assert "Roll up same expiry" not in actions
    same_strike_roll = next(row for row in group["candidates"] if row["action"] == "Roll out same strike")
    assert same_strike_roll["roll_close_cost"] == pytest.approx(1300.0)
    assert same_strike_roll["roll_new_credit"] == pytest.approx(1500.0)
    assert same_strike_roll["roll_net_credit"] == pytest.approx(200.0)
    assert same_strike_roll["incremental_exit_pnl"] == pytest.approx(200.0)


def test_recovery_call_rolls_compare_expected_value_not_only_higher_strikes():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {
            "ticker": "FUTU",
            "shares": 100,
            "cost_per_share": 147.45,
            "current_price": 110.0,
            "unrealized_pnl": -3745.0,
            "covered_shares": 100,
        }
    ]
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "FUTU",
            "option_type": "Call",
            "strike": 147.45,
            "expiration": "2026-06-18",
            "days_to_expiration": 22,
            "quantity": -1,
            "current_price": 110.0,
            "accounting_open_premium": 288.0,
            "strategy_premium_collected": 288.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "FUTU", "total_pnl": -5269.0, "realized_options_pnl": 218.0, "unrealized_pnl": -3745.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-27T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "FUTU", "expiry": "2026-06-18", "put_call": "CALL", "strike": 147.45, "mark": 0.62, "underlying_price": 110.0, "delta": 0.073, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "FUTU", "expiry": "2026-06-18", "put_call": "CALL", "strike": 125.0, "mark": 2.60, "underlying_price": 110.0, "delta": 0.26, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "FUTU", "expiry": "2026-06-18", "put_call": "CALL", "strike": 130.0, "mark": 1.90, "underlying_price": 110.0, "delta": 0.195, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "FUTU", "expiry": "2026-07-17", "put_call": "CALL", "strike": 160.0, "mark": 1.00, "underlying_price": 110.0, "delta": 0.084, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "FUTU")
    proposals = [row for row in group["candidates"] if not row.get("is_current_position")]

    assert proposals
    assert proposals[0]["action"] == "Roll down same expiry"
    assert proposals[0]["strike"] < 147.45
    assert proposals[0]["expected_value_vs_current"] >= proposals[-1]["expected_value_vs_current"]


def test_covered_call_candidate_economics_scale_with_contract_count():
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "CCJ", "shares": 200, "cost_per_share": 105.0, "current_price": 108.25, "unrealized_pnl": 650.0, "covered_shares": 200}
    ]
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "CCJ",
            "option_type": "Call",
            "strike": 120.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 23,
            "quantity": -2,
            "current_price": 108.25,
            "accounting_open_premium": 147.0,
            "strategy_premium_collected": 147.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "CCJ", "total_pnl": 2566.0, "realized_options_pnl": 1916.0, "unrealized_pnl": 650.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-25T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "CCJ", "expiry": "2026-06-18", "put_call": "CALL", "strike": 120.0, "mark": 2.18, "underlying_price": 108.25, "delta": 0.26, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "CCJ", "expiry": "2026-07-17", "put_call": "CALL", "strike": 130.0, "mark": 2.6, "underlying_price": 108.25, "delta": 0.23, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "CCJ")
    baseline = next(row for row in group["candidates"] if row["action"] == "Accept / monitor exit")
    roll = next(row for row in group["candidates"] if row["action"] == "Roll up/out")

    assert baseline["contract_count"] == 2
    assert baseline["roll_close_cost"] == pytest.approx(436.0)
    assert baseline["exit_pnl"] == pytest.approx(3147.0)
    assert baseline["upside_foregone"] == pytest.approx(0.0)
    assert roll["contract_count"] == 2
    assert roll["roll_new_credit"] == pytest.approx(520.0)
    assert roll["roll_net_credit"] == pytest.approx(84.0)
    assert roll["upside_left"] == pytest.approx(2000.0)
    assert roll["incremental_exit_pnl"] == pytest.approx(2084.0)


def test_far_otm_short_put_can_generate_roll_up_candidate():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "QCOM",
            "option_type": "Put",
            "strike": 100.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 23,
            "quantity": -1,
            "current_price": 140.0,
            "moneyness": -0.40,
            "accounting_open_premium": 90.0,
            "strategy_premium_collected": 90.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "QCOM", "total_pnl": 90.0, "realized_options_pnl": 0.0, "unrealized_pnl": 0.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-25T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "QCOM", "expiry": "2026-06-18", "put_call": "PUT", "strike": 100.0, "mark": 0.2, "underlying_price": 140.0, "delta": -0.03, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "QCOM", "expiry": "2026-06-18", "put_call": "PUT", "strike": 120.0, "mark": 1.35, "underlying_price": 140.0, "delta": -0.22, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    situation = next(row for row in data["ticker_situations"] if row["ticker"] == "QCOM")
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "QCOM")
    roll = next(row for row in group["candidates"] if row["action"] == "Roll put up")

    assert situation["category"] == "Harvest unused put risk"
    assert roll["roll_close_cost"] == pytest.approx(20.0)
    assert roll["roll_new_credit"] == pytest.approx(135.0)
    assert roll["roll_net_credit"] == pytest.approx(115.0)
    assert roll["added_assignment_exposure"] == pytest.approx(2000.0)


def test_near_strike_short_put_rolls_down_not_up():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "IBKR",
            "option_type": "Put",
            "strike": 77.5,
            "expiration": "2026-06-18",
            "days_to_expiration": 22,
            "quantity": -4,
            "current_price": 80.75,
            "moneyness": 0.04,
            "accounting_open_premium": 516.0,
            "strategy_premium_collected": 516.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "IBKR", "total_pnl": 3577.0, "realized_options_pnl": 2161.0, "unrealized_pnl": 516.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-05-27T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "IBKR", "expiry": "2026-06-18", "put_call": "PUT", "strike": 77.5, "mark": 1.2, "underlying_price": 80.75, "delta": -0.32, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "IBKR", "expiry": "2026-06-18", "put_call": "PUT", "strike": 95.0, "mark": 12.2, "underlying_price": 80.75, "delta": -0.88, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "IBKR", "expiry": "2026-06-18", "put_call": "PUT", "strike": 100.0, "mark": 14.8, "underlying_price": 80.75, "delta": -0.90, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "IBKR", "expiry": "2026-07-17", "put_call": "PUT", "strike": 72.5, "mark": 1.45, "underlying_price": 80.75, "delta": -0.21, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    situation = next(row for row in data["ticker_situations"] if row["ticker"] == "IBKR")
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "IBKR")
    roll = next(row for row in group["candidates"] if row["action"] == "Roll put down/out")

    assert situation["category"] == "Monitor assignment risk"
    assert all(row["strike"] <= 77.5 for row in group["candidates"])
    assert roll["strike"] == pytest.approx(72.5)
    assert roll["assignment_risk_reduction"] == pytest.approx(2000.0)


def test_short_put_roll_down_rejects_stale_indicative_day_close():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "PLD",
            "option_type": "Put",
            "strike": 135.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 16,
            "quantity": -2,
            "current_price": 139.04,
            "moneyness": 0.03,
            "accounting_open_premium": 296.0,
            "strategy_premium_collected": 296.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "PLD", "total_pnl": 296.0, "realized_options_pnl": 0.0, "unrealized_pnl": 296.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-06-02T12:00:00+00:00"},
        "contracts": [
            {
                "provider": "cutemarkets",
                "ticker": "PLD",
                "expiry": "2026-06-18",
                "put_call": "PUT",
                "strike": 135.0,
                "mark": 1.75,
                "underlying_price": 139.04,
                "delta": -0.30,
                "open_interest": 100,
                "volume": 20,
                "raw": {"price_source": "fmv"},
            },
            {
                "provider": "cutemarkets",
                "ticker": "PLD",
                "expiry": "2026-06-18",
                "put_call": "PUT",
                "strike": 85.0,
                "mark": 2.12,
                "underlying_price": 139.04,
                "delta": -0.03,
                "open_interest": 32,
                "volume": 1,
                "raw": {
                    "price_source": "day_close",
                    "source": {"day": {"last_updated": 1756843200000000000}},
                },
            },
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "PLD")

    assert [row["action"] for row in group["candidates"]] == ["Keep current put"]
    assert all(row["strike"] != pytest.approx(85.0) for row in group["candidates"])


def test_short_put_roll_down_rejects_same_expiry_price_inversion():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "PLD",
            "option_type": "Put",
            "strike": 135.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 16,
            "quantity": -2,
            "current_price": 139.04,
            "moneyness": 0.03,
            "accounting_open_premium": 296.0,
            "strategy_premium_collected": 296.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "PLD", "total_pnl": 296.0, "realized_options_pnl": 0.0, "unrealized_pnl": 296.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-06-02T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "PLD", "expiry": "2026-06-18", "put_call": "PUT", "strike": 135.0, "mark": 1.75, "underlying_price": 139.04, "delta": -0.30, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "PLD", "expiry": "2026-06-18", "put_call": "PUT", "strike": 85.0, "mark": 2.12, "underlying_price": 139.04, "delta": -0.03, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "PLD")

    assert [row["action"] for row in group["candidates"]] == ["Keep current put"]
    assert all(row["strike"] != pytest.approx(85.0) for row in group["candidates"])


def test_short_put_roll_down_rejects_negative_expected_value():
    payload = _base_payload()
    payload["positions"]["open_option_shorts"] = [
        {
            "ticker": "PLD",
            "option_type": "Put",
            "strike": 135.0,
            "expiration": "2026-06-18",
            "days_to_expiration": 16,
            "quantity": -2,
            "current_price": 139.04,
            "moneyness": 0.03,
            "accounting_open_premium": 296.0,
            "strategy_premium_collected": 296.0,
        }
    ]
    payload["tickers"]["items"] = [{"ticker": "PLD", "total_pnl": 296.0, "realized_options_pnl": 0.0, "unrealized_pnl": 296.0}]
    option_market_data = {
        "status": {"provider": "cutemarkets", "source": "stored", "last_fetched_at": "2026-06-02T12:00:00+00:00"},
        "contracts": [
            {"provider": "cutemarkets", "ticker": "PLD", "expiry": "2026-06-18", "put_call": "PUT", "strike": 135.0, "mark": 1.75, "underlying_price": 139.04, "delta": -0.30, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "PLD", "expiry": "2026-07-17", "put_call": "PUT", "strike": 125.0, "mark": 1.04, "underlying_price": 139.04, "delta": -0.14, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
            {"provider": "cutemarkets", "ticker": "PLD", "expiry": "2026-07-17", "put_call": "PUT", "strike": 130.0, "mark": 2.08, "underlying_price": 139.04, "delta": -0.23, "open_interest": 100, "volume": 20, "raw": {"price_source": "fmv"}},
        ],
    }

    data = build_decision_lab_data(payload, option_market_data=option_market_data)
    group = next(row for row in data["recommendation_candidates"] if row["ticker"] == "PLD")

    assert any(row["strike"] == pytest.approx(130.0) for row in group["candidates"])
    assert all(row["strike"] != pytest.approx(125.0) for row in group["candidates"])


def test_strike_quality_splits_puts_and_calls_by_risk_bucket():
    payload = _base_payload()
    payload["tickers"]["items"] = [
        {
            "ticker": "AAA",
            "realized_options_pnl": 150.0,
            "realized_stock_pnl": -25.0,
            "dividends": 5.0,
            "unrealized_pnl": -10.0,
            "total_pnl": 120.0,
        },
        {
            "ticker": "BBB",
            "realized_options_pnl": 120.0,
            "realized_stock_pnl": 0.0,
            "dividends": 0.0,
            "unrealized_pnl": 0.0,
            "total_pnl": 120.0,
        },
    ]
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
    assert sum(row["count"] for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"]) == 1
    assert sum(row["count"] for row in data["strike_quality"]["call_exit_quality"]["bucket_summary"]) == 1
    assert [row["bucket"] for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"]] == [
        "<=15%",
        "15-20%",
        "20-25%",
        "25-30%",
        ">30%",
    ]
    assert [row["bucket"] for row in data["strike_quality"]["call_exit_quality"]["bucket_summary"]] == [
        "<=15%",
        "15-20%",
        "20-25%",
        "25-30%",
        ">30%",
    ]
    put_bucket = next(row for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"] if row["count"])
    assert put_bucket["option_pnl"] == pytest.approx(150.0)
    assert put_bucket["stock_pnl"] == pytest.approx(-25.0)
    assert put_bucket["unrealized_drag"] == pytest.approx(-10.0)
    assert put_bucket["lifecycle_pnl"] == pytest.approx(120.0)
    assert put_bucket["pnl_per_capital"] == pytest.approx(120.0 / 9500.0)
    assert put_bucket["attribution_rate"] == pytest.approx(1.0)


def test_strike_quality_missing_attribution_is_none_not_zero():
    payload = _base_payload()
    matches = [
        {
            "matched": True,
            "assignment_risk_proxy": 0.22,
            "profit_probability": 0.78,
            "trade": {"ticker": "AAA", "put_call": "Put", "strike": 95, "qty": 1, "net_cash": 100},
        }
    ]

    data = build_decision_lab_data(payload, probability_matches=matches)

    put_bucket = next(row for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"] if row["count"])
    assert put_bucket["opening_premium"] == pytest.approx(100.0)
    assert put_bucket["option_pnl"] is None
    assert put_bucket["stock_pnl"] is None
    assert put_bucket["unrealized_drag"] is None
    assert put_bucket["lifecycle_pnl"] is None
    assert put_bucket["pnl_per_capital"] is None
    assert put_bucket["attribution_rate"] == pytest.approx(0.0)


def test_coverage_notes_are_compact_status_lines():
    payload = _base_payload()
    payload["dashboard"]["data_freshness"] = {"price_coverage": {"stocks_requested": 3, "stocks_fetched": 2, "missing_count": 1}}
    matches = [{"matched": True, "trade": {"put_call": "Put", "net_cash": 100, "strike": 50, "qty": 1}, "assignment_risk_proxy": 0.2}]

    data = build_decision_lab_data(payload, probability_matches=matches)

    messages = [note["message"] for note in data["coverage_notes"]]
    assert all(note["severity"] == "status" for note in data["coverage_notes"])
    assert "Price coverage: 2/3 tickers." in messages
    assert "Historical risk proxy: 1/1 short-option opening trades." in messages
    assert "Lifecycle attribution: option 0/1, stock 0/1, full 0/1." in messages
    assert not any("not a trading recommendation" in message.lower() for message in messages)


def test_decision_lab_uses_historical_provider_enrichments_for_strike_quality():
    payload = _base_payload()
    payload["tickers"]["items"] = [
        {
            "ticker": "AAA",
            "realized_options_pnl": 150.0,
            "realized_stock_pnl": -25.0,
            "dividends": 5.0,
            "unrealized_pnl": -10.0,
            "total_pnl": 120.0,
        }
    ]
    historical = [
        {
            "enrichment_id": "hist-1",
            "provider": "cutemarkets",
            "contract_symbol": "O:AAA240419P00095000",
            "provider_contract_matched": True,
            "option_close": 2.1,
            "option_vwap": 2.08,
            "trade": {
                "trade_id": "trade-1",
                "ticker": "AAA",
                "trade_date": "2024-03-15",
                "expiry": "2024-04-19",
                "put_call": "PUT",
                "strike": 95.0,
                "qty": 1.0,
                "trade_price": 2.14,
                "net_cash": 214.0,
                "source": "ibkr",
                "profit_probability": 0.78,
                "assignment_risk_proxy": 0.22,
            },
        }
    ]

    data = build_decision_lab_data(payload, historical_enrichments=historical)

    coverage = data["strike_quality"]["coverage"]
    assert coverage["historical_provider_trade_count"] == 1
    assert coverage["historical_provider_contract_match_count"] == 1
    assert coverage["historical_provider_option_price_count"] == 1
    assert coverage["risk_proxy_count"] == 1
    put_bucket = next(row for row in data["strike_quality"]["put_entry_quality"]["bucket_summary"] if row["count"])
    assert put_bucket["bucket"] == "20-25%"
    assert put_bucket["opening_premium"] == pytest.approx(214.0)
    assert put_bucket["option_pnl"] == pytest.approx(150.0)
    messages = [note["message"] for note in data["coverage_notes"]]
    assert "Historical option facts: 1 trades, 1 contract matches, 1 option price observations." in messages
