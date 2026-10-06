from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import visual_prototype
from portfolio_backend.visual_prototype import build_visual_prototype_data


def _dashboard_payload():
    return {
        "generated_at": "2026-07-10T12:00:00+00:00",
        "web": {"target_floor": 0.015, "target_return": 0.02},
        "dashboard": {
            "request": {"as_of": "2026-07-10"},
            "data_freshness": {"prices_updated_at": "2026-07-10T11:59:00+00:00"},
        },
        "monthly": {
            "active_cycle": {
                "cycle_label": "July 2026",
                "open_ticker_count": 2,
                "min_dte": 7,
                "max_dte": 7,
                "realized_cycle_pnl": 100.0,
                "open_premium_collected": 200.0,
                "projected_cycle_pnl": 300.0,
                "target_pnl": 400.0,
                "projected_return_roac": 0.015,
                "target_return": 0.02,
                "remaining_to_target": 100.0,
            },
            "months": [
                {
                    "month": "2026-07-31",
                    "total_realized_pnl": 100.0,
                    "projected_month_pnl": 300.0,
                    "projected_return_roac": 0.015,
                }
            ],
        },
        "open_shorts": {
            "items": [
                {
                    "ticker": "ABC",
                    "option_type": "Put",
                    "strike": 100.0,
                    "expiration": "2026-07-17",
                    "days_to_expiration": 7,
                    "current_price": 98.0,
                    "moneyness": 0.02,
                    "quantity": 2,
                    "accounting_open_premium": 250.0,
                },
                {
                    "ticker": "XYZ",
                    "option_type": "Call",
                    "strike": 55.0,
                    "expiration": "2026-08-21",
                    "days_to_expiration": 42,
                    "current_price": 50.0,
                    "moneyness": -0.10,
                    "quantity": 1,
                },
            ]
        },
        "positions": {
            "inventory": [
                {
                    "ticker": "XYZ",
                    "shares": 100,
                    "cost_per_share": 60.0,
                    "current_price": 50.0,
                    "buy_date": "2026-01-10",
                    "covered_shares": 100,
                    "covered_strike": 55.0,
                    "unrealized_pnl": -1000.0,
                }
            ]
        },
        "tickers": {"items": [{"ticker": "XYZ", "total_pnl": -400.0}]},
        "yearly": {
            "years": [
                {
                    "year": 2026,
                    "options_pnl": 1000.0,
                    "stock_pnl": -200.0,
                    "dividends": 50.0,
                    "realized": 850.0,
                    "roac": 0.12,
                }
            ]
        },
    }


def _decision_payload():
    return {
        "option_market_data": {
            "status": {
                "provider": "cutemarkets",
                "source": "stored",
                "contract_count": 20,
                "last_fetched_at": "2026-07-09T12:00:00+00:00",
            }
        },
        "ticker_situations": [
            {
                "ticker": "ABC",
                "priority": "high",
                "category": "Reduce assignment risk",
                "objective": "Reduce near-term risk",
                "recommendation": "Compare close vs roll",
                "expiry": "2026-07-17",
                "dte": 7,
                "realized_pnl": 500.0,
                "unrealized_pnl": -400.0,
                "total_pnl": 100.0,
                "signal_label": "ITM put unrealized loss",
                "signal_value": -400.0,
            }
        ],
        "recommendation_candidates": [
            {
                "ticker": "ABC",
                "category": "Reduce assignment risk",
                "objective": "Reduce near-term risk",
                "current_state": {"current_price": 98.0, "total_pnl": 100.0},
                "candidate_status": {"status": "available"},
                "candidates": [
                    {
                        "action": "Keep current put",
                        "strike": 100.0,
                        "expiry": "2026-07-17",
                        "dte": 7,
                        "premium": 250.0,
                        "expected_value": -100.0,
                        "expected_value_vs_current": 0.0,
                        "exercise_probability": 0.55,
                        "delta": 0.55,
                        "liquidity": "current",
                        "tradeability": "current",
                        "score": 65.0,
                        "is_current_position": True,
                    }
                ],
            }
        ],
    }


def test_visual_prototype_adapts_current_dashboard_data():
    payload = build_visual_prototype_data(_dashboard_payload(), _decision_payload())

    assert payload["active_cycle"]["cycle_label"] == "July 2026"
    assert payload["actions"][0]["group"] == "now"
    assert payload["risk_map"][0]["moneyness"] == 2.0
    assert payload["expiry_ladder"][0]["put_exposure"] == 20000.0
    assert payload["expiry_ladder"][0]["itm_put_exposure"] == 20000.0
    assert payload["recovery_map"][0]["status"] == "covered"
    assert payload["recovery_map"][0]["recovery_pct"] == pytest.approx(-100 / 6)
    assert payload["candidate_groups"][0]["candidates"][0]["expected_value"] == -100.0
    assert payload["yearly"][0]["dividends"] == 50.0


def test_visual_prototype_routes_do_not_require_production_dashboard_changes(monkeypatch):
    monkeypatch.setattr(
        visual_prototype,
        "_prototype_payload",
        lambda **_: {"active_cycle": {"cycle_label": "July 2026"}, "actions": []},
    )
    client = TestClient(visual_prototype.app)

    page = client.get("/")
    api = client.get("/api/prototype")

    assert page.status_code == 200
    assert "Visual prototype" in page.text
    assert "Assignment Outcomes" in page.text
    assert api.status_code == 200
    assert api.json()["active_cycle"]["cycle_label"] == "July 2026"
