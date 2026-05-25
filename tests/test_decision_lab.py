from __future__ import annotations

from portfolio_backend.decision_lab import build_decision_lab_data


def test_active_assignment_recovery_is_review_not_avoid():
    payload = {
        "dashboard": {"snapshot": {}, "monthly_target": {}, "issue_summary": {"total_count": 0}},
        "positions": {
            "inventory": [
                {
                    "ticker": "FUTU",
                    "shares": 100,
                    "cost_per_share": 160.0,
                    "unrealized_pnl": -5269.0,
                    "covered_shares": 0,
                }
            ],
            "open_option_shorts": [],
        },
        "tickers": {
            "items": [
                {
                    "ticker": "FUTU",
                    "realized_options_pnl": 200.0,
                    "realized_stock_pnl": 0.0,
                    "unrealized_pnl": -5469.0,
                    "total_pnl": -5269.0,
                }
            ]
        },
        "monthly": {"months": []},
        "issues": {"issues": []},
    }

    data = build_decision_lab_data(payload)

    action = next(item for item in data["action_queue"] if item["ticker"] == "FUTU" and item["source"] == "ticker quality")
    assert action["reason"] == "Negative total P&L during assignment recovery"
    assert action["suggested_action"] == "review before adding"

    scorecard_row = next(item for item in data["ticker_scorecard"] if item["ticker"] == "FUTU")
    assert scorecard_row["status"] == "Review"
