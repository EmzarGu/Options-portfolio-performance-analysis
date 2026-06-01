from types import SimpleNamespace

import pandas as pd
import pytest

from portfolio_backend.cycle_projection import (
    build_state_cycle_projection,
    build_state_future_cycle_projections,
)


def _state():
    return SimpleNamespace(
        as_of=pd.Timestamp("2026-06-01"),
        open_options=pd.DataFrame(
            [
                {
                    "ticker": "PUTT",
                    "type": "Put",
                    "strike": 100.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-06-18"),
                    "open_price": 2.0,
                },
                {
                    "ticker": "CALL",
                    "type": "Call",
                    "strike": 50.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-06-18"),
                    "open_price": 1.0,
                },
                {
                    "ticker": "NEXT",
                    "type": "Put",
                    "strike": 80.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-07-17"),
                    "open_price": 1.5,
                },
            ]
        ),
        stock_prices={"PUTT": 90.0, "CALL": 60.0, "NEXT": 100.0},
        monthly_cycles=pd.DataFrame(
            [{"avg_capital": 10000.0, "total_realized_pnl": 0.0}],
            index=[pd.Timestamp("2026-05-31")],
        ),
        stock_unreal=500.0,
        unrealized_blocked=False,
    )


def test_cycle_projection_keeps_risk_signals_out_of_projected_pnl():
    cycle = build_state_cycle_projection(
        _state(),
        year_month=(2026, 6),
        target_return=0.02,
        include_stock_unrealized=True,
    ).to_dict()

    assert cycle["open_premium_collected"] == pytest.approx(300.0)
    assert cycle["stock_unrealized_pnl"] == pytest.approx(500.0)
    assert cycle["itm_put_unrealized_loss"] == pytest.approx(-1000.0)
    assert cycle["covered_call_upside_foregone"] == pytest.approx(-1000.0)
    assert cycle["projected_cycle_pnl"] == pytest.approx(800.0)
    assert cycle["target_pnl"] == pytest.approx(200.0)
    assert cycle["projected_return_roac"] == pytest.approx(0.08)


def test_future_cycle_projection_uses_same_canonical_shape():
    rows = build_state_future_cycle_projections(
        _state(),
        target_return=0.02,
        include_current=True,
    )

    assert [row["cycle"] for row in rows] == ["2026-06", "2026-07"]
    assert rows[0]["projected_cycle_pnl"] == pytest.approx(800.0)
    assert rows[1]["projected_cycle_pnl"] == pytest.approx(150.0)
    assert rows[1]["stock_unrealized_pnl"] is None
