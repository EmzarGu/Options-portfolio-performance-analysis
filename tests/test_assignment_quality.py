from __future__ import annotations

import pandas as pd
import pytest

from portfolio_backend.ibkr.assignment_quality import compounded_opportunity_cost, decision_read, months_between


def test_assignment_quality_months_between_prorates_days():
    months = months_between(pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-31"))

    assert months == pytest.approx(30 / 30.4375)


def test_compounded_opportunity_cost_grows_redeployment_base():
    cost = compounded_opportunity_cost(10000, 0.015, 2)

    assert cost == pytest.approx(10000 * ((1.015**2) - 1))
    assert cost > 10000 * 0.015 * 2


def test_assignment_quality_decision_read_uses_dollar_and_capital_thresholds():
    assert decision_read(-1600, 50000) == "Graduate candidate"
    assert decision_read(-900, 8000) == "Graduate candidate"
    assert decision_read(100, 8000) == "Wheel worked better"
    assert decision_read(-100, 8000) == "Small difference"
