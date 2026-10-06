from __future__ import annotations

import pytest

from portfolio_backend.ibkr.assignment_quality import (
    _issue_mentions_active_ticker,
    build_assigned_holdings_review_analysis,
)
from tests.test_ibkr_accounting_cases import _option_eae, _report, _stock_eae, _trade


def _assigned_holding_report():
    put_open = _trade(
        symbol="EMN  260117P00070000",
        underlyingSymbol="EMN",
        description="EMN 17JAN26 70 P",
        conid="7001",
        tradeID="TP1",
        transactionID="XP1",
        ibExecID="EP1",
        tradeDate="20260110",
        dateTime="20260110;154500",
        expiry="20260117",
        strike="70",
        putCall="P",
        quantity="-4",
        tradePrice="2.00",
        proceeds="800",
        ibCommission="-4",
        netCash="796",
    )
    put_assignment = _option_eae(
        symbol="EMN  260117P00070000",
        underlyingSymbol="EMN",
        description="EMN 17JAN26 70 P",
        conid="7001",
        tradeID="TP1",
        date="20260117",
        expiry="20260117",
        strike="70",
        putCall="P",
        quantity="-4",
        realizedPnl="796",
    )
    assigned_buy = _stock_eae(
        symbol="EMN",
        tradeID="SP1",
        date="20260117",
        transactionType="Buy",
        quantity="400",
        tradePrice="70",
        proceeds="-28000",
    )
    call_open = _trade(
        symbol="EMN  260320C00075000",
        underlyingSymbol="EMN",
        description="EMN 20MAR26 75 C",
        conid="7501",
        tradeID="TC1",
        transactionID="XC1",
        ibExecID="EC1",
        tradeDate="20260201",
        dateTime="20260201;154500",
        expiry="20260320",
        strike="75",
        putCall="C",
        quantity="-2",
        tradePrice="1.50",
        proceeds="300",
        ibCommission="-2",
        netCash="298",
    )
    return _report(
        {
            "Trade": [put_open, call_open],
            "OptionEAE": [put_assignment, assigned_buy],
        }
    )


def test_review_analysis_preserves_assignment_lot_and_call_cap_economics():
    payload = build_assigned_holdings_review_analysis(
        _assigned_holding_report(),
        as_of="2026-02-15",
        prices={"EMN": 90.0},
        prices_updated_at="2026-02-15T22:00:00+00:00",
    )

    assert payload["schema_version"] == "1.0"
    assert payload["recommendations_allowed"] is True
    assert payload["summary"]["holding_count"] == 1
    holding = payload["holdings"][0]
    assert holding["ticker"] == "EMN"
    assert holding["current_shares"] == pytest.approx(400.0)
    assert holding["current_price"] == pytest.approx(90.0)

    lot = holding["assignment_lots"][0]
    assert lot["assignment_date"] == "2026-01-17"
    assert lot["assignment_strike"] == pytest.approx(70.0)
    assert lot["original_shares"] == pytest.approx(400.0)
    assert lot["remaining_shares"] == pytest.approx(400.0)
    assert lot["assigned_put_premium"] == pytest.approx(796.0)
    assert lot["realized_call_cashflow"] == pytest.approx(298.0)
    assert lot["market_stock_pnl"] == pytest.approx(8000.0)
    assert lot["stock_pnl_at_open_call_strikes"] == pytest.approx(5000.0)
    assert lot["capped_upside_at_current_price"] == pytest.approx(3000.0)
    assert lot["lifecycle_pnl_excluding_dividends"] == pytest.approx(6094.0)

    call = holding["open_calls"][0]
    assert call["contracts"] == pytest.approx(2.0)
    assert call["covered_shares"] == pytest.approx(200.0)
    assert call["unallocated_shares"] == pytest.approx(0.0)
    assert call["current_option_mark"] is None
    assert call["moneyness"] == pytest.approx(0.2)
    assert call["allocations"] == [
        {
            "assignment_lot_id": lot["id"],
            "shares": 200.0,
            "assignment_strike": 70.0,
            "call_strike": 75.0,
            "stock_pnl_if_called": 1000.0,
            "capped_upside_at_current_price": 3000.0,
        }
    ]
    assert holding["totals"]["market_stock_pnl"] == pytest.approx(8000.0)
    assert holding["totals"]["stock_pnl_at_open_call_strikes"] == pytest.approx(5000.0)
    assert holding["totals"]["capped_upside_at_current_price"] == pytest.approx(3000.0)


def test_review_analysis_keeps_missing_price_holding_with_nullable_values():
    payload = build_assigned_holdings_review_analysis(
        _assigned_holding_report(),
        as_of="2026-02-15",
        prices={},
    )

    assert payload["summary"]["missing_price_tickers"] == ["EMN"]
    assert payload["recommendations_allowed"] is True
    holding = payload["holdings"][0]
    assert holding["price_status"] == "missing"
    assert holding["current_price"] is None
    assert holding["assignment_lots"][0]["market_stock_pnl"] is None
    assert holding["assignment_lots"][0]["lifecycle_pnl_excluding_dividends"] is None
    assert holding["totals"]["market_value"] is None
    assert holding["data_quality"]["status"] == "blocked"
    assert holding["data_quality"]["blockers"][0]["code"] == "current_price_missing"


def test_review_analysis_blocks_recommendations_for_unhealthy_import():
    payload = build_assigned_holdings_review_analysis(
        _assigned_holding_report(),
        as_of="2026-02-15",
        prices={"EMN": 90.0},
        import_health={
            "status": "warning",
            "issues": [{"message": "Latest successful IBKR statement is stale."}],
        },
    )

    assert payload["recommendations_allowed"] is False
    assert payload["blockers"] == [
        {
            "code": "ibkr_import_unhealthy",
            "message": "Latest successful IBKR statement is stale.",
        }
    ]


def test_review_warning_filter_matches_only_active_ticker_tokens():
    active = ["FTNT", "BRK.B"]

    assert _issue_mentions_active_ticker("Excluded FTNT call execution.", active)
    assert _issue_mentions_active_ticker("BRK.B lot needs review.", active)
    assert not _issue_mentions_active_ticker("Excluded unrelated AAPL call.", active)
    assert not _issue_mentions_active_ticker("The LEFTNT suffix is unrelated.", active)
