import subprocess

import pandas as pd
import pytest
import streamlit_app as app

from streamlit_app import (
    CONTRACT_MULTIPLIER,
    OptionLot,
    OpenLot,
    StockTxn,
    build_open_options_frame,
    build_options_cycle_chart_data,
    build_capital_timeline,
    build_dashboard_unrealized_adjusted_return_series,
    build_dashboard_unrealized_snapshot,
    build_yearly_with_dashboard_unrealized,
    build_option_trades,
    build_per_ticker_totals,
    calculate_unrealized_positions,
    filter_df_to_range,
    process_option_positions,
    resolve_build_version,
)


def _make_df(rows):
    return pd.DataFrame(rows)


def _make_capital_daily(start: str, periods: int, total: float) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="D", name="date")
    return pd.DataFrame({"total": [total] * periods}, index=idx)


def test_realized_option_close_pnl():
    df = _make_df(
        [
            {
                "trans_date": pd.Timestamp("2024-01-01"),
                "ticker": "ABC",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-02-01"),
                "strike": 10,
                "qty": 1,
                "amount": 200,
                "commission": 0.0,
                "total_pnl": 200,
                "comment": "",
            },
            {
                "trans_date": pd.Timestamp("2024-01-05"),
                "ticker": "ABC",
                "type": "Put",
                "action": "Buy",
                "expiration": pd.Timestamp("2024-02-01"),
                "strike": 10,
                "qty": 1,
                "amount": -50,
                "commission": 0.0,
                "total_pnl": -50,
                "comment": "",
            },
        ]
    )
    trades = build_option_trades(df)
    events, open_lots, stock_txns, issues, _ = process_option_positions(trades, pd.Timestamp("2024-12-31"))
    assert not open_lots
    assert not stock_txns
    assert not issues
    assert len(events) == 1
    # (2.00 - 0.50) * 1 contract * 100 = 150
    assert events[0].pnl == 150.0


def test_put_assignment_creates_stock_lot():
    df = _make_df(
        [
            {
                "trans_date": pd.Timestamp("2024-01-01"),
                "ticker": "XYZ",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-01-15"),
                "strike": 20,
                "qty": 2,
                "amount": 300,
                "commission": 0.0,
                "total_pnl": 300,
                "comment": "assigned",
            }
        ]
    )
    trades = build_option_trades(df)
    events, open_lots, stock_txns, issues, _ = process_option_positions(trades, pd.Timestamp("2024-02-01"))
    assert not open_lots
    assert not issues
    assert len(events) == 1
    # assignment uses P_close=0
    assert events[0].pnl == 300.0
    assert len(stock_txns) == 1
    txn = stock_txns[0]
    assert txn.side == "BUY"
    assert txn.shares == 2 * CONTRACT_MULTIPLIER
    assert txn.price == 20


def test_unrealized_short_put_adds_stock_component_when_below_strike():
    open_put = OptionLot(
        ticker="DEF",
        otype="Put",
        strike=15.0,
        qty=1,
        open_date=pd.Timestamp("2024-01-01"),
        expiration=pd.Timestamp("2024-03-01"),
        open_price=2.0,
        comment="",
        assigned=False,
    )
    inv_df, per_ticker_unreal, total_unreal = calculate_unrealized_positions(
        [open_put],
        [],
        {"DEF": 12.0},
    )
    # premium 200 + stock component (12-15)*100 = -300 -> -100 total
    assert round(total_unreal, 2) == -100.0
    assert round(float(per_ticker_unreal["DEF"]), 2) == -100.0
    assert inv_df.empty is False


def test_covered_call_caps_unrealized_stock_pnl():
    open_call = OptionLot(
        ticker="CCC",
        otype="Call",
        strike=100.0,
        qty=1,
        open_date=pd.Timestamp("2024-01-01"),
        expiration=pd.Timestamp("2024-04-01"),
        open_price=2.0,
        comment="",
        assigned=False,
    )
    inventory = [OpenLot(ticker="CCC", buy_date=pd.Timestamp("2024-01-01"), shares_remaining=100, cost_per_share=90.0)]
    inv_df, per_ticker_unreal, total_unreal = calculate_unrealized_positions([open_call], inventory, {"CCC": 110.0})
    # Stock capped at strike: (100-90)*100 = 1000, option premium 200 -> 1200
    assert round(total_unreal, 2) == 1200.0
    assert round(float(per_ticker_unreal["CCC"]), 2) == 1200.0
    assert inv_df.loc[inv_df["ticker"] == "CCC", "unrealized_pnl"].iloc[0] == 1000.0


def test_capital_timeline_uses_put_reserve_days():
    lot = OptionLot(
        ticker="AAA",
        otype="Put",
        strike=10.0,
        qty=1,
        open_date=pd.Timestamp("2024-01-01"),
        expiration=pd.Timestamp("2024-01-05"),
        open_price=1.0,
        comment="",
        assigned=False,
        close_date=pd.Timestamp("2024-01-03"),
    )
    cap = build_capital_timeline([lot], [], pd.Timestamp("2024-01-10"), pd.DataFrame({"trans_date": [pd.Timestamp("2024-01-01")]}), {})
    # Reserve should be present for Jan 1 and Jan 2 (exclusive of close_date)
    reserve = lot.strike * CONTRACT_MULTIPLIER
    dates = cap.index.normalize()
    assert pd.Timestamp("2024-01-01") in dates
    assert pd.Timestamp("2024-01-02") in dates
    assert cap.loc[pd.Timestamp("2024-01-01"), "puts_reserve"] == reserve
    assert cap.loc[pd.Timestamp("2024-01-02"), "puts_reserve"] == reserve


def test_partial_close_preserves_remaining_open_lot_and_reserve_window():
    df = _make_df(
        [
            {
                "trans_date": pd.Timestamp("2024-01-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-02-01"),
                "strike": 10,
                "qty": 2,
                "amount": 200,
                "commission": 0.0,
                "total_pnl": 200,
                "comment": "",
            },
            {
                "trans_date": pd.Timestamp("2024-01-10"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Buy",
                "expiration": pd.Timestamp("2024-02-01"),
                "strike": 10,
                "qty": 1,
                "amount": -50,
                "commission": 0.0,
                "total_pnl": -50,
                "comment": "",
            },
        ]
    )
    trades = build_option_trades(df)
    events, open_lots, stock_txns, issues, all_lots = process_option_positions(trades, pd.Timestamp("2024-01-15"))

    assert len(events) == 1
    assert not stock_txns
    assert not issues
    assert len(open_lots) == 1
    assert open_lots[0].qty == 1
    assert open_lots[0].close_date is None

    cap = build_capital_timeline(all_lots, stock_txns, pd.Timestamp("2024-01-15"), df, {})
    reserve = 10 * CONTRACT_MULTIPLIER
    assert cap.loc[pd.Timestamp("2024-01-09"), "puts_reserve"] == 2 * reserve
    assert cap.loc[pd.Timestamp("2024-01-10"), "puts_reserve"] == reserve
    assert cap.loc[pd.Timestamp("2024-01-14"), "puts_reserve"] == reserve


def test_build_per_ticker_totals_includes_unrealized_only_tickers():
    realized = pd.DataFrame(
        [
            {
                "year": 2024,
                "ticker": "REAL",
                "options_pnl": 100.0,
                "stock_realized_pnl": 0.0,
                "combined_realized": 100.0,
            }
        ]
    )
    unreal = pd.Series({"OPEN": 250.0, "REAL": -25.0}, dtype=float)

    totals = build_per_ticker_totals(realized, unreal)

    assert list(totals["ticker"]) == ["OPEN", "REAL"]
    open_row = totals.loc[totals["ticker"] == "OPEN"].iloc[0]
    real_row = totals.loc[totals["ticker"] == "REAL"].iloc[0]
    assert open_row["combined_realized"] == 0.0
    assert open_row["unrealized_pnl"] == 250.0
    assert open_row["total_pnl"] == 250.0
    assert real_row["combined_realized"] == 100.0
    assert real_row["unrealized_pnl"] == -25.0
    assert real_row["total_pnl"] == 75.0


def test_build_options_cycle_chart_data_uses_total_realized_pnl():
    monthly = pd.DataFrame(
        {"total_realized_pnl": [120.0, -40.0]},
        index=pd.to_datetime(["2024-01-31", "2024-02-29"]),
    )

    chart_df = build_options_cycle_chart_data(monthly)

    assert list(chart_df.columns) == ["Date", "pnl", "color"]
    assert chart_df["pnl"].tolist() == [120.0, -40.0]
    assert chart_df["color"].tolist() == ["Positive", "Negative"]


def test_resolve_build_version_prefers_env(monkeypatch):
    monkeypatch.setenv("APP_BUILD_VERSION", "deploy-123")
    monkeypatch.setenv("BUILD_VERSION", "ignored")

    assert resolve_build_version() == "deploy-123"


def test_resolve_build_version_uses_git_metadata(monkeypatch):
    monkeypatch.delenv("APP_BUILD_VERSION", raising=False)
    monkeypatch.delenv("BUILD_VERSION", raising=False)

    calls = []

    def fake_run(cmd, cwd, check, capture_output, text):
        calls.append(tuple(cmd))
        if cmd[:3] == ["git", "rev-parse", "--short=12"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="abcdef123456\n")
        if cmd[:4] == ["git", "show", "-s", "--format=%cI"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="2026-03-20T23:16:47+01:00\n")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(app.subprocess, "run", fake_run)

    assert resolve_build_version() == "git:abcdef123456 (2026-03-20T23:16:47+01:00)"
    assert ("git", "rev-parse", "--short=12", "HEAD") in calls
    assert ("git", "show", "-s", "--format=%cI", "HEAD") in calls


def test_build_open_options_frame_preserves_expected_fields():
    lots = [
        OptionLot(
            ticker="AAA",
            otype="Put",
            strike=10.0,
            qty=2,
            open_date=pd.Timestamp("2024-01-01"),
            expiration=pd.Timestamp("2024-02-01"),
            open_price=1.5,
            comment="",
            assigned=False,
        )
    ]

    df = build_open_options_frame(lots)

    assert list(df.columns) == ["ticker", "type", "strike", "qty", "expiration", "trans_date", "open_price"]
    assert df.iloc[0].to_dict() == {
        "ticker": "AAA",
        "type": "Put",
        "strike": 10.0,
        "qty": 2,
        "expiration": pd.Timestamp("2024-02-01"),
        "trans_date": pd.Timestamp("2024-01-01"),
        "open_price": 1.5,
    }


def test_filter_df_to_range_applies_ytd_window():
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-12-31", "2025-01-15", "2025-03-01"]),
            "value": [1, 2, 3],
        }
    )

    filtered = filter_df_to_range(df, "Date", pd.Timestamp("2025-03-20"), "YTD")

    assert filtered["value"].tolist() == [2, 3]


def test_dashboard_unrealized_stock_only_characterization():
    inventory = [
        OpenLot(
            ticker="AAA",
            buy_date=pd.Timestamp("2025-03-01"),
            shares_remaining=100,
            cost_per_share=90.0,
        )
    ]
    snapshot = build_dashboard_unrealized_snapshot([], inventory, {"AAA": 110.0})

    assert snapshot["total_unreal"] == pytest.approx(2000.0)
    assert snapshot["stock_unreal"] == pytest.approx(2000.0)
    assert snapshot["option_unreal"] == pytest.approx(0.0)
    assert snapshot["unrealized_blocked"] is False
    assert snapshot["inv_df"]["source"].tolist() == ["stock_lot"]
    assert float(snapshot["per_ticker_unreal"]["AAA"]) == pytest.approx(2000.0)

    monthly_returns = pd.Series([0.10], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=10_000.0)
    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        snapshot["total_unreal"],
    )
    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.30)

    yearly = pd.DataFrame({"year": [2025], "total_realized_pnl": [100.0]})
    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        True,
        snapshot["total_unreal"],
        pd.Timestamp("2025-03-15"),
    )
    assert yearly_with_unreal.loc[0, "total_pnl_incl_unreal"] == pytest.approx(2100.0)


def test_dashboard_unrealized_option_only_characterization():
    open_call = OptionLot(
        ticker="OPT",
        otype="Call",
        strike=100.0,
        qty=1,
        open_date=pd.Timestamp("2025-03-01"),
        expiration=pd.Timestamp("2025-04-18"),
        open_price=2.0,
        comment="",
        assigned=False,
    )
    snapshot = build_dashboard_unrealized_snapshot([open_call], [], {})

    assert snapshot["total_unreal"] == pytest.approx(200.0)
    assert snapshot["stock_unreal"] == pytest.approx(0.0)
    assert snapshot["option_unreal"] == pytest.approx(200.0)
    assert snapshot["inv_df"].empty
    assert float(snapshot["per_ticker_unreal"]["OPT"]) == pytest.approx(200.0)

    monthly_returns = pd.Series([0.05], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=1_000.0)
    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        snapshot["total_unreal"],
    )
    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.25)

    yearly = pd.DataFrame({"year": [2025], "total_realized_pnl": [50.0]})
    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        True,
        snapshot["total_unreal"],
        pd.Timestamp("2025-03-15"),
    )
    assert yearly_with_unreal.loc[0, "total_pnl_incl_unreal"] == pytest.approx(250.0)


def test_dashboard_unrealized_mixed_portfolio_characterization():
    inventory = [
        OpenLot(
            ticker="AAA",
            buy_date=pd.Timestamp("2025-03-01"),
            shares_remaining=100,
            cost_per_share=100.0,
        )
    ]
    open_call = OptionLot(
        ticker="BBB",
        otype="Call",
        strike=120.0,
        qty=1,
        open_date=pd.Timestamp("2025-03-01"),
        expiration=pd.Timestamp("2025-04-18"),
        open_price=1.5,
        comment="",
        assigned=False,
    )
    open_put = OptionLot(
        ticker="CCC",
        otype="Put",
        strike=50.0,
        qty=1,
        open_date=pd.Timestamp("2025-03-01"),
        expiration=pd.Timestamp("2025-04-18"),
        open_price=1.0,
        comment="",
        assigned=False,
    )
    snapshot = build_dashboard_unrealized_snapshot(
        [open_call, open_put],
        inventory,
        {"AAA": 110.0, "CCC": 45.0},
    )

    assert snapshot["total_unreal"] == pytest.approx(750.0)
    assert snapshot["stock_unreal"] == pytest.approx(500.0)
    assert snapshot["option_unreal"] == pytest.approx(250.0)
    assert float(snapshot["per_ticker_unreal"]["AAA"]) == pytest.approx(1000.0)
    assert float(snapshot["per_ticker_unreal"]["BBB"]) == pytest.approx(150.0)
    assert float(snapshot["per_ticker_unreal"]["CCC"]) == pytest.approx(-400.0)

    monthly_returns = pd.Series([0.02], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=5_000.0)
    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        snapshot["total_unreal"],
    )
    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.17)

    yearly = pd.DataFrame({"year": [2025], "total_realized_pnl": [100.0]})
    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        True,
        snapshot["total_unreal"],
        pd.Timestamp("2025-03-15"),
    )
    assert yearly_with_unreal.loc[0, "total_pnl_incl_unreal"] == pytest.approx(850.0)


def test_dashboard_unrealized_missing_prices_characterization():
    inventory = [
        OpenLot(
            ticker="AAA",
            buy_date=pd.Timestamp("2025-03-01"),
            shares_remaining=100,
            cost_per_share=90.0,
        )
    ]
    open_put = OptionLot(
        ticker="BBB",
        otype="Put",
        strike=50.0,
        qty=1,
        open_date=pd.Timestamp("2025-03-01"),
        expiration=pd.Timestamp("2025-04-18"),
        open_price=1.0,
        comment="",
        assigned=False,
    )
    open_call = OptionLot(
        ticker="CCC",
        otype="Call",
        strike=120.0,
        qty=1,
        open_date=pd.Timestamp("2025-03-01"),
        expiration=pd.Timestamp("2025-04-18"),
        open_price=1.5,
        comment="",
        assigned=False,
    )
    snapshot = build_dashboard_unrealized_snapshot([open_put, open_call], inventory, {})

    assert snapshot["total_unreal"] == pytest.approx(250.0)
    assert snapshot["stock_unreal"] == pytest.approx(0.0)
    assert snapshot["option_unreal"] == pytest.approx(250.0)
    assert snapshot["inv_df"].empty
    assert snapshot["unrealized_blocked"] is True
    assert snapshot["missing_required_price_tickers"] == ["AAA", "BBB"]
    assert float(snapshot["per_ticker_unreal"]["BBB"]) == pytest.approx(100.0)
    assert float(snapshot["per_ticker_unreal"]["CCC"]) == pytest.approx(150.0)
    assert "AAA" not in snapshot["per_ticker_unreal"].index

    monthly_returns = pd.Series([0.0], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=1_000.0)
    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        snapshot["total_unreal"],
        snapshot["unrealized_blocked"],
    )
    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.0)

    yearly = pd.DataFrame(
        {
            "year": [2024, 2025],
            "total_realized_pnl": [80.0, 50.0],
        }
    )
    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        True,
        snapshot["total_unreal"],
        pd.Timestamp("2025-03-15"),
        snapshot["unrealized_blocked"],
    )
    assert yearly_with_unreal.loc[0, "total_pnl_incl_unreal"] == pytest.approx(80.0)
    assert pd.isna(yearly_with_unreal.loc[1, "total_pnl_incl_unreal"])


def test_dashboard_unrealized_adjusted_returns_still_change_when_snapshot_complete():
    monthly_returns = pd.Series([0.0], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=1_000.0)

    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        250.0,
        False,
    )

    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.25)


def test_complete_price_unrealized_adjusted_snapshot_unchanged_by_capital_fix():
    inventory = [
        OpenLot(
            ticker="AAA",
            buy_date=pd.Timestamp("2025-03-01"),
            shares_remaining=100,
            cost_per_share=90.0,
        )
    ]
    snapshot = build_dashboard_unrealized_snapshot([], inventory, {"AAA": 110.0})

    assert snapshot["unrealized_blocked"] is False
    assert snapshot["total_unreal"] == pytest.approx(2000.0)

    monthly_returns = pd.Series([0.10], index=pd.to_datetime(["2025-03-31"]))
    capital_daily = _make_capital_daily("2025-03-01", periods=3, total=10_000.0)
    unrealized_adjusted_returns = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        pd.Timestamp("2025-03-15"),
        True,
        snapshot["total_unreal"],
        snapshot["unrealized_blocked"],
    )

    assert unrealized_adjusted_returns.loc[pd.Timestamp("2025-03-31")] == pytest.approx(0.30)


def test_capital_timeline_weekend_carries_forward_friday_close():
    txns = [
        StockTxn(
            date=pd.Timestamp("2024-05-20"),
            ticker="AAA",
            side="BUY",
            shares=100,
            price=100.0,
            source="Assigned Put",
        )
    ]
    price_history = {
        "AAA": pd.Series([110.0], index=pd.to_datetime(["2024-05-24"]))
    }

    cap = build_capital_timeline(
        [],
        txns,
        pd.Timestamp("2024-05-28"),
        pd.DataFrame({"trans_date": [pd.Timestamp("2024-05-20")]}),
        price_history,
    )

    assert cap.loc[pd.Timestamp("2024-05-24"), "shares_invested"] == pytest.approx(11_000.0)
    assert cap.loc[pd.Timestamp("2024-05-25"), "shares_invested"] == pytest.approx(11_000.0)
    assert cap.loc[pd.Timestamp("2024-05-26"), "shares_invested"] == pytest.approx(11_000.0)


def test_capital_timeline_holiday_carries_forward_prior_trading_close():
    txns = [
        StockTxn(
            date=pd.Timestamp("2024-05-20"),
            ticker="AAA",
            side="BUY",
            shares=100,
            price=100.0,
            source="Assigned Put",
        )
    ]
    price_history = {
        "AAA": pd.Series([110.0], index=pd.to_datetime(["2024-05-24"]))
    }

    cap = build_capital_timeline(
        [],
        txns,
        pd.Timestamp("2024-05-28"),
        pd.DataFrame({"trans_date": [pd.Timestamp("2024-05-20")]}),
        price_history,
    )

    assert cap.loc[pd.Timestamp("2024-05-27"), "shares_invested"] == pytest.approx(11_000.0)


def test_capital_timeline_uses_cost_basis_when_no_prior_close_exists_yet():
    txns = [
        StockTxn(
            date=pd.Timestamp("2024-05-20"),
            ticker="AAA",
            side="BUY",
            shares=100,
            price=100.0,
            source="Assigned Put",
        )
    ]
    price_history = {
        "AAA": pd.Series([110.0], index=pd.to_datetime(["2024-05-21"]))
    }

    cap = build_capital_timeline(
        [],
        txns,
        pd.Timestamp("2024-05-22"),
        pd.DataFrame({"trans_date": [pd.Timestamp("2024-05-20")]}),
        price_history,
    )

    assert cap.loc[pd.Timestamp("2024-05-20"), "shares_invested"] == pytest.approx(10_000.0)
    assert cap.loc[pd.Timestamp("2024-05-21"), "shares_invested"] == pytest.approx(11_000.0)


def test_capital_timeline_uses_same_day_close_when_available():
    txns = [
        StockTxn(
            date=pd.Timestamp("2024-05-20"),
            ticker="AAA",
            side="BUY",
            shares=100,
            price=100.0,
            source="Assigned Put",
        )
    ]
    price_history = {
        "AAA": pd.Series([112.0], index=pd.to_datetime(["2024-05-22"]))
    }

    cap = build_capital_timeline(
        [],
        txns,
        pd.Timestamp("2024-05-23"),
        pd.DataFrame({"trans_date": [pd.Timestamp("2024-05-20")]}),
        price_history,
    )

    assert cap.loc[pd.Timestamp("2024-05-22"), "shares_invested"] == pytest.approx(11_200.0)
