import subprocess

import pandas as pd
import streamlit_app as app

from streamlit_app import (
    CONTRACT_MULTIPLIER,
    OptionLot,
    OpenLot,
    build_open_options_frame,
    build_options_cycle_chart_data,
    build_capital_timeline,
    build_option_trades,
    build_per_ticker_totals,
    calculate_unrealized_positions,
    filter_df_to_range,
    process_option_positions,
    resolve_build_version,
)


def _make_df(rows):
    return pd.DataFrame(rows)


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
