import subprocess

import pandas as pd
import pytest
import streamlit_app as app

from streamlit_app import (
    CONTRACT_MULTIPLIER,
    HoldSeg,
    OptionLot,
    OpenLot,
    StockTxn,
    align_benchmarks_monthly,
    assess_capital_history_coverage,
    build_open_options_frame,
    build_options_cycle_chart_data,
    build_capital_timeline,
    build_covered_return_series,
    build_dashboard_unrealized_adjusted_return_series,
    build_dashboard_unrealized_snapshot,
    build_yearly_with_dashboard_unrealized,
    build_option_trades,
    build_per_ticker_totals,
    calculate_performance_metrics,
    calculate_unrealized_positions,
    filter_df_to_range,
    period_returns,
    process_option_positions,
    resolve_build_version,
)


def _make_df(rows):
    return pd.DataFrame(rows)


def _make_capital_daily(start: str, periods: int, total: float) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="D", name="date")
    return pd.DataFrame({"total": [total] * periods}, index=idx)


@pytest.mark.parametrize(
    "type_value,strike_value,comment,expected_type,expected_strike",
    [
        ("Put/Call", "90/110", "short put, long call", "Put", 90.0),
        ("Put/Call", "90/110", "sold put, bought call", "Put", 90.0),
        ("Put/Call", "90/110", "written put / long call", "Put", 90.0),
        ("Put/Call", "90/110", "short call, long put", "Call", 110.0),
        ("Put/Call", "90/110", "sold call and bought put", "Call", 110.0),
        ("Put/Call", "90/110", "written call / long put", "Call", 110.0),
        ("Put/Call", "90/110", "collar roll", None, None),
        ("Put/Call", "90/110", "", None, None),
        ("Call/Put", "110/90", "short call, long put", "Call", 110.0),
        ("Call/Put", "110/90", "sold call and bought put", "Call", 110.0),
        ("Call/Put", "110/90", "written call / long put", "Call", 110.0),
        ("Call/Put", "110/90", "short put, long call", "Put", 90.0),
        ("Call/Put", "110/90", "sold put, bought call", "Put", 90.0),
        ("Call/Put", "110/90", "written put / long call", "Put", 90.0),
        ("Call/Put", "110/90", "collar roll", None, None),
        ("Call/Put", "110/90", "", None, None),
    ],
)
def test_mixed_leg_option_rows_characterize_current_short_leg_inference(
    type_value,
    strike_value,
    comment,
    expected_type,
    expected_strike,
):
    inferred_type, inferred_strike = app.infer_mixed_short_leg(
        {
            "type": type_value,
            "strike": strike_value,
            "comment": comment,
        }
    )

    assert inferred_type == expected_type
    if expected_strike is None:
        assert pd.isna(inferred_strike)
    else:
        assert inferred_strike == expected_strike


@pytest.mark.parametrize("comment", ["", "collar roll", "long put hedge"])
def test_mixed_leg_option_rows_with_ambiguous_comments_are_flagged_not_traded(comment):
    issues = []
    df = _make_df(
        [
            {
                "trans_date": pd.Timestamp("2024-01-01"),
                "ticker": "MIX",
                "type": "Put/Call",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-02-16"),
                "strike": "90/110",
                "qty": 1,
                "amount": 100.0,
                "commission": 0.0,
                "total_pnl": 100.0,
                "comment": comment,
            }
        ]
    )

    trades = build_option_trades(df, issues)

    assert trades == []
    assert len(issues) == 1
    assert "Mixed-leg option row for MIX on 2024-01-01 has ambiguous short leg" in issues[0]
    assert "short put, sold put, written put, short call, sold call, written call" in issues[0]


def test_pipeline_surfaces_ambiguous_mixed_leg_parse_issue(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "MIX",
                "type": "Put/Call",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-06-21"),
                "strike": "90/110",
                "qty": 1,
                "amount": 100.0,
                "commission": 0.0,
                "total_pnl": 100.0,
                "comment": "collar roll",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: ({}, [], {"requested": len(set(tickers)), "fetched": 0}),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({}, [], {"requested": len(set(tickers)), "fetched": 0}),
    )
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-05-20").date(), False, ["Options 2024"])

    assert any("Mixed-leg option row for MIX on 2024-05-01 has ambiguous short leg" in msg for msg in state["issues"])
    assert state["lots"] == []


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


def test_capital_history_coverage_flags_missing_history_after_first_day():
    segments = [
        HoldSeg(
            ticker="AAA",
            start=pd.Timestamp("2024-05-20"),
            end=pd.Timestamp("2024-05-23"),
            shares=100,
            cost_per_share=100.0,
        )
    ]

    coverage = assess_capital_history_coverage(segments, {})

    assert coverage["capital_history_incomplete"] is True
    assert coverage["capital_history_affected_tickers"] == ["AAA"]
    issue = coverage["capital_history_coverage_issues"][0]
    assert issue["ticker"] == "AAA"
    assert issue["start_date"] == pd.Timestamp("2024-05-21")
    assert issue["end_date"] == pd.Timestamp("2024-05-22")
    assert issue["reason"] == "missing_history"


def test_capital_history_coverage_flags_partial_tail_history():
    segments = [
        HoldSeg(
            ticker="AAA",
            start=pd.Timestamp("2024-05-20"),
            end=pd.Timestamp("2024-05-29"),
            shares=100,
            cost_per_share=100.0,
        )
    ]
    price_history = {
        "AAA": pd.Series(
            [101.0, 102.0],
            index=pd.to_datetime(["2024-05-21", "2024-05-22"]),
        )
    }

    coverage = assess_capital_history_coverage(segments, price_history)

    assert coverage["capital_history_incomplete"] is True
    assert coverage["capital_history_affected_tickers"] == ["AAA"]
    issue = coverage["capital_history_coverage_issues"][0]
    assert issue["reason"] == "stale_tail"
    assert issue["start_date"] == pd.Timestamp("2024-05-23")
    assert issue["end_date"] == pd.Timestamp("2024-05-28")


def test_capital_history_coverage_allows_legitimate_initial_cost_basis_fallback():
    segments = [
        HoldSeg(
            ticker="AAA",
            start=pd.Timestamp("2024-05-20"),
            end=pd.Timestamp("2024-05-22"),
            shares=100,
            cost_per_share=100.0,
        )
    ]
    price_history = {
        "AAA": pd.Series(
            [110.0],
            index=pd.to_datetime(["2024-05-21"]),
        )
    }

    coverage = assess_capital_history_coverage(segments, price_history)

    assert coverage["capital_history_incomplete"] is False
    assert coverage["capital_history_coverage_issues"] == []


def test_pipeline_suppresses_denominator_returns_when_historical_price_fetch_fails(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-10"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {},
            ["Historical price download failed: boom"],
            {"requested": len(set(tickers)), "fetched": 0},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 110.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-05-20").date(), False, ["Options 2024"])

    assert state["capital_history_incomplete"] is True
    assert state["capital_history_affected_tickers"] == ["AAA"]
    assert state["historical_price_summary"] == {"requested": 1, "fetched": 0}
    assert any("Historical price download failed: boom" in msg for msg in state["issues"])
    assert any("AAA" in msg for msg in state["issues"])
    assert state["monthly_cycles"]["roac"].isna().all()
    assert state["monthly_cycles"]["ropc"].isna().all()
    yearly_row = state["yearly"].loc[state["yearly"]["year"] == 2024].iloc[0]
    assert pd.isna(yearly_row["ann_roac"])
    assert pd.isna(yearly_row["annualized_return_twr"])
    assert pd.isna(yearly_row["annualized_return_twr_active"])


def test_pipeline_denominator_returns_remain_when_historical_price_history_is_complete(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-10"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {
                "AAA": pd.Series(
                    [101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
                    index=pd.to_datetime(
                        [
                            "2024-05-13",
                            "2024-05-14",
                            "2024-05-15",
                            "2024-05-16",
                            "2024-05-17",
                            "2024-05-20",
                        ]
                    ),
                )
            },
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 110.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-05-20").date(), False, ["Options 2024"])

    assert state["capital_history_incomplete"] is False
    assert state["capital_history_coverage_issues"] == []
    assert state["monthly_cycles"]["roac"].notna().all()
    assert state["monthly_cycles"]["ropc"].notna().all()
    yearly_row = state["yearly"].loc[state["yearly"]["year"] == 2024].iloc[0]
    assert pd.notna(yearly_row["ann_roac"])


def test_build_covered_return_series_truncates_at_first_incomplete_month():
    monthly_returns = pd.Series(
        [0.05, 0.04, 0.03],
        index=pd.to_datetime(["2024-03-31", "2024-04-30", "2024-05-31"]),
    )

    covered = build_covered_return_series(
        monthly_returns,
        [pd.Timestamp("2024-05-31")],
    )

    assert covered["first_incomplete_month"] == pd.Timestamp("2024-05-31")
    assert covered["last_complete_month"] == pd.Timestamp("2024-04-30")
    assert covered["truncated"] is True
    assert list(covered["covered_returns"].index) == [pd.Timestamp("2024-03-31"), pd.Timestamp("2024-04-30")]


def test_align_benchmarks_monthly_complete_history_remains_unchanged(monkeypatch):
    class FakeYF:
        def download(self, *args, **kwargs):
            return pd.DataFrame(
                {"Close": [100.0, 110.0, 121.0, 133.1]},
                index=pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"]),
            )

    monkeypatch.setattr(app, "yf", FakeYF())
    idx = pd.to_datetime(["2024-02-29", "2024-03-31", "2024-04-30"])

    aligned = align_benchmarks_monthly({"Bench": "BENCH"}, idx)

    assert list(aligned["Bench"].index) == list(idx)
    assert aligned["Bench"].tolist() == pytest.approx([0.10, 0.10, 0.10])


def test_align_benchmarks_monthly_internal_missing_month_is_not_forward_filled(monkeypatch):
    class FakeYF:
        def download(self, *args, **kwargs):
            return pd.DataFrame(
                {"Close": [100.0, 110.0, 133.1]},
                index=pd.to_datetime(["2024-01-31", "2024-02-29", "2024-04-30"]),
            )

    monkeypatch.setattr(app, "yf", FakeYF())
    idx = pd.to_datetime(["2024-02-29", "2024-03-31", "2024-04-30"])

    aligned = align_benchmarks_monthly({"Bench": "BENCH"}, idx)["Bench"]

    assert aligned.loc[pd.Timestamp("2024-02-29")] == pytest.approx(0.10)
    assert pd.isna(aligned.loc[pd.Timestamp("2024-03-31")])
    assert pd.isna(aligned.loc[pd.Timestamp("2024-04-30")])


def test_benchmark_metrics_do_not_compound_or_trail_through_missing_months():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
    gappy_benchmark = pd.Series([0.01, float("nan"), 0.03], index=idx)

    metrics = calculate_performance_metrics(gappy_benchmark)
    observed_metrics = calculate_performance_metrics(gappy_benchmark.dropna())
    ffilled_metrics = calculate_performance_metrics(gappy_benchmark.ffill())
    periods = period_returns(gappy_benchmark)

    assert metrics["CAGR"] == pytest.approx(observed_metrics["CAGR"])
    assert metrics["Sharpe"] == pytest.approx(observed_metrics["Sharpe"])
    assert metrics["CAGR"] != pytest.approx(ffilled_metrics["CAGR"])
    assert metrics["Sharpe"] != pytest.approx(ffilled_metrics["Sharpe"])
    assert pd.isna(periods["Return 3M"])
    assert pd.isna(periods["Return YTD"])
    assert pd.isna(periods["Return SI"])


def test_valid_strategy_returns_are_unaffected_by_gap_handling():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
    strategy_returns = pd.Series([0.01, 0.02, 0.03], index=idx)

    metrics = calculate_performance_metrics(strategy_returns)
    periods = period_returns(strategy_returns)

    assert metrics["CAGR"] == pytest.approx((1.01 * 1.02 * 1.03) ** 4 - 1)
    assert periods["Return 3M"] == pytest.approx((1.01 * 1.02 * 1.03) - 1)
    assert periods["Return YTD"] == pytest.approx((1.01 * 1.02 * 1.03) - 1)
    assert periods["Return SI"] == pytest.approx((1.01 * 1.02 * 1.03) - 1)


def test_pipeline_truncates_return_series_and_benchmark_metrics_to_last_complete_month(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-20"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {
                "AAA": pd.Series(
                    [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
                    index=pd.to_datetime(
                        [
                            "2024-05-21",
                            "2024-05-22",
                            "2024-05-23",
                            "2024-05-24",
                            "2024-05-27",
                            "2024-05-28",
                            "2024-05-29",
                            "2024-05-30",
                            "2024-05-31",
                        ]
                    ),
                )
            },
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 110.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(
        app,
        "align_benchmarks_monthly",
        lambda tickers, idx: {"Bench": pd.Series([0.02] * len(idx), index=idx)} if len(idx) else {},
    )

    state = app.build_pipeline(pd.Timestamp("2024-06-20").date(), False, ["Options 2024"])

    may = pd.Timestamp("2024-05-31")
    june = pd.Timestamp("2024-06-30")

    assert state["capital_history_incomplete"] is True
    assert state["first_incomplete_return_month"] == june
    assert state["last_complete_return_month"] == may
    assert state["monthly_cycles"].loc[may, "roac"] == pytest.approx(state["monthly_returns_covered"].iloc[0])
    assert pd.isna(state["monthly_cycles"].loc[june, "roac"])
    assert pd.isna(state["monthly_cycles"].loc[june, "ropc"])
    assert list(state["monthly_returns_covered"].index) == [may]
    assert list(state["aligned_bench_returns"]["Bench"].index) == [may]

    strategy_row = state["benchmark_metrics"].loc[state["benchmark_metrics"]["Series"] == "My Strategy"].iloc[0]
    assert strategy_row["Return YTD"] == pytest.approx(state["monthly_returns_covered"].iloc[0])
    assert pd.isna(strategy_row["Return 3M"])
    assert pd.isna(strategy_row["Return 6M"])
    assert pd.isna(strategy_row["Return 1Y"])


def test_pipeline_dividend_incomplete_keeps_returns_visible(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-10"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {
                "AAA": pd.Series(
                    [101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
                    index=pd.to_datetime(
                        [
                            "2024-05-13",
                            "2024-05-14",
                            "2024-05-15",
                            "2024-05-16",
                            "2024-05-17",
                            "2024-05-20",
                        ]
                    ),
                )
            },
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 110.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "collect_dividend_cashflows",
        lambda stock_txns, as_of: app.DividendFetchResult(
            cashflows=pd.DataFrame(columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]),
            coverage_complete=False,
            attempted_tickers=["AAA"],
            failed_tickers=["AAA"],
            errors=["AAA: dividend fetch failed"],
        ),
    )
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-05-20").date(), False, ["Options 2024"])

    assert state["dividend_coverage_complete"] is False
    assert state["dividend_affected_tickers"] == ["AAA"]
    assert state["dividend_errors"] == ["AAA: dividend fetch failed"]
    assert any("Dividend data incomplete for AAA" in msg for msg in state["issues"])
    assert state["monthly_cycles"]["roac"].notna().all()
    assert state["monthly_cycles"]["ropc"].notna().all()
    yearly_row = state["yearly"].loc[state["yearly"]["year"] == 2024].iloc[0]
    assert pd.notna(yearly_row["ann_roac"])


def test_pipeline_valid_zero_dividend_history_does_not_create_issue(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-10"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {
                "AAA": pd.Series(
                    [101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
                    index=pd.to_datetime(
                        [
                            "2024-05-13",
                            "2024-05-14",
                            "2024-05-15",
                            "2024-05-16",
                            "2024-05-17",
                            "2024-05-20",
                        ]
                    ),
                )
            },
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 110.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "collect_dividend_cashflows",
        lambda stock_txns, as_of: app.DividendFetchResult(
            cashflows=pd.DataFrame(columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]),
            coverage_complete=True,
            attempted_tickers=["AAA"],
            failed_tickers=[],
            errors=[],
        ),
    )
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-05-20").date(), False, ["Options 2024"])

    assert state["dividend_coverage_complete"] is True
    assert state["dividend_affected_tickers"] == ["AAA"]
    assert state["dividend_errors"] == []
    assert not any("Dividend data incomplete" in msg for msg in state["issues"])
    assert state["monthly_cycles"]["roac"].notna().all()
