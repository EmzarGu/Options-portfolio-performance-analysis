import io
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
from data_sources import (
    align_benchmarks_monthly as data_source_align_benchmarks_monthly,
    download_excel_workbook,
    load_options_from_excel_bytes,
    option_sheet_names_from_excel_bytes,
)
import streamlit_app as app
from portfolio_backend.api_payloads import PORTFOLIO_PAYLOAD_VERSION, build_portfolio_payload
from portfolio_backend.api_service import (
    PortfolioPayloadRequest,
    PortfolioServiceDependencies,
    build_payload_for_request,
)
from portfolio_backend.calculations import (
    assess_capital_history_coverage,
    build_capital_timeline,
    build_option_trades,
    process_option_positions,
    resolve_capital_price_on_day,
    resolve_capital_prices_for_days,
)
from portfolio_backend.charts import build_benchmark_growth_chart_data, build_options_cycle_chart_data
from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.models import HoldSeg, OpenLot, OptionLot, PipelineState, StockTxn
from portfolio_backend.performance import (
    build_covered_return_series,
    build_dashboard_unrealized_adjusted_return_series,
    build_dashboard_unrealized_snapshot,
    build_per_ticker_totals,
    build_yearly_with_dashboard_unrealized,
    calculate_performance_metrics,
    calculate_unrealized_positions,
    period_returns,
)
from portfolio_backend.serializers import serialize_portfolio_state, serialize_snapshot
from portfolio_backend.tables import build_open_options_frame, filter_df_to_range
from portfolio_backend.view_models import build_dashboard_view_model

resolve_build_version = app.resolve_build_version


def _make_df(rows):
    return pd.DataFrame(rows)


def _make_capital_daily(start: str, periods: int, total: float) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="D", name="date")
    return pd.DataFrame({"total": [total] * periods}, index=idx)


def test_load_options_from_excel_bytes_normalizes_option_sheets():
    raw_2026 = pd.DataFrame(
        [
            {
                "Trans date": "25/08/2025",
                "Tiker": " abc ",
                "Type": "put",
                "Action": "Bought",
                "Expiration": "19/09/2025",
                "Strike": "100.5",
                "Qty": "2",
                "Amount": "300.0",
                "Comission": "1.5",
                "Total P&L": "298.5",
                "Assigned": "1",
                "Comment": " assigned ",
            },
            {
                "Trans date": "2025-08-26",
                "Tiker": "skip",
                "Type": "stock",
                "Action": "Dividend",
                "Expiration": "2025-09-19",
                "Strike": "0",
                "Qty": "0",
                "Amount": "10.0",
                "Comission": "0",
                "Total P&L": "10.0",
                "Assigned": "",
                "Comment": "",
            },
        ]
    )
    raw_2025 = pd.DataFrame(
        [
            {
                "Trans date": "2025-01-02",
                "Tiker": "xyz",
                "Type": "Call",
                "Action": "Sell",
                "Expiration": "2025-02-21",
                "Strike": "50",
                "Qty": "1",
                "Amount": "100",
                "Comission": "0",
                "Total P&L": "100",
                "Assigned": "0",
                "Comment": "",
            }
        ]
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        raw_2026.to_excel(writer, sheet_name="Options 2026", index=False, startrow=1)
        raw_2025.to_excel(writer, sheet_name="Options 2025", index=False, startrow=1)
        pd.DataFrame({"A": [1]}).to_excel(writer, sheet_name="Notes", index=False)

    excel_bytes = buffer.getvalue()

    assert option_sheet_names_from_excel_bytes(excel_bytes) == ["Options 2025", "Options 2026"]
    loaded = load_options_from_excel_bytes(excel_bytes, ["Options 2026", "Options 2025"])

    assert loaded["source_sheet"].tolist() == ["Options 2026", "Options 2025"]
    first = loaded.iloc[0]
    assert first["ticker"] == "ABC"
    assert first["action"] == "Buy"
    assert first["type"] == "Put"
    assert first["strike"] == pytest.approx(100.5)
    assert first["assigned_flag"] == pytest.approx(1.0)
    assert first["trans_date"] == pd.Timestamp("2025-08-25")
    assert first["expiration"] == pd.Timestamp("2025-09-19")


def test_download_excel_workbook_uses_local_override(tmp_path):
    workbook = tmp_path / "portfolio.xlsx"
    workbook.write_bytes(b"PK-local-workbook")

    download = download_excel_workbook("ignored", local_excel_path=str(workbook))

    assert download.content == b"PK-local-workbook"
    assert download.source == "local"
    assert download.file_name == "portfolio.xlsx"
    assert download.file_modified_at is not None
    assert download.downloaded_at is not None


def test_download_excel_workbook_public_download(monkeypatch):
    class FakeResponse:
        content = b"PK-public-workbook"

        def raise_for_status(self):
            return None

    calls = []

    def fake_get(url, timeout):
        calls.append((url, timeout))
        return FakeResponse()

    monkeypatch.setattr("data_sources.requests.get", fake_get)

    download = download_excel_workbook("sheet-123")

    assert calls == [("https://docs.google.com/spreadsheets/d/sheet-123/export?format=xlsx", 15)]
    assert download.content == b"PK-public-workbook"
    assert download.source == "public"
    assert download.downloaded_at is not None


def test_backend_accounting_imports_without_streamlit():
    code = """
import sys
import data_sources
import portfolio_backend.charts
import portfolio_backend.calculations
import portfolio_backend.models
import portfolio_backend.performance
import portfolio_backend.pipeline
import portfolio_backend.api_payloads
import portfolio_backend.api_service
import portfolio_backend.serializers
import portfolio_backend.tables
import portfolio_backend.view_models
assert "streamlit" not in sys.modules
assert "streamlit_app" not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True)


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


def test_pipeline_reference_summary_for_refactor_guardrail(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-01-02"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-01-19"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 1.0,
                "comment": "assigned",
                "source_sheet": "Options 2024",
            },
            {
                "trans_date": pd.Timestamp("2024-02-01"),
                "ticker": "BBB",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2026-12-18"),
                "strike": 50.0,
                "qty": 2,
                "amount": 300.0,
                "commission": 0.0,
                "total_pnl": 300.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2026",
            },
            {
                "trans_date": pd.Timestamp("2026-03-02"),
                "ticker": "CCC",
                "type": "Call",
                "action": "Sell",
                "expiration": pd.Timestamp("2026-06-19"),
                "strike": 80.0,
                "qty": 1,
                "amount": 400.0,
                "commission": 0.0,
                "total_pnl": 400.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2026",
            },
            {
                "trans_date": pd.Timestamp("2026-04-01"),
                "ticker": "CCC",
                "type": "Call",
                "action": "Buy",
                "expiration": pd.Timestamp("2026-06-19"),
                "strike": 80.0,
                "qty": 1,
                "amount": -100.0,
                "commission": 0.0,
                "total_pnl": -100.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2026",
            },
        ]
    )
    price_index = pd.bdate_range("2024-01-19", "2026-06-30")
    price_history = {"AAA": pd.Series([100.0] * len(price_index), index=price_index)}
    dividends = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "ex_date": pd.Timestamp("2024-03-15"),
                "pay_date": pd.Timestamp("2024-03-15"),
                "per_share": 0.5,
                "shares": 100,
                "cash": 50.0,
            }
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {ticker: price_history[ticker] for ticker in set(tickers) if ticker in price_history},
            [],
            {"requested": len(set(tickers)), "fetched": len({ticker for ticker in set(tickers) if ticker in price_history})},
        ),
    )
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: (
            {"AAA": 120.0, "BBB": 45.0},
            [],
            {"requested": len(set(tickers)), "fetched": len(set(tickers))},
        ),
    )
    monkeypatch.setattr(
        app,
        "collect_dividend_cashflows",
        lambda stock_txns, as_of: app.DividendFetchResult(
            cashflows=dividends.copy(),
            coverage_complete=True,
            attempted_tickers=["AAA"],
            failed_tickers=[],
            errors=[],
        ),
    )
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})
    app.get_cached_current_prices.clear()

    state = app.build_pipeline(
        pd.Timestamp("2026-06-30").date(),
        True,
        ["Options 2024", "Options 2025", "Options 2026"],
        price_refresh_token=("reference-summary", 0),
    )

    assert state["issues"] == []
    assert state["price_summary"] == {"stocks_requested": 2, "stocks_fetched": 2}
    assert len(state["open_options"]) == 1
    assert len(state["inv_df"]) == 2
    assert state["cumulative_realized"] == pytest.approx(550.0)
    assert state["total_unreal"] == pytest.approx(1300.0)
    assert state["stock_unreal"] == pytest.approx(1000.0)
    assert state["option_unreal"] == pytest.approx(300.0)
    assert state["grand_total"] == pytest.approx(1850.0)

    ytd_row = state["yearly_with_unreal"].loc[state["yearly_with_unreal"]["year"] == 2026].iloc[0]
    assert ytd_row["total_realized_pnl"] == pytest.approx(300.0)
    assert ytd_row["total_pnl_incl_unreal"] == pytest.approx(1600.0)

    per_ticker = state["per_ticker_totals"].set_index("ticker")
    assert per_ticker.loc["AAA", "total_pnl"] == pytest.approx(2200.0)
    assert per_ticker.loc["BBB", "total_pnl"] == pytest.approx(-700.0)
    assert per_ticker.loc["CCC", "total_pnl"] == pytest.approx(300.0)


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


@pytest.mark.parametrize(
    "range_choice,metric_name,expected_periods",
    [
        ("3M", "Return 3M", 3),
        ("6M", "Return 6M", 6),
        ("YTD", "Return YTD", 2),
        ("1Y", "Return 1Y", 12),
        ("Since inception", "Return SI", 15),
    ],
)
def test_benchmark_growth_chart_reconciles_to_table_returns_for_all_ranges(range_choice, metric_name, expected_periods):
    idx = pd.date_range("2024-12-31", periods=15, freq="ME")
    strategy_returns = pd.Series([0.035, -0.01, 0.022, 0.015, 0.018, -0.005, 0.012, 0.011, 0.02, -0.004, 0.016, 0.009, 0.10, 0.008, 0.008], index=idx)
    bench_returns = pd.Series([0.012, 0.018, -0.006, 0.011, 0.009, 0.014, -0.004, 0.008, 0.010, 0.006, 0.013, -0.002, 0.000, 0.010, 0.010], index=idx)

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": bench_returns}, range_choice)

    strategy_chart = chart_df[chart_df["Series"] == "My Strategy"]
    bench_chart = chart_df[chart_df["Series"] == "Bench"]
    strategy_table_return = period_returns(strategy_returns)[metric_name]
    bench_table_return = period_returns(bench_returns)[metric_name]

    assert len(strategy_chart) == expected_periods + 1
    assert len(bench_chart) == expected_periods + 1
    assert strategy_chart["Date"].tolist() == bench_chart["Date"].tolist()
    assert strategy_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert bench_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert strategy_chart["Growth"].iloc[-1] == pytest.approx(1 + strategy_table_return)
    assert bench_chart["Growth"].iloc[-1] == pytest.approx(1 + bench_table_return)


@pytest.mark.parametrize(
    "range_choice,metric_name,missing_position",
    [
        ("3M", "Return 3M", -2),
        ("6M", "Return 6M", -4),
        ("YTD", "Return YTD", -1),
        ("1Y", "Return 1Y", -8),
        ("Since inception", "Return SI", 3),
    ],
)
def test_benchmark_growth_chart_omits_incomplete_benchmark_windows_for_all_ranges(range_choice, metric_name, missing_position):
    idx = pd.date_range("2024-12-31", periods=15, freq="ME")
    strategy_returns = pd.Series([0.01] * 15, index=idx)
    bench_returns = pd.Series([0.01] * 15, index=idx)
    bench_returns.iloc[missing_position] = float("nan")

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": bench_returns}, range_choice)

    assert pd.isna(period_returns(bench_returns)[metric_name])
    assert chart_df[chart_df["Series"] == "Bench"].empty
    assert not chart_df[chart_df["Series"] == "My Strategy"].empty


def test_benchmark_growth_chart_clips_strategy_window_to_table_as_of():
    idx = pd.date_range("2025-04-30", periods=13, freq="ME")
    strategy_returns = pd.Series([0.10] + [0.008] * 11 + [-0.004], index=idx)
    bench_returns = pd.Series([0.00] + [0.01] * 11, index=idx[:-1])
    as_of = pd.Timestamp("2026-04-27")

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": bench_returns}, "1Y", as_of)

    strategy_chart = chart_df[chart_df["Series"] == "My Strategy"]
    bench_chart = chart_df[chart_df["Series"] == "Bench"]
    table_strategy_returns = strategy_returns[strategy_returns.index <= as_of.normalize()]
    table_benchmark_returns = bench_returns[bench_returns.index <= as_of.normalize()]

    assert strategy_chart["Date"].iloc[0] == pd.Timestamp("2025-03-31")
    assert strategy_chart["Date"].iloc[1] == pd.Timestamp("2025-04-30")
    assert strategy_chart["Date"].iloc[-1] == pd.Timestamp("2026-03-31")
    assert strategy_chart["Date"].tolist() == bench_chart["Date"].tolist()
    assert strategy_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert bench_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert strategy_chart["Growth"].iloc[-1] == pytest.approx(1 + period_returns(table_strategy_returns)["Return 1Y"])
    assert bench_chart["Growth"].iloc[-1] == pytest.approx(1 + period_returns(table_benchmark_returns)["Return 1Y"])


def test_benchmark_growth_chart_order_matches_table_returns_across_ranges():
    idx = pd.date_range("2024-12-31", periods=15, freq="ME")
    strategy_returns = pd.Series([0.03] * 12 + [0.10, 0.02, 0.02], index=idx)
    schd_returns = pd.Series([0.01] * 15, index=idx)

    for range_choice, metric_name in [
        ("3M", "Return 3M"),
        ("6M", "Return 6M"),
        ("YTD", "Return YTD"),
        ("1Y", "Return 1Y"),
        ("Since inception", "Return SI"),
    ]:
        chart_df = build_benchmark_growth_chart_data(strategy_returns, {"SCHD ETF": schd_returns}, range_choice)
        strategy_table_return = period_returns(strategy_returns)[metric_name]
        schd_table_return = period_returns(schd_returns)[metric_name]
        strategy_endpoint = chart_df.loc[chart_df["Series"] == "My Strategy", "Growth"].iloc[-1]
        schd_endpoint = chart_df.loc[chart_df["Series"] == "SCHD ETF", "Growth"].iloc[-1]
        strategy_start = chart_df.loc[chart_df["Series"] == "My Strategy", "Growth"].iloc[0]
        schd_start = chart_df.loc[chart_df["Series"] == "SCHD ETF", "Growth"].iloc[0]

        assert strategy_table_return > schd_table_return
        assert strategy_start == pytest.approx(1.0)
        assert schd_start == pytest.approx(1.0)
        assert strategy_endpoint == pytest.approx(1 + strategy_table_return)
        assert schd_endpoint == pytest.approx(1 + schd_table_return)
        assert strategy_endpoint > schd_endpoint


@pytest.mark.parametrize(
    "range_choice,metric_name",
    [
        ("3M", "Return 3M"),
        ("6M", "Return 6M"),
        ("YTD", "Return YTD"),
        ("1Y", "Return 1Y"),
        ("Since inception", "Return SI"),
    ],
)
def test_e2e_chart_table_reconciliation_for_strategy_and_benchmark_all_ranges(range_choice, metric_name):
    idx = pd.date_range("2024-12-31", periods=15, freq="ME")
    strategy_returns = pd.Series(
        [0.025, -0.006, 0.018, 0.011, 0.013, -0.004, 0.016, 0.007, 0.019, -0.003, 0.012, 0.010, 0.040, 0.014, 0.009],
        index=idx,
    )
    benchmark_returns = pd.Series(
        [0.010, 0.012, -0.004, 0.008, 0.011, 0.006, -0.002, 0.007, 0.009, 0.004, 0.010, 0.005, 0.015, 0.008, 0.006],
        index=idx,
    )

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": benchmark_returns}, range_choice)
    strategy_chart = chart_df.loc[chart_df["Series"] == "My Strategy"].reset_index(drop=True)
    benchmark_chart = chart_df.loc[chart_df["Series"] == "Bench"].reset_index(drop=True)
    strategy_window = app._select_chart_return_window(strategy_returns, range_choice)
    benchmark_window = app._select_chart_return_window(benchmark_returns, range_choice)
    strategy_table_return = period_returns(strategy_returns)[metric_name]
    benchmark_table_return = period_returns(benchmark_returns)[metric_name]

    assert strategy_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert benchmark_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert strategy_chart["Growth"].iloc[-1] == pytest.approx(1 + strategy_table_return)
    assert benchmark_chart["Growth"].iloc[-1] == pytest.approx(1 + benchmark_table_return)
    assert strategy_chart["Growth"].iloc[-1] / strategy_chart["Growth"].iloc[0] - 1 == pytest.approx(strategy_table_return)
    assert benchmark_chart["Growth"].iloc[-1] / benchmark_chart["Growth"].iloc[0] - 1 == pytest.approx(benchmark_table_return)
    assert strategy_chart["Date"].iloc[1:].tolist() == strategy_window.index.tolist()
    assert benchmark_chart["Date"].iloc[1:].tolist() == benchmark_window.index.tolist()
    assert strategy_chart["Date"].tolist() == benchmark_chart["Date"].tolist()


def test_e2e_benchmark_gap_omits_line_without_forward_fill():
    idx = pd.date_range("2025-01-31", periods=6, freq="ME")
    strategy_returns = pd.Series([0.01, 0.02, 0.015, -0.005, 0.012, 0.011], index=idx)
    benchmark_returns = pd.Series([0.008, 0.009, float("nan"), 0.010, 0.011, 0.012], index=idx)

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": benchmark_returns}, "6M")

    assert pd.isna(period_returns(benchmark_returns)["Return 6M"])
    assert chart_df.loc[chart_df["Series"] == "Bench"].empty
    assert chart_df.loc[chart_df["Series"] == "My Strategy", "Growth"].iloc[0] == pytest.approx(1.0)
    assert chart_df.loc[chart_df["Series"] == "My Strategy", "Growth"].iloc[-1] == pytest.approx(
        1 + period_returns(strategy_returns)["Return 6M"]
    )
    assert benchmark_returns.ffill().iloc[2] == pytest.approx(benchmark_returns.iloc[1])


@pytest.mark.parametrize(
    "range_choice,metric_name",
    [
        ("3M", "Return 3M"),
        ("6M", "Return 6M"),
        ("YTD", "Return YTD"),
        ("1Y", "Return 1Y"),
        ("Since inception", "Return SI"),
    ],
)
def test_e2e_partial_month_as_of_excludes_future_return_periods_and_reconciles(range_choice, metric_name):
    idx = pd.date_range("2025-01-31", periods=16, freq="ME")
    strategy_returns = pd.Series([0.012, 0.008, -0.006, 0.015, 0.011, 0.009, -0.004, 0.013, 0.010, 0.007, 0.014, 0.006, 0.020, 0.011, 0.009, 0.050], index=idx)
    benchmark_returns = pd.Series([0.007, 0.006, -0.003, 0.010, 0.008, 0.006, -0.002, 0.009, 0.007, 0.005, 0.010, 0.004, 0.012, 0.008, 0.006, 0.040], index=idx)
    as_of = pd.Timestamp("2026-04-27")

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": benchmark_returns}, range_choice, as_of)
    strategy_chart = chart_df.loc[chart_df["Series"] == "My Strategy"].reset_index(drop=True)
    benchmark_chart = chart_df.loc[chart_df["Series"] == "Bench"].reset_index(drop=True)
    clipped_strategy = strategy_returns[strategy_returns.index <= as_of.normalize()]
    clipped_benchmark = benchmark_returns[benchmark_returns.index <= as_of.normalize()]
    strategy_window = app._select_chart_return_window(strategy_returns, range_choice, as_of)
    benchmark_window = app._select_chart_return_window(benchmark_returns, range_choice, as_of)

    assert pd.Timestamp("2026-04-30") not in strategy_chart["Date"].tolist()
    assert pd.Timestamp("2026-04-30") not in benchmark_chart["Date"].tolist()
    assert all(pd.to_datetime(strategy_chart["Date"].iloc[1:]) <= as_of.normalize())
    assert all(pd.to_datetime(benchmark_chart["Date"].iloc[1:]) <= as_of.normalize())
    assert strategy_chart["Date"].iloc[1:].tolist() == strategy_window.index.tolist()
    assert benchmark_chart["Date"].iloc[1:].tolist() == benchmark_window.index.tolist()
    assert strategy_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert benchmark_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert strategy_chart["Growth"].iloc[-1] == pytest.approx(1 + period_returns(clipped_strategy)[metric_name])
    assert benchmark_chart["Growth"].iloc[-1] == pytest.approx(1 + period_returns(clipped_benchmark)[metric_name])


@pytest.mark.parametrize(
    "range_choice,metric_name",
    [
        ("3M", "Return 3M"),
        ("6M", "Return 6M"),
        ("YTD", "Return YTD"),
        ("1Y", "Return 1Y"),
        ("Since inception", "Return SI"),
    ],
)
def test_e2e_chart_endpoint_ranking_matches_table_ranking_all_ranges(range_choice, metric_name):
    idx = pd.date_range("2024-12-31", periods=15, freq="ME")
    strategy_returns = pd.Series([0.02] * 12 + [0.04, 0.03, 0.03], index=idx)
    benchmark_returns = pd.Series([0.01] * 15, index=idx)

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": benchmark_returns}, range_choice)
    strategy_table_return = period_returns(strategy_returns)[metric_name]
    benchmark_table_return = period_returns(benchmark_returns)[metric_name]
    strategy_endpoint = chart_df.loc[chart_df["Series"] == "My Strategy", "Growth"].iloc[-1]
    benchmark_endpoint = chart_df.loc[chart_df["Series"] == "Bench", "Growth"].iloc[-1]

    assert strategy_table_return > benchmark_table_return
    assert strategy_endpoint > benchmark_endpoint


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


def test_backend_open_option_shorts_frame_adds_price_and_moneyness():
    from portfolio_backend.tables import build_open_option_shorts_frame

    open_options = pd.DataFrame(
        [
            {"ticker": "PUTT", "type": "Put", "strike": 100.0, "qty": 1},
            {"ticker": "CALL", "type": "Call", "strike": 50.0, "qty": 1},
            {"ticker": "MISS", "type": "Put", "strike": 25.0, "qty": 1},
        ]
    )

    enriched = build_open_option_shorts_frame(open_options, {"PUTT": 80.0, "CALL": 60.0})

    assert enriched.loc[enriched["ticker"] == "PUTT", "current_price"].iloc[0] == pytest.approx(80.0)
    assert enriched.loc[enriched["ticker"] == "PUTT", "moneyness_pct"].iloc[0] == pytest.approx(0.20)
    assert enriched.loc[enriched["ticker"] == "CALL", "moneyness_pct"].iloc[0] == pytest.approx(0.20)
    assert pd.isna(enriched.loc[enriched["ticker"] == "MISS", "moneyness_pct"].iloc[0])


def test_snapshot_yearly_and_monthly_realized_pnl_reconcile_as_of_current_month(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-03-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-03-20"),
                "strike": 100.0,
                "qty": 1,
                "amount": 100.0,
                "commission": 0.0,
                "total_pnl": 100.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2024",
            },
            {
                "trans_date": pd.Timestamp("2024-04-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-04-20"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2024",
            },
            {
                "trans_date": pd.Timestamp("2024-05-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-05-17"),
                "strike": 100.0,
                "qty": 1,
                "amount": 999.0,
                "commission": 0.0,
                "total_pnl": 999.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2024",
            },
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(app, "fetch_price_history_yf", lambda tickers, start, end: ({}, [], {"requested": 0, "fetched": 0}))
    monkeypatch.setattr(app, "fetch_current_prices_yf", lambda tickers: ({}, [], {"requested": 0, "fetched": 0}))
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-04-27").date(), False, ["Options 2024"])

    yearly_row = state["yearly"].loc[state["yearly"]["year"] == 2024].iloc[0]
    monthly_index = pd.to_datetime(state["monthly_cycles"].index)
    monthly_ytd_realized = state["monthly_cycles"].loc[
        monthly_index.year == 2024,
        "total_realized_pnl",
    ].sum()

    assert state["monthly_cycles"].index.tolist() == [pd.Timestamp("2024-03-31"), pd.Timestamp("2024-04-30")]
    assert monthly_ytd_realized == pytest.approx(300.0)
    assert yearly_row["total_realized_pnl"] == pytest.approx(monthly_ytd_realized)
    assert state["cumulative_realized"] == pytest.approx(monthly_ytd_realized)
    assert state["grand_total"] == pytest.approx(monthly_ytd_realized)


def test_current_partial_month_options_cycle_chart_filter_drops_month_end_row_characterization():
    events = [
        app.OptionPnLEvent(pd.Timestamp("2024-03-20"), "AAA", "Put", 100.0, 1, 100.0, 2.0, 0.0, "expiration"),
        app.OptionPnLEvent(pd.Timestamp("2024-04-20"), "AAA", "Put", 100.0, 1, 200.0, 2.0, 0.0, "expiration"),
    ]
    capital_daily = _make_capital_daily("2024-03-01", periods=58, total=10_000.0)
    as_of = pd.Timestamp("2024-04-27")

    monthly = app.build_monthly_summary(events, [], capital_daily, pd.DataFrame(), as_of)
    chart_df = build_options_cycle_chart_data(monthly)
    filtered_chart_df = filter_df_to_range(chart_df, "Date", as_of, "YTD")

    assert monthly.index.tolist() == [pd.Timestamp("2024-03-31"), pd.Timestamp("2024-04-30")]
    assert chart_df["pnl"].tolist() == [100.0, 200.0]
    assert filtered_chart_df["Date"].tolist() == [pd.Timestamp("2024-03-31")]


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


def test_vectorized_capital_price_resolution_matches_scalar_resolution():
    price_history = pd.Series(
        [110.0, None, 115.0],
        index=pd.to_datetime(["2024-05-24", "2024-05-27", "2024-05-28"]),
    )
    valuation_days = pd.date_range("2024-05-23", "2024-05-30", freq="D")

    vectorized = resolve_capital_prices_for_days(price_history, valuation_days, fallback_price=100.0)

    for valuation_day in valuation_days:
        scalar = resolve_capital_price_on_day(price_history, valuation_day, fallback_price=100.0)
        assert vectorized.loc[valuation_day] == pytest.approx(scalar)


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


def test_pipeline_denominator_incompleteness_unchanged_when_provider_fetch_fails(monkeypatch):
    class FailingHistoryYF:
        def download(self, **kwargs):
            raise RuntimeError("provider boom")

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

    monkeypatch.setattr(app, "yf", FailingHistoryYF())
    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
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

    assert state["historical_price_summary"] == {"requested": 1, "fetched": 0}
    assert state["capital_history_incomplete"] is True
    assert state["capital_history_affected_tickers"] == ["AAA"]
    assert any("Historical price download failed: provider boom" in msg for msg in state["issues"])
    assert state["monthly_cycles"]["roac"].isna().all()
    assert state["monthly_cycles"]["ropc"].isna().all()
    yearly_row = state["yearly"].loc[state["yearly"]["year"] == 2024].iloc[0]
    assert pd.isna(yearly_row["ann_roac"])
    assert pd.isna(yearly_row["annualized_return_twr"])


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


def test_pipeline_state_exposes_legacy_key_outputs(monkeypatch):
    df_opts = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-01-01"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-02-16"),
                "strike": 100.0,
                "qty": 1,
                "amount": 200.0,
                "commission": 0.0,
                "total_pnl": 200.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2024",
            },
            {
                "trans_date": pd.Timestamp("2024-01-10"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Buy",
                "expiration": pd.Timestamp("2024-02-16"),
                "strike": 100.0,
                "qty": 1,
                "amount": -50.0,
                "commission": 0.0,
                "total_pnl": -50.0,
                "assigned_flag": 0.0,
                "comment": "",
                "source_sheet": "Options 2024",
            },
        ]
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(app, "fetch_price_history_yf", lambda tickers, start, end: ({}, [], {"requested": 0, "fetched": 0}))
    monkeypatch.setattr(app, "fetch_current_prices_yf", lambda tickers: ({}, [], {"requested": 0, "fetched": 0}))
    monkeypatch.setattr(app, "collect_dividend_cashflows", lambda stock_txns, as_of: pd.DataFrame())
    monkeypatch.setattr(app, "align_benchmarks_monthly", lambda tickers, idx: {})

    state = app.build_pipeline(pd.Timestamp("2024-01-31").date(), False, ["Options 2024"])

    expected_keys = [
        "df_opts",
        "lots",
        "stock_txns",
        "realized_sales",
        "ending_inventory",
        "capital_daily",
        "monthly_cycles",
        "monthly_returns_w_div",
        "monthly_returns_covered",
        "monthly_returns_unrealized_adjusted",
        "monthly_returns_active",
        "open_options",
        "live_prices",
        "inv_df",
        "total_unreal",
        "option_unreal",
        "stock_unreal",
        "advanced_unreal",
        "yearly",
        "yearly_with_unreal",
        "per_ticker",
        "div_df",
        "as_of",
        "issues",
        "price_errors",
        "unrealized_blocked",
        "missing_required_price_tickers",
        "price_summary",
        "price_updated_at",
        "historical_price_summary",
        "historical_price_errors",
        "dividend_coverage_complete",
        "dividend_attempted_tickers",
        "dividend_failed_tickers",
        "dividend_affected_tickers",
        "dividend_errors",
        "dividend_summary",
        "stock_prices",
        "benchmark_metrics",
        "aligned_bench_returns",
        "per_ticker_totals",
        "grand_total",
        "cumulative_realized",
        "realized_option_events",
        "chain_outcomes",
        "sheet_counts",
        "capital_history_incomplete",
        "capital_history_coverage_issues",
        "capital_history_affected_months",
        "capital_history_affected_years",
        "capital_history_affected_tickers",
        "first_incomplete_return_month",
        "last_complete_return_month",
        "return_series_truncated",
    ]
    assert isinstance(state, PipelineState)
    assert state.keys() == expected_keys
    assert set(state.as_dict()) == set(expected_keys)
    assert state["yearly"] is state.yearly
    assert state.get("monthly_cycles") is state.monthly_cycles
    assert state["grand_total"] == pytest.approx(150.0)
    assert state.grand_total == pytest.approx(150.0)


def test_pipeline_cache_key_excludes_chart_period():
    selected_sheets = ["Options 2024", "Options 2025"]

    key_before_chart_change = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        True,
        selected_sheets,
        3,
    )
    chart_period = "1Y"
    key_after_chart_change = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        True,
        selected_sheets,
        3,
    )

    assert key_before_chart_change == key_after_chart_change
    assert chart_period not in key_after_chart_change
    assert key_after_chart_change == ("2026-04-27", ("Options 2024", "Options 2025"), 3)


def test_pipeline_cache_key_excludes_unrealized_adjusted_toggle():
    selected_sheets = ["Options 2024", "Options 2025"]

    key_toggle_off = app.build_pipeline_cache_key(pd.Timestamp("2026-04-27").date(), False, selected_sheets, 3)
    key_toggle_on = app.build_pipeline_cache_key(pd.Timestamp("2026-04-27").date(), True, selected_sheets, 3)

    assert key_toggle_off == key_toggle_on
    assert key_toggle_on == ("2026-04-27", ("Options 2024", "Options 2025"), 3)


def test_pipeline_cache_key_changes_when_reload_token_changes():
    selected_sheets = ["Options 2024", "Options 2025"]

    first_key = app.build_pipeline_cache_key(pd.Timestamp("2026-04-27").date(), False, selected_sheets, 3)
    refreshed_key = app.build_pipeline_cache_key(pd.Timestamp("2026-04-27").date(), False, selected_sheets, 4)

    assert first_key != refreshed_key
    assert first_key[:2] == refreshed_key[:2]
    assert first_key[2] == 3
    assert refreshed_key[2] == 4


def test_pipeline_cache_key_excludes_price_refresh_token():
    selected_sheets = ["Options 2024", "Options 2025"]

    key_before_price_refresh = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        False,
        selected_sheets,
        3,
    )
    price_refresh_token = "7:12"
    key_after_price_refresh = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        False,
        selected_sheets,
        3,
    )

    assert key_before_price_refresh == key_after_price_refresh
    assert price_refresh_token not in key_after_price_refresh


def _make_live_overlay_base_state() -> PipelineState:
    open_put = OptionLot(
        ticker="AAA",
        otype="Put",
        strike=100.0,
        qty=1,
        open_date=pd.Timestamp("2026-04-01"),
        expiration=pd.Timestamp("2026-05-17"),
        open_price=2.0,
        comment="",
        assigned=False,
    )
    monthly_cycles = pd.DataFrame(
        {"total_realized_pnl": [150.0], "roac": [0.03]},
        index=pd.to_datetime(["2026-04-30"]),
    )
    yearly = pd.DataFrame({"year": [2026], "total_realized_pnl": [150.0], "annualized_return_twr": [0.12]})
    benchmark_metrics = pd.DataFrame({"Series": ["My Strategy"], "CAGR": [0.12]})
    return PipelineState(
        df_opts=pd.DataFrame(),
        lots=[open_put],
        stock_txns=[],
        realized_sales=[],
        ending_inventory=[OpenLot("AAA", pd.Timestamp("2026-04-01"), 100, 100.0)],
        capital_daily=pd.DataFrame({"total": [10000.0]}, index=pd.to_datetime(["2026-04-29"]).rename("date")),
        monthly_cycles=monthly_cycles,
        monthly_returns_w_div=pd.Series([0.03], index=pd.to_datetime(["2026-04-30"])),
        monthly_returns_covered=pd.Series([0.03], index=pd.to_datetime(["2026-04-30"])),
        monthly_returns_unrealized_adjusted=pd.Series([0.03], index=pd.to_datetime(["2026-04-30"])),
        monthly_returns_active=pd.Series([0.03], index=pd.to_datetime(["2026-04-30"])),
        open_options=build_open_options_frame([open_put]),
        live_prices={"AAA": 50.0},
        inv_df=pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "buy_date": pd.Timestamp("2026-04-01"),
                    "shares": 100,
                    "cost_per_share": 100.0,
                    "current_price": 50.0,
                    "covered_shares": 0,
                    "covered_strike": None,
                    "unrealized_pnl": -5000.0,
                    "source": "stock_lot",
                }
            ]
        ),
        total_unreal=-5000.0,
        option_unreal=0.0,
        stock_unreal=-5000.0,
        advanced_unreal=pd.Series({"AAA": -5000.0}),
        yearly=yearly.copy(),
        yearly_with_unreal=yearly.copy(),
        per_ticker=pd.DataFrame(
            [
                {
                    "year": 2026,
                    "ticker": "AAA",
                    "options_pnl": 150.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 150.0,
                }
            ]
        ),
        div_df=pd.DataFrame(),
        as_of=pd.Timestamp("2026-04-29"),
        issues=["Historical capital price coverage incomplete: old issue"],
        price_errors=["stale price error"],
        unrealized_blocked=True,
        missing_required_price_tickers=["AAA"],
        price_summary={"stocks_requested": 1, "stocks_fetched": 0},
        price_updated_at="09:00:00",
        historical_price_summary={"requested": 0, "fetched": 0},
        historical_price_errors=[],
        dividend_coverage_complete=True,
        dividend_attempted_tickers=[],
        dividend_failed_tickers=[],
        dividend_affected_tickers=[],
        dividend_errors=[],
        dividend_summary={"attempted": 0, "failed": 0},
        stock_prices={"AAA": 50.0},
        benchmark_metrics=benchmark_metrics,
        aligned_bench_returns={"Cboe BXM": pd.Series([0.01], index=pd.to_datetime(["2026-04-30"]))},
        per_ticker_totals=pd.DataFrame(),
        grand_total=-4850.0,
        cumulative_realized=150.0,
        realized_option_events=[],
        chain_outcomes=[],
        sheet_counts=pd.DataFrame({"source_sheet": ["Options 2026"], "rows": [1]}),
        capital_history_incomplete=False,
        capital_history_coverage_issues=[],
        capital_history_affected_months=[],
        capital_history_affected_years=[],
        capital_history_affected_tickers=[],
        first_incomplete_return_month=None,
        last_complete_return_month=None,
        return_series_truncated=False,
    )


def test_price_refresh_token_updates_current_price_overlay(monkeypatch):
    app.get_cached_current_prices.clear()
    calls = []

    def fake_fetch(tickers):
        calls.append(tuple(tickers))
        price = 90.0 if len(calls) == 1 else 80.0
        return {"AAA": price}, [], {"requested": len(tickers), "fetched": len(tickers)}

    monkeypatch.setattr(app, "fetch_current_prices_yf", fake_fetch)
    base_state = _make_live_overlay_base_state()

    first = app.apply_live_price_overlay(base_state, price_refresh_token=("session-a", 0))
    cached = app.apply_live_price_overlay(base_state, price_refresh_token=("session-a", 0))
    refreshed = app.apply_live_price_overlay(base_state, price_refresh_token=("session-a", 1))

    assert calls == [("AAA",), ("AAA",)]
    assert first.stock_prices["AAA"] == pytest.approx(90.0)
    assert cached.stock_prices["AAA"] == pytest.approx(90.0)
    assert refreshed.stock_prices["AAA"] == pytest.approx(80.0)
    assert refreshed.total_unreal != pytest.approx(first.total_unreal)


def test_fresh_price_sessions_do_not_reuse_current_price_cache(monkeypatch):
    app.get_cached_current_prices.clear()
    calls = []

    def fake_fetch(tickers):
        calls.append(tuple(tickers))
        return {"AAA": float(len(calls))}, [], {"requested": len(tickers), "fetched": len(tickers)}

    monkeypatch.setattr(app, "fetch_current_prices_yf", fake_fetch)

    first_prices, _, _, _ = app.get_cached_current_prices("session-a", ("AAA",), 0)
    second_prices, _, _, _ = app.get_cached_current_prices("session-b", ("AAA",), 0)

    assert first_prices["AAA"] == pytest.approx(1.0)
    assert second_prices["AAA"] == pytest.approx(2.0)
    assert calls == [("AAA",), ("AAA",)]


def test_normal_reruns_keep_same_current_price_snapshot(monkeypatch):
    class FakeSessionState(dict):
        pass

    fake_st = type("FakeSt", (), {"session_state": FakeSessionState()})()
    monkeypatch.setattr(app, "st", fake_st)

    args = (pd.Timestamp("2026-04-27").date(), False, ["Options 2024", "Options 2025"])
    first_token = app._get_price_refresh_cache_token(*args)
    second_token = app._get_price_refresh_cache_token(*args)
    fake_st.session_state["chart_range"] = "1Y"
    chart_rerun_token = app._get_price_refresh_cache_token(*args)

    assert first_token == second_token == chart_rerun_token
    assert fake_st.session_state.get(app.PRICE_REFRESH_TOKEN_KEY, 0) == 0


def test_refresh_buttons_share_explicit_current_price_refresh_counter(monkeypatch):
    class FakeSessionState(dict):
        pass

    class FakeSt:
        session_state = FakeSessionState()

        @staticmethod
        def button(label, key):
            return key in {"refresh_prices_snapshot", "refresh_prices_positions"}

    monkeypatch.setattr(app, "st", FakeSt)
    monkeypatch.setattr(app, "_rerun_app", lambda: None)

    app._render_price_refresh_button("refresh_prices_snapshot")
    assert FakeSt.session_state[app.PRICE_REFRESH_TOKEN_KEY] == 1

    app._render_price_refresh_button("refresh_prices_positions")
    assert FakeSt.session_state[app.PRICE_REFRESH_TOKEN_KEY] == 2


def test_live_price_overlay_preserves_accounting_and_benchmark_outputs(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()

    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token="overlay-regression")

    pd.testing.assert_frame_equal(priced_state.monthly_cycles, base_state.monthly_cycles)
    pd.testing.assert_frame_equal(priced_state.yearly, base_state.yearly)
    pd.testing.assert_frame_equal(priced_state.yearly_with_unreal, base_state.yearly_with_unreal)
    pd.testing.assert_frame_equal(priced_state.benchmark_metrics, base_state.benchmark_metrics)
    assert priced_state.realized_option_events == base_state.realized_option_events
    assert priced_state.stock_txns == base_state.stock_txns


def test_positions_tab_data_uses_overlay_prices_not_stale_base_prices(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()

    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token="positions-overlay")
    open_options = app.build_open_options_positions_frame(priced_state.open_options, priced_state.stock_prices)

    assert priced_state.stock_prices["AAA"] == pytest.approx(120.0)
    assert priced_state.inv_df.loc[priced_state.inv_df["ticker"] == "AAA", "current_price"].iloc[0] == pytest.approx(120.0)
    assert open_options.loc[open_options["ticker"] == "AAA", "current_price"].iloc[0] == pytest.approx(120.0)
    assert open_options.loc[open_options["ticker"] == "AAA", "moneyness_pct"].iloc[0] == pytest.approx(-0.20)


def test_snapshot_and_positions_share_same_current_price_snapshot(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 125.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()

    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("session-a", 0))
    open_options = app.build_open_options_positions_frame(priced_state.open_options, priced_state.stock_prices)

    assert priced_state.live_prices is priced_state.stock_prices
    assert priced_state.stock_prices["AAA"] == pytest.approx(125.0)
    assert priced_state.inv_df.loc[priced_state.inv_df["ticker"] == "AAA", "current_price"].iloc[0] == pytest.approx(125.0)
    assert open_options.loc[open_options["ticker"] == "AAA", "current_price"].iloc[0] == pytest.approx(125.0)


def test_backend_serializers_emit_json_safe_portfolio_shapes(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()
    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("serializer", 0))
    state = app.apply_unrealized_adjusted_display(priced_state, True)

    payload = serialize_portfolio_state(state, include_unrealized_current_year=True)
    json.dumps(payload)

    assert set(payload) == {"snapshot", "positions", "yearly", "monthly", "per_ticker", "issues", "metadata"}
    assert payload["snapshot"]["as_of"] == "2026-04-29"
    assert payload["snapshot"]["ytd_realized_pnl"] == pytest.approx(150.0)
    assert payload["snapshot"]["ytd_total_pnl"] == pytest.approx(2350.0)
    assert payload["snapshot"]["unrealized"]["total"] == pytest.approx(2200.0)
    assert payload["snapshot"]["unrealized"]["options"] == pytest.approx(200.0)
    assert payload["snapshot"]["unrealized"]["stock"] == pytest.approx(2000.0)
    assert payload["snapshot"]["prices"] == {
        "updated_at": payload["snapshot"]["prices"]["updated_at"],
        "stocks_requested": 1,
        "stocks_fetched": 1,
    }
    assert payload["positions"]["assigned_holdings"][0]["current_price"] == pytest.approx(120.0)
    assert payload["positions"]["open_option_shorts"][0]["current_price"] == pytest.approx(120.0)
    assert payload["positions"]["open_option_shorts"][0]["moneyness_pct"] == pytest.approx(-0.20)
    assert payload["monthly"]["cycles"] == [
        {"month": "2026-04-30", "total_realized_pnl": 150.0, "roac": 0.03}
    ]
    assert payload["monthly"]["returns"] == [{"month": "2026-04-30", "return": 0.25}]
    assert payload["monthly"]["covered_returns"] == [{"month": "2026-04-30", "return": 0.03}]
    assert payload["monthly"]["unrealized_adjusted_returns"] == [{"month": "2026-04-30", "return": 0.25}]
    assert payload["monthly"]["active_returns"] == [{"month": "2026-04-30", "return": 0.03}]
    assert payload["per_ticker"][0]["ticker"] == "AAA"
    assert payload["metadata"]["sheet_counts"] == [{"source_sheet": "Options 2026", "rows": 1}]


def test_dashboard_view_model_matches_snapshot_display_inputs(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()
    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("view-model", 0))
    state = app.apply_unrealized_adjusted_display(priced_state, True)

    view_model = build_dashboard_view_model(state, include_unrealized_current_year=True)

    assert view_model.as_of_year == 2026
    assert view_model.realized_total == pytest.approx(150.0)
    assert view_model.ytd_total == pytest.approx(2350.0)
    assert view_model.ytd_twr is not pd.NA
    assert view_model.unrealized_blocked is False
    assert view_model.price_summary == {"stocks_requested": 1, "stocks_fetched": 1}
    assert view_model.dividend_warning_note is None
    assert view_model.covered_period_note is None
    assert view_model.yearly is state.yearly_with_unreal
    assert view_model.monthly_cycles is state.monthly_cycles


def test_portfolio_payload_matches_dashboard_view_model(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()
    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("api-payload", 0))
    state = app.apply_unrealized_adjusted_display(priced_state, True)
    view_model = build_dashboard_view_model(state, include_unrealized_current_year=True)

    payload = build_portfolio_payload(state, include_unrealized_current_year=True)
    json.dumps(payload)

    assert set(payload) == {"snapshot", "positions", "yearly", "monthly", "per_ticker", "issues", "metadata"}
    assert payload["metadata"]["payload_version"] == PORTFOLIO_PAYLOAD_VERSION
    assert payload["snapshot"]["year"] == view_model.as_of_year
    assert payload["snapshot"]["ytd_realized_pnl"] == pytest.approx(view_model.realized_total)
    assert payload["snapshot"]["ytd_total_pnl"] == pytest.approx(view_model.ytd_total)
    assert payload["snapshot"]["ytd_annualized_twr"] == pytest.approx(float(view_model.ytd_twr))
    assert payload["snapshot"]["unrealized"]["total"] == pytest.approx(state.total_unreal)
    assert payload["snapshot"]["unrealized"]["options"] == pytest.approx(state.option_unreal)
    assert payload["snapshot"]["unrealized"]["stock"] == pytest.approx(state.stock_unreal)
    assert payload["snapshot"]["prices"]["stocks_requested"] == view_model.price_summary["stocks_requested"]
    assert payload["snapshot"]["prices"]["stocks_fetched"] == view_model.price_summary["stocks_fetched"]
    assert payload["snapshot"]["covered_period_note"] == view_model.covered_period_note
    assert payload["snapshot"]["dividend_warning_note"] == view_model.dividend_warning_note
    assert payload["snapshot"]["issue_count"] == len(view_model.issues) + len(view_model.price_errors)
    assert payload["positions"]["assigned_holdings"]
    assert payload["positions"]["open_option_shorts"]


def test_generic_portfolio_payload_matches_sample_fixture(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()
    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("mobile-contract", 0))
    state = app.apply_unrealized_adjusted_display(priced_state, True)
    state.price_updated_at = "12:34:56"

    payload = build_portfolio_payload(state, include_unrealized_current_year=True)
    json.dumps(payload)

    # Legacy fixture name retained for now; this shape is the generic backend
    # payload, not the mobile API contract in docs/mobile-api-contract.md.
    fixture_path = Path(__file__).parent / "fixtures" / "mobile_portfolio_payload_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert payload == expected


def test_portfolio_payload_includes_dashboard_notes():
    state = _make_live_overlay_base_state()
    state.capital_history_incomplete = True
    state.monthly_returns_covered = pd.Series(dtype=float)
    state.return_series_truncated = True
    state.first_incomplete_return_month = pd.Timestamp("2026-03-31")
    state.last_complete_return_month = pd.Timestamp("2026-02-28")
    state.dividend_coverage_complete = False
    state.dividend_affected_tickers = ["AAA"]

    payload = build_portfolio_payload(state, include_unrealized_current_year=False)

    assert payload["snapshot"]["covered_period_note"] == (
        "Return-based charts and benchmark metrics are shown through 2026-02-28 only. "
        "Later periods are incomplete due to missing historical capital prices and are excluded."
    )
    assert payload["snapshot"]["dividend_warning_note"] == (
        "Dividend data is incomplete for AAA. "
        "Realized P&L and return metrics remain visible but may understate dividends."
    )


def test_api_service_builds_payload_with_live_price_overlay():
    calls = {}

    def load_options(sheet_id, selected_sheets):
        calls["load_options"] = (sheet_id, tuple(selected_sheets))
        return pd.DataFrame(
            [
                {
                    "trans_date": pd.Timestamp("2026-04-01"),
                    "ticker": "AAA",
                    "type": "Put",
                    "action": "Sell",
                    "expiration": pd.Timestamp("2026-05-17"),
                    "strike": 100.0,
                    "qty": 1,
                    "amount": 200.0,
                    "commission": 0.0,
                    "total_pnl": 200.0,
                    "assigned_flag": 0.0,
                    "comment": "",
                    "source_sheet": "Options 2026",
                }
            ]
        )

    def fetch_price_history(tickers, start, end):
        calls["fetch_price_history"] = (set(tickers), start, end)
        return {}, [], {"requested": len(tickers), "fetched": 0}

    def collect_dividend_cashflows(stock_txns, as_of):
        calls["collect_dividend_cashflows"] = (list(stock_txns), as_of)
        return pd.DataFrame(columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"])

    def align_benchmarks_monthly(tickers, idx):
        calls["align_benchmarks_monthly"] = (dict(tickers), idx.copy())
        return {}

    def fetch_current_prices(tickers):
        calls["fetch_current_prices"] = tuple(tickers)
        return {"AAA": 80.0}, [], {"requested": len(tickers), "fetched": len(tickers)}

    payload = build_payload_for_request(
        PortfolioPayloadRequest(
            sheet_id="sheet-1",
            as_of=pd.Timestamp("2026-04-29").date(),
            selected_sheets=["Options 2026"],
            include_unrealized_current_year=True,
            price_updated_at="12:34:56",
        ),
        PortfolioServiceDependencies(
            load_options=load_options,
            fetch_price_history=fetch_price_history,
            collect_dividend_cashflows=collect_dividend_cashflows,
            align_benchmarks_monthly=align_benchmarks_monthly,
            fetch_current_prices=fetch_current_prices,
        ),
    )
    json.dumps(payload)

    assert calls["load_options"] == ("sheet-1", ("Options 2026",))
    assert calls["fetch_current_prices"] == ("AAA",)
    assert payload["metadata"]["payload_version"] == PORTFOLIO_PAYLOAD_VERSION
    assert payload["metadata"]["sheet_counts"] == [{"source_sheet": "Options 2026", "rows": 1}]
    assert payload["snapshot"]["prices"] == {
        "updated_at": "12:34:56",
        "stocks_requested": 1,
        "stocks_fetched": 1,
    }
    assert payload["snapshot"]["unrealized"]["complete"] is True
    open_shorts = payload["positions"]["open_option_shorts"]
    assert len(open_shorts) == 1
    assert open_shorts[0]["ticker"] == "AAA"
    assert open_shorts[0]["current_price"] == pytest.approx(80.0)
    assert open_shorts[0]["moneyness_pct"] == pytest.approx(0.20)


def test_api_service_can_build_payload_without_live_price_dependency():
    def load_options(sheet_id, selected_sheets):
        return pd.DataFrame(
            [
                {
                    "trans_date": pd.Timestamp("2026-04-01"),
                    "ticker": "AAA",
                    "type": "Put",
                    "action": "Sell",
                    "expiration": pd.Timestamp("2026-05-17"),
                    "strike": 100.0,
                    "qty": 1,
                    "amount": 200.0,
                    "commission": 0.0,
                    "total_pnl": 200.0,
                    "assigned_flag": 0.0,
                    "comment": "",
                    "source_sheet": "Options 2026",
                }
            ]
        )

    payload = build_payload_for_request(
        PortfolioPayloadRequest(
            sheet_id="sheet-1",
            as_of=pd.Timestamp("2026-04-29").date(),
            selected_sheets=["Options 2026"],
            include_unrealized_current_year=False,
        ),
        PortfolioServiceDependencies(
            load_options=load_options,
            fetch_price_history=lambda tickers, start, end: ({}, [], {"requested": 0, "fetched": 0}),
            collect_dividend_cashflows=lambda stock_txns, as_of: pd.DataFrame(
                columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]
            ),
            align_benchmarks_monthly=lambda tickers, idx: {},
        ),
    )

    assert payload["snapshot"]["prices"] == {
        "updated_at": None,
        "stocks_requested": 0,
        "stocks_fetched": 0,
    }
    assert payload["positions"]["open_option_shorts"][0]["current_price"] is None
    assert payload["positions"]["open_option_shorts"][0]["moneyness_pct"] is None


def test_dashboard_view_model_builds_coverage_and_dividend_notes():
    state = _make_live_overlay_base_state()
    state.capital_history_incomplete = True
    state.monthly_returns_covered = pd.Series(dtype=float)
    state.return_series_truncated = True
    state.first_incomplete_return_month = pd.Timestamp("2026-03-31")
    state.last_complete_return_month = pd.Timestamp("2026-02-28")
    state.dividend_coverage_complete = False
    state.dividend_affected_tickers = ["AAA", "BBB"]
    state.dividend_errors = ["AAA: timeout"]

    view_model = build_dashboard_view_model(state, include_unrealized_current_year=False)

    assert view_model.covered_period_note == (
        "Return-based charts and benchmark metrics are shown through 2026-02-28 only. "
        "Later periods are incomplete due to missing historical capital prices and are excluded."
    )
    assert view_model.dividend_warning_note == (
        "Dividend data is incomplete for AAA, BBB. "
        "Realized P&L and return metrics remain visible but may understate dividends."
    )
    assert view_model.dividend_errors == ["AAA: timeout"]


def test_snapshot_serializer_matches_unrealized_toggle_totals(monkeypatch):
    app.get_cached_current_prices.clear()
    monkeypatch.setattr(
        app,
        "fetch_current_prices_yf",
        lambda tickers: ({"AAA": 120.0}, [], {"requested": len(tickers), "fetched": len(tickers)}),
    )
    base_state = _make_live_overlay_base_state()
    priced_state = app.apply_live_price_overlay(base_state, price_refresh_token=("serializer-toggle", 0))

    realized_snapshot = serialize_snapshot(
        app.apply_unrealized_adjusted_display(priced_state, False),
        include_unrealized_current_year=False,
    )
    adjusted_snapshot = serialize_snapshot(
        app.apply_unrealized_adjusted_display(priced_state, True),
        include_unrealized_current_year=True,
    )

    assert realized_snapshot["ytd_total_pnl"] == pytest.approx(150.0)
    assert adjusted_snapshot["ytd_total_pnl"] == pytest.approx(2350.0)
    assert realized_snapshot["ytd_annualized_twr"] == pytest.approx(0.12)
    assert adjusted_snapshot["ytd_annualized_twr"] is not None


def test_refresh_data_caches_clears_persistent_dividend_history_cache():
    class FakeTicker:
        def __init__(self, dividends):
            self._dividends = dividends

        @property
        def dividends(self):
            return self._dividends

    class FakeYF:
        def __init__(self):
            self.calls = []

        def Ticker(self, ticker):
            self.calls.append(ticker)
            return FakeTicker(pd.Series([0.5], index=pd.to_datetime(["2024-01-15"])))

    app.clear_dividend_history_cache()
    yf_module = FakeYF()
    provider = app.YFinanceDividendProvider(yf_module)

    provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    app._clear_data_caches()
    provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert yf_module.calls == ["AAA", "AAA"]


def test_refresh_data_caches_clears_current_price_cache(monkeypatch):
    app.get_cached_current_prices.clear()
    calls = []

    def fake_fetch(tickers):
        calls.append(tuple(tickers))
        return {"AAA": float(len(calls))}, [], {"requested": len(tickers), "fetched": len(tickers)}

    monkeypatch.setattr(app, "fetch_current_prices_yf", fake_fetch)

    first_prices, _, _, _ = app.get_cached_current_prices("session-a", ("AAA",), 0)
    cached_prices, _, _, _ = app.get_cached_current_prices("session-a", ("AAA",), 0)
    app._clear_data_caches()
    refreshed_prices, _, _, _ = app.get_cached_current_prices("session-a", ("AAA",), 0)

    assert first_prices["AAA"] == pytest.approx(1.0)
    assert cached_prices["AAA"] == pytest.approx(1.0)
    assert refreshed_prices["AAA"] == pytest.approx(2.0)
    assert calls == [("AAA",), ("AAA",)]


def test_price_refresh_counter_does_not_change_pipeline_cache_key(monkeypatch):
    class FakeSessionState(dict):
        pass

    fake_st = type("FakeSt", (), {"session_state": FakeSessionState()})()
    monkeypatch.setattr(app, "st", fake_st)
    selected_sheets = ["Options 2024", "Options 2025"]

    before_key = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        False,
        selected_sheets,
        app._get_pipeline_reload_token(),
    )
    app._increment_price_refresh_token()
    after_key = app.build_pipeline_cache_key(
        pd.Timestamp("2026-04-27").date(),
        False,
        selected_sheets,
        app._get_pipeline_reload_token(),
    )

    assert before_key == after_key


def test_unrealized_adjusted_display_step_matches_previous_pipeline_behavior(monkeypatch):
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

    history = pd.Series(
        [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
        index=pd.to_datetime(
            [
                "2024-05-10",
                "2024-05-13",
                "2024-05-14",
                "2024-05-15",
                "2024-05-16",
                "2024-05-17",
                "2024-05-20",
            ]
        ),
    )

    monkeypatch.setattr(app, "load_options", lambda sheet_id, sheets: df_opts.copy())
    monkeypatch.setattr(
        app,
        "fetch_price_history_yf",
        lambda tickers, start, end: (
            {"AAA": history.copy()},
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

    base_state = app.build_base_pipeline(pd.Timestamp("2024-05-20").date(), ["Options 2024"])

    for include_unrealized in (False, True):
        previous_state = app._build_pipeline_uncached(
            pd.Timestamp("2024-05-20").date(),
            include_unrealized,
            ["Options 2024"],
        )
        split_state = app.apply_unrealized_adjusted_display(base_state, include_unrealized)

        pd.testing.assert_series_equal(
            split_state.monthly_returns_unrealized_adjusted,
            previous_state.monthly_returns_unrealized_adjusted,
        )
        pd.testing.assert_frame_equal(split_state.yearly, previous_state.yearly)
        pd.testing.assert_frame_equal(split_state.yearly_with_unreal, previous_state.yearly_with_unreal)
        assert split_state.grand_total == pytest.approx(previous_state.grand_total)
        assert split_state.issues == previous_state.issues


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

    idx = pd.to_datetime(["2024-02-29", "2024-03-31", "2024-04-30"])

    aligned = data_source_align_benchmarks_monthly({"Bench": "BENCH"}, idx, FakeYF())

    assert list(aligned["Bench"].index) == list(idx)
    assert aligned["Bench"].tolist() == pytest.approx([0.10, 0.10, 0.10])


def test_align_benchmarks_monthly_internal_missing_month_is_not_forward_filled(monkeypatch):
    class FakeYF:
        def download(self, *args, **kwargs):
            return pd.DataFrame(
                {"Close": [100.0, 110.0, 133.1]},
                index=pd.to_datetime(["2024-01-31", "2024-02-29", "2024-04-30"]),
            )

    idx = pd.to_datetime(["2024-02-29", "2024-03-31", "2024-04-30"])

    aligned = data_source_align_benchmarks_monthly({"Bench": "BENCH"}, idx, FakeYF())["Bench"]

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


def test_benchmark_cagr_and_risk_metrics_are_unavailable_when_window_has_internal_gap():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"])
    strategy_returns = pd.Series([0.02, -0.01, 0.03, -0.02, 0.01], index=idx)
    gappy_benchmark = pd.Series([0.01, float("nan"), -0.02, 0.04, -0.01], index=idx)

    strategy_metric_index = strategy_returns.index
    benchmark_metric_index = gappy_benchmark.index
    previous_observed_only_index = gappy_benchmark.dropna().index
    strategy_metrics = calculate_performance_metrics(strategy_returns)
    benchmark_metrics = app.calculate_performance_metrics_if_complete(gappy_benchmark)

    assert strategy_metric_index.tolist() == idx.tolist()
    assert benchmark_metric_index.tolist() == [
        pd.Timestamp("2024-01-31"),
        pd.Timestamp("2024-02-29"),
        pd.Timestamp("2024-03-31"),
        pd.Timestamp("2024-04-30"),
        pd.Timestamp("2024-05-31"),
    ]
    assert previous_observed_only_index.tolist() != strategy_metric_index.tolist()
    for metric in ("CAGR", "Volatility", "Sharpe", "Sortino", "Max Drawdown"):
        assert pd.isna(benchmark_metrics[metric])
        assert metric in strategy_metrics


def test_complete_benchmark_cagr_and_risk_metrics_are_unchanged():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"])
    complete_benchmark = pd.Series([0.01, 0.02, -0.02, 0.04, -0.01], index=idx)

    complete_window_metrics = app.calculate_performance_metrics_if_complete(complete_benchmark)
    legacy_metrics = calculate_performance_metrics(complete_benchmark)

    for metric in ("CAGR", "Volatility", "Sharpe", "Sortino", "Max Drawdown"):
        assert complete_window_metrics[metric] == pytest.approx(legacy_metrics[metric])


def test_benchmark_risk_metric_window_matches_chart_one_year_window():
    idx = pd.date_range("2024-01-31", periods=15, freq="ME")
    strategy_returns = pd.Series([0.01] * 15, index=idx)
    benchmark_returns = pd.Series([0.02] * 15, index=idx)

    chart_df = build_benchmark_growth_chart_data(strategy_returns, {"Bench": benchmark_returns}, "1Y")
    risk_window = benchmark_returns.tail(12)
    benchmark_chart = chart_df.loc[chart_df["Series"] == "Bench"].reset_index(drop=True)

    assert benchmark_chart["Growth"].iloc[0] == pytest.approx(1.0)
    assert risk_window.index.tolist() == benchmark_chart["Date"].iloc[1:].tolist()


def test_sortino_is_unavailable_when_all_returns_exceed_monthly_risk_free_rate():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
    all_positive_returns = pd.Series([0.01, 0.02, 0.015], index=idx)

    metrics = calculate_performance_metrics(all_positive_returns)

    assert pd.isna(metrics["Sortino"])


def test_sortino_is_calculated_when_returns_include_downside_vs_risk_free_rate():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
    mixed_returns = pd.Series([0.02, -0.01, 0.015], index=idx)

    metrics = calculate_performance_metrics(mixed_returns)

    assert pd.notna(metrics["Sortino"])


def test_sortino_uses_monthly_risk_free_rate_not_zero_return_as_downside_threshold():
    idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
    positive_but_below_rf_returns = pd.Series([0.001, 0.002, 0.0015], index=idx)

    metrics = calculate_performance_metrics(positive_but_below_rf_returns)

    assert pd.notna(metrics["Sortino"])
    assert metrics["Sortino"] < 0


def test_sortino_single_observation_depends_on_downside_presence():
    downside_month = pd.Series([-0.01], index=pd.to_datetime(["2024-01-31"]))
    no_downside_month = pd.Series([0.01], index=pd.to_datetime(["2024-01-31"]))

    downside_metrics = calculate_performance_metrics(downside_month)
    no_downside_metrics = calculate_performance_metrics(no_downside_month)

    assert pd.notna(downside_metrics["Sortino"])
    assert pd.isna(no_downside_metrics["Sortino"])


def test_benchmark_table_formatting_renders_unavailable_sortino_as_na():
    df = pd.DataFrame({"Series": ["My Strategy"], "Sortino": [float("nan")]})

    html = app._format_df(df, float_cols=["Sortino"], hide_index=True, na_rep="n/a").to_html()

    assert "n/a" in html
    assert "None" not in html


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
