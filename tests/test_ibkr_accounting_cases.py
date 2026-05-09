from __future__ import annotations

import pandas as pd
import pytest

from portfolio_backend.calculations import build_option_trades, process_option_positions
from portfolio_backend.ibkr.dedupe import dedupe_key
from portfolio_backend.ibkr.flex_parser import IbkrRawRow
from portfolio_backend.ibkr.flex_parser import IbkrFlexReport
from portfolio_backend.ibkr.normalization import normalize_transactions
from portfolio_backend.ibkr.option_accounting import (
    cashflow_summary,
    filter_executions,
    option_executions_from_rows,
)
from portfolio_backend.ibkr.performance import (
    WheelHoldingSegment,
    cashflows_from_rows,
    stock_realized_from_rows,
    wheel_option_executions,
    yearly_performance_from_report,
)
from portfolio_backend.ibkr.source_adapter import option_trades_to_dataframe
from portfolio_backend.ibkr.pipeline import build_ibkr_base_pipeline, wheel_stock_transactions_from_report
from portfolio_backend.ibkr.pipeline import wheel_options_dataframe_from_report


def _trade(**overrides) -> IbkrRawRow:
    attrs = {
        "accountId": "U0000000",
        "assetCategory": "OPT",
        "currency": "USD",
        "symbol": "ABC  260117P00100000",
        "underlyingSymbol": "ABC",
        "description": "ABC 17JAN26 100 P",
        "conid": "1001",
        "tradeID": "T1",
        "transactionID": "X1",
        "ibExecID": "E1",
        "tradeDate": "20260115",
        "dateTime": "20260115;154500",
        "settleDateTarget": "20260116",
        "expiry": "20260117",
        "strike": "100",
        "putCall": "P",
        "buySell": "SELL",
        "openCloseIndicator": "O",
        "quantity": "-1",
        "multiplier": "100",
        "tradePrice": "2.50",
        "proceeds": "250",
        "ibCommission": "-1",
        "netCash": "249",
        "transactionType": "Trade",
    }
    attrs.update({k: str(v) for k, v in overrides.items()})
    return IbkrRawRow("Trade", attrs)


def _option_eae(**overrides) -> IbkrRawRow:
    attrs = {
        "accountId": "U0000000",
        "assetCategory": "OPT",
        "currency": "USD",
        "symbol": "ABC  260117P00100000",
        "underlyingSymbol": "ABC",
        "description": "ABC 17JAN26 100 P",
        "conid": "1001",
        "tradeID": "T1",
        "date": "20260117",
        "transactionType": "Assignment",
        "quantity": "-1",
        "tradePrice": "0",
        "proceeds": "0",
        "commisionsAndTax": "0",
        "costBasis": "0",
        "realizedPnl": "249",
        "mtmPnl": "0",
        "multiplier": "100",
        "strike": "100",
        "expiry": "20260117",
        "putCall": "P",
    }
    attrs.update({k: str(v) for k, v in overrides.items()})
    return IbkrRawRow("OptionEAE", attrs)


def _stock_eae(**overrides) -> IbkrRawRow:
    attrs = {
        "accountId": "U0000000",
        "assetCategory": "STK",
        "currency": "USD",
        "symbol": "ABC",
        "underlyingSymbol": "",
        "tradeID": "S1",
        "date": "20260117",
        "transactionType": "Buy",
        "quantity": "100",
        "tradePrice": "100",
        "proceeds": "-10000",
    }
    attrs.update({k: str(v) for k, v in overrides.items()})
    return IbkrRawRow("OptionEAE", attrs)


def _report(rows_by_section) -> IbkrFlexReport:
    return IbkrFlexReport(
        root_tag="FlexQueryResponse",
        metadata={},
        rows_by_section=rows_by_section,
        section_counts={section: len(rows) for section, rows in rows_by_section.items()},
    )


def _events_from_rows(trade_rows, eae_rows=(), *, as_of="2026-01-31"):
    df = option_trades_to_dataframe(trade_rows, eae_rows)
    trades = build_option_trades(df, [])
    return process_option_positions(trades, pd.Timestamp(as_of))


def _empty_price_history(_tickers, _start, _end):
    return {}, [], {"requested": 0, "fetched": 0}


def _empty_benchmarks(_tickers, _months):
    return {}


def test_ibkr_short_put_expiration_books_premium_without_stock():
    events, open_lots, stock_txns, issues, _ = _events_from_rows([_trade()])

    assert not issues
    assert not open_lots
    assert not stock_txns
    assert len(events) == 1
    assert events[0].reason == "expiration"
    assert events[0].pnl == pytest.approx(249.0)


def test_ibkr_short_put_assignment_creates_stock_buy():
    events, open_lots, stock_txns, issues, _ = _events_from_rows([_trade()], [_option_eae()])

    assert not issues
    assert not open_lots
    assert len(events) == 1
    assert events[0].reason == "assignment"
    assert events[0].pnl == pytest.approx(249.0)
    assert len(stock_txns) == 1
    assert stock_txns[0].ticker == "ABC"
    assert stock_txns[0].side == "BUY"
    assert stock_txns[0].shares == 100
    assert stock_txns[0].price == 100.0


def test_ibkr_assignment_book_trade_does_not_mask_assignment():
    book_close = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        tradeDate="20260117",
        dateTime="20260117;154500",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="0",
        ibCommission="0",
        netCash="0",
        transactionType="BookTrade",
        notes="A",
    )

    events, open_lots, stock_txns, issues, _ = _events_from_rows([_trade(), book_close], [_option_eae()])

    assert not issues
    assert not open_lots
    assert len(events) == 1
    assert events[0].reason == "assignment"
    assert len(stock_txns) == 1
    assert stock_txns[0].side == "BUY"


def test_ibkr_short_call_assignment_creates_stock_sell():
    call_open = _trade(
        symbol="ABC  260117C00110000",
        description="ABC 17JAN26 110 C",
        conid="2001",
        strike="110",
        putCall="C",
        quantity="-2",
        tradePrice="1.25",
        proceeds="250",
        ibCommission="-2",
        netCash="248",
    )
    call_assignment = _option_eae(
        symbol="ABC  260117C00110000",
        description="ABC 17JAN26 110 C",
        conid="2001",
        strike="110",
        putCall="C",
        quantity="-2",
        realizedPnl="248",
    )

    events, open_lots, stock_txns, issues, _ = _events_from_rows([call_open], [call_assignment])

    assert not issues
    assert not open_lots
    assert len(events) == 1
    assert events[0].reason == "assignment"
    assert len(stock_txns) == 1
    assert stock_txns[0].side == "SELL"
    assert stock_txns[0].shares == 200
    assert stock_txns[0].price == 110.0


def test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls():
    report = _report(
        {
            "OptionEAE": [
                _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000"),
                _stock_eae(
                    tradeID="S2",
                    date="20260220",
                    transactionType="Sell",
                    quantity="-100",
                    tradePrice="110",
                    proceeds="11000",
                ),
                _stock_eae(
                    symbol="XYZ",
                    tradeID="S3",
                    date="20260220",
                    transactionType="Sell",
                    quantity="-100",
                    tradePrice="50",
                    proceeds="5000",
                ),
            ]
        }
    )

    stock_txns, issues = wheel_stock_transactions_from_report(report, as_of=pd.Timestamp("2026-02-28"))

    assert [(txn.ticker, txn.side, txn.shares, txn.price) for txn in stock_txns] == [
        ("ABC", "BUY", 100, 100.0),
        ("ABC", "SELL", 100, 110.0),
    ]
    assert issues == [
        "Ignored 100 assigned-call sold shares of XYZ on 2026-02-20 because no assignment-derived stock inventory was available."
    ]


def test_ibkr_wheel_options_dataframe_preserves_prorated_call_execution():
    report = _report(
        {
            "Trade": [
                _trade(
                    symbol="ABC  260220C00110000",
                    description="ABC 20FEB26 110 C",
                    conid="2001",
                    tradeID="TC1",
                    transactionID="XC1",
                    ibExecID="EC1",
                    tradeDate="20260120",
                    dateTime="20260120;154500",
                    expiry="20260220",
                    strike="110",
                    putCall="C",
                    buySell="SELL",
                    openCloseIndicator="O",
                    quantity="-2",
                    tradePrice="1.00",
                    proceeds="200",
                    ibCommission="-2",
                    netCash="198",
                )
            ],
            "OptionEAE": [
                _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000")
            ],
        }
    )

    df, issues = wheel_options_dataframe_from_report(report, through=pd.Timestamp("2026-01-31"))

    assert len(df) == 1
    row = df.iloc[0]
    assert row["ticker"] == "ABC"
    assert row["type"] == "Call"
    assert row["action"] == "Sell"
    assert row["qty"] == pytest.approx(1.0)
    assert row["amount"] == pytest.approx(100.0)
    assert row["commission"] == pytest.approx(1.0)
    assert row["total_pnl"] == pytest.approx(99.0)
    assert issues == [
        "Prorated ABC call execution on 2026-01-20 to 100 wheel-held shares out of 200 required shares."
    ]


def test_ibkr_wheel_call_filter_does_not_reuse_same_shares_for_overlapping_calls():
    executions = option_executions_from_rows(
        [
            _trade(
                symbol="ABC  260220C00110000",
                description="ABC 20FEB26 110 C",
                conid="2001",
                tradeID="TC1",
                transactionID="XC1",
                ibExecID="EC1",
                tradeDate="20260110",
                dateTime="20260110;154500",
                expiry="20260220",
                strike="110",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="100",
                ibCommission="-1",
                netCash="99",
            ),
            _trade(
                symbol="ABC  260220C00115000",
                description="ABC 20FEB26 115 C",
                conid="2002",
                tradeID="TC2",
                transactionID="XC2",
                ibExecID="EC2",
                tradeDate="20260111",
                dateTime="20260111;154500",
                expiry="20260220",
                strike="115",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="100",
                ibCommission="-1",
                netCash="99",
            ),
            _trade(
                symbol="ABC  260220C00110000",
                description="ABC 20FEB26 110 C",
                conid="2001",
                tradeID="TC3",
                transactionID="XC3",
                ibExecID="EC3",
                tradeDate="20260120",
                dateTime="20260120;154500",
                expiry="20260220",
                strike="110",
                putCall="C",
                buySell="BUY",
                openCloseIndicator="C",
                quantity="1",
                proceeds="-50",
                ibCommission="-1",
                netCash="-51",
            ),
            _trade(
                symbol="ABC  260220C00120000",
                description="ABC 20FEB26 120 C",
                conid="2003",
                tradeID="TC4",
                transactionID="XC4",
                ibExecID="EC4",
                tradeDate="20260121",
                dateTime="20260121;154500",
                expiry="20260220",
                strike="120",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="100",
                ibCommission="-1",
                netCash="99",
            ),
        ]
    )
    holding_segments = [
        WheelHoldingSegment(
            ticker="ABC",
            start=pd.Timestamp("2026-01-01"),
            end=pd.Timestamp("2026-03-01"),
            shares=100,
            cost_per_share=100,
        )
    ]

    included, excluded, issues = wheel_option_executions(executions, holding_segments)

    assert [row.trade_id for row in included] == ["TC1", "TC3", "TC4"]
    assert [row.trade_id for row in excluded] == ["TC2"]
    assert issues == [
        "Excluded ABC call execution on 2026-01-11 because all assignment-derived stock inventory was already covering other calls."
    ]


def test_ibkr_wheel_call_filter_keeps_excluded_roll_chain_out_of_wheel_pnl():
    executions = option_executions_from_rows(
        [
            _trade(
                symbol="ABC  260417C00225000",
                description="ABC 17APR26 225 C",
                conid="3001",
                tradeID="TC1",
                transactionID="XC1",
                ibExecID="EC1",
                tradeDate="20260317",
                dateTime="20260317;154500",
                expiry="20260417",
                strike="225",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="306",
                ibCommission="-1",
                netCash="305",
            ),
            _trade(
                symbol="ABC  260417C00200000",
                description="ABC 17APR26 200 C",
                conid="3002",
                tradeID="TC2",
                transactionID="XC2",
                ibExecID="EC2",
                tradeDate="20260407",
                dateTime="20260407;154500",
                expiry="20260417",
                strike="200",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="151",
                ibCommission="-1",
                netCash="150",
            ),
            _trade(
                symbol="ABC  260417C00200000",
                description="ABC 17APR26 200 C",
                conid="3002",
                tradeID="TC3",
                transactionID="XC3",
                ibExecID="00014247.NONWHEEL1.03.01",
                tradeDate="20260414",
                dateTime="20260414;154500",
                expiry="20260417",
                strike="200",
                putCall="C",
                buySell="BUY",
                openCloseIndicator="C",
                quantity="1",
                proceeds="-761",
                ibCommission="-1",
                netCash="-762",
            ),
            _trade(
                symbol="ABC  260516C00210000",
                description="ABC 16MAY26 210 C",
                conid="3003",
                tradeID="TC4",
                transactionID="XC4",
                ibExecID="00014247.NONWHEEL1.02.01",
                tradeDate="20260414",
                dateTime="20260414;154600",
                expiry="20260516",
                strike="210",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="800",
                ibCommission="-1",
                netCash="799",
            ),
            _trade(
                symbol="ABC  260417C00225000",
                description="ABC 17APR26 225 C",
                conid="3001",
                tradeID="TC5",
                transactionID="XC5",
                ibExecID="",
                tradeDate="20260417",
                dateTime="20260417;154500",
                expiry="20260417",
                strike="225",
                putCall="C",
                buySell="BUY",
                openCloseIndicator="C",
                quantity="1",
                proceeds="0",
                ibCommission="0",
                netCash="0",
            ),
            _trade(
                symbol="ABC  260516C00210000",
                description="ABC 16MAY26 210 C",
                conid="3003",
                tradeID="TC6",
                transactionID="XC6",
                ibExecID="00014247.NONWHEEL2.03.01",
                tradeDate="20260501",
                dateTime="20260501;154500",
                expiry="20260516",
                strike="210",
                putCall="C",
                buySell="BUY",
                openCloseIndicator="C",
                quantity="1",
                proceeds="-840",
                ibCommission="-1",
                netCash="-841",
            ),
            _trade(
                symbol="ABC  260620C00215000",
                description="ABC 20JUN26 215 C",
                conid="3004",
                tradeID="TC7",
                transactionID="XC7",
                ibExecID="00014247.NONWHEEL2.02.01",
                tradeDate="20260501",
                dateTime="20260501;154600",
                expiry="20260620",
                strike="215",
                putCall="C",
                buySell="SELL",
                openCloseIndicator="O",
                quantity="-1",
                proceeds="946",
                ibCommission="-1",
                netCash="945",
            ),
        ]
    )
    holding_segments = [
        WheelHoldingSegment(
            ticker="ABC",
            start=pd.Timestamp("2026-03-01"),
            end=pd.Timestamp("2026-12-31"),
            shares=100,
            cost_per_share=237.5,
        )
    ]

    included, excluded, issues = wheel_option_executions(executions, holding_segments)

    assert [row.trade_id for row in included] == ["TC1", "TC5"]
    assert [row.trade_id for row in excluded] == ["TC2", "TC3", "TC4", "TC6", "TC7"]
    assert any("roll replacement" in issue and "non-wheel" in issue for issue in issues)


def test_ibkr_partial_buy_to_close_preserves_remaining_short_lot():
    open_put = _trade(quantity="-4", proceeds="1000", ibCommission="-4", netCash="996")
    close_two = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        tradeDate="20260116",
        dateTime="20260116;154500",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="2",
        tradePrice="1.00",
        proceeds="-200",
        ibCommission="-2",
        netCash="-202",
    )

    events, open_lots, stock_txns, issues, _ = _events_from_rows([open_put, close_two], as_of="2026-01-16")

    assert not issues
    assert not stock_txns
    assert len(events) == 1
    assert events[0].reason == "close"
    assert events[0].qty == 2
    assert events[0].pnl == pytest.approx(296.0)
    assert len(open_lots) == 1
    assert open_lots[0].qty == 2


def test_ibkr_roll_is_accounted_as_close_old_and_open_new():
    open_old = _trade()
    close_old = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        tradeDate="20260116",
        dateTime="20260116;154500",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-300",
        ibCommission="-1",
        netCash="-301",
    )
    open_new = _trade(
        tradeID="T3",
        transactionID="X3",
        ibExecID="E3",
        tradeDate="20260116",
        dateTime="20260116;154600",
        expiry="20260220",
        strike="95",
        quantity="-1",
        proceeds="450",
        ibCommission="-1",
        netCash="449",
    )

    events, open_lots, stock_txns, issues, _ = _events_from_rows([open_old, close_old, open_new], as_of="2026-01-31")

    assert not issues
    assert not stock_txns
    assert len(events) == 1
    assert events[0].reason == "close"
    assert events[0].pnl == pytest.approx(-52.0)
    assert len(open_lots) == 1
    assert open_lots[0].strike == 95.0
    assert open_lots[0].open_price == 4.49


def test_ibkr_pipeline_nets_same_day_roll_credit_on_close_date_without_double_counting_replacement():
    open_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeDate="20251101",
        dateTime="20251101;154500",
        expiry="20251219",
        strike="100",
        quantity="-1",
        proceeds="1000",
        ibCommission="-1",
        netCash="999",
    )
    close_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeID="T2",
        transactionID="X2",
        ibExecID="00014247.ROLLFIX.03.01",
        tradeDate="20251117",
        dateTime="20251117;154500",
        expiry="20251219",
        strike="100",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-5000",
        ibCommission="-1",
        netCash="-5001",
    )
    open_new = _trade(
        putCall="C",
        symbol="ABC  260220C00110000",
        description="ABC 20FEB26 110 C",
        tradeID="T3",
        transactionID="X3",
        ibExecID="00014247.ROLLFIX.02.01",
        tradeDate="20251117",
        dateTime="20251117;154600",
        expiry="20260220",
        strike="110",
        quantity="-1",
        proceeds="5300",
        ibCommission="-1",
        netCash="5299",
    )
    expire_new = _trade(
        putCall="C",
        symbol="ABC  260220C00110000",
        description="ABC 20FEB26 110 C",
        tradeID="T4",
        transactionID="X4",
        ibExecID="",
        tradeDate="20260220",
        dateTime="20260220;154500",
        expiry="20260220",
        strike="110",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="0",
        ibCommission="0",
        netCash="0",
    )
    report = _report({"Trade": [open_old, close_old, open_new, expire_new], "OptionEAE": [_stock_eae(date="20250101")]})

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-03-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    yearly = state.yearly.set_index("year")
    assert yearly.loc[2025, "realized_options_pnl"] == pytest.approx(1297.0)
    assert 2026 not in yearly.index or yearly.loc[2026, "realized_options_pnl"] == pytest.approx(0.0)
    assert [(event.date, event.pnl) for event in state.realized_option_events] == [
        (pd.Timestamp("2025-11-17"), pytest.approx(1297.0)),
        (pd.Timestamp("2026-02-20"), pytest.approx(0.0)),
    ]


def test_ibkr_pipeline_keeps_same_day_roll_replacement_open_with_zero_unrealized_premium():
    open_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeDate="20251101",
        dateTime="20251101;154500",
        expiry="20251219",
        strike="100",
        quantity="-1",
        proceeds="1000",
        ibCommission="-1",
        netCash="999",
    )
    close_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeID="T2",
        transactionID="X2",
        ibExecID="00014247.ROLLOPEN.03.01",
        tradeDate="20251117",
        dateTime="20251117;154500",
        expiry="20251219",
        strike="100",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-5000",
        ibCommission="-1",
        netCash="-5001",
    )
    open_new = _trade(
        putCall="C",
        symbol="ABC  260220C00110000",
        description="ABC 20FEB26 110 C",
        tradeID="T3",
        transactionID="X3",
        ibExecID="00014247.ROLLOPEN.02.01",
        tradeDate="20251117",
        dateTime="20251117;154600",
        expiry="20260220",
        strike="110",
        quantity="-1",
        proceeds="5300",
        ibCommission="-1",
        netCash="5299",
    )
    report = _report({"Trade": [open_old, close_old, open_new], "OptionEAE": [_stock_eae(date="20250101")]})

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2025-12-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert state.yearly.set_index("year").loc[2025, "realized_options_pnl"] == pytest.approx(1297.0)
    assert len(state.open_options) == 1
    open_row = state.open_options.iloc[0]
    assert open_row["ticker"] == "ABC"
    assert open_row["strike"] == 110.0
    assert open_row["open_price"] == pytest.approx(0.0)


def test_ibkr_pipeline_reports_non_rolled_option_on_close_or_expiration_year():
    open_put = _trade(
        tradeDate="20251220",
        dateTime="20251220;154500",
        expiry="20260117",
        quantity="-1",
        proceeds="300",
        ibCommission="-1",
        netCash="299",
    )

    state = build_ibkr_base_pipeline(
        _report({"Trade": [open_put]}),
        as_of=pd.Timestamp("2026-02-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    yearly = state.yearly.set_index("year")
    assert 2025 not in yearly.index or yearly.loc[2025, "realized_options_pnl"] == pytest.approx(0.0)
    assert yearly.loc[2026, "realized_options_pnl"] == pytest.approx(299.0)


def test_ibkr_pipeline_does_not_net_unrelated_same_day_close_and_open():
    open_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeDate="20251101",
        dateTime="20251101;154500",
        expiry="20251219",
        strike="100",
        quantity="-1",
        proceeds="1000",
        ibCommission="-1",
        netCash="999",
    )
    close_old = _trade(
        putCall="C",
        symbol="ABC  251219C00100000",
        description="ABC 19DEC25 100 C",
        tradeID="T2",
        transactionID="X2",
        ibExecID="00014247.UNRELATED_A.03.01",
        tradeDate="20251117",
        dateTime="20251117;154500",
        expiry="20251219",
        strike="100",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-5000",
        ibCommission="-1",
        netCash="-5001",
    )
    open_new = _trade(
        putCall="C",
        symbol="ABC  260220C00110000",
        description="ABC 20FEB26 110 C",
        tradeID="T3",
        transactionID="X3",
        ibExecID="00014247.UNRELATED_B.02.01",
        tradeDate="20251117",
        dateTime="20251117;154600",
        expiry="20260220",
        strike="110",
        quantity="-1",
        proceeds="5300",
        ibCommission="-1",
        netCash="5299",
    )
    report = _report({"Trade": [open_old, close_old, open_new], "OptionEAE": [_stock_eae(date="20250101")]})

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-03-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    yearly = state.yearly.set_index("year")
    assert yearly.loc[2025, "realized_options_pnl"] == pytest.approx(-4002.0)
    assert yearly.loc[2026, "realized_options_pnl"] == pytest.approx(5299.0)


def test_ibkr_roll_cashflow_counts_close_debit_and_new_open_credit_together():
    close_old = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        tradeDate="20251117",
        dateTime="20251117;154500",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-7708",
        ibCommission="-0.70",
        netCash="-7708.70",
        putCall="C",
        strike="210",
        expiry="20251219",
    )
    open_new = _trade(
        tradeID="T3",
        transactionID="X3",
        ibExecID="E3",
        tradeDate="20251117",
        dateTime="20251117;154600",
        buySell="SELL",
        openCloseIndicator="O",
        quantity="-1",
        proceeds="7610",
        ibCommission="-0.70",
        netCash="7609.30",
        putCall="C",
        strike="215",
        expiry="20260220",
    )

    executions = option_executions_from_rows([close_old, open_new])
    summary = cashflow_summary(executions)

    assert summary["rows"] == 2
    assert summary["buy_cash"] == pytest.approx(-7708.70)
    assert summary["sell_cash"] == pytest.approx(7609.30)
    assert summary["net_cash"] == pytest.approx(-99.40)
    assert summary["by_year"] == [
        {
            "year": 2025,
            "rows": 2,
            "sell_cash": pytest.approx(7609.30),
            "buy_cash": pytest.approx(-7708.70),
            "net_cash": pytest.approx(-99.40),
        }
    ]


def test_ibkr_cashflow_filter_uses_execution_date_not_expiration_date():
    old_year_open = _trade(
        tradeID="T1",
        transactionID="X1",
        ibExecID="E1",
        tradeDate="20241220",
        expiry="20250117",
        netCash="300",
    )
    current_year_close = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        tradeDate="20250103",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        expiry="20250117",
        proceeds="-100",
        netCash="-101",
    )

    executions = option_executions_from_rows([old_year_open, current_year_close])
    filtered = filter_executions(executions, since=pd.Timestamp("2025-01-01"), through=pd.Timestamp("2025-12-31"))

    assert len(filtered) == 1
    assert filtered[0].action == "Buy"
    assert cashflow_summary(filtered)["net_cash"] == pytest.approx(-101.0)


def test_ibkr_long_option_legs_are_excluded_from_short_strategy_adapter():
    long_open = _trade(buySell="BUY", openCloseIndicator="O", quantity="1", proceeds="-250", netCash="-251")
    long_close = _trade(
        tradeID="T2",
        transactionID="X2",
        ibExecID="E2",
        buySell="SELL",
        openCloseIndicator="C",
        quantity="-1",
        proceeds="300",
        netCash="299",
    )

    df = option_trades_to_dataframe([long_open, long_close])

    assert df.empty


def test_ibkr_manual_stock_trade_is_preserved_as_normalized_transaction_but_not_option_row():
    stock_trade = _trade(
        assetCategory="STK",
        symbol="ABC",
        underlyingSymbol="",
        description="ABC",
        putCall="",
        expiry="",
        strike="",
        buySell="BUY",
        openCloseIndicator="",
        quantity="100",
        multiplier="1",
        tradePrice="90",
        proceeds="-9000",
        netCash="-9001",
    )

    normalized = normalize_transactions([stock_trade])[0]
    df = option_trades_to_dataframe([stock_trade])

    assert normalized.asset_category == "STK"
    assert normalized.symbol == "ABC"
    assert normalized.quantity == 100.0
    assert df.empty


def test_ibkr_cash_transaction_has_stable_dedupe_key_for_future_dividend_layer():
    cash = IbkrRawRow(
        "CashTransaction",
        {
            "accountId": "U0000000",
            "transactionID": "C1",
            "actionID": "A1",
            "dateTime": "20260120;000000",
            "type": "Dividends",
            "amount": "25.00",
        },
    )

    assert dedupe_key(cash.section, cash.attrs) == "cash|U0000000|C1|A1|20260120;000000|Dividends"


def test_ibkr_stock_realized_uses_ibkr_fifo_realized_pnl_for_sells():
    stock_sell = _trade(
        assetCategory="STK",
        symbol="ABC",
        underlyingSymbol="",
        description="ABC",
        putCall="",
        expiry="",
        strike="",
        buySell="SELL",
        openCloseIndicator="C",
        quantity="-100",
        multiplier="1",
        tradePrice="110",
        proceeds="11000",
        ibCommission="-1",
        netCash="10999",
        fifoPnlRealized="998.50",
    )

    rows = stock_realized_from_rows([stock_sell])

    assert len(rows) == 1
    assert rows[0].ticker == "ABC"
    assert rows[0].quantity == 100
    assert rows[0].realized_pnl == pytest.approx(998.50)


def test_ibkr_cashflows_preserve_net_dividend_components():
    rows = cashflows_from_rows(
        [
            IbkrRawRow(
                "CashTransaction",
                {
                    "dateTime": "20260120;120000",
                    "symbol": "ABC",
                    "amount": "100",
                    "type": "Dividends",
                    "transactionID": "D1",
                    "actionID": "AD1",
                },
            ),
            IbkrRawRow(
                "CashTransaction",
                {
                    "dateTime": "20260120;120000",
                    "symbol": "ABC",
                    "amount": "-15",
                    "type": "Withholding Tax",
                    "transactionID": "D2",
                    "actionID": "AD2",
                },
            ),
        ]
    )

    assert [row.amount for row in rows] == [100.0, -15.0]


def test_ibkr_yearly_performance_combines_option_cash_stock_realized_and_net_dividends():
    from portfolio_backend.ibkr.flex_parser import IbkrFlexReport

    option_sell = _trade()
    assigned_buy = IbkrRawRow(
        "OptionEAE",
        {
            "date": "20260117",
            "assetCategory": "STK",
            "symbol": "ABC",
            "transactionType": "Buy",
            "quantity": "100",
            "proceeds": "-10000",
            "tradeID": "SB1",
        },
    )
    assigned_sell = IbkrRawRow(
        "OptionEAE",
        {
            "date": "20260131",
            "assetCategory": "STK",
            "symbol": "ABC",
            "transactionType": "Sell",
            "quantity": "-100",
            "proceeds": "11000",
            "tradeID": "SS1",
        },
    )
    unrelated_stock_sell = _trade(
        assetCategory="STK",
        symbol="XYZ",
        underlyingSymbol="",
        description="XYZ",
        putCall="",
        expiry="",
        strike="",
        buySell="SELL",
        openCloseIndicator="C",
        quantity="-100",
        multiplier="1",
        tradePrice="110",
        proceeds="11000",
        ibCommission="-1",
        netCash="10999",
        fifoPnlRealized="998.50",
    )
    dividend = IbkrRawRow(
        "CashTransaction",
        {
            "dateTime": "20260120;120000",
            "symbol": "ABC",
            "amount": "85",
            "type": "Dividends",
            "transactionID": "D1",
            "actionID": "AD1",
        },
    )
    report = IbkrFlexReport(
        root_tag="Test",
        metadata={},
        rows_by_section={
            "Trade": [option_sell, unrelated_stock_sell],
            "OptionEAE": [assigned_buy, assigned_sell],
            "CashTransaction": [dividend],
        },
        section_counts={},
    )

    yearly = yearly_performance_from_report(report)
    row = yearly.iloc[0]

    assert row["year"] == 2026
    assert row["option_cashflow_pnl"] == pytest.approx(249.0)
    assert row["stock_realized_pnl"] == pytest.approx(1000.0)
    assert row["dividends_net"] == pytest.approx(85.0)
    assert row["realized_strategy_cash_pnl"] == pytest.approx(1334.0)


def test_ibkr_yearly_performance_excludes_call_without_prior_put_assignment_inventory():
    from portfolio_backend.ibkr.flex_parser import IbkrFlexReport

    covered_call = _trade(
        symbol="ABC  260117C00110000",
        description="ABC 17JAN26 110 C",
        conid="2001",
        strike="110",
        putCall="C",
        quantity="-1",
        tradePrice="1.25",
        proceeds="125",
        ibCommission="-1",
        netCash="124",
    )
    report = IbkrFlexReport(
        root_tag="Test",
        metadata={},
        rows_by_section={"Trade": [covered_call], "OptionEAE": [], "CashTransaction": []},
        section_counts={},
    )

    yearly = yearly_performance_from_report(report)

    assert yearly.empty
    assert yearly.attrs["option_issues"] == [
        "Excluded ABC call execution on 2026-01-15 because no prior put-assignment stock inventory was held."
    ]


def test_ibkr_wheel_option_executions_include_calls_only_against_assignment_inventory():
    assigned_segment = [
        # 200 assigned shares, so two contracts are legitimate wheel calls.
        WheelHoldingSegment(
            ticker="ABC",
            start=pd.Timestamp("2026-01-10"),
            end=pd.Timestamp("2026-02-20"),
            shares=200,
            cost_per_share=100,
        )
    ]
    put = option_executions_from_rows([_trade(putCall="P")])[0]
    call = option_executions_from_rows(
        [
            _trade(
                tradeID="T2",
                transactionID="X2",
                ibExecID="E2",
                putCall="C",
                strike="110",
                quantity="-2",
                proceeds="240",
                netCash="238",
            )
        ]
    )[0]

    included, excluded, issues = wheel_option_executions([put, call], assigned_segment)

    assert [row.otype for row in included] == ["Put", "Call"]
    assert not excluded
    assert not issues
