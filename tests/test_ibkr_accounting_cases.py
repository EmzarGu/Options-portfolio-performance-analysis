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
from portfolio_backend.ibkr.pipeline import build_ibkr_base_pipeline, build_ibkr_pipeline, wheel_stock_transactions_from_report
from portfolio_backend.ibkr.pipeline import wheel_options_dataframe_from_report
from portfolio_backend.mobile_payloads import build_monthly_performance_rows


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


def _call_assignment_eae(**overrides) -> IbkrRawRow:
    attrs = {
        "symbol": "ABC  260117C00100000",
        "description": "ABC 17JAN26 100 C",
        "conid": "2001",
        "putCall": "C",
    }
    attrs.update(overrides)
    return _option_eae(**attrs)


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
                _option_eae(date="20260117", quantity="-1", strike="100", putCall="P"),
                _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000"),
                _call_assignment_eae(date="20260220", quantity="-1", strike="110", expiry="20260220"),
                _stock_eae(
                    tradeID="S2",
                    date="20260220",
                    transactionType="Sell",
                    quantity="-100",
                    tradePrice="110",
                    proceeds="11000",
                ),
                _call_assignment_eae(
                    symbol="XYZ  260220C00050000",
                    underlyingSymbol="XYZ",
                    description="XYZ 20FEB26 50 C",
                    conid="9001",
                    tradeID="TCXYZ",
                    date="20260220",
                    quantity="-1",
                    strike="50",
                    expiry="20260220",
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


def test_ibkr_stock_side_buy_without_put_assignment_does_not_seed_wheel_call_inventory():
    covered_call = _trade(
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
        quantity="-1",
        tradePrice="1.00",
        proceeds="100",
        ibCommission="-1",
        netCash="99",
    )
    report = _report(
        {
            "Trade": [covered_call],
            "OptionEAE": [
                # This could be a long-option exercise or other non-put stock receipt.
                # It must not make later calls eligible for wheel P&L.
                _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000")
            ],
        }
    )

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-01-31").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert state.stock_txns == []
    assert state.ending_inventory == []
    assert state.open_options.empty
    assert state.realized_option_events == []
    assert any(
        issue == "Excluded ABC call execution on 2026-01-20 because no prior put-assignment stock inventory was held."
        for issue in state.issues
    )


def test_ibkr_partial_covered_call_keeps_uncovered_assigned_stock_inventory_open():
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

    state = build_ibkr_base_pipeline(
        _report({"Trade": [put_open, call_open], "OptionEAE": [put_assignment, assigned_buy]}),
        as_of=pd.Timestamp("2026-02-15").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(txn.ticker, txn.side, txn.shares, txn.price) for txn in state.stock_txns] == [
        ("EMN", "BUY", 400, 70.0)
    ]
    assert state.realized_sales == []
    assert [(lot.ticker, lot.shares_remaining, lot.cost_per_share) for lot in state.ending_inventory] == [
        ("EMN", 400, 70.0)
    ]
    assert [(row["ticker"], row["type"], row["qty"], row["strike"], row["open_price"]) for _, row in state.open_options.iterrows()] == [
        ("EMN", "Call", 2, 75.0, pytest.approx(1.49))
    ]
    assert [(event.ticker, event.otype, event.qty, event.pnl, event.reason) for event in state.realized_option_events] == [
        ("EMN", "Put", 4, pytest.approx(796.0), "assignment")
    ]
    assert not [issue for issue in state.issues if "EMN call" in issue or "assigned-call sold shares of EMN" in issue]


def test_ibkr_multiple_calls_can_share_one_prior_assigned_put_inventory_pool():
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
    call_70_a = _trade(
        symbol="EMN  260320C00070000",
        underlyingSymbol="EMN",
        description="EMN 20MAR26 70 C",
        conid="7501",
        tradeID="TC1",
        transactionID="XC1",
        ibExecID="EC1",
        tradeDate="20260201",
        dateTime="20260201;154500",
        expiry="20260320",
        strike="70",
        putCall="C",
        quantity="-1",
        tradePrice="1.00",
        proceeds="100",
        ibCommission="-1",
        netCash="99",
    )
    call_70_b = _trade(
        symbol="EMN  260320C00070000",
        underlyingSymbol="EMN",
        description="EMN 20MAR26 70 C",
        conid="7501",
        tradeID="TC2",
        transactionID="XC2",
        ibExecID="EC2",
        tradeDate="20260202",
        dateTime="20260202;154500",
        expiry="20260320",
        strike="70",
        putCall="C",
        quantity="-1",
        tradePrice="1.10",
        proceeds="110",
        ibCommission="-1",
        netCash="109",
    )
    call_75 = _trade(
        symbol="EMN  260320C00075000",
        underlyingSymbol="EMN",
        description="EMN 20MAR26 75 C",
        conid="7502",
        tradeID="TC3",
        transactionID="XC3",
        ibExecID="EC3",
        tradeDate="20260203",
        dateTime="20260203;154500",
        expiry="20260320",
        strike="75",
        putCall="C",
        quantity="-2",
        tradePrice="1.50",
        proceeds="300",
        ibCommission="-2",
        netCash="298",
    )

    state = build_ibkr_base_pipeline(
        _report({"Trade": [put_open, call_70_a, call_70_b, call_75], "OptionEAE": [put_assignment, assigned_buy]}),
        as_of=pd.Timestamp("2026-02-15").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(row["ticker"], row["type"], row["strike"], row["qty"]) for _, row in state.open_options.iterrows()] == [
        ("EMN", "Call", 70.0, 2),
        ("EMN", "Call", 75.0, 2),
    ]
    assert sum(row["qty"] for _, row in state.open_options.iterrows()) == 4
    assert [(lot.ticker, lot.shares_remaining, lot.cost_per_share) for lot in state.ending_inventory] == [
        ("EMN", 400, 70.0)
    ]
    assert not [issue for issue in state.issues if "EMN call" in issue]


def test_ibkr_partial_call_assignment_sells_only_assigned_quantity_and_keeps_remaining_inventory():
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
    call_assignment = _option_eae(
        symbol="EMN  260320C00075000",
        underlyingSymbol="EMN",
        description="EMN 20MAR26 75 C",
        conid="7501",
        tradeID="TC1",
        date="20260320",
        expiry="20260320",
        strike="75",
        putCall="C",
        quantity="-2",
        realizedPnl="298",
    )
    assigned_sell = _stock_eae(
        symbol="EMN",
        tradeID="SC1",
        date="20260320",
        transactionType="Sell",
        quantity="-200",
        tradePrice="75",
        proceeds="15000",
    )

    state = build_ibkr_base_pipeline(
        _report(
            {
                "Trade": [put_open, call_open],
                "OptionEAE": [put_assignment, assigned_buy, call_assignment, assigned_sell],
            }
        ),
        as_of=pd.Timestamp("2026-04-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(txn.ticker, txn.side, txn.shares, txn.price) for txn in state.stock_txns] == [
        ("EMN", "BUY", 400, 70.0),
        ("EMN", "SELL", 200, 75.0),
    ]
    assert [(sale.ticker, sale.shares, sale.proceeds, sale.cost, sale.pnl) for sale in state.realized_sales] == [
        ("EMN", 200, 15000.0, 14000.0, 1000.0)
    ]
    assert [(lot.ticker, lot.shares_remaining, lot.cost_per_share) for lot in state.ending_inventory] == [
        ("EMN", 200, 70.0)
    ]
    assert state.open_options.empty
    assert [(event.ticker, event.otype, event.qty, event.pnl, event.reason) for event in state.realized_option_events] == [
        ("EMN", "Put", 4, pytest.approx(796.0), "assignment"),
        ("EMN", "Call", 2, pytest.approx(298.0), "assignment"),
    ]
    assert not [issue for issue in state.issues if "EMN call" in issue or "assigned-call sold shares of EMN" in issue]


def test_ibkr_same_day_covered_call_roll_replacement_reuses_released_assignment_inventory():
    put_open = _trade(
        symbol="FTNT  250815P00097500",
        underlyingSymbol="FTNT",
        description="FTNT 15AUG25 97.5 P",
        conid="FTNT-PUT",
        tradeID="FTNT-PUT-OPEN",
        transactionID="FTNT-PUT-X1",
        ibExecID="FTNT-PUT-E1",
        tradeDate="20250721",
        dateTime="20250721;154500",
        expiry="20250815",
        strike="97.5",
        putCall="P",
        quantity="-1",
        tradePrice="2.25",
        proceeds="225",
        ibCommission="-1",
        netCash="224",
    )
    put_assignment = _option_eae(
        symbol="FTNT  250815P00097500",
        underlyingSymbol="FTNT",
        description="FTNT 15AUG25 97.5 P",
        conid="FTNT-PUT",
        tradeID="FTNT-PUT-OPEN",
        date="20250808",
        expiry="20250815",
        strike="97.5",
        putCall="P",
        quantity="-1",
        realizedPnl="224",
    )
    assigned_buy = _stock_eae(
        symbol="FTNT",
        tradeID="FTNT-STOCK-BUY",
        date="20250808",
        transactionType="Buy",
        quantity="100",
        tradePrice="97.5",
        proceeds="-9750",
    )
    call_open_85 = _trade(
        symbol="FTNT  260220C00085000",
        underlyingSymbol="FTNT",
        description="FTNT 20FEB26 85 C",
        conid="FTNT-C85-FEB",
        tradeID="FTNT-CALL-1",
        transactionID="FTNT-XC1",
        ibExecID="FTNT-CALL-E1",
        tradeDate="20260203",
        dateTime="20260203;154500",
        expiry="20260220",
        strike="85",
        putCall="C",
        quantity="-1",
        tradePrice="1.94",
        proceeds="194",
        ibCommission="-1",
        netCash="193",
    )
    close_85 = _trade(
        symbol="FTNT  260220C00085000",
        underlyingSymbol="FTNT",
        description="FTNT 20FEB26 85 C",
        conid="FTNT-C85-FEB",
        tradeID="FTNT-CALL-2",
        transactionID="FTNT-XC2",
        ibExecID="00014247.FTNT1.02.01",
        tradeDate="20260209",
        dateTime="20260209;154500",
        expiry="20260220",
        strike="85",
        putCall="C",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        tradePrice="2.36",
        proceeds="-236",
        ibCommission="-1",
        netCash="-237",
    )
    open_87_5 = _trade(
        symbol="FTNT  260320C00087500",
        underlyingSymbol="FTNT",
        description="FTNT 20MAR26 87.5 C",
        conid="FTNT-C875-MAR",
        tradeID="FTNT-CALL-3",
        transactionID="FTNT-XC3",
        ibExecID="00014247.FTNT1.03.01",
        tradeDate="20260209",
        dateTime="20260209;154600",
        expiry="20260320",
        strike="87.5",
        putCall="C",
        quantity="-1",
        tradePrice="2.96",
        proceeds="296",
        ibCommission="-1",
        netCash="295",
    )
    close_87_5 = _trade(
        symbol="FTNT  260320C00087500",
        underlyingSymbol="FTNT",
        description="FTNT 20MAR26 87.5 C",
        conid="FTNT-C875-MAR",
        tradeID="FTNT-CALL-4",
        transactionID="FTNT-XC4",
        ibExecID="0000f84c.FTNT2.03.01",
        tradeDate="20260211",
        dateTime="20260211;154500",
        expiry="20260320",
        strike="87.5",
        putCall="C",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        tradePrice="4.01",
        proceeds="-401",
        ibCommission="-1",
        netCash="-402",
    )
    open_100 = _trade(
        symbol="FTNT  260717C00100000",
        underlyingSymbol="FTNT",
        description="FTNT 17JUL26 100 C",
        conid="FTNT-C100-JUL",
        tradeID="FTNT-CALL-5",
        transactionID="FTNT-XC5",
        ibExecID="0000f84c.FTNT2.02.01",
        tradeDate="20260211",
        dateTime="20260211;154600",
        expiry="20260717",
        strike="100",
        putCall="C",
        quantity="-1",
        tradePrice="4.34",
        proceeds="434",
        ibCommission="-1",
        netCash="433",
    )
    open_85_may = _trade(
        symbol="FTNT  260515C00085000",
        underlyingSymbol="FTNT",
        description="FTNT 15MAY26 85 C",
        conid="FTNT-C85-MAY",
        tradeID="FTNT-CALL-7",
        transactionID="FTNT-XC7",
        ibExecID="00014247.FTNT3.03.01",
        tradeDate="20260223",
        dateTime="20260223;154600",
        expiry="20260515",
        strike="85",
        putCall="C",
        quantity="-1",
        tradePrice="3.13",
        proceeds="313",
        ibCommission="-1",
        netCash="312",
    )
    close_100 = _trade(
        symbol="FTNT  260717C00100000",
        underlyingSymbol="FTNT",
        description="FTNT 17JUL26 100 C",
        conid="FTNT-C100-JUL",
        tradeID="FTNT-CALL-6",
        transactionID="FTNT-XC6",
        ibExecID="00014247.FTNT3.02.01",
        tradeDate="20260223",
        dateTime="20260223;154500",
        expiry="20260717",
        strike="100",
        putCall="C",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        tradePrice="1.57",
        proceeds="-157",
        ibCommission="-1",
        netCash="-158",
    )
    close_85_may = _trade(
        symbol="FTNT  260515C00085000",
        underlyingSymbol="FTNT",
        description="FTNT 15MAY26 85 C",
        conid="FTNT-C85-MAY",
        tradeID="FTNT-CALL-8",
        transactionID="FTNT-XC8",
        ibExecID="0000f84c.FTNT4.02.01",
        tradeDate="20260505",
        dateTime="20260505;154500",
        expiry="20260515",
        strike="85",
        putCall="C",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        tradePrice="7.46",
        proceeds="-746",
        ibCommission="-1",
        netCash="-747",
    )
    open_95_sep = _trade(
        symbol="FTNT  260918C00095000",
        underlyingSymbol="FTNT",
        description="FTNT 18SEP26 95 C",
        conid="FTNT-C95-SEP",
        tradeID="FTNT-CALL-9",
        transactionID="FTNT-XC9",
        ibExecID="0000f84c.FTNT4.03.01",
        tradeDate="20260505",
        dateTime="20260505;154600",
        expiry="20260918",
        strike="95",
        putCall="C",
        quantity="-1",
        tradePrice="8.17",
        proceeds="817",
        ibCommission="-1",
        netCash="816",
    )

    def current_prices(tickers):
        return {ticker: 114.07 for ticker in tickers}, [], {"requested": len(tickers), "fetched": len(tickers)}

    state = build_ibkr_pipeline(
        _report(
            {
                "Trade": [
                    put_open,
                    call_open_85,
                    close_85,
                    open_87_5,
                    close_87_5,
                    open_100,
                    # Intentionally listed sell/open before buy/close by strike/expiry
                    # to reproduce the raw IBKR ordering that previously broke FTNT.
                    open_85_may,
                    close_100,
                    close_85_may,
                    open_95_sep,
                ],
                "OptionEAE": [put_assignment, assigned_buy],
            }
        ),
        as_of=pd.Timestamp("2026-05-10").date(),
        include_unrealized_current_year=True,
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
        fetch_current_prices_fn=current_prices,
    )

    assert [(row["ticker"], row["type"], row["strike"], row["expiration"], row["qty"]) for _, row in state.open_options.iterrows()] == [
        ("FTNT", "Call", 95.0, pd.Timestamp("2026-09-18"), 1)
    ]
    ftnt_inventory = state.inv_df.loc[state.inv_df["ticker"].eq("FTNT")].iloc[0]
    assert ftnt_inventory["shares"] == 100
    assert ftnt_inventory["cost_per_share"] == 97.5
    assert ftnt_inventory["covered_shares"] == 100
    assert ftnt_inventory["covered_strike"] == 95.0
    assert ftnt_inventory["unrealized_pnl"] == pytest.approx(-250.0)
    ftnt_total = state.per_ticker_totals.loc[state.per_ticker_totals["ticker"].eq("FTNT")].iloc[0]
    assert ftnt_total["unrealized_pnl"] == pytest.approx(566.0)
    assert not [issue for issue in state.issues if "FTNT call" in issue]


def test_ibkr_assignment_book_trade_stock_sell_is_not_double_counted():
    put_open = _trade(
        symbol="ASAN  221216P00015000",
        underlyingSymbol="ASAN",
        description="ASAN 16DEC22 15 P",
        conid="ASAN-PUT",
        tradeID="ASAN-PUT-OPEN",
        transactionID="ASAN-XP1",
        ibExecID="ASAN-EP1",
        tradeDate="20221201",
        dateTime="20221201;154500",
        expiry="20221216",
        strike="15",
        putCall="P",
        quantity="-10",
        tradePrice="1.00",
        proceeds="1000",
        ibCommission="0",
        netCash="1000",
    )
    put_assignment = _option_eae(
        symbol="ASAN  221216P00015000",
        underlyingSymbol="ASAN",
        description="ASAN 16DEC22 15 P",
        conid="ASAN-PUT",
        tradeID="ASAN-PUT-ASSIGN",
        date="20221216",
        expiry="20221216",
        strike="15",
        putCall="P",
        quantity="-10",
        multiplier="100",
        realizedPnl="1000",
    )
    assigned_buy = _stock_eae(
        symbol="ASAN",
        tradeID="ASAN-STOCK-BUY",
        date="20221216",
        transactionType="Buy",
        quantity="1000",
        tradePrice="15",
        proceeds="-15000",
    )
    assigned_call = _call_assignment_eae(
        symbol="ASAN  230217C00015000",
        underlyingSymbol="ASAN",
        description="ASAN 17FEB23 15 C",
        conid="ASAN-CALL-1",
        tradeID="ASAN-CALL-ASSIGN",
        date="20230217",
        expiry="20230217",
        strike="15",
        putCall="C",
        quantity="4",
        multiplier="100",
        realizedPnl="0",
    )
    assigned_sell = _stock_eae(
        symbol="ASAN",
        tradeID="ASAN-STOCK-SELL",
        date="20230217",
        transactionType="Sell",
        quantity="-400",
        tradePrice="15",
        proceeds="6000",
    )
    duplicate_book_trade_sell = _trade(
        assetCategory="STK",
        symbol="ASAN",
        underlyingSymbol="",
        description="ASAN",
        conid="ASAN-STOCK",
        tradeID="ASAN-STOCK-SELL",
        transactionID="ASAN-STOCK-TXN",
        ibExecID="",
        tradeDate="20230217",
        dateTime="20230217;162000",
        expiry="",
        strike="",
        putCall="",
        buySell="SELL",
        openCloseIndicator="C",
        transactionType="BookTrade",
        quantity="-400",
        multiplier="1",
        tradePrice="15",
        proceeds="6000",
        ibCommission="0",
        netCash="6000",
        notes="A",
    )
    later_call_open = _trade(
        symbol="ASAN  230317C00016000",
        underlyingSymbol="ASAN",
        description="ASAN 17MAR23 16 C",
        conid="ASAN-CALL-2",
        tradeID="ASAN-CALL-OPEN-2",
        transactionID="ASAN-XC2",
        ibExecID="ASAN-EC2",
        tradeDate="20230221",
        dateTime="20230221;154500",
        expiry="20230317",
        strike="16",
        putCall="C",
        quantity="-6",
        tradePrice="0.50",
        proceeds="300",
        ibCommission="0",
        netCash="300",
    )

    state = build_ibkr_base_pipeline(
        _report(
            {
                "Trade": [put_open, duplicate_book_trade_sell, later_call_open],
                "OptionEAE": [put_assignment, assigned_buy, assigned_call, assigned_sell],
            }
        ),
        as_of=pd.Timestamp("2023-03-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(txn.ticker, txn.side, txn.shares, txn.price, txn.source) for txn in state.stock_txns] == [
        ("ASAN", "BUY", 1000, 15.0, "Assigned Put"),
        ("ASAN", "SELL", 400, 15.0, "Assigned Call"),
    ]
    assert [(lot.ticker, lot.shares_remaining, lot.cost_per_share) for lot in state.ending_inventory] == [
        ("ASAN", 600, 15.0)
    ]
    assert [(row["ticker"], row["type"], row["strike"], row["qty"]) for _, row in state.open_options.iterrows()] == [
        ("ASAN", "Call", 16.0, 6)
    ]
    assert not [issue for issue in state.issues if "ASAN call" in issue or "assigned-call sold shares of ASAN" in issue]


def test_ibkr_manual_stock_sell_consumes_assignment_inventory_only():
    put_open = _trade(
        symbol="AAPL  260117P00100000",
        underlyingSymbol="AAPL",
        description="AAPL 17JAN26 100 P",
        conid="8001",
        tradeID="AAPL-PUT-OPEN",
        transactionID="AAPL-X1",
        ibExecID="AAPL-E1",
        tradeDate="20260110",
        dateTime="20260110;154500",
        expiry="20260117",
        strike="100",
        putCall="P",
        quantity="-1",
        proceeds="250",
        ibCommission="-1",
        netCash="249",
    )
    put_assignment = _option_eae(
        symbol="AAPL  260117P00100000",
        underlyingSymbol="AAPL",
        description="AAPL 17JAN26 100 P",
        conid="8001",
        tradeID="AAPL-PUT-OPEN",
        date="20260117",
        expiry="20260117",
        strike="100",
        putCall="P",
        quantity="-1",
        multiplier="100",
    )
    assigned_buy = _stock_eae(
        symbol="AAPL",
        tradeID="AAPL-STOCK-BUY",
        date="20260117",
        transactionType="Buy",
        quantity="100",
        tradePrice="100",
        proceeds="-10000",
    )
    manual_sell = _trade(
        assetCategory="STK",
        symbol="AAPL",
        underlyingSymbol="",
        description="AAPL",
        conid="9001",
        tradeID="AAPL-STOCK-SELL",
        transactionID="AAPL-SX1",
        ibExecID="AAPL-SE1",
        tradeDate="20260210",
        dateTime="20260210;154500",
        expiry="",
        strike="",
        putCall="",
        buySell="SELL",
        openCloseIndicator="",
        quantity="-100",
        multiplier="1",
        tradePrice="120",
        proceeds="12000",
        ibCommission="0",
        netCash="12000",
    )
    unrelated_sell = _trade(
        assetCategory="STK",
        symbol="MSFT",
        underlyingSymbol="",
        description="MSFT",
        conid="9002",
        tradeID="MSFT-STOCK-SELL",
        transactionID="MSFT-SX1",
        ibExecID="MSFT-SE1",
        tradeDate="20260210",
        dateTime="20260210;154500",
        expiry="",
        strike="",
        putCall="",
        buySell="SELL",
        openCloseIndicator="",
        quantity="-100",
        multiplier="1",
        tradePrice="300",
        proceeds="30000",
        ibCommission="0",
        netCash="30000",
    )

    report = _report(
        {
            "Trade": [put_open, manual_sell, unrelated_sell],
            "OptionEAE": [put_assignment, assigned_buy],
        }
    )

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-02-28").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(txn.ticker, txn.side, txn.shares, txn.price, txn.source) for txn in state.stock_txns] == [
        ("AAPL", "BUY", 100, 100.0, "Assigned Put"),
        ("AAPL", "SELL", 100, 120.0, "Manual Stock Sell"),
    ]
    assert [(sale.ticker, sale.shares, sale.proceeds, sale.cost, sale.pnl, sale.source) for sale in state.realized_sales] == [
        ("AAPL", 100, 12000.0, 10000.0, 2000.0, "Manual Stock Sell")
    ]
    assert not state.ending_inventory
    assert not [
        issue
        for issue in state.issues
        if "MSFT" in issue or "manually sold shares" in issue or "assigned-call sold shares of AAPL" in issue
    ]


def test_ibkr_wheel_covered_call_expiration_realizes_premium_and_keeps_stock():
    put_open = _trade(
        symbol="ABC  260117P00100000",
        description="ABC 17JAN26 100 P",
        expiry="20260117",
        strike="100",
        putCall="P",
        quantity="-1",
        proceeds="250",
        ibCommission="-1",
        netCash="249",
    )
    put_assignment = _option_eae(date="20260117", expiry="20260117", strike="100", putCall="P", quantity="-1")
    assigned_buy = _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000")
    call_open = _trade(
        symbol="ABC  260220C00110000",
        description="ABC 20FEB26 110 C",
        conid="2001",
        tradeID="CALL-OPEN",
        transactionID="CALL-X1",
        ibExecID="CALL-E1",
        tradeDate="20260120",
        dateTime="20260120;154500",
        expiry="20260220",
        strike="110",
        putCall="C",
        quantity="-1",
        proceeds="150",
        ibCommission="-1",
        netCash="149",
    )

    state = build_ibkr_base_pipeline(
        _report({"Trade": [put_open, call_open], "OptionEAE": [put_assignment, assigned_buy]}),
        as_of=pd.Timestamp("2026-02-28").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(event.ticker, event.otype, event.qty, event.pnl, event.reason) for event in state.realized_option_events] == [
        ("ABC", "Put", 1, pytest.approx(249.0), "assignment"),
        ("ABC", "Call", 1, pytest.approx(149.0), "expiration"),
    ]
    assert [(lot.ticker, lot.shares_remaining, lot.cost_per_share) for lot in state.ending_inventory] == [
        ("ABC", 100, 100.0)
    ]


def test_ibkr_pipeline_keeps_put_roll_replacement_premium_open_until_expiration():
    open_old = _trade(
        putCall="P",
        symbol="ABC  251219P00100000",
        description="ABC 19DEC25 100 P",
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
        putCall="P",
        symbol="ABC  251219P00100000",
        description="ABC 19DEC25 100 P",
        tradeID="PUT-CLOSE",
        transactionID="PUT-X2",
        ibExecID="00014247.PUTROLL.03.01",
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
        putCall="P",
        symbol="ABC  260220P00090000",
        description="ABC 20FEB26 90 P",
        tradeID="PUT-OPEN-NEW",
        transactionID="PUT-X3",
        ibExecID="00014247.PUTROLL.02.01",
        tradeDate="20251117",
        dateTime="20251117;154600",
        expiry="20260220",
        strike="90",
        quantity="-1",
        proceeds="5300",
        ibCommission="-1",
        netCash="5299",
    )

    state = build_ibkr_base_pipeline(
        _report({"Trade": [open_old, close_old, open_new]}),
        as_of=pd.Timestamp("2026-03-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    yearly = state.yearly.set_index("year")
    assert yearly.loc[2025, "realized_options_pnl"] == pytest.approx(-4002.0)
    assert yearly.loc[2026, "realized_options_pnl"] == pytest.approx(5299.0)
    assert [(event.date, event.otype, event.pnl, event.reason) for event in state.realized_option_events] == [
        (pd.Timestamp("2025-11-17"), "Put", pytest.approx(-4002.0), "close"),
        (pd.Timestamp("2026-02-20"), "Put", pytest.approx(5299.0), "expiration"),
    ]


def test_ibkr_dividends_are_prorated_to_assignment_derived_shares():
    put_open = _trade()
    put_assignment = _option_eae(date="20260117", quantity="-1", strike="100", putCall="P")
    assigned_buy = _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000")
    dividend = IbkrRawRow(
        "CashTransaction",
        {
            "dateTime": "20260125;120000",
            "exDate": "20260124",
            "symbol": "ABC",
            "description": "ABC DIVIDEND USD 1.00 PER SHARE",
            "amount": "300",
            "type": "Dividends",
            "transactionID": "DIV-GROSS",
            "actionID": "DIV-ACTION",
        },
    )
    withholding = IbkrRawRow(
        "CashTransaction",
        {
            "dateTime": "20260125;120000",
            "exDate": "20260124",
            "symbol": "ABC",
            "amount": "-45",
            "type": "Withholding Tax",
            "transactionID": "DIV-WHT",
            "actionID": "DIV-ACTION",
        },
    )

    state = build_ibkr_base_pipeline(
        _report({"Trade": [put_open], "OptionEAE": [put_assignment, assigned_buy], "CashTransaction": [dividend, withholding]}),
        as_of=pd.Timestamp("2026-01-31").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert state.div_df[["ticker", "cash_type", "cash"]].to_dict("records") == [
        {"ticker": "ABC", "cash_type": "Dividends", "cash": pytest.approx(100.0)},
        {"ticker": "ABC", "cash_type": "Withholding Tax", "cash": pytest.approx(-15.0)},
    ]
    assert state.monthly_cycles.loc[pd.Timestamp("2026-01-31"), "dividends"] == pytest.approx(85.0)


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
                _option_eae(date="20260117", quantity="-1", strike="100", putCall="P"),
                _stock_eae(date="20260117", transactionType="Buy", quantity="100", tradePrice="100", proceeds="-10000"),
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


def test_ibkr_pipeline_keeps_call_roll_replacement_premium_open_until_expiration():
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
    report = _report(
        {
            "Trade": [open_old, close_old, open_new, expire_new],
            "OptionEAE": [_option_eae(date="20250101"), _stock_eae(date="20250101")],
        }
    )

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-03-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    yearly = state.yearly.set_index("year")
    assert yearly.loc[2025, "realized_options_pnl"] == pytest.approx(-4002.0)
    assert yearly.loc[2026, "realized_options_pnl"] == pytest.approx(5299.0)
    assert [(event.date, event.pnl) for event in state.realized_option_events] == [
        (pd.Timestamp("2025-11-17"), pytest.approx(-4002.0)),
        (pd.Timestamp("2026-02-20"), pytest.approx(5299.0)),
    ]


def test_ibkr_pipeline_keeps_same_day_roll_replacement_premium_open_until_close():
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
    report = _report(
        {
            "Trade": [open_old, close_old, open_new],
            "OptionEAE": [_option_eae(date="20250101"), _stock_eae(date="20250101")],
        }
    )

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2025-12-01").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert state.yearly.set_index("year").loc[2025, "realized_options_pnl"] == pytest.approx(-4002.0)
    assert len(state.open_options) == 1
    open_row = state.open_options.iloc[0]
    assert open_row["ticker"] == "ABC"
    assert open_row["strike"] == 110.0
    assert open_row["open_price"] == pytest.approx(52.99)


def test_ibkr_monthly_projection_reports_incremental_and_roll_adjusted_open_premium():
    open_old = _trade(
        underlyingSymbol="ZM",
        symbol="ZM   260515P00080000",
        description="ZM 15MAY26 80 P",
        tradeDate="20260410",
        dateTime="20260410;154500",
        expiry="20260515",
        strike="80",
        quantity="-1",
        proceeds="1000",
        ibCommission="-1",
        netCash="999",
    )
    close_old = _trade(
        underlyingSymbol="ZM",
        symbol="ZM   260515P00080000",
        description="ZM 15MAY26 80 P",
        tradeID="T2",
        transactionID="X2",
        ibExecID="00014247.ZMROLL.03.01",
        tradeDate="20260504",
        dateTime="20260504;154500",
        expiry="20260515",
        strike="80",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="1",
        proceeds="-600",
        ibCommission="-1",
        netCash="-601",
    )
    open_replacement = _trade(
        underlyingSymbol="ZM",
        symbol="ZM   260515P00075000",
        description="ZM 15MAY26 75 P",
        tradeID="T3",
        transactionID="X3",
        ibExecID="00014247.ZMROLL.02.01",
        tradeDate="20260504",
        dateTime="20260504;154600",
        expiry="20260515",
        strike="75",
        quantity="-1",
        proceeds="500",
        ibCommission="-1",
        netCash="499",
    )
    report = _report(
        {
            "Trade": [open_old, close_old, open_replacement],
            "OptionEAE": [_option_eae(date="20250101"), _stock_eae(date="20250101")],
        }
    )

    state = build_ibkr_base_pipeline(
        report,
        as_of=pd.Timestamp("2026-05-10").date(),
        fetch_price_history_fn=_empty_price_history,
        align_benchmarks_monthly_fn=_empty_benchmarks,
    )

    assert [(event.ticker, event.pnl) for event in state.realized_option_events] == [("ZM", pytest.approx(398.0))]
    assert len(state.open_options) == 1
    assert state.open_options.iloc[0]["ticker"] == "ZM"
    assert state.open_options.iloc[0]["open_price"] == pytest.approx(4.99)
    open_lots = [lot for lot in state.lots if lot.close_date is None]
    assert len(open_lots) == 1
    assert open_lots[0].roll_adjusted_open_price == pytest.approx(8.97)

    rows = build_monthly_performance_rows(state, target_return=0.015, monthly_range="ytd")
    may = next(row for row in rows if row["month"] == "2026-05-31")
    assert may["realized_options_pnl"] == pytest.approx(398.0)
    assert may["open_expiring_option_premium"] == pytest.approx(499.0)
    assert may["open_expiring_incremental_premium"] == pytest.approx(499.0)
    assert may["open_expiring_roll_adjusted_premium"] == pytest.approx(897.0)
    assert may["projected_month_pnl"] == pytest.approx(897.0)


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
    report = _report(
        {
            "Trade": [open_old, close_old, open_new],
            "OptionEAE": [_option_eae(date="20250101"), _stock_eae(date="20250101")],
        }
    )

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
            "OptionEAE": [
                _option_eae(date="20260117", quantity="-1", strike="100", putCall="P"),
                assigned_buy,
                _call_assignment_eae(date="20260131", quantity="-1", strike="110", expiry="20260131"),
                assigned_sell,
            ],
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


def test_ibkr_vertical_put_spread_is_excluded_from_wheel_put_pnl():
    long_put_open = _trade(
        symbol="SPY   230217P00350000",
        underlyingSymbol="SPY",
        description="SPY 17FEB23 350 P",
        conid="SPY-LONG-PUT",
        tradeID="SPY-LONG-OPEN",
        transactionID="SPY-LONG-X1",
        ibExecID="SPY-LONG-E1",
        tradeDate="20221228",
        dateTime="20221228;141554",
        expiry="20230217",
        strike="350",
        putCall="P",
        buySell="BUY",
        openCloseIndicator="O",
        quantity="10",
        tradePrice="3.30",
        proceeds="-3300",
        ibCommission="-10",
        netCash="-3310",
    )
    short_put_open = _trade(
        symbol="SPY   230217P00353000",
        underlyingSymbol="SPY",
        description="SPY 17FEB23 353 P",
        conid="SPY-SHORT-PUT",
        tradeID="SPY-SHORT-OPEN",
        transactionID="SPY-SHORT-X1",
        ibExecID="SPY-SHORT-E1",
        tradeDate="20221228",
        dateTime="20221228;141554",
        expiry="20230217",
        strike="353",
        putCall="P",
        buySell="SELL",
        openCloseIndicator="O",
        quantity="-10",
        tradePrice="3.83",
        proceeds="3830",
        ibCommission="-10",
        netCash="3820",
    )
    short_put_close = _trade(
        symbol="SPY   230217P00353000",
        underlyingSymbol="SPY",
        description="SPY 17FEB23 353 P",
        conid="SPY-SHORT-PUT",
        tradeID="SPY-SHORT-CLOSE",
        transactionID="SPY-SHORT-X2",
        ibExecID="SPY-SHORT-E2",
        tradeDate="20230110",
        dateTime="20230110;154607",
        expiry="20230217",
        strike="353",
        putCall="P",
        buySell="BUY",
        openCloseIndicator="C",
        quantity="10",
        tradePrice="1.27",
        proceeds="-1270",
        ibCommission="-10",
        netCash="-1280",
    )

    executions = option_executions_from_rows(
        [long_put_open, short_put_open, short_put_close],
        short_strategy_only=False,
    )
    included, excluded, issues = wheel_option_executions(executions, [])

    assert not included
    assert [row.trade_id for row in excluded] == ["SPY-LONG-OPEN", "SPY-SHORT-OPEN", "SPY-SHORT-CLOSE"]
    assert any("put spread contracts" in issue for issue in issues)
    assert any("put spread close contracts" in issue for issue in issues)
