import pandas as pd

from streamlit_app import (
    CONTRACT_MULTIPLIER,
    OptionLot,
    OpenLot,
    build_capital_timeline,
    build_option_trades,
    calculate_unrealized_positions,
    process_option_positions,
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
