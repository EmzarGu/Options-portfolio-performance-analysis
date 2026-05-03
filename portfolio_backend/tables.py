from __future__ import annotations

from typing import Dict, List, TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from portfolio_backend.models import OptionLot


def build_open_options_frame(open_option_lots: List["OptionLot"]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": lot.ticker,
                "type": lot.otype,
                "strike": lot.strike,
                "qty": lot.qty,
                "expiration": lot.expiration,
                "trans_date": lot.open_date,
                "open_price": lot.open_price,
            }
            for lot in open_option_lots
        ]
    )


def filter_df_to_range(df: pd.DataFrame, date_col: str, end: pd.Timestamp, range_choice: str) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return df
    end = pd.to_datetime(end)
    start = None
    if range_choice == "3M":
        start = end - pd.DateOffset(months=3)
    elif range_choice == "6M":
        start = end - pd.DateOffset(months=6)
    elif range_choice == "YTD":
        start = pd.Timestamp(end.year, 1, 1)
    elif range_choice == "1Y":
        start = end - pd.DateOffset(years=1)
    if start is not None:
        dates = pd.to_datetime(df[date_col])
        mask = (dates >= start) & (dates <= end)
        return df.loc[mask]
    return df


def build_assigned_holdings_frame(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory is None or inventory.empty:
        return pd.DataFrame() if inventory is None else inventory.copy()
    holdings = inventory.copy()
    if "source" in holdings.columns:
        holdings = holdings[holdings["source"].isin(["stock_lot", "put_gap"])]
    return holdings


def build_open_option_shorts_frame(open_options: pd.DataFrame, stock_prices: Dict[str, float]) -> pd.DataFrame:
    if open_options is None or open_options.empty:
        return pd.DataFrame() if open_options is None else open_options.copy()

    options = open_options.copy()
    options["current_price"] = options["ticker"].map(stock_prices or {})
    strike_num = pd.to_numeric(options["strike"], errors="coerce")
    current_num = pd.to_numeric(options["current_price"], errors="coerce")
    valid_moneyness = strike_num.notna() & current_num.notna() & (strike_num != 0)
    options["moneyness_pct"] = np.nan

    put_mask = (options["type"] == "Put") & valid_moneyness
    call_mask = (options["type"] == "Call") & valid_moneyness
    options.loc[put_mask, "moneyness_pct"] = (strike_num[put_mask] - current_num[put_mask]) / strike_num[put_mask]
    options.loc[call_mask, "moneyness_pct"] = (current_num[call_mask] - strike_num[call_mask]) / strike_num[call_mask]
    return options
