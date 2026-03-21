from __future__ import annotations

from typing import TYPE_CHECKING, List

import pandas as pd

if TYPE_CHECKING:
    from streamlit_app import OptionLot


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
