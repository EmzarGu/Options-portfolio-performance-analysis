from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Callable, List

import pandas as pd

if TYPE_CHECKING:
    from streamlit_app import HoldSeg, StockTxn


def collect_dividend_cashflows(
    stock_txns: List["StockTxn"],
    as_of: pd.Timestamp,
    build_holding_segments: Callable[[List["StockTxn"], pd.Timestamp], List["HoldSeg"]],
    yf_module,
) -> pd.DataFrame:
    if yf_module is None:
        return pd.DataFrame()
    try:
        segs = build_holding_segments(stock_txns, as_of)
        if not segs:
            return pd.DataFrame()

        by_ticker = defaultdict(list)
        for seg in segs:
            by_ticker[seg.ticker].append(
                (
                    pd.to_datetime(seg.start).normalize(),
                    pd.to_datetime(seg.end).normalize(),
                    seg.shares,
                )
            )

        div_rows = []
        for ticker, seg_list in by_ticker.items():
            try:
                raw_div_hist = yf_module.Ticker(ticker).dividends
                if raw_div_hist is None or raw_div_hist.empty:
                    continue

                # yfinance may return dividends as either:
                # - Series (historical behavior), or
                # - DataFrame with a "Dividends" column (newer behavior).
                if isinstance(raw_div_hist, pd.DataFrame):
                    if "Dividends" in raw_div_hist.columns:
                        div_hist = raw_div_hist["Dividends"]
                    else:
                        numeric_cols = raw_div_hist.select_dtypes(include="number").columns
                        if len(numeric_cols) == 0:
                            continue
                        div_hist = raw_div_hist[numeric_cols[0]]
                else:
                    div_hist = raw_div_hist

                if div_hist is None or div_hist.empty:
                    continue

                div_hist = div_hist.dropna()
                if div_hist.empty:
                    continue

                dt_index = pd.to_datetime(div_hist.index, errors="coerce", utc=True)
                dt_index = dt_index.tz_localize(None).normalize()
                div_hist.index = dt_index
                div_hist = div_hist[div_hist.index.notna()]
                if div_hist.empty:
                    continue

                for start, end, shares in seg_list:
                    divs_in_period = div_hist[(div_hist.index >= start) & (div_hist.index < end)]
                    for pay_date, per_share in divs_in_period.items():
                        if pd.isna(pay_date) or pd.isna(per_share):
                            continue
                        per_share = float(per_share)
                        div_rows.append(
                            {
                                "ticker": ticker,
                                "ex_date": pay_date,
                                "pay_date": pay_date,
                                "per_share": per_share,
                                "shares": shares,
                                "cash": per_share * shares,
                            }
                        )
            except Exception:
                continue
        return pd.DataFrame(div_rows)
    except Exception:
        return pd.DataFrame()
