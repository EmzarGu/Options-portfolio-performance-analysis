from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

import pandas as pd

if TYPE_CHECKING:
    from streamlit_app import HoldSeg, StockTxn


DIVIDEND_COLUMNS = ["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]


@dataclass(frozen=True)
class DividendFetchResult:
    cashflows: pd.DataFrame
    coverage_complete: bool
    attempted_tickers: List[str]
    failed_tickers: List[str]
    errors: List[str]


def _empty_dividend_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=DIVIDEND_COLUMNS)


def collect_dividend_cashflows(
    stock_txns: List["StockTxn"],
    as_of: pd.Timestamp,
    build_holding_segments: Callable[[List["StockTxn"], pd.Timestamp], List["HoldSeg"]],
    yf_module,
) -> DividendFetchResult:
    try:
        segs = build_holding_segments(stock_txns, as_of)
        if not segs:
            return DividendFetchResult(_empty_dividend_frame(), True, [], [], [])

        by_ticker = defaultdict(list)
        for seg in segs:
            by_ticker[seg.ticker].append(
                (
                    pd.to_datetime(seg.start).normalize(),
                    pd.to_datetime(seg.end).normalize(),
                    seg.shares,
                )
            )

        attempted_tickers = sorted(by_ticker)
        if yf_module is None:
            return DividendFetchResult(
                _empty_dividend_frame(),
                False,
                attempted_tickers,
                attempted_tickers,
                ["yfinance not installed; cannot fetch dividend history."],
            )

        div_rows = []
        failed_tickers: List[str] = []
        errors: List[str] = []
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
                            raise ValueError("dividend history returned no usable numeric dividend column")
                        div_hist = raw_div_hist[numeric_cols[0]]
                else:
                    div_hist = raw_div_hist

                if div_hist is None:
                    raise ValueError("dividend history returned no usable data")
                if div_hist.empty:
                    continue

                div_hist = div_hist.dropna()
                if div_hist.empty:
                    raise ValueError("dividend history contained no valid dividend entries")

                dt_index = pd.to_datetime(div_hist.index, errors="coerce", utc=True)
                dt_index = dt_index.tz_localize(None).normalize()
                div_hist.index = dt_index
                div_hist = div_hist[div_hist.index.notna()]
                if div_hist.empty:
                    raise ValueError("dividend history dates could not be parsed")

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
            except Exception as exc:
                failed_tickers.append(ticker)
                errors.append(f"{ticker}: {exc}")
        cashflows = pd.DataFrame(div_rows)
        if cashflows.empty:
            cashflows = _empty_dividend_frame()
        return DividendFetchResult(
            cashflows,
            len(failed_tickers) == 0,
            attempted_tickers,
            sorted(set(failed_tickers)),
            errors,
        )
    except Exception as exc:
        return DividendFetchResult(
            _empty_dividend_frame(),
            False,
            [],
            [],
            [f"Dividend fetch initialization failed: {exc}"],
        )
