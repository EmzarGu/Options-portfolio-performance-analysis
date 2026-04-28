from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

import pandas as pd

if TYPE_CHECKING:
    from streamlit_app import HoldSeg, StockTxn


DIVIDEND_COLUMNS = ["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]
DIVIDEND_HISTORY_CACHE: Dict[Tuple[str, Optional[pd.Timestamp], Optional[pd.Timestamp]], pd.Series] = {}


@dataclass(frozen=True)
class DividendFetchResult:
    cashflows: pd.DataFrame
    coverage_complete: bool
    attempted_tickers: List[str]
    failed_tickers: List[str]
    errors: List[str]


class DividendProvider:
    def get_dividend_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        raise NotImplementedError


class YFinanceDividendProvider(DividendProvider):
    def __init__(self, yf_module):
        self.yf_module = yf_module
        self._cache = DIVIDEND_HISTORY_CACHE

    def get_dividend_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        ticker_key = str(ticker).upper().strip()
        start_ts = pd.to_datetime(start).normalize() if pd.notna(start) else None
        end_ts = pd.to_datetime(end).normalize() if pd.notna(end) else None
        cache_key = (ticker_key, start_ts, end_ts)
        if cache_key in self._cache:
            return self._cache[cache_key].copy()

        if self.yf_module is None:
            raise RuntimeError("yfinance not installed; cannot fetch dividend history.")

        raw_div_hist = self.yf_module.Ticker(ticker_key).dividends
        div_hist = normalize_dividend_history(raw_div_hist)
        if not div_hist.empty:
            if start_ts is not None:
                div_hist = div_hist[div_hist.index >= start_ts]
            if end_ts is not None:
                div_hist = div_hist[div_hist.index < end_ts]
        self._cache[cache_key] = div_hist.copy()
        return div_hist.copy()


def clear_dividend_history_cache() -> None:
    DIVIDEND_HISTORY_CACHE.clear()


class PriceHistoryProvider:
    def get_price_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        raise NotImplementedError


class YFinancePriceHistoryProvider(PriceHistoryProvider):
    def __init__(self, yf_module):
        self.yf_module = yf_module
        self._cache: Dict[Tuple[str, Optional[pd.Timestamp], Optional[pd.Timestamp]], pd.Series] = {}

    def get_price_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        ticker_key = str(ticker).upper().strip()
        start_ts = pd.to_datetime(start).normalize() if pd.notna(start) else None
        end_ts = pd.to_datetime(end).normalize() if pd.notna(end) else None
        cache_key = (ticker_key, start_ts, end_ts)
        if cache_key in self._cache:
            return self._cache[cache_key].copy()

        if self.yf_module is None:
            raise RuntimeError("yfinance not installed; cannot fetch historical stock prices.")
        if not ticker_key or start_ts is None or end_ts is None:
            series = pd.Series(dtype=float, name=ticker_key)
            self._cache[cache_key] = series
            return series.copy()

        data = self.yf_module.download(
            tickers=[ticker_key],
            start=start_ts,
            end=end_ts + pd.Timedelta(days=1),
            progress=False,
            auto_adjust=False,
            group_by="ticker",
        )
        series = normalize_price_history(data, ticker_key)
        self._cache[cache_key] = series.copy()
        return series.copy()


def normalize_price_history(data, ticker: str) -> pd.Series:
    if data is None or data.empty:
        return pd.Series(dtype=float, name=ticker)
    if isinstance(data.columns, pd.MultiIndex):
        if (ticker, "Adj Close") in data:
            series = data[(ticker, "Adj Close")].dropna()
        elif (ticker, "Close") in data:
            series = data[(ticker, "Close")].dropna()
        else:
            return pd.Series(dtype=float, name=ticker)
    else:
        series = data["Adj Close"].dropna() if "Adj Close" in data else data.get("Close", pd.Series(dtype=float)).dropna()
    if series.empty:
        return pd.Series(dtype=float, name=ticker)
    series = series.copy()
    dt_index = pd.to_datetime(series.index, errors="coerce", utc=True)
    series.index = dt_index.tz_localize(None).normalize()
    series = series[series.index.notna()]
    if series.empty:
        return pd.Series(dtype=float, name=ticker)
    return series.rename(ticker)


def normalize_dividend_history(raw_div_hist) -> pd.Series:
    if raw_div_hist is None or raw_div_hist.empty:
        return pd.Series(dtype=float)

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
        return pd.Series(dtype=float)

    div_hist = div_hist.dropna()
    if div_hist.empty:
        raise ValueError("dividend history contained no valid dividend entries")

    dt_index = pd.to_datetime(div_hist.index, errors="coerce", utc=True)
    dt_index = dt_index.tz_localize(None).normalize()
    div_hist = div_hist.copy()
    div_hist.index = dt_index
    div_hist = div_hist[div_hist.index.notna()]
    if div_hist.empty:
        raise ValueError("dividend history dates could not be parsed")
    return div_hist


def _empty_dividend_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=DIVIDEND_COLUMNS)


def _ensure_dividend_provider(provider_or_yf_module) -> DividendProvider:
    if isinstance(provider_or_yf_module, DividendProvider):
        return provider_or_yf_module
    if hasattr(provider_or_yf_module, "get_dividend_history"):
        return provider_or_yf_module
    return YFinanceDividendProvider(provider_or_yf_module)


def collect_dividend_cashflows(
    stock_txns: List["StockTxn"],
    as_of: pd.Timestamp,
    build_holding_segments: Callable[[List["StockTxn"], pd.Timestamp], List["HoldSeg"]],
    dividend_provider,
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
        if dividend_provider is None:
            return DividendFetchResult(
                _empty_dividend_frame(),
                False,
                attempted_tickers,
                attempted_tickers,
                ["yfinance not installed; cannot fetch dividend history."],
            )
        provider = _ensure_dividend_provider(dividend_provider)

        div_rows = []
        failed_tickers: List[str] = []
        errors: List[str] = []
        for ticker, seg_list in by_ticker.items():
            try:
                ticker_start = min(start for start, _, _ in seg_list)
                ticker_end = max(end for _, end, _ in seg_list)
                div_hist = provider.get_dividend_history(ticker, ticker_start, ticker_end)
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
