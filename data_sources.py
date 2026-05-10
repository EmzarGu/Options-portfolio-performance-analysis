from __future__ import annotations

import io
import logging
import requests
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

import pandas as pd

from portfolio_backend.dividend_history_store import get_default_dividend_history_store
from portfolio_backend.price_history_store import get_default_price_history_store

if TYPE_CHECKING:
    from portfolio_backend.models import HoldSeg, StockTxn


logger = logging.getLogger(__name__)

DIVIDEND_COLUMNS = ["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]
DIVIDEND_HISTORY_CACHE: Dict[Tuple[str, Optional[pd.Timestamp], Optional[pd.Timestamp]], pd.Series] = {}
OPTION_COLUMN_MAP = {
    "Trans date": "trans_date",
    "Tiker": "ticker",
    "Type": "type",
    "Action": "action",
    "Expiration": "expiration",
    "Strike": "strike",
    "Qty": "qty",
    "Amount": "amount",
    "Comission": "commission",
    "Total P&L": "total_pnl",
    "Assigned": "assigned_flag",
    "Comment": "comment",
}
OPTION_DATE_COLUMNS = ["trans_date", "expiration"]
OPTION_NUMERIC_COLUMNS = ["strike", "qty", "amount", "commission", "total_pnl"]


@dataclass(frozen=True)
class DividendFetchResult:
    cashflows: pd.DataFrame
    coverage_complete: bool
    attempted_tickers: List[str]
    failed_tickers: List[str]
    errors: List[str]


@dataclass(frozen=True)
class SheetDownload:
    content: bytes
    downloaded_at: str
    source: str
    file_name: Optional[str] = None
    file_modified_at: Optional[str] = None
    file_version: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _authorized_session(credentials):
    try:
        from google.auth.transport.requests import AuthorizedSession
    except ImportError as exc:
        raise RuntimeError(
            "Google Drive credentials are configured, but google-auth's requests transport "
            "is unavailable. Ensure google-auth and requests are installed."
        ) from exc
    return AuthorizedSession(credentials)


def fetch_drive_file_metadata(sheet_id: str, credentials) -> Dict[str, str]:
    authed = _authorized_session(credentials)
    url = f"https://www.googleapis.com/drive/v3/files/{sheet_id}"
    resp = authed.get(url, params={"fields": "id,name,modifiedTime,version"}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        return data
    return {}


def download_excel_workbook(
    sheet_id: str,
    *,
    credentials=None,
    local_excel_path: Optional[str] = None,
) -> SheetDownload:
    downloaded_at = _now_iso()
    if local_excel_path:
        path = Path(local_excel_path).expanduser()
        if not path.exists():
            raise RuntimeError(f"LOCAL_EXCEL_PATH is set but file not found: {path}")
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
        return SheetDownload(
            content=path.read_bytes(),
            downloaded_at=downloaded_at,
            source="local",
            file_name=path.name,
            file_modified_at=modified_at,
        )

    meta = {}
    if credentials is not None:
        try:
            meta = fetch_drive_file_metadata(sheet_id, credentials)
        except Exception:
            meta = {}

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    if credentials is not None:
        authed = _authorized_session(credentials)
        resp = authed.get(url, timeout=15)
        resp.raise_for_status()
        if not resp.content.startswith(b"PK"):
            raise RuntimeError("Google Sheets export returned non-XLSX content; check sharing settings.")
        return SheetDownload(
            content=resp.content,
            downloaded_at=downloaded_at,
            source="drive",
            file_name=meta.get("name"),
            file_modified_at=meta.get("modifiedTime"),
            file_version=meta.get("version"),
        )

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        content = resp.content
        if not content.startswith(b"PK"):
            raise RuntimeError("Public sheet export returned non-XLSX content; check sharing settings.")
    except Exception as exc:
        msg = (
            "Public sheet download failed and no service account credentials were found. "
            "Share the sheet publicly or set GOOGLE_SERVICE_ACCOUNT_JSON / LOCAL_SECRETS_PATH / LOCAL_EXCEL_PATH."
        )
        raise RuntimeError(msg) from exc

    return SheetDownload(
        content=content,
        downloaded_at=downloaded_at,
        source="public",
        file_name=meta.get("name"),
        file_modified_at=meta.get("modifiedTime"),
    )


def option_sheet_names_from_excel_bytes(excel_bytes: bytes) -> List[str]:
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    names = [n for n in xls.sheet_names if pd.notna(n) and str(n).startswith("Options ")]
    return sorted(names)


def normalize_options_frame(raw: pd.DataFrame, source_sheet: str) -> pd.DataFrame:
    df = raw.rename(columns=OPTION_COLUMN_MAP)
    for column in OPTION_DATE_COLUMNS:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Parsing dates in .* format when dayfirst=True was specified.*",
                category=UserWarning,
            )
            df[column] = pd.to_datetime(df[column], errors="coerce", dayfirst=True).dt.tz_localize(None)
    for column in OPTION_NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["action"] = df["action"].astype(str).str.title().str.strip()
    df["action"] = df["action"].replace({"Bought": "Buy"})
    df["type"] = df["type"].astype(str).str.title().str.strip()
    df["comment"] = df["comment"].astype(str)
    if "assigned_flag" in df.columns:
        df["assigned_flag"] = pd.to_numeric(df["assigned_flag"], errors="coerce").fillna(0).astype(float)
    df["source_sheet"] = source_sheet
    return df


def load_options_from_excel_bytes(excel_bytes: bytes, sheets: List[str]) -> pd.DataFrame:
    frames = []
    for sheet in sheets:
        raw = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=sheet, header=1)
        frames.append(normalize_options_frame(raw, sheet))
    if not frames:
        return pd.DataFrame(columns=[*OPTION_COLUMN_MAP.values(), "source_sheet"])
    df_all = pd.concat(frames, ignore_index=True)
    return df_all[df_all["action"].isin(["Sell", "Buy"])]


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

        store = get_default_dividend_history_store()
        try:
            cached = store.get_history(ticker_key, start_ts, end_ts)
        except Exception as exc:
            logger.warning("dividend_history_store_read_failed ticker=%s error=%s", ticker_key, exc)
            cached = None
        if cached is not None and cached.fully_covered:
            self._cache[cache_key] = cached.series.copy()
            return cached.series.copy()

        raw_div_hist = self.yf_module.Ticker(ticker_key).dividends
        div_hist = normalize_dividend_history(raw_div_hist)
        if not div_hist.empty:
            if start_ts is not None:
                div_hist = div_hist[div_hist.index >= start_ts]
            if end_ts is not None:
                div_hist = div_hist[div_hist.index < end_ts]
        try:
            store.upsert_history(ticker_key, div_hist, start_ts, end_ts)
        except Exception as exc:
            logger.warning("dividend_history_store_write_failed ticker=%s error=%s", ticker_key, exc)
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


def fetch_current_prices_yf(tickers, yf_module) -> Tuple[Dict[str, float], List[str], Dict[str, int]]:
    """Fetch latest stock prices; return prices, error messages, and coverage summary."""
    errors: List[str] = []
    summary = {"requested": 0, "fetched": 0}
    if yf_module is None:
        errors.append("yfinance not installed; cannot fetch live stock prices.")
        return {}, errors, summary
    tickers = sorted({str(t).upper().strip() for t in tickers if isinstance(t, str) and t.strip()})
    summary["requested"] = len(tickers)
    prices: Dict[str, float] = {}
    if not tickers:
        return prices, errors, summary
    try:
        data = yf_module.download(
            tickers=tickers,
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=True,
        )
        if isinstance(data.columns, pd.MultiIndex):
            for t in tickers:
                for col in ("Adj Close", "Close"):
                    try:
                        series = data[(t, col)].dropna()
                        if not series.empty:
                            prices[t] = float(series.iloc[-1])
                            break
                    except KeyError:
                        continue
        else:
            series = data["Adj Close"].dropna() if "Adj Close" in data else data["Close"].dropna()
            if not series.empty and len(tickers) == 1:
                prices[tickers[0]] = float(series.iloc[-1])
    except Exception as exc:
        errors.append(f"Primary price download failed: {exc}")
    missing = [t for t in tickers if t not in prices]
    for t in missing:
        try:
            tk = yf_module.Ticker(t)
            hist = tk.history(period="5d", interval="1d")
            if not hist.empty:
                prices[t] = float(hist["Close"].iloc[-1])
                continue
            p = getattr(tk.fast_info, "last_price", None)
            if p:
                prices[t] = float(p)
        except Exception as exc:
            errors.append(f"{t}: {exc}")
    still_missing = [t for t in tickers if t not in prices]
    summary["fetched"] = len(prices)
    if still_missing:
        errors.append(f"Missing prices for tickers: {', '.join(still_missing)}")
    return prices, errors, summary


def fetch_price_history_yf(
    tickers,
    start: pd.Timestamp,
    end: pd.Timestamp,
    yf_module,
) -> Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]:
    """Daily close prices per ticker between start and end, inclusive."""
    history: Dict[str, pd.Series] = {}
    errors: List[str] = []
    summary = {"requested": 0, "fetched": 0}
    if yf_module is None:
        errors.append("yfinance not installed; cannot fetch historical stock prices.")
        return history, errors, summary
    tickers = sorted({t for t in tickers if t})
    summary["requested"] = len(tickers)
    if not tickers or pd.isna(start) or pd.isna(end):
        return history, errors, summary
    provider = YFinancePriceHistoryProvider(yf_module)
    store = get_default_price_history_store()
    cache_hits = 0
    cache_misses = 0
    cache_writes = 0
    yfinance_fetches = 0
    try:
        cached_by_ticker = store.get_many_history(tickers, start, end)
    except Exception as exc:
        logger.warning("price_history_store_bulk_read_failed error=%s", exc)
        cached_by_ticker = {}
    for ticker in tickers:
        try:
            cached = cached_by_ticker.get(str(ticker).upper().strip())
            if cached is not None and cached.fully_covered:
                cache_hits += 1
                series = cached.series
            else:
                cache_misses += 1
                yfinance_fetches += 1
                series = provider.get_price_history(ticker, start, end)
                if not series.empty:
                    try:
                        store.upsert_history(ticker, series, start, end)
                        cache_writes += 1
                    except Exception as exc:
                        logger.warning("price_history_store_write_failed ticker=%s error=%s", ticker, exc)
            if not series.empty:
                history[ticker] = series
        except Exception as exc:
            errors.append(f"Historical price download failed: {exc}")
    logger.warning(
        "historical_price_cache_summary requested=%s fetched=%s cache_hits=%s cache_misses=%s cache_writes=%s yfinance_fetches=%s",
        summary["requested"],
        len(history),
        cache_hits,
        cache_misses,
        cache_writes,
        yfinance_fetches,
    )
    summary["fetched"] = len(history)
    missing_tickers = [t for t in tickers if t not in history]
    if missing_tickers:
        errors.append(f"Missing historical price series for tickers: {', '.join(missing_tickers)}")
    return history, errors, summary


def align_benchmarks_monthly(
    tickers: Dict[str, str],
    idx: pd.DatetimeIndex,
    yf_module,
) -> Dict[str, pd.Series]:
    """Return benchmark monthly returns aligned to the strategy month-end index."""
    if yf_module is None or len(idx) == 0:
        return {}
    start = idx.min() - pd.DateOffset(months=2)
    end = idx.max() + pd.DateOffset(days=1)
    try:
        price_history, _errors, _summary = fetch_price_history_yf(
            set(tickers.values()),
            pd.to_datetime(start).normalize(),
            pd.to_datetime(end).normalize(),
            yf_module,
        )
    except Exception as exc:
        logger.warning("benchmark_price_history_fetch_failed error=%s", exc)
        return {}
    aligned = {}
    for name, ticker in tickers.items():
        try:
            px = price_history.get(ticker)
            if px is None:
                continue
            px = px.dropna()
            if px.empty:
                continue
            monthly_px = px.resample("ME").last()
            monthly_ret = monthly_px.pct_change(fill_method=None)
            monthly_ret = monthly_ret.reindex(idx)
            aligned[name] = monthly_ret
        except Exception:
            continue
    return aligned


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
