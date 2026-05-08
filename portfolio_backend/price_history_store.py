from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd


SCHEMA_VERSION = 1
COLLECTION_PRICE_HISTORY_CHUNKS = "price_history_chunks"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceHistoryLookup:
    series: pd.Series
    fully_covered: bool


class PriceHistoryStore:
    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> PriceHistoryLookup:
        raise NotImplementedError

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        raise NotImplementedError


class DisabledPriceHistoryStore(PriceHistoryStore):
    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> PriceHistoryLookup:
        _ = ticker, start, end
        return PriceHistoryLookup(pd.Series(dtype=float), fully_covered=False)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        _ = ticker, series, start, end


class MemoryPriceHistoryStore(PriceHistoryStore):
    def __init__(self):
        self._docs: Dict[str, Dict] = {}

    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> PriceHistoryLookup:
        return _lookup_from_docs(self._docs, ticker, start, end)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        _upsert_docs(self._docs, ticker, series, start, end)


class FirestorePriceHistoryStore(PriceHistoryStore):
    def __init__(self, *, project: Optional[str] = None, database: str = "(default)", client=None):
        if client is None:
            from google.cloud import firestore

            client = firestore.Client(project=project, database=database)
        self.client = client

    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> PriceHistoryLookup:
        ticker_key = _ticker_key(ticker)
        docs: Dict[str, Dict] = {}
        for year in _years_between(start, end):
            doc_id = _document_id(ticker_key, year)
            snapshot = self.client.collection(COLLECTION_PRICE_HISTORY_CHUNKS).document(doc_id).get()
            if snapshot.exists:
                docs[doc_id] = snapshot.to_dict() or {}
        return _lookup_from_docs(docs, ticker_key, start, end)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        ticker_key = _ticker_key(ticker)
        clean = _clean_series(series, ticker_key)
        if clean.empty:
            return
        for year in _years_between(start, end):
            doc_id = _document_id(ticker_key, year)
            ref = self.client.collection(COLLECTION_PRICE_HISTORY_CHUNKS).document(doc_id)
            snapshot = ref.get()
            docs = {doc_id: snapshot.to_dict() or {}} if snapshot.exists else {}
            _upsert_docs(docs, ticker_key, clean, start, end, years={year})
            if doc_id in docs:
                ref.set(docs[doc_id])


_DEFAULT_STORE: Optional[PriceHistoryStore] = None


def get_default_price_history_store() -> PriceHistoryStore:
    global _DEFAULT_STORE
    if _DEFAULT_STORE is not None:
        return _DEFAULT_STORE

    mode = os.getenv("PRICE_HISTORY_STORE", "auto").strip().lower()
    if mode in {"off", "disabled", "none", "memory"}:
        _DEFAULT_STORE = MemoryPriceHistoryStore() if mode == "memory" else DisabledPriceHistoryStore()
        return _DEFAULT_STORE

    project = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    running_on_cloud_run = bool(os.getenv("K_SERVICE"))
    if mode == "firestore" or project or running_on_cloud_run:
        try:
            _DEFAULT_STORE = FirestorePriceHistoryStore(
                project=project,
                database=os.getenv("FIRESTORE_DATABASE", "(default)"),
            )
            return _DEFAULT_STORE
        except Exception as exc:
            logger.warning("price_history_store_init_failed mode=%s error=%s", mode, exc)

    _DEFAULT_STORE = DisabledPriceHistoryStore()
    return _DEFAULT_STORE


def reset_default_price_history_store() -> None:
    global _DEFAULT_STORE
    _DEFAULT_STORE = None


def _ticker_key(ticker: str) -> str:
    return str(ticker or "").upper().strip()


def _document_id(ticker: str, year: int) -> str:
    safe_ticker = _ticker_key(ticker).replace("/", "_")
    return f"{safe_ticker}:{int(year)}"


def _years_between(start: pd.Timestamp, end: pd.Timestamp) -> list[int]:
    start_ts = pd.to_datetime(start).normalize()
    end_ts = pd.to_datetime(end).normalize()
    return list(range(int(start_ts.year), int(end_ts.year) + 1))


def _year_bounds(year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    return pd.Timestamp(year=int(year), month=1, day=1), pd.Timestamp(year=int(year), month=12, day=31)


def _clean_series(series: pd.Series, ticker: str) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=float, name=ticker)
    clean = pd.to_numeric(series, errors="coerce").dropna()
    clean.index = pd.to_datetime(clean.index, errors="coerce").normalize()
    clean = clean[clean.index.notna()]
    clean = clean.sort_index()
    clean = clean[~clean.index.duplicated(keep="last")]
    clean.name = ticker
    return clean


def _series_from_doc(doc: Dict, ticker: str) -> pd.Series:
    rows = doc.get("prices") or []
    if not rows:
        return pd.Series(dtype=float, name=ticker)
    dates = []
    closes = []
    for row in rows:
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        close = pd.to_numeric(row.get("close"), errors="coerce")
        if pd.notna(dt) and pd.notna(close):
            dates.append(dt.normalize())
            closes.append(float(close))
    if not dates:
        return pd.Series(dtype=float, name=ticker)
    series = pd.Series(closes, index=pd.DatetimeIndex(dates), name=ticker)
    return _clean_series(series, ticker)


def _doc_from_series(
    ticker: str,
    year: int,
    series: pd.Series,
    coverage_start: pd.Timestamp,
    coverage_end: pd.Timestamp,
    existing: Optional[Dict] = None,
) -> Dict:
    existing = existing or {}
    existing_series = _series_from_doc(existing, ticker)
    merged = pd.concat([existing_series, _clean_series(series, ticker)]).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]

    current_start = pd.to_datetime(existing.get("coverage_start"), errors="coerce")
    current_end = pd.to_datetime(existing.get("coverage_end"), errors="coerce")
    starts = [coverage_start]
    ends = [coverage_end]
    if pd.notna(current_start):
        starts.append(current_start.normalize())
    if pd.notna(current_end):
        ends.append(current_end.normalize())

    return {
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker,
        "year": int(year),
        "coverage_start": min(starts).date().isoformat(),
        "coverage_end": max(ends).date().isoformat(),
        "prices": [
            {"date": idx.date().isoformat(), "close": float(value)}
            for idx, value in merged.items()
            if int(idx.year) == int(year)
        ],
        "source": "yfinance",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _lookup_from_docs(
    docs: Dict[str, Dict],
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> PriceHistoryLookup:
    ticker_key = _ticker_key(ticker)
    start_ts = pd.to_datetime(start).normalize()
    end_ts = pd.to_datetime(end).normalize()
    series_parts = []
    fully_covered = True
    for year in _years_between(start_ts, end_ts):
        doc_id = _document_id(ticker_key, year)
        doc = docs.get(doc_id)
        chunk_start, chunk_end = _year_bounds(year)
        requested_start = max(start_ts, chunk_start)
        requested_end = min(end_ts, chunk_end)
        if not doc:
            fully_covered = False
            continue

        coverage_start = pd.to_datetime(doc.get("coverage_start"), errors="coerce")
        coverage_end = pd.to_datetime(doc.get("coverage_end"), errors="coerce")
        if (
            pd.isna(coverage_start)
            or pd.isna(coverage_end)
            or coverage_start.normalize() > requested_start
            or coverage_end.normalize() < requested_end
        ):
            fully_covered = False

        chunk = _series_from_doc(doc, ticker_key)
        if not chunk.empty:
            series_parts.append(chunk.loc[(chunk.index >= requested_start) & (chunk.index <= requested_end)])

    if series_parts:
        series = pd.concat(series_parts).sort_index()
        series = series[~series.index.duplicated(keep="last")]
        series.name = ticker_key
    else:
        series = pd.Series(dtype=float, name=ticker_key)
    return PriceHistoryLookup(series=series, fully_covered=fully_covered)


def _upsert_docs(
    docs: Dict[str, Dict],
    ticker: str,
    series: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    years: Optional[set[int]] = None,
) -> None:
    ticker_key = _ticker_key(ticker)
    clean = _clean_series(series, ticker_key)
    if clean.empty:
        return
    start_ts = pd.to_datetime(start).normalize()
    end_ts = pd.to_datetime(end).normalize()
    for year in _years_between(start_ts, end_ts):
        if years is not None and year not in years:
            continue
        year_start, year_end = _year_bounds(year)
        coverage_start = max(start_ts, year_start)
        coverage_end = min(end_ts, year_end)
        year_series = clean.loc[(clean.index >= year_start) & (clean.index <= year_end)]
        if year_series.empty:
            continue
        doc_id = _document_id(ticker_key, year)
        docs[doc_id] = _doc_from_series(
            ticker_key,
            year,
            year_series,
            coverage_start,
            coverage_end,
            docs.get(doc_id),
        )
