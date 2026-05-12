from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd


SCHEMA_VERSION = 1
COLLECTION_DIVIDEND_HISTORY = "dividend_history"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DividendHistoryLookup:
    series: pd.Series
    fully_covered: bool


class DividendHistoryStore:
    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> DividendHistoryLookup:
        raise NotImplementedError

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        raise NotImplementedError


class DisabledDividendHistoryStore(DividendHistoryStore):
    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> DividendHistoryLookup:
        _ = ticker, start, end
        return DividendHistoryLookup(pd.Series(dtype=float), fully_covered=False)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        _ = ticker, series, start, end


class MemoryDividendHistoryStore(DividendHistoryStore):
    def __init__(self):
        self._docs: Dict[str, Dict] = {}

    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> DividendHistoryLookup:
        return _lookup_from_doc(self._docs.get(_document_id(ticker)), ticker, start, end)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        doc_id = _document_id(ticker)
        self._docs[doc_id] = _doc_from_series(ticker, series, start, end, self._docs.get(doc_id))


class FirestoreDividendHistoryStore(DividendHistoryStore):
    def __init__(self, *, project: Optional[str] = None, database: str = "(default)", client=None):
        if client is None:
            from portfolio_backend.gcp import firestore_client

            client = firestore_client(project=project, database=database)
        self.client = client
        self._doc_cache: Dict[str, Optional[Dict]] = {}

    def get_history(self, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> DividendHistoryLookup:
        doc_id = _document_id(ticker)
        if doc_id not in self._doc_cache:
            snapshot = self.client.collection(COLLECTION_DIVIDEND_HISTORY).document(doc_id).get()
            self._doc_cache[doc_id] = snapshot.to_dict() or {} if snapshot.exists else None
        doc = self._doc_cache[doc_id]
        return _lookup_from_doc(doc, ticker, start, end)

    def upsert_history(self, ticker: str, series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
        doc_id = _document_id(ticker)
        ref = self.client.collection(COLLECTION_DIVIDEND_HISTORY).document(doc_id)
        snapshot = ref.get()
        existing = snapshot.to_dict() if snapshot.exists else None
        doc = _doc_from_series(ticker, series, start, end, existing)
        ref.set(doc)
        self._doc_cache[doc_id] = doc


_DEFAULT_STORE: Optional[DividendHistoryStore] = None


def get_default_dividend_history_store() -> DividendHistoryStore:
    global _DEFAULT_STORE
    if _DEFAULT_STORE is not None:
        return _DEFAULT_STORE

    mode = os.getenv("DIVIDEND_HISTORY_STORE", "auto").strip().lower()
    if mode in {"off", "disabled", "none", "memory"}:
        _DEFAULT_STORE = MemoryDividendHistoryStore() if mode == "memory" else DisabledDividendHistoryStore()
        return _DEFAULT_STORE

    project = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    running_on_cloud_run = bool(os.getenv("K_SERVICE"))
    if mode == "firestore" or project or running_on_cloud_run:
        try:
            _DEFAULT_STORE = FirestoreDividendHistoryStore(
                project=project,
                database=os.getenv("FIRESTORE_DATABASE", "(default)"),
            )
            return _DEFAULT_STORE
        except Exception as exc:
            logger.warning("dividend_history_store_init_failed mode=%s error=%s", mode, exc)

    _DEFAULT_STORE = DisabledDividendHistoryStore()
    return _DEFAULT_STORE


def reset_default_dividend_history_store() -> None:
    global _DEFAULT_STORE
    _DEFAULT_STORE = None


def _ticker_key(ticker: str) -> str:
    return str(ticker or "").upper().strip()


def _document_id(ticker: str) -> str:
    return _ticker_key(ticker).replace("/", "_")


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


def _series_from_doc(doc: Optional[Dict], ticker: str) -> pd.Series:
    if not doc:
        return pd.Series(dtype=float, name=ticker)
    rows = doc.get("dividends") or []
    dates = []
    values = []
    for row in rows:
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        value = pd.to_numeric(row.get("amount"), errors="coerce")
        if pd.notna(dt) and pd.notna(value):
            dates.append(dt.normalize())
            values.append(float(value))
    if not dates:
        return pd.Series(dtype=float, name=ticker)
    return _clean_series(pd.Series(values, index=pd.DatetimeIndex(dates), name=ticker), ticker)


def _lookup_from_doc(
    doc: Optional[Dict],
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> DividendHistoryLookup:
    ticker_key = _ticker_key(ticker)
    if not doc:
        return DividendHistoryLookup(pd.Series(dtype=float, name=ticker_key), fully_covered=False)

    start_ts = pd.to_datetime(start).normalize()
    end_ts = pd.to_datetime(end).normalize()
    coverage_start = pd.to_datetime(doc.get("coverage_start"), errors="coerce")
    coverage_end = pd.to_datetime(doc.get("coverage_end"), errors="coerce")
    fully_covered = (
        pd.notna(coverage_start)
        and pd.notna(coverage_end)
        and coverage_start.normalize() <= start_ts
        and coverage_end.normalize() >= end_ts
    )

    series = _series_from_doc(doc, ticker_key)
    if not series.empty:
        series = series.loc[(series.index >= start_ts) & (series.index < end_ts)]
    return DividendHistoryLookup(series=series, fully_covered=bool(fully_covered))


def _doc_from_series(
    ticker: str,
    series: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
    existing: Optional[Dict] = None,
) -> Dict:
    ticker_key = _ticker_key(ticker)
    existing = existing or {}
    existing_series = _series_from_doc(existing, ticker_key)
    clean_series = _clean_series(series, ticker_key)
    series_parts = [part for part in (existing_series, clean_series) if not part.empty]
    merged = (
        pd.concat(series_parts).sort_index()
        if series_parts
        else pd.Series(dtype=float, name=ticker_key)
    )
    merged = merged[~merged.index.duplicated(keep="last")]

    start_ts = pd.to_datetime(start).normalize()
    end_ts = pd.to_datetime(end).normalize()
    current_start = pd.to_datetime(existing.get("coverage_start"), errors="coerce")
    current_end = pd.to_datetime(existing.get("coverage_end"), errors="coerce")
    starts = [start_ts]
    ends = [end_ts]
    if pd.notna(current_start):
        starts.append(current_start.normalize())
    if pd.notna(current_end):
        ends.append(current_end.normalize())

    return {
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker_key,
        "coverage_start": min(starts).date().isoformat(),
        "coverage_end": max(ends).date().isoformat(),
        "dividends": [
            {"date": idx.date().isoformat(), "amount": float(value)}
            for idx, value in merged.items()
        ],
        "source": "yfinance",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
