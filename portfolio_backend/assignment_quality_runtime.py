"""Cache historical assignment accounting independently from live valuations."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import logging
from threading import Lock
from time import perf_counter, time
from typing import Any, Callable

import pandas as pd

from portfolio_backend.derived_payload_cache import derived_payload_key, load_derived_payload, save_derived_payload
from portfolio_backend.ibkr.assignment_quality import prepare_assignment_quality, build_assignment_quality_analysis, _historical_price_at_or_before

logger = logging.getLogger("uvicorn.error")
_CACHE: dict[str, tuple[float, dict]] = {}
_LOCK = Lock()
_TTL = 21600


def _history_points(store: Any, prepared: dict, as_of: pd.Timestamp) -> dict:
    """Store only the close needed by each mature horizon, using existing semantics."""
    dates = {}
    for row in prepared["lots"]:
        for months in (6, 12, 18):
            dt = pd.Timestamp(row["assignment_date"]) + pd.DateOffset(months=months)
            if dt <= as_of:
                dates.setdefault(row["ticker"], []).append(dt)
    if hasattr(store, "get_history_points"):
        series = store.get_history_points(dates)
    else:
        lookups = store.get_many_history(sorted(dates), pd.Timestamp("2000-01-01"), as_of) if dates else {}
        series = {t: lookup.series for t, lookup in lookups.items()}
    return {ticker: {dt.isoformat(): value for dt in sorted(set(points))
                     if (value := _historical_price_at_or_before(series.get(ticker), dt)) is not None}
            for ticker, points in dates.items()}


def _load_prepared(key: str | None, loader: Callable, timings: dict) -> tuple[dict, bool]:
    """Reuse a compact immutable snapshot for six hours, scoped to report and date."""
    started = perf_counter()
    with _LOCK:
        if key and key in _CACHE and time() - _CACHE[key][0] < _TTL:
            timings["prepared_memory_hit"] = 1
            return _CACHE[key][1], True
        cached = load_derived_payload(key) if key else None
        if cached and time() - float(cached.get("built_at", 0)) < _TTL:
            timings["prepared_persistent_hit"] = 1
            prepared = cached
        else:
            prepared = loader()
            prepared["built_at"] = time()
            if key:
                save_derived_payload(key, prepared, metadata={"namespace": "assignment_prepared"})
        if key:
            _CACHE[key] = (prepared["built_at"], prepared)
            while len(_CACHE) > 4:
                _CACHE.pop(min(_CACHE, key=lambda k: _CACHE[k][0]))
    timings["prepared_load_ms"] = (perf_counter() - started) * 1000
    return prepared, bool(cached)


def build_assignment_payload(*, state: Any, source_metadata: dict, report_loader: Callable,
                             history_store_factory: Callable, price_fetcher: Callable) -> dict:
    """Revalue prepared accounting with current prices; overlap independent I/O."""
    started = perf_counter()
    timings = {}
    prices = dict(getattr(state, "stock_prices", {}) or getattr(state, "live_prices", {}) or {})
    as_of = pd.Timestamp(getattr(state, "as_of", None) or datetime.now(timezone.utc).date()).normalize()
    identity = {k: source_metadata.get(k) for k in ("source_snapshot_id", "ibkr_import_run_id", "source_version")}
    key = derived_payload_key("assignment_prepared", {"version": 1, "source": identity, "as_of": as_of.isoformat()}) if any(identity.values()) else None

    def prepare():
        t = perf_counter()
        report = report_loader()
        timings["report_load_ms"] = (perf_counter() - t) * 1000
        t = perf_counter()
        prepared = prepare_assignment_quality(report, as_of=as_of)
        timings["accounting_prepare_ms"] = (perf_counter() - t) * 1000
        return prepared

    def fetch_prices(tickers):
        t = perf_counter()
        result = price_fetcher(tickers)[0] if tickers else {}
        timings["extra_price_fetch_ms"] = (perf_counter() - t) * 1000
        timings["extra_price_tickers"] = len(tickers)
        return result

    try:
        prepared, _ = _load_prepared(key, prepare, timings)
        tickers = sorted({row["ticker"] for row in prepared["lots"]})
        history_key = derived_payload_key("assignment_horizons", {"prepared": key, "version": 1}) if key else None
        history_errors = []

        def load_history():
            t = perf_counter()
            try:
                data, _ = _load_prepared(history_key, lambda: {"points": _history_points(history_store_factory(), prepared, as_of)}, {})
                return {ticker: pd.Series({pd.Timestamp(d): v for d, v in points.items()}, dtype=float)
                        for ticker, points in data["points"].items()}
            except Exception as exc:
                history_errors.append(str(exc))
                return {}
            finally:
                timings["horizon_load_ms"] = (perf_counter() - t) * 1000

        with ThreadPoolExecutor(max_workers=2) as pool:
            future_prices = pool.submit(fetch_prices, [t for t in tickers if t not in prices])
            history = load_history()
            try:
                prices.update(future_prices.result() or {})
            except Exception:
                pass  # Preserve existing missing-price coverage behavior.
        t = perf_counter()
        payload = build_assignment_quality_analysis(None, as_of=as_of, prices=prices,
                                                    historical_prices=history, prepared=prepared)
        timings["valuation_ms"] = (perf_counter() - t) * 1000
        if history_errors:
            payload.setdefault("coverage", {})["history_errors"] = history_errors
        return payload
    finally:
        timings["total_ms"] = (perf_counter() - started) * 1000
        logger.info("assignment_quality_stages %s", " ".join(f"{k}={v:.2f}" for k, v in timings.items()))
