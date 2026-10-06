"""Web payload loading, process caches and stored Decision Lab orchestration.

Market Data reads shared dated snapshots; the legacy provider retains its caches.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import date
from time import time
from typing import Any, Dict, Optional

from portfolio_backend.app_settings import default_monthly_target_band
from portfolio_backend.decision_lab import build_decision_lab_data
from portfolio_backend.derived_payload_cache import (
    derived_payload_key,
    load_derived_payload,
    save_derived_payload,
)
from portfolio_backend.gcp import firestore_client
from portfolio_backend.option_market.cutemarkets import CuteMarketsClient
from portfolio_backend.option_market.decision_data import decision_option_loader
from portfolio_backend.option_market.store import FirestoreOptionMarketStore
from portfolio_backend.web_dashboard_payloads import (
    build_assignment_quality_data as build_web_assignment_quality_data,
    build_dashboard_data as build_web_dashboard_data,
    dashboard_shell_data as build_web_dashboard_shell_data,
    get_web_context,
)


logger = logging.getLogger("uvicorn.error")
DEFAULT_DASHBOARD_DATA_CACHE_SECONDS = 0
DEFAULT_LAB_MEMORY_CACHE_SECONDS = 21600
_dashboard_data_cache_lock = threading.Lock()
_dashboard_data_cache: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_dashboard_data_key_locks: Dict[tuple, threading.Lock] = {}
_assignment_quality_cache_lock = threading.Lock()
_assignment_quality_cache: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_assignment_quality_key_locks: Dict[tuple, threading.Lock] = {}
_decision_lab_cache_lock = threading.Lock()
_decision_lab_cache: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_decision_lab_key_locks: Dict[tuple, threading.Lock] = {}
_probability_history_cache_lock = threading.Lock()
_probability_history_cache: tuple[float, list[dict[str, Any]]] | None = None
_option_history_cache_lock = threading.Lock()
_option_history_cache: tuple[float, list[dict[str, Any]]] | None = None


def _web_monthly_target_return_default() -> float:
    return float(default_monthly_target_band()["target_return"])


def _web_monthly_target_floor_default() -> float:
    return float(default_monthly_target_band()["target_floor"])


def _get_context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    force_rebuild: bool = False,
    timing_recorder=None,
):
    return get_web_context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        force_rebuild=force_rebuild,
        timing_recorder=timing_recorder,
    )


def _build_dashboard_data(
    *,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    target_return: Optional[float] = None,
    target_floor: Optional[float] = None,
    timing_recorder=None,
) -> Dict[str, Any]:
    return build_web_dashboard_data(
        as_of=as_of,
        include_unrealized=include_unrealized,
        target_return=target_return,
        target_floor=target_floor,
        default_target_return=_web_monthly_target_return_default(),
        default_target_floor=_web_monthly_target_floor_default(),
        timing_recorder=timing_recorder,
    )


def _dashboard_shell_data(*, include_unrealized: bool, target_return: float, target_floor: float) -> Dict[str, Any]:
    return build_web_dashboard_shell_data(
        include_unrealized=include_unrealized,
        target_return=target_return,
        target_floor=target_floor,
    )


def _dashboard_data_cache_seconds() -> int:
    raw = os.getenv("WEB_DASHBOARD_DATA_CACHE_SECONDS", str(DEFAULT_DASHBOARD_DATA_CACHE_SECONDS))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_DASHBOARD_DATA_CACHE_SECONDS


def _dashboard_cache_key(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    target_return: Optional[float],
    target_floor: Optional[float],
) -> tuple:
    rounded_target = round(float(target_return if target_return is not None else _web_monthly_target_return_default()), 8)
    rounded_floor = round(float(target_floor if target_floor is not None else _web_monthly_target_floor_default()), 8)
    return (as_of.isoformat() if as_of else "", bool(include_unrealized), rounded_floor, rounded_target)


def _clear_dashboard_data_cache() -> None:
    with _dashboard_data_cache_lock:
        _dashboard_data_cache.clear()
        _dashboard_data_key_locks.clear()
    with _assignment_quality_cache_lock:
        _assignment_quality_cache.clear()
        _assignment_quality_key_locks.clear()
    with _decision_lab_cache_lock:
        _decision_lab_cache.clear()
        _decision_lab_key_locks.clear()


def _lab_memory_cache_seconds() -> int:
    raw = os.getenv("WEB_LAB_MEMORY_CACHE_SECONDS", str(DEFAULT_LAB_MEMORY_CACHE_SECONDS))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_LAB_MEMORY_CACHE_SECONDS


def _payload_source_metadata(payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata = payload.get("source_metadata")
    if isinstance(metadata, dict):
        return metadata
    dashboard = payload.get("dashboard") or {}
    request = dashboard.get("request") or {}
    return {
        "source_snapshot_id": request.get("source_snapshot_id"),
        "as_of": request.get("as_of"),
        "generated_at": payload.get("generated_at"),
    }


def _lab_source_cache_key(payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata = _payload_source_metadata(payload)
    return {
        "source_snapshot_id": metadata.get("source_snapshot_id"),
        "ibkr_import_run_id": metadata.get("ibkr_import_run_id"),
        "pipeline_snapshot_id": metadata.get("pipeline_snapshot_id"),
        "as_of": ((payload.get("dashboard") or {}).get("request") or {}).get("as_of"),
        "price_updated_at": ((payload.get("dashboard") or {}).get("data_freshness") or {}).get("prices_updated_at"),
    }


def _memory_cache_get(cache: Dict[tuple, tuple[float, Dict[str, Any]]], key: tuple, ttl_seconds: int) -> Optional[Dict[str, Any]]:
    cached = cache.get(key)
    if ttl_seconds > 0 and cached and time() - cached[0] <= ttl_seconds:
        return cached[1]
    return None


def _memory_cache_put(
    cache: Dict[tuple, tuple[float, Dict[str, Any]]],
    key_locks: Dict[tuple, threading.Lock],
    key: tuple,
    payload: Dict[str, Any],
    *,
    max_items: int,
) -> None:
    cache[key] = (time(), payload)
    while len(cache) > max_items:
        oldest_key = min(cache, key=lambda cache_key: cache[cache_key][0])
        cache.pop(oldest_key, None)
        key_locks.pop(oldest_key, None)


def _get_cached_dashboard_data(
    *,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    target_return: Optional[float] = None,
    target_floor: Optional[float] = None,
    timing_recorder=None,
) -> Dict[str, Any]:
    ttl_seconds = _dashboard_data_cache_seconds()
    key = _dashboard_cache_key(
        as_of=as_of,
        include_unrealized=include_unrealized,
        target_return=target_return,
        target_floor=target_floor,
    )
    now = time()
    with _dashboard_data_cache_lock:
        cached = _dashboard_data_cache.get(key)
        if ttl_seconds > 0 and cached and now - cached[0] <= ttl_seconds:
            if timing_recorder is not None:
                timing_recorder("dashboard_data_cache_hit", 1)
            return cached[1]
        key_lock = _dashboard_data_key_locks.setdefault(key, threading.Lock())

    with key_lock:
        now = time()
        with _dashboard_data_cache_lock:
            cached = _dashboard_data_cache.get(key)
            if ttl_seconds > 0 and cached and now - cached[0] <= ttl_seconds:
                if timing_recorder is not None:
                    timing_recorder("dashboard_data_cache_hit", 1)
                return cached[1]
        if timing_recorder is not None:
            timing_recorder("dashboard_data_cache_hit", 0)

        payload = _build_dashboard_data(
            as_of=as_of,
            include_unrealized=include_unrealized,
            target_return=target_return,
            target_floor=target_floor,
            timing_recorder=timing_recorder,
        )
        if ttl_seconds > 0:
            with _dashboard_data_cache_lock:
                _dashboard_data_cache[key] = (time(), payload)
                while len(_dashboard_data_cache) > 8:
                    oldest_key = min(_dashboard_data_cache, key=lambda cache_key: _dashboard_data_cache[cache_key][0])
                    _dashboard_data_cache.pop(oldest_key, None)
                    _dashboard_data_key_locks.pop(oldest_key, None)
        return payload


def _assignment_quality_cache_key(payload: Dict[str, Any], *, as_of: Optional[date] = None) -> str:
    return derived_payload_key(
        "assignment_quality",
        {
            "assignment_quality_cache_version": 2,
            "source": _lab_source_cache_key(payload),
            "as_of": as_of.isoformat() if as_of else None,
        },
    )


def _decision_lab_cache_key(payload: Dict[str, Any]) -> str:
    web = payload.get("web") or {}
    return derived_payload_key(
        "decision_lab",
        {
            "decision_lab_cache_version": 4,
            "source": _lab_source_cache_key(payload),
            "include_unrealized": bool(web.get("include_unrealized")),
            "target_floor": round(float(web.get("target_floor") or _web_monthly_target_floor_default()), 8),
            "target_return": round(float(web.get("target_return") or _web_monthly_target_return_default()), 8),
        },
    )


def _get_cached_assignment_quality_data(
    *,
    source_payload: Optional[Dict[str, Any]] = None,
    as_of: Optional[date] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    if source_payload is None:
        source_payload = _get_cached_dashboard_data(as_of=as_of, include_unrealized=True)
    ttl_seconds = _lab_memory_cache_seconds()
    persistent_key = _assignment_quality_cache_key(source_payload, as_of=as_of)
    key = (persistent_key,)
    with _assignment_quality_cache_lock:
        cached = _memory_cache_get(_assignment_quality_cache, key, ttl_seconds)
        if cached is not None and not force_refresh:
            return cached
        key_lock = _assignment_quality_key_locks.setdefault(key, threading.Lock())

    with key_lock:
        with _assignment_quality_cache_lock:
            cached = _memory_cache_get(_assignment_quality_cache, key, ttl_seconds)
            if cached is not None and not force_refresh:
                return cached
        if not force_refresh:
            persistent = load_derived_payload(persistent_key)
            if persistent is not None:
                logger.info("assignment_quality_derived_cache_hit key=%s source=firestore", persistent_key)
                with _assignment_quality_cache_lock:
                    _memory_cache_put(_assignment_quality_cache, _assignment_quality_key_locks, key, persistent, max_items=4)
                return persistent

        payload = build_web_assignment_quality_data(as_of=as_of)
        save_derived_payload(
            persistent_key,
            payload,
            metadata={
                "namespace": "assignment_quality",
                "source_snapshot_id": _lab_source_cache_key(source_payload).get("source_snapshot_id"),
                "as_of": as_of.isoformat() if as_of else None,
            },
        )
        with _assignment_quality_cache_lock:
            _memory_cache_put(_assignment_quality_cache, _assignment_quality_key_locks, key, payload, max_items=4)
        return payload


def _probability_history_cache_seconds() -> int:
    raw = os.getenv("WEB_PROBABILITY_HISTORY_CACHE_SECONDS", "300")
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 300


def _load_probability_trade_matches() -> list[dict[str, Any]]:
    global _probability_history_cache
    ttl_seconds = _probability_history_cache_seconds()
    now = time()
    with _probability_history_cache_lock:
        if ttl_seconds > 0 and _probability_history_cache and now - _probability_history_cache[0] <= ttl_seconds:
            return _probability_history_cache[1]

    try:
        client = firestore_client()
        query = client.collection("option_probability_import_runs").order_by(
            "finished_at",
            direction="DESCENDING",
        ).limit(10)
        runs = [snapshot for snapshot in query.stream() if (snapshot.to_dict() or {}).get("status") == "succeeded"]
        if not runs:
            matches: list[dict[str, Any]] = []
        else:
            run_doc = runs[0].to_dict() or {}
            ids = [str(item) for item in run_doc.get("trade_match_ids", []) if item]
            refs = [client.collection("option_probability_trade_matches").document(doc_id) for doc_id in ids]
            matches = []
            for start in range(0, len(refs), 300):
                for snapshot in client.get_all(refs[start : start + 300]):
                    if snapshot.exists:
                        matches.append(snapshot.to_dict() or {})
    except Exception as exc:
        logger.warning("decision_lab_probability_history_load_failed error=%s", exc)
        matches = []

    with _probability_history_cache_lock:
        _probability_history_cache = (time(), matches)
    return matches


def _load_historical_option_enrichments() -> list[dict[str, Any]]:
    global _option_history_cache
    ttl_seconds = _probability_history_cache_seconds()
    now = time()
    with _option_history_cache_lock:
        if ttl_seconds > 0 and _option_history_cache and now - _option_history_cache[0] <= ttl_seconds:
            return _option_history_cache[1]

    try:
        store = FirestoreOptionMarketStore()
        run = store.load_latest_historical_enrichment_run(provider=CuteMarketsClient.provider)
        if not run:
            enrichments: list[dict[str, Any]] = []
        else:
            ids = [str(item) for item in run.get("enrichment_ids", []) if item]
            enrichments = store.load_historical_enrichments_by_ids(ids)
    except Exception as exc:
        logger.warning("decision_lab_historical_option_enrichment_load_failed error=%s", exc)
        enrichments = []

    with _option_history_cache_lock:
        _option_history_cache = (time(), enrichments)
    return enrichments


def _decision_option_store() -> FirestoreOptionMarketStore:
    return FirestoreOptionMarketStore()


def _decision_option_provider() -> CuteMarketsClient:
    return CuteMarketsClient()


def _decision_option_loader(*, force_refresh: bool = False):
    if os.getenv("DECISION_LAB_PROVIDER") == "marketdata":
        from portfolio_backend.option_market.marketdata_production import decision_loader
        return decision_loader(refresh=force_refresh)
    return decision_option_loader(
        store_factory=_decision_option_store,
        provider_factory=_decision_option_provider,
        force_refresh=force_refresh,
    )


def _build_decision_lab_payload(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
    probability_matches = _load_probability_trade_matches()
    historical_enrichments = _load_historical_option_enrichments()
    return build_decision_lab_data(
        payload,
        probability_matches=probability_matches,
        historical_enrichments=historical_enrichments,
        option_market_loader=_decision_option_loader(force_refresh=force_refresh),
    )


def _get_cached_decision_lab_payload(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
    if os.getenv("DECISION_LAB_PROVIDER") == "marketdata":
        # A small shared snapshot read sees background updates and session
        # rollovers immediately, without reusing a stale derived provider result.
        return _build_decision_lab_payload(payload, force_refresh=force_refresh)
    ttl_seconds = _lab_memory_cache_seconds()
    persistent_key = _decision_lab_cache_key(payload)
    key = (persistent_key,)
    with _decision_lab_cache_lock:
        cached = _memory_cache_get(_decision_lab_cache, key, ttl_seconds)
        if cached is not None and not force_refresh:
            return cached
        key_lock = _decision_lab_key_locks.setdefault(key, threading.Lock())

    with key_lock:
        with _decision_lab_cache_lock:
            cached = _memory_cache_get(_decision_lab_cache, key, ttl_seconds)
            if cached is not None and not force_refresh:
                return cached
        if not force_refresh:
            persistent = load_derived_payload(persistent_key)
            if persistent is not None:
                logger.info("decision_lab_derived_cache_hit key=%s source=firestore", persistent_key)
                with _decision_lab_cache_lock:
                    _memory_cache_put(_decision_lab_cache, _decision_lab_key_locks, key, persistent, max_items=4)
                return persistent

        payload_out = _build_decision_lab_payload(payload, force_refresh=force_refresh)
        save_derived_payload(
            persistent_key,
            payload_out,
            metadata={
                "namespace": "decision_lab",
                "source_snapshot_id": _lab_source_cache_key(payload).get("source_snapshot_id"),
                "option_source": ((payload_out.get("option_market_data") or {}).get("status") or {}).get("source"),
                "option_last_fetched_at": ((payload_out.get("option_market_data") or {}).get("status") or {}).get(
                    "last_fetched_at"
                ),
            },
        )
        with _decision_lab_cache_lock:
            _memory_cache_put(_decision_lab_cache, _decision_lab_key_locks, key, payload_out, max_items=4)
        return payload_out


def _with_decision_lab(payload: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(payload)
    enriched["decision_lab"] = {"deferred": True}
    return enriched
