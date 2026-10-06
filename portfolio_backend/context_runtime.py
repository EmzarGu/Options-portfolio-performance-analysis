"""Shared portfolio loading, refresh and snapshot coordination.

Both HTTP applications use the same runtime, caches and source-marker checks.
Financial calculations and existing refresh policies are unchanged.
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
import uuid
from collections import OrderedDict
from datetime import date, datetime
from inspect import Parameter, signature
from time import perf_counter, sleep
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException

import streamlit_app as dashboard_app
from portfolio_backend.ibkr import import_health
from portfolio_backend.ibkr.mobile_service import build_ibkr_mobile_payload_context
from portfolio_backend.ibkr.repository import load_flex_report_from_env
from portfolio_backend.market_calendar import previous_us_market_trading_day
from portfolio_backend.mobile_api_service import (
    MobilePayloadContext,
    MobilePayloadRequest,
    MobileServiceDependencies,
    build_mobile_payload_context,
)
from portfolio_backend.pipeline import (
    apply_live_price_overlay,
    apply_unrealized_adjusted_display,
    current_price_tickers_for_state,
)
from portfolio_backend.pipeline_snapshot_store import (
    get_default_pipeline_snapshot_store,
    pipeline_snapshot_id,
    snapshot_metadata_for_context,
)


logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)
DATA_SOURCE_GOOGLE_SHEETS = "google_sheets"
DATA_SOURCE_IBKR = "ibkr"
CONTEXT_CACHE_MAX_ITEMS = 16
DEFAULT_SOURCE_MARKER_CACHE_SECONDS = 30
DEFAULT_PIPELINE_BUILD_LEASE_SECONDS = 180
DEFAULT_PIPELINE_BUILD_WAIT_SECONDS = 90
DEFAULT_PIPELINE_BUILD_WAIT_POLL_SECONDS = 1.0
PRICE_ONLY_RELOAD_ENDPOINTS = [
    "/v1/mobile/dashboard",
    "/v1/mobile/positions",
    "/v1/mobile/assigned-holdings-review",
    "/v1/mobile/open-option-shorts",
    "/v1/mobile/tickers",
    "/v1/mobile/performance/monthly",
    "/v1/mobile/performance/yearly",
    "/v1/mobile/issues",
]
FULL_RELOAD_ENDPOINTS = [
    "/v1/mobile/dashboard",
    "/v1/mobile/positions",
    "/v1/mobile/assigned-holdings-review",
    "/v1/mobile/open-option-shorts",
    "/v1/mobile/tickers",
    "/v1/mobile/performance/monthly",
    "/v1/mobile/performance/yearly",
    "/v1/mobile/issues",
]
_context_cache_lock = threading.Lock()
_context_cache: "OrderedDict[Tuple[str, str, str, bool, Tuple[str, ...], int], Any]" = OrderedDict()
_context_build_locks: Dict[Tuple[str, str, str, bool, Tuple[str, ...], int], threading.Lock] = {}
_active_cache_bust = 1
_source_marker_cache_lock = threading.Lock()
_source_marker_cache: Dict[str, tuple[float, Optional[Dict[str, Any]]]] = {}


def _elapsed_ms(started_at: float) -> float:
    return (perf_counter() - started_at) * 1000


def _supports_timing_recorder(func) -> bool:
    try:
        parameters = signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "timing_recorder" or parameter.kind == Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _supports_keyword(func, keyword: str) -> bool:
    try:
        parameters = signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == keyword or parameter.kind == Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _options_source_hash(df) -> Optional[str]:
    if df is None:
        return None
    try:
        normalized = df.copy()
        normalized = normalized.reindex(sorted(normalized.columns), axis=1)
        row_hashes = pd.util.hash_pandas_object(normalized, index=False)
        digest = hashlib.sha256()
        digest.update("|".join(str(column) for column in normalized.columns).encode("utf-8"))
        digest.update(row_hashes.values.tobytes())
        return digest.hexdigest()
    except Exception as exc:
        logger.warning("source_hash_failed error=%s", exc)
        return None


def _sheet_row_counts(df) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True) or "source_sheet" not in df.columns:
        return []
    counts = df.groupby("source_sheet").size().reset_index(name="rows")
    return [
        {"name": str(row["source_sheet"]), "rows": int(row["rows"])}
        for row in counts.to_dict(orient="records")
    ]


def _source_snapshot_id(source_hash: Optional[str], selected_sheets: List[str]) -> Optional[str]:
    if not source_hash:
        return None
    sheets_key = ",".join(str(sheet) for sheet in selected_sheets)
    return hashlib.sha256(f"{source_hash}|{sheets_key}".encode("utf-8")).hexdigest()[:32]


def _dependencies(source_metadata: Optional[Dict[str, Any]] = None) -> MobileServiceDependencies:
    def load_options_with_metadata(sheet_id: str, sheets: List[str]):
        df = dashboard_app.load_options(sheet_id, sheets)
        if source_metadata is not None:
            download = None
            try:
                download = dashboard_app._download_excel(sheet_id)
            except Exception as exc:
                logger.warning("source_download_metadata_failed error=%s", exc)
            if download is not None:
                source_metadata.update(
                    {
                        "source_kind": getattr(download, "source", None),
                        "source_name": getattr(download, "file_name", None),
                        "source_downloaded_at": getattr(download, "downloaded_at", None),
                        "source_modified_at": getattr(download, "file_modified_at", None),
                        "source_version": getattr(download, "file_version", None),
                    }
                )
            source_hash = _options_source_hash(df)
            source_metadata.update(
                {
                    "source_content_hash": source_hash,
                    "source_row_count": int(len(df)),
                    "source_selected_sheets": [str(sheet) for sheet in sheets],
                    "source_sheet_counts": _sheet_row_counts(df),
                    "source_snapshot_id": _source_snapshot_id(source_hash, [str(sheet) for sheet in sheets]),
                }
            )
        return df

    return MobileServiceDependencies(
        load_options=load_options_with_metadata,
        fetch_price_history=dashboard_app.fetch_price_history_yf,
        collect_dividend_cashflows=dashboard_app.collect_dividend_cashflows,
        align_benchmarks_monthly=dashboard_app.align_benchmarks_monthly,
        fetch_current_prices=dashboard_app.fetch_current_prices_yf,
    )


def _data_source() -> str:
    value = os.getenv("OPTIONS_DATA_SOURCE", DATA_SOURCE_GOOGLE_SHEETS).strip().lower()
    if value in {"ibkr", "ibkr_flex"}:
        return DATA_SOURCE_IBKR
    return DATA_SOURCE_GOOGLE_SHEETS


def _available_sheets() -> List[str]:
    if _data_source() == DATA_SOURCE_IBKR:
        return ["IBKR Flex"]
    return dashboard_app.list_option_sheets(dashboard_app.SHEET_ID)


def _default_selected_sheets(available_sheets: List[str]) -> List[str]:
    if _data_source() == DATA_SOURCE_IBKR:
        return ["IBKR Flex"]
    prefs = dashboard_app.load_prefs()
    saved_sheets = [sheet for sheet in prefs.get("selected_sheets", []) if sheet in available_sheets]
    return saved_sheets or [sheet for sheet in available_sheets if sheet in dashboard_app.SHEETS] or available_sheets


def _normalize_selected_sheets(selected_sheets: Optional[List[str]], available_sheets: List[str]) -> List[str]:
    if _data_source() == DATA_SOURCE_IBKR:
        return ["IBKR Flex"]
    return selected_sheets or _default_selected_sheets(available_sheets)


def _common_request(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: int,
    timing_recorder=None,
) -> tuple[MobilePayloadRequest, List[str]]:
    started_at = perf_counter()
    available = _available_sheets()
    if timing_recorder is not None:
        timing_recorder("sheet_load_ms", _elapsed_ms(started_at))
    selected = _normalize_selected_sheets(selected_sheets, available)
    if not selected:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "no_selected_sheets",
                "message": "No selected option sheets are available.",
                "details": {"selected_sheets": selected, "available_sheets": available, "missing_sheets": []},
            },
        )
    return (
        MobilePayloadRequest(
            sheet_id="ibkr-flex" if _data_source() == DATA_SOURCE_IBKR else dashboard_app.SHEET_ID,
            as_of=as_of or _default_as_of_date(),
            selected_sheets=selected,
            include_unrealized=include_unrealized,
            cache_bust=cache_bust,
        ),
        available,
    )


def _default_as_of_date() -> date:
    today = date.today()
    if _data_source() == DATA_SOURCE_IBKR:
        return previous_us_market_trading_day(today)
    return today


def _resolve_cache_bust(cache_bust: Optional[int]) -> int:
    if cache_bust is not None:
        return int(cache_bust)
    with _context_cache_lock:
        return int(_active_cache_bust)


def _context_cache_key(request: MobilePayloadRequest) -> Tuple[str, str, str, bool, Tuple[str, ...], int]:
    return (
        _data_source(),
        request.sheet_id,
        request.as_of.isoformat(),
        bool(request.include_unrealized),
        tuple(str(sheet) for sheet in request.selected_sheets),
        int(request.cache_bust),
    )


def _cached_context_for_request(request: MobilePayloadRequest) -> Any:
    key = _context_cache_key(request)
    with _context_cache_lock:
        cached = _context_cache.get(key)
        if cached is not None:
            _context_cache.move_to_end(key)
        return cached


def _cached_context_for_marker(
    key: Tuple[str, str, str, bool, Tuple[str, ...], int],
    source_marker: Optional[Dict[str, Any]],
    *,
    timing_recorder=None,
) -> Optional[Any]:
    started_at = perf_counter()
    with _context_cache_lock:
        cached = _context_cache.get(key)
        if cached is not None and (_data_source() != DATA_SOURCE_IBKR or _source_marker_matches(cached, source_marker)):
            _context_cache.move_to_end(key)
            if timing_recorder is not None:
                timing_recorder("context_cache_lookup_ms", _elapsed_ms(started_at))
                timing_recorder("context_cache_hit", 1)
            return cached
        if cached is not None:
            _context_cache.pop(key, None)
    if timing_recorder is not None:
        timing_recorder("context_cache_lookup_ms", _elapsed_ms(started_at))
        timing_recorder("context_cache_hit", 0)
    return None


def _context_build_lock_for_key(
    key: Tuple[str, str, str, bool, Tuple[str, ...], int],
) -> threading.Lock:
    with _context_cache_lock:
        lock = _context_build_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _context_build_locks[key] = lock
        return lock


def _remember_context(key: Tuple[str, str, str, bool, Tuple[str, ...], int], context: Any) -> None:
    with _context_cache_lock:
        _context_cache[key] = context
        _context_cache.move_to_end(key)
        while len(_context_cache) > CONTEXT_CACHE_MAX_ITEMS:
            _context_cache.popitem(last=False)


def _set_active_cache_bust(cache_bust: int) -> None:
    global _active_cache_bust
    with _context_cache_lock:
        _active_cache_bust = int(cache_bust)


def _clear_context_cache() -> None:
    global _active_cache_bust
    with _context_cache_lock:
        _context_cache.clear()
        _context_build_locks.clear()
        _active_cache_bust = 1


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _source_marker_cache_seconds() -> int:
    value = os.getenv("SOURCE_MARKER_CACHE_SECONDS", str(DEFAULT_SOURCE_MARKER_CACHE_SECONDS)).strip()
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return DEFAULT_SOURCE_MARKER_CACHE_SECONDS


def _pipeline_build_lease_seconds() -> int:
    value = os.getenv("PIPELINE_BUILD_LEASE_SECONDS", str(DEFAULT_PIPELINE_BUILD_LEASE_SECONDS)).strip()
    try:
        return max(int(float(value)), 1)
    except ValueError:
        return DEFAULT_PIPELINE_BUILD_LEASE_SECONDS


def _pipeline_build_wait_seconds() -> int:
    value = os.getenv("PIPELINE_BUILD_WAIT_SECONDS", str(DEFAULT_PIPELINE_BUILD_WAIT_SECONDS)).strip()
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return DEFAULT_PIPELINE_BUILD_WAIT_SECONDS


def _pipeline_build_wait_poll_seconds() -> float:
    value = os.getenv("PIPELINE_BUILD_WAIT_POLL_SECONDS", str(DEFAULT_PIPELINE_BUILD_WAIT_POLL_SECONDS)).strip()
    try:
        return max(float(value), 0.1)
    except ValueError:
        return DEFAULT_PIPELINE_BUILD_WAIT_POLL_SECONDS


def _resolve_dependencies(source_metadata: Dict[str, Any]) -> MobileServiceDependencies:
    try:
        parameters = signature(_dependencies).parameters
    except (TypeError, ValueError):
        parameters = {}
    if parameters:
        return _dependencies(source_metadata)
    return _dependencies()


def _refresh_source_marker(timing_recorder=None) -> Optional[Dict[str, Any]]:
    """Return the newest successful IBKR import marker for smart refresh checks."""
    started_at = perf_counter()
    query_id = os.getenv("IBKR_FLEX_QUERY_ID", "").strip()
    if not query_id:
        if timing_recorder is not None:
            timing_recorder("source_check_ms", _elapsed_ms(started_at))
        return None
    cache_seconds = _source_marker_cache_seconds()
    if cache_seconds > 0:
        now = perf_counter()
        with _source_marker_cache_lock:
            cached = _source_marker_cache.get(query_id)
            if cached and now - cached[0] <= cache_seconds:
                if timing_recorder is not None:
                    timing_recorder("source_marker_cache_hit", 1)
                    timing_recorder("source_check_ms", _elapsed_ms(started_at))
                return dict(cached[1]) if cached[1] is not None else None
    if timing_recorder is not None:
        timing_recorder("source_marker_cache_hit", 0)
    marker: Optional[Dict[str, Any]] = None
    try:
        from portfolio_backend.gcp import firestore_client

        client = firestore_client()
        metadata_snap = client.collection("app_metadata").document(f"ibkr_latest_import_{query_id}").get()
        if metadata_snap.exists:
            doc = metadata_snap.to_dict() or {}
            if str(doc.get("status")) == "succeeded":
                latest = import_health._ibkr_import_marker_from_doc(doc, fallback_id=metadata_snap.id, query_id=query_id)
                if latest is not None:
                    marker = import_health._with_ibkr_import_health(client, query_id, latest)
                    if cache_seconds > 0:
                        with _source_marker_cache_lock:
                            _source_marker_cache[query_id] = (perf_counter(), dict(marker) if marker is not None else None)
                    return marker

        try:
            from google.cloud.firestore_v1 import FieldFilter

            docs = (
                client.collection("ibkr_import_runs")
                .where(filter=FieldFilter("query_id", "==", str(query_id)))
                .stream()
            )
        except Exception:
            docs = client.collection("ibkr_import_runs").where("query_id", "==", str(query_id)).stream()
        latest: Optional[Dict[str, Any]] = None
        for snap in docs:
            doc = snap.to_dict() or {}
            if str(doc.get("status")) != "succeeded":
                continue
            candidate = import_health._ibkr_import_marker_from_doc(doc, fallback_id=snap.id, query_id=query_id)
            if candidate is not None and (
                latest is None or str(candidate.get("finished_at") or "") > str(latest.get("finished_at") or "")
            ):
                latest = candidate
        if latest is None:
            return None
        try:
            client.collection("app_metadata").document(f"ibkr_latest_import_{query_id}").set(latest, merge=True)
        except Exception as exc:
            logger.warning("ibkr_refresh_marker_cache_write_failed error=%s", exc)
        marker = import_health._with_ibkr_import_health(client, query_id, latest)
        if cache_seconds > 0:
            with _source_marker_cache_lock:
                _source_marker_cache[query_id] = (perf_counter(), dict(marker) if marker is not None else None)
        return marker
    except Exception as exc:
        logger.warning("ibkr_refresh_source_check_failed error=%s", exc)
        return None
    finally:
        if timing_recorder is not None:
            timing_recorder("source_check_ms", _elapsed_ms(started_at))


def _source_metadata_for_marker(marker: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not marker:
        return {}
    return {
        "source_kind": "ibkr_flex",
        "source_name": "IBKR Flex",
        "source_version": marker.get("query_id"),
        "source_downloaded_at": marker.get("finished_at"),
        "source_modified_at": marker.get("finished_at"),
        "source_snapshot_id": marker.get("source_snapshot_id"),
        "source_selected_sheets": ["IBKR Flex"],
        "source_sheet_counts": [{"name": "IBKR Flex", "rows": None}],
        "ibkr_import_run_id": marker.get("import_run_id"),
        "ibkr_import_finished_at": marker.get("finished_at"),
        "ibkr_import_from_date": marker.get("from_date"),
        "ibkr_import_to_date": marker.get("to_date"),
        "ibkr_import_health": marker.get("import_health") or {},
        "ibkr_import_issues": (marker.get("import_health") or {}).get("issues") or [],
    }


def _source_marker_matches(context: Any, marker: Optional[Dict[str, Any]]) -> bool:
    if not marker:
        return False
    metadata = dict(getattr(context, "source_metadata", {}) or {})
    return (
        str(metadata.get("source_snapshot_id") or "") == str(marker.get("source_snapshot_id") or "")
        and str(metadata.get("ibkr_import_run_id") or "") == str(marker.get("import_run_id") or "")
    )


def _source_marker_from_metadata(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_snapshot_id = metadata.get("source_snapshot_id")
    if not source_snapshot_id:
        return None
    return {
        "source_snapshot_id": source_snapshot_id,
        "import_run_id": metadata.get("ibkr_import_run_id"),
        "finished_at": metadata.get("ibkr_import_finished_at") or metadata.get("source_modified_at"),
        "from_date": metadata.get("ibkr_import_from_date"),
        "to_date": metadata.get("ibkr_import_to_date"),
        "query_id": metadata.get("source_version"),
    }


def _pipeline_snapshot_id_for_request(
    source_marker: Optional[Dict[str, Any]],
    request: MobilePayloadRequest,
) -> Optional[str]:
    source_snapshot_id = (source_marker or {}).get("source_snapshot_id")
    if not source_snapshot_id:
        return None
    return pipeline_snapshot_id(
        source_snapshot_id=str(source_snapshot_id),
        as_of=request.as_of,
        selected_sheets=request.selected_sheets,
    )


def _save_pipeline_snapshot(
    context: Any,
    *,
    request: MobilePayloadRequest,
    available: List[str],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
) -> None:
    base_state = getattr(context, "base_state", None)
    snapshot_id = _pipeline_snapshot_id_for_request(source_marker, request)
    if base_state is None or snapshot_id is None:
        return
    started_at = perf_counter()
    try:
        get_default_pipeline_snapshot_store().save(
            snapshot_id,
            base_state,
            snapshot_metadata_for_context(
                source_marker=source_marker,
                request=request,
                available_sheets=available,
            ),
        )
    except Exception as exc:
        logger.warning("pipeline_snapshot_write_failed snapshot_id=%s error=%s", snapshot_id, exc)
    finally:
        if timing_recorder is not None:
            timing_recorder("pipeline_snapshot_write_ms", _elapsed_ms(started_at))


def _load_pipeline_snapshot_context(
    *,
    request: MobilePayloadRequest,
    available: List[str],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
) -> Optional[MobilePayloadContext]:
    snapshot_id = _pipeline_snapshot_id_for_request(source_marker, request)
    if snapshot_id is None:
        return None
    started_at = perf_counter()
    try:
        snapshot = get_default_pipeline_snapshot_store().load(snapshot_id)
    except Exception as exc:
        logger.warning("pipeline_snapshot_load_failed snapshot_id=%s error=%s", snapshot_id, exc)
        snapshot = None
    finally:
        if timing_recorder is not None:
            timing_recorder("pipeline_snapshot_lookup_ms", _elapsed_ms(started_at))

    if snapshot is None:
        if timing_recorder is not None:
            timing_recorder("pipeline_snapshot_hit", 0)
        return None
    if timing_recorder is not None:
        timing_recorder("pipeline_snapshot_hit", 1)
    metadata = _source_metadata_for_marker(source_marker)
    metadata["pipeline_snapshot_id"] = snapshot.snapshot_id
    metadata["pipeline_snapshot_created_at"] = snapshot.metadata.get("created_at")
    return MobilePayloadContext(
        state=snapshot.state,
        request={
            "as_of": request.as_of,
            "include_unrealized": request.include_unrealized,
            "selected_sheets": request.selected_sheets,
        },
        available_sheets=[str(sheet) for sheet in available] if available is not None else None,
        source_metadata=metadata,
        base_state=snapshot.state,
    )


def _load_and_refresh_pipeline_snapshot_context(
    *,
    request: MobilePayloadRequest,
    available: List[str],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
) -> Optional[MobilePayloadContext]:
    snapshot_context = _load_pipeline_snapshot_context(
        request=request,
        available=available,
        source_marker=source_marker,
        timing_recorder=timing_recorder,
    )
    if snapshot_context is None:
        return None
    return _refresh_prices_from_cached_base(
        snapshot_context,
        request=request,
        available=available,
        source_marker=source_marker,
        timing_recorder=timing_recorder,
    )


def _wait_for_pipeline_snapshot_context(
    *,
    request: MobilePayloadRequest,
    available: List[str],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
) -> Optional[MobilePayloadContext]:
    wait_seconds = _pipeline_build_wait_seconds()
    if wait_seconds <= 0:
        return None
    started_at = perf_counter()
    deadline = started_at + wait_seconds
    poll_seconds = _pipeline_build_wait_poll_seconds()
    while perf_counter() < deadline:
        sleep(poll_seconds)
        context = _load_and_refresh_pipeline_snapshot_context(
            request=request,
            available=available,
            source_marker=source_marker,
            timing_recorder=timing_recorder,
        )
        if context is not None:
            if timing_recorder is not None:
                timing_recorder("pipeline_snapshot_wait_ms", _elapsed_ms(started_at))
                timing_recorder("pipeline_snapshot_wait_hit", 1)
            return context
    if timing_recorder is not None:
        timing_recorder("pipeline_snapshot_wait_ms", _elapsed_ms(started_at))
        timing_recorder("pipeline_snapshot_wait_hit", 0)
    return None


def _pipeline_build_lease_id(snapshot_id: str) -> str:
    return f"pipeline_snapshot_build:{snapshot_id}"


def _pipeline_build_owner_id() -> str:
    revision = os.getenv("K_REVISION") or "local"
    return f"{revision}:{os.getpid()}:{threading.get_ident()}:{uuid.uuid4().hex}"


def _refresh_prices_from_cached_base(
    context: MobilePayloadContext,
    *,
    request: MobilePayloadRequest,
    available: List[str],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
) -> Optional[MobilePayloadContext]:
    base_state = getattr(context, "base_state", None)
    if base_state is None:
        return None
    metadata = dict(getattr(context, "source_metadata", {}) or {})
    metadata.update(_source_metadata_for_marker(source_marker))
    dependencies = _resolve_dependencies(metadata)
    if dependencies.fetch_current_prices is None:
        return None

    started_at = perf_counter()
    tickers = list(current_price_tickers_for_state(base_state))
    if timing_recorder is not None:
        timing_recorder("price_ticker_resolution_ms", _elapsed_ms(started_at))

    started_at = perf_counter()
    live_prices, price_errors, price_summary = dependencies.fetch_current_prices(tickers)
    if timing_recorder is not None:
        timing_recorder("price_fetch_ms", _elapsed_ms(started_at))

    prices_updated_at = _now_iso()
    metadata["prices_updated_at"] = prices_updated_at
    started_at = perf_counter()
    state = apply_live_price_overlay(
        base_state,
        live_prices,
        price_errors,
        price_summary,
        prices_updated_at,
    )
    if timing_recorder is not None:
        timing_recorder("price_overlay_ms", _elapsed_ms(started_at))

    started_at = perf_counter()
    state = apply_unrealized_adjusted_display(state, request.include_unrealized)
    if timing_recorder is not None:
        timing_recorder("unrealized_adjustment_ms", _elapsed_ms(started_at))

    return MobilePayloadContext(
        state=state,
        request={
            "as_of": request.as_of,
            "include_unrealized": request.include_unrealized,
            "selected_sheets": request.selected_sheets,
        },
        available_sheets=[str(sheet) for sheet in available] if available is not None else None,
        source_metadata=metadata,
        base_state=base_state,
    )


def _build_context_uncached(
    *,
    request: MobilePayloadRequest,
    available: List[str],
    key: Tuple[str, str, str, bool, Tuple[str, ...], int],
    use_memory_cache: bool,
    force_rebuild: bool,
    source_metadata: Dict[str, Any],
    source_marker: Optional[Dict[str, Any]],
    timing_recorder=None,
):
    started_at = perf_counter()
    build_lease_id: Optional[str] = None
    build_lease_owner: Optional[str] = None
    build_lease_acquired = False
    if _data_source() == DATA_SOURCE_IBKR:
        if "source_snapshot_id" not in source_metadata:
            source_marker = _refresh_source_marker(timing_recorder=timing_recorder)
            source_metadata.update(_source_metadata_for_marker(source_marker))
        if not force_rebuild:
            refreshed_context = _load_and_refresh_pipeline_snapshot_context(
                request=request,
                available=available,
                source_marker=source_marker,
                timing_recorder=timing_recorder,
            )
            if refreshed_context is not None:
                if timing_recorder is not None:
                    timing_recorder("context_build_total_ms", _elapsed_ms(started_at))
                _remember_context(key, refreshed_context)
                return refreshed_context

            snapshot_id = _pipeline_snapshot_id_for_request(source_marker, request)
            if snapshot_id is not None:
                build_lease_id = _pipeline_build_lease_id(snapshot_id)
                build_lease_owner = _pipeline_build_owner_id()
                store = get_default_pipeline_snapshot_store()
                lease_started_at = perf_counter()
                try:
                    build_lease_acquired = store.try_acquire_build_lease(
                        build_lease_id,
                        build_lease_owner,
                        ttl_seconds=_pipeline_build_lease_seconds(),
                    )
                except Exception as exc:
                    logger.warning("pipeline_snapshot_build_lease_failed lease_id=%s error=%s", build_lease_id, exc)
                    build_lease_acquired = True
                if timing_recorder is not None:
                    timing_recorder("pipeline_snapshot_build_lease_ms", _elapsed_ms(lease_started_at))
                    timing_recorder("pipeline_snapshot_build_lease_acquired", 1 if build_lease_acquired else 0)

                if not build_lease_acquired:
                    waited_context = _wait_for_pipeline_snapshot_context(
                        request=request,
                        available=available,
                        source_marker=source_marker,
                        timing_recorder=timing_recorder,
                    )
                    if waited_context is not None:
                        if timing_recorder is not None:
                            timing_recorder("context_build_total_ms", _elapsed_ms(started_at))
                        _remember_context(key, waited_context)
                        return waited_context
                    lease_started_at = perf_counter()
                    try:
                        build_lease_acquired = store.try_acquire_build_lease(
                            build_lease_id,
                            build_lease_owner,
                            ttl_seconds=_pipeline_build_lease_seconds(),
                        )
                    except Exception as exc:
                        logger.warning(
                            "pipeline_snapshot_build_lease_retry_failed lease_id=%s error=%s",
                            build_lease_id,
                            exc,
                        )
                        build_lease_acquired = True
                    if timing_recorder is not None:
                        timing_recorder("pipeline_snapshot_build_lease_retry_ms", _elapsed_ms(lease_started_at))
                        timing_recorder("pipeline_snapshot_build_lease_retry_acquired", 1 if build_lease_acquired else 0)
        context_kwargs = {"available_sheets": available}
        if _supports_keyword(build_ibkr_mobile_payload_context, "source_metadata"):
            context_kwargs["source_metadata"] = source_metadata
        if timing_recorder is not None and _supports_timing_recorder(build_ibkr_mobile_payload_context):
            context_kwargs["timing_recorder"] = timing_recorder
        try:
            context = build_ibkr_mobile_payload_context(
                request,
                _resolve_dependencies(source_metadata),
                load_flex_report_from_env(),
                **context_kwargs,
            )
        except Exception:
            if build_lease_acquired and build_lease_id and build_lease_owner:
                try:
                    get_default_pipeline_snapshot_store().release_build_lease(build_lease_id, build_lease_owner)
                except Exception as exc:
                    logger.warning("pipeline_snapshot_build_lease_release_failed lease_id=%s error=%s", build_lease_id, exc)
            raise
    else:
        context_kwargs = {"available_sheets": available}
        if _supports_keyword(build_mobile_payload_context, "source_metadata"):
            context_kwargs["source_metadata"] = source_metadata
        if timing_recorder is not None and _supports_timing_recorder(build_mobile_payload_context):
            context_kwargs["timing_recorder"] = timing_recorder
        context = build_mobile_payload_context(
            request,
            _resolve_dependencies(source_metadata),
            **context_kwargs,
        )
    if timing_recorder is not None:
        timing_recorder("context_build_total_ms", _elapsed_ms(started_at))
    if use_memory_cache:
        _remember_context(key, context)
    if _data_source() == DATA_SOURCE_IBKR:
        try:
            _save_pipeline_snapshot(
                context,
                request=request,
                available=available,
                source_marker=source_marker or _source_marker_from_metadata(dict(getattr(context, "source_metadata", {}) or {})),
                timing_recorder=timing_recorder,
            )
        finally:
            if build_lease_acquired and build_lease_id and build_lease_owner:
                try:
                    get_default_pipeline_snapshot_store().release_build_lease(build_lease_id, build_lease_owner)
                except Exception as exc:
                    logger.warning("pipeline_snapshot_build_lease_release_failed lease_id=%s error=%s", build_lease_id, exc)
    return context


def get_context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    force_rebuild: bool = False,
    timing_recorder=None,
    source_metadata_override: Optional[Dict[str, Any]] = None,
):
    """Load the shared context, validating its source marker before cache reuse."""
    request, available = _common_request(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=_resolve_cache_bust(cache_bust),
        timing_recorder=timing_recorder,
    )
    key = _context_cache_key(request)
    use_memory_cache = True
    source_metadata: Dict[str, Any] = dict(source_metadata_override or {})
    source_marker: Optional[Dict[str, Any]] = _source_marker_from_metadata(source_metadata)
    if _data_source() == DATA_SOURCE_IBKR and not force_rebuild and "source_snapshot_id" not in source_metadata:
        source_marker = _refresh_source_marker(timing_recorder=timing_recorder)
        source_metadata.update(_source_metadata_for_marker(source_marker))

    if use_memory_cache and not force_rebuild:
        cached = _cached_context_for_marker(key, source_marker, timing_recorder=timing_recorder)
        if cached is not None:
            return cached
    elif timing_recorder is not None:
        timing_recorder("context_cache_hit", 0)

    build_lock = _context_build_lock_for_key(key)
    lock_started_at = perf_counter()
    with build_lock:
        if timing_recorder is not None:
            timing_recorder("context_build_lock_wait_ms", _elapsed_ms(lock_started_at))
        if use_memory_cache and not force_rebuild:
            cached = _cached_context_for_marker(key, source_marker, timing_recorder=timing_recorder)
            if cached is not None:
                return cached
        return _build_context_uncached(
            request=request,
            available=available,
            key=key,
            use_memory_cache=use_memory_cache,
            force_rebuild=force_rebuild,
            source_metadata=source_metadata,
            source_marker=source_marker,
            timing_recorder=timing_recorder,
        )


def refresh_context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    timing_recorder=None,
) -> Tuple[Any, int, Dict[str, Any]]:
    """Refresh prices from a valid base snapshot, rebuilding only when required."""
    resolved_cache_bust = cache_bust if cache_bust is not None else _refresh_cache_bust()
    if _data_source() != DATA_SOURCE_IBKR:
        context = get_context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=resolved_cache_bust,
            force_rebuild=True,
            timing_recorder=timing_recorder,
        )
        _set_active_cache_bust(resolved_cache_bust)
        return (
            context,
            resolved_cache_bust,
            {
                "scope": "full",
                "pipeline_refreshed": True,
                "prices_refreshed": bool((getattr(context, "source_metadata", {}) or {}).get("prices_updated_at")),
                "source_checked": False,
                "source_changed": True,
                "reload_endpoints": FULL_RELOAD_ENDPOINTS,
            },
        )

    source_marker = _refresh_source_marker(timing_recorder=timing_recorder)
    active_request, active_available = _common_request(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=resolved_cache_bust,
        timing_recorder=timing_recorder,
    )

    snapshot_context = _load_pipeline_snapshot_context(
        request=active_request,
        available=active_available,
        source_marker=source_marker,
        timing_recorder=timing_recorder,
    )
    if snapshot_context is not None:
        refreshed_context = _refresh_prices_from_cached_base(
            snapshot_context,
            request=MobilePayloadRequest(
                sheet_id=active_request.sheet_id,
                as_of=active_request.as_of,
                selected_sheets=active_request.selected_sheets,
                include_unrealized=active_request.include_unrealized,
                cache_bust=resolved_cache_bust,
            ),
            available=active_available,
            source_marker=source_marker,
            timing_recorder=timing_recorder,
        )
        if refreshed_context is not None:
            refresh_metadata = {
                "scope": "prices_only",
                "pipeline_refreshed": False,
                "prices_refreshed": bool(refreshed_context.source_metadata.get("prices_updated_at")),
                "source_checked": True,
                "source_changed": False,
                "source_snapshot_id": (source_marker or {}).get("source_snapshot_id"),
                "pipeline_snapshot_id": snapshot_context.source_metadata.get("pipeline_snapshot_id"),
                "reload_endpoints": PRICE_ONLY_RELOAD_ENDPOINTS,
            }
            _set_active_cache_bust(resolved_cache_bust)
            _remember_context(_context_cache_key(active_request), refreshed_context)
            return (refreshed_context, resolved_cache_bust, refresh_metadata)

    if timing_recorder is not None:
        timing_recorder("context_cache_hit", 0)
    context = get_context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=resolved_cache_bust,
        force_rebuild=True,
        timing_recorder=timing_recorder,
        source_metadata_override=_source_metadata_for_marker(source_marker),
    )
    context.source_metadata.update(_source_metadata_for_marker(source_marker))
    refresh_metadata = {
        "scope": "full",
        "pipeline_refreshed": True,
        "prices_refreshed": bool(context.source_metadata.get("prices_updated_at")),
        "source_checked": source_marker is not None,
        "source_changed": True,
        "source_snapshot_id": (source_marker or {}).get("source_snapshot_id"),
        "reload_endpoints": FULL_RELOAD_ENDPOINTS,
    }
    _set_active_cache_bust(resolved_cache_bust)
    return (context, resolved_cache_bust, refresh_metadata)


def _refresh_cache_bust() -> int:
    return int(datetime.now().timestamp())
