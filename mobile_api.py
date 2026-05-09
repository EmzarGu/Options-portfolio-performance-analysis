from __future__ import annotations

import threading
import hashlib
import hmac
import logging
import os
from collections import OrderedDict
from datetime import date, datetime
from inspect import Parameter, signature
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

try:
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.responses import JSONResponse
except ModuleNotFoundError as exc:  # pragma: no cover - exercised only when dependency is absent.
    raise ModuleNotFoundError(
        "FastAPI is required to run mobile_api.py. Install dependencies with `pip install -r requirements.txt`."
    ) from exc

import streamlit_app as dashboard_app
import pandas as pd
from portfolio_backend.audit_store import RefreshAuditRecord, get_default_audit_store
from portfolio_backend.mobile_api_service import (
    MobilePayloadRequest,
    MobileServiceDependencies,
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_payload_context,
    build_mobile_positions_payload,
    build_mobile_refresh_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)
from portfolio_backend.mobile_payloads import build_mobile_config
from portfolio_backend.ibkr.mobile_service import build_ibkr_mobile_payload_context
from portfolio_backend.ibkr.repository import load_flex_report_from_env


app = FastAPI(title="Options ROI Mobile API", version="0.1.0")
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

MONTHLY_RANGES = {"3m", "6m", "ytd", "1y", "since_inception"}
OPEN_OPTION_SORTS = {"moneyness_risk", "expiration", "ticker", "moneyness_pct"}
SERVICE_NAME = "options-roi-mobile-api"
DATA_SOURCE_GOOGLE_SHEETS = "google_sheets"
DATA_SOURCE_IBKR = "ibkr"
CONTEXT_CACHE_MAX_ITEMS = 16
PUBLIC_PATHS = {"/v1/mobile/health"}

_context_cache_lock = threading.Lock()
_context_cache: "OrderedDict[Tuple[str, str, str, bool, Tuple[str, ...], int], Any]" = OrderedDict()
_active_cache_bust = 1


def _elapsed_ms(started_at: float) -> float:
    return (perf_counter() - started_at) * 1000


def _request_timings(request: Request) -> Dict[str, float]:
    timings = getattr(request.state, "mobile_timings", None)
    if timings is None:
        timings = {}
        request.state.mobile_timings = timings
    return timings


def _record_timing(request: Request, phase: str, elapsed_ms: float) -> None:
    _request_timings(request)[phase] = round(float(elapsed_ms), 2)


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


def _timing_recorder(request: Request):
    def record(phase: str, elapsed_ms: float) -> None:
        _record_timing(request, phase, elapsed_ms)

    return record


def _log_request_timing(
    request: Request,
    *,
    status_code: int,
    total_ms: float,
    cache_hit: Optional[bool] = None,
) -> None:
    if not request.url.path.startswith("/v1/mobile/"):
        return
    timings = dict(getattr(request.state, "mobile_timings", {}) or {})
    route_ms = float(timings.get("route_total_ms", 0) or 0)
    auth_ms = float(timings.get("request_auth_ms", 0) or 0)
    response_serialization_ms = max(float(total_ms) - route_ms - auth_ms, 0)
    parts = [
        "mobile_api_timing",
        f"method={request.method}",
        f"path={request.url.path}",
        f"status={status_code}",
        f"total_ms={total_ms:.2f}",
        f"response_serialization_ms={response_serialization_ms:.2f}",
    ]
    if cache_hit is not None:
        parts.append(f"cache_hit={str(cache_hit).lower()}")
    for key in sorted(timings):
        parts.append(f"{key}={timings[key]}")
    logger.info(" ".join(parts))


@app.middleware("http")
async def mobile_api_key_middleware(request: Request, call_next):
    request_started_at = perf_counter()
    auth_started_at = perf_counter()
    _request_timings(request)
    expected_key = os.getenv("MOBILE_API_KEY")
    if not expected_key or request.url.path in PUBLIC_PATHS or not request.url.path.startswith("/v1/mobile/"):
        _record_timing(request, "request_auth_ms", _elapsed_ms(auth_started_at))
        response = await call_next(request)
        _log_request_timing(
            request,
            status_code=response.status_code,
            total_ms=_elapsed_ms(request_started_at),
        )
        return response

    provided_key = request.headers.get("x-api-key", "")
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        provided_key = authorization[7:].strip()

    if hmac.compare_digest(provided_key, expected_key):
        _record_timing(request, "request_auth_ms", _elapsed_ms(auth_started_at))
        response = await call_next(request)
        _log_request_timing(
            request,
            status_code=response.status_code,
            total_ms=_elapsed_ms(request_started_at),
        )
        return response

    _record_timing(request, "request_auth_ms", _elapsed_ms(auth_started_at))
    response = JSONResponse(
        status_code=401,
        content={
            "error": {
                "code": "unauthorized",
                "message": "A valid mobile API key is required.",
                "details": {},
                "request_id": None,
            }
        },
    )
    _log_request_timing(
        request,
        status_code=response.status_code,
        total_ms=_elapsed_ms(request_started_at),
    )
    return response


@app.exception_handler(HTTPException)
def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "http_error", "message": str(exc.detail)}
    if "message" not in detail:
        detail = {**detail, "message": str(detail.get("code", "Request failed."))}
    detail.setdefault("details", {})
    detail.setdefault("request_id", None)
    return JSONResponse(status_code=exc.status_code, content={"error": detail})


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
            as_of=as_of or date.today(),
            selected_sheets=selected,
            include_unrealized=include_unrealized,
            cache_bust=cache_bust,
        ),
        available,
    )


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
        _active_cache_bust = 1


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _resolve_dependencies(source_metadata: Dict[str, Any]) -> MobileServiceDependencies:
    try:
        parameters = signature(_dependencies).parameters
    except (TypeError, ValueError):
        parameters = {}
    if parameters:
        return _dependencies(source_metadata)
    return _dependencies()


def _context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    force_rebuild: bool = False,
    timing_recorder=None,
):
    request, available = _common_request(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=_resolve_cache_bust(cache_bust),
        timing_recorder=timing_recorder,
    )
    key = _context_cache_key(request)
    if not force_rebuild:
        started_at = perf_counter()
        with _context_cache_lock:
            cached = _context_cache.get(key)
            if cached is not None:
                _context_cache.move_to_end(key)
                if timing_recorder is not None:
                    timing_recorder("context_cache_lookup_ms", _elapsed_ms(started_at))
                    timing_recorder("context_cache_hit", 1)
                return cached
        if timing_recorder is not None:
            timing_recorder("context_cache_lookup_ms", _elapsed_ms(started_at))
            timing_recorder("context_cache_hit", 0)
    elif timing_recorder is not None:
        timing_recorder("context_cache_hit", 0)

    started_at = perf_counter()
    source_metadata: Dict[str, Any] = {}
    if _data_source() == DATA_SOURCE_IBKR:
        context_kwargs = {"available_sheets": available}
        if _supports_keyword(build_ibkr_mobile_payload_context, "source_metadata"):
            context_kwargs["source_metadata"] = source_metadata
        context = build_ibkr_mobile_payload_context(
            request,
            _resolve_dependencies(source_metadata),
            load_flex_report_from_env(),
            **context_kwargs,
        )
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
    _remember_context(key, context)
    return context


def _record_refresh_audit(
    *,
    request: Request,
    context,
    payload: Dict[str, Any],
    cache_bust: int,
    started_at: str,
    finished_at: str,
) -> None:
    source_metadata = dict(getattr(context, "source_metadata", {}) or {})
    source_snapshot_id = source_metadata.get("source_snapshot_id")
    store = get_default_audit_store()
    try:
        if source_snapshot_id:
            store.upsert_source_snapshot(
                str(source_snapshot_id),
                {
                    "schema_version": 1,
                    "snapshot_id": source_snapshot_id,
                    "content_hash": source_metadata.get("source_content_hash"),
                    "source_kind": source_metadata.get("source_kind"),
                    "source_name": source_metadata.get("source_name"),
                    "source_version": source_metadata.get("source_version"),
                    "source_downloaded_at": source_metadata.get("source_downloaded_at"),
                    "source_modified_at": source_metadata.get("source_modified_at"),
                    "selected_sheets": source_metadata.get("source_selected_sheets"),
                    "sheet_counts": source_metadata.get("source_sheet_counts"),
                    "row_count": source_metadata.get("source_row_count"),
                    "last_seen_at": finished_at,
                },
            )
        refresh = payload.get("refresh", {}) if isinstance(payload, dict) else {}
        store.record_refresh_run(
            RefreshAuditRecord(
                run_id=f"mobile-refresh:{int(cache_bust)}",
                started_at=started_at,
                finished_at=finished_at,
                status=str(refresh.get("status") or "unknown"),
                request=payload.get("request", {}) if isinstance(payload, dict) else {},
                data_freshness=payload.get("data_freshness", {}) if isinstance(payload, dict) else {},
                refresh=refresh,
                timings_ms=dict(getattr(request.state, "mobile_timings", {}) or {}),
                source_snapshot_id=str(source_snapshot_id) if source_snapshot_id else None,
            )
        )
    except Exception as exc:
        logger.warning("refresh_audit_write_failed error=%s", exc)


def _refresh_cache_bust() -> int:
    return int(datetime.now().timestamp())


@app.get("/v1/mobile/health")
def get_mobile_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "version": app.version,
    }


@app.get("/v1/mobile/config")
def get_mobile_config() -> Dict[str, Any]:
    available = _available_sheets()
    prefs = dashboard_app.load_prefs()
    if _data_source() == DATA_SOURCE_IBKR:
        prefs = {**prefs, "selected_sheets": ["IBKR Flex"]}
    return build_mobile_config(
        available,
        prefs,
        default_sheets=available if _data_source() == DATA_SOURCE_IBKR else dashboard_app.SHEETS,
        as_of_default=date.today(),
        source_kind="ibkr_flex" if _data_source() == DATA_SOURCE_IBKR else "google_sheet",
        source_name="IBKR Flex" if _data_source() == DATA_SOURCE_IBKR else "Google Sheets",
        supports_selected_sheets=_data_source() != DATA_SOURCE_IBKR,
    )


@app.post("/v1/mobile/refresh")
def refresh_mobile_payloads(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    route_started_at = perf_counter()
    audit_started_at = _now_iso()
    resolved_cache_bust = cache_bust if cache_bust is not None else _refresh_cache_bust()
    context = _context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=resolved_cache_bust,
        force_rebuild=True,
        timing_recorder=_timing_recorder(request),
    )
    _set_active_cache_bust(resolved_cache_bust)
    started_at = perf_counter()
    payload = build_mobile_refresh_payload(
        context,
        cache_bust=resolved_cache_bust,
    )
    _record_timing(request, "dto_build_ms", _elapsed_ms(started_at))
    _record_timing(request, "route_total_ms", _elapsed_ms(route_started_at))
    _record_refresh_audit(
        request=request,
        context=context,
        payload=payload,
        cache_bust=resolved_cache_bust,
        started_at=audit_started_at,
        finished_at=_now_iso(),
    )
    return payload


@app.get("/v1/mobile/dashboard")
def get_mobile_dashboard(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_dashboard_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        )
    )


@app.get("/v1/mobile/positions")
def get_mobile_positions(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_positions_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        )
    )


@app.get("/v1/mobile/open-option-shorts")
def get_mobile_open_option_shorts(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    sort: str = "moneyness_risk",
    limit: Optional[int] = None,
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    if sort not in OPEN_OPTION_SORTS:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_open_option_sort",
                "message": f"Unsupported open option sort: {sort}",
                "details": {"allowed": sorted(OPEN_OPTION_SORTS), "received": sort},
            },
        )
    if limit is not None and limit < 0:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_limit",
                "message": "limit must be greater than or equal to 0.",
                "details": {"received": limit},
            },
        )
    return build_mobile_open_option_shorts_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        ),
        sort=sort,
        limit=limit,
    )


@app.get("/v1/mobile/tickers")
def get_mobile_tickers(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    year: Optional[int] = None,
    include_history: bool = False,
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_tickers_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        ),
        year=year,
        include_history=include_history,
    )


@app.get("/v1/mobile/performance/monthly")
def get_mobile_monthly_performance(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    target_return: float = 0.015,
    range: str = "ytd",
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    if range not in MONTHLY_RANGES:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_monthly_range",
                "message": f"Unsupported monthly range: {range}",
                "details": {"allowed": sorted(MONTHLY_RANGES), "received": range},
            },
        )
    return build_mobile_monthly_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        ),
        target_return=target_return,
        monthly_range=range,
    )


@app.get("/v1/mobile/performance/yearly")
def get_mobile_yearly_performance(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_yearly_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        )
    )


@app.get("/v1/mobile/issues")
def get_mobile_issues(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_issues_payload(
        _context(
            as_of=as_of,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            cache_bust=cache_bust,
        )
    )
