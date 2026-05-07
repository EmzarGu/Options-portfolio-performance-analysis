from __future__ import annotations

import threading
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


app = FastAPI(title="Options ROI Mobile API", version="0.1.0")
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

MONTHLY_RANGES = {"3m", "6m", "ytd", "1y", "since_inception"}
OPEN_OPTION_SORTS = {"moneyness_risk", "expiration", "ticker", "moneyness_pct"}
SERVICE_NAME = "options-roi-mobile-api"
CONTEXT_CACHE_MAX_ITEMS = 16
PUBLIC_PATHS = {"/v1/mobile/health"}

_context_cache_lock = threading.Lock()
_context_cache: "OrderedDict[Tuple[str, str, bool, Tuple[str, ...], int], Any]" = OrderedDict()
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


def _dependencies() -> MobileServiceDependencies:
    return MobileServiceDependencies(
        load_options=dashboard_app.load_options,
        fetch_price_history=dashboard_app.fetch_price_history_yf,
        collect_dividend_cashflows=dashboard_app.collect_dividend_cashflows,
        align_benchmarks_monthly=dashboard_app.align_benchmarks_monthly,
        fetch_current_prices=dashboard_app.fetch_current_prices_yf,
    )


def _available_sheets() -> List[str]:
    return dashboard_app.list_option_sheets(dashboard_app.SHEET_ID)


def _default_selected_sheets(available_sheets: List[str]) -> List[str]:
    prefs = dashboard_app.load_prefs()
    saved_sheets = [sheet for sheet in prefs.get("selected_sheets", []) if sheet in available_sheets]
    return saved_sheets or [sheet for sheet in available_sheets if sheet in dashboard_app.SHEETS] or available_sheets


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
    selected = selected_sheets or _default_selected_sheets(available)
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
            sheet_id=dashboard_app.SHEET_ID,
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


def _context_cache_key(request: MobilePayloadRequest) -> Tuple[str, str, bool, Tuple[str, ...], int]:
    return (
        request.sheet_id,
        request.as_of.isoformat(),
        bool(request.include_unrealized),
        tuple(str(sheet) for sheet in request.selected_sheets),
        int(request.cache_bust),
    )


def _remember_context(key: Tuple[str, str, bool, Tuple[str, ...], int], context: Any) -> None:
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
    context_kwargs = {"available_sheets": available}
    if timing_recorder is not None and _supports_timing_recorder(build_mobile_payload_context):
        context_kwargs["timing_recorder"] = timing_recorder
    context = build_mobile_payload_context(
        request,
        _dependencies(),
        **context_kwargs,
    )
    if timing_recorder is not None:
        timing_recorder("context_build_total_ms", _elapsed_ms(started_at))
    _remember_context(key, context)
    return context


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
    return build_mobile_config(
        available,
        prefs,
        default_sheets=dashboard_app.SHEETS,
        as_of_default=date.today(),
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
