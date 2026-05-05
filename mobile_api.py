from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import date, datetime
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

MONTHLY_RANGES = {"3m", "6m", "ytd", "1y", "since_inception"}
OPEN_OPTION_SORTS = {"moneyness_risk", "expiration", "ticker", "moneyness_pct"}
SERVICE_NAME = "options-roi-mobile-api"
CONTEXT_CACHE_MAX_ITEMS = 16

_context_cache_lock = threading.Lock()
_context_cache: "OrderedDict[Tuple[str, str, bool, Tuple[str, ...], int], Any]" = OrderedDict()
_active_cache_bust = 1


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
) -> tuple[MobilePayloadRequest, List[str]]:
    available = _available_sheets()
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
):
    request, available = _common_request(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=_resolve_cache_bust(cache_bust),
    )
    key = _context_cache_key(request)
    if not force_rebuild:
        with _context_cache_lock:
            cached = _context_cache.get(key)
            if cached is not None:
                _context_cache.move_to_end(key)
                return cached

    context = build_mobile_payload_context(
        request,
        _dependencies(),
        available_sheets=available,
    )
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
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    resolved_cache_bust = cache_bust if cache_bust is not None else _refresh_cache_bust()
    context = _context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=resolved_cache_bust,
        force_rebuild=True,
    )
    _set_active_cache_bust(resolved_cache_bust)
    return build_mobile_refresh_payload(
        context,
        cache_bust=resolved_cache_bust,
    )


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
