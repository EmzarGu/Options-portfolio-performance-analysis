from __future__ import annotations

from contextlib import asynccontextmanager
import hmac
import logging
import os
from datetime import date
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional

try:
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.responses import JSONResponse
except ModuleNotFoundError as exc:  # pragma: no cover - exercised only when dependency is absent.
    raise ModuleNotFoundError(
        "FastAPI is required to run mobile_api.py. Install dependencies with `pip install -r requirements.txt`."
    ) from exc

from portfolio_backend import context_runtime as context_service
from portfolio_backend.app_settings import load_monthly_target_band, save_monthly_target_band
from portfolio_backend.audit_store import RefreshAuditRecord, get_default_audit_store
from portfolio_backend.cloud_run_jobs import trigger_ibkr_import_job
from portfolio_backend.mobile_api_service import (
    MobilePayloadContext,
    build_mobile_assigned_holdings_review_payload,
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_positions_payload,
    build_mobile_refresh_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)
from portfolio_backend.mobile_payloads import build_mobile_config


def _local_auth_bypass() -> bool:
    """Explicit development opt-out, never effective on Cloud Run."""
    return (
        os.getenv("ALLOW_INSECURE_LOCAL_AUTH") == "1"
        and not (os.getenv("K_SERVICE") or os.getenv("CLOUD_RUN_JOB"))
    )


def _api_key() -> str:
    return os.getenv("MOBILE_API_KEY", "").strip()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Fail startup rather than publish an unprotected API."""
    if not _api_key() and not _local_auth_bypass():
        raise RuntimeError("MOBILE_API_KEY is required; local development may explicitly opt out.")
    yield


app = FastAPI(title="Options ROI Mobile API", version="0.1.0", lifespan=lifespan)
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)
MONTHLY_RANGES = {"3m", "6m", "ytd", "1y", "since_inception"}
OPEN_OPTION_SORTS = {"moneyness_risk", "expiration", "ticker", "moneyness_pct"}
SERVICE_NAME = "options-roi-mobile-api"
PUBLIC_PATHS = {"/v1/mobile/health"}


def _request_timings(request: Request) -> Dict[str, float]:
    timings = getattr(request.state, "mobile_timings", None)
    if timings is None:
        timings = {}
        request.state.mobile_timings = timings
    return timings


def _record_timing(request: Request, phase: str, elapsed_ms: float) -> None:
    _request_timings(request)[phase] = round(float(elapsed_ms), 2)


def _target_band_for_request(
    *,
    target_return: Optional[float] = None,
    target_floor: Optional[float] = None,
) -> Dict[str, Any]:
    band = load_monthly_target_band()
    if target_return is not None:
        band["target_return"] = max(min(float(target_return), 1.0), 0.0)
    if target_floor is not None:
        band["target_floor"] = max(min(float(target_floor), 1.0), 0.0)
    band["target_floor"] = min(float(band["target_floor"]), float(band["target_return"]))
    return band


def _timing_recorder(request: Request):
    def record(phase: str, elapsed_ms: float) -> None:
        _record_timing(request, phase, elapsed_ms)

    return record


def _build_mobile_read_payload(
    request: Request,
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    builder: Callable[[MobilePayloadContext], Dict[str, Any]],
) -> Dict[str, Any]:
    route_started_at = perf_counter()
    context = context_service.get_context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        timing_recorder=_timing_recorder(request),
    )
    started_at = perf_counter()
    payload = builder(context)
    _record_timing(request, "dto_build_ms", context_service._elapsed_ms(started_at))
    _record_timing(request, "route_total_ms", context_service._elapsed_ms(route_started_at))
    return payload


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
    expected_key = _api_key()
    if request.url.path not in PUBLIC_PATHS and not expected_key and not _local_auth_bypass():
        return JSONResponse(status_code=503, content={"error": {
            "code": "auth_not_configured", "message": "Mobile authentication is not configured.",
            "details": {}, "request_id": None,
        }})
    if request.url.path in PUBLIC_PATHS or (not expected_key and _local_auth_bypass()):
        _record_timing(request, "request_auth_ms", context_service._elapsed_ms(auth_started_at))
        response = await call_next(request)
        _log_request_timing(
            request,
            status_code=response.status_code,
            total_ms=context_service._elapsed_ms(request_started_at),
        )
        return response

    provided_key = request.headers.get("x-api-key", "")
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        provided_key = authorization[7:].strip()

    if hmac.compare_digest(provided_key, expected_key):
        _record_timing(request, "request_auth_ms", context_service._elapsed_ms(auth_started_at))
        response = await call_next(request)
        _log_request_timing(
            request,
            status_code=response.status_code,
            total_ms=context_service._elapsed_ms(request_started_at),
        )
        return response

    _record_timing(request, "request_auth_ms", context_service._elapsed_ms(auth_started_at))
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
        total_ms=context_service._elapsed_ms(request_started_at),
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


def _apply_refresh_metadata(payload: Dict[str, Any], refresh_metadata: Dict[str, Any]) -> Dict[str, Any]:
    refresh = payload.get("refresh")
    if isinstance(refresh, dict):
        refresh.update(
            {
                "scope": refresh_metadata.get("scope", "full"),
                "pipeline_refreshed": bool(refresh_metadata.get("pipeline_refreshed", True)),
                "prices_refreshed": bool(refresh_metadata.get("prices_refreshed", refresh.get("prices_refreshed"))),
                "source_checked": bool(refresh_metadata.get("source_checked", False)),
                "source_changed": bool(refresh_metadata.get("source_changed", True)),
                "reload_endpoints": list(refresh_metadata.get("reload_endpoints") or refresh.get("reload_endpoints") or []),
            }
        )
        if refresh_metadata.get("source_snapshot_id"):
            refresh["source_snapshot_id"] = refresh_metadata["source_snapshot_id"]
        if refresh_metadata.get("pipeline_snapshot_id"):
            refresh["pipeline_snapshot_id"] = refresh_metadata["pipeline_snapshot_id"]
    return payload


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


@app.get("/v1/mobile/health")
def get_mobile_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "version": app.version,
    }


@app.get("/v1/mobile/config")
def get_mobile_config() -> Dict[str, Any]:
    available = context_service._available_sheets()
    prefs = context_service.dashboard_app.load_prefs()
    if context_service._data_source() == context_service.DATA_SOURCE_IBKR:
        prefs = {**prefs, "selected_sheets": ["IBKR Flex"]}
    target_band = load_monthly_target_band()
    return build_mobile_config(
        available,
        prefs,
        default_sheets=available if context_service._data_source() == context_service.DATA_SOURCE_IBKR else context_service.dashboard_app.SHEETS,
        as_of_default=context_service._default_as_of_date(),
        source_kind="ibkr_flex" if context_service._data_source() == context_service.DATA_SOURCE_IBKR else "google_sheet",
        source_name="IBKR Flex" if context_service._data_source() == context_service.DATA_SOURCE_IBKR else "Google Sheets",
        supports_selected_sheets=context_service._data_source() != context_service.DATA_SOURCE_IBKR,
        monthly_target_band=target_band,
    )


@app.get("/v1/mobile/settings/monthly-target-band")
def get_mobile_monthly_target_band() -> Dict[str, Any]:
    return load_monthly_target_band()


@app.post("/v1/mobile/settings/monthly-target-band")
async def update_mobile_monthly_target_band(request: Request) -> Dict[str, Any]:
    body = await request.json()
    try:
        target_return = float(body.get("target_return"))
        target_floor = float(body.get("target_floor"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_monthly_target_band",
                "message": "target_floor and target_return must be rates between 0 and 1.",
                "details": {},
            },
        ) from exc
    if not 0 <= target_return <= 1 or not 0 <= target_floor <= 1:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_monthly_target_band",
                "message": "target_floor and target_return must be rates between 0 and 1.",
                "details": {"target_floor": target_floor, "target_return": target_return},
            },
        )
    return save_monthly_target_band(
        target_floor=target_floor,
        target_return=target_return,
        updated_by="mobile",
        source="mobile",
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
    audit_started_at = context_service._now_iso()
    context, resolved_cache_bust, refresh_metadata = context_service.refresh_context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        timing_recorder=_timing_recorder(request),
    )
    started_at = perf_counter()
    payload = build_mobile_refresh_payload(
        context,
        cache_bust=resolved_cache_bust,
    )
    payload = _apply_refresh_metadata(payload, refresh_metadata)
    _record_timing(request, "dto_build_ms", context_service._elapsed_ms(started_at))
    _record_timing(request, "route_total_ms", context_service._elapsed_ms(route_started_at))
    _record_refresh_audit(
        request=request,
        context=context,
        payload=payload,
        cache_bust=resolved_cache_bust,
        started_at=audit_started_at,
        finished_at=context_service._now_iso(),
    )
    return payload


@app.post("/v1/mobile/import")
def trigger_mobile_ibkr_import() -> Dict[str, Any]:
    try:
        import_start = trigger_ibkr_import_job()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "import_start_failed",
                "message": f"Could not start IBKR import job: {exc}",
                "details": {},
            },
        ) from exc
    return {
        "import": import_start.as_dict(),
        "reload_endpoints": [
            "/v1/mobile/issues",
            "/v1/mobile/dashboard",
            "/v1/mobile/positions",
            "/v1/mobile/assigned-holdings-review",
            "/v1/mobile/open-option-shorts",
            "/v1/mobile/tickers",
            "/v1/mobile/performance/monthly",
            "/v1/mobile/performance/yearly",
        ],
    }


@app.get("/v1/mobile/dashboard")
def get_mobile_dashboard(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    target_return: Optional[float] = None,
    target_floor: Optional[float] = None,
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    target_band = _target_band_for_request(target_return=target_return, target_floor=target_floor)
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=lambda context: build_mobile_dashboard_payload(
            context,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
        ),
    )


@app.get("/v1/mobile/positions")
def get_mobile_positions(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=build_mobile_positions_payload,
    )


@app.get("/v1/mobile/assigned-holdings-review")
def get_mobile_assigned_holdings_review(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    """Return canonical IBKR assignment lots and call caps for research automation."""
    try:
        report = context_service.load_flex_report_from_env()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "assigned_holdings_source_unavailable",
                "message": f"Could not load the canonical IBKR assignment source: {exc}",
                "details": {},
            },
        ) from exc
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=lambda context: build_mobile_assigned_holdings_review_payload(
            context,
            report=report,
        ),
    )


@app.get("/v1/mobile/open-option-shorts")
def get_mobile_open_option_shorts(
    request: Request,
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
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=lambda context: build_mobile_open_option_shorts_payload(context, sort=sort, limit=limit),
    )


@app.get("/v1/mobile/tickers")
def get_mobile_tickers(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    year: Optional[int] = None,
    include_history: bool = False,
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=lambda context: build_mobile_tickers_payload(context, year=year, include_history=include_history),
    )


@app.get("/v1/mobile/performance/monthly")
def get_mobile_monthly_performance(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    target_return: Optional[float] = None,
    target_floor: Optional[float] = None,
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
    target_band = _target_band_for_request(target_return=target_return, target_floor=target_floor)
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=lambda context: build_mobile_monthly_payload(
            context,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
            monthly_range=range,
        ),
    )


@app.get("/v1/mobile/performance/yearly")
def get_mobile_yearly_performance(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=build_mobile_yearly_payload,
    )


@app.get("/v1/mobile/issues")
def get_mobile_issues(
    request: Request,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
    selected_sheets: Optional[List[str]] = Query(default=None),
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return _build_mobile_read_payload(
        request,
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        builder=build_mobile_issues_payload,
    )
