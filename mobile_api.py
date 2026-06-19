from __future__ import annotations

import threading
import hashlib
import hmac
import logging
import os
from collections import OrderedDict
from datetime import date, datetime, timedelta
from inspect import Parameter, signature
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.responses import JSONResponse
except ModuleNotFoundError as exc:  # pragma: no cover - exercised only when dependency is absent.
    raise ModuleNotFoundError(
        "FastAPI is required to run mobile_api.py. Install dependencies with `pip install -r requirements.txt`."
    ) from exc

import streamlit_app as dashboard_app
import pandas as pd
from portfolio_backend.app_settings import load_monthly_target_band, save_monthly_target_band
from portfolio_backend.audit_store import RefreshAuditRecord, get_default_audit_store
from portfolio_backend.cloud_run_jobs import trigger_ibkr_import_job
from portfolio_backend.mobile_api_service import (
    MobilePayloadContext,
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


app = FastAPI(title="Options ROI Mobile API", version="0.1.0")
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

MONTHLY_RANGES = {"3m", "6m", "ytd", "1y", "since_inception"}
OPEN_OPTION_SORTS = {"moneyness_risk", "expiration", "ticker", "moneyness_pct"}
SERVICE_NAME = "options-roi-mobile-api"
DATA_SOURCE_GOOGLE_SHEETS = "google_sheets"
DATA_SOURCE_IBKR = "ibkr"
CONTEXT_CACHE_MAX_ITEMS = 16
DEFAULT_SOURCE_MARKER_CACHE_SECONDS = 30
DEFAULT_IBKR_IMPORT_STALE_DAYS = 3
PUBLIC_PATHS = {"/v1/mobile/health"}
PRICE_ONLY_RELOAD_ENDPOINTS = [
    "/v1/mobile/dashboard",
    "/v1/mobile/positions",
    "/v1/mobile/open-option-shorts",
    "/v1/mobile/tickers",
    "/v1/mobile/performance/monthly",
    "/v1/mobile/performance/yearly",
    "/v1/mobile/issues",
]
FULL_RELOAD_ENDPOINTS = [
    "/v1/mobile/dashboard",
    "/v1/mobile/positions",
    "/v1/mobile/open-option-shorts",
    "/v1/mobile/tickers",
    "/v1/mobile/performance/monthly",
    "/v1/mobile/performance/yearly",
    "/v1/mobile/issues",
]

_context_cache_lock = threading.Lock()
_context_cache: "OrderedDict[Tuple[str, str, str, bool, Tuple[str, ...], int], Any]" = OrderedDict()
_active_cache_bust = 1
_source_marker_cache_lock = threading.Lock()
_source_marker_cache: Dict[str, tuple[float, Optional[Dict[str, Any]]]] = {}


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
    context = _context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        timing_recorder=_timing_recorder(request),
    )
    started_at = perf_counter()
    payload = builder(context)
    _record_timing(request, "dto_build_ms", _elapsed_ms(started_at))
    _record_timing(request, "route_total_ms", _elapsed_ms(route_started_at))
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


def _source_marker_cache_seconds() -> int:
    value = os.getenv("SOURCE_MARKER_CACHE_SECONDS", str(DEFAULT_SOURCE_MARKER_CACHE_SECONDS)).strip()
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return DEFAULT_SOURCE_MARKER_CACHE_SECONDS


def _ibkr_import_stale_days() -> int:
    value = os.getenv("IBKR_IMPORT_STALE_DAYS", str(DEFAULT_IBKR_IMPORT_STALE_DAYS)).strip()
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return DEFAULT_IBKR_IMPORT_STALE_DAYS


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
                latest = _ibkr_import_marker_from_doc(doc, fallback_id=metadata_snap.id, query_id=query_id)
                if latest is not None:
                    marker = _with_ibkr_import_health(client, query_id, latest)
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
            candidate = _ibkr_import_marker_from_doc(doc, fallback_id=snap.id, query_id=query_id)
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
        marker = _with_ibkr_import_health(client, query_id, latest)
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


def _ibkr_import_marker_from_doc(
    doc: Dict[str, Any],
    *,
    fallback_id: str,
    query_id: str,
) -> Optional[Dict[str, Any]]:
    if str(doc.get("query_id") or query_id) != str(query_id):
        return None
    marker = {
        "import_run_id": str(doc.get("run_id") or doc.get("import_run_id") or fallback_id),
        "query_id": str(doc.get("query_id") or query_id),
        "status": str(doc.get("status") or "succeeded"),
        "finished_at": doc.get("finished_at"),
        "from_date": doc.get("from_date"),
        "to_date": doc.get("to_date"),
        "inserted_raw_rows": doc.get("inserted_raw_rows"),
        "updated_raw_rows": doc.get("updated_raw_rows"),
        "inserted_transactions": doc.get("inserted_transactions"),
        "updated_transactions": doc.get("updated_transactions"),
    }
    marker["source_snapshot_id"] = (
        f"ibkr-flex:{marker['query_id']}:{marker.get('finished_at') or marker['import_run_id']}"
    )
    return marker


def _with_ibkr_import_health(client, query_id: str, marker: Dict[str, Any]) -> Dict[str, Any]:
    marker = dict(marker)
    marker["import_health"] = _ibkr_import_health(client, query_id, marker)
    return marker


def _ibkr_import_health(client, query_id: str, latest_success_marker: Dict[str, Any]) -> Dict[str, Any]:
    """Detect unresolved IBKR import failures/deferred trailing statements.

    Pipeline data is loaded from successful raw-row imports. A trailing-day
    statement can fail/defer after the last successful import; without checking
    refresh_runs explicitly, the apps can incorrectly show data quality as OK
    while yesterday's assignment/expiration rows are still missing.
    """
    latest_success_finished = str(latest_success_marker.get("finished_at") or "")
    latest_success_to_date = _parse_iso_date(latest_success_marker.get("to_date"))
    issues: List[Dict[str, Any]] = []
    stale_issue = _ibkr_stale_import_issue(latest_success_marker, today=date.today())
    if stale_issue is not None:
        issues.append(stale_issue)
    unresolved_by_range: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    try:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            docs = client.collection("refresh_runs").where(filter=FieldFilter("source", "==", "ibkr_flex")).stream()
        except Exception:
            docs = client.collection("refresh_runs").where("source", "==", "ibkr_flex").stream()
        for snap in docs:
            doc = snap.to_dict() or {}
            status = str(doc.get("status") or "")
            if status not in {"failed", "deferred"}:
                continue
            doc_query_id = str(doc.get("query_id") or query_id)
            if doc_query_id != str(query_id):
                continue
            finished_at = str(doc.get("finished_at") or "")
            issue_to_date = _parse_iso_date(doc.get("to_date"))
            if _ibkr_import_issue_resolved(
                latest_success_finished=latest_success_finished,
                latest_success_to_date=latest_success_to_date,
                issue_finished_at=finished_at,
                issue_to_date=issue_to_date,
            ):
                continue
            from_date = doc.get("from_date")
            to_date = doc.get("to_date")
            reason = str(doc.get("defer_reason") or doc.get("error_message") or status)
            label = str(to_date or from_date or "latest requested date")
            if from_date and to_date and from_date != to_date:
                label = f"{from_date} to {to_date}"
            if reason == "trailing_statement_unavailable":
                message = f"IBKR import deferred for {label}: statement was not available yet."
            else:
                message = f"IBKR import {status} for {label}: {reason}"
            issue = {
                "category": "import",
                "severity": "warning",
                "status": status,
                "from_date": from_date,
                "to_date": to_date,
                "finished_at": finished_at,
                "message": message,
                "action": "retry_import",
            }
            key = (str(from_date or ""), str(to_date or ""), _statement_unavailable_issue_key(reason) or reason)
            previous = unresolved_by_range.get(key)
            if previous is None or str(issue.get("finished_at") or "") > str(previous.get("finished_at") or ""):
                unresolved_by_range[key] = issue
    except Exception as exc:
        logger.warning("ibkr_import_health_check_failed error=%s", exc)
    issues.extend(unresolved_by_range.values())
    issues.sort(key=lambda item: str(item.get("finished_at") or ""), reverse=True)
    return {
        "status": "warning" if issues else "ok",
        "issues": issues[:5],
        "unresolved_count": len(issues),
        "latest_success_finished_at": latest_success_marker.get("finished_at"),
        "latest_success_to_date": latest_success_marker.get("to_date"),
    }


def _statement_unavailable_issue_key(reason: str) -> Optional[str]:
    lowered = str(reason or "").lower()
    if reason == "trailing_statement_unavailable":
        return "statement_unavailable"
    if "statement is incomplete" in lowered or "statement is not available" in lowered:
        return "statement_unavailable"
    return None


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    compact = text[:8]
    if len(compact) == 8 and compact.isdigit():
        try:
            return datetime.strptime(compact, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _ibkr_stale_import_issue(latest_success_marker: Dict[str, Any], *, today: date) -> Optional[Dict[str, Any]]:
    stale_days = _ibkr_import_stale_days()
    if stale_days <= 0:
        return None
    to_date = _parse_iso_date(latest_success_marker.get("to_date"))
    if to_date is None:
        return {
            "category": "import",
            "severity": "warning",
            "status": "stale",
            "from_date": latest_success_marker.get("from_date"),
            "to_date": latest_success_marker.get("to_date"),
            "finished_at": latest_success_marker.get("finished_at"),
            "message": "IBKR import freshness cannot be verified: latest successful import has no valid statement end date.",
            "action": "retry_import",
        }
    latest_expected = today - timedelta(days=stale_days)
    if to_date >= latest_expected:
        return None
    return {
        "category": "import",
        "severity": "warning",
        "status": "stale",
        "from_date": latest_success_marker.get("from_date"),
        "to_date": latest_success_marker.get("to_date"),
        "finished_at": latest_success_marker.get("finished_at"),
        "message": (
            f"IBKR import stale: latest successful statement ends {to_date.isoformat()}, "
            f"more than {stale_days} days behind today."
        ),
        "action": "retry_import",
    }


def _ibkr_import_issue_resolved(
    *,
    latest_success_finished: str,
    latest_success_to_date: Optional[date],
    issue_finished_at: str,
    issue_to_date: Optional[date],
) -> bool:
    if latest_success_finished and issue_finished_at and latest_success_finished > issue_finished_at:
        if issue_to_date is None or latest_success_to_date is None:
            return True
        return latest_success_to_date >= issue_to_date
    return False


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


def _context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    force_rebuild: bool = False,
    timing_recorder=None,
    source_metadata_override: Optional[Dict[str, Any]] = None,
):
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

    started_at = perf_counter()
    if _data_source() == DATA_SOURCE_IBKR:
        if "source_snapshot_id" not in source_metadata:
            source_marker = _refresh_source_marker(timing_recorder=timing_recorder)
            source_metadata.update(_source_metadata_for_marker(source_marker))
        if not force_rebuild:
            snapshot_context = _load_pipeline_snapshot_context(
                request=request,
                available=available,
                source_marker=source_marker,
                timing_recorder=timing_recorder,
            )
            if snapshot_context is not None:
                refreshed_context = _refresh_prices_from_cached_base(
                    snapshot_context,
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
        context_kwargs = {"available_sheets": available}
        if _supports_keyword(build_ibkr_mobile_payload_context, "source_metadata"):
            context_kwargs["source_metadata"] = source_metadata
        if timing_recorder is not None and _supports_timing_recorder(build_ibkr_mobile_payload_context):
            context_kwargs["timing_recorder"] = timing_recorder
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
    if use_memory_cache:
        _remember_context(key, context)
    if _data_source() == DATA_SOURCE_IBKR:
        _save_pipeline_snapshot(
            context,
            request=request,
            available=available,
            source_marker=source_marker or _source_marker_from_metadata(dict(getattr(context, "source_metadata", {}) or {})),
            timing_recorder=timing_recorder,
        )
    return context


def _smart_refresh_context(
    *,
    as_of: Optional[date],
    include_unrealized: bool,
    selected_sheets: Optional[List[str]],
    cache_bust: Optional[int],
    timing_recorder=None,
) -> Tuple[Any, int, Dict[str, Any]]:
    resolved_cache_bust = cache_bust if cache_bust is not None else _refresh_cache_bust()
    if _data_source() != DATA_SOURCE_IBKR:
        context = _context(
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
    context = _context(
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
    target_band = load_monthly_target_band()
    return build_mobile_config(
        available,
        prefs,
        default_sheets=available if _data_source() == DATA_SOURCE_IBKR else dashboard_app.SHEETS,
        as_of_default=date.today(),
        source_kind="ibkr_flex" if _data_source() == DATA_SOURCE_IBKR else "google_sheet",
        source_name="IBKR Flex" if _data_source() == DATA_SOURCE_IBKR else "Google Sheets",
        supports_selected_sheets=_data_source() != DATA_SOURCE_IBKR,
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
    audit_started_at = _now_iso()
    context, resolved_cache_bust, refresh_metadata = _smart_refresh_context(
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
