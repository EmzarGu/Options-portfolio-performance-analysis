from __future__ import annotations

import base64
import hashlib
import html
import hmac
import json
import logging
import os
import secrets
import threading
from datetime import date
from time import perf_counter, time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token

from portfolio_backend.cloud_run_jobs import trigger_ibkr_import_job
from portfolio_backend.decision_lab import build_decision_lab_data
from portfolio_backend.decision_lab_templates import DECISION_LAB_HTML
from portfolio_backend.gcp import firestore_client
from portfolio_backend.app_settings import (
    default_monthly_target_band,
    load_monthly_target_band,
    save_monthly_target_band,
)
from portfolio_backend.mobile_api_service import build_mobile_refresh_payload
from portfolio_backend.option_market.cutemarkets import CuteMarketsClient
from portfolio_backend.option_market.decision_data import decision_option_loader
from portfolio_backend.option_market.store import FirestoreOptionMarketStore
from portfolio_backend.web_dashboard_payloads import (
    build_assignment_quality_data as build_web_assignment_quality_data,
    build_dashboard_data as build_web_dashboard_data,
    dashboard_shell_data as build_web_dashboard_shell_data,
    get_web_context,
)
from portfolio_backend.web_dashboard_templates import (
    BASE_CSS,
    DASHBOARD_HTML,
    GOOGLE_REDIRECT_CALLBACK_HTML,
    LOGIN_TEMPLATE,
)


app = FastAPI(title="Options ROI Web Dashboard", version="0.1.0")
logger = logging.getLogger("uvicorn.error")

COOKIE_NAME = "options_roi_web_session"
OAUTH_STATE_COOKIE_NAME = "options_roi_google_state"
DEFAULT_SESSION_DAYS = 90
DEFAULT_DASHBOARD_DATA_CACHE_SECONDS = 0
DEFAULT_ASSIGNMENT_QUALITY_CACHE_SECONDS = 300
NO_STORE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}

_dashboard_data_cache_lock = threading.Lock()
_dashboard_data_cache: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_dashboard_data_key_locks: Dict[tuple, threading.Lock] = {}
_assignment_quality_cache_lock = threading.Lock()
_assignment_quality_cache: Dict[tuple, tuple[float, Dict[str, Any]]] = {}
_assignment_quality_key_locks: Dict[tuple, threading.Lock] = {}
_probability_history_cache_lock = threading.Lock()
_probability_history_cache: tuple[float, list[dict[str, Any]]] | None = None
_option_history_cache_lock = threading.Lock()
_option_history_cache: tuple[float, list[dict[str, Any]]] | None = None


@app.middleware("http")
async def no_store_browser_cache(request: Request, call_next):
    response = await call_next(request)
    for header, value in NO_STORE_HEADERS.items():
        response.headers[header] = value
    return response


def _truthy_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _truthy_query(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _auth_enabled() -> bool:
    return _truthy_env("WEB_DASHBOARD_AUTH", True)


def _dashboard_password() -> Optional[str]:
    value = os.getenv("WEB_DASHBOARD_PASSWORD")
    return value.strip() if value else None


def _google_client_id() -> Optional[str]:
    value = os.getenv("WEB_GOOGLE_CLIENT_ID", "").strip()
    return value or None


def _allowed_google_emails() -> set[str]:
    raw = os.getenv("WEB_AUTH_ALLOWED_EMAILS", "")
    return {email.strip().lower() for email in raw.split(",") if email.strip()}


def _cookie_secret_configured() -> bool:
    return bool(os.getenv("WEB_DASHBOARD_COOKIE_SECRET") or _dashboard_password())


def _cookie_secret() -> str:
    return os.getenv("WEB_DASHBOARD_COOKIE_SECRET") or _dashboard_password() or "local-dev-dashboard-secret"


def _google_auth_configured() -> bool:
    return bool(_google_client_id() and _allowed_google_emails() and _cookie_secret_configured())


def _auth_configured() -> bool:
    return bool(_dashboard_password() or _google_auth_configured())


def _password_fallback_visible() -> bool:
    return _truthy_env("WEB_PASSWORD_FALLBACK_VISIBLE", False)


def _session_max_age_seconds() -> int:
    raw = os.getenv("WEB_SESSION_DAYS")
    if raw:
        try:
            days = max(int(raw), 1)
        except ValueError:
            days = DEFAULT_SESSION_DAYS
    else:
        days = DEFAULT_SESSION_DAYS
    return days * 24 * 60 * 60


def _web_monthly_target_return_default() -> float:
    return float(default_monthly_target_band()["target_return"])


def _web_monthly_target_floor_default() -> float:
    return float(default_monthly_target_band()["target_floor"])


def _coerce_target_return(value: Optional[str], *, percent: bool = False) -> Optional[float]:
    if value is None:
        return None
    try:
        target = float(value) / 100.0 if percent else float(value)
    except ValueError:
        return None
    return min(max(target, 0.0), 1.0)


def _monthly_target_band_from_request(request: Request) -> Dict[str, Any]:
    band = load_monthly_target_band()
    pct = _coerce_target_return(request.query_params.get("target_return_pct"), percent=True)
    if pct is not None:
        band["target_return"] = pct
    floor_pct = _coerce_target_return(request.query_params.get("target_floor_pct"), percent=True)
    if floor_pct is not None:
        band["target_floor"] = floor_pct
    band["target_floor"] = min(float(band["target_floor"]), float(band["target_return"]))
    return band


def _b64_json(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64_json(value: str) -> Dict[str, Any]:
    padded = value + "=" * (-len(value) % 4)
    decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    data = json.loads(decoded.decode("utf-8"))
    return data if isinstance(data, dict) else {}


def _sign_value(value: str) -> str:
    payload = value.encode("utf-8")
    digest = hmac.new(_cookie_secret().encode("utf-8"), payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _session_token(*, email: Optional[str] = None, auth_method: str = "key") -> str:
    payload = _b64_json(
        {
            "iat": int(time()),
            "email": email,
            "auth": auth_method,
        }
    )
    return f"{payload}.{_sign_value(payload)}"


def _session_info(token: str) -> Optional[Dict[str, Any]]:
    if not token or "." not in token:
        return None
    payload, signature = token.split(".", 1)
    if payload.isdigit():
        # Backward compatibility for the previous timestamp-only cookie shape.
        if not hmac.compare_digest(signature, _sign_value(payload)):
            return None
        issued_at = int(payload)
        return {"iat": issued_at, "email": None, "auth": "legacy_key"}
    if not hmac.compare_digest(signature, _sign_value(payload)):
        return None
    try:
        info = _unb64_json(payload)
    except Exception:
        return None
    issued_at = info.get("iat")
    if not isinstance(issued_at, int):
        return None
    if issued_at < int(time()) - _session_max_age_seconds():
        return None
    return info


def _valid_session(token: str) -> bool:
    return _session_info(token) is not None


def _is_authenticated(request: Request) -> bool:
    if not _auth_enabled():
        return True
    return _valid_session(request.cookies.get(COOKIE_NAME, ""))


def _authenticated_user(request: Request) -> Optional[str]:
    info = _session_info(request.cookies.get(COOKIE_NAME, ""))
    if not info:
        return None
    email = info.get("email")
    return str(email) if email else None


def _redirect_to_login() -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


def _set_session_cookie(response: Response, *, email: Optional[str], auth_method: str) -> None:
    response.set_cookie(
        COOKIE_NAME,
        _session_token(email=email, auth_method=auth_method),
        max_age=_session_max_age_seconds(),
        httponly=True,
        secure=True,
        samesite="lax",
    )


def _verify_google_credential(credential: str, *, nonce: Optional[str] = None) -> Dict[str, Any]:
    client_id = _google_client_id()
    if not client_id:
        raise ValueError("Google sign-in is not configured.")
    claims = google_id_token.verify_oauth2_token(
        credential,
        google_auth_requests.Request(),
        client_id,
    )
    if not claims.get("email_verified"):
        raise PermissionError("Google account email is not verified.")
    email = str(claims.get("email") or "").strip().lower()
    if not email:
        raise PermissionError("Google account did not include an email address.")
    if nonce is not None and claims.get("nonce") != nonce:
        raise PermissionError("Google sign-in session could not be verified.")
    allowed = _allowed_google_emails()
    if not allowed:
        raise PermissionError("No Google account allowlist is configured.")
    if email not in allowed:
        raise PermissionError("This Google account is not allowed for this dashboard.")
    return {**claims, "email": email}


def _oauth_state_token(*, state: str, nonce: str) -> str:
    payload = _b64_json({"state": state, "nonce": nonce, "iat": int(time())})
    return f"{payload}.{_sign_value(payload)}"


def _oauth_state_info(token: str) -> Optional[Dict[str, Any]]:
    info = _session_info(token)
    if not info:
        return None
    if int(info.get("iat", 0)) < int(time()) - 10 * 60:
        return None
    state = info.get("state")
    nonce = info.get("nonce")
    if not isinstance(state, str) or not isinstance(nonce, str):
        return None
    return {"state": state, "nonce": nonce}


def _google_redirect_uri(request: Request) -> str:
    public_base_url = os.getenv("WEB_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base_url:
        return f"{public_base_url}/auth/google"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"https://{host}/auth/google"



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


def _assignment_quality_cache_seconds() -> int:
    raw = os.getenv("WEB_ASSIGNMENT_QUALITY_CACHE_SECONDS", str(DEFAULT_ASSIGNMENT_QUALITY_CACHE_SECONDS))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_ASSIGNMENT_QUALITY_CACHE_SECONDS


def _get_cached_assignment_quality_data(*, as_of: Optional[date] = None) -> Dict[str, Any]:
    ttl_seconds = _assignment_quality_cache_seconds()
    key = (as_of.isoformat() if as_of else "",)
    now = time()
    with _assignment_quality_cache_lock:
        cached = _assignment_quality_cache.get(key)
        if ttl_seconds > 0 and cached and now - cached[0] <= ttl_seconds:
            return cached[1]
        key_lock = _assignment_quality_key_locks.setdefault(key, threading.Lock())

    with key_lock:
        now = time()
        with _assignment_quality_cache_lock:
            cached = _assignment_quality_cache.get(key)
            if ttl_seconds > 0 and cached and now - cached[0] <= ttl_seconds:
                return cached[1]

        payload = build_web_assignment_quality_data(as_of=as_of)
        if ttl_seconds > 0:
            with _assignment_quality_cache_lock:
                _assignment_quality_cache[key] = (time(), payload)
                while len(_assignment_quality_cache) > 4:
                    oldest_key = min(_assignment_quality_cache, key=lambda cache_key: _assignment_quality_cache[cache_key][0])
                    _assignment_quality_cache.pop(oldest_key, None)
                    _assignment_quality_key_locks.pop(oldest_key, None)
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


def _with_decision_lab(payload: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(payload)
    enriched["decision_lab"] = {"deferred": True}
    return enriched


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "options-roi-web-dashboard",
        "version": app.version,
    }


@app.get("/login", response_class=HTMLResponse)
def login_page() -> HTMLResponse:
    if _auth_enabled() and not _auth_configured():
        return HTMLResponse(_configuration_error_html(), status_code=500)
    return HTMLResponse(_login_html())


@app.post("/login")
async def login(request: Request) -> Response:
    if not _auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    expected = _dashboard_password()
    if not expected:
        return HTMLResponse(_configuration_error_html(), status_code=500)
    body = (await request.body()).decode("utf-8")
    submitted = parse_qs(body).get("password", [""])[0]
    if not hmac.compare_digest(submitted, expected):
        return HTMLResponse(_login_html("Invalid dashboard password."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    _set_session_cookie(response, email=None, auth_method="key")
    return response


@app.post("/auth/google")
async def google_login(request: Request) -> Response:
    if not _auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    body = (await request.body()).decode("utf-8")
    fields = parse_qs(body)
    credential = fields.get("credential", [""])[0]
    state = fields.get("state", [""])[0]
    body_csrf = fields.get("g_csrf_token", [""])[0]
    cookie_csrf = request.cookies.get("g_csrf_token", "")
    if body_csrf or cookie_csrf:
        if not body_csrf or not cookie_csrf or not hmac.compare_digest(body_csrf, cookie_csrf):
            return HTMLResponse(_login_html("Google sign-in failed CSRF validation."), status_code=400)
    if not credential:
        return HTMLResponse(_login_html("Google sign-in did not return a credential."), status_code=400)
    nonce = None
    if state:
        oauth_info = _oauth_state_info(request.cookies.get(OAUTH_STATE_COOKIE_NAME, ""))
        if not oauth_info or not hmac.compare_digest(state, oauth_info["state"]):
            return HTMLResponse(_login_html("Google sign-in failed session validation."), status_code=400)
        nonce = oauth_info["nonce"]
    try:
        claims = (
            _verify_google_credential(credential, nonce=nonce)
            if nonce is not None
            else _verify_google_credential(credential)
        )
    except PermissionError as exc:
        return HTMLResponse(_login_html(str(exc)), status_code=403)
    except Exception:
        return HTMLResponse(_login_html("Google sign-in could not be verified."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    _set_session_cookie(response, email=str(claims["email"]), auth_method="google")
    response.delete_cookie(OAUTH_STATE_COOKIE_NAME)
    return response


@app.get("/auth/google/start")
def google_redirect_start(request: Request) -> Response:
    client_id = _google_client_id()
    if not _auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if not client_id:
        return HTMLResponse(_login_html("Google sign-in is not configured."), status_code=500)
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    params = {
        "client_id": client_id,
        "redirect_uri": _google_redirect_uri(request),
        "response_type": "id_token",
        "scope": "openid email profile",
        "state": state,
        "nonce": nonce,
        "prompt": "select_account",
    }
    response = RedirectResponse(
        url=f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}",
        status_code=303,
    )
    response.set_cookie(
        OAUTH_STATE_COOKIE_NAME,
        _oauth_state_token(state=state, nonce=nonce),
        max_age=10 * 60,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@app.get("/auth/google", response_class=HTMLResponse)
def google_redirect_callback() -> HTMLResponse:
    return HTMLResponse(GOOGLE_REDIRECT_CALLBACK_HTML)


@app.post("/logout")
def logout() -> Response:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    response.delete_cookie(OAUTH_STATE_COOKIE_NAME)
    return response


@app.post("/refresh")
def refresh(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    started_at = perf_counter()
    timings: Dict[str, float] = {}

    def record_timing(phase: str, elapsed_ms: float) -> None:
        timings[phase] = round(float(elapsed_ms), 2)

    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    section = request.query_params.get("section") or "dashboard"
    if section not in {
        "dashboard",
        "decision_lab",
        "performance",
        "monthly",
        "tickers",
        "settings",
        "diagnostics",
        "methodology",
    }:
        section = "dashboard"
    _clear_dashboard_data_cache()
    # The web payload always builds the full unrealized-capable state and derives
    # both dashboard views from it. Persist the same state shape on refresh so the
    # follow-up /api/dashboard read can use the shared Firestore snapshot even
    # when the user pressed refresh while viewing the realized-only toggle.
    context, cache_bust = _get_context(
        as_of=None,
        include_unrealized=True,
        force_rebuild=True,
        timing_recorder=record_timing,
    )
    build_mobile_refresh_payload(context, cache_bust=cache_bust)
    timings["route_total_ms"] = round((perf_counter() - started_at) * 1000, 2)
    logger.info(
        "web_refresh_timing %s",
        " ".join([f"{key}={timings[key]}" for key in sorted(timings)]),
    )
    _clear_dashboard_data_cache()
    return RedirectResponse(
        url=(
            f"/?include_unrealized={1 if include_unrealized else 0}"
            f"&section={section}&refreshed={cache_bust}"
        ),
        status_code=303,
    )


@app.post("/import")
def trigger_import(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    section = request.query_params.get("section") or "diagnostics"
    try:
        import_start = trigger_ibkr_import_job()
        status = import_start.status
    except Exception as exc:
        logger.warning("web_import_start_failed error=%s", exc)
        status = "failed"
    _clear_dashboard_data_cache()
    return RedirectResponse(
        url=f"/?section={section}&import_start={status}&v={int(time())}",
        status_code=303,
    )


@app.get("/api/dashboard")
def dashboard_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    started_at = perf_counter()
    timings: Dict[str, float] = {}

    def record_timing(phase: str, elapsed_ms: float) -> None:
        timings[phase] = round(float(elapsed_ms), 2)

    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    settings_started_at = perf_counter()
    target_band = _monthly_target_band_from_request(request)
    record_timing("target_settings_ms", (perf_counter() - settings_started_at) * 1000)
    try:
        payload = _get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
            timing_recorder=record_timing,
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    decision_started_at = perf_counter()
    response_payload = _with_decision_lab(payload)
    record_timing("decision_lab_attach_ms", (perf_counter() - decision_started_at) * 1000)
    record_timing("route_total_ms", (perf_counter() - started_at) * 1000)
    logger.info(
        "web_dashboard_api_timing %s",
        " ".join([f"{key}={timings[key]}" for key in sorted(timings)]),
    )
    return JSONResponse(response_payload)


@app.get("/api/decision-lab")
def decision_lab_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    try:
        payload = _get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
        )
        return JSONResponse(_build_decision_lab_payload(payload, force_refresh=False))
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/assignment-quality")
def assignment_quality_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    started_at = perf_counter()
    try:
        payload = _get_cached_assignment_quality_data()
    except Exception as exc:
        logger.warning("assignment_quality_payload_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)
    logger.info("web_assignment_quality_timing total_ms=%.2f", (perf_counter() - started_at) * 1000)
    return JSONResponse(payload)


@app.post("/api/decision-lab/options/refresh")
def decision_lab_options_refresh(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    try:
        payload = _get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
        )
        _clear_dashboard_data_cache()
        return JSONResponse(_build_decision_lab_payload(payload, force_refresh=True))
    except Exception as exc:
        logger.warning("decision_lab_option_refresh_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/decision-lab", response_class=HTMLResponse)
def decision_lab_page(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    return HTMLResponse(DECISION_LAB_HTML)


@app.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    payload = _dashboard_shell_data(
        include_unrealized=include_unrealized,
        target_return=target_band["target_return"],
        target_floor=target_band["target_floor"],
    )
    data_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).replace("</", "<\\/")
    user = html.escape(_authenticated_user(request) or "")
    response = HTMLResponse(
        DASHBOARD_HTML.replace("__DASHBOARD_DATA__", data_json).replace("__AUTH_USER__", user)
    )
    return response


@app.get("/api/settings/monthly-target-band")
def monthly_target_band_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse(load_monthly_target_band())


@app.post("/api/settings/monthly-target-band")
async def update_monthly_target_band_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
        target_return = _coerce_target_return(body.get("target_return"))
        target_floor = _coerce_target_return(body.get("target_floor"))
        if target_return is None or target_floor is None:
            return JSONResponse({"error": "target_floor and target_return must be rates between 0 and 1."}, status_code=400)
        band = save_monthly_target_band(
            target_floor=target_floor,
            target_return=target_return,
            updated_by=_authenticated_user(request),
            source="web",
        )
        _clear_dashboard_data_cache()
        return JSONResponse(band)
    except Exception as exc:
        logger.warning("monthly_target_band_save_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


def _configuration_error_html() -> str:
    return """<!doctype html>
<html><head><title>Options ROI</title><style>{css}</style></head>
<body><main class="login"><h1>Dashboard is not configured</h1>
<p>Set WEB_GOOGLE_CLIENT_ID with WEB_AUTH_ALLOWED_EMAILS and a cookie secret, or set WEB_DASHBOARD_PASSWORD.</p></main></body></html>""".format(
        css=BASE_CSS
    )


def _login_html(error: str = "") -> str:
    safe_error = error.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    google_signin = _google_signin_html()
    show_fallback = bool(_dashboard_password() and (not google_signin or _password_fallback_visible()))
    fallback_open = "false" if google_signin else "true"
    fallback_label = "Use dashboard password instead" if google_signin else "Use dashboard password"
    fallback_html = ""
    if show_fallback:
        fallback_html = (
            '<details class="fallback-login"__FALLBACK_OPEN__><summary>__FALLBACK_LABEL__</summary>'
            '<form method="post" action="/login"><input name="password" type="password" '
            'autocomplete="current-password" autofocus placeholder="Dashboard password">'
            '<button type="submit">Open dashboard</button></form></details>'
        )
    return (
        LOGIN_TEMPLATE.replace("__BASE_CSS__", BASE_CSS)
        .replace("__ERROR__", safe_error)
        .replace("__GOOGLE_SIGNIN__", google_signin)
        .replace("__FALLBACK_LOGIN__", fallback_html)
        .replace("__FALLBACK_OPEN__", " open" if fallback_open == "true" else "")
        .replace("__FALLBACK_LABEL__", fallback_label)
    )


def _google_signin_html() -> str:
    client_id = _google_client_id()
    if not client_id:
        return ""
    return """
<div class="signin-block">
  <a class="google-login-button" href="/auth/google/start">
    <span class="google-login-icon" aria-hidden="true">G</span>
    <span>Sign in with Google</span>
  </a>
</div>"""
