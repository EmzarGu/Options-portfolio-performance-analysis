from __future__ import annotations

import html
import hmac
import json
import logging
import secrets
from time import perf_counter, time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from portfolio_backend import web_auth, web_data_service as web_data
from portfolio_backend.app_settings import load_monthly_target_band, save_monthly_target_band
from portfolio_backend.cloud_run_jobs import trigger_ibkr_import_job
from portfolio_backend.decision_lab_templates import DECISION_LAB_HTML
from portfolio_backend.mobile_api_service import build_mobile_refresh_payload
from portfolio_backend.web_dashboard_templates import DASHBOARD_HTML, GOOGLE_REDIRECT_CALLBACK_HTML


app = FastAPI(title="Options ROI Web Dashboard", version="0.1.0")
logger = logging.getLogger("uvicorn.error")
NO_STORE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


@app.middleware("http")
async def no_store_browser_cache(request: Request, call_next):
    response = await call_next(request)
    for header, value in NO_STORE_HEADERS.items():
        response.headers[header] = value
    return response


def _truthy_query(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


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


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "options-roi-web-dashboard",
        "version": app.version,
    }


@app.get("/login", response_class=HTMLResponse)
def login_page() -> HTMLResponse:
    if web_auth._auth_enabled() and not web_auth._auth_configured():
        return HTMLResponse(web_auth._configuration_error_html(), status_code=500)
    return HTMLResponse(web_auth._login_html())


@app.post("/login")
async def login(request: Request) -> Response:
    if not web_auth._auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    expected = web_auth._dashboard_password()
    if not expected:
        return HTMLResponse(web_auth._configuration_error_html(), status_code=500)
    body = (await request.body()).decode("utf-8")
    submitted = parse_qs(body).get("password", [""])[0]
    if not hmac.compare_digest(submitted, expected):
        return HTMLResponse(web_auth._login_html("Invalid dashboard password."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    web_auth._set_session_cookie(response, email=None, auth_method="key")
    return response


@app.post("/auth/google")
async def google_login(request: Request) -> Response:
    if not web_auth._auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    body = (await request.body()).decode("utf-8")
    fields = parse_qs(body)
    credential = fields.get("credential", [""])[0]
    state = fields.get("state", [""])[0]
    body_csrf = fields.get("g_csrf_token", [""])[0]
    cookie_csrf = request.cookies.get("g_csrf_token", "")
    if body_csrf or cookie_csrf:
        if not body_csrf or not cookie_csrf or not hmac.compare_digest(body_csrf, cookie_csrf):
            return HTMLResponse(web_auth._login_html("Google sign-in failed CSRF validation."), status_code=400)
    if not credential:
        return HTMLResponse(web_auth._login_html("Google sign-in did not return a credential."), status_code=400)
    nonce = None
    if state:
        oauth_info = web_auth._oauth_state_info(request.cookies.get(web_auth.OAUTH_STATE_COOKIE_NAME, ""))
        if not oauth_info or not hmac.compare_digest(state, oauth_info["state"]):
            return HTMLResponse(web_auth._login_html("Google sign-in failed session validation."), status_code=400)
        nonce = oauth_info["nonce"]
    try:
        claims = (
            web_auth._verify_google_credential(credential, nonce=nonce)
            if nonce is not None
            else web_auth._verify_google_credential(credential)
        )
    except PermissionError as exc:
        return HTMLResponse(web_auth._login_html(str(exc)), status_code=403)
    except Exception:
        return HTMLResponse(web_auth._login_html("Google sign-in could not be verified."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    web_auth._set_session_cookie(response, email=str(claims["email"]), auth_method="google")
    response.delete_cookie(web_auth.OAUTH_STATE_COOKIE_NAME)
    return response


@app.get("/auth/google/start")
def google_redirect_start(request: Request) -> Response:
    client_id = web_auth._google_client_id()
    if not web_auth._auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if not client_id:
        return HTMLResponse(web_auth._login_html("Google sign-in is not configured."), status_code=500)
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    params = {
        "client_id": client_id,
        "redirect_uri": web_auth._google_redirect_uri(request),
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
        web_auth.OAUTH_STATE_COOKIE_NAME,
        web_auth._oauth_state_token(state=state, nonce=nonce),
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
    response.delete_cookie(web_auth.COOKIE_NAME)
    response.delete_cookie(web_auth.OAUTH_STATE_COOKIE_NAME)
    return response


@app.post("/refresh")
def refresh(request: Request) -> Response:
    if not web_auth._is_authenticated(request):
        return web_auth._redirect_to_login()
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
    web_data._clear_dashboard_data_cache()
    # The web payload always builds the full unrealized-capable state and derives
    # both dashboard views from it. Persist the same state shape on refresh so the
    # follow-up /api/dashboard read can use the shared Firestore snapshot even
    # when the user pressed refresh while viewing the realized-only toggle.
    context, cache_bust = web_data._get_context(
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
    web_data._clear_dashboard_data_cache()
    return RedirectResponse(
        url=(
            f"/?include_unrealized={1 if include_unrealized else 0}"
            f"&section={section}&refreshed={cache_bust}"
        ),
        status_code=303,
    )


@app.post("/import")
def trigger_import(request: Request) -> Response:
    if not web_auth._is_authenticated(request):
        return web_auth._redirect_to_login()
    section = request.query_params.get("section") or "diagnostics"
    try:
        import_start = trigger_ibkr_import_job()
        status = import_start.status
    except Exception as exc:
        logger.warning("web_import_start_failed error=%s", exc)
        status = "failed"
    web_data._clear_dashboard_data_cache()
    return RedirectResponse(
        url=f"/?section={section}&import_start={status}&v={int(time())}",
        status_code=303,
    )


@app.get("/api/dashboard")
def dashboard_json(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
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
        payload = web_data._get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
            timing_recorder=record_timing,
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    decision_started_at = perf_counter()
    response_payload = web_data._with_decision_lab(payload)
    record_timing("decision_lab_attach_ms", (perf_counter() - decision_started_at) * 1000)
    record_timing("route_total_ms", (perf_counter() - started_at) * 1000)
    logger.info(
        "web_dashboard_api_timing %s",
        " ".join([f"{key}={timings[key]}" for key in sorted(timings)]),
    )
    return JSONResponse(response_payload)


@app.get("/api/decision-lab")
def decision_lab_json(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    started_at = perf_counter()
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    try:
        payload = web_data._get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
        )
        response_payload = web_data._get_cached_decision_lab_payload(payload, force_refresh=False)
        logger.info("web_decision_lab_timing total_ms=%.2f", (perf_counter() - started_at) * 1000)
        return JSONResponse(response_payload)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/assignment-quality")
def assignment_quality_json(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    started_at = perf_counter()
    try:
        source_payload = web_data._get_cached_dashboard_data(include_unrealized=True)
        payload = web_data._get_cached_assignment_quality_data(source_payload=source_payload)
    except Exception as exc:
        logger.warning("assignment_quality_payload_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)
    logger.info("web_assignment_quality_timing total_ms=%.2f", (perf_counter() - started_at) * 1000)
    return JSONResponse(payload)


@app.post("/api/decision-lab/options/refresh")
def decision_lab_options_refresh(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    try:
        payload = web_data._get_cached_dashboard_data(
            include_unrealized=include_unrealized,
            target_return=target_band["target_return"],
            target_floor=target_band["target_floor"],
        )
        web_data._clear_dashboard_data_cache()
        return JSONResponse(web_data._get_cached_decision_lab_payload(payload, force_refresh=True))
    except Exception as exc:
        logger.warning("decision_lab_option_refresh_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/decision-lab", response_class=HTMLResponse)
def decision_lab_page(request: Request) -> Response:
    if not web_auth._is_authenticated(request):
        return web_auth._redirect_to_login()
    return HTMLResponse(DECISION_LAB_HTML)


@app.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> Response:
    if not web_auth._is_authenticated(request):
        return web_auth._redirect_to_login()
    include_unrealized = _truthy_query(request.query_params.get("include_unrealized"), True)
    target_band = _monthly_target_band_from_request(request)
    payload = web_data._dashboard_shell_data(
        include_unrealized=include_unrealized,
        target_return=target_band["target_return"],
        target_floor=target_band["target_floor"],
    )
    data_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).replace("</", "<\\/")
    user = html.escape(web_auth._authenticated_user(request) or "")
    response = HTMLResponse(
        DASHBOARD_HTML.replace("__DASHBOARD_DATA__", data_json).replace("__AUTH_USER__", user)
    )
    return response


@app.get("/api/settings/monthly-target-band")
def monthly_target_band_json(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse(load_monthly_target_band())


@app.post("/api/settings/monthly-target-band")
async def update_monthly_target_band_json(request: Request) -> JSONResponse:
    if not web_auth._is_authenticated(request):
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
            updated_by=web_auth._authenticated_user(request),
            source="web",
        )
        web_data._clear_dashboard_data_cache()
        return JSONResponse(band)
    except Exception as exc:
        logger.warning("monthly_target_band_save_failed error=%s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)
