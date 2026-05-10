from __future__ import annotations

import base64
import hashlib
import html
import hmac
import json
import math
import os
import secrets
from datetime import date, datetime
from time import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token

import mobile_api
from portfolio_backend.charts import build_benchmark_growth_chart_data, build_options_cycle_chart_data
from portfolio_backend.mobile_api_service import (
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_positions_payload,
    build_mobile_refresh_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)
from portfolio_backend.performance import expectancies


app = FastAPI(title="Options ROI Web Dashboard", version="0.1.0")

COOKIE_NAME = "options_roi_web_session"
OAUTH_STATE_COOKIE_NAME = "options_roi_google_state"
DEFAULT_SESSION_DAYS = 90
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


def _truthy_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


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


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _frame_records(df: Any, *, index_name: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    frame = df.copy()
    if index_name is not None:
        frame = frame.reset_index().rename(columns={frame.reset_index().columns[0]: index_name})
    if limit is not None:
        frame = frame.head(limit)
    return _json_safe(frame.to_dict(orient="records"))


def _series_records(series: Any, value_name: str) -> List[Dict[str, Any]]:
    if series is None or getattr(series, "empty", True):
        return []
    frame = series.rename(value_name).reset_index()
    first_col = frame.columns[0]
    frame = frame.rename(columns={first_col: "month"})
    return _json_safe(frame.to_dict(orient="records"))


def _benchmark_growth_by_range(state: Any) -> Dict[str, List[Dict[str, Any]]]:
    ranges = ["3M", "6M", "YTD", "1Y", "Since inception"]
    return {
        range_choice: _frame_records(
            build_benchmark_growth_chart_data(
                state.monthly_returns_covered,
                state.aligned_bench_returns,
                range_choice,
                state.as_of,
            )
        )
        for range_choice in ranges
    }


def _stock_price_records(stock_prices: Any) -> List[Dict[str, Any]]:
    if not stock_prices:
        return []
    rows = [{"ticker": str(ticker), "price": price} for ticker, price in stock_prices.items()]
    rows.sort(key=lambda row: row["ticker"])
    return _json_safe(rows)


def _advanced_unreal_records(advanced_unreal: Any) -> List[Dict[str, Any]]:
    if advanced_unreal is None or getattr(advanced_unreal, "empty", True):
        return []
    if isinstance(advanced_unreal, pd.Series):
        frame = advanced_unreal.rename("unrealized_pnl").reset_index()
        first_col = frame.columns[0]
        frame = frame.rename(columns={first_col: "ticker"})
        return _json_safe(frame.to_dict(orient="records"))
    return _frame_records(advanced_unreal)


def _get_context(*, as_of: Optional[date], include_unrealized: bool, force_rebuild: bool = False):
    # The browser dashboard is the IBKR-first production surface. Apply these
    # defaults lazily so importing this module cannot alter Streamlit/mobile
    # tests that intentionally rely on sheet mode defaults.
    os.environ.setdefault("OPTIONS_DATA_SOURCE", "ibkr")
    os.environ.setdefault("IBKR_REPORT_SOURCE", "firestore")
    cache_bust = mobile_api._refresh_cache_bust() if force_rebuild else None
    context = mobile_api._context(
        as_of=as_of,
        include_unrealized=include_unrealized,
        selected_sheets=None,
        cache_bust=cache_bust,
        force_rebuild=force_rebuild,
    )
    if force_rebuild:
        mobile_api._set_active_cache_bust(cache_bust)
    return context, cache_bust


def _build_dashboard_data(*, as_of: Optional[date] = None, include_unrealized: bool = True) -> Dict[str, Any]:
    context, _ = _get_context(as_of=as_of, include_unrealized=include_unrealized)
    state = context.state
    dashboard = build_mobile_dashboard_payload(context)
    positions = build_mobile_positions_payload(context)
    open_shorts = build_mobile_open_option_shorts_payload(context, sort="moneyness_risk", limit=None)
    tickers = build_mobile_tickers_payload(context, include_history=False)
    monthly = build_mobile_monthly_payload(context, monthly_range="since_inception")
    yearly = build_mobile_yearly_payload(context)
    issues = build_mobile_issues_payload(context)

    benchmark_growth_by_range = _benchmark_growth_by_range(state)
    expectancy = expectancies(
        getattr(state, "realized_option_events", []),
        getattr(state, "realized_sales", []),
        state.monthly_cycles,
        getattr(state, "chain_outcomes", []),
    )

    return _json_safe(
        {
            "app": {
                "revision": os.getenv("K_REVISION", "local"),
                "restart_ts": os.getenv("WEB_RESTART_TS", ""),
            },
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "source": {
                "label": "IBKR Flex",
                "kind": "ibkr_flex",
                "row_count": int(len(getattr(state, "df_opts", []))),
                "sheet_counts": _frame_records(getattr(state, "sheet_counts", None)),
            },
            "dashboard": dashboard,
            "positions": positions,
            "open_shorts": open_shorts,
            "tickers": tickers,
            "monthly": monthly,
            "yearly": yearly,
            "issues": issues,
            "tables": {
                "monthly_cycles": _frame_records(state.monthly_cycles, index_name="month"),
                "yearly_realized": _frame_records(state.yearly),
                "yearly_with_unrealized": _frame_records(state.yearly_with_unreal),
                "per_ticker_yearly": _frame_records(state.per_ticker),
                "per_ticker_totals": _frame_records(state.per_ticker_totals),
                "benchmark_metrics": _frame_records(state.benchmark_metrics),
                "expectancy": _frame_records(expectancy),
                "inventory": _frame_records(state.inv_df),
                "open_options": _frame_records(state.open_options),
                "options_cycle_pnl": _frame_records(build_options_cycle_chart_data(state.monthly_cycles)),
                "stock_prices": _stock_price_records(getattr(state, "stock_prices", {}) or {}),
                "unrealized_by_ticker": _advanced_unreal_records(getattr(state, "advanced_unreal", None)),
                "capital_daily_tail": _frame_records(
                    getattr(state, "capital_daily", pd.DataFrame()).tail(30),
                    index_name="date",
                ),
                "dividends": _frame_records(getattr(state, "div_df", pd.DataFrame())),
            },
            "charts": {
                "benchmark_growth": benchmark_growth_by_range.get("YTD", []),
                "benchmark_growth_by_range": benchmark_growth_by_range,
                "monthly_returns": _series_records(state.monthly_returns_covered, "return"),
                "monthly_returns_unrealized_adjusted": _series_records(
                    state.monthly_returns_unrealized_adjusted,
                    "return",
                ),
            },
            "reconciliation_notes": [
                {
                    "case": "FTNT",
                    "status": "Matched",
                    "detail": "Open 95C expiring 2026-09-18 caps 100 assigned shares.",
                },
                {
                    "case": "CCJ/NVDA April",
                    "status": "Expected IBKR periodization",
                    "detail": "IBKR recognizes valid wheel roll/close economics on lifecycle dates.",
                },
                {
                    "case": "AAPL April",
                    "status": "Expected IBKR periodization",
                    "detail": "Assignment economics were recognized through prior roll events, not duplicated in April.",
                },
                {
                    "case": "SPY/ABR",
                    "status": "Excluded",
                    "detail": "Excluded from wheel P&L by accounting rules.",
                },
                {
                    "case": "ZM monthly premium",
                    "status": "Matched semantics",
                    "detail": "Incremental projection premium is separate from roll-adjusted open-chain premium.",
                },
            ],
        }
    )


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
    context, cache_bust = _get_context(as_of=None, include_unrealized=True, force_rebuild=True)
    build_mobile_refresh_payload(context, cache_bust=cache_bust)
    return RedirectResponse(url=f"/?refreshed={cache_bust}", status_code=303)


@app.get("/api/dashboard")
def dashboard_json(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse(_build_dashboard_data())


@app.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    try:
        payload = _build_dashboard_data()
    except Exception as exc:
        return HTMLResponse(_error_html(str(exc)), status_code=500)
    data_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).replace("</", "<\\/")
    user = html.escape(_authenticated_user(request) or "")
    return HTMLResponse(
        DASHBOARD_HTML.replace("__DASHBOARD_DATA__", data_json).replace("__AUTH_USER__", user)
    )


def _configuration_error_html() -> str:
    return """<!doctype html>
<html><head><title>Options ROI</title><style>{css}</style></head>
<body><main class="login"><h1>Dashboard is not configured</h1>
<p>Set WEB_GOOGLE_CLIENT_ID with WEB_AUTH_ALLOWED_EMAILS and a cookie secret, or set WEB_DASHBOARD_PASSWORD.</p></main></body></html>""".format(
        css=BASE_CSS
    )


def _error_html(message: str) -> str:
    safe = str(message).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return """<!doctype html>
<html><head><title>Options ROI</title><style>{css}</style></head>
<body><main class="login error-panel"><h1>Dashboard failed to load</h1>
<p>{message}</p><form method="post" action="/refresh"><button>Retry rebuild</button></form></main></body></html>""".format(
        css=BASE_CSS,
        message=safe,
    )


BASE_CSS = """
:root{color-scheme:dark;--bg:#090d0b;--panel:#121a16;--panel2:#18231e;--line:#2b3a32;--muted:#aab7ad;--text:#eef7ef;--accent:#48d0bd;--accent2:#7ddf8a;--warn:#f5b84c;--bad:#ff6b6b;--good:#72dd7d;--shadow:0 12px 40px rgba(0,0,0,.24)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
a{color:var(--accent)}button,input{font:inherit}.login{max-width:520px;margin:14vh auto;padding:32px;background:var(--panel);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow)}
.login h1{margin:0 0 8px;font-size:32px}.login p{color:var(--muted)}.login input{width:100%;padding:13px 14px;background:#0c120f;color:var(--text);border:1px solid var(--line);border-radius:8px;margin:12px 0}
.login button,.primary{background:var(--accent);color:#06201b;border:0;border-radius:8px;padding:12px 16px;font-weight:750;cursor:pointer}.secondary{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:8px;padding:10px 13px;cursor:pointer}
.signin-block{margin:18px 0}.google-login-button{display:inline-flex;align-items:center;gap:12px;background:#fff;color:#202124;border:1px solid #dadce0;border-radius:4px;padding:10px 16px;font-weight:700;text-decoration:none;box-shadow:0 1px 2px rgba(0,0,0,.18)}.google-login-icon{display:inline-grid;place-items:center;width:20px;height:20px;font-weight:900;color:#4285f4}.fallback-login{margin-top:18px;border-top:1px solid var(--line);padding-top:14px}.fallback-login summary{cursor:pointer;color:var(--muted);font-weight:750}.auth-user{color:var(--muted);font-size:13px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.error{color:var(--bad);font-weight:700}.error-panel{border-color:#5d2a2d;background:#211113}
"""


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


GOOGLE_REDIRECT_CALLBACK_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI</title><style>__BASE_CSS__</style></head>
<body><main class="login"><h1>Signing in</h1><p>Completing Google sign-in...</p><p class="error" id="error"></p></main>
<script>
(async () => {
  const params = new URLSearchParams(window.location.hash.slice(1) || window.location.search.slice(1));
  const credential = params.get("id_token");
  const state = params.get("state");
  if (!credential) {
    document.getElementById("error").textContent = "Google sign-in did not return a credential.";
    return;
  }
  const body = new URLSearchParams();
  body.set("credential", credential);
  if (state) body.set("state", state);
  const response = await fetch("/auth/google", {
    method: "POST",
    credentials: "same-origin",
    headers: {"Content-Type": "application/x-www-form-urlencoded"},
    body
  });
  if (response.redirected) {
    window.location.replace(response.url);
    return;
  }
  const text = await response.text();
  document.open();
  document.write(text);
  document.close();
})();
</script></body></html>""".replace("__BASE_CSS__", BASE_CSS)


LOGIN_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Options ROI</title><style>__BASE_CSS__</style></head>
<body><main class="login"><h1>Options ROI Dashboard</h1><p>Sign in with your allowed Google account. This browser will stay signed in.</p>
<p class="error">__ERROR__</p>__GOOGLE_SIGNIN__
__FALLBACK_LOGIN__</main></body></html>"""


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI Dashboard</title>
<style>
__BASE_CSS__
:root{--bg2:#0c1114;--panel3:#10161a;--panel4:#151f23;--line2:#26383b;--blue:#7aa7ff;--amber:#f6c25b;--red:#ff6f78;--green:#7ee092;--teal:#45d2c5}
body{background:#080c0f;color:var(--text)}
.topbar{position:sticky;top:0;z-index:40;background:rgba(8,12,15,.94);backdrop-filter:blur(14px);border-bottom:1px solid var(--line2)}
.topbar-inner{max-width:1540px;margin:0 auto;padding:10px 22px;display:flex;gap:16px;align-items:center;justify-content:space-between}
.brand{font-size:18px;font-weight:850;letter-spacing:.01em;white-space:nowrap}.brand small{display:block;color:var(--muted);font-size:11px;font-weight:650;margin-top:-2px}
.nav{display:flex;gap:4px;flex:1;justify-content:center;min-width:0}.nav button,.segmented button{background:transparent;color:var(--muted);border:1px solid transparent;border-radius:8px;padding:8px 11px;cursor:pointer;font-weight:750;white-space:nowrap}.nav button.active,.segmented button.active{color:#061a18;background:var(--accent);border-color:transparent}
.actions{display:flex;gap:8px;align-items:center}.actions form{margin:0}.auth-user{color:var(--muted);font-size:12px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shell{max-width:1540px;margin:0 auto;padding:22px 22px 64px}.hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:18px;align-items:end;margin:4px 0 16px}
.hero h1{font-size:34px;line-height:1.05;margin:0 0 8px}.sub{color:var(--muted);font-size:14px}.status-strip{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.badge{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line2);background:#111a1d;border-radius:999px;padding:5px 9px;color:var(--muted);font-size:12px;font-weight:750;white-space:nowrap}.badge.good{color:#c6f6ce;border-color:#2f6941}.badge.warn{color:#ffe3a3;border-color:#73551d}.badge.bad{color:#ffc0c5;border-color:#70303b}.badge.blue{color:#cfe0ff;border-color:#355189}
.control-strip{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;background:#0d1417;border:1px solid var(--line2);border-radius:8px;padding:10px 12px;margin:0 0 16px}.control-label{color:var(--muted);font-size:12px;text-transform:uppercase;font-weight:800;letter-spacing:.06em}.segmented{display:flex;gap:4px;flex-wrap:wrap}
.grid{display:grid;gap:12px}.metrics{grid-template-columns:repeat(4,minmax(180px,1fr))}.two{grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr)}.two-even{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.card,.panel{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.card{padding:14px}.panel{padding:16px}
.metric-label{color:var(--muted);font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}.metric-value{font-size:25px;font-weight:900;margin-top:5px;line-height:1.08}.metric-note{color:var(--muted);font-size:12px;margin-top:5px}.pos{color:var(--green)}.neg{color:var(--red)}.muted{color:var(--muted)}.warn-text{color:var(--amber)}.mono{font-variant-numeric:tabular-nums}
h2{font-size:21px;margin:22px 0 10px}h3{font-size:16px;margin:0 0 10px}.section{display:none}.section.active{display:block}.section-head{display:flex;align-items:end;justify-content:space-between;gap:14px;margin:20px 0 10px}.section-head h2{margin:0}.section-note{color:var(--muted);font-size:13px;margin-top:4px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:0 0 10px}.toolbar input,.toolbar select{background:#0b1113;color:var(--text);border:1px solid var(--line2);border-radius:8px;padding:9px 11px;min-height:38px}.toolbar input{min-width:260px}.toolbar .segmented button{padding:7px 10px}
.table-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;overflow:hidden}.table-title{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:12px 14px;border-bottom:1px solid var(--line2);background:#11191c}.table-title strong{font-size:15px}.table-title span{display:block;color:var(--muted);font-size:12px;margin-top:2px}
.table-scroll{overflow:auto;max-height:620px}table{width:100%;border-collapse:separate;border-spacing:0;font-size:12.5px;min-width:820px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #213034;vertical-align:middle;white-space:nowrap}th{position:sticky;top:0;z-index:2;background:#0f171a;color:#b6c4c1;font-size:11px;text-transform:uppercase;letter-spacing:.04em}th button{all:unset;cursor:pointer}td.num,th.num{text-align:right}.small-table table{min-width:620px}.wide-table table{min-width:1120px}.empty{padding:18px;color:var(--muted)}
.risk-row-itm td{background:rgba(255,111,120,.12)}.risk-row-near td{background:rgba(246,194,91,.11)}.risk-row-clear td{background:rgba(69,210,197,.07)}.risk-dot{display:inline-block;width:9px;height:9px;border-radius:999px;margin-right:6px;vertical-align:middle}.dot-bad{background:var(--red)}.dot-warn{background:var(--amber)}.dot-good{background:var(--green)}.dot-blue{background:var(--blue)}
.risk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px}.risk-card{background:#10181b;border:1px solid var(--line2);border-radius:8px;padding:12px}.risk-head{display:flex;justify-content:space-between;gap:8px}.risk-title{font-weight:900}.risk-meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:4px;margin-top:8px;color:var(--muted);font-size:12px}.pill{font-size:11px;border-radius:999px;padding:3px 7px;background:#1c2a2d;color:var(--muted);font-weight:850}.pill.bad{background:#3a171d;color:#ffc4c9}.pill.warn{background:#352714;color:#ffe1a0}.pill.good{background:#143420;color:#bff3c7}.pill.blue{background:#17243c;color:#cadcff}
.chart-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;padding:12px}.chart-title{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}.chart-title strong{font-size:15px}.chart{width:100%;height:290px;display:block}.chart text{fill:#a9b7b4;font-size:11px}.axis{stroke:#31464a;stroke-width:1}.grid-line{stroke:#1f2d30;stroke-width:1}.line{fill:none;stroke-width:2.4}.bar-pos{fill:#66d37a}.bar-neg{fill:#ff7078}.legend{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}.legend-item{font-size:12px;color:var(--muted);display:inline-flex;gap:5px;align-items:center}.legend-swatch{width:10px;height:10px;border-radius:2px}
.note-list{display:grid;gap:8px}.note{border-left:3px solid var(--accent);background:var(--panel3);border-radius:8px;padding:10px 12px}.note strong{display:block}details{border:1px solid var(--line2);border-radius:8px;padding:12px;background:var(--panel3)}summary{cursor:pointer;font-weight:850}.footnote{font-size:12px;color:var(--muted);margin-top:8px}
@media(max-width:1100px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.hero{grid-template-columns:1fr}.nav{justify-content:flex-start;overflow:auto}.topbar-inner{align-items:flex-start;flex-direction:column}.actions{width:100%;justify-content:space-between}.auth-user{max-width:48vw}.shell{padding-left:14px;padding-right:14px}.hero h1{font-size:28px}.chart{height:250px}}
@media(max-width:620px){.metrics{grid-template-columns:1fr}.toolbar input{min-width:100%;width:100%}.control-strip{align-items:flex-start}.metric-value{font-size:22px}}
</style>
</head>
<body>
<div class="topbar">
  <div class="topbar-inner">
    <div class="brand">Options ROI<small>IBKR dashboard</small></div>
    <nav class="nav" id="nav"></nav>
    <div class="actions">
      <span class="auth-user">__AUTH_USER__</span>
      <form method="post" action="/refresh"><button class="primary" type="submit">Refresh data</button></form>
      <button class="secondary" type="button" id="reloadApp">Reload app</button>
      <form method="post" action="/logout"><button class="secondary" type="submit">Logout</button></form>
    </div>
  </div>
</div>
<main class="shell">
  <section class="hero">
    <div>
      <h1>Portfolio Dashboard</h1>
      <div class="sub" id="subtitle"></div>
    </div>
    <div class="status-strip" id="statusStrip"></div>
  </section>
  <section class="control-strip">
    <div>
      <div class="control-label">Chart range</div>
      <div class="segmented" id="rangeControl"></div>
    </div>
    <div class="status-strip" id="workflowStrip"></div>
  </section>
  <section id="dashboard" class="section active"></section>
  <section id="monthly" class="section"></section>
  <section id="tickers" class="section"></section>
  <section id="positions" class="section"></section>
  <section id="performance" class="section"></section>
  <section id="diagnostics" class="section"></section>
  <section id="methodology" class="section"></section>
</main>
<script id="dashboard-data" type="application/json">__DASHBOARD_DATA__</script>
<script>
const data = JSON.parse(document.getElementById("dashboard-data").textContent);
const appState = {
  active: "dashboard",
  range: "YTD",
  openRisk: "all",
  openCoverage: "all",
  openSearch: "",
  tickerSearch: "",
  tickerYear: "all",
  sort: {}
};
const sections = [
  ["dashboard","Dashboard"], ["monthly","Monthly"], ["tickers","Tickers"],
  ["positions","Positions"], ["performance","Performance"], ["diagnostics","Diagnostics"],
  ["methodology","Methodology"]
];
const rangeOptions = ["3M","6M","YTD","1Y","Since inception"];
const colors = ["#45d2c5","#7aa7ff","#f6c25b","#b99cff","#ff8e96","#7ee092"];
const $ = (id) => document.getElementById(id);
const safe = (v) => String(v ?? "n/a").replace(/[&<>"']/g, (ch) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}[ch]));
const numeric = (v) => v === null || v === undefined || v === "" || Number.isNaN(Number(v)) ? null : Number(v);
const fmtMoney = (v, digits=0) => {
  const places = Number.isInteger(digits) ? digits : 0;
  return numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",minimumFractionDigits:places,maximumFractionDigits:places}).format(Number(v));
};
const fmtPct = (v) => numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"percent",minimumFractionDigits:1,maximumFractionDigits:1}).format(Number(v));
const fmtNum = (v) => numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{maximumFractionDigits:0}).format(Number(v));
const fmtDec = (v,d=2) => numeric(v) === null ? "n/a" : Number(v).toFixed(d);
const fmtDate = (v) => v ? String(v).slice(0,10) : "n/a";
const monthName = (v) => {
  if (!v) return "n/a";
  const d = new Date(String(v).slice(0,10) + "T00:00:00Z");
  return Number.isNaN(d.getTime()) ? fmtDate(v) : d.toLocaleDateString("en-US",{month:"short",year:"numeric",timeZone:"UTC"});
};
const cls = (v) => numeric(v) === null ? "" : Number(v) < 0 ? "neg" : Number(v) > 0 ? "pos" : "";
const moneynessCls = (v) => numeric(v) === null ? "" : Number(v) > 0 ? "neg" : Number(v) < 0 ? "pos" : "";
const get = (obj, key) => key.split(".").reduce((acc, part) => acc == null ? undefined : acc[part], obj);
function labelize(v){
  return safe(String(v ?? "n/a").replaceAll("_"," ").replace(/\\b\\w/g, ch => ch.toUpperCase()));
}
function badge(text, type=""){ return `<span class="badge ${type}">${safe(text)}</span>`; }
function card(label, value, note="", klass=""){
  return `<div class="card"><div class="metric-label">${safe(label)}</div><div class="metric-value mono ${klass}">${value}</div>${note ? `<div class="metric-note">${note}</div>` : ""}</div>`;
}
function sectionHead(title, note="", right=""){
  return `<div class="section-head"><div><h2>${safe(title)}</h2>${note ? `<div class="section-note">${note}</div>` : ""}</div>${right}</div>`;
}
function segmented(id, options, selected){
  const labels = {all:"All", itm:"ITM", near:"Near", clear:"Clear", covered:"Covered", cash_secured:"Cash-secured"};
  return `<div class="segmented" id="${id}">${options.map(o => `<button type="button" data-value="${safe(o)}" class="${o === selected ? "active" : ""}">${safe(labels[o] || o)}</button>`).join("")}</div>`;
}
function riskTone(row){
  const band = String(row.moneyness_band || "").toLowerCase();
  const m = numeric(row.moneyness);
  if (band === "in_the_money" || (m !== null && m >= 0)) return "bad";
  if (band === "at_strike" || band === "near_the_money" || (m !== null && m >= -0.05)) return "warn";
  if (m !== null && m < -0.10) return "blue";
  return "good";
}
function rowRiskClass(row){
  const tone = riskTone(row);
  if (tone === "bad") return "risk-row-itm";
  if (tone === "warn") return "risk-row-near";
  if (tone === "blue") return "risk-row-clear";
  return "";
}
function dot(row){
  const tone = riskTone(row);
  return `<span class="risk-dot ${tone === "bad" ? "dot-bad" : tone === "warn" ? "dot-warn" : tone === "blue" ? "dot-blue" : "dot-good"}"></span>`;
}
function compareValues(a,b){
  const na = numeric(a), nb = numeric(b);
  if (na !== null && nb !== null) return na - nb;
  const da = Date.parse(a), db = Date.parse(b);
  if (!Number.isNaN(da) && !Number.isNaN(db)) return da - db;
  return String(a ?? "").localeCompare(String(b ?? ""));
}
function sortedRows(tableId, rows, columns){
  const state = appState.sort[tableId];
  if (!state) return rows;
  const col = columns.find(c => c.key === state.key);
  if (!col) return rows;
  return [...rows].sort((a,b) => {
    const av = col.value ? col.value(a) : get(a, col.key);
    const bv = col.value ? col.value(b) : get(b, col.key);
    return compareValues(av,bv) * (state.dir === "desc" ? -1 : 1);
  });
}
function dataTable(tableId, rows, columns, opts={}){
  const tableRows = sortedRows(tableId, rows || [], columns);
  const title = opts.title ? `<div class="table-title"><div><strong>${safe(opts.title)}</strong>${opts.subtitle ? `<span>${safe(opts.subtitle)}</span>` : ""}</div>${opts.count === false ? "" : badge(`${fmtNum(tableRows.length)} rows`,"blue")}</div>` : "";
  if (!tableRows.length) return `<div class="table-card ${opts.small ? "small-table" : ""}">${title}<div class="empty">No rows.</div></div>`;
  const head = `<tr>${columns.map(c => `<th class="${c.num ? "num" : ""}"><button type="button" onclick="sortTable('${tableId}','${c.key}')">${safe(c.label)}</button></th>`).join("")}</tr>`;
  const body = tableRows.map(row => `<tr class="${opts.rowClass ? opts.rowClass(row) : ""}">${columns.map(c => {
    const raw = c.value ? c.value(row) : get(row, c.key);
    const value = c.format ? c.format(raw,row) : safe(raw);
    const cellClass = [c.num ? "num" : "", c.className ? c.className(raw,row) : ""].filter(Boolean).join(" ");
    return `<td class="${cellClass}">${value}</td>`;
  }).join("")}</tr>`).join("");
  return `<div class="table-card ${opts.small ? "small-table" : ""} ${opts.wide ? "wide-table" : ""}">${title}<div class="table-scroll" style="${opts.maxHeight ? `max-height:${opts.maxHeight}px` : ""}"><table><thead>${head}</thead><tbody>${body}</tbody></table></div></div>`;
}
function sortTable(tableId,key){
  const current = appState.sort[tableId] || {};
  appState.sort[tableId] = {key, dir: current.key === key && current.dir !== "desc" ? "desc" : "asc"};
  render();
}
function parseDate(v){ const d = new Date(fmtDate(v) + "T00:00:00Z"); return Number.isNaN(d.getTime()) ? null : d; }
function addMonths(date, months){ const d = new Date(date); d.setUTCMonth(d.getUTCMonth() + months); return d; }
function rangeFiltered(rows, dateKey){
  const range = appState.range;
  if (range === "Since inception") return rows || [];
  const asOf = parseDate(data.dashboard.request?.as_of || data.generated_at) || new Date();
  let start = null;
  if (range === "3M") start = addMonths(asOf, -3);
  if (range === "6M") start = addMonths(asOf, -6);
  if (range === "1Y") start = addMonths(asOf, -12);
  if (range === "YTD") start = new Date(Date.UTC(asOf.getUTCFullYear(),0,1));
  return (rows || []).filter(row => {
    const d = parseDate(get(row,dateKey));
    return !start || (d && d >= start && d <= asOf);
  });
}
function lineChart(title, rows, xKey, yKey, seriesKey, yFormat=fmtDec){
  const clean = (rows || []).filter(r => numeric(get(r,yKey)) !== null && get(r,xKey));
  if (clean.length < 2) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const w=900,h=310,pad={l:54,r:18,t:18,b:38};
  const dates=[...new Set(clean.map(r => fmtDate(get(r,xKey))))].sort();
  const yVals=clean.map(r=>numeric(get(r,yKey))).filter(v=>v!==null);
  let yMin=Math.min(...yVals), yMax=Math.max(...yVals);
  const yPad=(yMax-yMin)*0.12 || 0.05; yMin-=yPad; yMax+=yPad;
  const sx=(v)=> pad.l + (dates.indexOf(fmtDate(v)) / Math.max(dates.length-1,1)) * (w-pad.l-pad.r);
  const sy=(v)=> h-pad.b - ((v-yMin)/(yMax-yMin || 1)) * (h-pad.t-pad.b);
  const groups={}; clean.forEach(r => { const name = get(r,seriesKey) || "Series"; (groups[name] ||= []).push(r); });
  const grid=[0,.25,.5,.75,1].map(t => { const y=pad.t+t*(h-pad.t-pad.b); const value=yMax-t*(yMax-yMin); return `<line class="grid-line" x1="${pad.l}" y1="${y}" x2="${w-pad.r}" y2="${y}"></line><text x="8" y="${y+4}">${safe(yFormat(value))}</text>`; }).join("");
  const paths=Object.entries(groups).map(([name,vals],i) => {
    vals.sort((a,b)=>String(get(a,xKey)).localeCompare(String(get(b,xKey))));
    return `<path class="line" stroke="${colors[i%colors.length]}" d="${vals.map((r,j)=>(j?"L":"M")+sx(get(r,xKey))+","+sy(numeric(get(r,yKey)))).join(" ")}"><title>${safe(name)}</title></path>`;
  }).join("");
  const labels=Object.keys(groups).map((name,i)=>`<span class="legend-item"><span class="legend-swatch" style="background:${colors[i%colors.length]}"></span>${safe(name)}</span>`).join("");
  const first=dates[0], last=dates[dates.length-1];
  return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong><span class="muted">${safe(appState.range)}</span></div><svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="xMidYMid meet">${grid}<line class="axis" x1="${pad.l}" y1="${h-pad.b}" x2="${w-pad.r}" y2="${h-pad.b}"></line><line class="axis" x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${h-pad.b}"></line>${paths}<text x="${pad.l}" y="${h-10}">${safe(first)}</text><text x="${w-pad.r-84}" y="${h-10}">${safe(last)}</text></svg><div class="legend">${labels}</div></div>`;
}
function barChart(title, rows, xKey, yKey){
  const clean=(rows || []).filter(r=>numeric(get(r,yKey))!==null);
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const w=900,h=300,pad={l:58,r:18,t:18,b:48};
  const vals=clean.map(r=>numeric(get(r,yKey)) || 0);
  const max=Math.max(...vals.map(v=>Math.abs(v)),1);
  const zero=pad.t+(h-pad.t-pad.b)/2;
  const slot=(w-pad.l-pad.r)/clean.length;
  const bw=Math.max(8,slot*.68);
  const bars=clean.map((r,i)=>{
    const v=numeric(get(r,yKey)) || 0; const x=pad.l+i*slot+(slot-bw)/2;
    const bh=Math.abs(v)/max*((h-pad.t-pad.b)/2-8); const y=v>=0?zero-bh:zero;
    const label=clean.length<=14?`<text x="${x}" y="${h-18}" transform="rotate(35 ${x} ${h-18})">${safe(monthName(get(r,xKey)).split(" ")[0])}</text>`:"";
    return `<rect class="${v>=0?"bar-pos":"bar-neg"}" x="${x}" y="${y}" width="${bw}" height="${Math.max(bh,1)}"><title>${safe(monthName(get(r,xKey)))}: ${safe(fmtMoney(v))}</title></rect>${label}`;
  }).join("");
  return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong><span class="muted">${safe(appState.range)}</span></div><svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="xMidYMid meet"><line class="axis" x1="${pad.l}" y1="${zero}" x2="${w-pad.r}" y2="${zero}"></line><line class="axis" x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${h-pad.b}"></line><text x="8" y="${pad.t+8}">${safe(fmtMoney(max))}</text><text x="8" y="${h-pad.b}">${safe(fmtMoney(-max))}</text>${bars}</svg></div>`;
}
function growthFromReturns(rows){
  let growth=1;
  return rangeFiltered(rows || [],"month").sort((a,b)=>String(a.month).localeCompare(String(b.month))).map(row => {
    growth *= (1 + (numeric(row.return) || 0));
    return {month: row.month, Series: "Strategy", Growth: growth};
  });
}
function openShortRows(){
  const rows = data.positions.open_option_shorts || data.open_shorts.items || [];
  const q = appState.openSearch.trim().toUpperCase();
  return rows.filter(row => {
    const m = numeric(row.moneyness);
    const tone = riskTone(row);
    const riskOk = appState.openRisk === "all"
      || (appState.openRisk === "itm" && tone === "bad")
      || (appState.openRisk === "near" && tone === "warn")
      || (appState.openRisk === "clear" && (tone === "good" || tone === "blue"));
    const cov = String(row.covered_status || "").toLowerCase();
    const covOk = appState.openCoverage === "all"
      || (appState.openCoverage === "covered" && cov.includes("covered"))
      || (appState.openCoverage === "cash_secured" && cov.includes("cash"));
    const text = `${row.ticker || ""} ${row.option_type || ""} ${row.strike || ""} ${row.expiration || ""}`.toUpperCase();
    return riskOk && covOk && (!q || text.includes(q)) && m !== null;
  });
}
function openShortColumns(){
  return [
    {key:"ticker",label:"Ticker",format:(v,r)=>`${dot(r)}<strong>${safe(v)}</strong>`},
    {key:"option_type",label:"Type"},
    {key:"strike",label:"Strike",format:v=>fmtMoney(v,2),num:true},
    {key:"expiration",label:"Expiry",format:fmtDate},
    {key:"days_to_expiration",label:"DTE",num:true},
    {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
    {key:"moneyness",label:"Moneyness",format:fmtPct,num:true,className:moneynessCls},
    {key:"quantity",label:"Qty",num:true},
    {key:"premium_collected",label:"Premium",format:v=>fmtMoney(v,2),num:true,className:cls},
    {key:"covered_status",label:"Coverage"}
  ];
}
function openShortToolbar(){
  return `<div class="toolbar">
    ${segmented("riskControl",["all","itm","near","clear"],appState.openRisk)}
    ${segmented("coverageControl",["all","covered","cash_secured"],appState.openCoverage)}
    <input id="openSearch" value="${safe(appState.openSearch)}" placeholder="Filter ticker, strike, expiry">
  </div>
  <div class="footnote"><span class="risk-dot dot-bad"></span>ITM <span class="risk-dot dot-warn"></span>At/near strike <span class="risk-dot dot-good"></span>OK <span class="risk-dot dot-blue"></span>Deep OTM</div>`;
}
function riskCards(rows){
  const top = (rows || []).slice(0,6);
  if (!top.length) return `<div class="panel muted">No open shorts match the filters.</div>`;
  return `<div class="risk-grid">${top.map(r => {
    const tone = riskTone(r);
    return `<div class="risk-card"><div class="risk-head"><div><div class="risk-title">${safe(r.ticker)} ${safe(r.option_type)} ${safe(fmtDec(r.strike,2))}</div><div class="muted">${safe(fmtDate(r.expiration))} - ${safe(r.days_to_expiration)} DTE</div></div><span class="pill ${tone}">${labelize(r.risk_label || r.moneyness_band || tone)}</span></div><div class="risk-meta"><span>Current ${safe(fmtMoney(r.current_price,2))}</span><span>Moneyness ${safe(fmtPct(r.moneyness))}</span><span>Qty ${safe(r.quantity)}</span><span>${labelize(r.covered_status)}</span><span>Premium ${safe(fmtMoney(r.premium_collected,2))}</span><span>Opened ${safe(fmtDate(r.opened))}</span></div></div>`;
  }).join("")}</div>`;
}
function monthlyRows(){
  const cycles = new Map((data.tables.monthly_cycles || []).map(row => [fmtDate(row.month), row]));
  return (data.monthly.months || []).map(row => ({...(cycles.get(fmtDate(row.month)) || {}), ...row}));
}
function renderHeader(){
  const d=data.dashboard || {}, freshness=d.data_freshness || {}, price=freshness.price_coverage || {}, issue=d.issue_summary || {};
  const priced=price.priced_count ?? price.stocks_fetched ?? price.fetched ?? 0;
  const required=price.required_count ?? price.stocks_requested ?? price.requested ?? 0;
  const missing=price.missing_count ?? Math.max(required-priced,0);
  $("subtitle").textContent = `${data.source.label} - as of ${fmtDate(d.request?.as_of)} - generated ${fmtDate(data.generated_at)}`;
  $("statusStrip").innerHTML = [
    badge(`${priced}/${required} priced`, missing > 0 ? "bad" : "good"),
    badge(`${issue.total_count ?? 0} actionable issues`, (issue.total_count || 0) ? "bad" : "good"),
    badge(`${fmtNum(data.source.row_count)} source rows`, "blue"),
    badge(`Updated ${freshness.prices_updated_at ? String(freshness.prices_updated_at).slice(11,19) : "n/a"}`),
    badge(`App ${String(data.app?.revision || "local").replace(/^options-roi-web-/,"")}`)
  ].join("");
  $("workflowStrip").innerHTML = [
    badge("Dashboard: monitor current month"),
    badge("Positions: inventory first"),
    badge("Diagnostics: data quality")
  ].join("");
  $("rangeControl").innerHTML = rangeOptions.map(o => `<button type="button" data-value="${safe(o)}" class="${o === appState.range ? "active" : ""}">${safe(o)}</button>`).join("");
}
function renderDashboard(){
  const snap=data.dashboard.snapshot || {}, mt=data.dashboard.monthly_target || {}, shorts=openShortRows();
  const pnlRows = rangeFiltered(data.tables.options_cycle_pnl || data.tables.monthly_cycles || [], "Date");
  const benchmark = (data.charts.benchmark_growth_by_range || {})[appState.range] || data.charts.benchmark_growth || [];
  $("dashboard").innerHTML = `
    <div class="grid metrics">
      ${card("YTD total P&L", fmtMoney(snap.ytd_total_pnl), "Realized plus current unrealized snapshot", cls(snap.ytd_total_pnl))}
      ${card("YTD realized P&L", fmtMoney(snap.ytd_realized_pnl), "Options, stock P&L, and dividends", cls(snap.ytd_realized_pnl))}
      ${card("Current unrealized", fmtMoney(snap.current_unrealized_pnl), `Options ${safe(fmtMoney(snap.current_option_unrealized_pnl))} / Stock ${safe(fmtMoney(snap.current_stock_unrealized_pnl))}`, cls(snap.current_unrealized_pnl))}
      ${card("YTD annualized TWR", fmtPct(snap.ytd_annualized_twr), snap.unrealized_adjusted ? "Unrealized-adjusted" : "Realized only", cls(snap.ytd_annualized_twr))}
    </div>
    ${sectionHead("Current Month", "Projected values keep realized P&L separate from open premium.")}
    <div class="grid metrics">
      ${card("Projected month P&L", fmtMoney(mt.projected_month_pnl), `Realized ${safe(fmtMoney(mt.realized_month_pnl))} + incremental open premium ${safe(fmtMoney(mt.open_expiring_incremental_premium))}`, cls(mt.projected_month_pnl))}
      ${card("Projected return", `${safe(fmtPct(mt.projected_return_roac))} RoAC`, `Target ${safe(fmtPct(mt.target_return || data.monthly.target_return))} - ${labelize(mt.monthly_target_status || mt.status || "status n/a")}`, cls((numeric(mt.projected_return_roac)||0) - (numeric(mt.target_return || data.monthly.target_return)||0)))}
      ${card("Remaining to target", fmtMoney(mt.projected_remaining_pnl), "Based on projected monthly target", cls(-1*(numeric(mt.projected_remaining_pnl)||0)))}
      ${card("Roll-adjusted open premium", fmtMoney(mt.open_expiring_roll_adjusted_premium), "Display/reconciliation only", cls(mt.open_expiring_roll_adjusted_premium))}
    </div>
    ${sectionHead("Open Shorts Monitor", `${shorts.length} open shorts match current filters.`, "")}
    ${openShortToolbar()}
    ${riskCards(shorts)}
    <div style="height:12px"></div>
    ${dataTable("dashboard-open-shorts", shorts, openShortColumns(), {title:"Open shorts", subtitle:"Sorted by backend moneyness risk; click any header to sort.", rowClass:rowRiskClass, wide:true, maxHeight:460})}
    ${sectionHead("Performance Charts", "Use the range control above to match Streamlit chart behavior.")}
    <div class="grid two-even">${barChart("P&L by Options Cycle", pnlRows, "Date", "pnl")}${lineChart("Cumulative Growth vs Benchmarks", benchmark, "Date", "Growth", "Series", v=>fmtDec(v,2)+"x")}</div>
  `;
}
function renderMonthly(){
  const rows = monthlyRows();
  const filtered = rangeFiltered(rows, "month");
  const returns = growthFromReturns(data.charts.monthly_returns || []);
  const future = data.monthly.future_months || [];
  $("monthly").innerHTML = `
    ${sectionHead("Monthly Performance", "Calendar-month realized results plus explicit current/future projection fields.")}
    <div class="grid two-even">
      ${lineChart("Cumulative Growth by Month", returns, "month", "Growth", "Series", v=>fmtDec(v,2)+"x")}
      ${barChart("Monthly Realized P&L", filtered.map(r=>({Date:r.month,pnl:r.total_realized_pnl})), "Date", "pnl")}
    </div>
    <div style="height:12px"></div>
    ${dataTable("monthly-table", rows, [
      {key:"month",label:"Month",format:monthName},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true},
      {key:"total_realized_pnl",label:"Total realized",format:fmtMoney,num:true,className:cls},
      {key:"avg_capital",label:"Avg capital",format:fmtMoney,num:true},
      {key:"peak_capital",label:"Peak capital",format:fmtMoney,num:true},
      {key:"return_roac",label:"RoAC",format:fmtPct,num:true,className:cls},
      {key:"return_ropc",label:"RoPC",format:fmtPct,num:true,className:cls},
      {key:"open_expiring_incremental_premium",label:"Open incremental",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_roll_adjusted_premium",label:"Open roll-adjusted",format:fmtMoney,num:true,className:cls},
      {key:"projected_month_pnl",label:"Projected P&L",format:fmtMoney,num:true,className:cls},
      {key:"projected_return_roac",label:"Projected RoAC",format:fmtPct,num:true,className:cls},
      {key:"monthly_target_status",label:"Target status"}
    ], {title:"Monthly table", subtitle:"Matches Streamlit monthly columns and adds explicit projection semantics.", wide:true})}
    ${sectionHead("Future Open Expiry Months", "Future months are shown separately so iOS/web do not infer roll semantics client-side.")}
    ${dataTable("future-months", future, [
      {key:"month",label:"Month",format:monthName},
      {key:"open_option_count",label:"Open options",num:true},
      {key:"open_expiring_incremental_premium",label:"Incremental premium",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_roll_adjusted_premium",label:"Roll-adjusted premium",format:fmtMoney,num:true,className:cls},
      {key:"projected_month_pnl",label:"Projected P&L",format:fmtMoney,num:true,className:cls},
      {key:"projected_return_roac",label:"Projected RoAC",format:fmtPct,num:true},
      {key:"projection_basis",label:"Basis"}
    ], {title:"Future expiry months", small:true})}
  `;
}
function renderTickers(){
  const rows = data.tickers.items || data.tables.per_ticker_totals || [];
  const years = ["all", ...[...new Set((data.tables.per_ticker_yearly || []).map(r => String(r.year)))].sort()];
  const q = appState.tickerSearch.trim().toUpperCase();
  const filtered = rows.filter(r => !q || String(r.ticker || "").toUpperCase().includes(q));
  const yearly = (data.tables.per_ticker_yearly || []).filter(r => (appState.tickerYear === "all" || String(r.year) === appState.tickerYear) && (!q || String(r.ticker || "").toUpperCase().includes(q)));
  $("tickers").innerHTML = `
    ${sectionHead("Per-Ticker P&L", "Ticker totals include realized options, realized stock, dividends, unrealized snapshot, and total P&L.")}
    <div class="toolbar"><input id="tickerSearch" value="${safe(appState.tickerSearch)}" placeholder="Filter ticker"><select id="tickerYear">${years.map(y=>`<option value="${safe(y)}" ${y===appState.tickerYear?"selected":""}>${safe(y==="all"?"All years":y)}</option>`).join("")}</select></div>
    ${dataTable("ticker-totals", filtered, [
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true,className:cls},
      {key:"combined_realized_pnl",label:"Realized",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"total_pnl",label:"Total P&L",format:fmtMoney,num:true,className:cls},
      {key:"current_price",label:"Price",format:v=>fmtMoney(v,2),num:true},
      {key:"open_option_count",label:"Open options",num:true},
      {key:"inventory_share_count",label:"Shares",num:true}
    ], {title:"Ticker totals", subtitle:"Click headers to sort; use the filter above to inspect a ticker.", wide:true})}
    ${sectionHead("Per-Year Ticker Realized P&L")}
    ${dataTable("ticker-yearly", yearly, [
      {key:"year",label:"Year",num:true},
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`},
      {key:"options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"stock_realized_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"combined_realized",label:"Total realized",format:fmtMoney,num:true,className:cls}
    ], {title:"Per-year realized P&L", wide:true})}
  `;
}
function renderPositions(){
  const shorts = openShortRows();
  $("positions").innerHTML = `
    ${sectionHead("Assigned Holdings and Exposure", "Inventory comes first; open shorts are monitored separately below.")}
    ${dataTable("inventory", data.positions.inventory || [], [
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`},
      {key:"buy_date",label:"Buy date",format:fmtDate},
      {key:"shares",label:"Shares",num:true},
      {key:"cost_per_share",label:"Cost/share",format:v=>fmtMoney(v,2),num:true},
      {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
      {key:"covered_shares",label:"Covered shares",num:true},
      {key:"covered_strike",label:"Covered strike",format:v=>fmtMoney(v,2),num:true},
      {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"source",label:"Source"}
    ], {title:"Assigned holdings", subtitle:"Covered strike caps assigned holdings when covered calls are open.", wide:true})}
    ${sectionHead("Open Option Shorts", `${shorts.length} rows after filters.`)}
    ${openShortToolbar()}
    ${dataTable("positions-open-shorts", shorts, openShortColumns(), {title:"Open option shorts", subtitle:"Moneyness colors match the Streamlit control dashboard.", rowClass:rowRiskClass, wide:true})}
  `;
}
function renderPerformance(){
  const benchmark = (data.charts.benchmark_growth_by_range || {})[appState.range] || data.charts.benchmark_growth || [];
  const pnlRows = rangeFiltered(data.tables.options_cycle_pnl || [], "Date");
  $("performance").innerHTML = `
    ${sectionHead("Yearly Performance", "Realized yearly table plus unrealized-adjusted current-year view.")}
    ${dataTable("yearly-mobile", data.yearly.years || [], [
      {key:"year",label:"Year",num:true},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true},
      {key:"total_realized_pnl",label:"Realized",format:fmtMoney,num:true,className:cls},
      {key:"total_pnl_including_unrealized",label:"With unrealized",format:fmtMoney,num:true,className:cls},
      {key:"avg_capital",label:"Avg capital",format:fmtMoney,num:true},
      {key:"peak_capital",label:"Peak capital",format:fmtMoney,num:true},
      {key:"roac_year",label:"RoAC",format:fmtPct,num:true,className:cls},
      {key:"ropc_year",label:"RoPC",format:fmtPct,num:true,className:cls},
      {key:"annualized_twr",label:"Ann. TWR",format:fmtPct,num:true,className:cls},
      {key:"annualized_twr_unrealized_adjusted",label:"Ann. TWR adj.",format:fmtPct,num:true,className:cls}
    ], {title:"Yearly performance", wide:true})}
    <div style="height:12px"></div>
    <div class="grid two-even">${lineChart("Cumulative Growth vs Benchmarks", benchmark, "Date", "Growth", "Series", v=>fmtDec(v,2)+"x")}${barChart("P&L by Options Cycle", pnlRows, "Date", "pnl")}</div>
    ${sectionHead("Benchmark Metrics")}
    ${dataTable("benchmark-metrics", data.tables.benchmark_metrics || [], [
      {key:"Series",label:"Series"},
      {key:"CAGR",label:"CAGR",format:fmtPct,num:true},
      {key:"Volatility",label:"Volatility",format:fmtPct,num:true},
      {key:"Sharpe",label:"Sharpe",format:v=>fmtDec(v,2),num:true},
      {key:"Sortino",label:"Sortino",format:v=>fmtDec(v,2),num:true},
      {key:"Max Drawdown",label:"Max DD",format:fmtPct,num:true},
      {key:"Return 3M",label:"3M",format:fmtPct,num:true},
      {key:"Return 6M",label:"6M",format:fmtPct,num:true},
      {key:"Return YTD",label:"YTD",format:fmtPct,num:true},
      {key:"Return 1Y",label:"1Y",format:fmtPct,num:true},
      {key:"Return SI",label:"Since inception",format:fmtPct,num:true}
    ], {title:"Key performance metrics versus benchmarks", wide:true})}
    ${sectionHead("Expectancy Analysis")}
    ${dataTable("expectancy", data.tables.expectancy || [], [
      {key:"Category",label:"Category"},
      {key:"Count",label:"Count",num:true},
      {key:"Win rate",label:"Win rate",format:fmtPct,num:true},
      {key:"Avg win",label:"Avg win",format:fmtMoney,num:true,className:cls},
      {key:"Avg loss",label:"Avg loss",format:fmtMoney,num:true,className:cls},
      {key:"Expectancy",label:"Expectancy",format:fmtMoney,num:true,className:cls},
      {key:"Total P&L",label:"Total P&L",format:fmtMoney,num:true,className:cls}
    ], {title:"Expectancy", small:true})}
  `;
}
function renderDiagnostics(){
  const iss=data.issues || {}, sum=iss.summary || {}, aud=iss.audit_summary || {}, fresh=data.dashboard.data_freshness || {}, coverage=fresh.price_coverage || {};
  const priced=coverage.priced_count ?? coverage.stocks_fetched ?? coverage.fetched ?? 0;
  const required=coverage.required_count ?? coverage.stocks_requested ?? coverage.requested ?? 0;
  $("diagnostics").innerHTML = `
    ${sectionHead("Data Health", "Actionable issues are separated from expected IBKR audit notes.")}
    <div class="grid metrics">
      ${card("Actionable issues", fmtNum(sum.total_count || 0), "Warnings/errors requiring attention", (sum.total_count || 0) ? "neg" : "pos")}
      ${card("Audit notes", fmtNum(aud.total_count || 0), "Expected classification notes")}
      ${card("Current prices", `${safe(priced)}/${safe(required)}`, "Required pricing coverage", (required && priced < required) ? "neg" : "pos")}
      ${card("Source rows", fmtNum(data.source.row_count), data.source.kind || "source")}
    </div>
    ${sectionHead("Actionable Issues")}
    ${dataTable("issues", iss.issues || [], [
      {key:"severity",label:"Severity"},
      {key:"category",label:"Category"},
      {key:"message",label:"Message"}
    ], {title:"Warnings and errors", wide:true})}
    ${sectionHead("Source and Coverage")}
    <div class="grid two-even">
      ${dataTable("sheet-counts", data.source.sheet_counts || [], [
        {key:"source_sheet",label:"Source",value:r=>r.source_sheet || r.sheet || r.name},
        {key:"rows",label:"Rows",num:true,value:r=>r.rows}
      ], {title:"Loaded rows by source", small:true})}
      ${dataTable("stock-prices", data.tables.stock_prices || [], [
        {key:"ticker",label:"Ticker"},
        {key:"price",label:"Price",format:v=>fmtMoney(v,2),num:true}
      ], {title:"Stock prices used", small:true, maxHeight:360})}
    </div>
    ${sectionHead("Unrealized and Cashflow Detail")}
    <div class="grid two-even">
      ${dataTable("unrealized-by-ticker", data.tables.unrealized_by_ticker || [], [
        {key:"ticker",label:"Ticker"},
        {key:"unrealized_pnl",label:"Unrealized P&L",format:fmtMoney,num:true,className:cls}
      ], {title:"Unrealized by ticker", small:true, maxHeight:360})}
      ${dataTable("dividends", data.tables.dividends || [], [
        {key:"ex_date",label:"Ex/pay date",format:fmtDate},
        {key:"ticker",label:"Ticker"},
        {key:"shares",label:"Shares",num:true},
        {key:"per_share",label:"Per share",format:v=>fmtMoney(v,2),num:true},
        {key:"cash",label:"Cash",format:fmtMoney,num:true,className:cls}
      ], {title:"Dividends", small:true, maxHeight:360})}
    </div>
    ${sectionHead("Reconciliation Notes")}
    <div class="note-list">${(data.reconciliation_notes || []).map(n=>`<div class="note"><strong>${safe(n.case)} - ${safe(n.status)}</strong><span class="muted">${safe(n.detail)}</span></div>`).join("")}</div>
    ${sectionHead("Audit Notes")}
    <details><summary>Show ${fmtNum(aud.total_count || 0)} audit notes</summary><div style="height:10px"></div>${dataTable("audit-notes", (iss.audit_notes || []).slice(0,600), [
      {key:"category",label:"Category"},
      {key:"severity",label:"Severity"},
      {key:"message",label:"Message"}
    ], {title:"Wheel audit notes", subtitle:"Expected non-wheel exclusions and classification notes.", wide:true})}</details>
    ${sectionHead("Capital Tail")}
    ${dataTable("capital-tail", data.tables.capital_daily_tail || [], [
      {key:"date",label:"Date",format:fmtDate},
      {key:"total",label:"Total capital",format:fmtMoney,num:true},
      {key:"shares_invested",label:"Shares invested",format:fmtMoney,num:true},
      {key:"puts_reserve",label:"Put reserve",format:fmtMoney,num:true}
    ], {title:"Capital daily tail", wide:true})}
  `;
}
function renderMethodology(){
  $("methodology").innerHTML = `
    ${sectionHead("Methodology", "Same backend accounting as iOS, with web-only diagnostic breadth.")}
    <div class="grid two-even">
      <div class="panel"><h3>Source</h3><p>Production web and iOS read imported IBKR Flex data from Firestore. Streamlit remains the Google Sheets backup/control dashboard.</p><h3>Wheel scope</h3><p>Wheel P&L starts with assigned puts. Covered calls are included when backed by assignment-derived shares or valid covered-call roll replacements. Expected non-wheel exclusions are audit notes, not actionable issues.</p><h3>Monthly projections</h3><p>Realized P&L stays separate from projected values. Open expiring incremental premium is additive. Roll-adjusted open premium is displayed for reconciliation and is not added again.</p></div>
      <div class="panel"><h3>Unrealized snapshot</h3><p>Current unrealized values are monitoring snapshots, not complete option mark-to-market accounting. Missing required prices suppress affected unrealized fields.</p><h3>Benchmarks</h3><p>Return metrics compare monthly strategy returns with aligned benchmark monthly series when coverage is complete.</p><h3>Refresh</h3><p>Refresh rebuilds the cached backend context and price overlay without changing accounting rules.</p></div>
    </div>
  `;
}
function bindControls(){
  const reloadApp = $("reloadApp");
  if (reloadApp && !reloadApp.dataset.bound) {
    reloadApp.dataset.bound = "1";
    reloadApp.addEventListener("click", () => {
      const url = new URL(window.location.href);
      url.searchParams.set("v", Date.now().toString());
      window.location.replace(url.toString());
    });
  }
  document.querySelectorAll("#rangeControl button").forEach(btn => btn.addEventListener("click", () => { appState.range = btn.dataset.value; render(); }));
  const risk = $("riskControl"); if (risk) risk.querySelectorAll("button").forEach(btn => btn.addEventListener("click", () => { appState.openRisk = btn.dataset.value; render(); }));
  const coverage = $("coverageControl"); if (coverage) coverage.querySelectorAll("button").forEach(btn => btn.addEventListener("click", () => { appState.openCoverage = btn.dataset.value; render(); }));
  const openSearch = $("openSearch"); if (openSearch) openSearch.addEventListener("input", (e) => { appState.openSearch = e.target.value; render(); });
  const tickerSearch = $("tickerSearch"); if (tickerSearch) tickerSearch.addEventListener("input", (e) => { appState.tickerSearch = e.target.value; render(); });
  const tickerYear = $("tickerYear"); if (tickerYear) tickerYear.addEventListener("change", (e) => { appState.tickerYear = e.target.value; render(); });
}
function render(){
  try {
    renderHeader();
    [...$("nav").children].forEach(b => b.classList.toggle("active", b.dataset.section === appState.active));
    renderSection("dashboard", renderDashboard);
    renderSection("monthly", renderMonthly);
    renderSection("tickers", renderTickers);
    renderSection("positions", renderPositions);
    renderSection("performance", renderPerformance);
    renderSection("diagnostics", renderDiagnostics);
    renderSection("methodology", renderMethodology);
    document.querySelectorAll(".section").forEach(s => s.classList.toggle("active", s.id === appState.active));
    bindControls();
  } catch (err) {
    const target = $(appState.active);
    if (target) {
      target.innerHTML = `<div class="panel"><h2>Section failed to render</h2><p class="error">${safe(err && (err.stack || err.message) || err)}</p></div>`;
    }
    console.error(err);
  }
}
function renderSection(id, fn){
  try {
    fn();
  } catch (err) {
    const target = $(id);
    if (target) {
      target.innerHTML = `<div class="panel"><h2>${safe(id)} failed to render</h2><p class="error">${safe(err && (err.stack || err.message) || err)}</p></div>`;
    }
    console.error(err);
  }
}
function initNav(){
  $("nav").innerHTML = sections.map(([id,label]) => `<button type="button" data-section="${id}" class="${id === appState.active ? "active" : ""}">${safe(label)}</button>`).join("");
  [...$("nav").children].forEach(btn => btn.addEventListener("click", () => { appState.active = btn.dataset.section; render(); window.scrollTo({top:0,behavior:"instant"}); }));
}
initNav();
render();
</script>
</body>
</html>""".replace(
    "__BASE_CSS__", BASE_CSS
)
