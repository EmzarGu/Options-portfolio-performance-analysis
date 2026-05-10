from __future__ import annotations

import base64
import hashlib
import html
import hmac
import json
import math
import os
from datetime import date, datetime
from time import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token

import mobile_api
from portfolio_backend.charts import build_benchmark_growth_chart_data
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


app = FastAPI(title="Options ROI Web Dashboard", version="0.1.0")

COOKIE_NAME = "options_roi_web_session"
DEFAULT_SESSION_DAYS = 90


def _truthy_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _auth_enabled() -> bool:
    return _truthy_env("WEB_DASHBOARD_AUTH", True)


def _dashboard_password() -> Optional[str]:
    return os.getenv("WEB_DASHBOARD_PASSWORD") or os.getenv("MOBILE_API_KEY")


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


def _verify_google_credential(credential: str) -> Dict[str, Any]:
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
    allowed = _allowed_google_emails()
    if not allowed:
        raise PermissionError("No Google account allowlist is configured.")
    if email not in allowed:
        raise PermissionError("This Google account is not allowed for this dashboard.")
    return {**claims, "email": email}


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

    benchmark_growth = build_benchmark_growth_chart_data(
        state.monthly_returns_covered,
        state.aligned_bench_returns,
        "YTD",
        state.as_of,
    )

    return _json_safe(
        {
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
                "inventory": _frame_records(state.inv_df),
                "open_options": _frame_records(state.open_options),
            },
            "charts": {
                "benchmark_growth": _frame_records(benchmark_growth),
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
        return HTMLResponse(_login_html("Invalid password or API key."), status_code=401)
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
    body_csrf = fields.get("g_csrf_token", [""])[0]
    cookie_csrf = request.cookies.get("g_csrf_token", "")
    if body_csrf or cookie_csrf:
        if not body_csrf or not cookie_csrf or not hmac.compare_digest(body_csrf, cookie_csrf):
            return HTMLResponse(_login_html("Google sign-in failed CSRF validation."), status_code=400)
    if not credential:
        return HTMLResponse(_login_html("Google sign-in did not return a credential."), status_code=400)
    try:
        claims = _verify_google_credential(credential)
    except PermissionError as exc:
        return HTMLResponse(_login_html(str(exc)), status_code=403)
    except Exception:
        return HTMLResponse(_login_html("Google sign-in could not be verified."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    _set_session_cookie(response, email=str(claims["email"]), auth_method="google")
    return response


@app.post("/logout")
def logout() -> Response:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


@app.post("/refresh")
def refresh(request: Request) -> Response:
    if not _is_authenticated(request):
        return _redirect_to_login()
    context, cache_bust = _get_context(as_of=None, include_unrealized=True, force_rebuild=True)
    build_mobile_refresh_payload(context, cache_bust=cache_bust)
    return RedirectResponse(url="/?refreshed=1", status_code=303)


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
<p>Set WEB_GOOGLE_CLIENT_ID with WEB_AUTH_ALLOWED_EMAILS and a cookie secret, or set WEB_DASHBOARD_PASSWORD / MOBILE_API_KEY.</p></main></body></html>""".format(
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
.signin-block{margin:18px 0}.fallback-login{margin-top:18px;border-top:1px solid var(--line);padding-top:14px}.fallback-login summary{cursor:pointer;color:var(--muted);font-weight:750}.auth-user{color:var(--muted);font-size:13px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.error{color:var(--bad);font-weight:700}.error-panel{border-color:#5d2a2d;background:#211113}
"""


def _login_html(error: str = "") -> str:
    safe_error = error.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    google_signin = _google_signin_html()
    fallback_open = "false" if google_signin else "true"
    fallback_label = "Use API key instead" if google_signin else "Use API key"
    return (
        LOGIN_TEMPLATE.replace("__BASE_CSS__", BASE_CSS)
        .replace("__ERROR__", safe_error)
        .replace("__GOOGLE_SIGNIN__", google_signin)
        .replace("__FALLBACK_OPEN__", " open" if fallback_open == "true" else "")
        .replace("__FALLBACK_LABEL__", fallback_label)
    )


def _google_signin_html() -> str:
    client_id = _google_client_id()
    if not client_id:
        return ""
    safe_client_id = html.escape(client_id, quote=True)
    return f"""
<div class="signin-block">
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <div id="g_id_onload"
       data-client_id="{safe_client_id}"
       data-login_uri="/auth/google"
       data-auto_prompt="false"></div>
  <div class="g_id_signin"
       data-type="standard"
       data-theme="filled_black"
       data-size="large"
       data-text="signin_with"
       data-shape="rectangular"
       data-logo_alignment="left"></div>
</div>"""


LOGIN_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Options ROI</title><style>__BASE_CSS__</style></head>
<body><main class="login"><h1>Options ROI Dashboard</h1><p>Sign in with your allowed Google account. This browser will stay signed in.</p>
<p class="error">__ERROR__</p>__GOOGLE_SIGNIN__
<details class="fallback-login"__FALLBACK_OPEN__><summary>__FALLBACK_LABEL__</summary><form method="post" action="/login"><input name="password" type="password" autocomplete="current-password" autofocus placeholder="Password or API key"><button type="submit">Open dashboard</button></form></details></main></body></html>"""


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI Dashboard</title>
<style>
__BASE_CSS__
.shell{max-width:1440px;margin:0 auto;padding:24px 22px 56px}.topbar{position:sticky;top:0;z-index:10;background:rgba(9,13,11,.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--line)}
.topbar-inner{max-width:1440px;margin:0 auto;padding:12px 22px;display:flex;gap:14px;align-items:center;justify-content:space-between}.brand{font-size:18px;font-weight:800}.nav{display:flex;gap:4px;flex-wrap:wrap}.nav button{background:transparent;color:var(--muted);border:1px solid transparent;border-radius:8px;padding:8px 11px;cursor:pointer}.nav button.active{color:var(--text);background:var(--panel2);border-color:var(--line)}
.actions{display:flex;gap:8px;align-items:center}.actions form{margin:0}.header{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:18px;align-items:end;margin:24px 0 18px}.title h1{font-size:34px;line-height:1.1;margin:0 0 7px}.sub{color:var(--muted)}
.badge{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:var(--panel2);border-radius:999px;padding:5px 9px;color:var(--muted);font-size:13px}.badge.good{color:#b8f2c0;border-color:#315f39}.badge.warn{color:#ffd987;border-color:#6c5625}.badge.bad{color:#ffb0b0;border-color:#633}
.grid{display:grid;gap:12px}.metrics{grid-template-columns:repeat(4,minmax(0,1fr))}.two{grid-template-columns:1.15fr .85fr}.three{grid-template-columns:repeat(3,minmax(0,1fr))}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:16px;box-shadow:0 8px 26px rgba(0,0,0,.14)}
.metric-label{color:var(--muted);font-size:13px}.metric-value{font-size:26px;font-weight:850;margin-top:5px}.metric-note{color:var(--muted);font-size:12px;margin-top:2px}.pos{color:var(--good)}.neg{color:var(--bad)}.muted{color:var(--muted)}.warn-text{color:var(--warn)}
h2{font-size:22px;margin:26px 0 12px}h3{font-size:16px;margin:0 0 12px}.section{display:none}.section.active{display:block}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px}th,td{text-align:left;padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-weight:700;background:#101712;position:sticky;top:57px;z-index:2}td.num,th.num{text-align:right}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}.table-wrap table th:first-child{border-top-left-radius:8px}.table-wrap table th:last-child{border-top-right-radius:8px}
.risk-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:10px}.risk-card{border:1px solid var(--line);border-radius:8px;background:var(--panel);padding:12px}.risk-head{display:flex;justify-content:space-between;gap:10px}.risk-title{font-weight:850}.pill{font-size:12px;border-radius:999px;padding:3px 7px;background:#24322b;color:var(--muted)}.pill.bad{background:#3a191b;color:#ffc3c3}.pill.warn{background:#3a2b10;color:#ffd987}.pill.good{background:#153820;color:#b5f4be}.risk-meta{margin-top:8px;color:var(--muted);font-size:12px;display:grid;grid-template-columns:1fr 1fr;gap:4px}
.chart{width:100%;height:280px}.chart text{fill:var(--muted);font-size:11px}.chart .axis{stroke:#3a4a41}.chart .line{fill:none;stroke-width:2.2}.chart .bar-pos{fill:#52d273}.chart .bar-neg{fill:#ff6b6b}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin:10px 0 14px}.toolbar input{background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--text);padding:10px 12px;min-width:260px}
.note-list{display:grid;gap:8px}.note{border-left:3px solid var(--accent);background:var(--panel);border-radius:8px;padding:10px 12px}.note strong{display:block}details{border:1px solid var(--line);border-radius:8px;padding:12px;background:var(--panel)}summary{cursor:pointer;font-weight:800}.status-strip{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
@media(max-width:900px){.metrics,.two,.three{grid-template-columns:1fr}.header{grid-template-columns:1fr}.topbar-inner{align-items:flex-start;flex-direction:column}.actions{width:100%;justify-content:space-between}.title h1{font-size:28px}th{position:static}.shell{padding-left:14px;padding-right:14px}}
</style>
</head>
<body>
<div class="topbar"><div class="topbar-inner"><div class="brand">Options ROI</div><nav class="nav" id="nav"></nav><div class="actions"><span class="auth-user">__AUTH_USER__</span><form method="post" action="/refresh"><button class="primary" type="submit">Refresh</button></form><form method="post" action="/logout"><button class="secondary" type="submit">Logout</button></form></div></div></div>
<main class="shell">
<section class="header"><div class="title"><h1>Portfolio Dashboard</h1><div class="sub" id="subtitle"></div></div><div class="status-strip" id="statusStrip"></div></section>
<section id="overview" class="section active"></section>
<section id="monthly" class="section"></section>
<section id="tickers" class="section"></section>
<section id="positions" class="section"></section>
<section id="performance" class="section"></section>
<section id="diagnostics" class="section"></section>
<section id="methodology" class="section"></section>
</main>
<script id="dashboard-data" type="application/json">__DASHBOARD_DATA__</script>
<script>
const data = JSON.parse(document.getElementById('dashboard-data').textContent);
const sections = [
  ['overview','Dashboard'], ['monthly','Monthly'], ['tickers','Tickers'],
  ['positions','Positions'], ['performance','Performance'], ['diagnostics','Diagnostics'],
  ['methodology','Methodology']
];
const nav = document.getElementById('nav');
sections.forEach(([id,label],i)=>{ const b=document.createElement('button'); b.textContent=label; b.onclick=()=>show(id); if(i===0)b.className='active'; nav.appendChild(b); });
function show(id){ document.querySelectorAll('.section').forEach(s=>s.classList.toggle('active',s.id===id)); [...nav.children].forEach(b=>b.classList.toggle('active',b.textContent===sections.find(x=>x[0]===id)[1])); window.scrollTo({top:0,behavior:'instant'}); }
const $ = (id)=>document.getElementById(id);
const val = (v)=> v === null || v === undefined || Number.isNaN(v) ? null : Number(v);
const money = (v)=> val(v)===null ? 'n/a' : new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(v);
const money2 = (v)=> val(v)===null ? 'n/a' : new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2}).format(v);
const pct = (v)=> val(v)===null ? 'n/a' : new Intl.NumberFormat('en-US',{style:'percent',minimumFractionDigits:1,maximumFractionDigits:1}).format(v);
const num = (v)=> val(v)===null ? 'n/a' : new Intl.NumberFormat('en-US',{maximumFractionDigits:0}).format(v);
const dec = (v,d=2)=> val(v)===null ? 'n/a' : Number(v).toFixed(d);
const dateFmt = (v)=> v ? String(v).slice(0,10) : 'n/a';
const cls = (v)=> val(v)===null ? '' : Number(v) < 0 ? 'neg' : Number(v) > 0 ? 'pos' : '';
function card(label,value,note,klass=''){return `<div class="card"><div class="metric-label">${label}</div><div class="metric-value ${klass}">${value}</div>${note?`<div class="metric-note">${note}</div>`:''}</div>`}
function badge(text,type=''){return `<span class="badge ${type}">${text}</span>`}
function table(rows, cols, opts={}) {
  if(!rows || rows.length===0) return '<div class="card muted">No rows.</div>';
  const body = rows.map(r=>'<tr>'+cols.map(c=>{
    const raw = c.value ? c.value(r) : r[c.key];
    const value = c.format ? c.format(raw,r) : (raw ?? 'n/a');
    const k = c.num ? 'num ' + (c.className ? c.className(raw,r) : '') : (c.className ? c.className(raw,r) : '');
    return `<td class="${k}">${value}</td>`;
  }).join('')+'</tr>').join('');
  const head = '<tr>'+cols.map(c=>`<th class="${c.num?'num':''}">${c.label}</th>`).join('')+'</tr>';
  return `<div class="table-wrap"><table><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
}
function chartLine(rows, xKey, yKey, seriesKey) {
  if(!rows || rows.length<2) return '<div class="card muted">Chart unavailable.</div>';
  const w=760,h=280,p=34; const xs=[...new Set(rows.map(r=>dateFmt(r[xKey])))].sort();
  const yVals=rows.map(r=>val(r[yKey])).filter(v=>v!==null); let yMin=Math.min(...yVals), yMax=Math.max(...yVals); if(yMin===yMax){yMin-=.05;yMax+=.05}
  const sx=(x)=>p+(xs.indexOf(dateFmt(x))/(Math.max(xs.length-1,1)))*(w-p*2); const sy=(y)=>h-p-((y-yMin)/(yMax-yMin))*(h-p*2);
  const colors=['#48d0bd','#7ddf8a','#f5b84c','#b69bff','#ff8b8b'];
  const groups={}; rows.forEach(r=>{ const k=r[seriesKey]||'Series'; (groups[k] ||= []).push(r); });
  const paths=Object.entries(groups).map(([name,vals],i)=>`<path class="line" stroke="${colors[i%colors.length]}" d="${vals.map((r,j)=>(j?'L':'M')+sx(r[xKey])+','+sy(val(r[yKey]))).join(' ')}"><title>${name}</title></path>`).join('');
  const labels=Object.keys(groups).map((n,i)=>`<span class="badge" style="border-color:${colors[i%colors.length]}">${n}</span>`).join('');
  return `<div class="card"><svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><line class="axis" x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}"></line><line class="axis" x1="${p}" y1="${p}" x2="${p}" y2="${h-p}"></line>${paths}<text x="${p}" y="${p-8}">${yMax.toFixed(2)}</text><text x="${p}" y="${h-8}">${yMin.toFixed(2)}</text></svg><div class="status-strip">${labels}</div></div>`;
}
function chartBars(rows, xKey, yKey) {
  if(!rows || rows.length===0) return '<div class="card muted">Chart unavailable.</div>';
  const w=760,h=260,p=32; const vals=rows.map(r=>val(r[yKey])||0); const max=Math.max(...vals.map(v=>Math.abs(v)),1);
  const barW=(w-p*2)/rows.length*.72, gap=(w-p*2)/rows.length*.28; const zero=h/2;
  const bars=rows.map((r,i)=>{const v=val(r[yKey])||0; const x=p+i*(barW+gap); const bh=Math.abs(v)/max*(h/2-p); const y=v>=0?zero-bh:zero; return `<rect class="${v>=0?'bar-pos':'bar-neg'}" x="${x}" y="${y}" width="${barW}" height="${bh}"><title>${dateFmt(r[xKey])}: ${money2(v)}</title></rect>`}).join('');
  return `<div class="card"><svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><line class="axis" x1="${p}" y1="${zero}" x2="${w-p}" y2="${zero}"></line>${bars}<text x="${p}" y="18">${money(max)}</text><text x="${p}" y="${h-8}">${money(-max)}</text></svg></div>`;
}
function riskPill(r){ const band=r.moneyness_band; if(band==='in_the_money')return 'bad'; if(band==='at_strike'||band==='near_the_money')return 'warn'; return 'good'; }
function renderRiskCards(rows){
  if(!rows || rows.length===0) return '<div class="card muted">No open option shorts.</div>';
  return `<div class="risk-list">${rows.map(r=>`<div class="risk-card"><div class="risk-head"><div><div class="risk-title">${r.ticker} ${r.option_type||r.type} ${dec(r.strike,2)}</div><div class="muted">${dateFmt(r.expiration)} - ${r.days_to_expiration ?? 'n/a'} DTE</div></div><span class="pill ${riskPill(r)}">${r.risk_label || r.moneyness_band || 'Risk'}</span></div><div class="risk-meta"><span>Current ${money2(r.current_price)}</span><span>Moneyness ${pct(r.moneyness)}</span><span>Qty ${r.quantity ?? r.qty}</span><span>${r.covered_status || 'open'}</span><span>Premium ${money2(r.premium_collected ?? r.open_price*100*(r.quantity??r.qty??0))}</span><span>Opened ${dateFmt(r.opened || r.trans_date)}</span></div></div>`).join('')}</div>`;
}
function initHeader(){
  const d=data.dashboard; const freshness=d.data_freshness || {}; const price=freshness.price_coverage || {}; const issue=d.issue_summary || {};
  const priced = price.priced_count ?? price.stocks_fetched ?? price.fetched ?? 0;
  const required = price.required_count ?? price.stocks_requested ?? price.requested ?? 0;
  const missing = price.missing_count ?? (required - priced);
  $('subtitle').textContent = `${data.source.label} - as of ${dateFmt(d.request.as_of)} - generated ${dateFmt(data.generated_at)}`;
  $('statusStrip').innerHTML = [
    badge(`${priced}/${required} priced`, (missing||0)>0?'bad':'good'),
    badge(`${issue.total_count ?? 0} actionable issues`, (issue.total_count||0)>0?'bad':'good'),
    badge(`${data.source.row_count} source rows`),
    badge(`Updated ${freshness.prices_updated_at ? String(freshness.prices_updated_at).slice(11,19) : 'n/a'}`)
  ].join('');
}
function renderOverview(){
  const snap=data.dashboard.snapshot||{}, mt=data.dashboard.monthly_target||{}, open=data.open_shorts.items||data.positions.open_option_shorts||[];
  const rows=(data.tables.monthly_cycles||[]).slice(-8).map(r=>({month:r.month,total_realized_pnl:r.total_realized_pnl}));
  $('overview').innerHTML = `
  <div class="grid metrics">${card('YTD total P&L', money(snap.ytd_total_pnl),'Realized plus current unrealized snapshot',cls(snap.ytd_total_pnl))}${card('YTD realized P&L', money(snap.ytd_realized_pnl),'Closed options, stock P&L, dividends',cls(snap.ytd_realized_pnl))}${card('Current unrealized', money(snap.current_unrealized_pnl),`Options ${money(snap.current_option_unrealized_pnl)} - Stock ${money(snap.current_stock_unrealized_pnl)}`,cls(snap.current_unrealized_pnl))}${card('YTD annualized TWR', pct(snap.ytd_annualized_twr),'Unrealized adjusted when enabled',cls(snap.ytd_annualized_twr))}</div>
  <h2>Current Month</h2><div class="grid three">${card('Projected month P&L', money(mt.projected_month_pnl),`Realized ${money(mt.realized_month_pnl)} + incremental open premium ${money(mt.open_expiring_incremental_premium)}`,cls(mt.projected_month_pnl))}${card('Projected return', `${pct(mt.projected_return_roac)} RoAC`,`${mt.monthly_target_status || mt.status || 'status n/a'} - target ${pct(mt.target_return || data.monthly.target_return)}`,cls(mt.projected_return_roac - (mt.target_return || data.monthly.target_return || 0)))}${card('Roll-adjusted open premium', money(mt.open_expiring_roll_adjusted_premium),'Display/reconciliation only, not additive projection',cls(mt.open_expiring_roll_adjusted_premium))}</div>
  <h2>Open Shorts At Risk</h2>${renderRiskCards(open.slice(0,8))}
  <h2>Monthly P&L Trend</h2>${chartBars(rows,'month','total_realized_pnl')}
  <h2>Benchmark Growth</h2>${chartLine(data.charts.benchmark_growth,'Date','Growth','Series')}`;
}
function renderMonthly(){
  const m=data.monthly; const future=m.future_months||[]; const rows=m.months||[];
  $('monthly').innerHTML = `<h2>Monthly Performance</h2><div class="grid two"><div>${table(rows,[{key:'month',label:'Month',format:dateFmt},{key:'realized_options_pnl',label:'Options',format:money,num:true,className:cls},{key:'realized_stock_pnl',label:'Stock',format:money,num:true,className:cls},{key:'dividends',label:'Dividends',format:money,num:true},{key:'total_realized_pnl',label:'Realized',format:money,num:true,className:cls},{key:'open_expiring_incremental_premium',label:'Open incremental',format:money,num:true,className:cls},{key:'open_expiring_roll_adjusted_premium',label:'Open roll-adjusted',format:money,num:true,className:cls},{key:'projected_month_pnl',label:'Projected',format:money,num:true,className:cls},{key:'projected_return_roac',label:'Proj. RoAC',format:pct,num:true}])}</div><div><h3>Future Expiry Months</h3>${table(future,[{key:'month',label:'Month',format:dateFmt},{key:'open_option_count',label:'Open',num:true},{key:'open_expiring_incremental_premium',label:'Incremental',format:money,num:true,className:cls},{key:'open_expiring_roll_adjusted_premium',label:'Roll-adjusted',format:money,num:true,className:cls},{key:'projection_basis',label:'Basis'}])}</div></div><h2>Return Curve</h2>${chartLine(data.charts.monthly_returns.map(r=>({month:r.month,Series:'Strategy',Growth:(1+(r.return||0))})).reduce((acc,r,i)=>{const prev=i?acc[i-1].Growth:1; acc.push({...r,Growth:prev*(1+(r.return||0))}); return acc;},[]),'month','Growth','Series')}`;
}
function renderTickers(){
  const rows=data.tickers.items || data.tables.per_ticker_totals || [];
  $('tickers').innerHTML = `<h2>Ticker P&L</h2><div class="toolbar"><input id="tickerSearch" placeholder="Filter ticker..."></div><div id="tickerTable"></div><h2>Yearly Ticker P&L</h2>${table(data.tables.per_ticker_yearly,[{key:'year',label:'Year',num:true},{key:'ticker',label:'Ticker'},{key:'options_pnl',label:'Options',format:money,num:true,className:cls},{key:'stock_realized_pnl',label:'Stock',format:money,num:true,className:cls},{key:'combined_realized',label:'Realized',format:money,num:true,className:cls}])}`;
  const render=()=>{ const q=($('tickerSearch').value||'').toUpperCase(); const filtered=rows.filter(r=>!q || String(r.ticker).includes(q)); $('tickerTable').innerHTML=table(filtered,[{key:'ticker',label:'Ticker'},{key:'realized_options_pnl',label:'Options',format:money,num:true,className:cls},{key:'realized_stock_pnl',label:'Stock',format:money,num:true,className:cls},{key:'dividends',label:'Dividends',format:money,num:true},{key:'combined_realized_pnl',label:'Realized',format:money,num:true,className:cls},{key:'unrealized_pnl',label:'Unrealized',format:money,num:true,className:cls},{key:'total_pnl',label:'Total',format:money,num:true,className:cls},{key:'open_option_count',label:'Open options',num:true},{key:'inventory_share_count',label:'Shares',num:true}]); };
  $('tickerSearch').oninput=render; render();
}
function renderPositions(){
  $('positions').innerHTML = `<h2>Assigned Holdings</h2>${table(data.positions.inventory,[{key:'ticker',label:'Ticker'},{key:'buy_date',label:'Buy date',format:dateFmt},{key:'shares',label:'Shares',num:true},{key:'cost_per_share',label:'Cost/share',format:money2,num:true},{key:'current_price',label:'Current',format:money2,num:true},{key:'covered_shares',label:'Covered',num:true},{key:'covered_strike',label:'Cover strike',format:money2,num:true},{key:'unrealized_pnl',label:'Unrealized',format:money,num:true,className:cls},{key:'source',label:'Source'}])}<h2>Open Option Shorts</h2>${renderRiskCards(data.positions.open_option_shorts)}<div style="height:12px"></div>${table(data.positions.open_option_shorts,[{key:'ticker',label:'Ticker'},{key:'option_type',label:'Type'},{key:'strike',label:'Strike',format:money2,num:true},{key:'expiration',label:'Expiry',format:dateFmt},{key:'days_to_expiration',label:'DTE',num:true},{key:'quantity',label:'Qty',num:true},{key:'current_price',label:'Current',format:money2,num:true},{key:'moneyness',label:'Moneyness',format:pct,num:true},{key:'premium_collected',label:'Premium',format:money2,num:true},{key:'covered_status',label:'Coverage'}])}`;
}
function renderPerformance(){
  $('performance').innerHTML = `<h2>Yearly Performance</h2>${table(data.yearly.years,[{key:'year',label:'Year',num:true},{key:'realized_options_pnl',label:'Options',format:money,num:true,className:cls},{key:'realized_stock_pnl',label:'Stock',format:money,num:true,className:cls},{key:'dividends',label:'Dividends',format:money,num:true},{key:'total_realized_pnl',label:'Realized',format:money,num:true,className:cls},{key:'total_pnl_including_unrealized',label:'With unrealized',format:money,num:true,className:cls},{key:'avg_capital',label:'Avg capital',format:money,num:true},{key:'peak_capital',label:'Peak capital',format:money,num:true},{key:'roac_year',label:'RoAC',format:pct,num:true},{key:'ropc_year',label:'RoPC',format:pct,num:true},{key:'annualized_twr',label:'Ann. TWR',format:pct,num:true}])}<h2>Benchmark Metrics</h2>${table(data.tables.benchmark_metrics,[{key:'Series',label:'Series'},{key:'CAGR',label:'CAGR',format:pct,num:true},{key:'Volatility',label:'Volatility',format:pct,num:true},{key:'Sharpe',label:'Sharpe',format:(v)=>dec(v,2),num:true},{key:'Sortino',label:'Sortino',format:(v)=>dec(v,2),num:true},{key:'Max Drawdown',label:'Max DD',format:pct,num:true},{key:'Return 3M',label:'3M',format:pct,num:true},{key:'Return 6M',label:'6M',format:pct,num:true},{key:'Return YTD',label:'YTD',format:pct,num:true},{key:'Return SI',label:'Since inception',format:pct,num:true}])}<h2>Benchmark Growth</h2>${chartLine(data.charts.benchmark_growth,'Date','Growth','Series')}`;
}
function renderDiagnostics(){
  const iss=data.issues, sum=iss.summary||{}, aud=iss.audit_summary||{}, fresh=data.dashboard.data_freshness||{};
  const currentCoverage = fresh.price_coverage || {};
  const currentPriced = currentCoverage.priced_count ?? currentCoverage.stocks_fetched ?? currentCoverage.fetched ?? 0;
  const currentRequired = currentCoverage.required_count ?? currentCoverage.stocks_requested ?? currentCoverage.requested ?? 0;
  $('diagnostics').innerHTML = `<h2>Data Health</h2><div class="grid metrics">${card('Actionable issues', num(sum.total_count||0),'Warnings/errors requiring attention',(sum.total_count||0)>0?'neg':'pos')}${card('Audit notes', num(aud.total_count||0),'Expected IBKR classification notes','')}${card('Source rows', num(data.source.row_count),'IBKR Flex imported rows','')}${card('Prices updated', fresh.prices_updated_at || 'n/a','Current price snapshot','')}</div><h2>Actionable Issues</h2>${table(iss.issues||[],[{key:'severity',label:'Severity'},{key:'category',label:'Category'},{key:'message',label:'Message'}])}<h2>Coverage</h2><div class="grid three">${card('Current prices', `${currentPriced}/${currentRequired}`,'Missing prices block unrealized metrics if required')}${card('Historical prices', `${iss.coverage?.historical_prices?.fetched ?? 0}/${iss.coverage?.historical_prices?.requested ?? 0}`,'Capital and benchmark coverage')}${card('Dividends', `${iss.coverage?.dividends?.failed_tickers?.length ?? 0} failed`,'Dividend cashflow coverage')}</div><h2>Reconciliation Notes</h2><div class="note-list">${data.reconciliation_notes.map(n=>`<div class="note"><strong>${n.case} - ${n.status}</strong><span class="muted">${n.detail}</span></div>`).join('')}</div><h2>Audit Notes</h2><details><summary>Show ${aud.total_count||0} audit notes</summary>${table((iss.audit_notes||[]).slice(0,500),[{key:'category',label:'Category'},{key:'severity',label:'Severity'},{key:'message',label:'Message'}])}</details>`;
}
function renderMethodology(){
  $('methodology').innerHTML = `<h2>Methodology</h2><div class="grid two"><div class="card"><h3>Source</h3><p>Production web and iOS read from imported IBKR Flex data in Firestore. Streamlit Cloud remains the Google Sheets backup/control dashboard.</p><h3>Wheel scope</h3><p>Wheel P&L starts with assigned puts. Covered calls are included only when backed by assignment-derived shares or valid covered-call roll replacements. Expected non-wheel exclusions are audit notes, not actionable issues.</p><h3>Monthly projections</h3><p>Realized P&L stays separate from projected values. Open expiring incremental premium is additive for projected month P&L. Roll-adjusted open premium is a display/reconciliation field and is not added again.</p></div><div class="card"><h3>Unrealized snapshot</h3><p>Current unrealized values are a monitoring snapshot, not full option mark-to-market accounting. Missing required current prices suppress unrealized totals.</p><h3>Benchmarks</h3><p>Return metrics compare monthly strategy returns against aligned benchmark monthly series where coverage is complete.</p><h3>Refresh</h3><p>The refresh button rebuilds the cached backend context and current price overlay. It does not mutate accounting rules.</p></div></div>`;
}
initHeader(); renderOverview(); renderMonthly(); renderTickers(); renderPositions(); renderPerformance(); renderDiagnostics(); renderMethodology();
</script>
</body>
</html>""".replace(
    "__BASE_CSS__", BASE_CSS
)
