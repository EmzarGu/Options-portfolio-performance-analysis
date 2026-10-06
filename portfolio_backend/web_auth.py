"""Browser authentication, signed sessions and login rendering.

Sessions and OAuth state use separate, expiring signed formats. Legacy cookies are rejected.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from itsdangerous import BadData, URLSafeTimedSerializer

from fastapi import Request
from fastapi.responses import RedirectResponse, Response
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token

from portfolio_backend.web_dashboard_templates import BASE_CSS, LOGIN_TEMPLATE


COOKIE_NAME = "options_roi_web_session"
OAUTH_STATE_COOKIE_NAME = "options_roi_google_state"
DEFAULT_SESSION_DAYS = 90
MIN_COOKIE_SECRET_LENGTH = 32


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
    return len(os.getenv("WEB_DASHBOARD_COOKIE_SECRET", "").strip()) >= MIN_COOKIE_SECRET_LENGTH


def _cookie_secret() -> str:
    secret = os.getenv("WEB_DASHBOARD_COOKIE_SECRET", "").strip()
    if not _cookie_secret_configured():
        raise RuntimeError("WEB_DASHBOARD_COOKIE_SECRET must contain at least 32 characters.")
    return secret


def _google_auth_configured() -> bool:
    return bool(_google_client_id() and _allowed_google_emails() and _cookie_secret_configured())


def _auth_configured() -> bool:
    return bool(_cookie_secret_configured() and (_password_login_enabled() or _google_auth_configured()))


def _password_login_enabled() -> bool:
    """Password access is local only; Cloud Run always requires Google sign-in."""
    return bool(not _cloud_runtime() and _dashboard_password())


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


def _serializer(purpose: str) -> URLSafeTimedSerializer:
    """Separate browser sessions from short-lived OAuth state."""
    return URLSafeTimedSerializer(_cookie_secret(), salt=f"options-roi-{purpose}-v2")


def _session_token(*, email: Optional[str] = None, auth_method: str = "key") -> str:
    return _serializer("session").dumps({"email": email, "auth": auth_method})


def _session_info(token: str) -> Optional[Dict[str, Any]]:
    if not token or not _auth_configured():
        return None
    try:
        info = _serializer("session").loads(token, max_age=_session_max_age_seconds())
    except BadData:
        return None
    if not isinstance(info, dict) or info.get("auth") not in {"key", "google"}:
        return None
    if info["auth"] == "google" and info.get("email") not in _allowed_google_emails():
        return None
    if info["auth"] == "key" and not _password_login_enabled():
        return None
    return info


def _valid_session(token: str) -> bool:
    return _session_info(token) is not None


def _is_authenticated(request: Request) -> bool:
    if not _auth_enabled():
        return not _cloud_runtime()
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
    return _serializer("oauth-state").dumps({"state": state, "nonce": nonce})


def _oauth_state_info(token: str) -> Optional[Dict[str, Any]]:
    if not token or not _auth_configured():
        return None
    try:
        info = _serializer("oauth-state").loads(token, max_age=600)
    except BadData:
        return None
    if not isinstance(info, dict):
        return None
    if not isinstance(info.get("state"), str) or not isinstance(info.get("nonce"), str):
        return None
    return info


def _cloud_runtime() -> bool:
    return bool(os.getenv("K_SERVICE") or os.getenv("CLOUD_RUN_JOB"))


def validate_configuration() -> None:
    """Refuse cloud startup with disabled or incomplete authentication."""
    if _cloud_runtime() and (not _auth_enabled() or not _auth_configured()):
        raise RuntimeError("Production requires Google sign-in, an email allowlist and a cookie secret of at least 32 characters.")


def _google_redirect_uri(request: Request) -> str:
    public_base_url = os.getenv("WEB_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base_url:
        return f"{public_base_url}/auth/google"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"https://{host}/auth/google"


def _configuration_error_html() -> str:
    return """<!doctype html>
<html><head><title>Options ROI</title><style>{css}</style></head>
<body><main class="login"><h1>Dashboard is not configured</h1>
<p>Configure Google sign-in, an allowed email address and a cookie secret of at least 32 characters. Password login is available only in local development.</p></main></body></html>""".format(
        css=BASE_CSS
    )


def _login_html(error: str = "") -> str:
    safe_error = error.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    google_signin = _google_signin_html()
    show_fallback = bool(_password_login_enabled() and (not google_signin or _password_fallback_visible()))
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
