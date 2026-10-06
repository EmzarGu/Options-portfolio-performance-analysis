"""Misconfiguration and session regressions must block all portfolio operations."""
import base64
import hashlib
import hmac
import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import mobile_api
import web_dashboard
from portfolio_backend import web_auth


@pytest.fixture(autouse=True)
def isolated_auth(monkeypatch):
    # Authentication tests exercise real guards with inert data dependencies.
    monkeypatch.setattr(mobile_api.context_service, "_available_sheets", lambda: ["IBKR Flex"])
    monkeypatch.setattr(mobile_api.context_service.dashboard_app, "load_prefs", lambda: {})
    monkeypatch.setattr(mobile_api, "load_monthly_target_band", lambda: {
        "target_return": 0.015, "target_floor": 0.01, "source": "test",
    })
    for name in ("MOBILE_API_KEY", "ALLOW_INSECURE_LOCAL_AUTH", "K_SERVICE", "CLOUD_RUN_JOB",
                 "WEB_DASHBOARD_PASSWORD", "WEB_DASHBOARD_COOKIE_SECRET", "WEB_GOOGLE_CLIENT_ID",
                 "WEB_AUTH_ALLOWED_EMAILS", "WEB_DASHBOARD_AUTH"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("path,method", [
    ("/v1/mobile/config", "get"), ("/v1/mobile/dashboard", "get"),
    ("/v1/mobile/refresh", "post"), ("/v1/mobile/import", "post"),
    ("/docs", "get"), ("/openapi.json", "get"),
])
@pytest.mark.parametrize("key", ["", "  "])
def test_missing_mobile_key_blocks_before_data_loading(monkeypatch, path, method, key):
    monkeypatch.setenv("MOBILE_API_KEY", key)
    monkeypatch.setattr(mobile_api.context_service, "get_context", lambda **_: pytest.fail("Loaded protected data"))
    response = getattr(TestClient(mobile_api.app), method)(path)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "auth_not_configured"


def test_cloud_cannot_use_local_opt_out(monkeypatch):
    monkeypatch.setenv("ALLOW_INSECURE_LOCAL_AUTH", "1")
    monkeypatch.setenv("K_SERVICE", "production")
    assert TestClient(mobile_api.app).get("/v1/mobile/config").status_code == 503
    with pytest.raises(RuntimeError, match="MOBILE_API_KEY"):
        with TestClient(mobile_api.app):
            pass


def test_explicit_local_mobile_mode_and_public_health(monkeypatch):
    assert TestClient(mobile_api.app).get("/v1/mobile/health").status_code == 200
    monkeypatch.setenv("ALLOW_INSECURE_LOCAL_AUTH", "1")
    with TestClient(mobile_api.app) as client:
        assert client.get("/v1/mobile/config").status_code == 200


def test_mobile_wrong_key_still_rejected(monkeypatch):
    monkeypatch.setenv("MOBILE_API_KEY", "test-api-key")
    with TestClient(mobile_api.app) as client:
        assert client.get("/v1/mobile/config", headers={"x-api-key": "wrong"}).status_code == 401
        assert client.get("/v1/mobile/config", headers={"authorization": "Bearer test-api-key"}).status_code == 200


@pytest.mark.parametrize("path,method", [
    ("/", "get"), ("/api/dashboard", "get"), ("/refresh", "post"),
    ("/import", "post"), ("/login", "post"), ("/auth/google", "post"),
])
def test_known_fallback_cookie_never_bypasses_missing_config(path, method):
    payload = base64.urlsafe_b64encode(json.dumps({"iat": 1791300000, "auth": "key"}).encode()).decode().rstrip("=")
    sig = base64.urlsafe_b64encode(hmac.new(b"local-dev-dashboard-secret", payload.encode(), hashlib.sha256).digest()).decode().rstrip("=")
    client = TestClient(web_dashboard.app)
    client.cookies.set(web_auth.COOKIE_NAME, f"{payload}.{sig}")
    assert getattr(client, method)(path).status_code == 503


def configure_web(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "test-password")
    monkeypatch.setenv("WEB_DASHBOARD_COOKIE_SECRET", "independent-test-session-secret")


def test_web_requires_dedicated_secret(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "test-password")
    assert not web_auth._auth_configured()
    monkeypatch.setenv("K_SERVICE", "production")
    with pytest.raises(RuntimeError):
        with TestClient(web_dashboard.app):
            pass


def test_cloud_rejects_web_auth_disabled(monkeypatch):
    configure_web(monkeypatch)
    monkeypatch.setenv("K_SERVICE", "production")
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    assert TestClient(web_dashboard.app).get("/api/dashboard").status_code == 503


def test_session_expiry_tampering_future_timestamp_and_secret_rotation(monkeypatch):
    configure_web(monkeypatch)
    with patch("itsdangerous.timed.time.time", return_value=1000000000):
        token = web_auth._session_token()
        assert web_auth._valid_session(token)
        assert not web_auth._valid_session(token + "tampered")
    with patch("itsdangerous.timed.time.time", return_value=1000000000 + web_auth._session_max_age_seconds() + 1):
        assert not web_auth._valid_session(token)
    with patch("itsdangerous.timed.time.time", return_value=999999999):
        assert not web_auth._valid_session(token)
    monkeypatch.setenv("WEB_DASHBOARD_COOKIE_SECRET", "rotated-independent-test-secret")
    assert not web_auth._valid_session(token)


def test_legacy_cookie_and_oauth_state_are_not_sessions(monkeypatch):
    configure_web(monkeypatch)
    assert not web_auth._valid_session("1.legacy-signature")
    oauth = web_auth._oauth_state_token(state="state", nonce="nonce")
    assert web_auth._oauth_state_info(oauth) == {"state": "state", "nonce": "nonce"}
    assert not web_auth._valid_session(oauth)
    assert web_auth._oauth_state_info(web_auth._session_token()) is None
    with patch("itsdangerous.timed.time.time", return_value=1000000000):
        old = web_auth._oauth_state_token(state="state", nonce="nonce")
    with patch("itsdangerous.timed.time.time", return_value=1000000601):
        assert web_auth._oauth_state_info(old) is None


def test_google_session_rechecks_allowlist(monkeypatch):
    configure_web(monkeypatch)
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "allowed@example.com")
    token = web_auth._session_token(email="allowed@example.com", auth_method="google")
    assert web_auth._valid_session(token)
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "different@example.com")
    assert not web_auth._valid_session(token)
