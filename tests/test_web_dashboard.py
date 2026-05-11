from __future__ import annotations

from fastapi.testclient import TestClient

import web_dashboard


def _fake_dashboard_data():
    return {
        "app": {"revision": "options-roi-web-test", "restart_ts": ""},
        "generated_at": "2026-05-10T12:00:00+00:00",
        "source": {"label": "IBKR Flex", "kind": "ibkr_flex", "row_count": 1, "sheet_counts": []},
        "web": {"include_unrealized": True, "target_return": 0.015},
        "dashboard": {
            "request": {"as_of": "2026-05-10", "include_unrealized": True, "selected_sheets": ["IBKR Flex"]},
            "data_freshness": {
                "prices_updated_at": "2026-05-10T12:00:00+00:00",
                "price_coverage": {"priced_count": 1, "required_count": 1, "missing_count": 0},
            },
            "snapshot": {
                "currency": "USD",
                "year": 2026,
                "ytd_total_pnl": 1000.0,
                "ytd_realized_pnl": 800.0,
                "current_unrealized_pnl": 200.0,
                "current_option_unrealized_pnl": 50.0,
                "current_stock_unrealized_pnl": 150.0,
                "ytd_annualized_twr": 0.12,
            },
            "monthly_target": {
                "month": "2026-05-31",
                "target_return": 0.015,
                "realized_month_pnl": 100.0,
                "open_expiring_incremental_premium": 25.0,
                "open_expiring_roll_adjusted_premium": 30.0,
                "projected_month_pnl": 125.0,
                "projected_return_roac": 0.02,
                "monthly_target_status": "beat",
            },
            "open_option_short_preview": [],
            "issue_summary": {"severity": "ok", "total_count": 0},
        },
        "positions": {"inventory": [], "open_option_shorts": []},
        "open_shorts": {"items": []},
        "tickers": {"items": []},
        "monthly": {
            "target_return": 0.015,
            "current_month": {},
            "months": [],
            "future_months": [],
        },
        "yearly": {"years": []},
        "issues": {
            "summary": {"severity": "ok", "total_count": 0},
            "issues": [],
            "audit_summary": {"total_count": 0},
            "audit_notes": [],
            "coverage": {},
        },
        "tables": {
            "monthly_cycles": [],
            "yearly_realized": [],
            "yearly_with_unrealized": [],
            "per_ticker_yearly": [],
            "per_ticker_totals": [],
            "benchmark_metrics": [],
            "inventory": [],
            "open_options": [],
        },
        "charts": {
            "benchmark_growth": [],
            "monthly_returns": [],
            "monthly_returns_unrealized_adjusted": [],
        },
        "reconciliation_notes": [],
    }


def test_web_dashboard_health_is_public(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_web_dashboard_redirects_to_login_without_session(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app, follow_redirects=False)

    response = client.get("/")

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_web_dashboard_renders_when_auth_disabled(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", lambda **_: _fake_dashboard_data())
    client = TestClient(web_dashboard.app)

    response = client.get("/")

    assert response.status_code == 200
    assert "Dashboard" in response.text
    assert "IBKR Flex" in response.text
    assert "dashboard-data" in response.text
    assert "Period" in response.text
    assert "With unrealized" in response.text
    assert "Reload app" in response.text
    assert "Open Shorts Monitor" in response.text
    assert "Settings" in response.text
    assert "Target return %" in response.text
    assert "Monthly Return vs Target" in response.text
    assert "Monthly P&amp;L vs Target" not in response.text
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["expires"] == "0"


def test_web_dashboard_passes_target_return_from_query(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        return _fake_dashboard_data()

    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", fake_build)
    client = TestClient(web_dashboard.app)

    response = client.get("/?target_return_pct=2.25")

    assert response.status_code == 200
    assert captured["target_return"] == 0.0225
    assert f"{web_dashboard.TARGET_RETURN_COOKIE_NAME}=0.022500" in response.headers["set-cookie"]


def test_web_dashboard_reads_target_return_from_cookie(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        return _fake_dashboard_data()

    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", fake_build)
    client = TestClient(web_dashboard.app)
    client.cookies.set(web_dashboard.TARGET_RETURN_COOKIE_NAME, "0.03")

    response = client.get("/")

    assert response.status_code == 200
    assert captured["target_return"] == 0.03


def test_web_dashboard_api_requires_auth(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app)

    response = client.get("/api/dashboard")

    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"


def test_web_dashboard_login_accepts_dashboard_password(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", lambda **_: _fake_dashboard_data())
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/login", data={"password": "secret"}, follow_redirects=False)

    assert response.status_code == 303
    assert COOKIE_HEADER(response).startswith(f"{web_dashboard.COOKIE_NAME}=")

    dashboard_response = client.get("/")
    assert dashboard_response.status_code == 200
    assert "Dashboard" in dashboard_response.text


def test_web_dashboard_login_sets_long_lived_session(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/login", data={"password": "secret"}, follow_redirects=False)

    cookie = COOKIE_HEADER(response)
    assert response.status_code == 303
    assert "Max-Age=7776000" in cookie


def test_web_dashboard_login_rejects_wrong_password(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/login", data={"password": "wrong"})

    assert response.status_code == 401
    assert "Invalid dashboard password." in response.text


def test_web_dashboard_login_page_renders_google_sign_in_when_configured(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.delenv("WEB_DASHBOARD_PASSWORD", raising=False)
    monkeypatch.setenv("WEB_DASHBOARD_COOKIE_SECRET", "cookie-secret")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "user@example.com")
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.get("/login")

    assert response.status_code == 200
    assert 'href="/auth/google/start"' in response.text
    assert "Sign in with Google" in response.text
    assert "Use dashboard password instead" not in response.text
    assert "Dashboard password" not in response.text


def test_web_dashboard_login_page_can_show_password_fallback(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "user@example.com")
    monkeypatch.setenv("WEB_PASSWORD_FALLBACK_VISIBLE", "1")
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.get("/login")

    assert response.status_code == 200
    assert "Use dashboard password instead" in response.text
    assert "Dashboard password" in response.text


def test_web_dashboard_google_start_uses_public_https_redirect(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "user@example.com")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app, base_url="http://options-roi-web.example.run.app")

    response = client.get("/auth/google/start", follow_redirects=False)

    assert response.status_code == 303
    assert "redirect_uri=https%3A%2F%2Foptions-roi-web.example.run.app%2Fauth%2Fgoogle" in response.headers[
        "location"
    ]


def test_web_dashboard_login_page_requires_google_allowlist(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.delenv("WEB_DASHBOARD_PASSWORD", raising=False)
    monkeypatch.setenv("WEB_DASHBOARD_COOKIE_SECRET", "cookie-secret")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.delenv("WEB_AUTH_ALLOWED_EMAILS", raising=False)
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.get("/login")

    assert response.status_code == 500
    assert "Dashboard is not configured" in response.text


def test_web_dashboard_google_login_accepts_allowed_verified_email(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "user@example.com")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setattr(
        web_dashboard,
        "_verify_google_credential",
        lambda credential: {"email": "user@example.com"} if credential == "good" else {},
    )
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/auth/google", data={"credential": "good"}, follow_redirects=False)

    assert response.status_code == 303
    assert COOKIE_HEADER(response).startswith(f"{web_dashboard.COOKIE_NAME}=")


def test_web_dashboard_google_login_rejects_disallowed_email(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_GOOGLE_CLIENT_ID", "client-id.apps.googleusercontent.com")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_EMAILS", "user@example.com")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")

    def reject(_credential):
        raise PermissionError("This Google account is not allowed for this dashboard.")

    monkeypatch.setattr(web_dashboard, "_verify_google_credential", reject)
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/auth/google", data={"credential": "good"})

    assert response.status_code == 403
    assert "This Google account is not allowed for this dashboard." in response.text


def COOKIE_HEADER(response) -> str:
    return response.headers.get("set-cookie", "")
