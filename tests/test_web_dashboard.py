from __future__ import annotations

from fastapi.testclient import TestClient

import web_dashboard


def _fake_dashboard_data():
    return {
        "generated_at": "2026-05-10T12:00:00+00:00",
        "source": {"label": "IBKR Flex", "kind": "ibkr_flex", "row_count": 1, "sheet_counts": []},
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
    monkeypatch.setenv("MOBILE_API_KEY", "secret")
    client = TestClient(web_dashboard.app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_web_dashboard_redirects_to_login_without_session(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("MOBILE_API_KEY", "secret")
    client = TestClient(web_dashboard.app, follow_redirects=False)

    response = client.get("/")

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_web_dashboard_renders_when_auth_disabled(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", lambda: _fake_dashboard_data())
    client = TestClient(web_dashboard.app)

    response = client.get("/")

    assert response.status_code == 200
    assert "Portfolio Dashboard" in response.text
    assert "IBKR Flex" in response.text
    assert "dashboard-data" in response.text


def test_web_dashboard_api_requires_auth(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("MOBILE_API_KEY", "secret")
    client = TestClient(web_dashboard.app)

    response = client.get("/api/dashboard")

    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}


def test_web_dashboard_login_accepts_mobile_api_key(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("MOBILE_API_KEY", "secret")
    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", lambda: _fake_dashboard_data())
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/login", data={"password": "secret"}, follow_redirects=False)

    assert response.status_code == 303
    assert COOKIE_HEADER(response).startswith(f"{web_dashboard.COOKIE_NAME}=")

    dashboard_response = client.get("/")
    assert dashboard_response.status_code == 200
    assert "Portfolio Dashboard" in dashboard_response.text


def test_web_dashboard_login_rejects_wrong_key(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("MOBILE_API_KEY", "secret")
    client = TestClient(web_dashboard.app, base_url="https://testserver")

    response = client.post("/login", data={"password": "wrong"})

    assert response.status_code == 401
    assert "Invalid password or API key." in response.text


def COOKIE_HEADER(response) -> str:
    return response.headers.get("set-cookie", "")
