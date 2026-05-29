from __future__ import annotations

import json

from fastapi.testclient import TestClient

import web_dashboard
from portfolio_backend.web_dashboard_templates import DASHBOARD_HTML


def _embedded_dashboard_data(html: str):
    marker = '<script id="dashboard-data" type="application/json">'
    start = html.index(marker) + len(marker)
    end = html.index("</script>", start)
    return json.loads(html[start:end])


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


def _fake_decision_lab_data():
    return {
        "summary": {"action_item_count": 0},
        "ticker_situations": [],
        "active_cycle": {
            "cycle_label": "June 2026",
            "expiry_dates": ["2026-06-18"],
            "min_dte": 20,
            "max_dte": 20,
            "open_contract_count": 2,
            "realized_cycle_pnl": 0.0,
            "premium_component": 100.0,
            "itm_put_unrealized_loss": 0.0,
            "projected_pnl": 100.0,
            "target_pnl": 200.0,
            "remaining_to_target": 100.0,
            "projected_return_roac": 0.01,
            "target_return": 0.02,
            "target_floor": 0.01,
            "cycle_put_exposure": 10000.0,
            "cycle_itm_put_exposure": 0.0,
            "portfolio_put_exposure": 10000.0,
            "portfolio_itm_put_exposure": 0.0,
            "covered_call_upside_foregone": 0.0,
        },
        "option_market_data": {
            "status": {
                "provider": "cutemarkets",
                "source": "stored",
                "last_fetched_at": "2026-05-29T12:00:00+00:00",
                "contract_count": 10,
            }
        },
        "recommendation_candidates": [],
        "strike_quality": {},
        "coverage_notes": [],
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
    monkeypatch.setattr(
        web_dashboard,
        "_build_dashboard_data",
        lambda **_: (_ for _ in ()).throw(AssertionError("dashboard page should render a shell only")),
    )
    client = TestClient(web_dashboard.app)

    response = client.get("/")

    assert response.status_code == 200
    shell_data = _embedded_dashboard_data(response.text)
    assert shell_data["loading"] is True
    assert shell_data["source"]["kind"] == "ibkr_flex"
    assert "Dashboard" in response.text
    assert "Decision Lab" in response.text
    assert "IBKR Flex" in response.text
    assert "dashboard-data" in response.text
    assert "Period" in response.text
    assert "With unrealized" in response.text
    assert "Reload app" in response.text
    assert "Open Option Shorts" in response.text
    assert "Positions" not in response.text
    assert "Settings" in response.text
    assert "Lower band %" in response.text
    assert "Upper band / target %" in response.text
    assert "Monthly Return vs Target" in response.text
    assert "Monthly P&amp;L vs Target" not in response.text
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["expires"] == "0"


def test_web_dashboard_keeps_backend_rebased_benchmark_growth_series():
    assert "trimFiniteGrowthRows" not in DASHBOARD_HTML


def test_web_dashboard_does_not_add_extra_benchmark_baseline_label():
    assert "Start (${fullLabels[0]})" not in DASHBOARD_HTML


def test_web_dashboard_passes_target_return_from_query(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    client = TestClient(web_dashboard.app)

    response = client.get("/?target_return_pct=2.25")

    assert response.status_code == 200
    assert _embedded_dashboard_data(response.text)["web"]["target_return"] == 0.0225
    assert f"{web_dashboard.TARGET_RETURN_COOKIE_NAME}=0.022500" in response.headers["set-cookie"]


def test_web_dashboard_reads_target_return_from_cookie(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    client = TestClient(web_dashboard.app)
    client.cookies.set(web_dashboard.TARGET_RETURN_COOKIE_NAME, "0.03")

    response = client.get("/")

    assert response.status_code == 200
    assert _embedded_dashboard_data(response.text)["web"]["target_return"] == 0.03


def test_web_dashboard_api_builds_payload_and_reuses_short_cache(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setenv("WEB_DASHBOARD_DATA_CACHE_SECONDS", "300")
    web_dashboard._clear_dashboard_data_cache()
    calls = []

    def fake_build(**kwargs):
        calls.append(kwargs)
        payload = _fake_dashboard_data()
        payload["web"]["target_return"] = kwargs["target_return"]
        return payload

    monkeypatch.setattr(web_dashboard, "_build_dashboard_data", fake_build)
    monkeypatch.setattr(web_dashboard, "_build_decision_lab_payload", lambda payload, force_refresh=False: _fake_decision_lab_data())
    client = TestClient(web_dashboard.app)

    first = client.get("/api/dashboard?target_return_pct=2.25")
    second = client.get("/api/dashboard?target_return_pct=2.25")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["web"]["target_return"] == 0.0225
    assert first.json()["decision_lab"]["active_cycle"]["cycle_label"] == "June 2026"
    assert calls == [{"as_of": None, "include_unrealized": True, "target_return": 0.0225}]
    web_dashboard._clear_dashboard_data_cache()


def test_web_dashboard_refresh_accepts_decision_lab_section(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setattr(web_dashboard, "_get_context", lambda **_: (object(), 123))
    monkeypatch.setattr(web_dashboard, "build_mobile_refresh_payload", lambda *_, **__: {})
    client = TestClient(web_dashboard.app, follow_redirects=False)

    response = client.post("/refresh?section=decision_lab&target_return=0.02")

    assert response.status_code == 303
    assert "section=decision_lab" in response.headers["location"]


def test_web_dashboard_api_requires_auth(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "1")
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    client = TestClient(web_dashboard.app)

    response = client.get("/api/dashboard")

    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"


def test_decision_lab_renders_shell_when_auth_disabled(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    client = TestClient(web_dashboard.app)

    response = client.get("/decision-lab")

    assert response.status_code == 200
    assert "Decision Dashboard Lab" in response.text
    assert "/api/decision-lab" in response.text
    assert "Current dashboard" in response.text
    assert "Only ticker-level situations" not in response.text
    assert "not a trading recommendation" not in response.text
    assert "Probability coverage" not in response.text
    assert "Fetch option data" in response.text


def test_decision_lab_api_builds_real_data_model(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setattr(web_dashboard, "_get_cached_dashboard_data", lambda **_: _fake_dashboard_data())
    monkeypatch.setattr(
        web_dashboard,
        "_load_probability_trade_matches",
        lambda: [
            {
                "matched": True,
                "profit_probability": 0.78,
                "assignment_risk_proxy": 0.22,
                "trade": {
                    "ticker": "AAA",
                    "put_call": "Put",
                    "trade_date": "2024-03-15",
                    "strike": 95,
                    "qty": 1,
                    "net_cash": 100,
                    "profit_probability": 0.78,
                    "assignment_risk_proxy": 0.22,
                },
            }
        ],
    )
    monkeypatch.setattr(web_dashboard, "_load_historical_option_enrichments", lambda: [])
    monkeypatch.setattr(
        web_dashboard,
        "_decision_option_loader",
        lambda force_refresh=False: (lambda _situations, _cycle, _candidates, _payload: {"status": {"source": "none"}, "contracts": []}),
    )
    client = TestClient(web_dashboard.app)

    response = client.get("/api/decision-lab")

    assert response.status_code == 200
    data = response.json()
    assert data["summary"]["probability_match_count"] == 1
    assert data["strike_quality"]["put_entry_quality"]["bucket_summary"]
    assert "ticker_scorecard" not in data
    assert "open_positions" not in data
    assert "performance_insights" not in data
    assert data["active_cycle"]["portfolio_put_exposure"] == 0


def test_decision_lab_option_refresh_uses_force_loader(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    monkeypatch.setattr(web_dashboard, "_get_cached_dashboard_data", lambda **_: _fake_dashboard_data())
    monkeypatch.setattr(web_dashboard, "_load_probability_trade_matches", lambda: [])
    monkeypatch.setattr(web_dashboard, "_load_historical_option_enrichments", lambda: [])
    calls = []

    def fake_loader(force_refresh=False):
        calls.append(force_refresh)
        return lambda _situations, _cycle, _candidates, _payload: {
            "status": {
                "provider": "cutemarkets",
                "source": "provider_refresh",
                "last_fetched_at": "2026-05-25T12:00:00+00:00",
                "contract_count": 0,
            },
            "contracts": [],
        }

    monkeypatch.setattr(web_dashboard, "_decision_option_loader", fake_loader)
    client = TestClient(web_dashboard.app)

    response = client.post("/api/decision-lab/options/refresh")

    assert response.status_code == 200
    assert calls == [True]
    assert response.json()["option_market_data"]["status"]["source"] == "provider_refresh"


def test_web_dashboard_import_route_starts_job(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_AUTH", "0")
    calls = []

    class Started:
        status = "started"

    def fake_trigger():
        calls.append(True)
        return Started()

    monkeypatch.setattr(web_dashboard, "trigger_ibkr_import_job", fake_trigger)
    client = TestClient(web_dashboard.app, follow_redirects=False)

    response = client.post("/import?section=diagnostics")

    assert response.status_code == 303
    assert "section=diagnostics" in response.headers["location"]
    assert "import_start=started" in response.headers["location"]
    assert calls == [True]


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
