from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import mobile_api


def _request_payload(context):
    request = context.request
    return {
        "as_of": request.as_of.isoformat(),
        "include_unrealized": request.include_unrealized,
        "selected_sheets": list(request.selected_sheets),
    }


@pytest.fixture
def api_harness(monkeypatch):
    mobile_api._clear_context_cache()
    calls = SimpleNamespace(contexts=[], builders={})

    monkeypatch.setattr(mobile_api.dashboard_app, "SHEET_ID", "sheet-id")
    monkeypatch.setattr(mobile_api.dashboard_app, "SHEETS", ["Options 2024", "Options 2025", "Options 2026"])
    monkeypatch.setattr(
        mobile_api,
        "_available_sheets",
        lambda: ["Options 2024", "Options 2025", "Options 2026"],
    )
    monkeypatch.setattr(
        mobile_api.dashboard_app,
        "load_prefs",
        lambda: {"selected_sheets": ["Options 2025"], "include_unrealized": True},
    )
    monkeypatch.setattr(mobile_api, "_dependencies", lambda: SimpleNamespace(name="deps"))

    def build_context(request, dependencies, *, available_sheets=None):
        context = SimpleNamespace(
            request=request,
            dependencies=dependencies,
            available_sheets=list(available_sheets or []),
        )
        calls.contexts.append(context)
        return context

    def build_dashboard(context):
        calls.builders["dashboard"] = context
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "snapshot": {},
            "monthly_target": {},
            "open_option_short_preview": [],
            "issue_summary": {},
        }

    def build_positions(context):
        calls.builders["positions"] = context
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "inventory": [],
            "open_option_shorts": [],
        }

    def build_open_option_shorts(context, *, sort="moneyness_risk", limit=None):
        calls.builders["open_option_shorts"] = {"context": context, "sort": sort, "limit": limit}
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "moneyness_legend": [],
            "items": [],
        }

    def build_tickers(context, *, year=None, include_history=False):
        calls.builders["tickers"] = {"context": context, "year": year, "include_history": include_history}
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "items": [],
        }

    def build_monthly(context, *, target_return=0.015, monthly_range="ytd"):
        calls.builders["monthly"] = {
            "context": context,
            "target_return": target_return,
            "monthly_range": monthly_range,
        }
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "target_return": target_return,
            "target_basis": "average_capital",
            "return_metric": "roac",
            "current_month": {},
            "months": [],
        }

    def build_yearly(context):
        calls.builders["yearly"] = context
        return {"request": _request_payload(context), "data_freshness": {}, "years": []}

    def build_issues(context):
        calls.builders["issues"] = context
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "summary": {},
            "issues": [],
            "coverage": {},
        }

    def build_refresh(context, *, cache_bust=None):
        calls.builders["refresh"] = {"context": context, "cache_bust": cache_bust}
        return {
            "request": _request_payload(context),
            "data_freshness": {},
            "refresh": {
                "status": "refreshed",
                "pipeline_refreshed": True,
                "prices_refreshed": True,
                "cache_bust": cache_bust,
                "missing_price_count": 0,
                "missing_sheet_count": 0,
                "reload_endpoints": [],
            },
        }

    def build_config(available_sheets, prefs, *, default_sheets, as_of_default):
        calls.builders["config"] = {
            "available_sheets": available_sheets,
            "prefs": prefs,
            "default_sheets": default_sheets,
            "as_of_default": as_of_default,
        }
        return {
            "available_sheets": available_sheets,
            "default_selected_sheets": prefs["selected_sheets"],
            "defaults": {"include_unrealized": prefs["include_unrealized"]},
            "capabilities": {},
        }

    monkeypatch.setattr(mobile_api, "build_mobile_payload_context", build_context)
    monkeypatch.setattr(mobile_api, "build_mobile_dashboard_payload", build_dashboard)
    monkeypatch.setattr(mobile_api, "build_mobile_positions_payload", build_positions)
    monkeypatch.setattr(mobile_api, "build_mobile_open_option_shorts_payload", build_open_option_shorts)
    monkeypatch.setattr(mobile_api, "build_mobile_tickers_payload", build_tickers)
    monkeypatch.setattr(mobile_api, "build_mobile_monthly_payload", build_monthly)
    monkeypatch.setattr(mobile_api, "build_mobile_yearly_payload", build_yearly)
    monkeypatch.setattr(mobile_api, "build_mobile_issues_payload", build_issues)
    monkeypatch.setattr(mobile_api, "build_mobile_refresh_payload", build_refresh)
    monkeypatch.setattr(mobile_api, "build_mobile_config", build_config)
    monkeypatch.setattr(mobile_api, "_refresh_cache_bust", lambda: 99)

    return SimpleNamespace(client=TestClient(mobile_api.app), calls=calls)


def test_health_route_is_lightweight(api_harness):
    response = api_harness.client.get("/v1/mobile/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "options-roi-mobile-api",
        "version": "0.1.0",
    }
    assert api_harness.calls.contexts == []
    assert api_harness.calls.builders == {}


def test_api_key_is_not_required_when_unset(api_harness, monkeypatch):
    monkeypatch.delenv("MOBILE_API_KEY", raising=False)

    response = api_harness.client.get("/v1/mobile/config")

    assert response.status_code == 200


def test_api_key_protects_mobile_routes(api_harness, monkeypatch):
    monkeypatch.setenv("MOBILE_API_KEY", "secret")

    response = api_harness.client.get("/v1/mobile/config")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def test_api_key_accepts_header(api_harness, monkeypatch):
    monkeypatch.setenv("MOBILE_API_KEY", "secret")

    response = api_harness.client.get("/v1/mobile/config", headers={"X-API-Key": "secret"})

    assert response.status_code == 200


def test_api_key_accepts_bearer_token(api_harness, monkeypatch):
    monkeypatch.setenv("MOBILE_API_KEY", "secret")

    response = api_harness.client.get("/v1/mobile/config", headers={"Authorization": "Bearer secret"})

    assert response.status_code == 200


def test_health_route_stays_public_when_api_key_is_set(api_harness, monkeypatch):
    monkeypatch.setenv("MOBILE_API_KEY", "secret")

    response = api_harness.client.get("/v1/mobile/health")

    assert response.status_code == 200


def test_config_route_dispatches_available_sheets_and_defaults(api_harness):
    response = api_harness.client.get("/v1/mobile/config")

    assert response.status_code == 200
    assert set(response.json()) == {
        "available_sheets",
        "default_selected_sheets",
        "defaults",
        "capabilities",
    }
    assert api_harness.calls.builders["config"] == {
        "available_sheets": ["Options 2024", "Options 2025", "Options 2026"],
        "prefs": {"selected_sheets": ["Options 2025"], "include_unrealized": True},
        "default_sheets": ["Options 2024", "Options 2025", "Options 2026"],
        "as_of_default": date.today(),
    }


def test_dashboard_route_parses_common_query_and_repeated_selected_sheets(api_harness):
    response = api_harness.client.get(
        "/v1/mobile/dashboard",
        params=[
            ("as_of", "2026-05-03"),
            ("include_unrealized", "false"),
            ("selected_sheets", "Options 2024"),
            ("selected_sheets", "Options 2026"),
            ("cache_bust", "7"),
        ],
    )

    assert response.status_code == 200
    assert set(response.json()) == {
        "request",
        "data_freshness",
        "snapshot",
        "monthly_target",
        "open_option_short_preview",
        "issue_summary",
    }
    context = api_harness.calls.contexts[-1]
    assert context.request.sheet_id == "sheet-id"
    assert context.request.as_of == date(2026, 5, 3)
    assert context.request.include_unrealized is False
    assert context.request.selected_sheets == ["Options 2024", "Options 2026"]
    assert context.request.cache_bust == 7
    assert context.available_sheets == ["Options 2024", "Options 2025", "Options 2026"]


def test_refresh_route_parses_common_query_and_generates_cache_bust(api_harness):
    response = api_harness.client.post(
        "/v1/mobile/refresh",
        params=[
            ("as_of", "2026-05-03"),
            ("include_unrealized", "false"),
            ("selected_sheets", "Options 2024"),
            ("selected_sheets", "Options 2026"),
        ],
    )

    assert response.status_code == 200
    assert set(response.json()) == {"request", "data_freshness", "refresh"}
    context = api_harness.calls.contexts[-1]
    assert context.request.as_of == date(2026, 5, 3)
    assert context.request.include_unrealized is False
    assert context.request.selected_sheets == ["Options 2024", "Options 2026"]
    assert context.request.cache_bust == 99
    assert api_harness.calls.builders["refresh"]["cache_bust"] == 99


def test_refresh_route_respects_explicit_cache_bust(api_harness):
    response = api_harness.client.post("/v1/mobile/refresh?cache_bust=123")

    assert response.status_code == 200
    assert api_harness.calls.contexts[-1].request.cache_bust == 123
    assert response.json()["refresh"]["cache_bust"] == 123


def test_refresh_route_records_best_effort_audit(api_harness, monkeypatch):
    class FakeAuditStore:
        def __init__(self):
            self.source_snapshots = {}
            self.refresh_runs = []

        def upsert_source_snapshot(self, snapshot_id, snapshot):
            self.source_snapshots[snapshot_id] = snapshot

        def record_refresh_run(self, record):
            self.refresh_runs.append(record)

    store = FakeAuditStore()
    monkeypatch.setattr(mobile_api, "get_default_audit_store", lambda: store)

    response = api_harness.client.post("/v1/mobile/refresh?cache_bust=123")

    assert response.status_code == 200
    assert len(store.refresh_runs) == 1
    record = store.refresh_runs[0]
    assert record.run_id == "mobile-refresh:123"
    assert record.status == "refreshed"
    assert record.request["selected_sheets"] == ["Options 2025"]
    assert "route_total_ms" in record.timings_ms


def test_refresh_audit_records_source_snapshot(monkeypatch):
    class FakeAuditStore:
        def __init__(self):
            self.source_snapshots = {}
            self.refresh_runs = []

        def upsert_source_snapshot(self, snapshot_id, snapshot):
            self.source_snapshots[snapshot_id] = snapshot

        def record_refresh_run(self, record):
            self.refresh_runs.append(record)

    store = FakeAuditStore()
    monkeypatch.setattr(mobile_api, "get_default_audit_store", lambda: store)
    request = SimpleNamespace(state=SimpleNamespace(mobile_timings={"price_fetch_ms": 12.3}))
    context = SimpleNamespace(
        source_metadata={
            "source_snapshot_id": "snapshot-1",
            "source_content_hash": "hash-1",
            "source_selected_sheets": ["Options 2026"],
            "source_sheet_counts": [{"name": "Options 2026", "rows": 5}],
            "source_row_count": 5,
        }
    )
    payload = {
        "request": {"selected_sheets": ["Options 2026"]},
        "data_freshness": {"source_sheets": [{"name": "Options 2026", "rows": 5}]},
        "refresh": {"status": "refreshed", "reload_endpoints": []},
    }

    mobile_api._record_refresh_audit(
        request=request,
        context=context,
        payload=payload,
        cache_bust=123,
        started_at="2026-05-08T20:00:00+00:00",
        finished_at="2026-05-08T20:00:01+00:00",
    )

    assert store.source_snapshots["snapshot-1"]["content_hash"] == "hash-1"
    assert store.source_snapshots["snapshot-1"]["row_count"] == 5
    assert store.refresh_runs[0].source_snapshot_id == "snapshot-1"
    assert store.refresh_runs[0].timings_ms == {"price_fetch_ms": 12.3}


def test_read_routes_reuse_context_for_same_request(api_harness):
    dashboard_response = api_harness.client.get("/v1/mobile/dashboard")
    positions_response = api_harness.client.get("/v1/mobile/positions")

    assert dashboard_response.status_code == 200
    assert positions_response.status_code == 200
    assert len(api_harness.calls.contexts) == 1
    assert api_harness.calls.builders["dashboard"] is api_harness.calls.builders["positions"]


def test_refresh_updates_default_read_cache_bust(api_harness):
    refresh_response = api_harness.client.post("/v1/mobile/refresh?cache_bust=123")
    dashboard_response = api_harness.client.get("/v1/mobile/dashboard")

    assert refresh_response.status_code == 200
    assert dashboard_response.status_code == 200
    assert len(api_harness.calls.contexts) == 1
    assert api_harness.calls.builders["dashboard"].request.cache_bust == 123


@pytest.mark.parametrize(
    ("path", "expected_keys", "builder_name"),
    [
        (
            "/v1/mobile/positions",
            {"request", "data_freshness", "inventory", "open_option_shorts"},
            "positions",
        ),
        (
            "/v1/mobile/performance/yearly",
            {"request", "data_freshness", "years"},
            "yearly",
        ),
        (
            "/v1/mobile/issues",
            {"request", "data_freshness", "summary", "issues", "coverage"},
            "issues",
        ),
    ],
)
def test_read_routes_use_default_selected_sheets(api_harness, path, expected_keys, builder_name):
    response = api_harness.client.get(path)

    assert response.status_code == 200
    assert set(response.json()) == expected_keys
    assert api_harness.calls.contexts[-1].request.selected_sheets == ["Options 2025"]
    assert builder_name in api_harness.calls.builders


def test_open_option_shorts_route_parses_sort_and_limit(api_harness):
    response = api_harness.client.get("/v1/mobile/open-option-shorts?sort=expiration&limit=2")

    assert response.status_code == 200
    assert set(response.json()) == {"request", "data_freshness", "moneyness_legend", "items"}
    builder_call = api_harness.calls.builders["open_option_shorts"]
    assert builder_call["sort"] == "expiration"
    assert builder_call["limit"] == 2


def test_tickers_route_parses_detail_query(api_harness):
    response = api_harness.client.get("/v1/mobile/tickers?year=2026&include_history=true")

    assert response.status_code == 200
    assert set(response.json()) == {"request", "data_freshness", "items"}
    builder_call = api_harness.calls.builders["tickers"]
    assert builder_call["year"] == 2026
    assert builder_call["include_history"] is True


def test_monthly_route_parses_target_and_range(api_harness):
    response = api_harness.client.get("/v1/mobile/performance/monthly?target_return=0.02&range=3m")

    assert response.status_code == 200
    assert set(response.json()) == {
        "request",
        "data_freshness",
        "target_return",
        "target_basis",
        "return_metric",
        "current_month",
        "months",
    }
    builder_call = api_harness.calls.builders["monthly"]
    assert builder_call["target_return"] == pytest.approx(0.02)
    assert builder_call["monthly_range"] == "3m"


@pytest.mark.parametrize(
    ("path", "status_code", "error_code"),
    [
        ("/v1/mobile/open-option-shorts?sort=unsupported", 400, "invalid_open_option_sort"),
        ("/v1/mobile/open-option-shorts?limit=-1", 400, "invalid_limit"),
        ("/v1/mobile/performance/monthly?range=unsupported", 400, "invalid_monthly_range"),
    ],
)
def test_route_validation_errors_use_error_envelope(api_harness, path, status_code, error_code):
    response = api_harness.client.get(path)

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code
    assert "message" in response.json()["error"]
    assert "details" in response.json()["error"]
    assert response.json()["error"]["request_id"] is None
    assert api_harness.calls.contexts == []


def test_no_selected_sheets_returns_contract_error(monkeypatch, api_harness):
    monkeypatch.setattr(mobile_api, "_available_sheets", lambda: [])
    monkeypatch.setattr(mobile_api.dashboard_app, "load_prefs", lambda: {"selected_sheets": []})

    response = api_harness.client.get("/v1/mobile/dashboard")

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "no_selected_sheets",
            "message": "No selected option sheets are available.",
            "details": {"selected_sheets": [], "available_sheets": [], "missing_sheets": []},
            "request_id": None,
        }
    }
    assert api_harness.calls.contexts == []
