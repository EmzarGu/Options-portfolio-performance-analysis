from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import web_dashboard
from portfolio_backend import web_auth, web_data_service as web_data
from portfolio_backend.app_settings import DEFAULT_DECISION_RULES, normalize_decision_rules
from portfolio_backend.decision_cards import action_group, build_decision_card, with_decision_cards
from portfolio_backend.web_dashboard_templates import DASHBOARD_HTML


@pytest.fixture(autouse=True)
def explicit_test_cookie_secret(monkeypatch):
    monkeypatch.setenv("WEB_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("WEB_DASHBOARD_COOKIE_SECRET", "test-only-cookie-signing-secret-long-enough-for-validation")


def _put_situation(ticker="AAA", priority="high", category="Reduce assignment risk", dte=16):
    return {
        "ticker": ticker,
        "priority": priority,
        "category": category,
        "objective": "Reduce near-term put assignment risk",
        "reason": "Open put is in the money",
        "signal_label": "ITM put unrealized loss",
        "signal_value": -470.0,
        "expiry": "2026-11-06",
        "dte": dte,
        "recommendation": "Compare close vs roll down/out",
    }


def _put_group(ticker="AAA", proposals=None):
    baseline = {
        "action": "Keep current put",
        "is_current_position": True,
        "strike": 150.0,
        "expiry": "2026-11-06",
        "dte": 16,
        "premium": 210.0,
        "delta": 0.64,
        "liquidity": "good",
        "roll_close_cost": 580.0,
        "exercise_result": -470.0,
        "no_exercise_result": 0.0,
        "contract_count": 1,
        "score": 65.0,
        "score_reason": "Baseline current put.",
    }
    if proposals is None:
        proposals = [
            {
                "action": "Roll down/out",
                "strike": 140.0,
                "expiry": "2026-12-04",
                "dte": 43,
                "premium": 600.0,
                "delta": 0.29,
                "liquidity": "fair",
                "roll_net_credit": 20.0,
                "expected_value_vs_current": 180.0,
                "exercise_result": 20.0,
                "no_exercise_result": 20.0,
                "contract_count": 1,
                "score": 71.0,
                "score_reason": "Roll down/out: scenario vs current $180.",
                "quote_date": "2026-10-21",
            },
            {
                "action": "Roll out same strike",
                "strike": 150.0,
                "expiry": "2026-12-04",
                "dte": 58,
                "premium": 745.0,
                "delta": 0.56,
                "liquidity": "wide spread",
                "wide_spread": True,
                "roll_net_credit": 165.0,
                "expected_value_vs_current": 90.0,
                "contract_count": 1,
                "score": 60.0,
                "score_reason": "Wide bid/ask spread.",
            },
        ]
    return {
        "ticker": ticker,
        "category": "Reduce assignment risk",
        "option_category": "Reduce assignment risk",
        "objective": "Reduce near-term put assignment risk",
        "current_state": {"current_price": 145.3, "open_options": [{"type": "Put", "strike": 150.0, "quantity": -1}]},
        "candidates": [baseline, *proposals],
        "candidate_status": {"status": "eligible", "message": "3 actionable contract(s) found."},
    }


def test_action_group_follows_priority_and_near_expiry():
    assert action_group({"priority": "high", "category": "Reduce assignment risk"}) == "now"
    assert action_group({"priority": "medium", "category": "Evaluate exit vs roll", "dte": 6}) == "now"
    assert action_group({"priority": "medium", "category": "Harvest unused put risk", "dte": 6}) == "plan"
    assert action_group({"priority": "medium", "category": "Recover with covered call"}) == "plan"
    assert action_group({"priority": "low", "category": "Accept / monitor exit", "dte": 30}) == "monitor"


def test_put_card_lays_out_keep_close_and_ranked_rolls():
    card = build_decision_card(_put_group(), _put_situation(), normalize_decision_rules(None))
    kinds = [option["kind"] for option in card["options"]]
    assert kinds == ["keep", "close", "proposal", "proposal"]

    keep, close, top, second = card["options"]
    assert keep["headline"]["value"] == 210.0
    assert keep["metrics"][0] == {"label": "Cash needed at strike", "value": 15000.0, "format": "money"}
    # A final close realizes the carried chain balance less the buyback cost.
    assert close["headline"]["value"] == pytest.approx(210.0 - 580.0)
    assert close["metrics"][0]["value"] == -580.0

    assert top["top"] is True and second["top"] is False
    assert top["headline"] == {"label": "net credit after closing", "value": 20.0, "format": "money"}
    checks = {check["rule"]: check["status"] for check in top["checks"]}
    assert checks == {"dte": "ok", "delta": "near", "spread": "ok"}
    second_checks = {check["rule"]: check["status"] for check in second["checks"]}
    assert second_checks == {"dte": "fail", "delta": "fail", "spread": "fail"}

    assert card["verdict"]["tone"] == "positive"
    assert "Roll down/out 140P 2026-12-04" in card["verdict"]["text"]
    assert "delta 0.29" in card["verdict"]["text"]
    assert card["quote_date"] == "2026-10-21"


def test_card_does_not_promote_an_alternative_that_scores_below_keeping():
    group = _put_group(proposals=[{**_put_group()["candidates"][1], "expected_value_vs_current": -40.0}])
    card = build_decision_card(group, _put_situation(), normalize_decision_rules(None))
    assert not any(option["top"] for option in card["options"])
    assert card["verdict"] == {"tone": "neutral", "text": "No alternative scores better than keeping the current position."}


def test_covered_call_entry_card_has_no_keep_column_or_put_delta_rule():
    group = {
        "ticker": "BBB",
        "category": "Recover with covered call",
        "option_category": "Recover with covered call",
        "current_state": {"current_price": 67.8, "assigned_shares": 300},
        "candidates": [
            {
                "action": "Sell covered call",
                "strike": 72.5,
                "expiry": "2026-12-18",
                "dte": 57,
                "premium": 165.0,
                "delta": 0.18,
                "liquidity": "fair",
                "upside_left": 1410.0,
                "contract_count": 3,
                "score": 66.0,
            }
        ],
        "candidate_status": {"status": "eligible"},
    }
    card = build_decision_card(group, {"ticker": "BBB", "category": "Recover with covered call"}, normalize_decision_rules(None))
    assert [option["kind"] for option in card["options"]] == ["proposal"]
    option = card["options"][0]
    assert option["headline"] == {"label": "premium received", "value": 165.0, "format": "money"}
    assert [check["rule"] for check in option["checks"]] == ["dte", "spread"]
    assert card["put_call"] == "CALL"


def test_card_without_priced_options_explains_why():
    group = {
        "ticker": "CCC",
        "category": "Accept / monitor exit",
        "candidates": [],
        "candidate_status": {"message": "No actionable contracts found: wide spread.", "near_miss_message": "Closest: Dec 105 call."},
    }
    card = build_decision_card(group, {"ticker": "CCC"}, normalize_decision_rules(None))
    assert card["options"] == []
    assert card["verdict"]["text"] == "No actionable contracts found: wide spread. Closest: Dec 105 call."


def test_with_decision_cards_groups_queue_and_keeps_lab_payload():
    lab = {
        "summary": {"action_item_count": 3},
        "ticker_situations": [
            {**_put_situation("MMM", priority="low", category="Accept / monitor exit", dte=30)},
            _put_situation("AAA"),
            {**_put_situation("KKK", priority="medium", category="Recover with covered call"), "dte": None},
        ],
        "recommendation_candidates": [_put_group("AAA")],
    }
    out = with_decision_cards(lab, {"put_delta_max": 0.3})
    assert out["summary"] == lab["summary"]
    assert "action_queue" not in lab
    assert [row["ticker"] for row in out["action_queue"]] == ["AAA", "KKK", "MMM"]
    assert [row["group"] for row in out["action_queue"]] == ["now", "plan", "monitor"]
    assert [row["has_card"] for row in out["action_queue"]] == [True, False, False]
    assert [card["ticker"] for card in out["decision_cards"]] == ["AAA"]
    assert out["decision_rules"]["put_delta_max"] == 0.3
    top = next(option for option in out["decision_cards"][0]["options"] if option["top"])
    assert {check["rule"]: check["status"] for check in top["checks"]}["delta"] == "ok"


def test_with_decision_cards_passes_through_deferred_and_error_payloads():
    assert with_decision_cards({"deferred": True}) == {"deferred": True}
    assert with_decision_cards({"error": "boom"}) == {"error": "boom"}


def test_normalize_decision_rules_defaults_bounds_and_order():
    assert {key: normalize_decision_rules(None)[key] for key in DEFAULT_DECISION_RULES} == DEFAULT_DECISION_RULES
    rules = normalize_decision_rules({"put_delta_min": 0.3, "put_delta_max": 0.2, "dte_min": 60, "dte_max": 30, "annual_target_return": 4})
    assert (rules["put_delta_min"], rules["put_delta_max"]) == (0.2, 0.3)
    assert (rules["dte_min"], rules["dte_max"]) == (30.0, 60.0)
    assert rules["annual_target_return"] == DEFAULT_DECISION_RULES["annual_target_return"]


def test_decision_rules_api_requires_auth():
    client = TestClient(web_dashboard.app)
    assert client.get("/api/settings/decision-rules").status_code == 401
    assert client.post("/api/settings/decision-rules", json={}).status_code == 401


def test_decision_rules_api_validates_and_saves(monkeypatch):
    monkeypatch.setattr(web_auth, "_is_authenticated", lambda request: True)
    monkeypatch.setattr(web_auth, "_authenticated_user", lambda request: "owner@example.com")
    saved = {}

    def fake_save(values, *, updated_by=None, source="user"):
        saved.update(values=values, updated_by=updated_by, source=source)
        return {**normalize_decision_rules(values), "source": source}

    monkeypatch.setattr(web_dashboard, "save_decision_rules", fake_save)
    monkeypatch.setattr(web_dashboard, "load_decision_rules", lambda: normalize_decision_rules({"dte_max": 40}))
    client = TestClient(web_dashboard.app)

    assert client.get("/api/settings/decision-rules").json()["dte_max"] == 40.0
    bad = client.post("/api/settings/decision-rules", json={"put_delta_min": 0.4, "put_delta_max": 0.2})
    assert bad.status_code == 400
    out_of_range = client.post("/api/settings/decision-rules", json={"dte_max": 500})
    assert out_of_range.status_code == 400
    assert "between" in out_of_range.json()["error"]
    assert not saved

    ok = client.post(
        "/api/settings/decision-rules",
        json={"annual_target_return": 0.18, "put_delta_min": 0.15, "put_delta_max": 0.3, "dte_min": 21, "dte_max": 45},
    )
    assert ok.status_code == 200
    assert saved["values"]["put_delta_max"] == 0.3
    assert saved["updated_by"] == "owner@example.com" and saved["source"] == "web"


def test_decision_lab_api_attaches_cards(monkeypatch):
    monkeypatch.setattr(web_auth, "_is_authenticated", lambda request: True)
    monkeypatch.setattr(web_data, "_get_cached_dashboard_data", lambda **_: {"dashboard": {}})
    monkeypatch.setattr(
        web_data,
        "_get_cached_decision_lab_payload",
        lambda payload, force_refresh=False: {"ticker_situations": [_put_situation()], "recommendation_candidates": [_put_group()]},
    )
    monkeypatch.setattr(web_dashboard, "load_decision_rules", lambda: normalize_decision_rules(None))
    client = TestClient(web_dashboard.app)
    body = client.get("/api/decision-lab").json()
    assert body["action_queue"][0]["group"] == "now"
    assert body["decision_cards"][0]["ticker"] == "AAA"
    refreshed = client.post("/api/decision-lab/options/refresh").json()
    assert refreshed["decision_cards"][0]["options"][1]["kind"] == "close"


def test_refresh_returns_to_new_sections(monkeypatch):
    monkeypatch.setattr(web_auth, "_is_authenticated", lambda request: True)
    monkeypatch.setattr(web_data, "_get_context", lambda **_: (object(), 123))
    monkeypatch.setattr(web_dashboard, "build_mobile_refresh_payload", lambda *_, **__: {})
    client = TestClient(web_dashboard.app)
    response = client.post("/refresh?section=today", follow_redirects=False)
    assert "section=today" in response.headers["location"]


def test_dashboard_template_adds_new_tabs_without_replacing_existing_ones():
    for existing in ['["dashboard","Dashboard"]', '["decision_lab","Decision Lab"]', '["assignment_quality","Assignment Quality Lab"]']:
        assert existing in DASHBOARD_HTML
    assert '["today","Today"]' in DASHBOARD_HTML
    assert '["decision_cards","Decision cards"]' in DASHBOARD_HTML
    assert '<section id="today" class="section"></section>' in DASHBOARD_HTML
    assert '<section id="decision_cards" class="section"></section>' in DASHBOARD_HTML
    assert "today: renderToday" in DASHBOARD_HTML
    assert "decision_cards: renderDecisionCards" in DASHBOARD_HTML
    assert '"/api/settings/decision-rules"' in DASHBOARD_HTML
    # Dashboard stays the default landing section.
    assert 'active: "dashboard"' in DASHBOARD_HTML
    assert '<section id="dashboard" class="section active"></section>' in DASHBOARD_HTML


def test_load_decision_rules_falls_back_to_defaults_when_firestore_fails(monkeypatch):
    import portfolio_backend.app_settings as app_settings

    monkeypatch.setattr(app_settings, "_decision_rules_cache", None)
    monkeypatch.setenv("APP_SETTINGS_CACHE_SECONDS", "0")

    def broken_client():
        raise RuntimeError("firestore unavailable")

    monkeypatch.setattr(app_settings, "firestore_client", broken_client)
    rules = app_settings.load_decision_rules()
    assert {key: rules[key] for key in DEFAULT_DECISION_RULES} == DEFAULT_DECISION_RULES
    assert rules["source"] == "default"
