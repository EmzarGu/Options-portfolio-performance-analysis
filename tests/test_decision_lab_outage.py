"""Exercise the uncached page and provider outage path that production exposed."""
from datetime import date
from types import SimpleNamespace

import pytest
import requests
from fastapi.testclient import TestClient

import web_dashboard
from portfolio_backend import web_auth, web_data_service as web_data
from portfolio_backend.option_market import cutemarkets, decision_data
from portfolio_backend.option_market.models import OptionMarketFetchResult
from portfolio_backend.option_market.store import MemoryOptionMarketStore
from tests.test_decision_lab import _base_payload
from tests.test_option_market_validation import _contract


def universe():
    return decision_data.build_decision_option_universe([
        {"ticker": f"T{i}", "contract_requests": [
            {"expiry": expiry, "put_call": "CALL"}
            for expiry in ("2026-06-18", "2026-07-17")
        ]} for i in range(9)
    ], as_of=date(2026, 5, 25))


class OfflineProvider:
    configured = True
    calls = 0

    def fetch_chain(self, request, *, deadline=None):
        self.calls += 1
        assert deadline is not None
        raise requests.ConnectTimeout("simulated offline service")


def test_page_load_without_option_cache_never_contacts_provider():
    provider = OfflineProvider()
    data = decision_data.load_or_fetch_decision_option_data(
        store=MemoryOptionMarketStore(), universe=universe(),
        provider_client=provider, allow_fetch=False,
    )
    assert provider.calls == 0
    assert data.contracts == []
    assert data.status["status"] == "not_fetched"
    assert data.status["request_count"] == 18


def test_refresh_stops_after_first_connection_timeout():
    provider = OfflineProvider()
    data = decision_data.load_or_fetch_decision_option_data(
        store=MemoryOptionMarketStore(), universe=universe(),
        provider_client=provider, force_refresh=True,
    )
    assert provider.calls == 1
    assert data.status["status"] == "failed"
    assert data.status["skipped_request_count"] == 17
    assert "unavailable" in data.status["message"]


def test_partial_error_preserves_last_good_chain_even_without_universe_run():
    store = MemoryOptionMarketStore()
    chain = universe().requests[0]
    good = OptionMarketFetchResult(chain, [_contract(chain, mark=1.25)], [],
                                   "2026-05-25T12:00:00Z", 10, 200)
    store.save_chain_snapshot(good)

    class PartialProvider:
        configured = True

        def fetch_chain(self, request, *, deadline=None):
            return OptionMarketFetchResult(request, [_contract(request, mark=99)], [],
                                           "2026-05-26T12:00:00Z", 10, 503, "service unavailable")

    data = decision_data.load_or_fetch_decision_option_data(
        store=store, universe=universe(), provider_client=PartialProvider(), force_refresh=True,
    )
    assert data.status["status"] == "failed_refresh_kept_previous"
    assert data.contracts[0].mark == 1.25
    assert store.load_contracts(chain)[0]["mark"] == 1.25
    assert store.load_chain_snapshot(chain)["fetched_at"] == good.fetched_at


def test_refresh_uses_one_deadline_for_all_chains(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(decision_data, "monotonic", lambda: clock[0])
    deadlines = []

    class SlowProvider:
        configured = True

        def fetch_chain(self, request, *, deadline=None):
            deadlines.append(deadline)
            clock[0] += 16
            return OptionMarketFetchResult(request, [], [], "2026-05-25T12:00:00Z", 16000, 200)

    data = decision_data.load_or_fetch_decision_option_data(
        store=MemoryOptionMarketStore(), universe=universe(),
        provider_client=SlowProvider(), force_refresh=True,
    )
    assert deadlines == [130.0, 130.0]
    assert data.status["skipped_request_count"] == 16


def test_retry_after_cannot_exceed_refresh_budget(monkeypatch):
    monkeypatch.setattr(cutemarkets.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(cutemarkets.time, "sleep", lambda _: pytest.fail("must not sleep past budget"))
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=429, headers={"Retry-After": "3600"})

    client = cutemarkets.CuteMarketsClient(api_key="test", session=SimpleNamespace(get=get))
    with pytest.raises(requests.Timeout):
        client.fetch_chain(universe().requests[0], deadline=130.0)
    assert len(calls) == 1
    assert calls[0]["timeout"] == (5.0, 15.0)


def test_pagination_stops_at_shared_deadline(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(cutemarkets.time, "monotonic", lambda: clock[0])
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        clock[0] += 16
        return SimpleNamespace(status_code=200, headers={}, json=lambda: {
            "results": [], "next_url": "https://api.cutemarkets.com/next-page",
        })

    client = cutemarkets.CuteMarketsClient(api_key="test", session=SimpleNamespace(get=get))
    with pytest.raises(requests.Timeout):
        client.fetch_chain(universe().requests[0], deadline=130.0)
    assert len(calls) == 2
    assert calls[1]["timeout"] == (5.0, 7.0)


def test_authenticated_uncached_route_survives_outage_and_refresh(monkeypatch):
    payload = _base_payload()
    payload["positions"]["inventory"] = [
        {"ticker": "ASAN", "shares": 100, "cost_per_share": 17.5,
         "current_price": 6.51, "unrealized_pnl": -1099.0},
    ]
    store, provider = MemoryOptionMarketStore(), OfflineProvider()
    web_data._clear_dashboard_data_cache()
    monkeypatch.setattr(web_auth, "_is_authenticated", lambda _: True)
    monkeypatch.setattr(web_dashboard, "_monthly_target_band_from_request", lambda _: {"target_floor": .01, "target_return": .015})
    monkeypatch.setattr(web_data, "_get_cached_dashboard_data", lambda **_: payload)
    monkeypatch.setattr(web_data, "load_derived_payload", lambda _: None)
    monkeypatch.setattr(web_data, "save_derived_payload", lambda *a, **kw: None)
    monkeypatch.setattr(web_data, "_load_probability_trade_matches", lambda: [])
    monkeypatch.setattr(web_data, "_load_historical_option_enrichments", lambda: [])
    monkeypatch.setattr(web_data, "_decision_option_store", lambda: store)
    monkeypatch.setattr(web_data, "_decision_option_provider", lambda: provider)
    client = TestClient(web_dashboard.app)
    try:
        response = client.get("/api/decision-lab")
        assert response.status_code == 200
        assert provider.calls == 0
        assert response.json()["ticker_situations"][0]["ticker"] == "ASAN"
        assert response.json()["option_market_data"]["status"]["status"] == "not_fetched"
        refreshed = client.post("/api/decision-lab/options/refresh")
        assert refreshed.status_code == 200
        assert provider.calls == 1
        assert refreshed.json()["ticker_situations"] == response.json()["ticker_situations"]
        assert refreshed.json()["strike_quality"] == response.json()["strike_quality"]
        assert refreshed.json()["option_market_data"]["status"]["status"] == "failed"
    finally:
        web_data._clear_dashboard_data_cache()
