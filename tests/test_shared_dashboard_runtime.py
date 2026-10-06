"""Regression coverage for shared web/mobile orchestration and financial payloads."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
import subprocess
import sys
from threading import Event, Lock
from types import SimpleNamespace

import pandas as pd
import pytest

from portfolio_backend import context_runtime, mobile_api_service
from portfolio_backend import web_dashboard_payloads as web
from portfolio_backend.mobile_api_service import MobilePayloadContext, MobilePayloadRequest
from portfolio_backend.pipeline import apply_live_price_overlay, apply_unrealized_adjusted_display
from tests.test_pnl import _make_live_overlay_base_state


def test_web_import_does_not_load_mobile_http_application():
    """A fresh web process must not instantiate the mobile transport as a dependency."""
    result = subprocess.run(
        [sys.executable, "-c", "import web_dashboard; import sys; assert 'mobile_api' not in sys.modules"],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_concurrent_web_and_mobile_reads_share_one_context_build(monkeypatch):
    """Overlapping cold reads from both clients reuse one process-local build."""
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "firestore")
    request = MobilePayloadRequest("ibkr-flex", date(2026, 9, 19), ["Options 2026"], True)
    marker = {"source_snapshot_id": "shared-test", "import_run_id": "import-test"}
    context = SimpleNamespace(source_metadata={
        "source_snapshot_id": "shared-test", "ibkr_import_run_id": "import-test",
    })
    monkeypatch.setattr(context_runtime, "_common_request", lambda **kwargs: (request, request.selected_sheets))
    monkeypatch.setattr(context_runtime, "_refresh_source_marker", lambda **kwargs: marker)
    original_lock = context_runtime._context_build_lock_for_key
    counter_lock = Lock()
    both_requested = Event()
    release_build = Event()
    lock_requests = 0
    build_count = 0

    def acquire_build_lock(key):
        """Signal when both callers have reached the same cache miss."""
        nonlocal lock_requests
        with counter_lock:
            lock_requests += 1
            if lock_requests == 2:
                both_requested.set()
        return original_lock(key)

    def build_context(**kwargs):
        """Hold the cold build until the other client is waiting for it."""
        nonlocal build_count
        build_count += 1
        assert release_build.wait(5), "Second reader did not reach the build lock"
        context_runtime._remember_context(kwargs["key"], context)
        return context

    monkeypatch.setattr(context_runtime, "_context_build_lock_for_key", acquire_build_lock)
    monkeypatch.setattr(context_runtime, "_build_context_uncached", build_context)
    context_runtime._clear_context_cache()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            web_read = pool.submit(web.get_web_context, as_of=request.as_of, include_unrealized=True)
            mobile_read = pool.submit(
                context_runtime.get_context, as_of=request.as_of, include_unrealized=True,
                selected_sheets=None, cache_bust=None,
            )
            try:
                assert both_requested.wait(5), "Both clients must overlap on a cold cache"
            finally:
                release_build.set()
            assert web_read.result(timeout=5)[0] is context
            assert mobile_read.result(timeout=5) is context
        assert build_count == 1
    finally:
        context_runtime._clear_context_cache()


@pytest.mark.parametrize("include_unrealized,target_return,target_floor", [
    (True, 0.015, 0.01), (False, 0.02, 0.005), (True, 0.0, 0.0),
])
def test_web_financial_sections_match_mobile_payloads(monkeypatch, include_unrealized, target_return, target_floor):
    """Use real accounting state and DTO builders to compare both client contracts."""
    state = _make_live_overlay_base_state()
    state = apply_unrealized_adjusted_display(
        apply_live_price_overlay(state, {"AAA": 120}, [], {"requested": 1, "fetched": 1}, "12:00:00"), True,
    )
    context = MobilePayloadContext(state, {
        "as_of": state.as_of, "include_unrealized": True, "selected_sheets": ["Options 2026"],
    }, ["Options 2026"], {})
    monkeypatch.setattr(web, "get_web_context", lambda **kwargs: (context, None))
    monkeypatch.setattr(mobile_api_service, "_now_iso", lambda: "2026-09-19T12:00:00+00:00")
    payload = web.build_dashboard_data(
        as_of=state.as_of.date(), include_unrealized=include_unrealized,
        target_return=target_return, target_floor=target_floor,
    )
    targets = {"target_return": target_return, "target_floor": target_floor}
    expected = {
        "dashboard": mobile_api_service.build_mobile_dashboard_payload(context, **targets),
        "positions": mobile_api_service.build_mobile_positions_payload(context),
        "open_shorts": mobile_api_service.build_mobile_open_option_shorts_payload(context, sort="moneyness_risk", limit=None),
        "tickers": mobile_api_service.build_mobile_tickers_payload(context, include_history=False),
        "monthly": mobile_api_service.build_mobile_monthly_payload(context, monthly_range="since_inception", **targets),
        "yearly": mobile_api_service.build_mobile_yearly_payload(context),
        "issues": mobile_api_service.build_mobile_issues_payload(context),
    }
    for section, value in expected.items():
        assert payload[section] == value, section
    assert payload["web"] == {"include_unrealized": include_unrealized, **targets}
    assert payload["views"]["snapshots"]["with_unrealized"] == web.build_mobile_snapshot(state, True)
    assert payload["views"]["snapshots"]["realized_only"] == web.build_mobile_snapshot(state, False)
    assert payload["dashboard"]["snapshot"]["ytd_total_pnl"] == 2350.0


@pytest.mark.parametrize("index_name,limit,expected", [
    ("month", None, [
        {"month": "2026-01-01T00:00:00", "pnl": 12.5},
        {"month": "2026-02-01T00:00:00", "pnl": None},
    ]),
    ("month", 1, [{"month": "2026-01-01T00:00:00", "pnl": 12.5}]),
    (None, None, [{"pnl": 12.5}, {"pnl": None}]),
])
def test_frame_records_preserves_values_and_source_frame(index_name, limit, expected):
    """Index conversion and limiting preserve JSON values without mutating input."""
    frame = pd.DataFrame({"pnl": [12.5, float("nan")]}, index=pd.to_datetime(["2026-01-01", "2026-02-01"]))
    frame.index.name = "period"
    original = frame.copy(deep=True)
    assert web._frame_records(frame, index_name=index_name, limit=limit) == expected
    pd.testing.assert_frame_equal(frame, original)


def test_existing_visual_prototype_uses_shared_data_service(monkeypatch):
    """Keep the existing local prototype's data path working after service extraction."""
    import visual_prototype
    from tests.test_visual_prototype import _dashboard_payload, _decision_payload

    dashboard = _dashboard_payload()
    decision_lab = _decision_payload()
    band = {"target_return": 0.02, "target_floor": 0.015}
    monkeypatch.setenv("VISUAL_PROTOTYPE_ASSIGNMENT_QUALITY", "1")
    monkeypatch.setattr(visual_prototype, "load_monthly_target_band", lambda: band)

    def load_dashboard(**kwargs):
        """Check the prototype passes its requested view and saved target band."""
        assert kwargs == {"as_of": date(2026, 9, 19), "include_unrealized": False, **band}
        return dashboard

    def load_decision_lab(source_payload):
        """Use the dashboard's source identity for the related payload."""
        assert source_payload is dashboard
        return decision_lab

    def load_assignment_quality(**kwargs):
        """Ensure the optional assignment view keeps the same source and date."""
        assert kwargs == {"source_payload": dashboard, "as_of": date(2026, 9, 19)}
        return {}

    monkeypatch.setattr(visual_prototype.web_data, "_get_cached_dashboard_data", load_dashboard)
    monkeypatch.setattr(visual_prototype.web_data, "_get_cached_decision_lab_payload", load_decision_lab)
    monkeypatch.setattr(visual_prototype.web_data, "_get_cached_assignment_quality_data", load_assignment_quality)
    payload = visual_prototype._prototype_payload(as_of=date(2026, 9, 19), include_unrealized=False)
    assert payload == visual_prototype.build_visual_prototype_data(dashboard, decision_lab, {})
