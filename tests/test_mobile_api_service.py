from datetime import date

import pandas as pd

import portfolio_backend.mobile_api_service as service
from portfolio_backend.mobile_api_service import (
    MobilePayloadRequest,
    MobileServiceDependencies,
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_payload_context,
    build_mobile_positions_payload,
    build_mobile_refresh_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)
from tests.test_mobile_payloads import _mobile_state


def _dependencies():
    return MobileServiceDependencies(
        load_options=lambda sheet_id, sheets: pd.DataFrame(),
        fetch_price_history=lambda tickers, start, end: ({}, [], {"requested": 0, "fetched": 0}),
        collect_dividend_cashflows=lambda stock_txns, as_of: None,
        align_benchmarks_monthly=lambda tickers, idx: {},
        fetch_current_prices=lambda tickers: (
            {"PUTT": 80.0, "CALL": 60.0, "CLEAR": 120.0},
            ["MISS: no price returned"],
            {"requested": 4, "fetched": 3},
        ),
    )


def test_mobile_payload_context_builds_priced_state(monkeypatch):
    base_state = _mobile_state()

    monkeypatch.setattr(service, "build_base_pipeline", lambda *args, **kwargs: base_state)
    monkeypatch.setattr(service, "current_price_tickers_for_state", lambda state: ("CALL", "CLEAR", "MISS", "PUTT"))

    def apply_live(base_state, live_prices, price_errors, price_summary, price_updated_at):
        base_state.stock_prices = live_prices
        base_state.price_errors = price_errors
        base_state.price_summary = {
            "stocks_requested": price_summary["requested"],
            "stocks_fetched": price_summary["fetched"],
        }
        base_state.price_updated_at = price_updated_at
        return base_state

    monkeypatch.setattr(service, "apply_live_price_overlay", apply_live)
    monkeypatch.setattr(service, "apply_unrealized_adjusted_display", lambda state, include_unrealized: state)
    monkeypatch.setattr(service, "_now_iso", lambda: "2026-05-04T20:00:00+02:00")

    context = build_mobile_payload_context(
        MobilePayloadRequest(
            sheet_id="sheet",
            as_of=date(2026, 5, 3),
            selected_sheets=["Options 2025", "Options 2026"],
            include_unrealized=True,
        ),
        _dependencies(),
        available_sheets=["Options 2025"],
        source_metadata={"source_downloaded_at": "2026-05-04T19:59:00+02:00"},
    )

    assert context.request == {
        "as_of": date(2026, 5, 3),
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert context.available_sheets == ["Options 2025"]
    assert context.source_metadata == {
        "source_downloaded_at": "2026-05-04T19:59:00+02:00",
        "pipeline_built_at": "2026-05-04T20:00:00+02:00",
        "prices_updated_at": "2026-05-04T20:00:00+02:00",
    }
    assert context.state.price_errors == ["MISS: no price returned"]
    assert context.state.price_summary == {"stocks_requested": 4, "stocks_fetched": 3}


def test_mobile_payload_wrappers_emit_expected_top_level_shapes(monkeypatch):
    base_state = _mobile_state()
    monkeypatch.setattr(service, "build_base_pipeline", lambda *args, **kwargs: base_state)
    monkeypatch.setattr(service, "current_price_tickers_for_state", lambda state: ())
    monkeypatch.setattr(service, "apply_unrealized_adjusted_display", lambda state, include_unrealized: state)
    monkeypatch.setattr(service, "_now_iso", lambda: "2026-05-04T20:00:00+02:00")

    deps = _dependencies()
    deps = MobileServiceDependencies(
        load_options=deps.load_options,
        fetch_price_history=deps.fetch_price_history,
        collect_dividend_cashflows=deps.collect_dividend_cashflows,
        align_benchmarks_monthly=deps.align_benchmarks_monthly,
        fetch_current_prices=None,
    )
    context = build_mobile_payload_context(
        MobilePayloadRequest(
            sheet_id="sheet",
            as_of=date(2026, 5, 3),
            selected_sheets=["Options 2025", "Options 2026"],
            include_unrealized=True,
        ),
        deps,
        available_sheets=["Options 2025"],
    )

    assert set(build_mobile_dashboard_payload(context)) == {
        "request",
        "data_freshness",
        "snapshot",
        "monthly_target",
        "monthly_target_band",
        "open_option_short_preview",
        "issue_summary",
    }
    assert set(build_mobile_positions_payload(context)) == {"request", "data_freshness", "inventory", "open_option_shorts"}
    assert set(build_mobile_open_option_shorts_payload(context, limit=2)) == {
        "request",
        "data_freshness",
        "moneyness_legend",
        "items",
    }
    assert set(build_mobile_tickers_payload(context, year=2026, include_history=True)) == {
        "request",
        "data_freshness",
        "items",
    }
    assert set(build_mobile_monthly_payload(context)) == {
        "request",
        "data_freshness",
        "target_return",
        "target_floor",
        "target_basis",
        "return_metric",
        "active_cycle",
        "current_month",
        "months",
        "future_months",
    }
    assert set(build_mobile_yearly_payload(context)) == {"request", "data_freshness", "years"}
    assert set(build_mobile_issues_payload(context)) == {
        "request",
        "data_freshness",
        "summary",
        "issues",
        "audit_summary",
        "audit_notes",
        "coverage",
    }
    assert set(build_mobile_refresh_payload(context, cache_bust=9)) == {"request", "data_freshness", "refresh"}
