from __future__ import annotations

from datetime import datetime
from time import perf_counter
from typing import Any, Callable, Dict, Iterable, Optional

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport
from portfolio_backend.ibkr.pipeline import build_ibkr_base_pipeline
from portfolio_backend.mobile_api_service import (
    MobilePayloadContext,
    MobilePayloadRequest,
    MobileServiceDependencies,
)
from portfolio_backend.pipeline import (
    apply_live_price_overlay,
    apply_unrealized_adjusted_display,
    current_price_tickers_for_state,
)


TimingRecorderFn = Callable[[str, float], None]


def build_ibkr_mobile_payload_context(
    request: MobilePayloadRequest,
    dependencies: MobileServiceDependencies,
    report: IbkrFlexReport,
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    timing_recorder: Optional[TimingRecorderFn] = None,
) -> MobilePayloadContext:
    def record(phase: str, started_at: float) -> None:
        if timing_recorder is not None:
            timing_recorder(phase, (perf_counter() - started_at) * 1000)

    metadata = dict(source_metadata or {})
    metadata.setdefault("source", "ibkr_flex")
    metadata.setdefault("from_date", report.metadata.get("fromDate"))
    metadata.setdefault("to_date", report.metadata.get("toDate"))
    metadata.setdefault("source_files", report.metadata.get("sourceFiles"))
    metadata.setdefault("pipeline_built_at", datetime.now().astimezone().isoformat(timespec="seconds"))

    started_at = perf_counter()
    base_state = build_ibkr_base_pipeline(
        report,
        as_of=request.as_of,
        fetch_price_history_fn=dependencies.fetch_price_history,
        align_benchmarks_monthly_fn=dependencies.align_benchmarks_monthly,
        selected_sheets=request.selected_sheets,
        cache_bust=request.cache_bust,
        timing_recorder=timing_recorder,
    )
    record("pipeline_build_ms", started_at)

    state = base_state
    if dependencies.fetch_current_prices is not None:
        started_at = perf_counter()
        tickers = list(current_price_tickers_for_state(base_state))
        record("price_ticker_resolution_ms", started_at)
        started_at = perf_counter()
        live_prices, price_errors, price_summary = dependencies.fetch_current_prices(tickers)
        record("price_fetch_ms", started_at)
        prices_updated_at = datetime.now().astimezone().isoformat(timespec="seconds")
        metadata.setdefault("prices_updated_at", prices_updated_at)
        started_at = perf_counter()
        state = apply_live_price_overlay(
            base_state,
            live_prices,
            price_errors,
            price_summary,
            prices_updated_at,
        )
        record("price_overlay_ms", started_at)

    started_at = perf_counter()
    state = apply_unrealized_adjusted_display(state, request.include_unrealized)
    record("unrealized_adjustment_ms", started_at)

    return MobilePayloadContext(
        state=state,
        request={
            "as_of": request.as_of,
            "include_unrealized": request.include_unrealized,
            "selected_sheets": request.selected_sheets,
        },
        available_sheets=[str(sheet) for sheet in available_sheets] if available_sheets is not None else None,
        source_metadata=metadata,
        base_state=base_state,
    )
