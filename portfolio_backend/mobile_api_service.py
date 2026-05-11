from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from time import perf_counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from portfolio_backend.mobile_payloads import (
    build_mobile_dashboard,
    build_mobile_issues,
    build_mobile_monthly_performance,
    build_mobile_open_option_shorts,
    build_mobile_positions,
    build_mobile_refresh,
    build_mobile_tickers,
    build_mobile_yearly_performance,
)
from portfolio_backend.models import PipelineState
from portfolio_backend.pipeline import (
    apply_live_price_overlay,
    apply_unrealized_adjusted_display,
    build_base_pipeline,
    current_price_tickers_for_state,
)


LoadOptionsFn = Callable[[str, List[str]], pd.DataFrame]
FetchPriceHistoryFn = Callable[[set, pd.Timestamp, pd.Timestamp], Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]]
CollectDividendCashflowsFn = Callable[[List, pd.Timestamp], Any]
AlignBenchmarksMonthlyFn = Callable[[Dict[str, str], pd.DatetimeIndex], Dict[str, pd.Series]]
FetchCurrentPricesFn = Callable[[List[str]], Tuple[Dict[str, float], List[str], Dict[str, int]]]
TimingRecorderFn = Callable[[str, float], None]


@dataclass(frozen=True)
class MobilePayloadRequest:
    sheet_id: str
    as_of: date
    selected_sheets: List[str]
    include_unrealized: bool
    cache_bust: int = 1


@dataclass(frozen=True)
class MobileServiceDependencies:
    load_options: LoadOptionsFn
    fetch_price_history: FetchPriceHistoryFn
    collect_dividend_cashflows: CollectDividendCashflowsFn
    align_benchmarks_monthly: AlignBenchmarksMonthlyFn
    fetch_current_prices: Optional[FetchCurrentPricesFn] = None


@dataclass(frozen=True)
class MobilePayloadContext:
    state: PipelineState
    request: Dict[str, Any]
    available_sheets: Optional[List[str]]
    source_metadata: Dict[str, Any]
    base_state: Optional[PipelineState] = None


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _request_dict(request: MobilePayloadRequest) -> Dict[str, Any]:
    return {
        "as_of": request.as_of,
        "include_unrealized": request.include_unrealized,
        "selected_sheets": request.selected_sheets,
    }


def build_mobile_payload_context(
    request: MobilePayloadRequest,
    dependencies: MobileServiceDependencies,
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
    timing_recorder: Optional[TimingRecorderFn] = None,
) -> MobilePayloadContext:
    def record(phase: str, started_at: float) -> None:
        if timing_recorder is not None:
            timing_recorder(phase, (perf_counter() - started_at) * 1000)

    started_at = perf_counter()
    base_state = build_base_pipeline(
        request.sheet_id,
        request.as_of,
        request.selected_sheets,
        dependencies.load_options,
        dependencies.fetch_price_history,
        dependencies.collect_dividend_cashflows,
        dependencies.align_benchmarks_monthly,
        cache_bust=request.cache_bust,
        timing_recorder=timing_recorder,
    )
    record("pipeline_build_ms", started_at)

    metadata = dict(source_metadata or {})
    metadata.setdefault("pipeline_built_at", _now_iso())
    state = base_state
    if dependencies.fetch_current_prices is not None:
        started_at = perf_counter()
        tickers = list(current_price_tickers_for_state(base_state))
        record("price_ticker_resolution_ms", started_at)
        started_at = perf_counter()
        live_prices, price_errors, price_summary = dependencies.fetch_current_prices(tickers)
        record("price_fetch_ms", started_at)
        prices_updated_at = _now_iso()
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
        request=_request_dict(request),
        available_sheets=[str(sheet) for sheet in available_sheets] if available_sheets is not None else None,
        source_metadata=metadata,
        base_state=base_state,
    )


def build_mobile_dashboard_payload(
    context: MobilePayloadContext,
    *,
    target_return: float = 0.015,
) -> Dict[str, Any]:
    return build_mobile_dashboard(
        context.state,
        context.request,
        target_return=target_return,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_positions_payload(context: MobilePayloadContext) -> Dict[str, Any]:
    return build_mobile_positions(
        context.state,
        context.request,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_open_option_shorts_payload(
    context: MobilePayloadContext,
    *,
    sort: str = "moneyness_risk",
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_open_option_shorts(
        context.state,
        context.request,
        sort=sort,
        limit=limit,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_tickers_payload(
    context: MobilePayloadContext,
    *,
    year: Optional[int] = None,
    include_history: bool = False,
) -> Dict[str, Any]:
    return build_mobile_tickers(
        context.state,
        context.request,
        year=year,
        include_history=include_history,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_monthly_payload(
    context: MobilePayloadContext,
    *,
    target_return: float = 0.015,
    monthly_range: str = "ytd",
) -> Dict[str, Any]:
    return build_mobile_monthly_performance(
        context.state,
        context.request,
        target_return=target_return,
        monthly_range=monthly_range,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_yearly_payload(context: MobilePayloadContext) -> Dict[str, Any]:
    return build_mobile_yearly_performance(
        context.state,
        context.request,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_issues_payload(context: MobilePayloadContext) -> Dict[str, Any]:
    return build_mobile_issues(
        context.state,
        context.request,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )


def build_mobile_refresh_payload(
    context: MobilePayloadContext,
    *,
    cache_bust: Optional[int] = None,
) -> Dict[str, Any]:
    return build_mobile_refresh(
        context.state,
        context.request,
        cache_bust=cache_bust,
        available_sheets=context.available_sheets,
        source_metadata=context.source_metadata,
    )
