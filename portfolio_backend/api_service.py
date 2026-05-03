from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from portfolio_backend.api_payloads import build_portfolio_payload
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


@dataclass(frozen=True)
class PortfolioPayloadRequest:
    sheet_id: str
    as_of: date
    selected_sheets: List[str]
    include_unrealized_current_year: bool
    cache_bust: int = 1
    price_updated_at: Optional[str] = None


@dataclass(frozen=True)
class PortfolioServiceDependencies:
    load_options: LoadOptionsFn
    fetch_price_history: FetchPriceHistoryFn
    collect_dividend_cashflows: CollectDividendCashflowsFn
    align_benchmarks_monthly: AlignBenchmarksMonthlyFn
    fetch_current_prices: Optional[FetchCurrentPricesFn] = None


def build_payload_for_request(
    request: PortfolioPayloadRequest,
    dependencies: PortfolioServiceDependencies,
) -> Dict[str, Any]:
    base_state = build_base_pipeline(
        request.sheet_id,
        request.as_of,
        request.selected_sheets,
        dependencies.load_options,
        dependencies.fetch_price_history,
        dependencies.collect_dividend_cashflows,
        dependencies.align_benchmarks_monthly,
        cache_bust=request.cache_bust,
    )

    state = base_state
    if dependencies.fetch_current_prices is not None:
        tickers = list(current_price_tickers_for_state(base_state))
        live_prices, price_errors, price_summary = dependencies.fetch_current_prices(tickers)
        price_updated_at = request.price_updated_at or datetime.now().strftime("%H:%M:%S")
        state = apply_live_price_overlay(
            base_state,
            live_prices,
            price_errors,
            price_summary,
            price_updated_at,
        )

    state = apply_unrealized_adjusted_display(state, request.include_unrealized_current_year)
    return build_portfolio_payload(state, request.include_unrealized_current_year)
