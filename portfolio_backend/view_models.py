from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from portfolio_backend.models import PipelineState


@dataclass(frozen=True)
class DashboardViewModel:
    yearly: pd.DataFrame
    monthly_cycles: pd.DataFrame
    as_of_year: int
    realized_total: float
    ytd_total: float
    ytd_twr: Any
    issues: List[str]
    price_errors: List[str]
    unrealized_blocked: bool
    missing_required_price_tickers: List[str]
    price_summary: Dict[str, int]
    capital_history_incomplete: bool
    capital_history_coverage_issues: List[Dict[str, Any]]
    capital_history_affected_years: Set[int]
    dividend_coverage_complete: bool
    dividend_affected_tickers: List[str]
    dividend_errors: List[str]
    monthly_returns_covered: pd.Series
    first_incomplete_return_month: Optional[pd.Timestamp]
    last_complete_return_month: Optional[pd.Timestamp]
    return_series_truncated: bool
    covered_period_note: Optional[str]
    dividend_warning_note: Optional[str]


def _current_year_row(yearly: pd.DataFrame, as_of_year: int) -> pd.Series:
    if yearly is None or yearly.empty or "year" not in yearly.columns:
        rows = pd.DataFrame()
    else:
        rows = yearly.loc[yearly["year"] == as_of_year]
    if not rows.empty:
        return rows.iloc[0]
    return pd.Series(
        {
            "total_realized_pnl": 0.0,
            "ann_roac": pd.NA,
            "annualized_return_twr": pd.NA,
        }
    )


def build_dashboard_view_model(
    state: PipelineState,
    include_unrealized_current_year: bool,
) -> DashboardViewModel:
    yearly = state.yearly_with_unreal if include_unrealized_current_year else state.yearly
    monthly_cycles = state.monthly_cycles

    as_of_year = int(pd.to_datetime(state.as_of).year)
    ytd_row = _current_year_row(yearly, as_of_year)
    realized_total = float(ytd_row.get("total_realized_pnl", 0.0) or 0.0)
    ytd_total = realized_total + (state.total_unreal if include_unrealized_current_year else 0.0)
    twr_field = (
        "annualized_return_twr_unrealized_adjusted"
        if include_unrealized_current_year
        else "annualized_return_twr"
    )
    ytd_twr = ytd_row.get(twr_field, pd.NA)

    capital_history_incomplete = state.capital_history_incomplete
    monthly_returns_covered = state.monthly_returns_covered
    first_incomplete_return_month = state.first_incomplete_return_month
    last_complete_return_month = state.last_complete_return_month
    return_series_truncated = state.return_series_truncated
    covered_period_note = None
    if return_series_truncated and pd.notna(last_complete_return_month) and pd.notna(first_incomplete_return_month):
        covered_period_note = (
            "Return-based charts and benchmark metrics are shown through "
            f"{pd.to_datetime(last_complete_return_month).date()} only. "
            "Later periods are incomplete due to missing historical capital prices and are excluded."
        )
    elif capital_history_incomplete and monthly_returns_covered.empty:
        covered_period_note = (
            "No fully covered return period is available because historical capital price coverage is incomplete."
        )

    dividend_warning_note = None
    if not state.dividend_coverage_complete:
        if state.dividend_affected_tickers:
            dividend_warning_note = (
                "Dividend data is incomplete for "
                + ", ".join(state.dividend_affected_tickers)
                + ". Realized P&L and return metrics remain visible but may understate dividends."
            )
        else:
            dividend_warning_note = (
                "Dividend data is incomplete. Realized P&L and return metrics remain visible but may understate dividends."
            )

    return DashboardViewModel(
        yearly=yearly,
        monthly_cycles=monthly_cycles,
        as_of_year=as_of_year,
        realized_total=realized_total,
        ytd_total=ytd_total,
        ytd_twr=ytd_twr,
        issues=state.issues,
        price_errors=state.price_errors,
        unrealized_blocked=state.unrealized_blocked,
        missing_required_price_tickers=state.missing_required_price_tickers,
        price_summary=state.price_summary,
        capital_history_incomplete=capital_history_incomplete,
        capital_history_coverage_issues=state.capital_history_coverage_issues,
        capital_history_affected_years=set(state.capital_history_affected_years),
        dividend_coverage_complete=state.dividend_coverage_complete,
        dividend_affected_tickers=state.dividend_affected_tickers,
        dividend_errors=state.dividend_errors,
        monthly_returns_covered=monthly_returns_covered,
        first_incomplete_return_month=first_incomplete_return_month,
        last_complete_return_month=last_complete_return_month,
        return_series_truncated=return_series_truncated,
        covered_period_note=covered_period_note,
        dividend_warning_note=dividend_warning_note,
    )
