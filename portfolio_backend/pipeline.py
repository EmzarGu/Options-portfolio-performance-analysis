from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from portfolio_backend.calculations import (
    assess_capital_history_coverage,
    build_capital_timeline,
    build_holding_segments,
    build_option_trades,
    compute_stock_realized_and_inventory,
    process_option_positions,
)
from portfolio_backend.models import OptionLot, PipelineState
from portfolio_backend.performance import (
    build_chains,
    build_covered_return_series,
    build_dashboard_unrealized_adjusted_return_series,
    build_dashboard_unrealized_snapshot,
    build_per_ticker_totals,
    build_monthly_summary,
    build_yearly_with_dashboard_unrealized,
    calculate_performance_metrics,
    calculate_performance_metrics_if_complete,
    period_returns,
    per_ticker_yearly_from_realized,
    twr_annualized_by_year,
    yearly_summary_from_monthly,
)
from portfolio_backend.tables import build_open_options_frame


@dataclass(frozen=True)
class DividendCashflowResult:
    cashflows: pd.DataFrame
    coverage_complete: bool
    attempted_tickers: List[str]
    failed_tickers: List[str]
    errors: List[str]


def normalize_dividend_fetch_result(result) -> DividendCashflowResult:
    if hasattr(result, "cashflows"):
        return DividendCashflowResult(
            cashflows=getattr(result, "cashflows"),
            coverage_complete=bool(getattr(result, "coverage_complete", False)),
            attempted_tickers=list(getattr(result, "attempted_tickers", [])),
            failed_tickers=list(getattr(result, "failed_tickers", [])),
            errors=list(getattr(result, "errors", [])),
        )
    if isinstance(result, pd.DataFrame):
        return DividendCashflowResult(
            cashflows=result,
            coverage_complete=True,
            attempted_tickers=[],
            failed_tickers=[],
            errors=[],
        )
    return DividendCashflowResult(
        cashflows=pd.DataFrame(columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]),
        coverage_complete=False,
        attempted_tickers=[],
        failed_tickers=[],
        errors=["Dividend fetch returned an unexpected result shape."],
    )


def build_base_pipeline(
    sheet_id: str,
    as_of: date,
    selected_sheets: List[str],
    load_options_fn: Callable[[str, List[str]], pd.DataFrame],
    fetch_price_history_fn: Callable[[set, pd.Timestamp, pd.Timestamp], Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]],
    collect_dividend_cashflows_fn: Callable[[List, pd.Timestamp], Any],
    align_benchmarks_monthly_fn: Callable[[Dict[str, str], pd.DatetimeIndex], Dict[str, pd.Series]],
    cache_bust: int = 1,
    timing_recorder: Optional[Callable[[str, float], None]] = None,
    stock_txns_override_fn: Optional[Callable[[List, pd.Timestamp], Tuple[List, List[str]]]] = None,
    option_positions_override_fn: Optional[Callable[[List, pd.Timestamp], Tuple[List, List, List, List[str], List]]] = None,
) -> PipelineState:
    def record(phase: str, started_at: float) -> None:
        if timing_recorder is not None:
            timing_recorder(phase, (perf_counter() - started_at) * 1000)

    _ = cache_bust
    started_at = perf_counter()
    df_opts = load_options_fn(sheet_id, selected_sheets)
    record("pipeline_load_options_ms", started_at)

    started_at = perf_counter()
    sheet_counts = df_opts.groupby("source_sheet").size().rename("rows").reset_index()
    today_norm = pd.Timestamp.today().normalize()
    as_of_ts = min(pd.Timestamp(as_of), today_norm)
    issues: List[str] = []
    price_errors: List[str] = []
    historical_price_errors: List[str] = []

    df_opts = df_opts[df_opts["trans_date"] <= as_of_ts].copy()
    record("pipeline_prepare_options_ms", started_at)

    started_at = perf_counter()
    trades = build_option_trades(df_opts, issues)
    record("pipeline_build_option_trades_ms", started_at)

    started_at = perf_counter()
    if option_positions_override_fn is not None:
        realized_option_events, open_option_lots, stock_txns, trade_issues, all_option_lots = option_positions_override_fn(
            trades,
            as_of_ts,
        )
    else:
        realized_option_events, open_option_lots, stock_txns, trade_issues, all_option_lots = process_option_positions(trades, as_of_ts)
    issues.extend(trade_issues)
    if stock_txns_override_fn is not None:
        stock_txns, stock_override_issues = stock_txns_override_fn(stock_txns, as_of_ts)
        issues.extend(stock_override_issues)
    record("pipeline_process_option_positions_ms", started_at)

    started_at = perf_counter()
    realized_sales, ending_inventory = compute_stock_realized_and_inventory(stock_txns, issues)
    record("pipeline_stock_inventory_ms", started_at)

    started_at = perf_counter()
    chain_outcomes = build_chains(stock_txns, realized_option_events, as_of_ts)
    record("pipeline_chain_build_ms", started_at)

    start_date = df_opts["trans_date"].min() if not df_opts.empty else as_of_ts
    started_at = perf_counter()
    if pd.notna(start_date):
        price_history, historical_price_errors, historical_price_summary = fetch_price_history_fn(
            {t.ticker for t in stock_txns},
            pd.to_datetime(start_date).normalize(),
            as_of_ts.normalize(),
        )
    else:
        price_history, historical_price_errors, historical_price_summary = {}, [], {"requested": 0, "fetched": 0}
    record("pipeline_historical_price_fetch_ms", started_at)

    started_at = perf_counter()
    capital_daily = build_capital_timeline(all_option_lots, stock_txns, as_of_ts, df_opts, price_history)
    record("pipeline_capital_timeline_ms", started_at)

    started_at = perf_counter()
    holding_segments = build_holding_segments(stock_txns, as_of_ts)
    capital_history_state = assess_capital_history_coverage(holding_segments, price_history)
    record("pipeline_capital_coverage_ms", started_at)

    started_at = perf_counter()
    dividend_fetch_result = normalize_dividend_fetch_result(collect_dividend_cashflows_fn(stock_txns, as_of_ts))
    div_df = dividend_fetch_result.cashflows
    dividend_coverage_complete = bool(dividend_fetch_result.coverage_complete)
    dividend_attempted_tickers = dividend_fetch_result.attempted_tickers
    dividend_failed_tickers = dividend_fetch_result.failed_tickers
    dividend_affected_tickers = dividend_failed_tickers or dividend_attempted_tickers
    dividend_errors = dividend_fetch_result.errors
    dividend_summary = {
        "attempted": len(dividend_attempted_tickers),
        "failed": len(dividend_failed_tickers),
    }
    record("pipeline_dividend_fetch_ms", started_at)

    started_at = perf_counter()
    monthly_summary = build_monthly_summary(realized_option_events, realized_sales, capital_daily, div_df, as_of_ts)
    affected_months = capital_history_state["capital_history_affected_months"]
    if capital_history_state["capital_history_incomplete"] and not monthly_summary.empty:
        affected_month_mask = monthly_summary.index.isin(affected_months)
        for col in ("roac", "ropc"):
            if col in monthly_summary.columns:
                monthly_summary.loc[affected_month_mask, col] = np.nan
    monthly_returns = monthly_summary["roac"].dropna() if "roac" in monthly_summary else pd.Series(dtype=float)
    monthly_returns.index = pd.to_datetime(monthly_returns.index, errors="coerce")
    monthly_returns = monthly_returns[monthly_returns.index.notna()]
    covered_return_state = build_covered_return_series(
        monthly_returns,
        capital_history_state["capital_history_affected_months"],
    )
    monthly_returns_covered = covered_return_state["covered_returns"]
    monthly_returns_unrealized_adjusted = monthly_returns.copy()
    if "realized_options_pnl" in monthly_summary and "roac" in monthly_summary:
        monthly_returns_active = monthly_summary.loc[monthly_summary["realized_options_pnl"] != 0, "roac"].dropna()
    else:
        monthly_returns_active = pd.Series(dtype=float)

    open_options_df = build_open_options_frame(open_option_lots)
    record("pipeline_monthly_summary_ms", started_at)

    live_prices: Dict[str, float] = {}
    price_summary = {"stocks_requested": 0, "stocks_fetched": 0}
    inv_df = pd.DataFrame()
    per_ticker_unreal = pd.Series(dtype=float)
    total_unreal = 0.0
    stock_unreal = 0.0
    option_unreal = 0.0
    put_assignment_unreal = 0.0
    itm_put_cash_required = 0.0
    itm_put_market_value = 0.0
    itm_put_contracts = 0
    itm_put_shares = 0
    missing_required_price_tickers: List[str] = []
    unrealized_blocked = False
    monthly_returns_unrealized_adjusted = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        as_of_ts,
        False,
        total_unreal,
        unrealized_blocked,
    )

    if historical_price_errors:
        issues.extend([f"Historical price error: {e}" for e in historical_price_errors])
    if capital_history_state["capital_history_incomplete"]:
        capital_history_issue_text = "; ".join(
            f"{item['ticker']} ({pd.to_datetime(item['start_date']).date()} to {pd.to_datetime(item['end_date']).date()})"
            for item in capital_history_state["capital_history_coverage_issues"]
        )
        issues.append(
            "Historical capital price coverage incomplete: "
            + capital_history_issue_text
            + ". Denominator-based return metrics are suppressed for affected periods."
        )
    if not dividend_coverage_complete:
        if dividend_affected_tickers:
            issues.append(
                "Dividend data incomplete for "
                + ", ".join(dividend_affected_tickers)
                + ". Realized P&L and return metrics remain visible but may understate dividends."
            )
        else:
            issues.append(
                "Dividend data incomplete. Realized P&L and return metrics remain visible but may understate dividends."
            )
    if price_errors:
        issues.extend([f"Price error: {e}" for e in price_errors])

    started_at = perf_counter()
    yearly = yearly_summary_from_monthly(monthly_summary, capital_daily, as_of_ts)
    twr_annualized = twr_annualized_by_year(monthly_returns.dropna())
    if not twr_annualized.empty:
        yearly = yearly.merge(twr_annualized.rename("annualized_return_twr"), left_on="year", right_index=True, how="left")
    twr_annualized_unrealized_adjusted = (
        twr_annualized_by_year(monthly_returns_unrealized_adjusted.dropna())
        if hasattr(monthly_returns_unrealized_adjusted.index, "year")
        else pd.Series(dtype=float)
    )
    if not unrealized_blocked and not twr_annualized_unrealized_adjusted.empty:
        yearly = yearly.merge(
            twr_annualized_unrealized_adjusted.rename("annualized_return_twr_unrealized_adjusted"),
            left_on="year",
            right_index=True,
            how="left",
        )
    twr_active = twr_annualized_by_year(monthly_returns_active.dropna())
    if not twr_active.empty:
        yearly = yearly.merge(twr_active.rename("annualized_return_twr_active"), left_on="year", right_index=True, how="left")
    if capital_history_state["capital_history_incomplete"] and not yearly.empty:
        affected_years = set(capital_history_state["capital_history_affected_years"])
        affected_year_mask = yearly["year"].isin(affected_years)
        for col in (
            "roac_year",
            "ropc_year",
            "ann_roac",
            "ann_ropc",
            "annualized_return_twr",
            "annualized_return_twr_active",
            "annualized_return_twr_unrealized_adjusted",
        ):
            if col not in yearly.columns:
                yearly[col] = np.nan
            yearly.loc[affected_year_mask, col] = np.nan

    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        False,
        total_unreal,
        as_of_ts,
        unrealized_blocked,
    )
    record("pipeline_yearly_summary_ms", started_at)

    started_at = perf_counter()
    per_ticker = per_ticker_yearly_from_realized(realized_option_events, realized_sales, as_of_ts)
    per_ticker_totals = build_per_ticker_totals(per_ticker, per_ticker_unreal)

    cumulative_realized = float(monthly_summary["total_realized_pnl"].sum()) if not monthly_summary.empty else 0.0
    grand_total = cumulative_realized + total_unreal
    record("pipeline_per_ticker_summary_ms", started_at)

    started_at = perf_counter()
    benchmark_tickers = {"Cboe BXM": "^BXM", "PUTW ETF": "PUTW", "SCHD ETF": "SCHD"}
    strat_rets = monthly_returns_covered.copy()
    if not strat_rets.empty:
        strat_rets.index = pd.to_datetime(strat_rets.index).to_period("M").to_timestamp("M")
        strat_rets = strat_rets[strat_rets.index <= as_of_ts.normalize()]
    aligned_bench_returns = align_benchmarks_monthly_fn(
        benchmark_tickers,
        strat_rets.index if not strat_rets.empty else pd.DatetimeIndex([]),
    )
    benchmark_metrics_rows = []
    strat_for_metrics = strat_rets.tail(12) if not strat_rets.empty else strat_rets
    strat_full = calculate_performance_metrics(strat_rets)
    strat_risk = calculate_performance_metrics(strat_for_metrics)
    strategy_row = {"Series": "My Strategy", **strat_full, **period_returns(strat_rets)}
    for key in ["Volatility", "Sharpe", "Sortino", "Max Drawdown"]:
        if key in strat_risk:
            strategy_row[key] = strat_risk[key]
    benchmark_metrics_rows.append(strategy_row)
    for name, rets in aligned_bench_returns.items():
        rets = rets.copy()
        rets.index = pd.to_datetime(rets.index, errors="coerce")
        rets = rets[rets.index.notna()].sort_index()
        full = calculate_performance_metrics_if_complete(rets)
        risk_window = rets.tail(12)
        risk = calculate_performance_metrics_if_complete(risk_window)
        row = {"Series": name, **full, **period_returns(rets)}
        for key in ["Volatility", "Sharpe", "Sortino", "Max Drawdown"]:
            if key in risk:
                row[key] = risk[key]
        benchmark_metrics_rows.append(row)
    benchmark_metrics_df = pd.DataFrame(benchmark_metrics_rows)
    record("pipeline_benchmark_alignment_ms", started_at)

    return PipelineState(
        df_opts=df_opts,
        lots=all_option_lots,
        stock_txns=stock_txns,
        realized_sales=realized_sales,
        ending_inventory=ending_inventory,
        capital_daily=capital_daily,
        monthly_cycles=monthly_summary,
        monthly_returns_w_div=monthly_returns,
        monthly_returns_covered=monthly_returns_covered,
        monthly_returns_unrealized_adjusted=monthly_returns_unrealized_adjusted,
        monthly_returns_active=monthly_returns_active,
        open_options=open_options_df,
        live_prices=live_prices,
        inv_df=inv_df,
        total_unreal=total_unreal,
        option_unreal=option_unreal,
        stock_unreal=stock_unreal,
        put_assignment_unreal=put_assignment_unreal,
        itm_put_cash_required=itm_put_cash_required,
        itm_put_market_value=itm_put_market_value,
        itm_put_contracts=itm_put_contracts,
        itm_put_shares=itm_put_shares,
        advanced_unreal=per_ticker_unreal,
        yearly=yearly,
        yearly_with_unreal=yearly_with_unreal,
        per_ticker=per_ticker,
        div_df=div_df,
        as_of=as_of_ts,
        issues=issues,
        price_errors=price_errors,
        unrealized_blocked=unrealized_blocked,
        missing_required_price_tickers=missing_required_price_tickers,
        price_summary=price_summary,
        price_updated_at=None,
        historical_price_summary=historical_price_summary,
        historical_price_errors=historical_price_errors,
        dividend_coverage_complete=dividend_coverage_complete,
        dividend_attempted_tickers=dividend_attempted_tickers,
        dividend_failed_tickers=dividend_failed_tickers,
        dividend_affected_tickers=dividend_affected_tickers,
        dividend_errors=dividend_errors,
        dividend_summary=dividend_summary,
        stock_prices=live_prices,
        benchmark_metrics=benchmark_metrics_df,
        aligned_bench_returns=aligned_bench_returns,
        per_ticker_totals=per_ticker_totals,
        grand_total=grand_total,
        cumulative_realized=cumulative_realized,
        realized_option_events=realized_option_events,
        chain_outcomes=chain_outcomes,
        sheet_counts=sheet_counts,
        capital_history_incomplete=capital_history_state["capital_history_incomplete"],
        capital_history_coverage_issues=capital_history_state["capital_history_coverage_issues"],
        capital_history_affected_months=capital_history_state["capital_history_affected_months"],
        capital_history_affected_years=capital_history_state["capital_history_affected_years"],
        capital_history_affected_tickers=capital_history_state["capital_history_affected_tickers"],
        first_incomplete_return_month=covered_return_state["first_incomplete_month"],
        last_complete_return_month=covered_return_state["last_complete_month"],
        return_series_truncated=covered_return_state["truncated"],
    )


def apply_unrealized_adjusted_display(
    base_state: PipelineState,
    include_unrealized_current_year: bool,
) -> PipelineState:
    monthly_returns_unrealized_adjusted = build_dashboard_unrealized_adjusted_return_series(
        base_state.monthly_returns_w_div,
        base_state.capital_daily,
        base_state.as_of,
        include_unrealized_current_year,
        base_state.total_unreal,
        base_state.unrealized_blocked,
    )

    yearly = base_state.yearly.drop(columns=["annualized_return_twr_unrealized_adjusted"], errors="ignore").copy()
    twr_annualized_unrealized_adjusted = (
        twr_annualized_by_year(monthly_returns_unrealized_adjusted.dropna())
        if hasattr(monthly_returns_unrealized_adjusted.index, "year")
        else pd.Series(dtype=float)
    )
    if not base_state.unrealized_blocked and not twr_annualized_unrealized_adjusted.empty:
        yearly["annualized_return_twr_unrealized_adjusted"] = yearly["year"].map(twr_annualized_unrealized_adjusted)
    elif include_unrealized_current_year:
        yearly["annualized_return_twr_unrealized_adjusted"] = np.nan

    adjusted_col = "annualized_return_twr_unrealized_adjusted"
    active_col = "annualized_return_twr_active"
    if adjusted_col in yearly.columns and active_col in yearly.columns:
        cols = [c for c in yearly.columns if c != adjusted_col]
        cols.insert(cols.index(active_col), adjusted_col)
        yearly = yearly[cols]

    if base_state.capital_history_incomplete and not yearly.empty:
        affected_years = set(base_state.capital_history_affected_years)
        affected_year_mask = yearly["year"].isin(affected_years)
        for col in (
            "roac_year",
            "ropc_year",
            "ann_roac",
            "ann_ropc",
            "annualized_return_twr",
            "annualized_return_twr_active",
            "annualized_return_twr_unrealized_adjusted",
        ):
            if col not in yearly.columns:
                yearly[col] = np.nan
            yearly.loc[affected_year_mask, col] = np.nan

    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        include_unrealized_current_year,
        base_state.total_unreal,
        base_state.as_of,
        base_state.unrealized_blocked,
    )

    return replace(
        base_state,
        monthly_returns_unrealized_adjusted=monthly_returns_unrealized_adjusted,
        yearly=yearly,
        yearly_with_unreal=yearly_with_unreal,
    )


def open_option_lots_for_state(state: PipelineState) -> List[OptionLot]:
    return [lot for lot in state.lots if lot.close_date is None]


def current_price_tickers_for_state(state: PipelineState) -> Tuple[str, ...]:
    open_option_lots = open_option_lots_for_state(state)
    tickers = {lot.ticker for lot in state.ending_inventory}
    tickers.update(lot.ticker for lot in open_option_lots)
    return tuple(sorted(tickers))


def issues_without_current_price_messages(issues: List[str]) -> List[str]:
    current_price_prefixes = (
        "Price coverage incomplete:",
        "Current unrealized snapshot incomplete:",
        "Price error:",
    )
    return [issue for issue in issues if not str(issue).startswith(current_price_prefixes)]


def apply_live_price_overlay(
    base_state: PipelineState,
    live_prices: Dict[str, float],
    stock_price_errors: List[str],
    stock_summary: Dict[str, int],
    price_updated_at: str | None,
) -> PipelineState:
    price_errors = list(stock_price_errors)
    price_summary = {
        "stocks_requested": stock_summary.get("requested", 0),
        "stocks_fetched": stock_summary.get("fetched", 0),
    }

    open_option_lots = open_option_lots_for_state(base_state)
    unrealized_snapshot = build_dashboard_unrealized_snapshot(
        open_option_lots,
        base_state.ending_inventory,
        live_prices,
    )
    inv_df = unrealized_snapshot["inv_df"]
    per_ticker_unreal = unrealized_snapshot["per_ticker_unreal"]
    total_unreal = unrealized_snapshot["total_unreal"]
    stock_unreal = unrealized_snapshot["stock_unreal"]
    option_unreal = unrealized_snapshot["option_unreal"]
    put_assignment_unreal = unrealized_snapshot["put_assignment_unreal"]
    itm_put_cash_required = unrealized_snapshot["itm_put_cash_required"]
    itm_put_market_value = unrealized_snapshot["itm_put_market_value"]
    itm_put_contracts = unrealized_snapshot["itm_put_contracts"]
    itm_put_shares = unrealized_snapshot["itm_put_shares"]
    missing_required_price_tickers = unrealized_snapshot["missing_required_price_tickers"]
    per_ticker_totals = build_per_ticker_totals(base_state.per_ticker, per_ticker_unreal)
    grand_total = base_state.cumulative_realized + total_unreal

    issues = issues_without_current_price_messages(base_state.issues)
    if price_summary["stocks_fetched"] < price_summary["stocks_requested"]:
        issues.append(
            f"Price coverage incomplete: Stocks priced: {price_summary['stocks_fetched']}/{price_summary['stocks_requested']}"
        )
    if missing_required_price_tickers:
        issues.append(
            "Current unrealized snapshot incomplete: missing required prices for "
            + ", ".join(missing_required_price_tickers)
        )
    if price_errors:
        issues.extend([f"Price error: {e}" for e in price_errors])

    return replace(
        base_state,
        live_prices=live_prices,
        inv_df=inv_df,
        total_unreal=total_unreal,
        option_unreal=option_unreal,
        stock_unreal=stock_unreal,
        put_assignment_unreal=put_assignment_unreal,
        itm_put_cash_required=itm_put_cash_required,
        itm_put_market_value=itm_put_market_value,
        itm_put_contracts=itm_put_contracts,
        itm_put_shares=itm_put_shares,
        advanced_unreal=per_ticker_unreal,
        issues=issues,
        price_errors=price_errors,
        unrealized_blocked=unrealized_snapshot["unrealized_blocked"],
        missing_required_price_tickers=missing_required_price_tickers,
        price_summary=price_summary,
        price_updated_at=price_updated_at,
        stock_prices=live_prices,
        per_ticker_totals=per_ticker_totals,
        grand_total=grand_total,
    )


def build_pipeline_without_live_prices(
    sheet_id: str,
    as_of: date,
    include_unrealized_current_year: bool,
    selected_sheets: List[str],
    load_options_fn: Callable[[str, List[str]], pd.DataFrame],
    fetch_price_history_fn: Callable[[set, pd.Timestamp, pd.Timestamp], Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]],
    collect_dividend_cashflows_fn: Callable[[List, pd.Timestamp], Any],
    align_benchmarks_monthly_fn: Callable[[Dict[str, str], pd.DatetimeIndex], Dict[str, pd.Series]],
    cache_bust: int = 1,
) -> PipelineState:
    base_state = build_base_pipeline(
        sheet_id,
        as_of,
        selected_sheets,
        load_options_fn,
        fetch_price_history_fn,
        collect_dividend_cashflows_fn,
        align_benchmarks_monthly_fn,
        cache_bust=cache_bust,
    )
    return apply_unrealized_adjusted_display(base_state, include_unrealized_current_year)
