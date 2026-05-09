from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from portfolio_backend.calculations import build_holding_segments
from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow
from portfolio_backend.ibkr.option_accounting import (
    IbkrOptionExecution,
    filter_executions,
    option_executions_from_report,
)
from portfolio_backend.ibkr.performance import (
    cashflows_from_report,
    wheel_dividend_cashflows,
    wheel_option_executions,
    wheel_stock_movements_from_report,
    compute_wheel_stock_realized_and_segments,
)
from portfolio_backend.ibkr.source_adapter import assignment_keys
from portfolio_backend.models import PipelineState, StockTxn
from portfolio_backend.pipeline import (
    apply_live_price_overlay,
    apply_unrealized_adjusted_display,
    build_base_pipeline,
    current_price_tickers_for_state,
)


FetchPriceHistoryFn = Callable[[set, pd.Timestamp, pd.Timestamp], Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]]
AlignBenchmarksMonthlyFn = Callable[[Dict[str, str], pd.DatetimeIndex], Dict[str, pd.Series]]
FetchCurrentPricesFn = Callable[[List[str]], Tuple[Dict[str, float], List[str], Dict[str, int]]]


def _execution_identity(execution: IbkrOptionExecution) -> tuple[Optional[str], Optional[str], Optional[str]]:
    return execution.trade_id, execution.transaction_id, execution.ib_exec_id


def _row_identity(row: IbkrRawRow) -> tuple[Optional[str], Optional[str], Optional[str]]:
    attrs = row.attrs
    return (
        _blank_or_none(attrs.get("tradeID")),
        _blank_or_none(attrs.get("transactionID")),
        _blank_or_none(attrs.get("ibExecID")),
    )


def _blank_or_none(value) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def wheel_option_trade_rows_from_report(
    report: IbkrFlexReport,
    *,
    since: Optional[pd.Timestamp] = None,
    through: Optional[pd.Timestamp] = None,
) -> tuple[list[IbkrRawRow], list[str]]:
    """Return short-option Trade rows that belong to the wheel strategy.

    Puts are retained as wheel starters. Calls are retained only when they are
    backed by stock acquired through a prior put assignment. The returned rows
    are still raw IBKR rows, so downstream adapters can preserve existing app
    semantics for lifecycle realized/open option handling.
    """
    wheel_movements = wheel_stock_movements_from_report(report)
    _, holding_segments, stock_issues = compute_wheel_stock_realized_and_segments(
        wheel_movements,
        as_of=pd.to_datetime(through).normalize() if through is not None else None,
    )
    executions = filter_executions(
        option_executions_from_report(report),
        since=since,
        through=through,
    )
    included, _, option_issues = wheel_option_executions(executions, holding_segments)
    included_ids = {_execution_identity(execution) for execution in included}
    return (
        [
            row
            for row in report.rows("Trade")
            if row.attrs.get("assetCategory") == "OPT" and _row_identity(row) in included_ids
        ],
        [*stock_issues, *option_issues],
    )


def wheel_options_dataframe_from_report(
    report: IbkrFlexReport,
    *,
    since: Optional[pd.Timestamp] = None,
    through: Optional[pd.Timestamp] = None,
) -> tuple[pd.DataFrame, list[str]]:
    wheel_movements = wheel_stock_movements_from_report(report)
    _, holding_segments, stock_issues = compute_wheel_stock_realized_and_segments(
        wheel_movements,
        as_of=pd.to_datetime(through).normalize() if through is not None else None,
    )
    executions = filter_executions(
        option_executions_from_report(report),
        since=since,
        through=through,
    )
    included, _, option_issues = wheel_option_executions(executions, holding_segments)
    return _option_executions_to_dataframe(included, report.rows("OptionEAE")), [*stock_issues, *option_issues]


def _option_executions_to_dataframe(
    executions: Iterable[IbkrOptionExecution],
    option_eae_rows: Iterable[IbkrRawRow] = (),
) -> pd.DataFrame:
    assigned_keys = assignment_keys(option_eae_rows)
    rows = []
    for execution in executions:
        key = (
            execution.ticker,
            execution.otype,
            float(execution.strike),
            pd.to_datetime(execution.expiration).normalize(),
        )
        rows.append(
            {
                "trans_date": execution.date,
                "ticker": execution.ticker,
                "type": execution.otype,
                "action": execution.action,
                "expiration": execution.expiration,
                "strike": execution.strike,
                "qty": execution.qty,
                "amount": execution.proceeds,
                "commission": abs(execution.commission),
                "total_pnl": execution.net_cash,
                "assigned_flag": 1.0 if execution.action == "Sell" and key in assigned_keys else 0.0,
                "comment": _execution_comment(execution),
                "source_sheet": f"IBKR Flex {pd.to_datetime(execution.date).year}",
                "ibkr_trade_id": execution.trade_id,
                "ibkr_transaction_id": execution.transaction_id,
                "ibkr_exec_id": execution.ib_exec_id,
                "ibkr_open_close": execution.open_close,
                "ibkr_asset_category": "OPT",
            }
        )
    return pd.DataFrame(rows, columns=_option_dataframe_columns())


def _execution_comment(execution: IbkrOptionExecution) -> str:
    parts = [f"openCloseIndicator={execution.open_close}"]
    if execution.trade_id:
        parts.append(f"tradeID={execution.trade_id}")
    if execution.transaction_id:
        parts.append(f"transactionID={execution.transaction_id}")
    if execution.ib_exec_id:
        parts.append(f"ibExecID={execution.ib_exec_id}")
    return "; ".join(parts)


def _option_dataframe_columns() -> list[str]:
    return [
        "trans_date",
        "ticker",
        "type",
        "action",
        "expiration",
        "strike",
        "qty",
        "amount",
        "commission",
        "total_pnl",
        "assigned_flag",
        "comment",
        "source_sheet",
        "ibkr_trade_id",
        "ibkr_transaction_id",
        "ibkr_exec_id",
        "ibkr_open_close",
        "ibkr_asset_category",
    ]


def ibkr_dividend_cashflows_dataframe(
    report: IbkrFlexReport,
    stock_txns: Iterable[Any],
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    holding_segments = build_holding_segments(list(stock_txns), as_of)
    included = wheel_dividend_cashflows(
        cashflows_from_report(report),
        holding_segments,
        raw_cash_rows=report.rows("CashTransaction"),
    )
    rows = [
        {
            "ticker": row.ticker,
            "ex_date": row.date,
            "pay_date": row.date,
            "per_share": None,
            "shares": None,
            "cash": row.amount,
            "cash_type": row.cash_type,
            "transaction_id": row.transaction_id,
            "action_id": row.action_id,
        }
        for row in included
        if row.date <= as_of.normalize()
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "ticker",
            "ex_date",
            "pay_date",
            "per_share",
            "shares",
            "cash",
            "cash_type",
            "transaction_id",
            "action_id",
        ],
    )


def wheel_stock_transactions_from_report(
    report: IbkrFlexReport,
    *,
    as_of: pd.Timestamp,
) -> tuple[list[StockTxn], list[str]]:
    """Build wheel-only stock transactions from OptionEAE stock-side rows.

    The sheet-compatible option adapter can miss assignment stock when the
    original option trade predates the Flex window. OptionEAE stock movements
    are the authoritative source for assignment-derived stock ownership.
    """
    cutoff = pd.to_datetime(as_of).normalize()
    inventory: dict[str, float] = {}
    stock_txns: list[StockTxn] = []
    issues: list[str] = []
    movements = [
        movement
        for movement in wheel_stock_movements_from_report(report)
        if movement.date <= cutoff
    ]
    for movement in sorted(movements, key=lambda row: (row.date, row.ticker, row.side, row.trade_id or "")):
        shares = int(round(movement.shares))
        if shares <= 0:
            continue
        if movement.side == "BUY":
            inventory[movement.ticker] = inventory.get(movement.ticker, 0.0) + shares
            stock_txns.append(
                StockTxn(
                    movement.date,
                    movement.ticker,
                    "BUY",
                    shares,
                    movement.price,
                    "Assigned Put",
                )
            )
            continue

        available = inventory.get(movement.ticker, 0.0)
        matched = int(round(min(shares, available)))
        if matched > 0:
            inventory[movement.ticker] = available - matched
            stock_txns.append(
                StockTxn(
                    movement.date,
                    movement.ticker,
                    "SELL",
                    matched,
                    movement.price,
                    "Assigned Call",
                )
            )
        unmatched = shares - matched
        if unmatched > 0:
            issues.append(
                f"Ignored {unmatched:g} assigned-call sold shares of {movement.ticker} on {movement.date.date()} "
                "because no assignment-derived stock inventory was available."
            )
    return stock_txns, issues


def build_ibkr_base_pipeline(
    report: IbkrFlexReport,
    *,
    as_of: date,
    fetch_price_history_fn: FetchPriceHistoryFn,
    align_benchmarks_monthly_fn: AlignBenchmarksMonthlyFn,
    selected_sheets: Optional[List[str]] = None,
    cache_bust: int = 1,
) -> PipelineState:
    as_of_ts = pd.Timestamp(as_of).normalize()
    selected = selected_sheets or ["IBKR Flex"]
    df_opts, wheel_issues = wheel_options_dataframe_from_report(report, through=as_of_ts)

    def load_options(_sheet_id: str, _selected_sheets: List[str]) -> pd.DataFrame:
        return df_opts.copy()

    def collect_dividends(stock_txns, dividend_as_of: pd.Timestamp) -> pd.DataFrame:
        return ibkr_dividend_cashflows_dataframe(report, stock_txns, pd.to_datetime(dividend_as_of).normalize())

    def override_stock_txns(_stock_txns, stock_as_of: pd.Timestamp):
        return wheel_stock_transactions_from_report(report, as_of=stock_as_of)

    state = build_base_pipeline(
        "ibkr-flex",
        as_of,
        selected,
        load_options,
        fetch_price_history_fn,
        collect_dividends,
        align_benchmarks_monthly_fn,
        cache_bust=cache_bust,
        stock_txns_override_fn=override_stock_txns,
    )
    issues = [*state.issues]
    for issue in wheel_issues:
        if issue not in issues:
            issues.append(issue)
    sheet_counts = pd.DataFrame(
        [{"source_sheet": selected[0] if selected else "IBKR Flex", "rows": int(len(state.df_opts))}]
    )
    return _replace_state(state, issues=issues, sheet_counts=sheet_counts)


def build_ibkr_pipeline(
    report: IbkrFlexReport,
    *,
    as_of: date,
    include_unrealized_current_year: bool,
    fetch_price_history_fn: FetchPriceHistoryFn,
    align_benchmarks_monthly_fn: AlignBenchmarksMonthlyFn,
    fetch_current_prices_fn: Optional[FetchCurrentPricesFn] = None,
    selected_sheets: Optional[List[str]] = None,
    cache_bust: int = 1,
    price_updated_at: Optional[str] = None,
) -> PipelineState:
    state = build_ibkr_base_pipeline(
        report,
        as_of=as_of,
        fetch_price_history_fn=fetch_price_history_fn,
        align_benchmarks_monthly_fn=align_benchmarks_monthly_fn,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
    )
    if fetch_current_prices_fn is not None:
        tickers = list(current_price_tickers_for_state(state))
        live_prices, price_errors, price_summary = fetch_current_prices_fn(tickers)
        state = apply_live_price_overlay(
            state,
            live_prices,
            price_errors,
            price_summary,
            price_updated_at,
        )
    return apply_unrealized_adjusted_display(state, include_unrealized_current_year)


def _replace_state(state: PipelineState, **changes) -> PipelineState:
    from dataclasses import replace

    return replace(state, **changes)
