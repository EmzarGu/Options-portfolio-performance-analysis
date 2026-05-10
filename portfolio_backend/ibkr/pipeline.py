from __future__ import annotations

from collections import defaultdict
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
from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.models import OptionLot, OptionPnLEvent, PipelineState, StockTxn
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
        option_executions_from_report(report, short_strategy_only=False),
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
        option_executions_from_report(report, short_strategy_only=False),
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
                    "Manual Stock Sell" if movement.source == "Manual Stock Sell" else "Assigned Call",
                )
            )
        unmatched = shares - matched
        if unmatched > 0 and movement.source == "Manual Stock Sell":
            if matched > 0:
                issues.append(
                    f"Ignored {unmatched:g} manually sold shares of {movement.ticker} on {movement.date.date()} "
                    "because no additional assignment-derived stock inventory was available."
                )
            continue
        if unmatched > 0:
            issues.append(
                f"Ignored {unmatched:g} assigned-call sold shares of {movement.ticker} on {movement.date.date()} "
                "because no assignment-derived stock inventory was available."
            )
    return stock_txns, issues


def ibkr_option_positions_from_report(
    report: IbkrFlexReport,
    *,
    as_of: pd.Timestamp,
) -> tuple[list[OptionPnLEvent], list[OptionLot], list[StockTxn], list[str], list[OptionLot]]:
    wheel_movements = wheel_stock_movements_from_report(report)
    _, holding_segments, stock_issues = compute_wheel_stock_realized_and_segments(
        wheel_movements,
        as_of=pd.to_datetime(as_of).normalize(),
    )
    executions = filter_executions(
        option_executions_from_report(report, short_strategy_only=False),
        through=pd.to_datetime(as_of).normalize(),
    )
    included, _, option_issues = wheel_option_executions(executions, holding_segments)
    assigned = assignment_keys(report.rows("OptionEAE"))
    realized, open_lots, all_lots, processing_issues = _process_roll_adjusted_option_executions(
        included,
        assigned,
        pd.to_datetime(as_of).normalize(),
    )
    return realized, open_lots, [], [*stock_issues, *option_issues, *processing_issues], all_lots


def _process_roll_adjusted_option_executions(
    executions: Iterable[IbkrOptionExecution],
    assigned_keys: set[tuple],
    as_of: pd.Timestamp,
) -> tuple[list[OptionPnLEvent], list[OptionLot], list[OptionLot], list[str]]:
    ordered = sorted(
        executions,
        key=lambda item: (
            item.date,
            item.ticker,
            0 if item.action == "Buy" else 1,
            item.otype,
            item.expiration,
            item.strike,
            item.trade_id or "",
            item.transaction_id or "",
            item.ib_exec_id or "",
        ),
    )
    roll_allocations, rolled_sells = _plan_same_day_roll_allocations(ordered)
    open_map: dict[tuple, list[OptionLot]] = defaultdict(list)
    realized: list[OptionPnLEvent] = []
    all_lots: list[OptionLot] = []
    issues: list[str] = []

    def lot_key(execution: IbkrOptionExecution) -> tuple:
        return (execution.ticker, execution.otype, float(execution.strike), pd.to_datetime(execution.expiration).normalize())

    def is_assigned(execution: IbkrOptionExecution) -> bool:
        return lot_key(execution) in assigned_keys

    def snapshot(
        lot: OptionLot,
        *,
        qty: float | None = None,
        close_date: pd.Timestamp | None = None,
        close_price: float | None = None,
        close_reason: str | None = None,
    ) -> OptionLot:
        return OptionLot(
            ticker=lot.ticker,
            otype=lot.otype,
            strike=lot.strike,
            qty=int(round(lot.qty if qty is None else qty)),
            open_date=lot.open_date,
            expiration=lot.expiration,
            open_price=lot.open_price,
            comment=lot.comment,
            assigned=lot.assigned,
            close_date=close_date,
            close_price=close_price,
            close_reason=close_reason,
            roll_adjusted_open_price=lot.roll_adjusted_open_price,
        )

    def consume_roll_allocations(execution_index: int, qty: float) -> list[dict[str, float]]:
        remaining = qty
        consumed: list[dict[str, float]] = []
        allocations = roll_allocations.get(execution_index, [])
        while remaining > 1e-9 and allocations:
            allocation = allocations[0]
            take = min(remaining, allocation["qty"])
            ratio = take / allocation["qty"]
            net_cash = allocation["net_cash"] * ratio
            consumed.append({"sell_index": allocation["sell_index"], "qty": take, "net_cash": net_cash})
            allocation["qty"] -= take
            allocation["net_cash"] -= net_cash
            remaining -= take
            if allocation["qty"] <= 1e-9:
                allocations.pop(0)
        return consumed

    def contract_multiplier(execution: IbkrOptionExecution) -> float:
        return float(execution.multiplier or CONTRACT_MULTIPLIER)

    roll_adjusted_cash_by_sell_index: dict[int, float] = defaultdict(float)

    def add_open_lot(
        execution: IbkrOptionExecution,
        qty: float,
        net_cash: float,
        *,
        roll_adjusted_net_cash: float | None = None,
    ) -> None:
        if qty <= 1e-9:
            return
        multiplier = contract_multiplier(execution)
        roll_adjusted_cash = net_cash if roll_adjusted_net_cash is None else roll_adjusted_net_cash
        open_map[lot_key(execution)].append(
            OptionLot(
                ticker=execution.ticker,
                otype=execution.otype,
                strike=execution.strike,
                qty=int(round(qty)),
                open_date=execution.date,
                expiration=execution.expiration,
                open_price=net_cash / (qty * multiplier),
                comment=_execution_comment(execution),
                assigned=is_assigned(execution),
                roll_adjusted_open_price=roll_adjusted_cash / (qty * multiplier),
            )
        )

    for index, execution in enumerate(ordered):
        key = lot_key(execution)
        if execution.action == "Sell":
            rolled = rolled_sells.get(index, {"qty": 0.0, "net_cash": 0.0})
            rolled_qty = min(float(rolled["qty"]), execution.qty)
            add_open_lot(
                execution,
                rolled_qty,
                0.0,
                roll_adjusted_net_cash=roll_adjusted_cash_by_sell_index.get(index, 0.0),
            )
            residual_qty = execution.qty - rolled_qty
            residual_cash = execution.net_cash - float(rolled["net_cash"])
            add_open_lot(execution, residual_qty, residual_cash)
            continue

        qty_to_close = execution.qty
        buckets = open_map.get(key, [])
        if qty_to_close > 1e-9 and not buckets:
            issues.append(
                f"Buy {execution.ticker} {execution.otype} {execution.strike} on {execution.date.date()} had no open short to close."
            )
        while qty_to_close > 1e-9 and buckets:
            lot = buckets[0]
            take = min(qty_to_close, lot.qty)
            buy_cash = execution.net_cash * (take / execution.qty)
            roll_contributions = consume_roll_allocations(index, take)
            roll_credit = sum(contribution["net_cash"] for contribution in roll_contributions)
            multiplier = contract_multiplier(execution)
            pnl = lot.open_price * take * multiplier + buy_cash + roll_credit
            for contribution in roll_contributions:
                contribution_qty = contribution["qty"]
                contribution_cash = (
                    lot.open_price * contribution_qty * multiplier
                    + execution.net_cash * (contribution_qty / execution.qty)
                    + contribution["net_cash"]
                )
                roll_adjusted_cash_by_sell_index[int(contribution["sell_index"])] += contribution_cash
            close_debit_price = max(-buy_cash / (take * multiplier), 0.0)
            realized.append(
                OptionPnLEvent(
                    date=execution.date,
                    ticker=execution.ticker,
                    otype=execution.otype,
                    strike=execution.strike,
                    qty=int(round(take)),
                    pnl=pnl,
                    p_open=lot.open_price,
                    p_close=close_debit_price,
                    reason="close",
                )
            )
            all_lots.append(
                snapshot(
                    lot,
                    qty=take,
                    close_date=execution.date,
                    close_price=close_debit_price,
                    close_reason="close",
                )
            )
            lot.qty -= int(round(take))
            qty_to_close -= take
            if lot.qty <= 0:
                buckets.pop(0)
        if qty_to_close > 1e-9:
            issues.append(
                f"Unmatched buy quantity for {execution.ticker} {execution.otype} {execution.strike} "
                f"on {execution.date.date()}: {qty_to_close:g} remaining."
            )
        open_map[key] = buckets

    open_lots: list[OptionLot] = []
    for buckets in open_map.values():
        for lot in buckets:
            if as_of >= pd.to_datetime(lot.expiration).normalize():
                pnl = lot.open_price * lot.qty * CONTRACT_MULTIPLIER
                reason = "assignment" if lot.assigned else "expiration"
                realized.append(
                    OptionPnLEvent(
                        date=pd.to_datetime(lot.expiration).normalize(),
                        ticker=lot.ticker,
                        otype=lot.otype,
                        strike=lot.strike,
                        qty=lot.qty,
                        pnl=pnl,
                        p_open=lot.open_price,
                        p_close=0.0,
                        reason=reason,
                    )
                )
                all_lots.append(
                    snapshot(
                        lot,
                        close_date=pd.to_datetime(lot.expiration).normalize(),
                        close_price=0.0,
                        close_reason=reason,
                    )
                )
            else:
                open_snapshot = snapshot(lot)
                open_lots.append(open_snapshot)
                all_lots.append(open_snapshot)
    return (
        sorted(realized, key=lambda event: (event.date, event.ticker, event.otype, event.strike)),
        open_lots,
        all_lots,
        issues,
    )


def _plan_same_day_roll_allocations(
    executions: list[IbkrOptionExecution],
) -> tuple[dict[int, list[dict[str, float]]], dict[int, dict[str, float]]]:
    by_group: dict[tuple, list[tuple[int, IbkrOptionExecution]]] = defaultdict(list)
    for index, execution in enumerate(executions):
        exec_group = _ibkr_roll_execution_group(execution.ib_exec_id)
        if exec_group is None:
            continue
        by_group[(execution.date, execution.ticker, execution.otype, exec_group)].append((index, execution))

    roll_allocations: dict[int, list[dict[str, float]]] = defaultdict(list)
    rolled_sells: dict[int, dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "net_cash": 0.0})
    for rows in by_group.values():
        buys = [
            {"index": index, "execution": execution, "remaining": execution.qty}
            for index, execution in rows
            if execution.action == "Buy"
            and execution.open_close == "C"
            and abs(execution.net_cash) > 1e-9
            and execution.ib_exec_id
        ]
        sells = [
            {"index": index, "execution": execution, "remaining": execution.qty}
            for index, execution in rows
            if execution.action == "Sell" and execution.open_close == "O" and execution.ib_exec_id
        ]
        for buy in buys:
            for sell in sells:
                if buy["remaining"] <= 1e-9:
                    break
                if sell["remaining"] <= 1e-9:
                    continue
                take = min(buy["remaining"], sell["remaining"])
                sell_execution = sell["execution"]
                net_cash = sell_execution.net_cash * (take / sell_execution.qty)
                roll_allocations[buy["index"]].append(
                    {"sell_index": sell["index"], "qty": take, "net_cash": net_cash}
                )
                rolled_sells[sell["index"]]["qty"] += take
                rolled_sells[sell["index"]]["net_cash"] += net_cash
                buy["remaining"] -= take
                sell["remaining"] -= take
    return roll_allocations, rolled_sells


def _ibkr_roll_execution_group(ib_exec_id: Optional[str]) -> Optional[str]:
    """Return the stable IBKR combo execution prefix when present.

    IBKR combo/roll executions are commonly emitted as IDs such as
    ``00014247.691b2b75.03.01`` and ``00014247.691b2b75.02.01`` for the two
    legs. The first two dot-separated parts identify the shared execution
    group; the later parts identify the leg/fill. IDs without this structure
    are not strong enough evidence for roll netting.
    """
    text = str(ib_exec_id or "").strip()
    parts = text.split(".")
    if len(parts) < 4 or not parts[0] or not parts[1]:
        return None
    return ".".join(parts[:2])


def build_ibkr_base_pipeline(
    report: IbkrFlexReport,
    *,
    as_of: date,
    fetch_price_history_fn: FetchPriceHistoryFn,
    align_benchmarks_monthly_fn: AlignBenchmarksMonthlyFn,
    selected_sheets: Optional[List[str]] = None,
    cache_bust: int = 1,
    timing_recorder: Optional[Callable[[str, float], None]] = None,
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

    def override_option_positions(_trades, option_as_of: pd.Timestamp):
        return ibkr_option_positions_from_report(report, as_of=option_as_of)

    state = build_base_pipeline(
        "ibkr-flex",
        as_of,
        selected,
        load_options,
        fetch_price_history_fn,
        collect_dividends,
        align_benchmarks_monthly_fn,
        cache_bust=cache_bust,
        timing_recorder=timing_recorder,
        stock_txns_override_fn=override_stock_txns,
        option_positions_override_fn=override_option_positions,
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
    timing_recorder: Optional[Callable[[str, float], None]] = None,
) -> PipelineState:
    state = build_ibkr_base_pipeline(
        report,
        as_of=as_of,
        fetch_price_history_fn=fetch_price_history_fn,
        align_benchmarks_monthly_fn=align_benchmarks_monthly_fn,
        selected_sheets=selected_sheets,
        cache_bust=cache_bust,
        timing_recorder=timing_recorder,
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
