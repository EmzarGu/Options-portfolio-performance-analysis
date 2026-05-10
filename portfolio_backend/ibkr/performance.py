from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from typing import Iterable, Optional

import pandas as pd

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow
from portfolio_backend.ibkr.option_accounting import (
    IbkrOptionExecution,
    cashflow_summary,
    filter_executions,
    option_executions_from_report,
)


DIVIDEND_CASH_TYPES = {
    "Dividends",
    "Payment In Lieu Of Dividends",
    "Withholding Tax",
    "871(m) Withholding",
}
INTEREST_CASH_TYPES = {
    "Broker Interest Received",
    "Broker Interest Paid",
    "Bond Interest Received",
    "Bond Interest Paid",
}
FEE_CASH_TYPES = {
    "Other Fees",
    "Broker Fees",
    "Advisor Fees",
    "Commission Adjustments",
}


@dataclass(frozen=True)
class IbkrStockRealized:
    date: pd.Timestamp
    ticker: str
    side: str
    quantity: float
    proceeds: float
    commission: float
    realized_pnl: float
    trade_id: Optional[str]
    transaction_id: Optional[str]

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WheelStockMovement:
    date: pd.Timestamp
    ticker: str
    side: str
    shares: float
    price: float
    proceeds: float
    source: str
    trade_id: Optional[str]

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WheelHoldingSegment:
    ticker: str
    start: pd.Timestamp
    end: pd.Timestamp
    shares: float
    cost_per_share: float

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WheelStockRealized:
    date: pd.Timestamp
    ticker: str
    shares: float
    proceeds: float
    cost: float
    realized_pnl: float
    source: str

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class IbkrCashflow:
    date: pd.Timestamp
    ticker: str
    cash_type: str
    amount: float
    transaction_id: Optional[str]
    action_id: Optional[str]

    def as_dict(self) -> dict:
        return asdict(self)


def _blank_to_none(value) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _float_or_zero(value) -> float:
    text = _blank_to_none(value)
    if text is None:
        return 0.0
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return 0.0


def _date_or_nat(value):
    text = _blank_to_none(value)
    if text is None:
        return pd.NaT
    if len(text) == 8 and text.isdigit():
        return pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    if ";" in text:
        date_part = text.split(";", 1)[0]
        if len(date_part) == 8 and date_part.isdigit():
            return pd.to_datetime(date_part, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(text, errors="coerce")


def _dividend_per_share(description: str) -> Optional[float]:
    match = re.search(r"(?:DIVIDEND|DISTRIBUTION)\s+[A-Z]{3}\s+([0-9]+(?:\.[0-9]+)?)\s+PER SHARE", description or "", re.I)
    if not match:
        return None
    return float(match.group(1))


def stock_realized_from_rows(trade_rows: Iterable[IbkrRawRow]) -> list[IbkrStockRealized]:
    realized: list[IbkrStockRealized] = []
    for row in trade_rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "STK":
            continue
        side = str(attrs.get("buySell", "")).upper()
        if side != "SELL":
            continue
        date = _date_or_nat(attrs.get("tradeDate") or attrs.get("dateTime"))
        if pd.isna(date):
            continue
        realized_pnl = _float_or_zero(attrs.get("fifoPnlRealized"))
        realized.append(
            IbkrStockRealized(
                date=pd.to_datetime(date).normalize(),
                ticker=(_blank_to_none(attrs.get("symbol")) or "").upper(),
                side=side,
                quantity=abs(_float_or_zero(attrs.get("quantity"))),
                proceeds=_float_or_zero(attrs.get("proceeds")),
                commission=_float_or_zero(attrs.get("ibCommission")),
                realized_pnl=realized_pnl,
                trade_id=_blank_to_none(attrs.get("tradeID")),
                transaction_id=_blank_to_none(attrs.get("transactionID")),
            )
        )
    return sorted(realized, key=lambda row: (row.date, row.ticker, row.trade_id or ""))


def stock_realized_from_report(report: IbkrFlexReport) -> list[IbkrStockRealized]:
    return stock_realized_from_rows(report.rows("Trade"))


def wheel_stock_movements_from_rows(option_eae_rows: Iterable[IbkrRawRow]) -> list[WheelStockMovement]:
    rows = list(option_eae_rows)
    assignment_links = _wheel_assignment_stock_links(rows)
    movements: list[WheelStockMovement] = []
    for row in rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "STK":
            continue
        transaction_type = str(attrs.get("transactionType", "")).title()
        if transaction_type not in {"Buy", "Sell"}:
            continue
        date = _date_or_nat(attrs.get("date"))
        if pd.isna(date):
            continue
        shares = abs(_float_or_zero(attrs.get("quantity")))
        if shares <= 0:
            continue
        proceeds = _float_or_zero(attrs.get("proceeds"))
        price = abs(proceeds) / shares if shares else 0.0
        matched_shares = _consume_assignment_stock_link(
            assignment_links,
            date=pd.to_datetime(date).normalize(),
            ticker=(_blank_to_none(attrs.get("symbol")) or "").upper(),
            side="BUY" if transaction_type == "Buy" else "SELL",
            shares=shares,
            price=price,
        )
        if matched_shares <= 1e-9:
            continue
        ratio = matched_shares / shares
        movements.append(
            WheelStockMovement(
                date=pd.to_datetime(date).normalize(),
                ticker=(_blank_to_none(attrs.get("symbol")) or "").upper(),
                side="BUY" if transaction_type == "Buy" else "SELL",
                shares=matched_shares,
                price=price,
                proceeds=proceeds * ratio,
                source=f"Option Assignment {transaction_type}",
                trade_id=_blank_to_none(attrs.get("tradeID")),
            )
        )
    return sorted(movements, key=lambda row: (row.date, row.ticker, row.side, row.trade_id or ""))


def _stock_movement_trade_id_key(*, date: pd.Timestamp, ticker: str, side: str, trade_id: Optional[str]) -> Optional[tuple]:
    if not trade_id:
        return None
    return ("trade_id", date, ticker, side, trade_id)


def _stock_movement_terms_key(*, date: pd.Timestamp, ticker: str, side: str, shares: float, price: float) -> tuple:
    return ("terms", date, ticker, side, round(shares, 6), round(price, 6))


def _assignment_sell_duplicate_keys(movements: Iterable[WheelStockMovement]) -> tuple[set[tuple], set[tuple]]:
    trade_id_keys: set[tuple] = set()
    terms_keys: set[tuple] = set()
    for movement in movements:
        if movement.side != "SELL":
            continue
        trade_id_key = _stock_movement_trade_id_key(
            date=movement.date,
            ticker=movement.ticker,
            side=movement.side,
            trade_id=movement.trade_id,
        )
        if trade_id_key is not None:
            trade_id_keys.add(trade_id_key)
        else:
            terms_keys.add(
                _stock_movement_terms_key(
                    date=movement.date,
                    ticker=movement.ticker,
                    side=movement.side,
                    shares=movement.shares,
                    price=movement.price,
                )
            )
    return trade_id_keys, terms_keys


def manual_stock_sell_movements_from_rows(
    trade_rows: Iterable[IbkrRawRow],
    *,
    exclude_assignment_sell_trade_ids: Optional[set[tuple]] = None,
    exclude_assignment_sell_terms: Optional[set[tuple]] = None,
) -> list[WheelStockMovement]:
    movements: list[WheelStockMovement] = []
    exclude_assignment_sell_trade_ids = exclude_assignment_sell_trade_ids or set()
    exclude_assignment_sell_terms = exclude_assignment_sell_terms or set()
    for row in trade_rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "STK":
            continue
        if str(attrs.get("buySell", "")).upper() != "SELL":
            continue
        date = _date_or_nat(attrs.get("tradeDate") or attrs.get("dateTime"))
        if pd.isna(date):
            continue
        shares = abs(_float_or_zero(attrs.get("quantity")))
        if shares <= 1e-9:
            continue
        proceeds = _float_or_zero(attrs.get("netCash"))
        if abs(proceeds) <= 1e-9:
            proceeds = _float_or_zero(attrs.get("proceeds"))
        price = abs(proceeds) / shares if shares else 0.0
        ticker = (_blank_to_none(attrs.get("symbol")) or "").upper()
        trade_id = _blank_to_none(attrs.get("tradeID"))
        trade_id_key = _stock_movement_trade_id_key(
            date=pd.to_datetime(date).normalize(),
            ticker=ticker,
            side="SELL",
            trade_id=trade_id,
        )
        terms_key = _stock_movement_terms_key(
            date=pd.to_datetime(date).normalize(),
            ticker=ticker,
            side="SELL",
            shares=shares,
            price=price,
        )
        if trade_id_key in exclude_assignment_sell_trade_ids or terms_key in exclude_assignment_sell_terms:
            continue
        movements.append(
            WheelStockMovement(
                date=pd.to_datetime(date).normalize(),
                ticker=ticker,
                side="SELL",
                shares=shares,
                price=price,
                proceeds=proceeds,
                source="Manual Stock Sell",
                trade_id=trade_id,
            )
        )
    return sorted(movements, key=lambda row: (row.date, row.ticker, row.trade_id or ""))


def _wheel_assignment_stock_links(option_eae_rows: Iterable[IbkrRawRow]) -> list[dict]:
    links: list[dict] = []
    for row in option_eae_rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "OPT":
            continue
        if str(attrs.get("transactionType", "")).title() != "Assignment":
            continue
        put_call = str(attrs.get("putCall", "")).upper()
        if put_call not in {"P", "C"}:
            continue
        date = _date_or_nat(attrs.get("date"))
        if pd.isna(date):
            continue
        quantity = abs(_float_or_zero(attrs.get("quantity")))
        multiplier = _float_or_zero(attrs.get("multiplier")) or 100.0
        shares = quantity * multiplier
        if shares <= 1e-9:
            continue
        ticker = (_blank_to_none(attrs.get("underlyingSymbol")) or _blank_to_none(attrs.get("symbol")) or "").upper()
        links.append(
            {
                "date": pd.to_datetime(date).normalize(),
                "ticker": ticker,
                "side": "BUY" if put_call == "P" else "SELL",
                "shares": shares,
                "remaining": shares,
                "price": abs(_float_or_zero(attrs.get("strike"))),
            }
        )
    return links


def _consume_assignment_stock_link(
    links: list[dict],
    *,
    date: pd.Timestamp,
    ticker: str,
    side: str,
    shares: float,
    price: float,
) -> float:
    remaining = shares
    matched = 0.0
    for link in links:
        if remaining <= 1e-9:
            break
        if link["remaining"] <= 1e-9:
            continue
        if link["date"] != date or link["ticker"] != ticker or link["side"] != side:
            continue
        link_price = float(link.get("price") or 0.0)
        if link_price > 1e-9 and price > 1e-9 and abs(link_price - price) > 1e-6:
            continue
        take = min(remaining, link["remaining"])
        link["remaining"] -= take
        remaining -= take
        matched += take
    return matched


def wheel_stock_movements_from_report(report: IbkrFlexReport) -> list[WheelStockMovement]:
    assignment_movements = wheel_stock_movements_from_rows(report.rows("OptionEAE"))
    duplicate_trade_id_keys, duplicate_terms_keys = _assignment_sell_duplicate_keys(assignment_movements)
    return sorted(
        [
            *assignment_movements,
            *manual_stock_sell_movements_from_rows(
                report.rows("Trade"),
                exclude_assignment_sell_trade_ids=duplicate_trade_id_keys,
                exclude_assignment_sell_terms=duplicate_terms_keys,
            ),
        ],
        key=lambda row: (row.date, row.ticker, 0 if row.side == "BUY" else 1, row.trade_id or ""),
    )


def compute_wheel_stock_realized_and_segments(
    movements: Iterable[WheelStockMovement],
    *,
    as_of: Optional[pd.Timestamp] = None,
) -> tuple[list[WheelStockRealized], list[WheelHoldingSegment], list[str]]:
    cutoff = pd.to_datetime(as_of).normalize() if as_of is not None else pd.Timestamp.max.normalize()
    inventory: dict[str, list[dict]] = {}
    realized: list[WheelStockRealized] = []
    segments: list[WheelHoldingSegment] = []
    issues: list[str] = []
    for movement in sorted(movements, key=lambda row: (row.date, row.ticker, row.side)):
        if movement.date > cutoff:
            continue
        inventory.setdefault(movement.ticker, [])
        if movement.side == "BUY":
            inventory[movement.ticker].append(
                {
                    "date": movement.date,
                    "shares": movement.shares,
                    "cost_per_share": movement.price,
                }
            )
            continue

        remaining = movement.shares
        matched_proceeds = 0.0
        matched_cost = 0.0
        matched_shares = 0.0
        while remaining > 1e-9 and inventory[movement.ticker]:
            lot = inventory[movement.ticker][0]
            take = min(remaining, lot["shares"])
            matched_shares += take
            matched_proceeds += take * movement.price
            matched_cost += take * lot["cost_per_share"]
            segments.append(
                WheelHoldingSegment(
                    ticker=movement.ticker,
                    start=lot["date"],
                    end=movement.date,
                    shares=take,
                    cost_per_share=lot["cost_per_share"],
                )
            )
            lot["shares"] -= take
            remaining -= take
            if lot["shares"] <= 1e-9:
                inventory[movement.ticker].pop(0)
        if matched_shares > 1e-9:
            realized.append(
                WheelStockRealized(
                    date=movement.date,
                    ticker=movement.ticker,
                    shares=matched_shares,
                    proceeds=matched_proceeds,
                    cost=matched_cost,
                    realized_pnl=matched_proceeds - matched_cost,
                    source=movement.source,
                )
            )
        if remaining > 1e-9 and movement.source == "Manual Stock Sell":
            if matched_shares > 1e-9:
                issues.append(
                    f"Ignored {remaining:g} manually sold shares of {movement.ticker} on {movement.date.date()} "
                    "because no additional assignment-derived stock inventory was available."
                )
            continue
        if remaining > 1e-9:
            issues.append(
                f"Ignored {remaining:g} assigned-call sold shares of {movement.ticker} on {movement.date.date()} "
                "because no assignment-derived stock inventory was available."
            )

    for ticker, lots in inventory.items():
        for lot in lots:
            if lot["shares"] > 1e-9:
                segments.append(
                    WheelHoldingSegment(
                        ticker=ticker,
                        start=lot["date"],
                        end=cutoff,
                        shares=lot["shares"],
                        cost_per_share=lot["cost_per_share"],
                    )
                )
    return realized, segments, issues


def wheel_option_executions(
    executions: Iterable[IbkrOptionExecution],
    holding_segments: Iterable[WheelHoldingSegment],
) -> tuple[list[IbkrOptionExecution], list[IbkrOptionExecution], list[str]]:
    """Keep puts plus calls covered by prior put-assignment inventory.

    Wheel performance starts with short puts. Short calls are part of the
    strategy only when they are written against stock acquired through an
    assigned put. Calls without that inventory are covered-call trades, not
    wheel trades for this dashboard.
    """
    segments = list(holding_segments)
    included: list[IbkrOptionExecution] = []
    excluded: list[IbkrOptionExecution] = []
    issues: list[str] = []
    open_call_lots: dict[tuple, list[dict]] = {}
    excluded_call_lots: dict[tuple, list[dict]] = {}
    excluded_roll_groups: set[tuple] = set()

    def call_key(execution: IbkrOptionExecution) -> tuple:
        return (execution.ticker, execution.otype, execution.strike, execution.expiration)

    def roll_group_key(execution: IbkrOptionExecution) -> Optional[tuple]:
        group = _ibkr_roll_execution_group(execution.ib_exec_id)
        if group is None:
            return None
        return (execution.date, execution.ticker, execution.otype, group)

    def held_shares_for(execution: IbkrOptionExecution) -> float:
        return sum(
            segment.shares
            for segment in segments
            if segment.ticker == execution.ticker and segment.start <= execution.date <= segment.end
        )

    def expire_call_allocations(current_date: pd.Timestamp) -> None:
        for allocations in (open_call_lots, excluded_call_lots):
            for key, lots in list(allocations.items()):
                active_lots = [lot for lot in lots if lot["expiration"] >= current_date and lot["qty"] > 1e-9]
                if active_lots:
                    allocations[key] = active_lots
                else:
                    allocations.pop(key, None)

    def append_call_lot(allocations: dict[tuple, list[dict]], execution: IbkrOptionExecution) -> None:
        allocations.setdefault(call_key(execution), []).append(
            {
                "ticker": execution.ticker,
                "qty": execution.qty,
                "multiplier": execution.multiplier,
                "expiration": execution.expiration,
            }
        )

    def consume_call_lots(allocations: dict[tuple, list[dict]], execution: IbkrOptionExecution, qty: float) -> float:
        remaining = qty
        consumed = 0.0
        key = call_key(execution)
        buckets = allocations.get(key, [])
        while remaining > 1e-9 and buckets:
            lot = buckets[0]
            take = min(remaining, lot["qty"])
            consumed += take
            lot["qty"] -= take
            remaining -= take
            if lot["qty"] <= 1e-9:
                buckets.pop(0)
        if not buckets:
            allocations.pop(key, None)
        return consumed

    def mark_excluded_roll_replacement(execution: IbkrOptionExecution) -> None:
        group_key = roll_group_key(execution)
        if group_key is not None:
            excluded_roll_groups.add(group_key)

    def is_excluded_roll_replacement(execution: IbkrOptionExecution) -> bool:
        group_key = roll_group_key(execution)
        return group_key is not None and group_key in excluded_roll_groups

    def add_excluded_sell(execution: IbkrOptionExecution) -> None:
        excluded.append(execution)
        append_call_lot(excluded_call_lots, execution)

    def allocated_call_shares(ticker: str) -> float:
        return sum(
            lot["qty"] * lot["multiplier"]
            for lots in open_call_lots.values()
            for lot in lots
            if lot["ticker"] == ticker
        )

    def split_execution(
        execution: IbkrOptionExecution,
        include_qty: float,
    ) -> tuple[Optional[IbkrOptionExecution], Optional[IbkrOptionExecution]]:
        include_qty = min(max(include_qty, 0.0), execution.qty)
        if include_qty <= 1e-9:
            return None, execution
        if execution.qty - include_qty <= 1e-9:
            return execution, None
        ratio = include_qty / execution.qty
        return (
            replace(
                execution,
                qty=include_qty,
                net_cash=execution.net_cash * ratio,
                proceeds=execution.proceeds * ratio,
                commission=execution.commission * ratio,
            ),
            replace(
                execution,
                qty=execution.qty - include_qty,
                net_cash=execution.net_cash * (1 - ratio),
                proceeds=execution.proceeds * (1 - ratio),
                commission=execution.commission * (1 - ratio),
            ),
        )

    for execution in executions:
        if execution.otype == "Put":
            included.append(execution)
            continue
        if execution.otype != "Call":
            excluded.append(execution)
            continue

        expire_call_allocations(execution.date)
        action = execution.action
        open_close = execution.open_close

        if action == "Buy" and open_close == "C":
            included_qty = consume_call_lots(open_call_lots, execution, execution.qty)
            remaining = execution.qty - included_qty
            excluded_qty = consume_call_lots(excluded_call_lots, execution, remaining) if remaining > 1e-9 else 0.0
            unmatched_qty = execution.qty - included_qty - excluded_qty

            included_part, after_included = split_execution(execution, included_qty)
            if included_part is not None:
                included.append(included_part)
            after_excluded = after_included
            if excluded_qty > 1e-9:
                mark_excluded_roll_replacement(execution)
                excluded_part, after_excluded = split_execution(after_included or execution, excluded_qty)
                if excluded_part is not None:
                    excluded.append(excluded_part)
                    issues.append(
                        f"Excluded {excluded_part.qty:g} {execution.ticker} call close contracts on {execution.date.date()} "
                        "because they close a non-wheel call lot."
                    )
            if unmatched_qty > 1e-9:
                unmatched_part, _ = split_execution(after_excluded or execution, unmatched_qty)
                if unmatched_part is not None:
                    excluded.append(unmatched_part)
                    issues.append(
                        f"Excluded {unmatched_part.qty:g} {execution.ticker} call close contracts on {execution.date.date()} "
                        "because no included wheel call lot was open."
                    )
            continue

        if action != "Sell":
            excluded.append(execution)
            continue

        if open_close == "O" and is_excluded_roll_replacement(execution):
            add_excluded_sell(execution)
            issues.append(
                f"Excluded {execution.ticker} call roll replacement on {execution.date.date()} "
                "because the closed call lot was non-wheel."
            )
            continue

        held_shares = held_shares_for(execution)
        required_shares = execution.qty * execution.multiplier
        available_shares = max(held_shares - allocated_call_shares(execution.ticker), 0.0)
        if available_shares <= 1e-9:
            add_excluded_sell(execution)
            if held_shares <= 1e-9:
                issues.append(
                    f"Excluded {execution.ticker} call execution on {execution.date.date()} "
                    "because no prior put-assignment stock inventory was held."
                )
            else:
                issues.append(
                    f"Excluded {execution.ticker} call execution on {execution.date.date()} "
                    "because all assignment-derived stock inventory was already covering other calls."
                )
            continue

        include_qty = min(execution.qty, available_shares / execution.multiplier)
        included_part, excluded_part = split_execution(execution, include_qty)
        if included_part is not None:
            included.append(included_part)
            append_call_lot(open_call_lots, included_part)
        if excluded_part is None:
            continue

        add_excluded_sell(excluded_part)
        issues.append(
            f"Prorated {execution.ticker} call execution on {execution.date.date()} to {include_qty * execution.multiplier:g} "
            f"wheel-held shares out of {required_shares:g} required shares."
        )

    return included, excluded, issues


def _ibkr_roll_execution_group(ib_exec_id: Optional[str]) -> Optional[str]:
    text = str(ib_exec_id or "").strip()
    parts = text.split(".")
    if len(parts) < 4 or not parts[0] or not parts[1]:
        return None
    return ".".join(parts[:2])


def cashflows_from_rows(cash_rows: Iterable[IbkrRawRow]) -> list[IbkrCashflow]:
    cashflows: list[IbkrCashflow] = []
    for row in cash_rows:
        attrs = row.attrs
        date = _date_or_nat(attrs.get("dateTime") or attrs.get("reportDate"))
        if pd.isna(date):
            continue
        cashflows.append(
            IbkrCashflow(
                date=pd.to_datetime(date).normalize(),
                ticker=(_blank_to_none(attrs.get("symbol")) or _blank_to_none(attrs.get("underlyingSymbol")) or "").upper(),
                cash_type=_blank_to_none(attrs.get("type")) or "",
                amount=_float_or_zero(attrs.get("amount")),
                transaction_id=_blank_to_none(attrs.get("transactionID")),
                action_id=_blank_to_none(attrs.get("actionID")),
            )
        )
    return sorted(cashflows, key=lambda row: (row.date, row.cash_type, row.ticker, row.transaction_id or ""))


def cashflows_from_report(report: IbkrFlexReport) -> list[IbkrCashflow]:
    return cashflows_from_rows(report.rows("CashTransaction"))


def wheel_dividend_cashflows(
    cashflows: Iterable[IbkrCashflow],
    holding_segments: Iterable[WheelHoldingSegment],
    *,
    raw_cash_rows: Iterable[IbkrRawRow] = (),
) -> list[IbkrCashflow]:
    segments = list(holding_segments)
    raw_by_key = {
        (_blank_to_none(row.attrs.get("transactionID")), _blank_to_none(row.attrs.get("actionID"))): row.attrs
        for row in raw_cash_rows
    }
    grouped: dict[tuple, list[IbkrCashflow]] = {}
    for cashflow in cashflows:
        if cashflow.cash_type not in DIVIDEND_CASH_TYPES:
            continue
        key = (cashflow.date, cashflow.ticker, cashflow.action_id)
        grouped.setdefault(key, []).append(cashflow)

    included: list[IbkrCashflow] = []
    for (_date, ticker, _action_id), rows in grouped.items():
        if not ticker:
            continue
        # Prefer ex-date for eligibility when present, but keep payment date
        # for the cashflow date/year.
        eligibility_date = rows[0].date
        for row in rows:
            raw = raw_by_key.get((row.transaction_id, row.action_id), {})
            ex_date = _date_or_nat(raw.get("exDate"))
            if pd.notna(ex_date):
                eligibility_date = pd.to_datetime(ex_date).normalize()
                break
        held_shares = sum(
            segment.shares
            for segment in segments
            if segment.ticker == ticker and segment.start <= eligibility_date <= segment.end
        )
        if held_shares <= 1e-9:
            continue

        positive_rows = [row for row in rows if row.amount > 0]
        gross_total = sum(row.amount for row in positive_rows)
        allocated_gross = gross_total
        if positive_rows:
            per_share = None
            for row in positive_rows:
                raw = raw_by_key.get((row.transaction_id, row.action_id), {})
                per_share = _dividend_per_share(raw.get("description", ""))
                if per_share:
                    break
            if per_share:
                allocated_gross = min(gross_total, held_shares * per_share)
        ratio = 1.0 if gross_total <= 0 else min(1.0, allocated_gross / gross_total)
        for row in rows:
            amount = row.amount * ratio
            if abs(amount) <= 1e-9:
                continue
            included.append(
                IbkrCashflow(
                    date=row.date,
                    ticker=row.ticker,
                    cash_type=row.cash_type,
                    amount=amount,
                    transaction_id=row.transaction_id,
                    action_id=row.action_id,
                )
            )
    return sorted(included, key=lambda row: (row.date, row.ticker, row.cash_type, row.transaction_id or ""))


def _filter_by_date(rows, *, since: Optional[pd.Timestamp], through: Optional[pd.Timestamp]):
    since_ts = pd.to_datetime(since).normalize() if since is not None else None
    through_ts = pd.to_datetime(through).normalize() if through is not None else None
    out = []
    for row in rows:
        if since_ts is not None and row.date < since_ts:
            continue
        if through_ts is not None and row.date > through_ts:
            continue
        out.append(row)
    return out


def stock_realized_to_dataframe(rows: Iterable[IbkrStockRealized]) -> pd.DataFrame:
    return pd.DataFrame(
        [row.as_dict() for row in rows],
        columns=[
            "date",
            "ticker",
            "side",
            "quantity",
            "proceeds",
            "commission",
            "realized_pnl",
            "trade_id",
            "transaction_id",
        ],
    )


def cashflows_to_dataframe(rows: Iterable[IbkrCashflow]) -> pd.DataFrame:
    return pd.DataFrame(
        [row.as_dict() for row in rows],
        columns=["date", "ticker", "cash_type", "amount", "transaction_id", "action_id"],
    )


def yearly_performance_from_report(
    report: IbkrFlexReport,
    *,
    since: Optional[pd.Timestamp] = None,
    through: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    through_ts = pd.to_datetime(through).normalize() if through is not None else None
    wheel_movements = wheel_stock_movements_from_report(report)
    stock_realized_all, holding_segments, stock_issues = compute_wheel_stock_realized_and_segments(
        wheel_movements,
        as_of=through_ts,
    )
    option_executions_all = filter_executions(
        option_executions_from_report(report),
        since=since,
        through=through,
    )
    option_executions, _, call_issues = wheel_option_executions(option_executions_all, holding_segments)
    stock_realized = _filter_by_date(stock_realized_all, since=since, through=through)
    cashflows_all = cashflows_from_report(report)
    wheel_cashflows = wheel_dividend_cashflows(
        cashflows_all,
        holding_segments,
        raw_cash_rows=report.rows("CashTransaction"),
    )
    cashflows = _filter_by_date(wheel_cashflows, since=since, through=through)

    option_df = pd.DataFrame(cashflow_summary(option_executions)["by_year"])
    if option_df.empty:
        option_df = pd.DataFrame(columns=["year", "option_rows", "option_sell_cash", "option_buy_cash", "option_cashflow_pnl"])
    else:
        option_df = option_df.rename(
            columns={
                "rows": "option_rows",
                "sell_cash": "option_sell_cash",
                "buy_cash": "option_buy_cash",
                "net_cash": "option_cashflow_pnl",
            }
        )

    stock_df = stock_realized_to_dataframe(stock_realized)
    if stock_df.empty:
        stock_yearly = pd.DataFrame(columns=["year", "stock_sell_rows", "stock_realized_pnl"])
    else:
        stock_df["year"] = pd.to_datetime(stock_df["date"]).dt.year
        stock_yearly = (
            stock_df.groupby("year")
            .agg(stock_sell_rows=("ticker", "size"), stock_realized_pnl=("realized_pnl", "sum"))
            .reset_index()
        )

    cash_df = cashflows_to_dataframe(cashflows)
    if cash_df.empty:
        cash_yearly = pd.DataFrame(
            columns=["year", "dividends_gross", "payment_in_lieu", "withholding_tax", "dividends_net", "interest", "fees"]
        )
    else:
        cash_df["year"] = pd.to_datetime(cash_df["date"]).dt.year
        cash_df["dividends_gross"] = cash_df["amount"].where(cash_df["cash_type"].eq("Dividends"), 0.0)
        cash_df["payment_in_lieu"] = cash_df["amount"].where(cash_df["cash_type"].eq("Payment In Lieu Of Dividends"), 0.0)
        cash_df["withholding_tax"] = cash_df["amount"].where(cash_df["cash_type"].isin({"Withholding Tax", "871(m) Withholding"}), 0.0)
        cash_df["dividends_net_component"] = cash_df["amount"].where(cash_df["cash_type"].isin(DIVIDEND_CASH_TYPES), 0.0)
        cash_df["interest"] = cash_df["amount"].where(cash_df["cash_type"].isin(INTEREST_CASH_TYPES), 0.0)
        cash_df["fees"] = cash_df["amount"].where(cash_df["cash_type"].isin(FEE_CASH_TYPES), 0.0)
        cash_yearly = (
            cash_df.groupby("year")
            .agg(
                dividends_gross=("dividends_gross", "sum"),
                payment_in_lieu=("payment_in_lieu", "sum"),
                withholding_tax=("withholding_tax", "sum"),
                dividends_net=("dividends_net_component", "sum"),
                interest=("interest", "sum"),
                fees=("fees", "sum"),
            )
            .reset_index()
        )

    frames = [option_df, stock_yearly, cash_yearly]
    years = sorted({int(year) for frame in frames for year in frame.get("year", pd.Series(dtype=int)).dropna().tolist()})
    out = pd.DataFrame({"year": years})
    for frame in frames:
        if "year" in frame and not frame.empty:
            out = out.merge(frame, on="year", how="left")
    for col in (
        "option_rows",
        "option_sell_cash",
        "option_buy_cash",
        "option_cashflow_pnl",
        "stock_sell_rows",
        "stock_realized_pnl",
        "dividends_gross",
        "payment_in_lieu",
        "withholding_tax",
        "dividends_net",
        "interest",
        "fees",
    ):
        if col not in out.columns:
            out[col] = 0.0
    numeric_cols = [col for col in out.columns if col != "year"]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    count_cols = [col for col in out.columns if col.endswith("_rows")]
    for col in count_cols:
        out[col] = out[col].astype(int)
    out["realized_strategy_cash_pnl"] = (
        out.get("option_cashflow_pnl", 0.0)
        + out.get("stock_realized_pnl", 0.0)
        + out.get("dividends_net", 0.0)
    )
    out.attrs["stock_issues"] = stock_issues
    out.attrs["option_issues"] = call_issues
    return out.round(6)
