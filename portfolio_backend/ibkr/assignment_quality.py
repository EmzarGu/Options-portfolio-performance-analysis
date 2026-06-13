from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import pandas as pd

from portfolio_backend.ibkr.option_accounting import option_executions_from_report
from portfolio_backend.ibkr.performance import (
    compute_wheel_stock_realized_and_segments,
    wheel_option_executions,
    wheel_stock_movements_from_report,
)
from portfolio_backend.ibkr.pipeline import ibkr_option_positions_from_report


DEFAULT_OPPORTUNITY_MONTHLY_RATE = 0.015
MONTH_LENGTH_DAYS = 30.4375


@dataclass
class AssignmentQualityLot:
    lot_id: str
    ticker: str
    assignment_date: pd.Timestamp
    expiration: pd.Timestamp
    strike: float
    original_shares: float
    remaining_shares: float
    put_pnl: float
    stock_realized_pnl: float = 0.0
    stock_unrealized_pnl: float = 0.0
    call_cashflow_pnl: float = 0.0
    sale_shares: float = 0.0
    sale_proceeds: float = 0.0
    open_call_covered_shares: float = 0.0
    open_call_min_strike: Optional[float] = None
    sales: list[dict[str, Any]] = field(default_factory=list)

    @property
    def cost_basis(self) -> float:
        return self.original_shares * self.strike


def months_between(start: pd.Timestamp, end: pd.Timestamp) -> float:
    return max((pd.to_datetime(end).normalize() - pd.to_datetime(start).normalize()).days / MONTH_LENGTH_DAYS, 0.0)


def compounded_opportunity_cost(capital: float, monthly_rate: float, months: float) -> float:
    if capital <= 0 or monthly_rate <= 0 or months <= 0:
        return 0.0
    return float(capital * ((1.0 + monthly_rate) ** months - 1.0))


def decision_read(delta: float, assigned_capital: float) -> str:
    if delta <= -1500 or (assigned_capital > 0 and delta / assigned_capital <= -0.10):
        return "Graduate candidate"
    if delta > 0:
        return "Wheel worked better"
    return "Small difference"


def build_assignment_quality_analysis(
    report: Any,
    *,
    as_of: Any,
    prices: Optional[dict[str, float]] = None,
    historical_prices: Optional[dict[str, pd.Series]] = None,
    opportunity_monthly_rate: float = DEFAULT_OPPORTUNITY_MONTHLY_RATE,
    horizons_months: Iterable[int] = (6, 12, 18),
) -> dict[str, Any]:
    as_of_ts = pd.to_datetime(as_of).normalize()
    prices = {str(k).upper(): float(v) for k, v in (prices or {}).items() if v is not None}
    historical_prices = historical_prices or {}

    all_lots = _assignment_lots(report, as_of_ts)
    _allocate_stock_sales(all_lots, report, as_of_ts)
    call_allocations = _allocate_call_cashflows(all_lots, all_lots, report, as_of_ts)
    _apply_open_call_caps(all_lots, report, prices, as_of_ts)

    current_rows = [
        row
        for row in (
            _lot_result_row(
                lot,
                evaluation_date=as_of_ts,
                price=prices.get(lot.ticker),
                opportunity_monthly_rate=opportunity_monthly_rate,
                horizon_label="to_as_of",
                include_actual=True,
            )
            for lot in all_lots
        )
        if row is not None
    ]
    missing_current_price = sorted({lot.ticker for lot in all_lots if lot.ticker not in prices})

    cohort_rows = _cohort_rows(all_lots, current_rows, report, as_of_ts)
    by_ticker = _aggregate(cohort_rows, ["ticker"])
    by_year = _aggregate(cohort_rows, ["assignment_year"])
    summary = _summary(cohort_rows, total_lots=len(all_lots), missing_price_count=len(missing_current_price))

    current_frame = pd.DataFrame(current_rows)
    post_assignment_put_by_cohort = {
        (str(row["ticker"]), int(row["assignment_year"])): float(row.get("post_assignment_put_pnl") or 0.0)
        for row in cohort_rows
    }
    capital_by_cohort = (
        current_frame.groupby(["ticker", "assignment_year"])["assigned_capital"].sum().to_dict()
        if not current_frame.empty
        else {}
    )
    for row in current_rows:
        cohort_key = (str(row.get("ticker")), int(row.get("assignment_year")))
        cohort_capital = float(capital_by_cohort.get(cohort_key) or 0.0)
        extra_put_pnl = (
            float(post_assignment_put_by_cohort.get(cohort_key) or 0.0)
            * float(row.get("assigned_capital") or 0.0)
            / cohort_capital
            if cohort_capital > 0
            else 0.0
        )
        row["assigned_put_pnl"] = float(row.get("put_pnl") or 0.0)
        row["post_assignment_put_pnl"] = extra_put_pnl
        row["put_pnl"] = row["assigned_put_pnl"] + extra_put_pnl
        if row.get("actual_total_pnl") is not None:
            row["actual_total_pnl"] = float(row["actual_total_pnl"]) + extra_put_pnl
        if row.get("delta_actual_minus_hold") is not None:
            row["delta_actual_minus_hold"] = float(row["actual_total_pnl"]) - float(row["counterfactual_total_pnl"])
            row["decision_read"] = decision_read(float(row["delta_actual_minus_hold"]), float(row.get("assigned_capital") or 0.0))
    current_by_lot_id = {str(row.get("lot_id")): row for row in current_rows}
    horizon_rows: list[dict[str, Any]] = []
    horizon_detail: list[dict[str, Any]] = []
    for horizon in horizons_months:
        label = f"{int(horizon)}M"
        complete = 0
        incomplete = 0
        horizon_lot_rows: list[dict[str, Any]] = []
        for lot in all_lots:
            evaluation_date = lot.assignment_date + pd.DateOffset(months=int(horizon))
            if evaluation_date > as_of_ts:
                incomplete += 1
                continue
            price = _historical_price_at_or_before(historical_prices.get(lot.ticker), evaluation_date)
            if price is None:
                incomplete += 1
                continue
            complete += 1
            row = _lot_result_row(
                lot,
                evaluation_date=pd.to_datetime(evaluation_date).normalize(),
                price=price,
                opportunity_monthly_rate=opportunity_monthly_rate,
                horizon_label=label,
                include_actual=False,
            )
            if row is not None:
                current_actual = current_by_lot_id.get(str(row.get("lot_id"))) or {}
                for key in [
                    "assigned_put_pnl",
                    "post_assignment_put_pnl",
                    "put_pnl",
                    "call_cashflow_pnl",
                    "stock_realized_pnl",
                    "stock_unrealized_pnl",
                    "actual_stock_pnl",
                    "actual_total_pnl",
                ]:
                    row[key] = current_actual.get(key, row.get(key))
                actual = row.get("actual_total_pnl")
                hold = row.get("counterfactual_total_pnl")
                row["delta_actual_minus_hold"] = (
                    float(actual) - float(hold)
                    if actual is not None and hold is not None
                    else None
                )
                row["decision_read"] = (
                    decision_read(float(row["delta_actual_minus_hold"]), float(row.get("assigned_capital") or 0.0))
                    if row.get("delta_actual_minus_hold") is not None
                    else "Horizon only"
                )
                horizon_lot_rows.append(row)
        aggregate = _summary(horizon_lot_rows, total_lots=len(all_lots), missing_price_count=incomplete)
        aggregate.update(
            {
                "horizon": label,
                "horizon_months": int(horizon),
                "complete_lots": complete,
                "incomplete_lots": incomplete,
            }
        )
        horizon_rows.append(aggregate)
        horizon_detail.extend(horizon_lot_rows)

    current_horizon = dict(summary)
    current_horizon.update(
        {
            "horizon": "To current",
            "horizon_months": None,
            "complete_lots": len(current_rows),
            "incomplete_lots": max(len(all_lots) - len(current_rows), 0),
        }
    )
    by_horizon = [*horizon_rows, current_horizon]

    return {
        "summary": summary,
        "by_ticker": by_ticker,
        "by_assignment_year": by_year,
        "by_horizon": by_horizon,
        "cohort_detail": cohort_rows,
        "horizon_detail": horizon_detail,
        "lot_detail": current_rows,
        "call_allocations": call_allocations[:500],
        "open_call_caps": _open_call_cap_rows(current_rows),
        "controls": {
            "assignment_years": sorted({row["assignment_year"] for row in current_rows}),
            "horizons": ["to_as_of", *[f"{int(h)}M" for h in horizons_months]],
            "default_horizon": "to_as_of",
            "opportunity_monthly_rate": opportunity_monthly_rate,
        },
        "methodology": {
            "scope": "All assigned short puts available in the IBKR Flex report.",
            "period_definition": "Year filters use assignment year, not P&L recognition year.",
            "actual": "Assigned-put premium + post-assignment option cashflows + realized stock P&L + remaining assigned-stock value with open-call caps.",
            "hold": "Put premium + hold-stock P&L through evaluation date - opportunity cost.",
            "opportunity_cost": f"Assigned strike capital compounded at {opportunity_monthly_rate:.2%} per month for the hold period.",
            "dividends": "Excluded.",
        },
        "coverage": {
            "missing_current_price_tickers": missing_current_price,
            "historical_price_source": "optional price history series supplied by dashboard context",
        },
    }


def assignment_quality_tickers(report: Any, *, as_of: Any) -> list[str]:
    return sorted({lot.ticker for lot in _assignment_lots(report, pd.to_datetime(as_of).normalize())})


def _assignment_lots(report: Any, as_of: pd.Timestamp) -> list[AssignmentQualityLot]:
    _, _, _, _, all_option_lots = ibkr_option_positions_from_report(report, as_of=as_of)
    lots: list[AssignmentQualityLot] = []
    for lot in all_option_lots:
        if lot.otype != "Put" or not lot.assigned or lot.close_date is None:
            continue
        expiration = pd.to_datetime(lot.expiration).normalize()
        assignment_date = pd.to_datetime(lot.close_date or lot.expiration).normalize()
        ticker = str(lot.ticker).upper().strip()
        shares = abs(float(lot.qty)) * 100.0
        strike = float(lot.strike)
        if not ticker or pd.isna(expiration) or pd.isna(assignment_date) or shares <= 0 or strike <= 0:
            continue
        lot_id = f"{ticker}-{expiration.date()}-{strike:g}-{assignment_date.date()}-{len(lots)}"
        lots.append(
            AssignmentQualityLot(
                lot_id=lot_id,
                ticker=ticker,
                assignment_date=assignment_date,
                expiration=expiration,
                strike=strike,
                original_shares=shares,
                remaining_shares=shares,
                put_pnl=float(lot.open_price) * float(lot.qty) * 100.0,
            )
        )
    return sorted(lots, key=lambda x: (x.assignment_date, x.ticker, x.expiration, x.strike, x.lot_id))


def _allocate_stock_sales(all_lots: list[AssignmentQualityLot], report: Any, as_of: pd.Timestamp) -> None:
    movements = [m for m in wheel_stock_movements_from_report(report) if m.date <= as_of]
    lots_by_ticker: dict[str, list[AssignmentQualityLot]] = {}
    for lot in all_lots:
        lots_by_ticker.setdefault(lot.ticker, []).append(lot)
    for sale in [m for m in movements if m.side == "SELL"]:
        remaining = sale.shares
        for lot in lots_by_ticker.get(sale.ticker, []):
            if remaining <= 1e-9:
                break
            if lot.assignment_date > sale.date or lot.remaining_shares <= 1e-9:
                continue
            take = min(remaining, lot.remaining_shares)
            pnl = take * (sale.price - lot.strike)
            lot.remaining_shares -= take
            lot.sale_shares += take
            lot.sale_proceeds += take * sale.price
            lot.stock_realized_pnl += pnl
            lot.sales.append(
                {
                    "date": sale.date.date().isoformat(),
                    "shares": take,
                    "price": sale.price,
                    "source": sale.source,
                    "stock_pnl": pnl,
                }
            )
            remaining -= take


def _allocate_call_cashflows(
    in_scope_lots: list[AssignmentQualityLot],
    all_lots: list[AssignmentQualityLot],
    report: Any,
    as_of: pd.Timestamp,
) -> list[dict[str, Any]]:
    _, holding_segments, _ = compute_wheel_stock_realized_and_segments(
        wheel_stock_movements_from_report(report),
        as_of=as_of,
    )
    included, _, _ = wheel_option_executions(option_executions_from_report(report, short_strategy_only=False), holding_segments)
    call_rows: list[dict[str, Any]] = []
    for ex in included:
        if ex.otype != "Call" or ex.date > as_of:
            continue
        shares_covered = ex.qty * ex.multiplier
        if shares_covered <= 0:
            continue
        total_held = sum(
            lot.remaining_shares + sum(s["shares"] for s in lot.sales if pd.Timestamp(s["date"]) >= ex.date)
            for lot in all_lots
            if lot.ticker == ex.ticker and lot.assignment_date <= ex.date
        )
        selected_lots = [
            lot
            for lot in in_scope_lots
            if lot.ticker == ex.ticker
            and lot.assignment_date <= ex.date
            and (lot.remaining_shares > 1e-9 or any(pd.Timestamp(s["date"]) >= ex.date for s in lot.sales))
        ]
        selected_held = sum(
            lot.remaining_shares + sum(s["shares"] for s in lot.sales if pd.Timestamp(s["date"]) >= ex.date)
            for lot in selected_lots
        )
        if total_held <= 1e-9 or selected_held <= 1e-9:
            continue
        selected_cash = ex.net_cash * min(selected_held / total_held, 1.0)
        for lot in selected_lots:
            lot_held = lot.remaining_shares + sum(s["shares"] for s in lot.sales if pd.Timestamp(s["date"]) >= ex.date)
            alloc = selected_cash * lot_held / selected_held
            lot.call_cashflow_pnl += alloc
            call_rows.append(
                {
                    "date": ex.date.date().isoformat(),
                    "ticker": ex.ticker,
                    "action": ex.action,
                    "open_close": ex.open_close,
                    "expiration": ex.expiration.date().isoformat(),
                    "strike": ex.strike,
                    "qty": ex.qty,
                    "net_cash": ex.net_cash,
                    "allocated_to_lot": lot.lot_id,
                    "allocated_cashflow": alloc,
                }
            )
    return call_rows


def _apply_open_call_caps(
    all_lots: list[AssignmentQualityLot],
    report: Any,
    prices: dict[str, float],
    as_of: pd.Timestamp,
) -> None:
    _, open_lots, _, _, _ = ibkr_option_positions_from_report(report, as_of=as_of)
    open_calls = sorted(
        [lot for lot in open_lots if lot.otype == "Call" and lot.qty > 0],
        key=lambda lot: (lot.ticker, float(lot.strike), pd.to_datetime(lot.expiration)),
    )
    lots_by_ticker: dict[str, list[AssignmentQualityLot]] = {}
    covered_by_lot: dict[str, list[tuple[float, float]]] = {}
    for lot in all_lots:
        lot.stock_unrealized_pnl = 0.0
        lot.open_call_covered_shares = 0.0
        lot.open_call_min_strike = None
        lots_by_ticker.setdefault(lot.ticker, []).append(lot)
        covered_by_lot[lot.lot_id] = []
    for lots in lots_by_ticker.values():
        lots.sort(key=lambda lot: (lot.assignment_date, lot.expiration, lot.strike, lot.lot_id))

    for call in open_calls:
        ticker = str(call.ticker).upper().strip()
        shares_to_cover = abs(float(call.qty)) * 100.0
        for lot in lots_by_ticker.get(ticker, []):
            if shares_to_cover <= 1e-9:
                break
            already_covered = sum(shares for shares, _ in covered_by_lot[lot.lot_id])
            available = max(lot.remaining_shares - already_covered, 0.0)
            if available <= 1e-9:
                continue
            take = min(shares_to_cover, available)
            strike = float(call.strike)
            covered_by_lot[lot.lot_id].append((take, strike))
            lot.open_call_covered_shares += take
            lot.open_call_min_strike = strike if lot.open_call_min_strike is None else min(lot.open_call_min_strike, strike)
            shares_to_cover -= take

    for lot in all_lots:
        price = prices.get(lot.ticker)
        if price is None:
            continue
        pnl = 0.0
        covered_shares = 0.0
        for shares, call_strike in covered_by_lot.get(lot.lot_id, []):
            covered_shares += shares
            pnl += shares * (min(price, call_strike) - lot.strike)
        uncovered = max(lot.remaining_shares - covered_shares, 0.0)
        pnl += uncovered * (price - lot.strike)
        lot.stock_unrealized_pnl = pnl


def _lot_result_row(
    lot: AssignmentQualityLot,
    *,
    evaluation_date: pd.Timestamp,
    price: Optional[float],
    opportunity_monthly_rate: float,
    horizon_label: str,
    include_actual: bool,
) -> Optional[dict[str, Any]]:
    if price is None:
        return None
    months_held = months_between(lot.assignment_date, evaluation_date)
    opportunity_cost = compounded_opportunity_cost(lot.cost_basis, opportunity_monthly_rate, months_held)
    hold_stock_pnl = lot.original_shares * (float(price) - lot.strike)
    counterfactual_before_opportunity = lot.put_pnl + hold_stock_pnl
    counterfactual = counterfactual_before_opportunity - opportunity_cost
    actual = (
        lot.put_pnl + lot.call_cashflow_pnl + lot.stock_realized_pnl + lot.stock_unrealized_pnl
        if include_actual
        else None
    )
    delta = actual - counterfactual if actual is not None else None
    return {
        "lot_id": lot.lot_id,
        "ticker": lot.ticker,
        "assignment_date": lot.assignment_date.date().isoformat(),
        "assignment_year": int(lot.assignment_date.year),
        "expiration": lot.expiration.date().isoformat(),
        "strike": lot.strike,
        "original_shares": lot.original_shares,
        "remaining_shares": lot.remaining_shares,
        "sale_shares": lot.sale_shares,
        "open_call_covered_shares": lot.open_call_covered_shares,
        "open_call_min_strike": lot.open_call_min_strike,
        "put_pnl": lot.put_pnl,
        "call_cashflow_pnl": lot.call_cashflow_pnl if include_actual else 0.0,
        "stock_realized_pnl": lot.stock_realized_pnl if include_actual else 0.0,
        "stock_unrealized_pnl": lot.stock_unrealized_pnl if include_actual else 0.0,
        "actual_stock_pnl": (lot.stock_realized_pnl + lot.stock_unrealized_pnl) if include_actual else None,
        "hold_stock_pnl": hold_stock_pnl,
        "months_held": months_held,
        "opportunity_cost": opportunity_cost,
        "counterfactual_before_opportunity": counterfactual_before_opportunity,
        "counterfactual_total_pnl": counterfactual,
        "actual_total_pnl": actual,
        "delta_actual_minus_hold": delta,
        "assigned_capital": lot.cost_basis,
        "evaluation_date": evaluation_date.date().isoformat(),
        "evaluation_price": float(price),
        "horizon": horizon_label,
        "decision_read": decision_read(delta, lot.cost_basis) if delta is not None else "Horizon only",
        "sales_summary": "; ".join(f"{s['date']} {s['shares']:.0f}@{s['price']:.2f} {s['source']}" for s in lot.sales),
    }


def _aggregate(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    sum_cols = [
        "assigned_capital",
        "assigned_put_pnl",
        "post_assignment_put_pnl",
        "put_pnl",
        "call_cashflow_pnl",
        "stock_realized_pnl",
        "stock_unrealized_pnl",
        "actual_stock_pnl",
        "hold_stock_pnl",
        "opportunity_cost",
        "counterfactual_before_opportunity",
        "actual_total_pnl",
        "counterfactual_total_pnl",
        "delta_actual_minus_hold",
    ]
    lot_col = "lot_id" if "lot_id" in frame.columns else "lots"
    share_col = "original_shares" if "original_shares" in frame.columns else "assigned_shares"
    agg = frame.groupby(keys, as_index=False).agg(
        lots=(lot_col, "count" if lot_col == "lot_id" else "sum"),
        assigned_shares=(share_col, "sum"),
        **{col: (col, "sum") for col in sum_cols if col in frame.columns},
    )
    for _, row in agg.iterrows():
        pass
    result = agg.to_dict(orient="records")
    for row in result:
        row["decision_read"] = decision_read(float(row.get("delta_actual_minus_hold") or 0.0), float(row.get("assigned_capital") or 0.0))
    return result


def _cohort_rows(
    lots: list[AssignmentQualityLot],
    lot_rows: list[dict[str, Any]],
    report: Any,
    as_of: pd.Timestamp,
) -> list[dict[str, Any]]:
    if not lot_rows:
        return []
    lot_frame = pd.DataFrame(lot_rows)
    cohort_keys = (
        lot_frame.groupby(["ticker", "assignment_year"], as_index=False)
        .agg(inclusion_date=("assignment_date", "min"))
        .to_dict(orient="records")
    )
    cohorts = [
        {
            "ticker": str(row["ticker"]),
            "assignment_year": int(row["assignment_year"]),
            "inclusion_date": pd.to_datetime(row["inclusion_date"]).normalize(),
            "put_pnl": 0.0,
            "call_cashflow_pnl": 0.0,
        }
        for row in cohort_keys
    ]
    cohorts_by_ticker: dict[str, list[dict[str, Any]]] = {}
    for cohort in cohorts:
        cohorts_by_ticker.setdefault(cohort["ticker"], []).append(cohort)
    for ticker_cohorts in cohorts_by_ticker.values():
        ticker_cohorts.sort(key=lambda row: row["inclusion_date"])

    _, holding_segments, _ = compute_wheel_stock_realized_and_segments(
        wheel_stock_movements_from_report(report),
        as_of=as_of,
    )
    included, _, _ = wheel_option_executions(option_executions_from_report(report, short_strategy_only=False), holding_segments)
    for ex in included:
        if ex.date > as_of:
            continue
        ticker_cohorts = cohorts_by_ticker.get(str(ex.ticker).upper())
        if not ticker_cohorts:
            continue
        selected = None
        for cohort in ticker_cohorts:
            if cohort["inclusion_date"] <= ex.date:
                selected = cohort
            else:
                break
        if selected is None:
            continue
        if ex.otype == "Put":
            selected["put_pnl"] += ex.net_cash
        elif ex.otype == "Call":
            selected["call_cashflow_pnl"] += ex.net_cash

    result: list[dict[str, Any]] = []
    for cohort in cohorts:
        rows = lot_frame[
            (lot_frame["ticker"] == cohort["ticker"])
            & (lot_frame["assignment_year"] == cohort["assignment_year"])
        ]
        stock_realized = float(rows["stock_realized_pnl"].sum())
        stock_unrealized = float(rows["stock_unrealized_pnl"].sum())
        actual_stock = stock_realized + stock_unrealized
        assigned_put_pnl = float(rows["put_pnl"].sum())
        post_assignment_put_pnl = float(cohort["put_pnl"])
        total_put_pnl = assigned_put_pnl + post_assignment_put_pnl
        actual_total = float(total_put_pnl + cohort["call_cashflow_pnl"] + actual_stock)
        counterfactual = float(rows["counterfactual_total_pnl"].sum())
        assigned_capital = float(rows["assigned_capital"].sum())
        delta = actual_total - counterfactual
        result.append(
            {
                "ticker": cohort["ticker"],
                "assignment_year": cohort["assignment_year"],
                "inclusion_date": cohort["inclusion_date"].date().isoformat(),
                "lots": int(len(rows)),
                "assigned_shares": float(rows["original_shares"].sum()),
                "assigned_capital": assigned_capital,
                "assigned_put_pnl": assigned_put_pnl,
                "post_assignment_put_pnl": post_assignment_put_pnl,
                "put_pnl": total_put_pnl,
                "call_cashflow_pnl": float(cohort["call_cashflow_pnl"]),
                "stock_realized_pnl": stock_realized,
                "stock_unrealized_pnl": stock_unrealized,
                "actual_stock_pnl": actual_stock,
                "hold_stock_pnl": float(rows["hold_stock_pnl"].sum()),
                "opportunity_cost": float(rows["opportunity_cost"].sum()),
                "counterfactual_before_opportunity": float(rows["counterfactual_before_opportunity"].sum()),
                "actual_total_pnl": actual_total,
                "counterfactual_total_pnl": counterfactual,
                "delta_actual_minus_hold": delta,
                "decision_read": decision_read(delta, assigned_capital),
            }
        )
    return sorted(result, key=lambda row: (row["assignment_year"], row["ticker"]))


def _summary(rows: list[dict[str, Any]], *, total_lots: int, missing_price_count: int) -> dict[str, Any]:
    if not rows:
        return {
            "lots": 0,
            "total_lots": total_lots,
            "missing_price_count": missing_price_count,
            "actual_total_pnl": 0.0,
            "counterfactual_total_pnl": 0.0,
            "delta_actual_minus_hold": 0.0,
        }
    frame = pd.DataFrame(rows)
    lots = len(frame)
    actual_count = int(frame["actual_total_pnl"].notna().sum()) if "actual_total_pnl" in frame else 0
    return {
        "lots": int(lots),
        "total_lots": int(total_lots),
        "complete_lots": int(lots),
        "missing_price_count": int(missing_price_count),
        "assigned_capital": float(frame["assigned_capital"].sum()),
        "assigned_shares": float(frame["original_shares"].sum() if "original_shares" in frame.columns else frame["assigned_shares"].sum()),
        "assigned_put_pnl": float(frame["assigned_put_pnl"].sum()) if "assigned_put_pnl" in frame else 0.0,
        "post_assignment_put_pnl": float(frame["post_assignment_put_pnl"].sum()) if "post_assignment_put_pnl" in frame else 0.0,
        "put_pnl": float(frame["put_pnl"].sum()),
        "call_cashflow_pnl": float(frame["call_cashflow_pnl"].sum()),
        "actual_stock_pnl": float(frame["actual_stock_pnl"].dropna().sum()) if "actual_stock_pnl" in frame else 0.0,
        "hold_stock_pnl": float(frame["hold_stock_pnl"].sum()),
        "opportunity_cost": float(frame["opportunity_cost"].sum()),
        "counterfactual_before_opportunity": float(frame["counterfactual_before_opportunity"].sum()),
        "actual_total_pnl": float(frame["actual_total_pnl"].dropna().sum()) if "actual_total_pnl" in frame else 0.0,
        "counterfactual_total_pnl": float(frame["counterfactual_total_pnl"].sum()),
        "delta_actual_minus_hold": float(frame["delta_actual_minus_hold"].dropna().sum()) if "delta_actual_minus_hold" in frame else 0.0,
        "actual_beat_hold_rate": (
            float((frame["delta_actual_minus_hold"].dropna() > 0).mean())
            if "delta_actual_minus_hold" in frame and actual_count
            else None
        ),
    }


def _open_call_cap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        if (row.get("open_call_covered_shares") or 0) <= 0:
            continue
        market_stock_pnl = (row["evaluation_price"] - row["strike"]) * row["remaining_shares"]
        capped_drag = max(market_stock_pnl - (row.get("stock_unrealized_pnl") or 0.0), 0.0)
        if capped_drag <= 1:
            continue
        result.append(
            {
                "ticker": row["ticker"],
                "assignment_date": row["assignment_date"],
                "assigned_capital": row["assigned_capital"],
                "remaining_shares": row["remaining_shares"],
                "covered_shares": row["open_call_covered_shares"],
                "assignment_strike": row["strike"],
                "current_price": row["evaluation_price"],
                "open_call_min_strike": row["open_call_min_strike"],
                "cap_drag": capped_drag,
            }
        )
    return sorted(result, key=lambda item: item["cap_drag"], reverse=True)


def _historical_price_at_or_before(series: Optional[pd.Series], date: pd.Timestamp) -> Optional[float]:
    if series is None or series.empty:
        return None
    clean = pd.to_numeric(series, errors="coerce").dropna()
    clean.index = pd.to_datetime(clean.index, errors="coerce").normalize()
    clean = clean[clean.index.notna()].sort_index()
    clean = clean[clean.index <= pd.to_datetime(date).normalize()]
    if clean.empty:
        return None
    return float(clean.iloc[-1])
