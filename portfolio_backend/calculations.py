from __future__ import annotations

import math
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.market_calendar import is_us_market_trading_day
from portfolio_backend.models import (
    HoldSeg,
    OptionLot,
    OptionPnLEvent,
    OptionTrade,
    OpenLot,
    RealizedSale,
    StockTxn,
)


def parse_strike_pair(s: str) -> Tuple[float, float]:
    try:
        a, b = str(s).split("/")
        return float(a), float(b)
    except Exception:
        return math.nan, math.nan


MIXED_SHORT_PUT_PHRASES = ("short put", "sold put", "written put")
MIXED_SHORT_CALL_PHRASES = ("short call", "sold call", "written call")


def infer_mixed_short_leg(row: pd.Series) -> Tuple[Optional[str], float]:
    t_low = str(row.get("type", "")).lower()
    c_low = str(row.get("comment", "")).lower()
    a, b = parse_strike_pair(row.get("strike", ""))
    put_strike = call_strike = math.nan
    if "put/call" in t_low:
        put_strike, call_strike = a, b
    elif "call/put" in t_low:
        call_strike, put_strike = a, b
    put_hint = any(phrase in c_low for phrase in MIXED_SHORT_PUT_PHRASES)
    call_hint = any(phrase in c_low for phrase in MIXED_SHORT_CALL_PHRASES)
    if put_hint and not call_hint:
        return "Put", put_strike
    if call_hint and not put_hint:
        return "Call", call_strike
    return None, math.nan


def _mixed_leg_parse_issue(row) -> str:
    row_dict = row._asdict() if hasattr(row, "_asdict") else row
    ticker = str(row_dict.get("ticker", "")).upper().strip()
    trans_date = pd.to_datetime(row_dict.get("trans_date", pd.NaT), errors="coerce")
    date_text = trans_date.date() if pd.notna(trans_date) else "unknown date"
    comment = row_dict.get("comment", "")
    return (
        f"Mixed-leg option row for {ticker or 'unknown ticker'} on {date_text} "
        f"has ambiguous short leg. Type={row_dict.get('type', '')}, strike={row_dict.get('strike', '')}, "
        f"comment={comment!r}. Add one of: short put, sold put, written put, short call, sold call, written call."
    )


def _price_per_share(row: pd.Series) -> float:
    accessor = row.get if hasattr(row, "get") else lambda k, default=None: getattr(row, k, default)
    qty_raw = accessor("qty", 0)
    qty = abs(float(qty_raw) if pd.notna(qty_raw) else 0.0)
    if qty == 0:
        return 0.0
    pnl_val = accessor("total_pnl", None)
    amount_val = accessor("amount", None)
    commission_val = accessor("commission", 0.0) or 0.0
    net_cash = None
    if pd.notna(pnl_val):
        net_cash = float(pnl_val)
    elif pd.notna(amount_val):
        net_cash = float(amount_val) - float(commission_val)
    if net_cash is None:
        return 0.0
    return net_cash / (qty * CONTRACT_MULTIPLIER)


def build_option_trades(df: pd.DataFrame, issues: Optional[List[str]] = None) -> List[OptionTrade]:
    trades: List[OptionTrade] = []
    rows = df.sort_values(["ticker", "trans_date"]).reset_index(drop=True)
    # Pre-count sells per option key to ignore standalone long buys (protective hedges)
    sell_counts: Dict[Tuple, int] = defaultdict(int)
    for r in rows.itertuples(index=False):
        t_raw = str(r.type).strip()
        action = r.action
        otype = None
        if t_raw in ("Put", "Call"):
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
            else:
                strike_val = math.nan
        else:
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        if action == "Sell" and otype is not None and not pd.isna(strike_val):
            key = (str(r.ticker).upper().strip(), otype, strike_val, pd.to_datetime(r.expiration).normalize())
            sell_counts[key] += 1

    for r in rows.itertuples(index=False):
        action = r.action
        if action not in ("Sell", "Buy"):
            continue
        t_raw = str(r.type).strip()
        cmt = r.comment if pd.notna(r.comment) else ""
        assigned_flag = False
        if hasattr(r, "assigned_flag"):
            try:
                assigned_flag = float(getattr(r, "assigned_flag")) > 0
            except Exception:
                assigned_flag = False
        assigned = assigned_flag or ("assigned" in cmt.lower())
        otype = None
        if t_raw in ("Put", "Call"):
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
            else:
                strike_val = math.nan
                if issues is not None:
                    issues.append(_mixed_leg_parse_issue(r))
        else:
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        if otype is None or pd.isna(strike_val):
            continue
        key = (str(r.ticker).upper().strip(), otype, strike_val, pd.to_datetime(r.expiration).normalize())
        if action == "Buy" and sell_counts.get(key, 0) == 0:
            # Ignore standalone protective longs
            continue
        price = _price_per_share(r)
        if action == "Buy":
            price = abs(price)
        qty = int(round(float(r.qty))) if pd.notna(r.qty) else 0
        trades.append(
            OptionTrade(
                date=pd.to_datetime(r.trans_date),
                ticker=r.ticker,
                otype=otype,
                action=action,
                strike=strike_val,
                expiration=pd.to_datetime(r.expiration),
                qty=qty,
                price=price,
                comment=cmt,
                assigned=assigned,
            )
        )
    return trades


def process_option_positions(trades: List[OptionTrade], as_of: pd.Timestamp):
    open_map: Dict[Tuple, List[OptionLot]] = defaultdict(list)
    realized_events: List[OptionPnLEvent] = []
    stock_txns: List[StockTxn] = []
    issues: List[str] = []
    all_lots: List[OptionLot] = []

    def snapshot_lot(
        lot: OptionLot,
        qty: Optional[int] = None,
        close_date: Optional[pd.Timestamp] = None,
        close_price: Optional[float] = None,
        close_reason: Optional[str] = None,
    ) -> OptionLot:
        return OptionLot(
            ticker=lot.ticker,
            otype=lot.otype,
            strike=lot.strike,
            qty=lot.qty if qty is None else qty,
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

    for t in sorted(trades, key=lambda x: (x.date, x.ticker)):
        key = (t.ticker, t.otype, t.strike, pd.to_datetime(t.expiration).normalize())
        if t.action == "Sell":
            lot = OptionLot(
                ticker=t.ticker,
                otype=t.otype,
                strike=t.strike,
                qty=t.qty,
                open_date=pd.to_datetime(t.date),
                expiration=pd.to_datetime(t.expiration),
                open_price=t.price,
                comment=t.comment,
                assigned=t.assigned,
            )
            open_map[key].append(lot)
        else:
            qty_to_close = t.qty
            buckets = open_map.get(key, [])
            if qty_to_close > 0 and not buckets:
                issues.append(f"Buy {t.ticker} {t.otype} {t.strike} on {t.date.date()} had no open short to close.")
            while qty_to_close > 0 and buckets:
                lot = buckets[0]
                take = min(qty_to_close, lot.qty)
                pnl = (lot.open_price - t.price) * take * CONTRACT_MULTIPLIER
                realized_events.append(
                    OptionPnLEvent(
                        date=pd.to_datetime(t.date),
                        ticker=t.ticker,
                        otype=t.otype,
                        strike=t.strike,
                        qty=take,
                        pnl=pnl,
                        p_open=lot.open_price,
                        p_close=t.price,
                        reason="close",
                    )
                )
                all_lots.append(
                    snapshot_lot(
                        lot,
                        qty=take,
                        close_date=pd.to_datetime(t.date),
                        close_price=t.price,
                        close_reason="close",
                    )
                )
                lot.qty -= take
                qty_to_close -= take
                if lot.qty == 0:
                    buckets.pop(0)
            if qty_to_close > 0:
                issues.append(f"Unmatched buy quantity for {t.ticker} {t.otype} {t.strike} on {t.date.date()}: {qty_to_close} remaining.")
            open_map[key] = buckets

    open_lots: List[OptionLot] = []
    for buckets in open_map.values():
        for lot in buckets:
            if pd.isna(lot.expiration):
                continue
            if as_of.normalize() >= pd.to_datetime(lot.expiration).normalize():
                close_date = pd.to_datetime(lot.expiration).normalize()
                pnl = (lot.open_price - 0.0) * lot.qty * CONTRACT_MULTIPLIER
                reason = "assignment" if lot.assigned else "expiration"
                realized_events.append(
                    OptionPnLEvent(
                        date=close_date,
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
                    snapshot_lot(
                        lot,
                        close_date=close_date,
                        close_price=0.0,
                        close_reason=reason,
                    )
                )
                shares = int(round(lot.qty * CONTRACT_MULTIPLIER))
                if lot.assigned and shares > 0:
                    if lot.otype == "Put":
                        stock_txns.append(
                            StockTxn(close_date, lot.ticker, "BUY", shares, lot.strike, "Assigned Put")
                        )
                    else:
                        stock_txns.append(
                            StockTxn(close_date, lot.ticker, "SELL", shares, lot.strike, "Assigned Call")
                        )
            else:
                open_snapshot = snapshot_lot(lot)
                open_lots.append(open_snapshot)
                all_lots.append(open_snapshot)
    return realized_events, open_lots, stock_txns, issues, all_lots


def compute_stock_realized_and_inventory(txns: List[StockTxn], issues: Optional[List[str]] = None):
    by_ticker: Dict[str, List[OpenLot]] = defaultdict(list)
    realized: List[RealizedSale] = []
    for t in sorted(txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            by_ticker[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty_to_sell = t.shares
            cost_accum = 0.0
            while qty_to_sell > 0 and by_ticker[t.ticker]:
                lot = by_ticker[t.ticker][0]
                take = min(qty_to_sell, lot.shares_remaining)
                cost_accum += take * lot.cost_per_share
                lot.shares_remaining -= take
                qty_to_sell -= take
                if lot.shares_remaining == 0:
                    by_ticker[t.ticker].pop(0)
            if qty_to_sell > 0:
                # Not enough inventory; assume pre-owned shares for assigned calls -> zero P&L on uncovered portion
                if issues is not None and t.source != "Assigned Call":
                    issues.append(f"Selling {t.shares} shares of {t.ticker} on {t.date.date()} exceeded inventory by {qty_to_sell}.")
                cost_accum += qty_to_sell * t.price
                qty_to_sell = 0
            proceeds = t.shares * t.price
            cost = cost_accum
            realized.append(RealizedSale(t.date, t.ticker, t.shares, proceeds, cost, proceeds - cost, t.source))
    inventory: List[OpenLot] = []
    for _, lots_list in by_ticker.items():
        for lot in lots_list:
            if lot.shares_remaining > 0:
                inventory.append(lot)
    return realized, inventory


def build_holding_segments(txns: List[StockTxn], as_of: pd.Timestamp) -> List[HoldSeg]:
    open_buys: Dict[str, List[OpenLot]] = defaultdict(list)
    segs: List[HoldSeg] = []
    for t in sorted(txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            open_buys[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty = t.shares
            while qty > 0 and open_buys[t.ticker]:
                lot = open_buys[t.ticker][0]
                used = min(qty, lot.shares_remaining)
                segs.append(
                    HoldSeg(
                        t.ticker,
                        lot.buy_date.normalize(),
                        min(t.date.normalize(), as_of),
                        int(used),
                        lot.cost_per_share,
                    )
                )
                lot.shares_remaining -= used
                qty -= used
                if lot.shares_remaining == 0:
                    open_buys[t.ticker].pop(0)
    for tk, lots_list in open_buys.items():
        for lot in lots_list:
            if lot.shares_remaining > 0:
                segs.append(HoldSeg(tk, lot.buy_date.normalize(), as_of, int(lot.shares_remaining), lot.cost_per_share))
    return segs


def daterange_days(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if pd.isna(start) or pd.isna(end):
        return pd.DatetimeIndex([])
    start = start.normalize()
    end = end.normalize()
    if end <= start:
        end = start + pd.Timedelta(days=1)
    return pd.date_range(start, end, freq="D", inclusive="left")


def resolve_capital_price_on_day(
    px_series: Optional[pd.Series],
    valuation_date: pd.Timestamp,
    fallback_price: float,
) -> float:
    """Resolve a price for capital-denominator use: same-day close, else last prior close, else fallback."""
    if px_series is None:
        return fallback_price
    try:
        prices = px_series.dropna().copy()
        if prices.empty:
            return fallback_price
        prices.index = pd.to_datetime(prices.index, errors="coerce")
        prices = prices[prices.index.notna()].sort_index()
        if prices.empty:
            return fallback_price
        valuation_date = pd.to_datetime(valuation_date).normalize()
        exact_price = prices.get(valuation_date, np.nan)
        if pd.notna(exact_price):
            return float(exact_price)
        prior_prices = prices.loc[prices.index <= valuation_date]
        if not prior_prices.empty:
            return float(prior_prices.iloc[-1])
    except Exception:
        return fallback_price
    return fallback_price


def _normalized_capital_price_series(px_series: Optional[pd.Series]) -> pd.Series:
    if px_series is None:
        return pd.Series(dtype=float)
    prices = pd.to_numeric(px_series, errors="coerce").dropna()
    if prices.empty:
        return pd.Series(dtype=float)
    prices.index = pd.to_datetime(prices.index, errors="coerce")
    prices = prices[prices.index.notna()].sort_index()
    if prices.empty:
        return pd.Series(dtype=float)
    prices.index = prices.index.normalize()
    return prices[~prices.index.duplicated(keep="last")]


def resolve_capital_prices_for_days(
    px_series: Optional[pd.Series],
    valuation_days: pd.DatetimeIndex,
    fallback_price: float,
) -> pd.Series:
    """Resolve capital-denominator prices for many days with same fallback semantics as one-day lookup."""
    if valuation_days.empty:
        return pd.Series(dtype=float, index=valuation_days)
    fallback = float(fallback_price)
    if px_series is None:
        return pd.Series(fallback, index=valuation_days, dtype=float)
    try:
        prices = _normalized_capital_price_series(px_series)
        if prices.empty:
            return pd.Series(fallback, index=valuation_days, dtype=float)
        resolved = prices.reindex(prices.index.union(valuation_days)).sort_index().ffill().reindex(valuation_days)
        return resolved.fillna(fallback).astype(float)
    except Exception:
        return pd.Series(fallback, index=valuation_days, dtype=float)


def build_capital_timeline(
    option_lots: List[OptionLot],
    txns: List[StockTxn],
    as_of: pd.Timestamp,
    df_opts: pd.DataFrame,
    price_history: Dict[str, pd.Series],
) -> pd.DataFrame:
    component_series: Dict[str, List[pd.Series]] = defaultdict(list)
    for lot in option_lots:
        if lot.otype != "Put":
            continue
        open_d = pd.to_datetime(lot.open_date).normalize()
        close_candidate = lot.close_date if lot.close_date is not None else lot.expiration
        close_d = pd.to_datetime(close_candidate if pd.notna(close_candidate) else as_of).normalize()
        close_d = min(close_d, as_of.normalize())
        if pd.isna(open_d) or pd.isna(close_d):
            continue
        reserve = lot.strike * CONTRACT_MULTIPLIER * int(round(lot.qty))
        days = daterange_days(open_d, close_d)
        if not days.empty:
            component_series["puts_reserve"].append(pd.Series(float(reserve), index=days))

    segs = build_holding_segments(txns, as_of)
    for seg in segs:
        days = daterange_days(seg.start, seg.end)
        if days.empty:
            continue
        px_series = price_history.get(seg.ticker)
        prices = resolve_capital_prices_for_days(px_series, days, seg.cost_per_share)
        component_series["shares_invested"].append(prices * seg.shares)

    if not component_series:
        start_date = df_opts["trans_date"].min().normalize() if not df_opts.empty else as_of.normalize()
        idx = pd.date_range(start_date, as_of, freq="D")
        daily = pd.DataFrame({"puts_reserve": [0.0] * len(idx)}, index=idx)
        daily["total"] = daily.sum(axis=1)
        daily.index.name = "date"
        return daily

    columns = {}
    for component, series_list in component_series.items():
        if series_list:
            values = pd.concat(series_list).groupby(level=0).sum().sort_index()
            columns[component] = values
    daily = pd.DataFrame(columns).fillna(0.0)
    daily["total"] = daily.sum(axis=1)
    daily.index.name = "date"
    return daily


def _count_business_days(start: pd.Timestamp, end: pd.Timestamp) -> int:
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if pd.isna(start) or pd.isna(end) or start > end:
        return 0
    return sum(
        1
        for day in pd.date_range(start.normalize(), end.normalize(), freq="D")
        if is_us_market_trading_day(day.date())
    )


def _business_day_gap_ranges(sorted_dates: pd.DatetimeIndex) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    if len(sorted_dates) < 2:
        return []
    starts = sorted_dates[:-1] + pd.Timedelta(days=1)
    ends = sorted_dates[1:] - pd.Timedelta(days=1)
    valid = starts <= ends
    if not valid.any():
        return []

    counts = np.zeros(len(starts), dtype=int)
    counts[valid] = [
        _count_business_days(pd.Timestamp(start), pd.Timestamp(end))
        for start, end in zip(starts[valid], ends[valid])
    ]

    return [
        (pd.Timestamp(start), pd.Timestamp(end))
        for start, end, count in zip(starts, ends, counts)
        if count > 1
    ]


def assess_capital_history_coverage(
    holding_segments: List[HoldSeg],
    price_history: Dict[str, pd.Series],
) -> Dict[str, object]:
    """
    Identify denominator-history gaps that should block return metrics.

    Cost-basis fallback remains acceptable only before a position has any prior
    fetched close. Once a segment should have durable historical coverage, stale
    or missing history marks the capital timeline as incomplete.
    """
    coverage_issues: List[Dict[str, object]] = []
    affected_months = set()
    affected_years = set()
    affected_tickers = set()

    def add_issue(ticker: str, start_date: pd.Timestamp, end_date: pd.Timestamp, reason: str) -> None:
        start_ts = pd.to_datetime(start_date).normalize()
        end_ts = pd.to_datetime(end_date).normalize()
        if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
            return
        coverage_issues.append(
            {
                "ticker": ticker,
                "start_date": start_ts,
                "end_date": end_ts,
                "reason": reason,
            }
        )
        affected_tickers.add(ticker)
        for period in pd.period_range(start_ts.to_period("M"), end_ts.to_period("M"), freq="M"):
            affected_months.add(period.to_timestamp("M"))
        affected_years.update(range(start_ts.year, end_ts.year + 1))

    normalized_price_history = {
        ticker: _normalized_capital_price_series(series)
        for ticker, series in price_history.items()
    }

    for seg in holding_segments:
        valuation_days = daterange_days(seg.start, seg.end)
        if valuation_days.empty:
            continue

        valuation_days = pd.DatetimeIndex(
            [day for day in valuation_days if is_us_market_trading_day(pd.Timestamp(day).date())]
        )
        if valuation_days.empty:
            continue

        prices = normalized_price_history.get(seg.ticker)
        if prices is None or prices.empty:
            if len(valuation_days) > 1:
                add_issue(seg.ticker, valuation_days[1], valuation_days[-1], "missing_history")
            continue

        first_price_date = prices.index.min()
        last_price_date = prices.index.max()

        early_days = valuation_days[valuation_days < first_price_date]
        if len(early_days) > 1:
            missing_bdays_before_first = _count_business_days(
                early_days[0] + pd.Timedelta(days=1),
                first_price_date - pd.Timedelta(days=1),
            )
            if missing_bdays_before_first > 1:
                add_issue(seg.ticker, early_days[1], early_days[-1], "missing_before_first_close")

        in_range_prices = prices[(prices.index >= first_price_date) & (prices.index <= last_price_date)]
        for gap_start, gap_end in _business_day_gap_ranges(in_range_prices.index):
            add_issue(seg.ticker, gap_start, gap_end, "internal_gap")

        late_days = valuation_days[valuation_days > last_price_date]
        if len(late_days) > 0:
            missing_bdays_after_last = _count_business_days(
                last_price_date + pd.Timedelta(days=1),
                late_days[-1],
            )
            if missing_bdays_after_last > 1:
                add_issue(seg.ticker, late_days[0], late_days[-1], "stale_tail")

    return {
        "capital_history_incomplete": bool(coverage_issues),
        "capital_history_coverage_issues": coverage_issues,
        "capital_history_affected_months": sorted(affected_months),
        "capital_history_affected_years": sorted(affected_years),
        "capital_history_affected_tickers": sorted(affected_tickers),
    }
