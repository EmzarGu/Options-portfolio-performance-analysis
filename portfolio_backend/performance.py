from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

import numpy as np
import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.models import (
    ChainOutcome,
    OpenLot,
    OptionLot,
    OptionPnLEvent,
    RealizedSale,
    StockTxn,
)


def _fill_numeric_columns(df: pd.DataFrame, columns: List[str], value: float = 0.0) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = value
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(value)
    return df


def build_monthly_summary(
    realized_option_events: List[OptionPnLEvent],
    realized_sales: List[RealizedSale],
    capital_daily: pd.DataFrame,
    dividends_df: pd.DataFrame,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    def month_end(d):
        return pd.to_datetime(d).to_period("M").to_timestamp("M")

    opt_series = pd.Series(dtype=float, name="realized_options_pnl")
    if realized_option_events:
        df = pd.DataFrame(
            [{"date": e.date, "pnl": e.pnl} for e in realized_option_events if pd.to_datetime(e.date) <= as_of]
        )
        if not df.empty:
            opt_series = df.groupby(df["date"].apply(month_end))["pnl"].sum().rename("realized_options_pnl")

    stock_series = pd.Series(dtype=float, name="realized_stock_pnl")
    if realized_sales:
        rs_df = pd.DataFrame(
            [{"date": r.date, "pnl": r.pnl} for r in realized_sales if pd.to_datetime(r.date) <= as_of]
        )
        if not rs_df.empty:
            stock_series = rs_df.groupby(rs_df["date"].apply(month_end))["pnl"].sum().rename("realized_stock_pnl")

    div_series = pd.Series(dtype=float, name="dividends")
    if dividends_df is not None and not dividends_df.empty:
        div_filtered = dividends_df[dividends_df["pay_date"] <= as_of] if "pay_date" in dividends_df else dividends_df.copy()
        if not div_filtered.empty:
            date_col = "pay_date" if "pay_date" in div_filtered else "ex_date"
            div_series = div_filtered.groupby(div_filtered[date_col].apply(month_end))["cash"].sum().rename("dividends")

    cap = capital_daily.copy()
    cap.index = pd.to_datetime(cap.index).normalize()
    cap["month"] = cap.index.to_series().apply(month_end)
    avg_cap = cap.groupby("month")["total"].mean().rename("avg_capital")
    peak_cap = cap.groupby("month")["total"].max().rename("peak_capital")

    combined = pd.concat([opt_series, stock_series, div_series, avg_cap, peak_cap], axis=1)
    combined = _fill_numeric_columns(
        combined,
        ["realized_options_pnl", "realized_stock_pnl", "dividends", "avg_capital", "peak_capital"],
    )
    combined["total_realized_pnl"] = combined["realized_options_pnl"] + combined["realized_stock_pnl"] + combined["dividends"]
    combined["roac"] = np.where(combined["avg_capital"] > 0, combined["total_realized_pnl"] / combined["avg_capital"], np.nan)
    combined["ropc"] = np.where(combined["peak_capital"] > 0, combined["total_realized_pnl"] / combined["peak_capital"], np.nan)
    combined.index.name = "month"
    as_of_month_end = pd.to_datetime(as_of).to_period("M").to_timestamp("M")
    combined = combined[combined.index <= as_of_month_end].sort_index()
    return combined


def yearly_summary_from_monthly(monthly_df: pd.DataFrame, capital_daily: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    if monthly_df is None or monthly_df.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "realized_options_pnl",
                "realized_stock_pnl",
                "dividends",
                "total_realized_pnl",
                "avg_capital",
                "peak_capital",
                "roac_year",
                "ropc_year",
                "ann_roac",
                "ann_ropc",
            ]
        )
    m = monthly_df.copy()
    # Guard against object indexes (e.g., strings) that break .index.year
    m.index = pd.to_datetime(m.index, errors="coerce")
    m = m[m.index.notna()]
    if m.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "realized_options_pnl",
                "realized_stock_pnl",
                "dividends",
                "total_realized_pnl",
                "avg_capital",
                "peak_capital",
                "roac_year",
                "ropc_year",
                "ann_roac",
                "ann_ropc",
            ]
        )
    m["year"] = m.index.year
    agg = (
        m.groupby("year")
        .agg(
            realized_options_pnl=("realized_options_pnl", "sum"),
            realized_stock_pnl=("realized_stock_pnl", "sum"),
            dividends=("dividends", "sum"),
            total_realized_pnl=("total_realized_pnl", "sum"),
            roac_year=("roac", lambda s: (1 + s.dropna()).prod() - 1 if len(s.dropna()) else np.nan),
            ropc_year=("ropc", lambda s: (1 + s.dropna()).prod() - 1 if len(s.dropna()) else np.nan),
        )
        .reset_index()
    )
    cap_stats = capital_stats_by_year(capital_daily)
    agg = agg.merge(cap_stats, on="year", how="left")
    month_counts = m.groupby("year").size()
    days_elapsed = (
        capital_daily.reset_index()
        .assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
        .groupby("year")["date"]
        .nunique()
    )
    agg["ann_roac"] = agg["roac_year"]
    agg["ann_ropc"] = agg["ropc_year"]
    for idx, row in agg.iterrows():
        year = row["year"]
        months = month_counts.get(year, 0)
        if months == 12:
            continue
        days = days_elapsed.get(year, np.nan)
        if pd.notna(row["roac_year"]) and pd.notna(days) and days > 0:
            agg.at[idx, "ann_roac"] = (1 + row["roac_year"]) ** (365.0 / days) - 1
        if pd.notna(row["ropc_year"]) and pd.notna(days) and days > 0:
            agg.at[idx, "ann_ropc"] = (1 + row["ropc_year"]) ** (365.0 / days) - 1
    agg = agg.sort_values("year")
    return agg


def realized_option_pnl_by_year(realized_option_events: List[OptionPnLEvent]) -> pd.DataFrame:
    if not realized_option_events:
        return pd.DataFrame(columns=["year", "options_pnl"])
    df = pd.DataFrame([{"date": e.date, "pnl": e.pnl} for e in realized_option_events])
    df["year"] = pd.to_datetime(df["date"]).dt.year
    return df.groupby("year")["pnl"].sum().rename("options_pnl").reset_index()


def realized_stock_pnl_by_year(realized_sales: List[RealizedSale]) -> pd.DataFrame:
    if not realized_sales:
        return pd.DataFrame(columns=["year", "stock_realized_pnl"])
    df = pd.DataFrame([{"date": r.date, "pnl": r.pnl} for r in realized_sales])
    df["year"] = pd.to_datetime(df["date"]).dt.year
    return df.groupby("year")["pnl"].sum().rename("stock_realized_pnl").reset_index()


def per_ticker_yearly_from_realized(
    realized_option_events: List[OptionPnLEvent],
    realized_sales: List[RealizedSale],
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    opt_df = pd.DataFrame(
        [
            {"year": pd.to_datetime(e.date).year, "ticker": e.ticker, "options_pnl": e.pnl}
            for e in realized_option_events
            if pd.to_datetime(e.date) <= as_of
        ]
    )
    stock_df = pd.DataFrame(
        [
            {"year": pd.to_datetime(r.date).year, "ticker": r.ticker, "stock_realized_pnl": r.pnl}
            for r in realized_sales or []
            if pd.to_datetime(r.date) <= as_of
        ]
    )
    if not opt_df.empty:
        opt_df = opt_df.groupby(["year", "ticker"])["options_pnl"].sum().reset_index()
    else:
        opt_df = pd.DataFrame(columns=["year", "ticker", "options_pnl"])
    if not stock_df.empty:
        stock_df = stock_df.groupby(["year", "ticker"])["stock_realized_pnl"].sum().reset_index()
    else:
        stock_df = pd.DataFrame(columns=["year", "ticker", "stock_realized_pnl"])
    out = opt_df.merge(stock_df, on=["year", "ticker"], how="outer")
    out = _fill_numeric_columns(out, ["options_pnl", "stock_realized_pnl"])
    out["combined_realized"] = out["options_pnl"] + out["stock_realized_pnl"]
    return out.sort_values(["year", "combined_realized"], ascending=[True, False])


def build_per_ticker_totals(per_ticker_realized: pd.DataFrame, per_ticker_unreal: pd.Series) -> pd.DataFrame:
    realized_cols = ["options_pnl", "stock_realized_pnl", "combined_realized"]
    if per_ticker_realized is not None and not per_ticker_realized.empty:
        realized_totals = (
            per_ticker_realized.groupby("ticker")[realized_cols]
            .sum()
            .reset_index()
        )
    else:
        realized_totals = pd.DataFrame(columns=["ticker", *realized_cols])

    if per_ticker_unreal is not None and not per_ticker_unreal.empty:
        unreal_totals = per_ticker_unreal.rename_axis("ticker").reset_index(name="unrealized_pnl")
    else:
        unreal_totals = pd.DataFrame(columns=["ticker", "unrealized_pnl"])

    all_tickers = sorted(set(realized_totals.get("ticker", pd.Series(dtype=str))).union(unreal_totals.get("ticker", pd.Series(dtype=str))))
    if not all_tickers:
        return pd.DataFrame(columns=["ticker", *realized_cols, "unrealized_pnl", "total_pnl"])

    out = pd.DataFrame({"ticker": all_tickers})
    out = out.merge(realized_totals, on="ticker", how="left").merge(unreal_totals, on="ticker", how="left")
    out = _fill_numeric_columns(out, [*realized_cols, "unrealized_pnl"])
    out["total_pnl"] = out["combined_realized"] + out["unrealized_pnl"]
    return out


def twr_annualized_by_year(ret_series):
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return pd.Series(dtype=float)
    rs = ret_series.copy()
    rs.index = pd.to_datetime(rs.index, errors="coerce")
    rs = rs[rs.index.notna()]
    if rs.empty:
        return pd.Series(dtype=float)
    grouped = ret_series.groupby(ret_series.index.year)
    return grouped.apply(lambda r: (1 + r).prod() ** (12 / len(r)) - 1)


def expectancies(realized_option_events: List[OptionPnLEvent], realized_sales: List[RealizedSale], monthly_summary: pd.DataFrame, chain_outcomes: List["ChainOutcome"]):
    rows = []

    def add_row(name, pnls):
        if len(pnls) == 0:
            return
        pnls = np.array([p for p in pnls if pd.notna(p)], dtype=float)
        wins = pnls[pnls > 0]
        losses = pnls[pnls < 0]
        win_rate = (pnls > 0).mean() if len(pnls) else np.nan
        avg_win = wins.mean() if len(wins) else 0.0
        avg_loss = losses.mean() if len(losses) else 0.0
        expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss
        rows.append(
            {
                "Category": name,
                "Count": len(pnls),
                "Win rate": win_rate,
                "Avg win": avg_win,
                "Avg loss": avg_loss,
                "Expectancy": expectancy,
                "Total P&L": pnls.sum(),
            }
        )

    add_row("Options Trades", [e.pnl for e in realized_option_events])
    add_row("Stock Trades", [r.pnl for r in realized_sales])
    if monthly_summary is not None and not monthly_summary.empty and "total_realized_pnl" in monthly_summary:
        add_row("Monthly Totals", monthly_summary["total_realized_pnl"].tolist())
    if chain_outcomes:
        add_row("Chains", [c.total_pnl for c in chain_outcomes if c.end is not None])

    return pd.DataFrame(rows)


def calculate_performance_metrics(ret_series: pd.Series, rf: float = 0.04):
    ret_series = ret_series.dropna()
    if ret_series.empty:
        return {}
    m = len(ret_series)
    ec = (1 + ret_series).cumprod()
    cagr = ec.iloc[-1] ** (12 / m) - 1 if m > 0 else 0.0
    ann_vol = ret_series.std() * np.sqrt(12)
    rf_m = (1 + rf) ** (1 / 12) - 1
    excess = ret_series - rf_m
    sharpe = (excess.mean() / ret_series.std()) * np.sqrt(12) if ret_series.std() != 0 else np.nan
    down_std = np.sqrt((excess[excess < 0] ** 2).sum() / m) if m > 0 else 0
    sortino = (excess.mean() / down_std) * np.sqrt(12) if down_std != 0 else np.nan
    # prepend baseline equity of 1.0 so an initial negative month counts as a drawdown
    ec_dd = pd.concat([pd.Series([1.0]), ec.reset_index(drop=True)], ignore_index=True)
    max_dd = (ec_dd / ec_dd.cummax() - 1).min()
    return {"CAGR": cagr, "Volatility": ann_vol, "Sharpe": sharpe, "Sortino": sortino, "Max Drawdown": max_dd}


def calculate_performance_metrics_if_complete(ret_series: pd.Series, rf: float = 0.04):
    metric_keys = ["CAGR", "Volatility", "Sharpe", "Sortino", "Max Drawdown"]
    if ret_series is None or ret_series.empty:
        return {}
    returns = ret_series.copy()
    returns.index = pd.to_datetime(returns.index, errors="coerce")
    returns = returns[returns.index.notna()].sort_index()
    if returns.empty:
        return {}
    if not returns.notna().all():
        return {key: np.nan for key in metric_keys}
    return calculate_performance_metrics(returns, rf=rf)


def period_returns(ret_series: pd.Series):
    out = {}
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return out
    srt = ret_series.copy()
    srt.index = pd.to_datetime(srt.index, errors="coerce")
    srt = srt[srt.index.notna()].sort_index()
    if srt.empty:
        return out

    def compound_if_complete(sub):
        return (1 + sub).prod() - 1 if len(sub) and sub.notna().all() else np.nan

    def trailing_n(n):
        sub = srt.tail(n)
        return compound_if_complete(sub) if len(sub) == n else np.nan

    out["Return 3M"] = trailing_n(3)
    out["Return 6M"] = trailing_n(6)
    out["Return 1Y"] = trailing_n(12)
    latest_year = srt.index.max().year
    ytd = srt[srt.index.year == latest_year]
    out["Return YTD"] = compound_if_complete(ytd)
    out["Return SI"] = compound_if_complete(srt)
    return out


def capital_stats_by_year(capital_daily: pd.DataFrame) -> pd.DataFrame:
    df = capital_daily.reset_index()
    df["year"] = df["date"].dt.year
    return df.groupby("year").agg(avg_capital=("total", "mean"), peak_capital=("total", "max")).reset_index()


def build_covered_return_series(
    monthly_returns: pd.Series,
    affected_months: List[pd.Timestamp],
) -> Dict[str, object]:
    """
    Return the contiguous fully covered prefix for return-based charts/metrics.

    Once denominator incompleteness starts, later months are excluded from
    cumulative return displays and benchmark comparisons.
    """
    covered = monthly_returns.copy()
    covered.index = pd.to_datetime(covered.index, errors="coerce")
    covered = covered[covered.index.notna()].sort_index()
    affected = sorted(pd.to_datetime(m).to_period("M").to_timestamp("M") for m in affected_months if pd.notna(m))
    first_incomplete_month = affected[0] if affected else None
    if first_incomplete_month is not None:
        covered = covered[covered.index < first_incomplete_month]
    last_complete_month = covered.index.max() if not covered.empty else None
    return {
        "covered_returns": covered,
        "first_incomplete_month": first_incomplete_month,
        "last_complete_month": last_complete_month,
        "truncated": first_incomplete_month is not None and last_complete_month is not None,
    }


def calculate_unrealized_positions(
    open_options: List[OptionLot],
    inventory: List[OpenLot],
    prices: Dict[str, float],
) -> tuple[pd.DataFrame, pd.Series, float]:
    """Compute unrealized P&L by ticker using rules for short options and covered calls."""
    per_ticker = defaultdict(float)
    stock_rows = []
    # Build coverage map for open calls (shares capped at strike)
    coverage: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for lot in open_options:
        if lot.otype == "Call" and lot.qty > 0:
            coverage[lot.ticker].append({"strike": lot.strike, "shares": lot.qty * CONTRACT_MULTIPLIER})
    for cov_list in coverage.values():
        cov_list.sort(key=lambda x: x["strike"])  # use lowest strikes first

    # Option unrealized (premium received) + short put stock component
    for lot in open_options:
        premium_total = lot.open_price * lot.qty * CONTRACT_MULTIPLIER
        per_ticker[lot.ticker] += premium_total
        if lot.otype == "Put":
            px = prices.get(lot.ticker)
            if px is not None and not pd.isna(px) and px < lot.strike:
                stock_component = (px - lot.strike) * lot.qty * CONTRACT_MULTIPLIER
                per_ticker[lot.ticker] += stock_component
                stock_rows.append(
                    {
                        "ticker": lot.ticker,
                        "buy_date": None,
                        "shares": lot.qty * CONTRACT_MULTIPLIER,
                        "cost_per_share": lot.strike,
                        "current_price": px,
                        "covered_shares": 0,
                        "covered_strike": lot.strike,
                        "unrealized_pnl": stock_component,
                        "source": "put_gap",
                    }
                )

    # Stock inventory unrealized with covered call cap
    for lot in inventory:
        px = prices.get(lot.ticker)
        if px is None or pd.isna(px):
            continue
        shares_remaining = lot.shares_remaining
        lot_pnl = 0.0
        covered_used = 0
        covered_strike_min = None
        cov_list = coverage.get(lot.ticker, [])
        while shares_remaining > 0:
            if cov_list:
                leg = cov_list[0]
                use = min(shares_remaining, leg["shares"])
                effective_px = min(px, leg["strike"])
                lot_pnl += (effective_px - lot.cost_per_share) * use
                covered_used += use
                covered_strike_min = leg["strike"] if covered_strike_min is None else min(covered_strike_min, leg["strike"])
                leg["shares"] -= use
                shares_remaining -= use
                if leg["shares"] == 0:
                    cov_list.pop(0)
            else:
                lot_pnl += (px - lot.cost_per_share) * shares_remaining
                shares_remaining = 0
        per_ticker[lot.ticker] += lot_pnl
        stock_rows.append(
            {
                "ticker": lot.ticker,
                "buy_date": lot.buy_date,
                "shares": lot.shares_remaining,
                "cost_per_share": lot.cost_per_share,
                "current_price": px,
                "covered_shares": covered_used,
                "covered_strike": covered_strike_min,
                "unrealized_pnl": lot_pnl,
                "source": "stock_lot",
            }
        )

    inv_df = pd.DataFrame(stock_rows)
    per_ticker_series = pd.Series(per_ticker, dtype=float)
    total_unreal = float(per_ticker_series.sum()) if not per_ticker_series.empty else 0.0
    return inv_df, per_ticker_series, total_unreal


def build_dashboard_unrealized_snapshot(
    open_option_lots: List[OptionLot],
    ending_inventory: List[OpenLot],
    live_prices: Dict[str, float],
) -> Dict[str, object]:
    """Return the current dashboard unrealized snapshot.

    Open short put assignment exposure is part of option unrealized P&L. If an
    open put is in the money, the assignment gap is recognized as:
    ``(current_price - strike) * contracts * 100``. The synthetic inventory row
    remains in ``inv_df`` for position/risk display, but it is excluded from the
    stock-unrealized split because those shares are not yet owned.
    """
    required_price_tickers = {lot.ticker for lot in ending_inventory}
    required_price_tickers.update(lot.ticker for lot in open_option_lots if lot.otype == "Put")
    missing_required_price_tickers = sorted(
        ticker
        for ticker in required_price_tickers
        if ticker not in live_prices or pd.isna(live_prices.get(ticker))
    )
    inv_df, per_ticker_unreal, total_unreal = calculate_unrealized_positions(
        open_option_lots,
        ending_inventory,
        live_prices,
    )
    if not inv_df.empty and "source" in inv_df.columns:
        put_gap_mask = inv_df["source"].eq("put_gap")
        stock_rows = inv_df.loc[~put_gap_mask]
        put_gap_rows = inv_df.loc[put_gap_mask]
    else:
        stock_rows = inv_df
        put_gap_rows = pd.DataFrame()

    stock_unreal = float(stock_rows["unrealized_pnl"].sum()) if not stock_rows.empty else 0.0
    put_assignment_unreal = float(put_gap_rows["unrealized_pnl"].sum()) if not put_gap_rows.empty else 0.0
    itm_put_cash_required = (
        float(
            (
                pd.to_numeric(put_gap_rows["cost_per_share"], errors="coerce")
                * pd.to_numeric(put_gap_rows["shares"], errors="coerce")
            ).sum()
        )
        if not put_gap_rows.empty and {"cost_per_share", "shares"}.issubset(put_gap_rows.columns)
        else 0.0
    )
    itm_put_market_value = (
        float(
            (
                pd.to_numeric(put_gap_rows["current_price"], errors="coerce")
                * pd.to_numeric(put_gap_rows["shares"], errors="coerce")
            ).sum()
        )
        if not put_gap_rows.empty and {"current_price", "shares"}.issubset(put_gap_rows.columns)
        else 0.0
    )
    itm_put_shares = (
        int(pd.to_numeric(put_gap_rows["shares"], errors="coerce").fillna(0).sum())
        if not put_gap_rows.empty and "shares" in put_gap_rows.columns
        else 0
    )
    itm_put_contracts = int(round(itm_put_shares / CONTRACT_MULTIPLIER)) if itm_put_shares else 0
    option_unreal = total_unreal - stock_unreal
    return {
        "inv_df": inv_df,
        "per_ticker_unreal": per_ticker_unreal,
        "total_unreal": total_unreal,
        "stock_unreal": stock_unreal,
        "option_unreal": option_unreal,
        "put_assignment_unreal": put_assignment_unreal,
        "itm_put_cash_required": itm_put_cash_required,
        "itm_put_market_value": itm_put_market_value,
        "itm_put_contracts": itm_put_contracts,
        "itm_put_shares": itm_put_shares,
        "unrealized_blocked": bool(missing_required_price_tickers),
        "missing_required_price_tickers": missing_required_price_tickers,
    }


def build_dashboard_unrealized_adjusted_return_series(
    monthly_returns: pd.Series,
    capital_daily: pd.DataFrame,
    as_of_ts: pd.Timestamp,
    include_unrealized_current_year: bool,
    total_unreal: float,
    unrealized_blocked: bool = False,
) -> pd.Series:
    """Apply the dashboard's current unrealized-adjusted return treatment."""
    monthly_returns_unrealized_adjusted = monthly_returns.copy()
    if unrealized_blocked:
        return monthly_returns_unrealized_adjusted
    if include_unrealized_current_year and total_unreal != 0:
        cap_year_stats = capital_stats_by_year(capital_daily)
        cap_curr_year = cap_year_stats.loc[cap_year_stats["year"] == as_of_ts.year, "avg_capital"]
        cap_basis = float(cap_curr_year.iloc[0]) if not cap_curr_year.empty else np.nan
        if pd.notna(cap_basis) and cap_basis > 0:
            unrealized_return_component = total_unreal / cap_basis
            month_end = pd.to_datetime(as_of_ts).to_period("M").to_timestamp("M")
            base_ret = (
                monthly_returns_unrealized_adjusted.loc[month_end]
                if month_end in monthly_returns_unrealized_adjusted.index
                else 0.0
            )
            monthly_returns_unrealized_adjusted.loc[month_end] = base_ret + unrealized_return_component
    return monthly_returns_unrealized_adjusted


def build_yearly_with_dashboard_unrealized(
    yearly: pd.DataFrame,
    include_unrealized_current_year: bool,
    total_unreal: float,
    as_of_ts: pd.Timestamp,
    unrealized_blocked: bool = False,
) -> pd.DataFrame:
    """Apply the dashboard's current unrealized total to the yearly summary."""
    yearly_with_unreal = yearly.copy()
    yearly_with_unreal["total_pnl_incl_unreal"] = yearly_with_unreal.get("total_realized_pnl", pd.Series(dtype=float))
    if unrealized_blocked and not yearly_with_unreal.empty:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        yearly_with_unreal.loc[mask_curr, "total_pnl_incl_unreal"] = np.nan
        return yearly_with_unreal
    if include_unrealized_current_year and total_unreal != 0 and not yearly_with_unreal.empty:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        yearly_with_unreal.loc[mask_curr, "total_pnl_incl_unreal"] = (
            yearly_with_unreal.loc[mask_curr, "total_realized_pnl"] + total_unreal
        )
    return yearly_with_unreal


def _chain_stock_realized(stock_txns: List[StockTxn]) -> float:
    by_ticker: Dict[str, List[OpenLot]] = defaultdict(list)
    realized = 0.0
    for t in sorted(stock_txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            by_ticker[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty = t.shares
            cost_accum = 0.0
            while qty > 0 and by_ticker[t.ticker]:
                lot = by_ticker[t.ticker][0]
                take = min(qty, lot.shares_remaining)
                cost_accum += take * lot.cost_per_share
                lot.shares_remaining -= take
                qty -= take
                if lot.shares_remaining == 0:
                    by_ticker[t.ticker].pop(0)
            # uncovered sells assume zero profit (pre-owned)
            cost_accum += qty * t.price
            realized += t.shares * t.price - cost_accum
    return realized


def build_chains(stock_txns: List[StockTxn], option_events: List[OptionPnLEvent], as_of: pd.Timestamp) -> List[ChainOutcome]:
    chains: Dict[str, List[Dict]] = defaultdict(list)
    balances: Dict[str, int] = defaultdict(int)
    # Build chains from stock txn flow
    for t in sorted(stock_txns, key=lambda x: (x.date, x.ticker)):
        tk = t.ticker
        cur_balance = balances[tk]
        active = chains[tk][-1] if chains[tk] else None
        if active is None:
            active = {"start": t.date, "end": None, "txns": [], "option_events": []}
            chains[tk].append(active)
        active["txns"].append(t)
        if t.side == "BUY":
            cur_balance += t.shares
        else:
            cur_balance = max(0, cur_balance - t.shares)
        balances[tk] = cur_balance
        if cur_balance == 0:
            active["end"] = t.date
    # Attach option events to chains by ticker and date window
    for ev in sorted(option_events, key=lambda x: (x.date, x.ticker)):
        tk = ev.ticker
        assigned_chain = None
        for ch in chains.get(tk, []):
            end_date = ch["end"] if ch["end"] is not None else as_of
            if ch["start"] <= ev.date <= end_date:
                assigned_chain = ch
                break
        if assigned_chain is None:
            # standalone option chain with no stock flow; treat any realized event as a closed chain
            ch = {"start": ev.date, "end": ev.date, "txns": [], "option_events": [ev]}
            chains[tk].append(ch)
        else:
            assigned_chain["option_events"].append(ev)

    outcomes: List[ChainOutcome] = []
    for tk, ch_list in chains.items():
        for ch in ch_list:
            stock_pnl = _chain_stock_realized(ch["txns"])
            option_pnl = sum(e.pnl for e in ch["option_events"])
            outcomes.append(
                ChainOutcome(
                    ticker=tk,
                    start=pd.to_datetime(ch["start"]),
                    end=pd.to_datetime(ch["end"]) if ch["end"] is not None else None,
                    option_pnl=option_pnl,
                    stock_pnl=stock_pnl,
                    total_pnl=option_pnl + stock_pnl,
                )
            )
    return outcomes
