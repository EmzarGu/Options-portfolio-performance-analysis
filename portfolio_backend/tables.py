from __future__ import annotations

from typing import Dict, List, TYPE_CHECKING

import numpy as np
import pandas as pd

from portfolio_backend.option_accounting import canonical_open_price

if TYPE_CHECKING:
    from portfolio_backend.models import OptionLot


def build_open_options_frame(open_option_lots: List["OptionLot"]) -> pd.DataFrame:
    columns = [
        "ticker",
        "type",
        "strike",
        "qty",
        "expiration",
        "trans_date",
        "open_price",
        "roll_adjusted_open_price",
    ]
    df = pd.DataFrame(
        [
            {
                "ticker": lot.ticker,
                "type": lot.otype,
                "strike": lot.strike,
                "qty": lot.qty,
                "expiration": lot.expiration,
                "trans_date": lot.open_date,
                "open_price": lot.open_price,
                "roll_adjusted_open_price": canonical_open_price(lot.open_price, lot.roll_adjusted_open_price),
            }
            for lot in open_option_lots
        ]
    )
    if df.empty:
        return pd.DataFrame(columns=columns)

    grouped_rows = []
    group_cols = ["ticker", "type", "strike", "expiration"]
    for keys, group in df.groupby(group_cols, dropna=False, sort=False):
        qty = pd.to_numeric(group["qty"], errors="coerce").fillna(0)
        open_prices = pd.to_numeric(group["open_price"], errors="coerce")
        roll_adjusted_open_prices = pd.to_numeric(group["roll_adjusted_open_price"], errors="coerce")
        weights = qty.abs()
        if weights.sum() > 0:
            open_price = float((open_prices.fillna(0) * weights).sum() / weights.sum())
            roll_adjusted_open_price = float((roll_adjusted_open_prices.fillna(0) * weights).sum() / weights.sum())
        else:
            open_price = float(open_prices.mean()) if open_prices.notna().any() else np.nan
            roll_adjusted_open_price = (
                float(roll_adjusted_open_prices.mean()) if roll_adjusted_open_prices.notna().any() else np.nan
            )
        ticker, option_type, strike, expiration = keys
        grouped_rows.append(
            {
                "ticker": ticker,
                "type": option_type,
                "strike": strike,
                "qty": int(round(float(qty.sum()))),
                "expiration": expiration,
                "trans_date": pd.to_datetime(group["trans_date"], errors="coerce").min(),
                "open_price": open_price,
                "roll_adjusted_open_price": roll_adjusted_open_price,
            }
        )
    return pd.DataFrame(grouped_rows, columns=columns)


def filter_df_to_range(df: pd.DataFrame, date_col: str, end: pd.Timestamp, range_choice: str) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return df
    end = pd.to_datetime(end)
    start = None
    if range_choice == "3M":
        start = end - pd.DateOffset(months=3)
    elif range_choice == "6M":
        start = end - pd.DateOffset(months=6)
    elif range_choice == "YTD":
        start = pd.Timestamp(end.year, 1, 1)
    elif range_choice == "1Y":
        start = end - pd.DateOffset(years=1)
    if start is not None:
        dates = pd.to_datetime(df[date_col])
        mask = (dates >= start) & (dates <= end)
        return df.loc[mask]
    return df


def _stock_lot_inventory(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory is None or inventory.empty:
        return pd.DataFrame() if inventory is None else inventory.copy()
    holdings = inventory.copy()
    if "source" in holdings.columns:
        holdings = holdings[holdings["source"] == "stock_lot"]
    return holdings


def build_assigned_holdings_frame(inventory: pd.DataFrame) -> pd.DataFrame:
    holdings = _stock_lot_inventory(inventory)
    if holdings is None or holdings.empty or "ticker" not in holdings.columns:
        return holdings

    grouped_rows = []
    for ticker, group in holdings.groupby(holdings["ticker"].astype(str).str.upper().str.strip(), sort=True):
        shares = pd.to_numeric(group.get("shares"), errors="coerce").fillna(0)
        share_total = float(shares.sum())
        weights = shares.abs()

        cost = pd.to_numeric(group.get("cost_per_share"), errors="coerce")
        current = pd.to_numeric(group.get("current_price"), errors="coerce")
        unrealized = pd.to_numeric(group.get("unrealized_pnl"), errors="coerce")
        covered_shares = pd.to_numeric(group.get("covered_shares"), errors="coerce").fillna(0)
        covered_strike = pd.to_numeric(group.get("covered_strike"), errors="coerce")
        buy_dates = pd.to_datetime(group.get("buy_date"), errors="coerce")

        cost_basis = float((cost.fillna(0) * weights).sum()) if weights.sum() else 0.0
        avg_cost = cost_basis / float(weights.sum()) if weights.sum() else np.nan

        current_weights = weights.where(current.notna(), 0)
        avg_current = (
            float((current.fillna(0) * current_weights).sum() / current_weights.sum())
            if current_weights.sum()
            else np.nan
        )

        covered_strikes = covered_strike.loc[covered_shares.gt(0) & covered_strike.notna()].drop_duplicates()
        strike_mixed = len(covered_strikes) > 1
        lot_count = int(len(group))
        first_buy = buy_dates.min() if buy_dates.notna().any() else pd.NaT
        latest_buy = buy_dates.max() if buy_dates.notna().any() else pd.NaT

        grouped_rows.append(
            {
                "ticker": ticker,
                "buy_date": latest_buy,
                "first_buy_date": first_buy,
                "latest_buy_date": latest_buy,
                "lot_count": lot_count,
                "shares": int(round(share_total)),
                "cost_per_share": avg_cost,
                "current_price": avg_current,
                "covered_shares": int(round(float(covered_shares.sum()))),
                "covered_strike": np.nan if strike_mixed or covered_strikes.empty else float(covered_strikes.iloc[0]),
                "covered_strike_mixed": bool(strike_mixed),
                "unrealized_pnl": float(unrealized.sum()) if unrealized.notna().any() else np.nan,
                "source": "stock_lot" if lot_count == 1 else "stock_group",
            }
        )

    return pd.DataFrame(grouped_rows)


def build_assigned_holding_lots_frame(inventory: pd.DataFrame) -> pd.DataFrame:
    return _stock_lot_inventory(inventory)


def build_open_option_shorts_frame(open_options: pd.DataFrame, stock_prices: Dict[str, float]) -> pd.DataFrame:
    if open_options is None or open_options.empty:
        return pd.DataFrame() if open_options is None else open_options.copy()

    options = open_options.copy()
    options["current_price"] = options["ticker"].map(stock_prices or {})
    strike_num = pd.to_numeric(options["strike"], errors="coerce")
    current_num = pd.to_numeric(options["current_price"], errors="coerce")
    valid_moneyness = strike_num.notna() & current_num.notna() & (strike_num != 0)
    options["moneyness_pct"] = np.nan

    put_mask = (options["type"] == "Put") & valid_moneyness
    call_mask = (options["type"] == "Call") & valid_moneyness
    options.loc[put_mask, "moneyness_pct"] = (strike_num[put_mask] - current_num[put_mask]) / strike_num[put_mask]
    options.loc[call_mask, "moneyness_pct"] = (current_num[call_mask] - strike_num[call_mask]) / strike_num[call_mask]
    return options
