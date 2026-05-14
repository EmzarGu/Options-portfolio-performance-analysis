from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd


def build_options_cycle_chart_data(monthly_summary: pd.DataFrame) -> pd.DataFrame:
    if monthly_summary is None or monthly_summary.empty or "total_realized_pnl" not in monthly_summary.columns:
        return pd.DataFrame(columns=["Date", "pnl", "color"])
    pnl_df = monthly_summary[["total_realized_pnl"]].reset_index()
    first_col = pnl_df.columns[0]
    pnl_df = pnl_df.rename(columns={first_col: "Date", "total_realized_pnl": "pnl"})
    pnl_df["color"] = np.where(pnl_df["pnl"] >= 0, "Positive", "Negative")
    return pnl_df


def clean_monthly_return_series(ret_series: pd.Series, as_of: Optional[pd.Timestamp] = None) -> pd.Series:
    if ret_series is None or ret_series.empty:
        return pd.Series(dtype=float)
    returns = ret_series.copy()
    returns.index = pd.to_datetime(returns.index, errors="coerce")
    returns = returns[returns.index.notna()].sort_index()
    if returns.empty:
        return pd.Series(dtype=float)
    returns.index = returns.index.to_period("M").to_timestamp("M")
    if as_of is not None and pd.notna(as_of):
        returns = returns[returns.index <= pd.to_datetime(as_of).normalize()]
    return returns


def select_chart_return_window(
    ret_series: pd.Series,
    range_choice: str,
    as_of: Optional[pd.Timestamp] = None,
) -> pd.Series:
    returns = clean_monthly_return_series(ret_series, as_of=as_of)
    if returns.empty:
        return returns
    required_periods = {"3M": 3, "6M": 6, "1Y": 12}
    if range_choice in required_periods:
        n = required_periods[range_choice]
        window = returns.tail(n)
        if len(window) != n or not window.notna().all():
            return pd.Series(dtype=float)
        return window
    if range_choice == "YTD":
        latest_year = returns.index.max().year
        window = returns[returns.index.year == latest_year]
        if window.empty or not window.notna().all():
            return pd.Series(dtype=float)
        return window
    window = returns
    if window.empty or not window.notna().all():
        return pd.Series(dtype=float)
    return window


def build_benchmark_growth_chart_data(
    strategy_returns: pd.Series,
    aligned_bench_returns: Dict[str, pd.Series],
    range_choice: str,
    as_of: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    def growth_curve_frame(returns: pd.Series, series_name: str) -> pd.DataFrame:
        cumulative = (1 + returns).cumprod()
        normalized = cumulative / cumulative.iloc[0]
        return pd.DataFrame(
            {
                "Date": normalized.index.tolist(),
                "Series": series_name,
                "Growth": normalized.values.tolist(),
            }
        )

    curves = []
    strategy_window = select_chart_return_window(strategy_returns, range_choice, as_of=as_of)
    if not strategy_window.empty:
        curves.append(growth_curve_frame(strategy_window, "My Strategy"))
    else:
        return pd.DataFrame(columns=["Date", "Series", "Growth"])

    for name, series in (aligned_bench_returns or {}).items():
        benchmark_returns = clean_monthly_return_series(series, as_of=as_of).reindex(strategy_window.index)
        if benchmark_returns.notna().all():
            curves.append(growth_curve_frame(benchmark_returns, name))

    if not curves:
        return pd.DataFrame(columns=["Date", "Series", "Growth"])
    return pd.concat(curves, ignore_index=True)
