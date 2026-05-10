from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class OptionTrade:
    date: pd.Timestamp
    ticker: str
    otype: str  # "Put" or "Call"
    action: str  # "Sell" (open short) or "Buy" (close)
    strike: float
    expiration: pd.Timestamp
    qty: int
    price: float  # per-share net price (after commission; always positive)
    comment: str
    assigned: bool


@dataclass
class OptionLot:
    ticker: str
    otype: str
    strike: float
    qty: int
    open_date: pd.Timestamp
    expiration: pd.Timestamp
    open_price: float  # per-share net credit/debit when opened
    comment: str
    assigned: bool
    close_date: Optional[pd.Timestamp] = None
    close_price: Optional[float] = None
    close_reason: Optional[str] = None
    roll_adjusted_open_price: Optional[float] = None


@dataclass
class OptionPnLEvent:
    date: pd.Timestamp
    ticker: str
    otype: str
    strike: float
    qty: int
    pnl: float
    p_open: float
    p_close: float
    reason: str  # close | expiration | assignment


@dataclass
class StockTxn:
    date: pd.Timestamp
    ticker: str
    side: str  # "BUY" or "SELL"
    shares: int
    price: float
    source: str  # "Assigned"


@dataclass
class RealizedSale:
    date: pd.Timestamp
    ticker: str
    shares: int
    proceeds: float
    cost: float
    pnl: float
    source: str = ""


@dataclass
class OpenLot:
    ticker: str
    buy_date: pd.Timestamp
    shares_remaining: int
    cost_per_share: float


@dataclass
class HoldSeg:
    ticker: str
    start: pd.Timestamp
    end: pd.Timestamp
    shares: int
    cost_per_share: float


@dataclass
class ChainOutcome:
    ticker: str
    start: pd.Timestamp
    end: Optional[pd.Timestamp]
    option_pnl: float
    stock_pnl: float
    total_pnl: float


@dataclass
class PipelineState:
    df_opts: pd.DataFrame
    lots: List[OptionLot]
    stock_txns: List[StockTxn]
    realized_sales: List[RealizedSale]
    ending_inventory: List[OpenLot]
    capital_daily: pd.DataFrame
    monthly_cycles: pd.DataFrame
    monthly_returns_w_div: pd.Series
    monthly_returns_covered: pd.Series
    monthly_returns_unrealized_adjusted: pd.Series
    monthly_returns_active: pd.Series
    open_options: pd.DataFrame
    live_prices: Dict[str, float]
    inv_df: pd.DataFrame
    total_unreal: float
    option_unreal: float
    stock_unreal: float
    advanced_unreal: pd.Series
    yearly: pd.DataFrame
    yearly_with_unreal: pd.DataFrame
    per_ticker: pd.DataFrame
    div_df: pd.DataFrame
    as_of: pd.Timestamp
    issues: List[str]
    price_errors: List[str]
    unrealized_blocked: bool
    missing_required_price_tickers: List[str]
    price_summary: Dict[str, Any]
    price_updated_at: Optional[str]
    historical_price_summary: Dict[str, int]
    historical_price_errors: List[str]
    dividend_coverage_complete: bool
    dividend_attempted_tickers: List[str]
    dividend_failed_tickers: List[str]
    dividend_affected_tickers: List[str]
    dividend_errors: List[str]
    dividend_summary: Dict[str, int]
    stock_prices: Dict[str, float]
    benchmark_metrics: pd.DataFrame
    aligned_bench_returns: Dict[str, pd.Series]
    per_ticker_totals: pd.DataFrame
    grand_total: float
    cumulative_realized: float
    realized_option_events: List[OptionPnLEvent]
    chain_outcomes: List[ChainOutcome]
    sheet_counts: pd.DataFrame
    capital_history_incomplete: bool
    capital_history_coverage_issues: List[Dict[str, object]]
    capital_history_affected_months: List[pd.Timestamp]
    capital_history_affected_years: List[int]
    capital_history_affected_tickers: List[str]
    first_incomplete_return_month: Optional[pd.Timestamp]
    last_complete_return_month: Optional[pd.Timestamp]
    return_series_truncated: bool

    def as_dict(self) -> Dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def get(self, key: str, default=None) -> Any:
        return getattr(self, key, default)

    def keys(self) -> List[str]:
        return [field.name for field in fields(self)]

    def items(self):
        return self.as_dict().items()

    def __contains__(self, key: str) -> bool:
        return key in self.keys()
