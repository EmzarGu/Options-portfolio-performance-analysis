from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Optional

import pandas as pd

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow
from portfolio_backend.ibkr.source_adapter import ACTION_MAP, OPTION_TYPE_MAP


@dataclass(frozen=True)
class IbkrOptionExecution:
    date: pd.Timestamp
    ticker: str
    otype: str
    action: str
    open_close: str
    expiration: pd.Timestamp
    strike: float
    qty: float
    multiplier: float
    net_cash: float
    proceeds: float
    commission: float
    trade_id: Optional[str]
    transaction_id: Optional[str]
    ib_exec_id: Optional[str]

    def as_dict(self) -> dict:
        return asdict(self)


def _blank_to_none(value) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _float_or_none(value) -> Optional[float]:
    text = _blank_to_none(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


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


def _is_short_strategy_execution(action: str, open_close: str) -> bool:
    if open_close == "O":
        return action == "Sell"
    if open_close == "C":
        return action == "Buy"
    return not open_close


def option_executions_from_rows(
    trade_rows: Iterable[IbkrRawRow],
    *,
    short_strategy_only: bool = True,
) -> list[IbkrOptionExecution]:
    executions: list[IbkrOptionExecution] = []
    for row in trade_rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "OPT":
            continue
        action = ACTION_MAP.get(str(attrs.get("buySell", "")).upper())
        otype = OPTION_TYPE_MAP.get(str(attrs.get("putCall", "")).upper())
        if not action or not otype:
            continue
        open_close = str(attrs.get("openCloseIndicator", "")).upper()
        if short_strategy_only and not _is_short_strategy_execution(action, open_close):
            continue
        date = _date_or_nat(attrs.get("tradeDate") or attrs.get("dateTime"))
        expiration = _date_or_nat(attrs.get("expiry"))
        strike = _float_or_none(attrs.get("strike"))
        qty = _float_or_none(attrs.get("quantity"))
        multiplier = _float_or_none(attrs.get("multiplier")) or 100.0
        net_cash = _float_or_none(attrs.get("netCash"))
        proceeds = _float_or_none(attrs.get("proceeds"))
        commission = _float_or_none(attrs.get("ibCommission"))
        ticker = (_blank_to_none(attrs.get("underlyingSymbol")) or _blank_to_none(attrs.get("symbol")) or "").upper()
        if pd.isna(date) or pd.isna(expiration) or strike is None or qty is None:
            continue
        executions.append(
            IbkrOptionExecution(
                date=pd.to_datetime(date).normalize(),
                ticker=ticker,
                otype=otype,
                action=action,
                open_close=open_close,
                expiration=pd.to_datetime(expiration).normalize(),
                strike=float(strike),
                qty=abs(float(qty)),
                multiplier=float(multiplier),
                net_cash=float(net_cash if net_cash is not None else proceeds or 0.0),
                proceeds=float(proceeds or 0.0),
                commission=float(commission or 0.0),
                trade_id=_blank_to_none(attrs.get("tradeID")),
                transaction_id=_blank_to_none(attrs.get("transactionID")),
                ib_exec_id=_blank_to_none(attrs.get("ibExecID")),
            )
        )
    return sorted(
        executions,
        key=lambda e: (
            e.date,
            e.ticker,
            e.expiration,
            e.strike,
            e.otype,
            e.action,
            e.trade_id or "",
            e.transaction_id or "",
            e.ib_exec_id or "",
        ),
    )


def option_executions_from_report(
    report: IbkrFlexReport,
    *,
    short_strategy_only: bool = True,
) -> list[IbkrOptionExecution]:
    return option_executions_from_rows(report.rows("Trade"), short_strategy_only=short_strategy_only)


def executions_to_dataframe(executions: Iterable[IbkrOptionExecution]) -> pd.DataFrame:
    rows = [execution.as_dict() for execution in executions]
    return pd.DataFrame(
        rows,
        columns=[
            "date",
            "ticker",
            "otype",
            "action",
            "open_close",
            "expiration",
            "strike",
            "qty",
            "multiplier",
            "net_cash",
            "proceeds",
            "commission",
            "trade_id",
            "transaction_id",
            "ib_exec_id",
        ],
    )


def filter_executions(
    executions: Iterable[IbkrOptionExecution],
    *,
    since: Optional[pd.Timestamp] = None,
    through: Optional[pd.Timestamp] = None,
) -> list[IbkrOptionExecution]:
    since_ts = pd.to_datetime(since).normalize() if since is not None else None
    through_ts = pd.to_datetime(through).normalize() if through is not None else None
    filtered = []
    for execution in executions:
        if since_ts is not None and execution.date < since_ts:
            continue
        if through_ts is not None and execution.date > through_ts:
            continue
        filtered.append(execution)
    return filtered


def cashflow_summary(executions: Iterable[IbkrOptionExecution]) -> dict:
    df = executions_to_dataframe(executions)
    if df.empty:
        return {
            "rows": 0,
            "net_cash": 0.0,
            "sell_cash": 0.0,
            "buy_cash": 0.0,
            "date_min": None,
            "date_max": None,
            "by_year": [],
            "by_ticker": [],
        }
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year
    sells = df.loc[df["action"].eq("Sell"), "net_cash"].sum()
    buys = df.loc[df["action"].eq("Buy"), "net_cash"].sum()
    by_year = (
        df.groupby("year")
        .agg(
            rows=("ticker", "size"),
            sell_cash=("net_cash", lambda s: float(s[df.loc[s.index, "action"].eq("Sell")].sum())),
            buy_cash=("net_cash", lambda s: float(s[df.loc[s.index, "action"].eq("Buy")].sum())),
            net_cash=("net_cash", "sum"),
        )
        .reset_index()
        .round(6)
        .to_dict(orient="records")
    )
    by_ticker = (
        df.groupby("ticker")
        .agg(
            rows=("ticker", "size"),
            sell_cash=("net_cash", lambda s: float(s[df.loc[s.index, "action"].eq("Sell")].sum())),
            buy_cash=("net_cash", lambda s: float(s[df.loc[s.index, "action"].eq("Buy")].sum())),
            net_cash=("net_cash", "sum"),
        )
        .reset_index()
        .sort_values("net_cash", ascending=False)
        .round(6)
        .to_dict(orient="records")
    )
    return {
        "rows": int(len(df)),
        "net_cash": round(float(df["net_cash"].sum()), 6),
        "sell_cash": round(float(sells), 6),
        "buy_cash": round(float(buys), 6),
        "date_min": str(df["date"].min().date()) if df["date"].notna().any() else None,
        "date_max": str(df["date"].max().date()) if df["date"].notna().any() else None,
        "by_year": by_year,
        "by_ticker": by_ticker,
    }
