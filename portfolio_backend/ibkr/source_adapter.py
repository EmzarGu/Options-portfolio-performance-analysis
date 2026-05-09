from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Set, Tuple

import pandas as pd

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow


OPTION_TYPE_MAP = {
    "P": "Put",
    "PUT": "Put",
    "C": "Call",
    "CALL": "Call",
}

ACTION_MAP = {
    "BUY": "Buy",
    "SELL": "Sell",
}


def _blank_to_none(value):
    text = str(value).strip() if value is not None else ""
    return text or None


def _float_or_none(value):
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


def _option_key_from_attrs(attrs: Mapping[str, str]) -> Tuple[str, str, float, pd.Timestamp]:
    ticker = (_blank_to_none(attrs.get("underlyingSymbol")) or _blank_to_none(attrs.get("symbol")) or "").upper()
    option_type = OPTION_TYPE_MAP.get(str(attrs.get("putCall", "")).upper(), "")
    strike = _float_or_none(attrs.get("strike"))
    expiry = _date_or_nat(attrs.get("expiry"))
    return ticker, option_type, float(strike) if strike is not None else float("nan"), expiry.normalize() if pd.notna(expiry) else pd.NaT


def assignment_keys(option_eae_rows: Iterable[IbkrRawRow]) -> Set[Tuple[str, str, float, pd.Timestamp]]:
    keys = set()
    for row in option_eae_rows:
        transaction_type = str(row.attrs.get("transactionType", "")).lower()
        if "assignment" not in transaction_type:
            continue
        key = _option_key_from_attrs(row.attrs)
        if key[0] and key[1] and pd.notna(key[3]):
            keys.add(key)
    return keys


def option_trades_to_dataframe(
    trade_rows: Iterable[IbkrRawRow],
    option_eae_rows: Iterable[IbkrRawRow] = (),
    *,
    short_strategy_only: bool = True,
) -> pd.DataFrame:
    """Map IBKR option trade rows to the current pipeline's sheet-shaped DataFrame.

    This is a compatibility adapter. It intentionally emits the same columns as
    `load_options_from_excel_bytes` so downstream pipeline behavior can be
    compared before changing the active app source.

    The active pipeline models short option strategies: a `Sell` opens a short
    lot and a later `Buy` closes it. IBKR also returns long option legs where a
    `Buy` opens and a `Sell` closes. Those rows are retained in normalized raw
    transactions, but excluded from this compatibility adapter by default.
    """
    assigned_keys = assignment_keys(option_eae_rows)
    rows = []
    for row in trade_rows:
        attrs = row.attrs
        if attrs.get("assetCategory") != "OPT":
            continue
        if _is_system_book_trade(attrs):
            continue
        action = ACTION_MAP.get(str(attrs.get("buySell", "")).upper())
        option_type = OPTION_TYPE_MAP.get(str(attrs.get("putCall", "")).upper())
        if not action or not option_type:
            continue
        open_close = str(attrs.get("openCloseIndicator", "")).upper()
        if short_strategy_only and not _is_short_strategy_row(action, open_close):
            continue

        key = _option_key_from_attrs(attrs)
        proceeds = _float_or_none(attrs.get("proceeds"))
        commission = _float_or_none(attrs.get("ibCommission"))
        net_cash = _float_or_none(attrs.get("netCash"))
        qty = _float_or_none(attrs.get("quantity"))
        rows.append(
            {
                "trans_date": _date_or_nat(attrs.get("tradeDate") or attrs.get("dateTime")),
                "ticker": key[0],
                "type": option_type,
                "action": action,
                "expiration": key[3],
                "strike": key[2],
                "qty": abs(qty) if qty is not None else None,
                "amount": proceeds,
                "commission": abs(commission) if commission is not None else 0.0,
                "total_pnl": net_cash if net_cash is not None else proceeds,
                "assigned_flag": 1.0 if action == "Sell" and key in assigned_keys else 0.0,
                "comment": _comment(attrs),
                "source_sheet": _source_label(attrs),
                "ibkr_trade_id": attrs.get("tradeID"),
                "ibkr_transaction_id": attrs.get("transactionID"),
                "ibkr_exec_id": attrs.get("ibExecID"),
                "ibkr_open_close": attrs.get("openCloseIndicator"),
                "ibkr_asset_category": attrs.get("assetCategory"),
            }
        )
    return pd.DataFrame(rows, columns=_option_dataframe_columns())


def _option_dataframe_columns():
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


def _is_short_strategy_row(action: str, open_close: str) -> bool:
    if open_close == "O":
        return action == "Sell"
    if open_close == "C":
        return action == "Buy"
    # If IBKR omits the indicator, preserve the row for later reconciliation.
    return not open_close


def _is_system_book_trade(attrs: Mapping[str, str]) -> bool:
    if str(attrs.get("transactionType", "")).upper() != "BOOKTRADE":
        return False
    note = str(attrs.get("notes", "")).upper()
    return note in {"A", "EP"}


def _comment(attrs: Mapping[str, str]) -> str:
    parts = []
    for key in ("transactionType", "openCloseIndicator", "notes"):
        value = _blank_to_none(attrs.get(key))
        if value:
            parts.append(f"{key}={value}")
    return "; ".join(parts)


def _source_label(attrs: Mapping[str, str]) -> str:
    trade_date = _date_or_nat(attrs.get("tradeDate") or attrs.get("dateTime"))
    if pd.notna(trade_date):
        return f"IBKR Flex {trade_date.year}"
    return "IBKR Flex"


def options_dataframe_from_report(report: IbkrFlexReport, *, short_strategy_only: bool = True) -> pd.DataFrame:
    return option_trades_to_dataframe(
        report.rows("Trade"),
        report.rows("OptionEAE"),
        short_strategy_only=short_strategy_only,
    )


def summarize_options_frame(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "rows": 0,
            "action_type_counts": {},
            "action_sums": {},
            "assigned_rows": 0,
        }
    action_type_counts = (
        df.groupby(["action", "type"]).size().rename("rows").reset_index().to_dict(orient="records")
    )
    sums = (
        df.groupby("action")[["qty", "amount", "commission", "total_pnl"]]
        .sum(numeric_only=True)
        .round(6)
        .reset_index()
        .to_dict(orient="records")
    )
    return {
        "rows": int(len(df)),
        "date_min": str(pd.to_datetime(df["trans_date"]).min().date()) if df["trans_date"].notna().any() else None,
        "date_max": str(pd.to_datetime(df["trans_date"]).max().date()) if df["trans_date"].notna().any() else None,
        "action_type_counts": action_type_counts,
        "action_sums": sums,
        "assigned_rows": int(pd.to_numeric(df["assigned_flag"], errors="coerce").fillna(0).gt(0).sum()),
    }


def group_key_counts(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    key_counts = defaultdict(int)
    for row in df.itertuples(index=False):
        key = (
            row.ticker,
            row.type,
            row.action,
            pd.to_datetime(row.expiration).date().isoformat() if pd.notna(row.expiration) else "",
            float(row.strike) if pd.notna(row.strike) else None,
        )
        key_counts[key] += 1
    return {str(key): count for key, count in sorted(key_counts.items(), key=lambda item: str(item[0]))}
