from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

from portfolio_backend.ibkr.dedupe import dedupe_key, raw_row_id
from portfolio_backend.ibkr.flex_parser import IbkrRawRow


@dataclass(frozen=True)
class IbkrNormalizedTransaction:
    transaction_id: str
    source: str
    source_section: str
    raw_row_id: str
    account_id: Optional[str]
    currency: Optional[str]
    asset_category: Optional[str]
    sub_category: Optional[str]
    symbol: Optional[str]
    underlying_symbol: Optional[str]
    description: Optional[str]
    conid: Optional[str]
    trade_id: Optional[str]
    ib_exec_id: Optional[str]
    ib_order_id: Optional[str]
    ib_transaction_id: Optional[str]
    related_trade_id: Optional[str]
    related_transaction_id: Optional[str]
    trade_date: Optional[str]
    date_time: Optional[str]
    settle_date: Optional[str]
    expiry: Optional[str]
    strike: Optional[float]
    put_call: Optional[str]
    buy_sell: Optional[str]
    transaction_type: Optional[str]
    open_close_indicator: Optional[str]
    quantity: Optional[float]
    multiplier: Optional[float]
    trade_price: Optional[float]
    trade_money: Optional[float]
    proceeds: Optional[float]
    ib_commission: Optional[float]
    taxes: Optional[float]
    net_cash: Optional[float]
    cost_basis: Optional[float]
    realized_pnl: Optional[float]
    mtm_pnl: Optional[float]
    notes: Optional[str]
    dedupe_key: str

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _blank_to_none(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _float_or_none(value: Any) -> Optional[float]:
    text = _blank_to_none(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _date_or_none(value: Any) -> Optional[str]:
    text = _blank_to_none(value)
    if text is None:
        return None
    if len(text) == 8 and text.isdigit():
        try:
            return date(int(text[0:4]), int(text[4:6]), int(text[6:8])).isoformat()
        except ValueError:
            return text
    return text


def _datetime_or_none(value: Any) -> Optional[str]:
    text = _blank_to_none(value)
    if text is None:
        return None
    for separator in (";", " "):
        if separator in text:
            date_part, time_part = text.split(separator, 1)
            if len(date_part) == 8 and date_part.isdigit() and len(time_part) >= 6:
                try:
                    parsed = datetime(
                        int(date_part[0:4]),
                        int(date_part[4:6]),
                        int(date_part[6:8]),
                        int(time_part[0:2]),
                        int(time_part[2:4]),
                        int(time_part[4:6]),
                    )
                    return parsed.isoformat()
                except ValueError:
                    return text
    return _date_or_none(text)


def _transaction_id(section: str, attrs: Mapping[str, str]) -> str:
    return raw_row_id(section, attrs)


def normalize_trade(row: IbkrRawRow) -> IbkrNormalizedTransaction:
    attrs = row.attrs
    key = dedupe_key(row.section, attrs)
    return IbkrNormalizedTransaction(
        transaction_id=_transaction_id(row.section, attrs),
        source="ibkr_flex",
        source_section=row.section,
        raw_row_id=raw_row_id(row.section, attrs),
        account_id=_blank_to_none(attrs.get("accountId")),
        currency=_blank_to_none(attrs.get("currency")),
        asset_category=_blank_to_none(attrs.get("assetCategory")),
        sub_category=_blank_to_none(attrs.get("subCategory")),
        symbol=_blank_to_none(attrs.get("symbol")),
        underlying_symbol=_blank_to_none(attrs.get("underlyingSymbol")),
        description=_blank_to_none(attrs.get("description")),
        conid=_blank_to_none(attrs.get("conid")),
        trade_id=_blank_to_none(attrs.get("tradeID")),
        ib_exec_id=_blank_to_none(attrs.get("ibExecID")),
        ib_order_id=_blank_to_none(attrs.get("ibOrderID")),
        ib_transaction_id=_blank_to_none(attrs.get("transactionID")),
        related_trade_id=_blank_to_none(attrs.get("relatedTradeID")),
        related_transaction_id=_blank_to_none(attrs.get("relatedTransactionID")),
        trade_date=_date_or_none(attrs.get("tradeDate")),
        date_time=_datetime_or_none(attrs.get("dateTime")),
        settle_date=_date_or_none(attrs.get("settleDateTarget")),
        expiry=_date_or_none(attrs.get("expiry")),
        strike=_float_or_none(attrs.get("strike")),
        put_call=_blank_to_none(attrs.get("putCall")),
        buy_sell=_blank_to_none(attrs.get("buySell")),
        transaction_type=_blank_to_none(attrs.get("transactionType")),
        open_close_indicator=_blank_to_none(attrs.get("openCloseIndicator")),
        quantity=_float_or_none(attrs.get("quantity")),
        multiplier=_float_or_none(attrs.get("multiplier")),
        trade_price=_float_or_none(attrs.get("tradePrice")),
        trade_money=_float_or_none(attrs.get("tradeMoney")),
        proceeds=_float_or_none(attrs.get("proceeds")),
        ib_commission=_float_or_none(attrs.get("ibCommission")),
        taxes=_float_or_none(attrs.get("taxes")),
        net_cash=_float_or_none(attrs.get("netCash")),
        cost_basis=_float_or_none(attrs.get("cost")),
        realized_pnl=_float_or_none(attrs.get("fifoPnlRealized")),
        mtm_pnl=_float_or_none(attrs.get("mtmPnl")),
        notes=_blank_to_none(attrs.get("notes")),
        dedupe_key=key,
    )


def normalize_option_eae(row: IbkrRawRow) -> IbkrNormalizedTransaction:
    attrs = row.attrs
    key = dedupe_key(row.section, attrs)
    return IbkrNormalizedTransaction(
        transaction_id=_transaction_id(row.section, attrs),
        source="ibkr_flex",
        source_section=row.section,
        raw_row_id=raw_row_id(row.section, attrs),
        account_id=_blank_to_none(attrs.get("accountId")),
        currency=_blank_to_none(attrs.get("currency")),
        asset_category=_blank_to_none(attrs.get("assetCategory")),
        sub_category=_blank_to_none(attrs.get("subCategory")),
        symbol=_blank_to_none(attrs.get("symbol")),
        underlying_symbol=_blank_to_none(attrs.get("underlyingSymbol")),
        description=_blank_to_none(attrs.get("description")),
        conid=_blank_to_none(attrs.get("conid")),
        trade_id=_blank_to_none(attrs.get("tradeID")),
        ib_exec_id=None,
        ib_order_id=None,
        ib_transaction_id=None,
        related_trade_id=None,
        related_transaction_id=None,
        trade_date=_date_or_none(attrs.get("date")),
        date_time=_date_or_none(attrs.get("date")),
        settle_date=None,
        expiry=_date_or_none(attrs.get("expiry")),
        strike=_float_or_none(attrs.get("strike")),
        put_call=_blank_to_none(attrs.get("putCall")),
        buy_sell=None,
        transaction_type=_blank_to_none(attrs.get("transactionType")),
        open_close_indicator=None,
        quantity=_float_or_none(attrs.get("quantity")),
        multiplier=_float_or_none(attrs.get("multiplier")),
        trade_price=_float_or_none(attrs.get("tradePrice")),
        trade_money=None,
        proceeds=_float_or_none(attrs.get("proceeds")),
        ib_commission=_float_or_none(attrs.get("commisionsAndTax")),
        taxes=None,
        net_cash=None,
        cost_basis=_float_or_none(attrs.get("costBasis")),
        realized_pnl=_float_or_none(attrs.get("realizedPnl")),
        mtm_pnl=_float_or_none(attrs.get("mtmPnl")),
        notes=None,
        dedupe_key=key,
    )


def normalize_transactions(rows: Iterable[IbkrRawRow]) -> List[IbkrNormalizedTransaction]:
    normalized: List[IbkrNormalizedTransaction] = []
    for row in rows:
        if row.section == "Trade":
            normalized.append(normalize_trade(row))
        elif row.section == "OptionEAE":
            normalized.append(normalize_option_eae(row))
    return normalized


def redacted_preview(txn: IbkrNormalizedTransaction) -> Dict[str, Any]:
    data = txn.as_dict()
    for key in (
        "transaction_id",
        "raw_row_id",
        "account_id",
        "symbol",
        "underlying_symbol",
        "description",
        "conid",
        "trade_id",
        "ib_exec_id",
        "ib_order_id",
        "ib_transaction_id",
        "related_trade_id",
        "related_transaction_id",
        "dedupe_key",
    ):
        if data.get(key):
            data[key] = "<redacted>"
    return data
