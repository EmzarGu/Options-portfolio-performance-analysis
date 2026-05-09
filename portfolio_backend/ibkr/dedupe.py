from __future__ import annotations

import hashlib
import json
from typing import Mapping, Optional


RUN_SPECIFIC_FIELDS = {
    "reportDate",
}


def _present(*values: Optional[str]) -> bool:
    return all(str(value or "").strip() for value in values)


def canonical_row_hash(section: str, attrs: Mapping[str, object]) -> str:
    stable_attrs = {
        str(key): value
        for key, value in attrs.items()
        if str(key) not in RUN_SPECIFIC_FIELDS and value not in (None, "")
    }
    payload = f"{section}\n" + json.dumps(stable_attrs, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def natural_key(section: str, attrs: Mapping[str, str]) -> Optional[str]:
    account_id = attrs.get("accountId")
    if section == "Trade":
        trade_id = attrs.get("tradeID")
        transaction_id = attrs.get("transactionID")
        ib_exec_id = attrs.get("ibExecID")
        if _present(account_id, trade_id, transaction_id, ib_exec_id):
            return f"trade|{account_id}|{trade_id}|{transaction_id}|{ib_exec_id}"
        if _present(account_id, trade_id, transaction_id):
            return f"trade|{account_id}|{trade_id}|{transaction_id}"
    if section == "OptionEAE":
        trade_id = attrs.get("tradeID")
        date = attrs.get("date")
        transaction_type = attrs.get("transactionType")
        conid = attrs.get("conid")
        quantity = attrs.get("quantity")
        if _present(account_id, trade_id, date, transaction_type, conid, quantity):
            return f"option_eae|{account_id}|{trade_id}|{date}|{transaction_type}|{conid}|{quantity}"
    if section == "CashTransaction":
        transaction_id = attrs.get("transactionID")
        action_id = attrs.get("actionID")
        date_time = attrs.get("dateTime")
        cash_type = attrs.get("type")
        if _present(account_id, transaction_id, action_id, date_time, cash_type):
            return f"cash|{account_id}|{transaction_id}|{action_id}|{date_time}|{cash_type}"
    if section == "OpenPosition":
        report_date = attrs.get("reportDate")
        conid = attrs.get("conid")
        expiry = attrs.get("expiry")
        strike = attrs.get("strike")
        put_call = attrs.get("putCall")
        if _present(account_id, report_date, conid):
            return f"position|{account_id}|{report_date}|{conid}|{expiry or ''}|{strike or ''}|{put_call or ''}"
    return None


def dedupe_key(section: str, attrs: Mapping[str, str]) -> str:
    return natural_key(section, attrs) or f"hash|{canonical_row_hash(section, attrs)}"


def raw_row_id(section: str, attrs: Mapping[str, str]) -> str:
    return hashlib.sha256(dedupe_key(section, attrs).encode("utf-8")).hexdigest()
