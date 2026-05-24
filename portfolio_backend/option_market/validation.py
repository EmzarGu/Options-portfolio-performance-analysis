from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

import pandas as pd

from portfolio_backend.ibkr.option_accounting import IbkrOptionExecution, option_executions_from_report
from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionMarketContract,
    OptionMarketMatch,
    OptionTradeCandidate,
    float_or_none,
    normalize_put_call,
    parse_probability,
    stable_hash,
)


PROFIT_PROBABILITY_COLUMNS = (
    "profit_probability",
    "Profit probability",
    "Profit probability (>70%)",
    "Profit probability \n(>70%)",
    "Profit probability\n(>70%)",
)


@dataclass(frozen=True)
class ValidationReport:
    summary: dict[str, Any]
    matches: list[dict[str, Any]]
    bucket_summary: list[dict[str, Any]]
    warnings: list[str]


def candidates_from_ibkr_report(report: Any, *, year: int = 2024) -> list[OptionTradeCandidate]:
    executions = option_executions_from_report(report, short_strategy_only=True)
    opens = [
        execution
        for execution in executions
        if execution.date.year == year and execution.action == "Sell" and execution.open_close == "O"
    ]
    return [candidate_from_execution(execution) for execution in opens]


def candidate_from_execution(execution: IbkrOptionExecution) -> OptionTradeCandidate:
    trade_id = (
        execution.trade_id
        or execution.transaction_id
        or execution.ib_exec_id
        or stable_hash(execution.as_dict(), length=24)
    )
    trade_price = abs(execution.net_cash) / max(execution.qty * execution.multiplier, 1.0)
    return OptionTradeCandidate(
        trade_id=str(trade_id),
        ticker=execution.ticker.upper(),
        trade_date=execution.date.date(),
        expiry=execution.expiration.date(),
        put_call=normalize_put_call(execution.otype),
        strike=float(execution.strike),
        qty=float(execution.qty),
        trade_price=float(trade_price),
        net_cash=float(execution.net_cash),
        source="ibkr",
    )


def extract_sheet_probability_rows(sheet_df: pd.DataFrame, *, year: int = 2024) -> list[dict[str, Any]]:
    if sheet_df.empty:
        return []
    probability_column = _find_probability_column(sheet_df)
    if probability_column is None:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in sheet_df.iterrows():
        probability = parse_probability(row.get(probability_column))
        if probability is None:
            continue
        trans_date = _to_date(row.get("trans_date") or row.get("Trans date"))
        expiry = _to_date(row.get("expiration") or row.get("Expiration"))
        if trans_date is None or expiry is None or trans_date.year != year:
            continue
        ticker = str(row.get("ticker") or row.get("Tiker") or "").upper().strip()
        put_call = normalize_put_call(row.get("type") or row.get("Type"))
        strike = float_or_none(row.get("strike") or row.get("Strike"))
        action = str(row.get("action") or row.get("Action") or "").title().strip()
        if not ticker or not put_call or strike is None or action != "Sell":
            continue
        rows.append(
            {
                "ticker": ticker,
                "trade_date": trans_date,
                "expiry": expiry,
                "put_call": put_call,
                "strike": strike,
                "profit_probability": probability,
                "source_sheet": row.get("source_sheet"),
            }
        )
    return rows


def attach_sheet_probabilities(
    candidates: Iterable[OptionTradeCandidate],
    sheet_probability_rows: Iterable[dict[str, Any]],
) -> tuple[list[OptionTradeCandidate], list[dict[str, Any]]]:
    by_key: dict[tuple[str, date, date, str, float], list[dict[str, Any]]] = {}
    used: Counter[tuple[str, date, date, str, float]] = Counter()
    for row in sheet_probability_rows:
        key = _probability_key(row["ticker"], row["trade_date"], row["expiry"], row["put_call"], row["strike"])
        by_key.setdefault(key, []).append(row)

    resolved: list[OptionTradeCandidate] = []
    for candidate in candidates:
        key = _probability_key(
            candidate.ticker,
            candidate.trade_date,
            candidate.expiry,
            candidate.put_call,
            candidate.strike,
        )
        probability_rows = by_key.get(key) or []
        probability = probability_rows[0].get("profit_probability") if probability_rows else None
        if probability_rows:
            used[key] += 1
        resolved.append(
            OptionTradeCandidate(
                trade_id=candidate.trade_id,
                ticker=candidate.ticker,
                trade_date=candidate.trade_date,
                expiry=candidate.expiry,
                put_call=candidate.put_call,
                strike=candidate.strike,
                qty=candidate.qty,
                trade_price=candidate.trade_price,
                net_cash=candidate.net_cash,
                source=candidate.source,
                profit_probability=probability,
            )
        )

    unmatched = []
    for key, rows in by_key.items():
        if used[key]:
            continue
        unmatched.extend(rows)
    return resolved, unmatched


def dedupe_chain_requests(candidates: Iterable[OptionTradeCandidate], *, provider: str) -> list[OptionChainRequest]:
    requests: dict[tuple[str, date, date, str], OptionChainRequest] = {}
    for candidate in candidates:
        key = candidate.request_key
        requests[key] = OptionChainRequest(
            provider=provider,
            ticker=key[0],
            trade_date=key[1],
            expiry=key[2],
            put_call=key[3],
        )
    return sorted(requests.values(), key=lambda r: (r.trade_date, r.ticker, r.expiry, r.put_call))


def match_trade_to_contract(
    candidate: OptionTradeCandidate,
    request_id: str,
    contracts: Iterable[OptionMarketContract],
    *,
    strike_tolerance: float = 0.001,
) -> OptionMarketMatch:
    compatible = [
        contract
        for contract in contracts
        if contract.ticker.upper() == candidate.ticker.upper()
        and contract.expiry == candidate.expiry
        and normalize_put_call(contract.put_call) == normalize_put_call(candidate.put_call)
        and abs(float(contract.strike) - float(candidate.strike)) <= strike_tolerance
    ]
    warnings: list[str] = []
    if not compatible:
        return OptionMarketMatch(
            trade=candidate,
            request_id=request_id,
            contract_id=None,
            matched=False,
            delta_risk=None,
            mark_minus_fill=None,
            bid_ask_contains_fill=None,
            warnings=["missing_provider_contract"],
        )
    contract = compatible[0]
    delta_risk = abs(contract.delta) if contract.delta is not None else None
    if delta_risk is None:
        warnings.append("missing_delta")
    mark_minus_fill = contract.mark - candidate.trade_price if contract.mark is not None else None
    bid_ask_contains_fill = None
    if contract.bid is not None and contract.ask is not None:
        low, high = sorted([contract.bid, contract.ask])
        bid_ask_contains_fill = low <= candidate.trade_price <= high
        if not bid_ask_contains_fill:
            warnings.append("fill_outside_provider_bid_ask")
    if contract.underlying_price is None:
        warnings.append("missing_underlying_price")
    return OptionMarketMatch(
        trade=candidate,
        request_id=request_id,
        contract_id=contract.contract_id,
        matched=True,
        delta_risk=delta_risk,
        mark_minus_fill=mark_minus_fill,
        bid_ask_contains_fill=bid_ask_contains_fill,
        warnings=warnings,
    )


def build_validation_report(matches: Iterable[OptionMarketMatch], unmatched_sheet_rows: Iterable[dict[str, Any]]) -> ValidationReport:
    match_docs = [match.as_dict() for match in matches]
    matched_docs = [doc for doc in match_docs if doc["matched"]]
    matched_with_delta = [doc for doc in matched_docs if doc.get("delta_risk") is not None]
    warnings = sorted({warning for doc in match_docs for warning in doc.get("warnings", [])})
    unmatched_sheet = list(unmatched_sheet_rows)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "trade_count": len(match_docs),
        "matched_contract_count": len(matched_docs),
        "matched_contract_rate": _ratio(len(matched_docs), len(match_docs)),
        "matched_delta_count": len(matched_with_delta),
        "matched_delta_rate": _ratio(len(matched_with_delta), len(matched_docs)),
        "unmatched_sheet_probability_rows": len(unmatched_sheet),
        "warning_types": warnings,
    }
    return ValidationReport(
        summary=summary,
        matches=match_docs,
        bucket_summary=risk_bucket_summary(match_docs),
        warnings=warnings,
    )


def risk_bucket_summary(match_docs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = [
        ("<=15%", None, 0.15),
        ("15-20%", 0.15, 0.20),
        ("20-25%", 0.20, 0.25),
        ("25-30%", 0.25, 0.30),
        (">30%", 0.30, None),
    ]
    rows: list[dict[str, Any]] = []
    docs = list(match_docs)
    for label, low, high in buckets:
        bucket_docs = [
            doc
            for doc in docs
            if _in_bucket(doc.get("sheet_assignment_risk_proxy"), low, high)
        ]
        rows.append(
            {
                "bucket": label,
                "trades": len(bucket_docs),
                "matched": sum(1 for doc in bucket_docs if doc.get("matched")),
                "avg_sheet_assignment_risk_proxy": _avg(
                    doc.get("sheet_assignment_risk_proxy") for doc in bucket_docs
                ),
                "avg_provider_delta_risk": _avg(doc.get("delta_risk") for doc in bucket_docs),
                "avg_mark_minus_fill": _avg(doc.get("mark_minus_fill") for doc in bucket_docs),
            }
        )
    return rows


def _find_probability_column(df: pd.DataFrame) -> Optional[str]:
    normalized = {str(column).strip().replace("\r", ""): column for column in df.columns}
    for column in PROFIT_PROBABILITY_COLUMNS:
        if column in df.columns:
            return column
        if column in normalized:
            return normalized[column]
    lower = {str(column).strip().lower().replace("\n", " "): column for column in df.columns}
    for name, column in lower.items():
        if name.startswith("profit probability"):
            return column
    return None


def _to_date(value: Any) -> Optional[date]:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _probability_key(ticker: str, trade_date: date, expiry: date, put_call: str, strike: float):
    return (ticker.upper(), trade_date, expiry, normalize_put_call(put_call), round(float(strike), 6))


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    if denominator == 0:
        return None
    return round(numerator / denominator, 6)


def _avg(values: Iterable[Any]) -> Optional[float]:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 6)


def _in_bucket(value: Any, low: Optional[float], high: Optional[float]) -> bool:
    if value is None:
        return False
    numeric = float(value)
    if low is not None and numeric <= low:
        return False
    if high is not None and numeric > high:
        return False
    return True
