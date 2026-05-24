from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(data: Any, *, length: int = 16) -> str:
    encoded = json.dumps(data, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:length]


def normalize_put_call(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"P", "PUT"}:
        return "PUT"
    if text in {"C", "CALL"}:
        return "CALL"
    return text


def parse_probability(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    text = text.replace("%", "").replace(",", ".")
    try:
        parsed = float(text)
    except ValueError:
        return None
    if parsed > 1.0:
        parsed /= 100.0
    if parsed < 0.0 or parsed > 1.0:
        return None
    return parsed


def float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def int_or_none(value: Any) -> Optional[int]:
    parsed = float_or_none(value)
    if parsed is None:
        return None
    return int(parsed)


def date_from_value(value: Any) -> date:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("date value is required")
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text[:10])


@dataclass(frozen=True)
class OptionChainRequest:
    provider: str
    ticker: str
    trade_date: date
    expiry: date
    put_call: str

    @property
    def request_id(self) -> str:
        parts = [
            self.provider.lower(),
            self.ticker.upper(),
            self.trade_date.isoformat(),
            self.expiry.isoformat(),
            normalize_put_call(self.put_call),
        ]
        return "_".join(part.replace("-", "") for part in parts)

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "ticker": self.ticker.upper(),
            "trade_date": self.trade_date.isoformat(),
            "expiry": self.expiry.isoformat(),
            "put_call": normalize_put_call(self.put_call),
            "request_id": self.request_id,
        }


@dataclass(frozen=True)
class OptionMarketContract:
    provider: str
    request_id: str
    ticker: str
    trade_date: date
    expiry: date
    put_call: str
    strike: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark: Optional[float] = None
    underlying_price: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    volatility: Optional[float] = None
    open_interest: Optional[int] = None
    volume: Optional[int] = None
    contract_symbol: Optional[str] = None
    raw: Optional[dict[str, Any]] = None

    @property
    def contract_id(self) -> str:
        data = {
            "provider": self.provider,
            "ticker": self.ticker,
            "trade_date": self.trade_date.isoformat(),
            "expiry": self.expiry.isoformat(),
            "put_call": self.put_call,
            "strike": round(float(self.strike), 6),
            "contract_symbol": self.contract_symbol,
        }
        return f"{self.request_id}_{stable_hash(data)}"

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["trade_date"] = self.trade_date.isoformat()
        data["expiry"] = self.expiry.isoformat()
        data["put_call"] = normalize_put_call(self.put_call)
        data["ticker"] = self.ticker.upper()
        data["contract_id"] = self.contract_id
        data["updated_at"] = now_iso()
        return data


def contract_from_dict(data: dict[str, Any]) -> OptionMarketContract:
    return OptionMarketContract(
        provider=str(data["provider"]),
        request_id=str(data["request_id"]),
        ticker=str(data["ticker"]).upper(),
        trade_date=date_from_value(data["trade_date"]),
        expiry=date_from_value(data["expiry"]),
        put_call=normalize_put_call(data["put_call"]),
        strike=float(data["strike"]),
        bid=float_or_none(data.get("bid")),
        ask=float_or_none(data.get("ask")),
        mark=float_or_none(data.get("mark")),
        underlying_price=float_or_none(data.get("underlying_price")),
        delta=float_or_none(data.get("delta")),
        gamma=float_or_none(data.get("gamma")),
        theta=float_or_none(data.get("theta")),
        vega=float_or_none(data.get("vega")),
        volatility=float_or_none(data.get("volatility")),
        open_interest=int_or_none(data.get("open_interest")),
        volume=int_or_none(data.get("volume")),
        contract_symbol=data.get("contract_symbol"),
        raw=data.get("raw") if isinstance(data.get("raw"), dict) else None,
    )


@dataclass(frozen=True)
class OptionMarketFetchResult:
    request: OptionChainRequest
    contracts: list[OptionMarketContract]
    raw_pages: list[dict[str, Any]]
    fetched_at: str
    latency_ms: int
    status_code: int
    error: Optional[str] = None

    def as_snapshot_doc(self) -> dict[str, Any]:
        return {
            **self.request.as_dict(),
            "fetched_at": self.fetched_at,
            "latency_ms": self.latency_ms,
            "status_code": self.status_code,
            "error": self.error,
            "contract_count": len(self.contracts),
            "raw_pages": self.raw_pages,
            "updated_at": now_iso(),
        }


@dataclass(frozen=True)
class OptionTradeCandidate:
    trade_id: str
    ticker: str
    trade_date: date
    expiry: date
    put_call: str
    strike: float
    qty: float
    trade_price: float
    net_cash: float
    source: str
    profit_probability: Optional[float] = None

    @property
    def request_key(self) -> tuple[str, date, date, str]:
        return (self.ticker.upper(), self.trade_date, self.expiry, normalize_put_call(self.put_call))

    @property
    def assignment_risk_proxy(self) -> Optional[float]:
        if self.profit_probability is None:
            return None
        return 1.0 - self.profit_probability

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["trade_date"] = self.trade_date.isoformat()
        data["expiry"] = self.expiry.isoformat()
        data["put_call"] = normalize_put_call(self.put_call)
        data["assignment_risk_proxy"] = self.assignment_risk_proxy
        return data


@dataclass(frozen=True)
class OptionMarketMatch:
    trade: OptionTradeCandidate
    request_id: str
    contract_id: Optional[str]
    matched: bool
    delta_risk: Optional[float]
    mark_minus_fill: Optional[float]
    bid_ask_contains_fill: Optional[bool]
    warnings: list[str]

    @property
    def match_id(self) -> str:
        return stable_hash({"trade": self.trade.as_dict(), "request_id": self.request_id}, length=24)

    def as_dict(self) -> dict[str, Any]:
        return {
            "match_id": self.match_id,
            "trade": self.trade.as_dict(),
            "request_id": self.request_id,
            "contract_id": self.contract_id,
            "matched": self.matched,
            "delta_risk": self.delta_risk,
            "sheet_assignment_risk_proxy": self.trade.assignment_risk_proxy,
            "mark_minus_fill": self.mark_minus_fill,
            "bid_ask_contains_fill": self.bid_ask_contains_fill,
            "warnings": list(self.warnings),
            "updated_at": now_iso(),
        }
