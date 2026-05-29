from __future__ import annotations

import os
import time
from datetime import date
from time import perf_counter
from typing import Any, Optional
from urllib.parse import urljoin

import requests

from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionMarketContract,
    OptionMarketFetchResult,
    date_from_value,
    float_or_none,
    int_or_none,
    normalize_put_call,
    now_iso,
)


class CuteMarketsClient:
    provider = "cutemarkets"

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: str = "https://api.cutemarkets.com",
        timeout: int = 20,
        session: Any = None,
    ) -> None:
        self.api_key = api_key or os.getenv("CUTEMARKETS_API_KEY", "").strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.min_interval_seconds = float(os.getenv("CUTEMARKETS_MIN_REQUEST_INTERVAL_SECONDS", "0") or 0)
        self._last_request_at = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def fetch_chain(self, request: OptionChainRequest, *, limit: int = 100) -> OptionMarketFetchResult:
        if not self.api_key:
            raise RuntimeError("CUTEMARKETS_API_KEY is not configured")

        started = perf_counter()
        url = f"{self.base_url}/v1/options/chain/{request.ticker.upper()}"
        params: dict[str, Any] = {
            "expiration_date": request.expiry.isoformat(),
            "contract_type": "put" if normalize_put_call(request.put_call) == "PUT" else "call",
            "limit": limit,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        raw_pages: list[dict[str, Any]] = []
        contracts: list[OptionMarketContract] = []
        status_code = 0
        error = None

        while url:
            response = self._get(url, params=params, headers=headers)
            status_code = response.status_code
            try:
                payload = response.json()
            except Exception:
                payload = {"text": response.text}
            raw_pages.append(_compact_page(payload))
            if response.status_code >= 400:
                error = _error_message(payload) or f"HTTP {response.status_code}"
                break
            for row in _result_rows(payload):
                contract = normalize_cutemarkets_contract(row, request)
                if contract is not None:
                    contracts.append(contract)
            next_url = payload.get("next_url") if isinstance(payload, dict) else None
            url = urljoin(self.base_url, str(next_url)) if next_url else ""
            params = None

        return OptionMarketFetchResult(
            request=request,
            contracts=contracts,
            raw_pages=raw_pages,
            fetched_at=now_iso(),
            latency_ms=round((perf_counter() - started) * 1000),
            status_code=status_code,
            error=error,
        )

    def fetch_historical_contracts(
        self,
        *,
        ticker: str,
        trade_date: date,
        expiry: date,
        put_call: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not self.api_key:
            raise RuntimeError("CUTEMARKETS_API_KEY is not configured")
        url = f"{self.base_url}/v1/options/contracts/"
        params: dict[str, Any] = {
            "underlying_ticker": ticker.upper(),
            "as_of": trade_date.isoformat(),
            "expiration_date": expiry.isoformat(),
            "contract_type": "put" if normalize_put_call(put_call) == "PUT" else "call",
            "limit": limit,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        rows: list[dict[str, Any]] = []
        while url:
            response = self._get(url, params=params, headers=headers)
            payload = response.json() if response.content else {}
            if response.status_code >= 400:
                raise RuntimeError(_error_message(payload) or f"HTTP {response.status_code}")
            rows.extend(_result_rows(payload))
            next_url = payload.get("next_url") if isinstance(payload, dict) else None
            url = urljoin(self.base_url, str(next_url)) if next_url else ""
            params = None
        return rows

    def fetch_option_daily_aggregate(self, contract_symbol: str, trade_date: date) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("CUTEMARKETS_API_KEY is not configured")
        symbol = str(contract_symbol).strip()
        url = f"{self.base_url}/v1/options/aggs/{symbol}/1/day/{trade_date.isoformat()}/{trade_date.isoformat()}/"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = self._get(url, params={"limit": 10}, headers=headers)
        payload = response.json() if response.content else {}
        if response.status_code >= 400:
            raise RuntimeError(_error_message(payload) or f"HTTP {response.status_code}")
        rows = _result_rows(payload)
        return rows[0] if rows else {}

    def _get(self, url: str, *, params: Optional[dict[str, Any]], headers: dict[str, str]) -> Any:
        for attempt in range(3):
            self._throttle()
            response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            self._last_request_at = time.monotonic()
            if response.status_code != 429 or attempt == 2:
                return response
            retry_after = response.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else self.min_interval_seconds
            except ValueError:
                wait = self.min_interval_seconds
            time.sleep(max(wait, self.min_interval_seconds, 1.0))
        return response

    def _throttle(self) -> None:
        if self.min_interval_seconds <= 0 or self._last_request_at <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)


def normalize_cutemarkets_contract(row: dict[str, Any], request: OptionChainRequest) -> Optional[OptionMarketContract]:
    details = row.get("details") if isinstance(row.get("details"), dict) else {}
    greeks = row.get("greeks") if isinstance(row.get("greeks"), dict) else {}
    quote = row.get("last_quote") if isinstance(row.get("last_quote"), dict) else {}
    trade = row.get("last_trade") if isinstance(row.get("last_trade"), dict) else {}
    day = row.get("day") if isinstance(row.get("day"), dict) else {}
    underlying = row.get("underlying_asset") if isinstance(row.get("underlying_asset"), dict) else {}

    strike = float_or_none(details.get("strike_price") or row.get("strike_price"))
    if strike is None:
        return None
    expiry = _optional_date(details.get("expiration_date") or row.get("expiration_date")) or request.expiry
    put_call = normalize_put_call(details.get("contract_type") or row.get("contract_type") or request.put_call)
    bid = float_or_none(quote.get("bid"))
    ask = float_or_none(quote.get("ask"))
    mark, price_source = _mark_price(row, quote, trade, day)
    return OptionMarketContract(
        provider=request.provider,
        request_id=request.request_id,
        ticker=request.ticker.upper(),
        trade_date=request.trade_date,
        expiry=expiry,
        put_call=put_call,
        strike=strike,
        bid=bid,
        ask=ask,
        mark=mark,
        underlying_price=float_or_none(underlying.get("price") or row.get("underlying_price")),
        delta=float_or_none(greeks.get("delta")),
        gamma=float_or_none(greeks.get("gamma")),
        theta=float_or_none(greeks.get("theta")),
        vega=float_or_none(greeks.get("vega")),
        volatility=float_or_none(row.get("implied_volatility") or row.get("iv")),
        open_interest=int_or_none(row.get("open_interest")),
        volume=int_or_none(day.get("volume") or trade.get("size") or row.get("volume")),
        contract_symbol=details.get("ticker") or row.get("ticker"),
        raw={
            "price_source": price_source,
            "has_quote": bid is not None or ask is not None,
            "has_greeks": any(greeks.get(name) is not None for name in ("delta", "gamma", "theta", "vega")),
            "source": row,
        },
    )


def _mark_price(
    row: dict[str, Any],
    quote: dict[str, Any],
    trade: dict[str, Any],
    day: dict[str, Any],
) -> tuple[Optional[float], Optional[str]]:
    for key, source in [
        ("midpoint", "quote_midpoint"),
        ("fmv", "fmv"),
    ]:
        value = float_or_none((quote if key == "midpoint" else row).get(key))
        if value is not None:
            return value, source
    if float_or_none(quote.get("bid")) is not None and float_or_none(quote.get("ask")) is not None:
        return (float_or_none(quote.get("bid")) + float_or_none(quote.get("ask"))) / 2, "quote_bid_ask_mid"
    for key, source in [
        ("close", "day_close"),
        ("vwap", "day_vwap"),
        ("price", "last_trade"),
    ]:
        source_row = trade if source == "last_trade" else day
        value = float_or_none(source_row.get(key))
        if value is not None:
            return value, source
    return None, None


def _optional_date(value: Any) -> Optional[date]:
    try:
        return date_from_value(value)
    except Exception:
        return None


def _result_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        results = payload.get("results") or payload.get("data") or []
    elif isinstance(payload, list):
        results = payload
    else:
        results = []
    return [row for row in results if isinstance(row, dict)]


def _compact_page(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"data_count": 0}
    rows = _result_rows(payload)
    return {
        "meta": {
            "status": payload.get("status"),
            "request_id": payload.get("request_id"),
            "next_url": bool(payload.get("next_url")),
        },
        "data_count": len(rows),
    }


def _error_message(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    value = payload.get("error") or payload.get("message") or payload.get("detail")
    return str(value) if value else None
