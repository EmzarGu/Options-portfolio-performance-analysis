from __future__ import annotations

import os
import time
from datetime import date
from typing import Any, Optional

import requests

from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionMarketContract,
    OptionMarketFetchResult,
    float_or_none,
    int_or_none,
    normalize_put_call,
    now_iso,
)


class OptionChainIQClient:
    """Provider adapter for OptionChainIQ historical option contracts.

    OptionChainIQ filters by `put_call`; strike filtering is intentionally done
    locally because API probing showed strike-style filters can be ignored.
    """

    provider = "optionchainiq"

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        session: Any = None,
        timeout: int = 30,
        page_limit: int = 500,
        max_pages: int = 20,
    ) -> None:
        self.api_key = api_key or os.getenv("OPTIONCHAINIQ_API_KEY")
        if not self.api_key:
            raise ValueError("OPTIONCHAINIQ_API_KEY is required for OptionChainIQ requests.")
        self.base_url = (base_url or os.getenv("OPTIONCHAINIQ_BASE_URL") or "https://api.optionchainiq.com").rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.page_limit = page_limit
        self.max_pages = max_pages

    def fetch_historical_chain(
        self,
        *,
        ticker: str,
        trade_date: date,
        expiry: date,
        put_call: str,
    ) -> OptionMarketFetchResult:
        request = OptionChainRequest(
            provider=self.provider,
            ticker=ticker.upper(),
            trade_date=trade_date,
            expiry=expiry,
            put_call=normalize_put_call(put_call),
        )
        started = time.monotonic()
        raw_pages: list[dict[str, Any]] = []
        contracts: list[OptionMarketContract] = []
        status_code = 0
        error: Optional[str] = None
        offset = 0
        headers = {"X-API-Key": self.api_key}

        try:
            for _ in range(self.max_pages):
                response = self.session.get(
                    f"{self.base_url}/v1/contracts",
                    headers=headers,
                    params={
                        "symbol": request.ticker,
                        "date": request.trade_date.isoformat(),
                        "expiration": request.expiry.isoformat(),
                        "put_call": request.put_call,
                        "limit": self.page_limit,
                        "offset": offset,
                    },
                    timeout=self.timeout,
                )
                status_code = response.status_code
                response.raise_for_status()
                payload = response.json()
                raw_pages.append(payload)
                page_contracts = payload.get("data") or []
                if not isinstance(page_contracts, list):
                    page_contracts = []
                contracts.extend(_normalize_contracts(request, page_contracts))
                meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
                count = int(meta.get("count") or len(page_contracts))
                total = int(meta.get("total") or len(contracts))
                offset += count
                if count <= 0 or offset >= total:
                    break
        except Exception as exc:
            error = str(exc)

        latency_ms = int((time.monotonic() - started) * 1000)
        return OptionMarketFetchResult(
            request=request,
            contracts=contracts,
            raw_pages=raw_pages,
            fetched_at=now_iso(),
            latency_ms=latency_ms,
            status_code=status_code,
            error=error,
        )


def _normalize_contracts(request: OptionChainRequest, rows: list[dict[str, Any]]) -> list[OptionMarketContract]:
    out: list[OptionMarketContract] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        put_call = normalize_put_call(row.get("put_call") or row.get("option_type") or row.get("type"))
        if put_call and put_call != request.put_call:
            continue
        strike = float_or_none(row.get("strike"))
        if strike is None:
            continue
        out.append(
            OptionMarketContract(
                provider=request.provider,
                request_id=request.request_id,
                ticker=request.ticker,
                trade_date=request.trade_date,
                expiry=request.expiry,
                put_call=request.put_call,
                strike=strike,
                bid=float_or_none(row.get("bid")),
                ask=float_or_none(row.get("ask")),
                mark=float_or_none(row.get("mark") or row.get("mid") or row.get("last")),
                underlying_price=float_or_none(row.get("underlying_price") or row.get("underlyingPrice")),
                delta=float_or_none(row.get("delta")),
                gamma=float_or_none(row.get("gamma")),
                theta=float_or_none(row.get("theta")),
                vega=float_or_none(row.get("vega")),
                volatility=float_or_none(row.get("volatility") or row.get("iv") or row.get("implied_volatility")),
                open_interest=int_or_none(row.get("open_interest") or row.get("openInterest")),
                volume=int_or_none(row.get("volume")),
                contract_symbol=row.get("option_symbol") or row.get("symbol") or row.get("contract_symbol"),
                raw=row,
            )
        )
    return out
