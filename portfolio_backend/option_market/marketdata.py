"""Bounded Market Data access for dated Decision Lab comparisons.

Production coordination lives in marketdata_production. No historical Greek
backfill, automatic retries, or unbounded chain queries. The token only goes to
the fixed provider host in an auth header.
"""
from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from portfolio_backend.market_calendar import is_us_market_trading_day, previous_us_market_trading_day
from portfolio_backend.option_market.models import OptionChainRequest, OptionMarketContract, OptionMarketFetchResult

NY = ZoneInfo("America/New_York")


class MarketDataError(RuntimeError):
    """Safe error message; never includes request headers or provider error text."""


def latest_free_session(now: datetime) -> date:
    local = now.astimezone(NY)
    # Identify the last session that has OPENED, then take its predecessor.
    session = local.date()
    if not is_us_market_trading_day(session) or (local.hour, local.minute, local.second) < (9, 30, 1):
        session = previous_us_market_trading_day(session - timedelta(days=1))
    return previous_us_market_trading_day(session - timedelta(days=1))


@dataclass(frozen=True)
class Selection:
    strike: float | None = None
    delta: float | None = None

    def parameters(self) -> dict[str, str]:
        if (self.strike is None) == (self.delta is None):
            raise ValueError("Select exactly one strike or absolute delta")
        value = self.strike if self.strike is not None else self.delta
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError("Selection must be positive and finite")
        if self.delta is not None and value >= 1:
            raise ValueError("Delta must be between zero and one")
        # Do not combine strikeLimit and delta: the near-money limit is applied
        # before delta selection and can defeat the intended target.
        return {"strike" if self.strike is not None else "delta": f"{value:g}"}


class CreditLedger:
    """Conservative per-UTC-day cap. Caller holds an exclusive run lock.

    Reserve before sending; uncertain responses retain their reservation. The
    provider's remaining allowance can lower, never increase, the local budget.
    This file is for one local runner, not a distributed production quota store.
    """
    def __init__(self, path: Path, *, daily_limit: int = 80, initial_used: int = 0, now=None):
        if not 1 <= daily_limit <= 100 or not 0 <= initial_used <= 100:
            raise ValueError("Invalid free-account credit budget")
        self.path, self.limit = Path(path), daily_limit
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {}
        today = self.now().astimezone(timezone.utc).date().isoformat()
        if self.state.get("day") != today:
            self.state = {"day": today, "used": initial_used, "remaining": None}
        self._save()

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".pending")
        temp.write_text(json.dumps(self.state, indent=2))
        temp.chmod(0o600)
        temp.replace(self.path)

    def reserve(self):
        if self.state["used"] >= self.limit or self.state.get("remaining") == 0:
            raise MarketDataError("Local daily credit ceiling reached")
        self.state["used"] += 1
        if self.state.get("remaining") is not None:
            self.state["remaining"] = max(0, self.state["remaining"] - 1)
        self._save()

    def reconcile(self, headers):
        h = {k.lower(): v for k, v in headers.items()}
        try:
            consumed = int(h["x-api-ratelimit-consumed"])
            remaining = int(h["x-api-ratelimit-remaining"])
            if consumed < 0 or remaining < 0:
                raise ValueError
        except (KeyError, ValueError, TypeError):
            raise MarketDataError("Missing credit accounting; stop further requests") from None
        self.state["used"] += consumed - 1
        self.state["provider_remaining"] = remaining
        previous = self.state.get("remaining")
        self.state["remaining"] = remaining if previous is None else min(previous, remaining)
        self._save()


def _number(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise MarketDataError("Malformed numeric field")
    return value


def normalize_chain(body: dict[str, Any], request: OptionChainRequest, *, fetched_at: datetime) -> list[OptionMarketContract]:
    if not isinstance(body, dict):
        raise MarketDataError("Malformed option response")
    if body.get("s") == "no_data":
        return []
    if body.get("s") != "ok":
        raise MarketDataError("Provider did not return option data")
    required = ("optionSymbol", "underlying", "expiration", "side", "strike", "updated", "bid", "ask", "delta")
    symbols = body.get("optionSymbol")
    if not isinstance(symbols, list) or len(symbols) > 1:
        raise MarketDataError("Single-contract query returned an unexpected row count")
    for key in required:
        if not isinstance(body.get(key), list) or len(body[key]) != len(symbols):
            raise MarketDataError("Incomplete option response columns")
    for key, values in body.items():
        if isinstance(values, list) and len(values) != len(symbols):
            raise MarketDataError("Mismatched option response columns")
    contracts = []
    for i, symbol in enumerate(symbols):
        row = {key: values[i] for key, values in body.items() if isinstance(values, list)}
        try:
            observed = datetime.fromtimestamp(_number(row["updated"]), timezone.utc)
            expiry = datetime.fromtimestamp(_number(row["expiration"]), NY).date()
        except (TypeError, ValueError, OverflowError, OSError):
            raise MarketDataError("Invalid option timestamp") from None
        if observed > fetched_at or expiry != request.expiry:
            raise MarketDataError("Unexpected quote date or expiration")
        if row["underlying"] != request.ticker or str(row["side"]).upper() != request.put_call:
            raise MarketDataError("Response does not match the requested contract group")
        strike = _number(row["strike"])
        if strike is None or strike <= 0:
            raise MarketDataError("Invalid strike")
        expected = f"{request.ticker}{expiry:%y%m%d}{request.put_call[0]}{round(strike * 1000):08d}"
        if symbol != expected:
            raise MarketDataError("Unexpected or adjusted contract symbol")
        bid, ask, delta = (_number(row[k]) for k in ("bid", "ask", "delta"))
        if delta is not None and (abs(delta) > 1 or (request.put_call == "CALL" and delta < 0) or (request.put_call == "PUT" and delta > 0)):
            raise MarketDataError("Invalid delta sign or magnitude")
        contracts.append(OptionMarketContract(
            provider="marketdata", request_id=request.request_id, ticker=request.ticker,
            trade_date=observed.astimezone(NY).date(), expiry=expiry, put_call=request.put_call,
            strike=strike, bid=bid, ask=ask, mark=(bid + ask) / 2 if bid is not None and ask is not None else None,
            underlying_price=_number(row.get("underlyingPrice")), delta=delta,
            volatility=_number(row.get("iv")), gamma=_number(row.get("gamma")),
            theta=_number(row.get("theta")), vega=_number(row.get("vega")),
            open_interest=_number(row.get("openInterest")), volume=_number(row.get("volume")),
            contract_symbol=symbol, raw={"price_source": "quote_midpoint", "feed_type": "free_account_dated",
                "observed_at": observed.isoformat(), "fetched_at": fetched_at.isoformat(),
                "greek_observed_at": None, "field_timing_verified": False, "source": row},
        ))
    return contracts


class MarketDataClient:
    provider = "marketdata"

    def __init__(self, token: str, *, ledger: CreditLedger, session=None, now=None):
        self._token = token.strip()
        self.configured = bool(self._token)
        self.ledger = ledger
        self.session = session or requests.Session()
        self.now = now or (lambda: datetime.now(timezone.utc))

    def fetch_selected(self, request: OptionChainRequest, selection: Selection, *, deadline=None):
        if not self.configured:
            raise MarketDataError("MARKETDATA_API_TOKEN is not configured")
        if request.provider != self.provider or not re.fullmatch(r"[A-Z][A-Z.\-]{0,9}", request.ticker) or request.put_call not in ("CALL", "PUT"):
            raise ValueError("Invalid Market Data chain request")
        now = self.now()
        if request.trade_date != now.astimezone(NY).date():
            raise ValueError("Default-chain requests must use today's analysis date")
        if request.expiry < request.trade_date:
            raise ValueError("Cannot request an expired default chain")
        params = {"side": request.put_call.lower(), "expiration": request.expiry.isoformat(),
                  "nonstandard": "false", **selection.parameters()}
        remaining = 15 if deadline is None else min(15, deadline - time.monotonic())
        if remaining <= 0:
            raise MarketDataError("Local refresh time limit reached")
        self.ledger.reserve()
        start = time.monotonic()
        try:
            response = self.session.get(f"https://api.marketdata.app/v1/options/chain/{request.ticker}/",
                params=params, headers={"Authorization": f"Bearer {self._token}"},
                timeout=(min(5, remaining / 2), remaining / 2), allow_redirects=False)
        except requests.RequestException:
            raise MarketDataError("Options service unavailable; reserved credit retained") from None
        self.ledger.reconcile(response.headers)
        if response.status_code == 404:
            # Provider SDKs treat an absent contract as an empty result. An
            # unlisted basis strike must not abort all later portfolio tickers.
            return OptionMarketFetchResult(request, [], [{"s": "no_data", "http_status": 404}],
                self.now().isoformat(), int((time.monotonic() - start) * 1000), 404)
        if response.status_code not in (200, 203):
            raise MarketDataError(f"Options service returned HTTP {response.status_code}; no retry")
        try:
            body = response.json()
        except ValueError:
            raise MarketDataError("Options service returned invalid JSON") from None
        fetched = self.now()
        contracts = normalize_chain(body, request, fetched_at=fetched)
        if selection.strike is not None and any(c.strike != selection.strike for c in contracts):
            raise MarketDataError("Response strike differs from requested strike")
        return OptionMarketFetchResult(request, contracts, [body], fetched.isoformat(),
            int((time.monotonic() - start) * 1000), response.status_code)
