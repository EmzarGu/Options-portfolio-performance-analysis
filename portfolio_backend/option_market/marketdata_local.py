"""Opt-in local adapter; never constructs a Firestore store or writes production.

The existing engine consumes these contracts unchanged. Results remain an
evaluation until separate Greek timestamps and the production age policy are
agreed. Saved selection responses make subsequent reads network-free.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

from portfolio_backend.option_market.decision_data import build_decision_option_universe
from portfolio_backend.option_market.marketdata import MarketDataError, Selection, latest_free_session, normalize_chain
from portfolio_backend.option_market.models import stable_hash


def plan_requests(groups, *, as_of: date):
    universe = build_decision_option_universe(groups, as_of=as_of, provider="marketdata")
    states = {g["ticker"]: g.get("current_state") or {} for g in groups}
    plan = []
    for request in universe.requests:
        state = states[request.ticker]
        selections = []
        # Both legs of a roll must be priced. Include every current leg, not
        # merely the first option for a ticker.
        for option in state.get("open_options") or []:
            if str(option.get("expiry"))[:10] == request.expiry.isoformat() and str(option.get("type", "")).upper() == request.put_call:
                if option.get("strike") is not None:
                    selections.append(Selection(strike=float(option["strike"])))
        selections += [Selection(delta=value) for value in (.15, .30)]
        # Basis may be an unlisted fractional strike. A no_data result is valid;
        # never round it into a claim that the basis strike exists.
        if request.put_call == "CALL" and state.get("cost_basis"):
            selections.append(Selection(strike=float(state["cost_basis"])))
        for selection in dict.fromkeys(selections):
            plan.append((request, selection))
    return universe, plan


def write_private_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".pending")
    temp.write_text(json.dumps(data, indent=2, allow_nan=False))
    temp.chmod(0o600)
    temp.replace(path)


def load_local_market_data(groups, *, as_of: date, root: Path, client=None,
                           refresh=False, force=False, now=None, time_limit=60):
    now = now or datetime.now(timezone.utc)
    expected = latest_free_session(now)
    universe, plan = plan_requests(groups, as_of=as_of)
    deadline = time.monotonic() + time_limit
    contracts = {}
    rows = []
    stopped = None
    for request, selection in plan:
        # Key includes exact selector and request analysis date; cached results
        # still undergo freshness validation on every read.
        key = stable_hash({**request.as_dict(), **selection.parameters()}, length=32)
        path = root / "responses" / f"{key}.json"
        origin = "stored"
        error = None
        try:
            saved = json.loads(path.read_text()) if path.exists() else None
            if saved is not None and not isinstance(saved, dict):
                raise ValueError
        except (ValueError, OSError):
            saved = None
            error = "Invalid saved response"
        cache_current = False
        if saved:
            try:
                cached = normalize_chain(saved['body'], request, fetched_at=datetime.fromisoformat(saved['fetched_at']))
                cache_current = bool(cached) and all(c.trade_date == expected for c in cached)
                if not cached:
                    fetched = datetime.fromisoformat(saved['fetched_at'])
                    cache_current = fetched.date() == now.date() and latest_free_session(fetched) == expected
            except (MarketDataError, ValueError, KeyError, TypeError):
                pass
        if refresh and client is not None and stopped is None and (force or not cache_current):
            try:
                result = client.fetch_selected(request, selection, deadline=deadline)
                if any(c.trade_date != expected for c in result.contracts):
                    raise MarketDataError("Quote is not from the latest available free-plan session")
                saved = {"body": result.raw_pages[0], "fetched_at": result.fetched_at,
                         "request": request.as_dict(), "selection": selection.parameters(),
                         "latency_ms": result.latency_ms, "status_code": result.status_code}
                write_private_json(path, saved)
                origin = "provider"
                error = None
            except MarketDataError as exc:
                # Stop on account/transport/schema failures. Leave each last
                # successful response intact and report any cached fallback.
                stopped = error = str(exc)
        parsed = []
        if saved:
            try:
                fetched = datetime.fromisoformat(saved["fetched_at"])
                parsed = normalize_chain(saved["body"], request, fetched_at=fetched)
            except (MarketDataError, ValueError, KeyError, TypeError):
                error = "Invalid saved response"
        accepted = []
        for contract in parsed:
            if contract.trade_date != expected:
                error = "Quote is not from the latest available free-plan session"
                continue
            if selection.strike is not None and contract.strike != selection.strike:
                error = "Saved strike differs from selection"
                continue
            # A delta target is nearest-neighbour, not a guarantee. Existing
            # engine eligibility applies to the returned delta, not the target.
            accepted.append(contract.contract_symbol)
            contracts[contract.contract_symbol] = {**contract.as_dict(), "updated_at": saved["fetched_at"]}
        rows.append({**request.as_dict(), "selection": selection.parameters(),
                     "source": origin if saved else "missing", "symbols": accepted,
                     "status": "error" if error else "ok" if accepted else "no_data" if saved else "not_fetched",
                     "error": error, "fetched_at": saved.get("fetched_at") if saved else None})
    values = list(contracts.values())
    # Do not combine conflicting quote dates across legs/tickers; the strict
    # session gate above also prevents a newly downloaded old quote passing.
    status = {"provider": "marketdata", "source": "local_evaluation", "status": "empty_universe" if not plan else "partial" if stopped or any(r["status"] in ("error", "not_fetched") for r in rows) else "succeeded",
        "message": "Dated local evaluation; field timing unverified. No execution prices.",
        "production_ready": False, "field_timing_verified": False, "error": stopped,
        "contract_count": len(values), "request_count": len(plan), "expected_quote_date": expected.isoformat(),
        "quote_coverage_count": sum(c["bid"] is not None and c["ask"] is not None for c in values),
        "greek_coverage_count": sum(c["delta"] is not None for c in values),
        "last_fetched_at": max((r["fetched_at"] for r in rows if r["fetched_at"]), default=None)}
    return {"universe": universe.as_dict(), "contracts": values, "status": status, "coverage": rows}
