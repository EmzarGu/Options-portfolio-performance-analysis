"""Shared Market Data snapshots and transactional quota/refresh coordination."""
from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from portfolio_backend.gcp import firestore_client
from portfolio_backend.market_calendar import is_us_market_trading_day
from portfolio_backend.option_market.marketdata import MarketDataClient, MarketDataError, NY, latest_free_session, normalize_chain
from portfolio_backend.option_market.marketdata_local import plan_requests
from portfolio_backend.option_market.models import stable_hash

COLLECTION = "decision_lab_marketdata"
MAX_DISPLAY_QUOTE_AGE_DAYS = 7


def quota_window(now):
    local = now.astimezone(NY)
    opening = local.replace(hour=9, minute=30, second=0, microsecond=0)
    if local < opening:
        opening -= timedelta(days=1)
    return opening.isoformat()


def selection_key(request, selection):
    # The snapshot document supplies observation date. Reuse it across an
    # accounting-date change, e.g. the following European morning.
    return stable_hash({"ticker": request.ticker, "expiry": request.expiry.isoformat(),
                        "side": request.put_call, **selection.parameters()}, length=32)


class FirestoreMarketDataStore:
    def __init__(self, client=None):
        self.client = client or firestore_client()
        self.state_ref = self.client.collection(COLLECTION).document("control")

    def state(self):
        return self.state_ref.get().to_dict() or {}

    def mutate(self, change):
        from google.cloud import firestore
        @firestore.transactional
        def transaction(tx):
            state = self.state_ref.get(transaction=tx).to_dict() or {}
            updated = change(state)
            tx.set(self.state_ref, updated)
            return updated
        return transaction(self.client.transaction())

    def load(self, session):
        return self.client.collection(COLLECTION).document(session.isoformat()).get().to_dict() or {}

    def load_previous(self, session, *, earliest):
        # One bounded database read; normal page loads never call the provider.
        dates = []
        day = session - timedelta(days=1)
        while day >= earliest:
            if is_us_market_trading_day(day):
                dates.append(day)
            day -= timedelta(days=1)
        if not dates:
            return []
        refs = [self.client.collection(COLLECTION).document(day.isoformat()) for day in dates]
        docs = {doc.id: doc.to_dict() or {} for doc in self.client.get_all(refs)}
        return [(day, docs.get(day.isoformat(), {})) for day in dates]

    def save(self, session, responses, *, owner=None):
        document = {
            "responses": responses, "quote_date": session.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(), "schema_version": 1}
        ref = self.client.collection(COLLECTION).document(session.isoformat())
        if owner is None:
            ref.set(document)
            return
        from google.cloud import firestore
        @firestore.transactional
        def transaction(tx):
            state = self.state_ref.get(transaction=tx).to_dict() or {}
            if state.get('owner') != owner or state.get('lease_until', 0) <= time.time():
                raise MarketDataError('Refresh lease expired; newer saved data retained')
            tx.set(ref, document)
        transaction(self.client.transaction())


class SharedCredits:
    def __init__(self, store, owner, *, now=None, limit=90):
        self.store, self.owner, self.limit = store, owner, min(90, max(1, limit))
        self.now = now or (lambda: datetime.now(timezone.utc))

    def _owned(self, state):
        if state.get("owner") != self.owner or state.get("lease_until", 0) <= self.now().timestamp():
            raise MarketDataError("Another refresh owns the options update; please try later")

    def reserve(self):
        now = self.now()
        def change(state):
            self._owned(state)
            window = quota_window(now)
            if state.get("quota_window") != window:
                state.update(quota_window=window, used=0, remaining=None)
            if state.get("used", 0) >= self.limit or state.get("remaining") == 0:
                raise MarketDataError("Daily option-data allowance reached; saved quotes remain available")
            state["used"] = state.get("used", 0) + 1
            if state.get("remaining") is not None:
                state["remaining"] = max(0, state["remaining"] - 1)
            state["last_provider_call"] = now.timestamp()
            return state
        self.store.mutate(change)

    def reconcile(self, headers):
        h = {k.lower(): v for k,v in headers.items()}
        try:
            consumed, remaining = int(h["x-api-ratelimit-consumed"]), int(h["x-api-ratelimit-remaining"])
            if consumed < 0 or remaining < 0: raise ValueError
        except (ValueError, KeyError, TypeError):
            raise MarketDataError("Credit accounting unavailable; refresh stopped") from None
        def change(state):
            self._owned(state)
            state["used"] = max(state.get("used", 1) + consumed - 1, 100 - remaining)
            state["remaining"] = remaining
            return state
        self.store.mutate(change)


def _acquire(store, owner, now, seconds):
    def change(state):
        if state.get("lease_until", 0) > now.timestamp():
            raise MarketDataError("Option data is already being updated")
        # Avoid rapid switching between Cloud Run egress IPs on the free plan.
        if now.timestamp() - state.get("last_provider_call", 0) < 300:
            raise MarketDataError("Recent option update saved; please wait five minutes before another fetch")
        state.update(owner=owner, lease_until=now.timestamp()+seconds+20)
        return state
    store.mutate(change)


def _release(store, owner):
    def change(state):
        if state.get("owner") == owner:
            state.update(owner=None, lease_until=0)
        return state
    store.mutate(change)


def _validated_snapshot(responses, plan, session):
    contracts, missing, invalid = {}, set(), False
    for request, selection in plan:
        key = selection_key(request, selection)
        record = responses.get(key)
        if record is None:
            missing.add(key)
            continue
        try:
            fetched = datetime.fromisoformat(record["fetched_at"])
            rows = normalize_chain(record["body"], request, fetched_at=fetched)
            if any(c.trade_date != session or (selection.strike is not None and c.strike != selection.strike) for c in rows):
                raise MarketDataError("Unexpected saved quote")
            for contract in rows:
                contracts[contract.contract_symbol] = {**contract.as_dict(), "updated_at": record["fetched_at"]}
        except (MarketDataError, ValueError, TypeError, KeyError):
            missing.add(key)
            invalid = True
    return list(contracts.values()), missing, invalid


def load_market_data(groups, *, refresh=False, store=None, client_factory=None, now=None, time_limit=20):
    store = store or FirestoreMarketDataStore()
    now = now or datetime.now(timezone.utc)
    expected = latest_free_session(now)
    universe, plan = plan_requests(groups, as_of=now.astimezone(NY).date())
    snapshot = store.load(expected)
    responses = dict(snapshot.get("responses") or {})
    _, missing, _ = _validated_snapshot(responses, plan, expected)
    error = None
    refreshed = 0
    if refresh and missing:
        owner = uuid.uuid4().hex
        acquired = False
        try:
            _acquire(store, owner, now, time_limit)
            acquired = True
            # A previous writer may have finished between initial read and lease.
            responses = dict(store.load(expected).get("responses") or {})
            _, missing, _ = _validated_snapshot(responses, plan, expected)
            credits = SharedCredits(store, owner)
            client = (client_factory or (lambda ledger: MarketDataClient(os.getenv("MARKETDATA_API_TOKEN", ""), ledger=ledger)))(credits)
            deadline = time.monotonic() + time_limit
            for request, selection in plan:
                key = selection_key(request, selection)
                if key not in missing:
                    continue
                if time.monotonic() >= deadline:
                    raise MarketDataError("Partial update saved; fetch again later to complete coverage")
                result = client.fetch_selected(request, selection, deadline=deadline)
                if any(c.trade_date != expected for c in result.contracts):
                    raise MarketDataError("Provider quote date did not match the latest available session")
                responses[key] = {"body": result.raw_pages[0], "fetched_at": result.fetched_at,
                                  "request": request.as_dict(), "selection": selection.parameters()}
                refreshed += 1
        except MarketDataError as exc:
            error = str(exc)
        finally:
            if acquired:
                try:
                    if refreshed:
                        store.save(expected, responses, owner=owner)
                finally:
                    _release(store, owner)
    values, missing, invalid = _validated_snapshot(responses, plan, expected)
    missing_count = len(missing)
    if invalid and not error:
        error = "Some saved quotes failed validation and were excluded"
    displayed_date = expected
    if missing_count:
        earliest = now.astimezone(NY).date() - timedelta(days=MAX_DISPLAY_QUOTE_AGE_DAYS)
        for session, previous in store.load_previous(expected, earliest=earliest):
            older, older_missing, _ = _validated_snapshot(previous.get("responses") or {}, plan, session)
            if not older_missing:
                # Publish one coherent quote date, only when every selection for
                # the current portfolio has a valid response (including no_data).
                values, displayed_date = older, session
                break
    fallback = displayed_date != expected
    state = store.state()
    current_window = state.get("quota_window") == quota_window(now)
    message = "Dated planning estimates, not live prices. Delta and IV timestamps are not separately supplied."
    if fallback:
        message += f" Showing complete saved selections dated {displayed_date.isoformat()} while quotes dated {expected.isoformat()} are incomplete."
    elif missing_count:
        message += " Showing partial data; no recent complete dataset covers the current selections."
    if error:
        message += " " + error
    elif missing_count:
        message += " Some quotes are not prepared yet. Fetch option data to update, or wait for scheduled preparation."
    return {"universe": universe.as_dict(), "contracts": values, "status": {
        "provider": "marketdata", "source": "stored_fallback" if fallback else "provider_refresh" if refreshed else "stored",
        "status": "partial" if missing_count or error else "succeeded" if plan else "empty_universe",
        "message": message, "quote_date": displayed_date.isoformat(), "field_timing_verified": False,
        "latest_quote_date": expected.isoformat(), "using_previous_complete": fallback,
        "prepared_request_count": len(plan) - missing_count,
        "display_missing_request_count": 0 if fallback else missing_count,
        "contract_count": len(values), "request_count": len(plan), "missing_request_count": missing_count,
        "quote_coverage_count": sum(c["bid"] is not None and c["ask"] is not None for c in values),
        "greek_coverage_count": sum(c["delta"] is not None for c in values),
        "last_fetched_at": max((c["raw"]["fetched_at"] for c in values), default=None),
        "credits_used": state.get("used", 0) if current_window else 0,
        "credits_limit": 90, "account_daily_limit": 100,
    }}


def decision_loader(*, refresh=False):
    return lambda _situations, _cycle, groups, _payload: load_market_data(groups, refresh=refresh)


def prepare_from_context(context):
    """Reuse the existing scheduled import; never fail valid IBKR imports on feed outage."""
    from portfolio_backend.decision_lab import build_decision_lab_data
    from portfolio_backend.mobile_api_service import build_mobile_positions_payload, build_mobile_tickers_payload
    payload = {"dashboard": {"request": context.request}, "positions": build_mobile_positions_payload(context),
               "tickers": build_mobile_tickers_payload(context, include_history=False)}
    groups = build_decision_lab_data(payload)["recommendation_candidates"]
    return load_market_data(groups, refresh=True, time_limit=90)["status"]
