from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
import os
from typing import Any, Callable, Optional, Protocol

from portfolio_backend.option_market.cutemarkets import CuteMarketsClient
from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionMarketContract,
    contract_from_dict,
    normalize_put_call,
    now_iso,
    stable_hash,
)
from portfolio_backend.option_market.store import OptionMarketStore


@dataclass(frozen=True)
class DecisionOptionUniverse:
    universe_key: str
    provider: str
    requests: list[OptionChainRequest]

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe_key": self.universe_key,
            "provider": self.provider,
            "request_count": len(self.requests),
            "request_ids": [request.request_id for request in self.requests],
            "requests": [request.as_dict() for request in self.requests],
        }


@dataclass(frozen=True)
class DecisionOptionData:
    universe: DecisionOptionUniverse
    contracts: list[OptionMarketContract]
    status: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe": self.universe.as_dict(),
            "contracts": [contract.as_dict() for contract in self.contracts],
            "status": self.status,
        }


class DecisionOptionDataProvider(Protocol):
    provider: str
    configured: bool

    def fetch_chain(self, request: OptionChainRequest):
        ...


def build_decision_option_universe(
    candidate_groups: list[dict[str, Any]],
    *,
    as_of: date,
    provider: str = "cutemarkets",
) -> DecisionOptionUniverse:
    request_map: dict[tuple[str, date, str], OptionChainRequest] = {}
    request_order: list[tuple[str, date, str]] = []
    for group in candidate_groups:
        ticker = str(group.get("ticker") or "").upper()
        if not ticker:
            continue
        for request_row in group.get("contract_requests") or []:
            expiry = _date_or_none(request_row.get("expiry"))
            put_call = request_row.get("put_call")
            if expiry is None or put_call is None:
                continue
            put_call = normalize_put_call(put_call)
            key = (ticker, expiry, put_call)
            if key not in request_map:
                request_order.append(key)
            request_map[key] = OptionChainRequest(
                provider=provider,
                ticker=ticker,
                trade_date=as_of,
                expiry=expiry,
                put_call=put_call,
            )
        for candidate in group.get("candidates") or []:
            expiry = _date_or_none(candidate.get("expiry"))
            put_call = _candidate_put_call(candidate, group)
            if expiry is None or put_call is None:
                continue
            key = (ticker, expiry, put_call)
            if key not in request_map:
                request_order.append(key)
            request_map[key] = OptionChainRequest(
                provider=provider,
                ticker=ticker,
                trade_date=as_of,
                expiry=expiry,
                put_call=put_call,
            )
    requests = [request_map[key] for key in request_order]
    universe_key = stable_hash(
        {
            "provider": provider,
            "as_of": as_of.isoformat(),
            "requests": [
                {
                    "ticker": request.ticker,
                    "expiry": request.expiry.isoformat(),
                    "put_call": normalize_put_call(request.put_call),
                }
                for request in requests
            ],
        },
        length=24,
    )
    return DecisionOptionUniverse(universe_key=universe_key, provider=provider, requests=requests)


def load_or_fetch_decision_option_data(
    *,
    store: OptionMarketStore,
    universe: DecisionOptionUniverse,
    provider_client: DecisionOptionDataProvider,
    force_refresh: bool = False,
) -> DecisionOptionData:
    previous_run = store.load_latest_successful_fetch_run(
        universe_key=universe.universe_key,
        provider=universe.provider,
    )
    if previous_run and not force_refresh:
        contracts = _contracts_from_store(store, previous_run)
        return DecisionOptionData(
            universe=universe,
            contracts=contracts,
            status=_status_from_run(previous_run, contracts, source="stored"),
        )

    stored_contracts, stored_request_ids = _existing_contracts_for_universe(store, universe)
    expected_request_ids = {request.request_id for request in universe.requests}
    missing_request_ids = sorted(expected_request_ids - stored_request_ids)
    if stored_contracts and not force_refresh:
        return DecisionOptionData(
            universe=universe,
            contracts=stored_contracts,
            status={
                **_coverage_from_contracts(stored_contracts),
                "provider": universe.provider,
                "source": "stored" if not missing_request_ids else "stored_partial",
                "status": "succeeded",
                "message": (
                    "Loaded stored option contracts for the current universe without provider calls."
                    if not missing_request_ids
                    else f"Loaded stored option contracts; {len(missing_request_ids)} request group(s) are not stored yet."
                ),
                "contract_count": len(stored_contracts),
                "request_count": len(universe.requests),
                "stored_request_count": len(stored_request_ids),
                "missing_request_count": len(missing_request_ids),
                "request_ids": sorted(stored_request_ids),
                "missing_request_ids": missing_request_ids,
                "last_fetched_at": _latest_contract_updated_at(stored_contracts),
            },
        )

    if not provider_client.configured:
        contracts = _contracts_from_store(store, previous_run) if previous_run else []
        return DecisionOptionData(
            universe=universe,
            contracts=contracts,
            status={
                **(_status_from_run(previous_run, contracts, source="stored") if previous_run else {}),
                "provider": universe.provider,
                "source": "stored" if previous_run else "none",
                "status": "missing_api_key",
                "message": "CUTEMARKETS_API_KEY is not configured",
                "contract_count": len(contracts),
                "request_count": len(universe.requests),
            },
        )

    run_id = f"decision_{universe.provider}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{universe.universe_key[:8]}"
    started_at = now_iso()
    store.begin_fetch_run(
        {
            "run_id": run_id,
            "provider": universe.provider,
            "universe_key": universe.universe_key,
            "status": "running",
            "started_at": started_at,
            "purpose": "decision_lab_options",
            "request_ids": [request.request_id for request in universe.requests],
            "request_count": len(universe.requests),
        }
    )

    fetched = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    request_ids: list[str] = []
    contract_count = 0
    provider_call_budget = _provider_call_budget()
    provider_calls = 0
    for request in universe.requests:
        existing_contracts = store.load_contracts(request)
        if existing_contracts and not force_refresh:
            request_ids.append(request.request_id)
            fetched += 1
            contract_count += len(existing_contracts)
            continue
        if provider_calls >= provider_call_budget:
            skipped += 1
            continue
        provider_calls += 1
        try:
            result = provider_client.fetch_chain(request)
        except Exception as exc:
            failed += 1
            errors.append(f"{request.ticker} {request.put_call} {request.expiry}: {exc}")
            continue
        request_ids.append(request.request_id)
        store.save_chain_snapshot(result)
        if result.error:
            failed += 1
            errors.append(f"{request.ticker} {request.put_call} {request.expiry}: {result.error}")
        else:
            fetched += 1
            contract_count += len(result.contracts)

    succeeded = fetched > 0 and contract_count > 0
    finished_at = now_iso()
    updates = {
        "status": "succeeded" if succeeded else "failed",
        "finished_at": finished_at,
        "request_count": len(universe.requests),
        "fetched_request_count": fetched,
        "failed_request_count": failed,
        "skipped_request_count": skipped,
        "provider_call_budget": provider_call_budget,
        "contract_count": contract_count,
        "request_ids": request_ids or [request.request_id for request in universe.requests],
        "errors": errors[:20],
    }
    store.finish_fetch_run(run_id, updates)
    if succeeded:
        run_doc = {
            "run_id": run_id,
            "provider": universe.provider,
            "universe_key": universe.universe_key,
            "started_at": started_at,
            **updates,
        }
        contracts = _contracts_from_store(store, run_doc)
        return DecisionOptionData(
            universe=universe,
            contracts=contracts,
            status=_status_from_run(run_doc, contracts, source="provider_refresh"),
        )

    contracts = _contracts_from_store(store, previous_run) if previous_run else []
    status = _status_from_run(previous_run, contracts, source="stored_after_failed_refresh") if previous_run else {}
    status.update(
        {
            "provider": universe.provider,
            "source": status.get("source") or "none",
            "status": "failed_refresh_kept_previous" if previous_run else "failed",
            "message": "; ".join(errors[:3]) if errors else "Provider refresh failed",
            "request_count": len(universe.requests),
            "contract_count": len(contracts),
            "skipped_request_count": skipped,
        }
    )
    return DecisionOptionData(universe=universe, contracts=contracts, status=status)


def decision_option_loader(
    *,
    store_factory: Callable[[], OptionMarketStore],
    provider_factory: Callable[[], CuteMarketsClient],
    force_refresh: bool = False,
) -> Callable[[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], dict[str, Any]], dict[str, Any]]:
    def load(
        _ticker_situations: list[dict[str, Any]],
        _active_cycle: dict[str, Any],
        candidate_groups: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        universe = build_decision_option_universe(
            candidate_groups,
            as_of=_as_of_date(payload),
            provider=CuteMarketsClient.provider,
        )
        if not universe.requests:
            return DecisionOptionData(
                universe=universe,
                contracts=[],
                status={"provider": universe.provider, "source": "none", "status": "empty_universe", "contract_count": 0},
            ).as_dict()
        try:
            data = load_or_fetch_decision_option_data(
                store=store_factory(),
                universe=universe,
                provider_client=provider_factory(),
                force_refresh=force_refresh,
            )
            return data.as_dict()
        except Exception as exc:
            return DecisionOptionData(
                universe=universe,
                contracts=[],
                status={
                    "provider": universe.provider,
                    "source": "none",
                    "status": "load_failed",
                    "message": str(exc),
                    "request_count": len(universe.requests),
                    "contract_count": 0,
                },
            ).as_dict()

    return load


def _contracts_from_store(store: OptionMarketStore, run_doc: Optional[dict[str, Any]]) -> list[OptionMarketContract]:
    if not run_doc:
        return []
    docs = store.load_contracts_by_request_ids([str(item) for item in run_doc.get("request_ids", []) if item])
    contracts = []
    for doc in docs:
        try:
            contracts.append(contract_from_dict(doc))
        except Exception:
            continue
    return contracts


def _existing_contracts_for_universe(
    store: OptionMarketStore,
    universe: DecisionOptionUniverse,
) -> tuple[list[OptionMarketContract], set[str]]:
    docs = []
    for request in universe.requests:
        request_docs = store.load_contracts(request)
        if not request_docs:
            request_docs = store.load_latest_contracts_for_chain(request)
        docs.extend(request_docs)
    contracts = []
    request_ids: set[str] = set()
    seen = set()
    for doc in docs:
        key = (
            doc.get("request_id"),
            doc.get("ticker"),
            doc.get("expiry"),
            doc.get("put_call"),
            doc.get("strike"),
            doc.get("contract_symbol"),
        )
        if key in seen:
            continue
        seen.add(key)
        try:
            contract = contract_from_dict(doc)
        except Exception:
            continue
        if doc.get("updated_at"):
            raw = dict(contract.raw or {})
            raw["updated_at"] = doc.get("updated_at")
            contract = replace(contract, raw=raw)
        contracts.append(contract)
        if contract.request_id:
            request_ids.add(contract.request_id)
    return contracts, request_ids


def _latest_contract_updated_at(contracts: list[OptionMarketContract]) -> Optional[str]:
    values = []
    for contract in contracts:
        raw = contract.raw if isinstance(contract.raw, dict) else {}
        updated_at = raw.get("updated_at")
        if updated_at:
            values.append(str(updated_at))
    return max(values) if values else None


def _provider_call_budget() -> int:
    raw = os.getenv("CUTEMARKETS_FETCH_REQUEST_BUDGET", "1000").strip()
    try:
        value = int(raw)
    except ValueError:
        return 1000
    return max(value, 0)


def _coverage_from_contracts(contracts: list[OptionMarketContract]) -> dict[str, Any]:
    quote_count = sum(1 for contract in contracts if contract.bid is not None or contract.ask is not None)
    greek_count = sum(1 for contract in contracts if contract.delta is not None)
    return {
        "contract_count": len(contracts),
        "quote_coverage_count": quote_count,
        "quote_coverage_rate": quote_count / len(contracts) if contracts else None,
        "greek_coverage_count": greek_count,
        "greek_coverage_rate": greek_count / len(contracts) if contracts else None,
    }


def _status_from_run(run_doc: Optional[dict[str, Any]], contracts: list[OptionMarketContract], *, source: str) -> dict[str, Any]:
    if not run_doc:
        return {}
    return {
        **_coverage_from_contracts(contracts),
        "provider": run_doc.get("provider"),
        "source": source,
        "status": run_doc.get("status"),
        "run_id": run_doc.get("run_id"),
        "universe_key": run_doc.get("universe_key"),
        "last_fetched_at": run_doc.get("finished_at") or run_doc.get("started_at"),
        "request_count": run_doc.get("request_count"),
        "fetched_request_count": run_doc.get("fetched_request_count"),
        "failed_request_count": run_doc.get("failed_request_count"),
        "skipped_request_count": run_doc.get("skipped_request_count"),
        "provider_call_budget": run_doc.get("provider_call_budget"),
    }


def _candidate_put_call(candidate: dict[str, Any], group: dict[str, Any]) -> Optional[str]:
    action = str(candidate.get("action") or "").lower()
    category = str(group.get("category") or "").lower()
    if "put" in action or "assignment risk" in category:
        return "PUT"
    if "call" in action or "exit" in category or "recovery" in category:
        return "CALL"
    return None


def _date_or_none(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _as_of_date(payload: dict[str, Any]) -> date:
    request = ((payload.get("dashboard") or {}).get("request") or {})
    parsed = _date_or_none(request.get("as_of"))
    return parsed or date.today()
