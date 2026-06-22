from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Protocol

from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionHistoricalEnrichment,
    OptionMarketFetchResult,
    OptionMarketMatch,
    OptionProbabilityRow,
    OptionProbabilityTradeMatch,
)


class OptionMarketStore(Protocol):
    def begin_fetch_run(self, run_doc: dict[str, Any]) -> None:
        ...

    def finish_fetch_run(self, run_id: str, updates: dict[str, Any]) -> None:
        ...

    def load_chain_snapshot(self, request: OptionChainRequest) -> Optional[dict[str, Any]]:
        ...

    def load_contracts(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        ...

    def load_latest_contracts_for_chain(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        ...

    def load_latest_successful_fetch_run(self, *, universe_key: str, provider: str) -> Optional[dict[str, Any]]:
        ...

    def load_contracts_by_request_ids(self, request_ids: list[str]) -> list[dict[str, Any]]:
        ...

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        ...

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        ...

    def begin_probability_import_run(self, run_doc: dict[str, Any]) -> None:
        ...

    def finish_probability_import_run(self, run_id: str, updates: dict[str, Any]) -> None:
        ...

    def save_probability_history(
        self,
        rows: list[OptionProbabilityRow],
        matches: list[OptionProbabilityTradeMatch],
    ) -> None:
        ...

    def begin_historical_enrichment_run(self, run_doc: dict[str, Any]) -> None:
        ...

    def finish_historical_enrichment_run(self, run_id: str, updates: dict[str, Any]) -> None:
        ...

    def load_latest_historical_enrichment_run(self, *, provider: str) -> Optional[dict[str, Any]]:
        ...

    def load_historical_enrichments_by_ids(self, enrichment_ids: list[str]) -> list[dict[str, Any]]:
        ...

    def load_historical_enrichment(self, provider: str, trade_id: str) -> Optional[dict[str, Any]]:
        ...

    def save_historical_enrichments(self, enrichments: list[OptionHistoricalEnrichment]) -> None:
        ...


class MemoryOptionMarketStore:
    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.contracts: dict[str, dict[str, Any]] = {}
        self.matches: dict[str, dict[str, Any]] = {}
        self.probability_runs: dict[str, dict[str, Any]] = {}
        self.probability_rows: dict[str, dict[str, Any]] = {}
        self.probability_matches: dict[str, dict[str, Any]] = {}
        self.historical_runs: dict[str, dict[str, Any]] = {}
        self.historical_enrichments: dict[str, dict[str, Any]] = {}

    def begin_fetch_run(self, run_doc: dict[str, Any]) -> None:
        self.runs[str(run_doc["run_id"])] = dict(run_doc)

    def finish_fetch_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.runs[str(run_id)] = {**self.runs.get(str(run_id), {"run_id": run_id}), **updates}

    def load_chain_snapshot(self, request: OptionChainRequest) -> Optional[dict[str, Any]]:
        return self.snapshots.get(request.request_id)

    def load_contracts(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        return [
            doc
            for doc in self.contracts.values()
            if str(doc.get("request_id")) == request.request_id
        ]

    def load_latest_contracts_for_chain(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        candidates = [
            doc
            for doc in self.contracts.values()
            if str(doc.get("provider") or "").lower() == request.provider.lower()
            and str(doc.get("ticker") or "").upper() == request.ticker.upper()
            and str(doc.get("expiry") or "")[:10] == request.expiry.isoformat()
            and str(doc.get("put_call") or "").upper() == request.put_call.upper()
        ]
        return _latest_contract_group(candidates)

    def load_latest_successful_fetch_run(self, *, universe_key: str, provider: str) -> Optional[dict[str, Any]]:
        candidates = [
            doc
            for doc in self.runs.values()
            if doc.get("universe_key") == universe_key
            and str(doc.get("provider") or "").lower() == provider.lower()
            and doc.get("status") == "succeeded"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda doc: str(doc.get("finished_at") or doc.get("started_at") or ""))

    def load_contracts_by_request_ids(self, request_ids: list[str]) -> list[dict[str, Any]]:
        wanted = set(request_ids)
        return [doc for doc in self.contracts.values() if str(doc.get("request_id")) in wanted]

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        self.snapshots[result.request.request_id] = result.as_snapshot_doc()
        for contract in result.contracts:
            self.contracts[contract.contract_id] = contract.as_dict()

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        for match in matches:
            self.matches[match.match_id] = match.as_dict()

    def begin_probability_import_run(self, run_doc: dict[str, Any]) -> None:
        self.probability_runs[str(run_doc["run_id"])] = dict(run_doc)

    def finish_probability_import_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.probability_runs[str(run_id)] = {
            **self.probability_runs.get(str(run_id), {"run_id": run_id}),
            **updates,
        }

    def save_probability_history(
        self,
        rows: list[OptionProbabilityRow],
        matches: list[OptionProbabilityTradeMatch],
    ) -> None:
        for row in rows:
            self.probability_rows[row.row_id] = row.as_dict()
        for match in matches:
            self.probability_matches[match.match_id] = match.as_dict()

    def begin_historical_enrichment_run(self, run_doc: dict[str, Any]) -> None:
        self.historical_runs[str(run_doc["run_id"])] = dict(run_doc)

    def finish_historical_enrichment_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.historical_runs[str(run_id)] = {
            **self.historical_runs.get(str(run_id), {"run_id": run_id}),
            **updates,
        }

    def load_latest_historical_enrichment_run(self, *, provider: str) -> Optional[dict[str, Any]]:
        candidates = [
            doc
            for doc in self.historical_runs.values()
            if doc.get("status") == "succeeded" and str(doc.get("provider") or "").lower() == provider.lower()
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda doc: str(doc.get("finished_at") or doc.get("started_at") or ""))

    def load_historical_enrichments_by_ids(self, enrichment_ids: list[str]) -> list[dict[str, Any]]:
        wanted = set(enrichment_ids)
        return [doc for key, doc in self.historical_enrichments.items() if key in wanted]

    def load_historical_enrichment(self, provider: str, trade_id: str) -> Optional[dict[str, Any]]:
        for doc in self.historical_enrichments.values():
            trade = doc.get("trade") if isinstance(doc.get("trade"), dict) else {}
            if str(doc.get("provider") or "").lower() == provider.lower() and str(trade.get("trade_id") or "") == str(trade_id):
                return doc
        return None

    def save_historical_enrichments(self, enrichments: list[OptionHistoricalEnrichment]) -> None:
        for enrichment in enrichments:
            self.historical_enrichments[enrichment.enrichment_id] = enrichment.as_dict()


class LocalJsonOptionMarketStore:
    def __init__(self, root_dir: str | Path = "tmp/option_market_validation/firestore_sim") -> None:
        self.root_dir = Path(root_dir)

    def begin_fetch_run(self, run_doc: dict[str, Any]) -> None:
        self._write_doc("option_market_fetch_runs", str(run_doc["run_id"]), run_doc)

    def finish_fetch_run(self, run_id: str, updates: dict[str, Any]) -> None:
        path = self._doc_path("option_market_fetch_runs", str(run_id))
        existing = _read_json(path) if path.exists() else {"run_id": run_id}
        self._write_doc("option_market_fetch_runs", str(run_id), {**existing, **updates})

    def load_chain_snapshot(self, request: OptionChainRequest) -> Optional[dict[str, Any]]:
        path = self._doc_path("option_market_chain_snapshots", request.request_id)
        if not path.exists():
            return None
        return _read_json(path)

    def load_contracts(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        root = self.root_dir / "option_market_contracts"
        if not root.exists():
            return []
        docs = []
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            if str(doc.get("request_id")) == request.request_id:
                docs.append(doc)
        return docs

    def load_latest_contracts_for_chain(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        root = self.root_dir / "option_market_contracts"
        if not root.exists():
            return []
        candidates = []
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            if (
                str(doc.get("provider") or "").lower() == request.provider.lower()
                and str(doc.get("ticker") or "").upper() == request.ticker.upper()
                and str(doc.get("expiry") or "")[:10] == request.expiry.isoformat()
                and str(doc.get("put_call") or "").upper() == request.put_call.upper()
            ):
                candidates.append(doc)
        return _latest_contract_group(candidates)

    def load_latest_successful_fetch_run(self, *, universe_key: str, provider: str) -> Optional[dict[str, Any]]:
        root = self.root_dir / "option_market_fetch_runs"
        if not root.exists():
            return None
        candidates = []
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            if (
                doc.get("universe_key") == universe_key
                and str(doc.get("provider") or "").lower() == provider.lower()
                and doc.get("status") == "succeeded"
            ):
                candidates.append(doc)
        if not candidates:
            return None
        return max(candidates, key=lambda doc: str(doc.get("finished_at") or doc.get("started_at") or ""))

    def load_contracts_by_request_ids(self, request_ids: list[str]) -> list[dict[str, Any]]:
        root = self.root_dir / "option_market_contracts"
        if not root.exists():
            return []
        wanted = set(request_ids)
        docs = []
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            if str(doc.get("request_id")) in wanted:
                docs.append(doc)
        return docs

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        self._write_doc("option_market_chain_snapshots", result.request.request_id, result.as_snapshot_doc())
        for contract in result.contracts:
            self._write_doc("option_market_contracts", contract.contract_id, contract.as_dict())

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        for match in matches:
            self._write_doc("option_market_trade_matches", match.match_id, match.as_dict())

    def begin_probability_import_run(self, run_doc: dict[str, Any]) -> None:
        self._write_doc("option_probability_import_runs", str(run_doc["run_id"]), run_doc)

    def finish_probability_import_run(self, run_id: str, updates: dict[str, Any]) -> None:
        path = self._doc_path("option_probability_import_runs", str(run_id))
        existing = _read_json(path) if path.exists() else {"run_id": run_id}
        self._write_doc("option_probability_import_runs", str(run_id), {**existing, **updates})

    def save_probability_history(
        self,
        rows: list[OptionProbabilityRow],
        matches: list[OptionProbabilityTradeMatch],
    ) -> None:
        for row in rows:
            self._write_doc("option_probability_rows", row.row_id, row.as_dict())
        for match in matches:
            self._write_doc("option_probability_trade_matches", match.match_id, match.as_dict())

    def begin_historical_enrichment_run(self, run_doc: dict[str, Any]) -> None:
        self._write_doc("option_historical_enrichment_runs", str(run_doc["run_id"]), run_doc)

    def finish_historical_enrichment_run(self, run_id: str, updates: dict[str, Any]) -> None:
        path = self._doc_path("option_historical_enrichment_runs", str(run_id))
        existing = _read_json(path) if path.exists() else {"run_id": run_id}
        self._write_doc("option_historical_enrichment_runs", str(run_id), {**existing, **updates})

    def load_latest_historical_enrichment_run(self, *, provider: str) -> Optional[dict[str, Any]]:
        root = self.root_dir / "option_historical_enrichment_runs"
        if not root.exists():
            return None
        candidates = []
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            if doc.get("status") == "succeeded" and str(doc.get("provider") or "").lower() == provider.lower():
                candidates.append(doc)
        if not candidates:
            return None
        return max(candidates, key=lambda doc: str(doc.get("finished_at") or doc.get("started_at") or ""))

    def load_historical_enrichments_by_ids(self, enrichment_ids: list[str]) -> list[dict[str, Any]]:
        root = self.root_dir / "option_historical_trade_enrichments"
        if not root.exists():
            return []
        docs = []
        wanted = set(enrichment_ids)
        for doc_id in wanted:
            path = root / f"{doc_id}.json"
            if path.exists():
                docs.append(_read_json(path))
        return docs

    def load_historical_enrichment(self, provider: str, trade_id: str) -> Optional[dict[str, Any]]:
        root = self.root_dir / "option_historical_trade_enrichments"
        if not root.exists():
            return None
        for path in sorted(root.glob("*.json")):
            doc = _read_json(path)
            trade = doc.get("trade") if isinstance(doc.get("trade"), dict) else {}
            if str(doc.get("provider") or "").lower() == provider.lower() and str(trade.get("trade_id") or "") == str(trade_id):
                return doc
        return None

    def save_historical_enrichments(self, enrichments: list[OptionHistoricalEnrichment]) -> None:
        for enrichment in enrichments:
            self._write_doc("option_historical_trade_enrichments", enrichment.enrichment_id, enrichment.as_dict())

    def _doc_path(self, collection: str, doc_id: str) -> Path:
        return self.root_dir / collection / f"{doc_id}.json"

    def _write_doc(self, collection: str, doc_id: str, data: dict[str, Any]) -> None:
        path = self._doc_path(collection, doc_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str), encoding="utf-8")


class FirestoreOptionMarketStore:
    def __init__(
        self,
        *,
        client: Any = None,
        fetch_runs_collection: str = "option_market_fetch_runs",
        chain_snapshots_collection: str = "option_market_chain_snapshots",
        contracts_collection: str = "option_market_contracts",
        trade_matches_collection: str = "option_market_trade_matches",
        probability_import_runs_collection: str = "option_probability_import_runs",
        probability_rows_collection: str = "option_probability_rows",
        probability_trade_matches_collection: str = "option_probability_trade_matches",
    ) -> None:
        self.client = client or _firestore_client()
        self.fetch_runs_collection = fetch_runs_collection
        self.chain_snapshots_collection = chain_snapshots_collection
        self.contracts_collection = contracts_collection
        self.trade_matches_collection = trade_matches_collection
        self.probability_import_runs_collection = probability_import_runs_collection
        self.probability_rows_collection = probability_rows_collection
        self.probability_trade_matches_collection = probability_trade_matches_collection
        self.historical_enrichment_runs_collection = "option_historical_enrichment_runs"
        self.historical_trade_enrichments_collection = "option_historical_trade_enrichments"

    def begin_fetch_run(self, run_doc: dict[str, Any]) -> None:
        self.client.collection(self.fetch_runs_collection).document(str(run_doc["run_id"])).set(run_doc, merge=True)

    def finish_fetch_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.client.collection(self.fetch_runs_collection).document(str(run_id)).set(updates, merge=True)

    def load_chain_snapshot(self, request: OptionChainRequest) -> Optional[dict[str, Any]]:
        snap = self.client.collection(self.chain_snapshots_collection).document(request.request_id).get()
        if not snap.exists:
            return None
        return snap.to_dict() or {}

    def load_contracts(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            query = self.client.collection(self.contracts_collection).where(
                filter=FieldFilter("request_id", "==", request.request_id)
            )
        except Exception:
            query = self.client.collection(self.contracts_collection).where("request_id", "==", request.request_id)
        return [snap.to_dict() or {} for snap in query.stream()]

    def load_latest_contracts_for_chain(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            query = (
                self.client.collection(self.contracts_collection)
                .where(filter=FieldFilter("provider", "==", request.provider.lower()))
                .where(filter=FieldFilter("ticker", "==", request.ticker.upper()))
                .where(filter=FieldFilter("expiry", "==", request.expiry.isoformat()))
                .where(filter=FieldFilter("put_call", "==", request.put_call.upper()))
            )
        except Exception:
            query = (
                self.client.collection(self.contracts_collection)
                .where("provider", "==", request.provider.lower())
                .where("ticker", "==", request.ticker.upper())
                .where("expiry", "==", request.expiry.isoformat())
                .where("put_call", "==", request.put_call.upper())
            )
        return _latest_contract_group([snap.to_dict() or {} for snap in query.stream()])

    def load_latest_successful_fetch_run(self, *, universe_key: str, provider: str) -> Optional[dict[str, Any]]:
        provider_key = provider.lower()
        try:
            from google.cloud.firestore_v1 import FieldFilter

            query = (
                self.client.collection(self.fetch_runs_collection)
                .where(filter=FieldFilter("universe_key", "==", universe_key))
            )
        except Exception:
            query = (
                self.client.collection(self.fetch_runs_collection)
                .where("universe_key", "==", universe_key)
            )
        candidates = [
            snap.to_dict() or {}
            for snap in query.stream()
            if (snap.to_dict() or {}).get("status") == "succeeded"
            and str((snap.to_dict() or {}).get("provider") or "").lower() == provider_key
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda doc: str(doc.get("finished_at") or doc.get("started_at") or ""))

    def load_contracts_by_request_ids(self, request_ids: list[str]) -> list[dict[str, Any]]:
        if not request_ids:
            return []
        docs: list[dict[str, Any]] = []
        for chunk in _chunks(list(dict.fromkeys(request_ids)), 30):
            try:
                from google.cloud.firestore_v1 import FieldFilter

                query = self.client.collection(self.contracts_collection).where(
                    filter=FieldFilter("request_id", "in", chunk)
                )
            except Exception:
                query = self.client.collection(self.contracts_collection).where("request_id", "in", chunk)
            docs.extend([snap.to_dict() or {} for snap in query.stream()])
        return docs

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        snapshot = result.as_snapshot_doc()
        snapshot["raw_pages"] = _compact_raw_pages(snapshot.get("raw_pages") or [])
        self.client.collection(self.chain_snapshots_collection).document(result.request.request_id).set(
            snapshot,
            merge=True,
        )
        docs = [(contract.contract_id, contract.as_dict()) for contract in result.contracts]
        self._upsert_docs(self.contracts_collection, docs)

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        self._upsert_docs(self.trade_matches_collection, [(match.match_id, match.as_dict()) for match in matches])

    def begin_probability_import_run(self, run_doc: dict[str, Any]) -> None:
        self.client.collection(self.probability_import_runs_collection).document(str(run_doc["run_id"])).set(
            run_doc,
            merge=True,
        )

    def finish_probability_import_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.client.collection(self.probability_import_runs_collection).document(str(run_id)).set(updates, merge=True)

    def save_probability_history(
        self,
        rows: list[OptionProbabilityRow],
        matches: list[OptionProbabilityTradeMatch],
    ) -> None:
        self._upsert_docs(self.probability_rows_collection, [(row.row_id, row.as_dict()) for row in rows])
        self._upsert_docs(
            self.probability_trade_matches_collection,
            [(match.match_id, match.as_dict()) for match in matches],
        )

    def begin_historical_enrichment_run(self, run_doc: dict[str, Any]) -> None:
        self.client.collection(self.historical_enrichment_runs_collection).document(str(run_doc["run_id"])).set(
            run_doc,
            merge=True,
        )

    def finish_historical_enrichment_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.client.collection(self.historical_enrichment_runs_collection).document(str(run_id)).set(updates, merge=True)

    def load_latest_historical_enrichment_run(self, *, provider: str) -> Optional[dict[str, Any]]:
        provider_key = provider.lower()
        query = self.client.collection(self.historical_enrichment_runs_collection).order_by(
            "finished_at",
            direction="DESCENDING",
        ).limit(25)
        for snap in query.stream():
            doc = snap.to_dict() or {}
            if doc.get("status") == "succeeded" and str(doc.get("provider") or "").lower() == provider_key:
                return doc
        return None

    def load_historical_enrichments_by_ids(self, enrichment_ids: list[str]) -> list[dict[str, Any]]:
        refs = [
            self.client.collection(self.historical_trade_enrichments_collection).document(str(doc_id))
            for doc_id in enrichment_ids
            if doc_id
        ]
        docs: list[dict[str, Any]] = []
        for chunk in _chunks(refs, 300):
            docs.extend([snap.to_dict() or {} for snap in self.client.get_all(chunk) if snap.exists])
        return docs

    def load_historical_enrichment(self, provider: str, trade_id: str) -> Optional[dict[str, Any]]:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            query = (
                self.client.collection(self.historical_trade_enrichments_collection)
                .where(filter=FieldFilter("provider", "==", provider.lower()))
                .where(filter=FieldFilter("trade.trade_id", "==", str(trade_id)))
                .limit(1)
            )
        except Exception:
            query = (
                self.client.collection(self.historical_trade_enrichments_collection)
                .where("provider", "==", provider.lower())
                .where("trade.trade_id", "==", str(trade_id))
                .limit(1)
            )
        for snap in query.stream():
            return snap.to_dict() or {}
        return None

    def save_historical_enrichments(self, enrichments: list[OptionHistoricalEnrichment]) -> None:
        self._upsert_docs(
            self.historical_trade_enrichments_collection,
            [(enrichment.enrichment_id, enrichment.as_dict()) for enrichment in enrichments],
        )

    def _upsert_docs(self, collection: str, docs: list[tuple[str, dict[str, Any]]]) -> None:
        for chunk in _chunks(docs, 400):
            batch = self.client.batch()
            for doc_id, data in chunk:
                batch.set(self.client.collection(collection).document(doc_id), data, merge=True)
            batch.commit()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_contract_group(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not docs:
        return []
    groups: dict[str, list[dict[str, Any]]] = {}
    for doc in docs:
        request_id = str(doc.get("request_id") or "")
        if not request_id:
            continue
        groups.setdefault(request_id, []).append(doc)
    if not groups:
        return []
    latest_request_id = max(
        groups,
        key=lambda request_id: max(
            str(doc.get("updated_at") or doc.get("trade_date") or "") for doc in groups[request_id]
        ),
    )
    return groups[latest_request_id]


def _chunks(items: list[Any], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _compact_raw_pages(raw_pages: list[Any]) -> list[dict[str, Any]]:
    compact = []
    for page in raw_pages:
        if not isinstance(page, dict):
            continue
        data = page.get("data")
        compact.append(
            {
                "meta": page.get("meta") if isinstance(page.get("meta"), dict) else {},
                "data_count": page.get("data_count") if page.get("data_count") is not None else (len(data) if isinstance(data, list) else 0),
            }
        )
    return compact


def _firestore_client():
    from portfolio_backend.gcp import firestore_client

    return firestore_client()
