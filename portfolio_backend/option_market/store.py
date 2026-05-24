from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Protocol

from portfolio_backend.option_market.models import OptionChainRequest, OptionMarketFetchResult, OptionMarketMatch


class OptionMarketStore(Protocol):
    def begin_fetch_run(self, run_doc: dict[str, Any]) -> None:
        ...

    def finish_fetch_run(self, run_id: str, updates: dict[str, Any]) -> None:
        ...

    def load_chain_snapshot(self, request: OptionChainRequest) -> Optional[dict[str, Any]]:
        ...

    def load_contracts(self, request: OptionChainRequest) -> list[dict[str, Any]]:
        ...

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        ...

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        ...


class MemoryOptionMarketStore:
    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.contracts: dict[str, dict[str, Any]] = {}
        self.matches: dict[str, dict[str, Any]] = {}

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

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        self.snapshots[result.request.request_id] = result.as_snapshot_doc()
        for contract in result.contracts:
            self.contracts[contract.contract_id] = contract.as_dict()

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        for match in matches:
            self.matches[match.match_id] = match.as_dict()


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

    def save_chain_snapshot(self, result: OptionMarketFetchResult) -> None:
        self._write_doc("option_market_chain_snapshots", result.request.request_id, result.as_snapshot_doc())
        for contract in result.contracts:
            self._write_doc("option_market_contracts", contract.contract_id, contract.as_dict())

    def save_trade_matches(self, matches: list[OptionMarketMatch]) -> None:
        for match in matches:
            self._write_doc("option_market_trade_matches", match.match_id, match.as_dict())

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
    ) -> None:
        self.client = client or _firestore_client()
        self.fetch_runs_collection = fetch_runs_collection
        self.chain_snapshots_collection = chain_snapshots_collection
        self.contracts_collection = contracts_collection
        self.trade_matches_collection = trade_matches_collection

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

    def _upsert_docs(self, collection: str, docs: list[tuple[str, dict[str, Any]]]) -> None:
        for chunk in _chunks(docs, 400):
            batch = self.client.batch()
            for doc_id, data in chunk:
                batch.set(self.client.collection(collection).document(doc_id), data, merge=True)
            batch.commit()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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
                "data_count": len(data) if isinstance(data, list) else 0,
            }
        )
    return compact


def _firestore_client():
    from portfolio_backend.gcp import firestore_client

    return firestore_client()
