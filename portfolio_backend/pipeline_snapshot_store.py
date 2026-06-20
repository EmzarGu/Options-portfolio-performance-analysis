from __future__ import annotations

import base64
import gzip
import hashlib
import os
import pickle
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


COLLECTION_PIPELINE_SNAPSHOTS = "pipeline_snapshots"
COLLECTION_APP_METADATA = "app_metadata"
CHUNK_SUBCOLLECTION = "chunks"
SNAPSHOT_SCHEMA_VERSION = 6
CHUNK_SIZE = 700_000


@dataclass(frozen=True)
class PipelineSnapshot:
    snapshot_id: str
    metadata: Dict[str, Any]
    state: Any


class PipelineSnapshotStore:
    def load(self, snapshot_id: str) -> Optional[PipelineSnapshot]:
        raise NotImplementedError

    def save(self, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        raise NotImplementedError

    def load_latest(self, pointer_id: str) -> Optional[PipelineSnapshot]:
        raise NotImplementedError

    def save_latest(self, pointer_id: str, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        raise NotImplementedError

    def try_acquire_build_lease(self, lease_id: str, owner_id: str, *, ttl_seconds: int) -> bool:
        _ = lease_id, owner_id, ttl_seconds
        return True

    def release_build_lease(self, lease_id: str, owner_id: str) -> None:
        _ = lease_id, owner_id


class DisabledPipelineSnapshotStore(PipelineSnapshotStore):
    def load(self, snapshot_id: str) -> Optional[PipelineSnapshot]:
        _ = snapshot_id
        return None

    def save(self, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        _ = snapshot_id, state, metadata

    def load_latest(self, pointer_id: str) -> Optional[PipelineSnapshot]:
        _ = pointer_id
        return None

    def save_latest(self, pointer_id: str, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        _ = pointer_id, snapshot_id, state, metadata


class MemoryPipelineSnapshotStore(PipelineSnapshotStore):
    def __init__(self):
        self.snapshots: Dict[str, PipelineSnapshot] = {}
        self.latest: Dict[str, str] = {}
        self.leases: Dict[str, Dict[str, Any]] = {}
        self._lease_lock = threading.Lock()

    def load(self, snapshot_id: str) -> Optional[PipelineSnapshot]:
        return self.snapshots.get(snapshot_id)

    def save(self, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        self.snapshots[snapshot_id] = PipelineSnapshot(snapshot_id, dict(metadata), state)

    def load_latest(self, pointer_id: str) -> Optional[PipelineSnapshot]:
        snapshot_id = self.latest.get(pointer_id)
        if snapshot_id is None:
            return None
        return self.load(snapshot_id)

    def save_latest(self, pointer_id: str, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        self.save(snapshot_id, state, metadata)
        self.latest[pointer_id] = snapshot_id

    def try_acquire_build_lease(self, lease_id: str, owner_id: str, *, ttl_seconds: int) -> bool:
        now = datetime.now(timezone.utc)
        with self._lease_lock:
            lease = self.leases.get(lease_id)
            if lease and lease.get("expires_at") and lease["expires_at"] > now and lease.get("owner_id") != owner_id:
                return False
            self.leases[lease_id] = {
                "owner_id": owner_id,
                "expires_at": now + timedelta(seconds=max(int(ttl_seconds), 1)),
            }
            return True

    def release_build_lease(self, lease_id: str, owner_id: str) -> None:
        with self._lease_lock:
            lease = self.leases.get(lease_id)
            if lease and lease.get("owner_id") == owner_id:
                self.leases.pop(lease_id, None)


class FirestorePipelineSnapshotStore(PipelineSnapshotStore):
    def __init__(self, *, project: Optional[str] = None, database: str = "(default)", client=None):
        if client is None:
            from portfolio_backend.gcp import firestore_client

            client = firestore_client(project=project, database=database)
        self.client = client

    def load(self, snapshot_id: str) -> Optional[PipelineSnapshot]:
        doc_ref = self.client.collection(COLLECTION_PIPELINE_SNAPSHOTS).document(snapshot_id)
        doc = doc_ref.get()
        if not doc.exists:
            return None
        metadata = doc.to_dict() or {}
        if int(metadata.get("schema_version") or 0) != SNAPSHOT_SCHEMA_VERSION:
            return None

        chunk_count = int(metadata.get("chunk_count") or 0)
        if chunk_count <= 0:
            return None
        chunks: List[str] = []
        for index in range(chunk_count):
            chunk_doc = doc_ref.collection(CHUNK_SUBCOLLECTION).document(f"{index:04d}").get()
            if not chunk_doc.exists:
                return None
            chunk = (chunk_doc.to_dict() or {}).get("data")
            if not isinstance(chunk, str):
                return None
            chunks.append(chunk)

        encoded = "".join(chunks)
        payload_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        if payload_hash != str(metadata.get("payload_hash") or ""):
            return None
        state = _decode_state(encoded)
        return PipelineSnapshot(snapshot_id=snapshot_id, metadata=metadata, state=state)

    def save(self, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        encoded = _encode_state(state)
        chunks = [encoded[index : index + CHUNK_SIZE] for index in range(0, len(encoded), CHUNK_SIZE)]
        doc_ref = self.client.collection(COLLECTION_PIPELINE_SNAPSHOTS).document(snapshot_id)
        doc = _json_safe(
            {
                **metadata,
                "schema_version": SNAPSHOT_SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "encoding": "pickle+gzip+base64",
                "chunk_count": len(chunks),
                "payload_size": len(encoded),
                "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
            }
        )
        doc_ref.set(doc, merge=True)
        for index, chunk in enumerate(chunks):
            doc_ref.collection(CHUNK_SUBCOLLECTION).document(f"{index:04d}").set({"data": chunk})

    def load_latest(self, pointer_id: str) -> Optional[PipelineSnapshot]:
        pointer = self.client.collection(COLLECTION_APP_METADATA).document(pointer_id).get()
        if not pointer.exists:
            return None
        snapshot_id = (pointer.to_dict() or {}).get("snapshot_id")
        if not snapshot_id:
            return None
        return self.load(str(snapshot_id))

    def save_latest(self, pointer_id: str, snapshot_id: str, state: Any, metadata: Dict[str, Any]) -> None:
        self.save(snapshot_id, state, metadata)
        self.client.collection(COLLECTION_APP_METADATA).document(pointer_id).set(
            _json_safe(
                {
                    "pointer_id": pointer_id,
                    "snapshot_id": snapshot_id,
                    "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                    "source_snapshot_id": metadata.get("source_snapshot_id"),
                    "as_of": metadata.get("as_of"),
                    "selected_sheets": metadata.get("selected_sheets"),
                    "include_unrealized": metadata.get("include_unrealized"),
                    "cache_bust": metadata.get("cache_bust"),
                    "prices_updated_at": metadata.get("prices_updated_at"),
                }
            ),
            merge=True,
        )

    def try_acquire_build_lease(self, lease_id: str, owner_id: str, *, ttl_seconds: int) -> bool:
        from google.cloud import firestore

        doc_ref = self.client.collection(COLLECTION_APP_METADATA).document(str(lease_id))
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=max(int(ttl_seconds), 1))
        transaction = self.client.transaction()

        @firestore.transactional
        def claim(txn):
            snapshot = doc_ref.get(transaction=txn)
            data = snapshot.to_dict() or {} if snapshot.exists else {}
            current_expires_at = _datetime_from_firestore(data.get("expires_at"))
            current_owner = str(data.get("owner_id") or "")
            current_status = str(data.get("status") or "")
            if (
                current_status == "building"
                and current_expires_at is not None
                and current_expires_at > now
                and current_owner != owner_id
            ):
                return False
            txn.set(
                doc_ref,
                _json_safe(
                    {
                        "lease_id": str(lease_id),
                        "owner_id": owner_id,
                        "status": "building",
                        "created_at": data.get("created_at") or now.isoformat(timespec="seconds"),
                        "updated_at": now.isoformat(timespec="seconds"),
                        "expires_at": expires_at,
                    }
                ),
                merge=True,
            )
            return True

        return bool(claim(transaction))

    def release_build_lease(self, lease_id: str, owner_id: str) -> None:
        doc_ref = self.client.collection(COLLECTION_APP_METADATA).document(str(lease_id))
        try:
            snapshot = doc_ref.get()
            data = snapshot.to_dict() or {} if snapshot.exists else {}
            if str(data.get("owner_id") or "") == owner_id:
                doc_ref.set(
                    _json_safe(
                        {
                            "status": "released",
                            "released_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        }
                    ),
                    merge=True,
                )
        except Exception:
            return


_DEFAULT_STORE: Optional[PipelineSnapshotStore] = None


def get_default_pipeline_snapshot_store() -> PipelineSnapshotStore:
    global _DEFAULT_STORE
    if _DEFAULT_STORE is not None:
        return _DEFAULT_STORE

    mode = os.getenv("PIPELINE_SNAPSHOT_STORE", "auto").strip().lower()
    if mode in {"off", "disabled", "none"}:
        _DEFAULT_STORE = DisabledPipelineSnapshotStore()
        return _DEFAULT_STORE
    if mode == "memory":
        _DEFAULT_STORE = MemoryPipelineSnapshotStore()
        return _DEFAULT_STORE

    project = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    running_on_cloud_run = bool(os.getenv("K_SERVICE"))
    if mode == "firestore" or project or running_on_cloud_run:
        try:
            _DEFAULT_STORE = FirestorePipelineSnapshotStore(
                project=project,
                database=os.getenv("FIRESTORE_DATABASE", "(default)"),
            )
            return _DEFAULT_STORE
        except Exception:
            _DEFAULT_STORE = DisabledPipelineSnapshotStore()
            return _DEFAULT_STORE

    _DEFAULT_STORE = DisabledPipelineSnapshotStore()
    return _DEFAULT_STORE


def reset_default_pipeline_snapshot_store() -> None:
    global _DEFAULT_STORE
    _DEFAULT_STORE = None


def pipeline_snapshot_id(
    *,
    source_snapshot_id: str,
    as_of: Any,
    selected_sheets: Iterable[str],
    source_kind: str = "ibkr_flex",
) -> str:
    as_of_text = _date_text(as_of)
    selected = ",".join(str(sheet) for sheet in selected_sheets)
    raw = "|".join(
        [
            f"v{SNAPSHOT_SCHEMA_VERSION}",
            source_kind,
            str(source_snapshot_id),
            as_of_text,
            selected,
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"{source_kind}:{as_of_text}:{digest}"


def snapshot_metadata_for_context(
    *,
    source_marker: Optional[Dict[str, Any]],
    request: Any,
    available_sheets: Optional[Iterable[str]],
) -> Dict[str, Any]:
    return {
        "source_kind": "ibkr_flex",
        "source_snapshot_id": (source_marker or {}).get("source_snapshot_id"),
        "ibkr_import_run_id": (source_marker or {}).get("import_run_id"),
        "ibkr_import_finished_at": (source_marker or {}).get("finished_at"),
        "ibkr_import_from_date": (source_marker or {}).get("from_date"),
        "ibkr_import_to_date": (source_marker or {}).get("to_date"),
        "as_of": _date_text(getattr(request, "as_of", None)),
        "selected_sheets": list(getattr(request, "selected_sheets", []) or []),
        "available_sheets": list(available_sheets or []),
    }


def _encode_state(state: Any) -> str:
    payload = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
    compressed = gzip.compress(payload, compresslevel=6)
    return base64.b64encode(compressed).decode("ascii")


def _decode_state(encoded: str) -> Any:
    compressed = base64.b64decode(encoded.encode("ascii"))
    payload = gzip.decompress(compressed)
    return pickle.loads(payload)


def _date_text(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.date().isoformat()


def _datetime_from_firestore(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = pd.to_datetime(value, errors="coerce").to_pydatetime()
        except Exception:
            return None
    if pd.isna(dt):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
