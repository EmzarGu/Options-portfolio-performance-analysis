from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd


COLLECTION_REFRESH_RUNS = "refresh_runs"
COLLECTION_SOURCE_SNAPSHOTS = "source_snapshots"
SCHEMA_VERSION = 1

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefreshAuditRecord:
    run_id: str
    started_at: str
    finished_at: str
    status: str
    request: Dict[str, Any]
    data_freshness: Dict[str, Any]
    refresh: Dict[str, Any]
    timings_ms: Dict[str, float]
    source_snapshot_id: Optional[str] = None
    error: Optional[str] = None


class AuditStore:
    def record_refresh_run(self, record: RefreshAuditRecord) -> None:
        raise NotImplementedError

    def upsert_source_snapshot(self, snapshot_id: str, snapshot: Dict[str, Any]) -> None:
        raise NotImplementedError


class DisabledAuditStore(AuditStore):
    def record_refresh_run(self, record: RefreshAuditRecord) -> None:
        _ = record

    def upsert_source_snapshot(self, snapshot_id: str, snapshot: Dict[str, Any]) -> None:
        _ = snapshot_id, snapshot


class MemoryAuditStore(AuditStore):
    def __init__(self):
        self.refresh_runs: Dict[str, Dict[str, Any]] = {}
        self.source_snapshots: Dict[str, Dict[str, Any]] = {}

    def record_refresh_run(self, record: RefreshAuditRecord) -> None:
        self.refresh_runs[record.run_id] = _record_to_doc(record)

    def upsert_source_snapshot(self, snapshot_id: str, snapshot: Dict[str, Any]) -> None:
        self.source_snapshots[snapshot_id] = _json_safe(snapshot)


class FirestoreAuditStore(AuditStore):
    def __init__(self, *, project: Optional[str] = None, database: str = "(default)", client=None):
        if client is None:
            from google.cloud import firestore

            client = firestore.Client(project=project, database=database)
        self.client = client

    def record_refresh_run(self, record: RefreshAuditRecord) -> None:
        self.client.collection(COLLECTION_REFRESH_RUNS).document(record.run_id).set(_record_to_doc(record))

    def upsert_source_snapshot(self, snapshot_id: str, snapshot: Dict[str, Any]) -> None:
        self.client.collection(COLLECTION_SOURCE_SNAPSHOTS).document(snapshot_id).set(_json_safe(snapshot), merge=True)


_DEFAULT_STORE: Optional[AuditStore] = None


def get_default_audit_store() -> AuditStore:
    global _DEFAULT_STORE
    if _DEFAULT_STORE is not None:
        return _DEFAULT_STORE

    mode = os.getenv("AUDIT_STORE", "auto").strip().lower()
    if mode in {"off", "disabled", "none", "memory"}:
        _DEFAULT_STORE = MemoryAuditStore() if mode == "memory" else DisabledAuditStore()
        return _DEFAULT_STORE

    project = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    running_on_cloud_run = bool(os.getenv("K_SERVICE"))
    if mode == "firestore" or project or running_on_cloud_run:
        try:
            _DEFAULT_STORE = FirestoreAuditStore(
                project=project,
                database=os.getenv("FIRESTORE_DATABASE", "(default)"),
            )
            return _DEFAULT_STORE
        except Exception as exc:
            logger.warning("audit_store_init_failed mode=%s error=%s", mode, exc)

    _DEFAULT_STORE = DisabledAuditStore()
    return _DEFAULT_STORE


def reset_default_audit_store() -> None:
    global _DEFAULT_STORE
    _DEFAULT_STORE = None


def _record_to_doc(record: RefreshAuditRecord) -> Dict[str, Any]:
    return _json_safe(
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": record.run_id,
            "started_at": record.started_at,
            "finished_at": record.finished_at,
            "status": record.status,
            "request": record.request,
            "data_freshness": record.data_freshness,
            "refresh": record.refresh,
            "timings_ms": record.timings_ms,
            "source_snapshot_id": record.source_snapshot_id,
            "error": record.error,
        }
    )


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
