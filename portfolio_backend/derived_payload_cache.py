from __future__ import annotations

import base64
import hashlib
import json
import logging
import zlib
from datetime import date, datetime, timezone
from typing import Any, Optional

import pandas as pd

from portfolio_backend.gcp import firestore_client


COLLECTION_DERIVED_PAYLOADS = "dashboard_derived_payloads"
CHUNK_SUBCOLLECTION = "chunks"
DERIVED_PAYLOAD_SCHEMA_VERSION = 1
CHUNK_SIZE = 500_000

logger = logging.getLogger(__name__)


def derived_payload_key(namespace: str, components: dict[str, Any]) -> str:
    raw = json.dumps(_json_safe({"namespace": namespace, **components}), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"{namespace}:{digest}"


def load_derived_payload(cache_key: str) -> Optional[dict[str, Any]]:
    try:
        client = firestore_client()
        doc_ref = client.collection(COLLECTION_DERIVED_PAYLOADS).document(cache_key)
        doc = doc_ref.get()
        if not doc.exists:
            return None
        metadata = doc.to_dict() or {}
        if int(metadata.get("schema_version") or 0) != DERIVED_PAYLOAD_SCHEMA_VERSION:
            return None
        if metadata.get("status") != "ready":
            return None
        chunk_count = int(metadata.get("chunk_count") or 0)
        if chunk_count <= 0:
            return None
        chunks: list[str] = []
        for index in range(chunk_count):
            chunk = doc_ref.collection(CHUNK_SUBCOLLECTION).document(f"{index:04d}").get()
            if not chunk.exists:
                return None
            data = (chunk.to_dict() or {}).get("data")
            if not data:
                return None
            chunks.append(str(data))
        compressed = base64.b64decode("".join(chunks).encode("ascii"))
        payload = json.loads(zlib.decompress(compressed).decode("utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logger.warning("derived_payload_cache_load_failed key=%s error=%s", cache_key, exc)
        return None


def save_derived_payload(cache_key: str, payload: dict[str, Any], *, metadata: Optional[dict[str, Any]] = None) -> None:
    try:
        safe_payload = _json_safe(payload)
        encoded = base64.b64encode(zlib.compress(json.dumps(safe_payload, separators=(",", ":")).encode("utf-8"))).decode(
            "ascii"
        )
        chunks = [encoded[index : index + CHUNK_SIZE] for index in range(0, len(encoded), CHUNK_SIZE)] or [""]
        client = firestore_client()
        doc_ref = client.collection(COLLECTION_DERIVED_PAYLOADS).document(cache_key)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        doc_ref.set(
            _json_safe(
                {
                    "cache_key": cache_key,
                    "schema_version": DERIVED_PAYLOAD_SCHEMA_VERSION,
                    "status": "ready",
                    "chunk_count": len(chunks),
                    "payload_bytes_compressed_b64": len(encoded),
                    "updated_at": now,
                    **(metadata or {}),
                }
            ),
            merge=True,
        )
        for index, chunk in enumerate(chunks):
            doc_ref.collection(CHUNK_SUBCOLLECTION).document(f"{index:04d}").set({"data": chunk})
    except Exception as exc:
        logger.warning("derived_payload_cache_save_failed key=%s error=%s", cache_key, exc)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)
