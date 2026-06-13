from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from time import time
from typing import Any, Mapping, Optional

from portfolio_backend.gcp import firestore_client

logger = logging.getLogger(__name__)

APP_SETTINGS_COLLECTION = "app_settings"
MONTHLY_TARGET_BAND_DOCUMENT = "monthly_target_band"
DEFAULT_TARGET_RETURN = 0.015
DEFAULT_TARGET_FLOOR = 0.01
DEFAULT_SETTINGS_CACHE_SECONDS = 60
DEFAULT_SETTINGS_FIRESTORE_TIMEOUT_SECONDS = 5

_settings_cache_lock = threading.Lock()
_monthly_target_band_cache: tuple[float, dict[str, Any]] | None = None


def _settings_cache_seconds() -> int:
    raw = os.getenv("APP_SETTINGS_CACHE_SECONDS", str(DEFAULT_SETTINGS_CACHE_SECONDS)).strip()
    try:
        return max(0, int(float(raw)))
    except ValueError:
        return DEFAULT_SETTINGS_CACHE_SECONDS


def _settings_firestore_timeout_seconds() -> float:
    raw = os.getenv("APP_SETTINGS_FIRESTORE_TIMEOUT_SECONDS", str(DEFAULT_SETTINGS_FIRESTORE_TIMEOUT_SECONDS)).strip()
    try:
        return max(0.1, float(raw))
    except ValueError:
        return DEFAULT_SETTINGS_FIRESTORE_TIMEOUT_SECONDS


def _coerce_rate(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return None
    if not 0 <= rate <= 1:
        return None
    return rate


def _env_rate(*names: str) -> Optional[float]:
    for name in names:
        value = os.getenv(name)
        if value is None or str(value).strip() == "":
            continue
        rate = _coerce_rate(value)
        if rate is not None:
            return rate
    return None


def default_monthly_target_band() -> dict[str, Any]:
    target_return = _env_rate("WEB_MONTHLY_TARGET_RETURN", "MONTHLY_TARGET_RETURN") or DEFAULT_TARGET_RETURN
    target_floor = _env_rate("WEB_MONTHLY_TARGET_FLOOR", "MONTHLY_TARGET_FLOOR") or DEFAULT_TARGET_FLOOR
    target_floor = min(target_floor, target_return)
    return {
        "target_floor": float(target_floor),
        "target_return": float(target_return),
        "source": "default",
        "updated_at": None,
        "updated_by": None,
    }


def normalize_monthly_target_band(data: Mapping[str, Any] | None) -> dict[str, Any]:
    defaults = default_monthly_target_band()
    target_return = _coerce_rate((data or {}).get("target_return"))
    target_floor = _coerce_rate((data or {}).get("target_floor"))
    if target_return is None:
        target_return = defaults["target_return"]
    if target_floor is None:
        target_floor = defaults["target_floor"]
    target_floor = min(target_floor, target_return)
    return {
        "target_floor": float(target_floor),
        "target_return": float(target_return),
        "source": str((data or {}).get("source") or defaults["source"]),
        "updated_at": (data or {}).get("updated_at", defaults["updated_at"]),
        "updated_by": (data or {}).get("updated_by", defaults["updated_by"]),
    }


def load_monthly_target_band() -> dict[str, Any]:
    global _monthly_target_band_cache
    ttl_seconds = _settings_cache_seconds()
    now = time()
    if ttl_seconds > 0:
        with _settings_cache_lock:
            if _monthly_target_band_cache and now - _monthly_target_band_cache[0] <= ttl_seconds:
                return dict(_monthly_target_band_cache[1])
    try:
        snapshot = (
            firestore_client()
            .collection(APP_SETTINGS_COLLECTION)
            .document(MONTHLY_TARGET_BAND_DOCUMENT)
            .get(timeout=_settings_firestore_timeout_seconds())
        )
        if snapshot.exists:
            data = snapshot.to_dict() or {}
            band = normalize_monthly_target_band({**data, "source": data.get("source") or "firestore"})
            if ttl_seconds > 0:
                with _settings_cache_lock:
                    _monthly_target_band_cache = (time(), dict(band))
            return band
    except Exception as exc:
        logger.warning("monthly_target_band_load_failed error=%s", exc)
    band = default_monthly_target_band()
    if ttl_seconds > 0:
        with _settings_cache_lock:
            _monthly_target_band_cache = (time(), dict(band))
    return band


def save_monthly_target_band(
    *,
    target_floor: float,
    target_return: float,
    updated_by: Optional[str] = None,
    source: str = "user",
) -> dict[str, Any]:
    global _monthly_target_band_cache
    normalized = normalize_monthly_target_band(
        {
            "target_floor": target_floor,
            "target_return": target_return,
            "source": source,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "updated_by": updated_by,
        }
    )
    firestore_client().collection(APP_SETTINGS_COLLECTION).document(MONTHLY_TARGET_BAND_DOCUMENT).set(
        normalized,
        merge=True,
        timeout=_settings_firestore_timeout_seconds(),
    )
    with _settings_cache_lock:
        _monthly_target_band_cache = (time(), dict(normalized))
    return normalized
