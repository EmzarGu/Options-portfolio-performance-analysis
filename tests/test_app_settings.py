import pytest

import portfolio_backend.app_settings as app_settings
from portfolio_backend.app_settings import (
    DEFAULT_TARGET_FLOOR,
    DEFAULT_TARGET_RETURN,
    default_monthly_target_band,
    normalize_monthly_target_band,
)


def test_default_monthly_target_band_uses_env_and_clamps_floor(monkeypatch):
    monkeypatch.setenv("WEB_MONTHLY_TARGET_RETURN", "0.018")
    monkeypatch.setenv("WEB_MONTHLY_TARGET_FLOOR", "0.02")

    band = default_monthly_target_band()

    assert band["target_return"] == pytest.approx(0.018)
    assert band["target_floor"] == pytest.approx(0.018)
    assert band["source"] == "default"


def test_normalize_monthly_target_band_falls_back_to_defaults(monkeypatch):
    monkeypatch.delenv("WEB_MONTHLY_TARGET_RETURN", raising=False)
    monkeypatch.delenv("MONTHLY_TARGET_RETURN", raising=False)
    monkeypatch.delenv("WEB_MONTHLY_TARGET_FLOOR", raising=False)
    monkeypatch.delenv("MONTHLY_TARGET_FLOOR", raising=False)

    band = normalize_monthly_target_band({"target_return": "bad", "target_floor": -1})

    assert band["target_return"] == pytest.approx(DEFAULT_TARGET_RETURN)
    assert band["target_floor"] == pytest.approx(DEFAULT_TARGET_FLOOR)


def test_normalize_monthly_target_band_preserves_metadata_and_clamps():
    band = normalize_monthly_target_band(
        {
            "target_return": 0.015,
            "target_floor": 0.02,
            "source": "web",
            "updated_at": "2026-06-02T10:00:00+00:00",
            "updated_by": "user@example.com",
        }
    )

    assert band == {
        "target_return": 0.015,
        "target_floor": 0.015,
        "source": "web",
        "updated_at": "2026-06-02T10:00:00+00:00",
        "updated_by": "user@example.com",
    }


def test_load_monthly_target_band_uses_short_cache(monkeypatch):
    monkeypatch.setattr(app_settings, "_monthly_target_band_cache", None)
    calls = {"get": 0}

    class Snapshot:
        exists = True

        def to_dict(self):
            return {"target_return": 0.02, "target_floor": 0.015}

    class Document:
        def get(self, *, timeout=None):
            calls["get"] += 1
            assert timeout == pytest.approx(app_settings.DEFAULT_SETTINGS_FIRESTORE_TIMEOUT_SECONDS)
            return Snapshot()

    class Collection:
        def document(self, _document_id):
            return Document()

    class Client:
        def collection(self, _collection_name):
            return Collection()

    monkeypatch.setenv("APP_SETTINGS_CACHE_SECONDS", "60")
    monkeypatch.delenv("APP_SETTINGS_FIRESTORE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(app_settings, "firestore_client", lambda: Client())

    first = app_settings.load_monthly_target_band()
    second = app_settings.load_monthly_target_band()

    assert first["target_return"] == pytest.approx(0.02)
    assert second == first
    assert calls["get"] == 1
