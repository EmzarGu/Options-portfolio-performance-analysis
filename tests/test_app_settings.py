import pytest

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
