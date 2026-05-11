import os

import pytest

import streamlit_app


@pytest.fixture(autouse=True)
def restore_streamlit_source_env():
    keys = [
        "STREAMLIT_DASHBOARD_SOURCE",
        "OPTIONS_DATA_SOURCE",
        "IBKR_REPORT_SOURCE",
        "FIRESTORE_PROJECT_ID",
        "IBKR_FLEX_QUERY_ID",
        "PRICE_HISTORY_STORE",
        "DIVIDEND_HISTORY_STORE",
        "AUDIT_STORE",
    ]
    original = {key: os.environ.get(key) for key in keys}
    yield
    for key, value in original.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_streamlit_ibkr_source_mode_normalizes_selected_sheets(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")

    assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.available_sources_for_mode() == [streamlit_app.IBKR_SOURCE_LABEL]
    assert streamlit_app.normalize_selected_sheets_for_mode(
        ["Options 2024", "Options 2025", "Options 2026"],
        [streamlit_app.IBKR_SOURCE_LABEL],
    ) == [streamlit_app.IBKR_SOURCE_LABEL]


def test_streamlit_ibkr_source_mode_reads_streamlit_secret(monkeypatch):
    monkeypatch.delenv("OPTIONS_DATA_SOURCE", raising=False)
    monkeypatch.setattr(streamlit_app.st, "secrets", {"OPTIONS_DATA_SOURCE": "ibkr_flex"})

    try:
        assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_IBKR
        assert streamlit_app.os.getenv("OPTIONS_DATA_SOURCE") == "ibkr_flex"
    finally:
        streamlit_app.os.environ.pop("OPTIONS_DATA_SOURCE", None)


def test_streamlit_sheet_source_mode_stays_backend_default(monkeypatch):
    monkeypatch.delenv("OPTIONS_DATA_SOURCE", raising=False)

    assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS


def test_streamlit_app_source_mode_defaults_to_sheet_backup(monkeypatch):
    monkeypatch.delenv("STREAMLIT_DASHBOARD_SOURCE", raising=False)
    monkeypatch.delenv("OPTIONS_DATA_SOURCE", raising=False)
    monkeypatch.delenv("IBKR_REPORT_SOURCE", raising=False)
    monkeypatch.delenv("FIRESTORE_PROJECT_ID", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)
    monkeypatch.delenv("PRICE_HISTORY_STORE", raising=False)
    monkeypatch.delenv("DIVIDEND_HISTORY_STORE", raising=False)
    monkeypatch.delenv("AUDIT_STORE", raising=False)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") is None
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") is None
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") is None
    assert streamlit_app.os.getenv("PRICE_HISTORY_STORE") == "memory"
    assert streamlit_app.os.getenv("DIVIDEND_HISTORY_STORE") == "memory"
    assert streamlit_app.os.getenv("AUDIT_STORE") == "off"


def test_streamlit_sheet_backup_disables_firestore_stores_from_hosting_secrets(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "google_sheets")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID)
    monkeypatch.setenv("PRICE_HISTORY_STORE", "firestore")
    monkeypatch.setenv("DIVIDEND_HISTORY_STORE", "firestore")
    monkeypatch.setenv("AUDIT_STORE", "firestore")

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == "options-performance-dashboard"
    assert streamlit_app.os.getenv("PRICE_HISTORY_STORE") == "memory"
    assert streamlit_app.os.getenv("DIVIDEND_HISTORY_STORE") == "memory"
    assert streamlit_app.os.getenv("AUDIT_STORE") == "off"


def test_streamlit_ibkr_dashboard_source_defaults_report_source_to_firestore_rest(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "ibkr")
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.delenv("IBKR_REPORT_SOURCE", raising=False)
    monkeypatch.delenv("FIRESTORE_PROJECT_ID", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID


def test_streamlit_ibkr_source_mode_replaces_blank_hosting_defaults(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "ibkr")
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", "   ")
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "")

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID


def test_streamlit_ibkr_source_mode_normalizes_old_firestore_secret(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "ibkr")
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", '"firestore"')
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID)
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE


def test_streamlit_ibkr_source_mode_preserves_explicit_report_source(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "ibkr")
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "local_json")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", "custom-project")
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "custom-query")

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == "local_json"
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == "custom-project"
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == "custom-query"


def test_streamlit_sheet_source_mode_remains_explicit_rollback(monkeypatch):
    monkeypatch.setenv("STREAMLIT_DASHBOARD_SOURCE", "google_sheets")
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")

    assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert streamlit_app.normalize_selected_sheets_for_mode(
        ["Options 2024", "Options 2026"],
        ["Options 2024", "Options 2025", "Options 2026"],
        streamlit_app.DATA_SOURCE_GOOGLE_SHEETS,
    ) == ["Options 2024", "Options 2026"]


def test_streamlit_pipeline_cache_key_includes_source_mode(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr_flex")
    ibkr_key = streamlit_app.build_pipeline_cache_key(
        streamlit_app.date(2026, 5, 10),
        True,
        ["Options 2024", "Options 2025", "Options 2026"],
        7,
    )

    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "google_sheets")
    sheet_key = streamlit_app.build_pipeline_cache_key(
        streamlit_app.date(2026, 5, 10),
        True,
        ["Options 2024", "Options 2025", "Options 2026"],
        7,
    )

    assert ibkr_key[0] == streamlit_app.DATA_SOURCE_IBKR
    assert ibkr_key[2] == (streamlit_app.IBKR_SOURCE_LABEL,)
    assert sheet_key[0] == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert sheet_key[2] == ("Options 2024", "Options 2025", "Options 2026")


def test_streamlit_sheet_multiselect_defaults_ignore_stale_ibkr_session(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "google_sheets")
    stale_defaults = ["IBKR Flex", "Options 2025"]
    available = ["Options 2024", "Options 2025", "Options 2026"]

    sanitized = streamlit_app._sanitize_sheet_defaults(
        stale_defaults,
        available,
        ["Options 2024", "Options 2025", "Options 2026"],
    )

    assert sanitized == ["Options 2025"]
