import streamlit_app


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


def test_streamlit_app_source_mode_defaults_to_ibkr(monkeypatch):
    monkeypatch.delenv("OPTIONS_DATA_SOURCE", raising=False)
    monkeypatch.delenv("IBKR_REPORT_SOURCE", raising=False)
    monkeypatch.delenv("FIRESTORE_PROJECT_ID", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID


def test_streamlit_ibkr_source_mode_defaults_report_source_to_firestore_rest(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.delenv("IBKR_REPORT_SOURCE", raising=False)
    monkeypatch.delenv("FIRESTORE_PROJECT_ID", raising=False)
    monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID


def test_streamlit_ibkr_source_mode_replaces_blank_hosting_defaults(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", "   ")
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "")

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID


def test_streamlit_ibkr_source_mode_normalizes_old_firestore_secret(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "firestore")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", streamlit_app.DEFAULT_FIRESTORE_PROJECT_ID)
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", streamlit_app.DEFAULT_IBKR_FLEX_QUERY_ID)

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == streamlit_app.DEFAULT_STREAMLIT_IBKR_REPORT_SOURCE


def test_streamlit_ibkr_source_mode_preserves_explicit_report_source(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "local_json")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", "custom-project")
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "custom-query")

    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_IBKR
    assert streamlit_app.os.getenv("IBKR_REPORT_SOURCE") == "local_json"
    assert streamlit_app.os.getenv("FIRESTORE_PROJECT_ID") == "custom-project"
    assert streamlit_app.os.getenv("IBKR_FLEX_QUERY_ID") == "custom-query"


def test_streamlit_sheet_source_mode_remains_explicit_rollback(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "google_sheets")

    assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert streamlit_app.streamlit_app_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
    assert streamlit_app.normalize_selected_sheets_for_mode(
        ["Options 2024", "Options 2026"],
        ["Options 2024", "Options 2025", "Options 2026"],
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
