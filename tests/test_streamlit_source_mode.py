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


def test_streamlit_sheet_source_mode_stays_default(monkeypatch):
    monkeypatch.delenv("OPTIONS_DATA_SOURCE", raising=False)

    assert streamlit_app.data_source_mode() == streamlit_app.DATA_SOURCE_GOOGLE_SHEETS
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
