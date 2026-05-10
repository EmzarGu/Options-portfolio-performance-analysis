from __future__ import annotations

from pathlib import Path
from datetime import date
from types import SimpleNamespace

import pytest

from portfolio_backend.ibkr.dedupe import dedupe_key, raw_row_id
from portfolio_backend.ibkr.flex_client import DateRange, plan_backfill_ranges, processing_message
from portfolio_backend.ibkr.flex_parser import count_trade_asset_categories, parse_flex_xml_file
from portfolio_backend.ibkr.import_job import _auto_target_range_from_args, _date_range_from_args, plan_missing_import_ranges
from portfolio_backend.ibkr.importer import IbkrImportService, LocalJsonImportStore, LocalRawReportStore
from portfolio_backend.ibkr.normalization import normalize_transactions, redacted_preview
from portfolio_backend.ibkr.persisted_report import LocalJsonFlexReportRepository
from portfolio_backend.ibkr.repository import combine_flex_reports, load_flex_report_from_env, resolve_local_flex_xml_paths
from portfolio_backend.ibkr.source_adapter import options_dataframe_from_report, summarize_options_frame
from scripts.ibkr_backfill import _is_statement_unavailable, _split_range


FIXTURE = Path(__file__).parent / "fixtures" / "ibkr_flex_sample.xml"


def test_parse_flex_xml_sections_and_metadata():
    report = parse_flex_xml_file(FIXTURE)

    assert report.root_tag == "FlexQueryResponse"
    assert report.metadata == {
        "fromDate": "20260101",
        "toDate": "20260131",
        "period": "Last365CalendarDays",
    }
    assert len(report.rows("Trade")) == 2
    assert len(report.rows("OptionEAE")) == 1
    assert len(report.rows("CashTransaction")) == 1
    assert len(report.rows("OpenPosition")) == 1
    assert len(report.rows("SecurityInfo")) == 1
    assert count_trade_asset_categories(report.rows("Trade")) == {"OPT": 1, "STK": 1}


def test_dedupe_prefers_ibkr_trade_identifiers():
    trade = parse_flex_xml_file(FIXTURE).rows("Trade")[0]

    assert dedupe_key(trade.section, trade.attrs) == "trade|U0000000|T1|X1|E1"
    assert len(raw_row_id(trade.section, trade.attrs)) == 64


def test_normalize_trade_and_option_eae_rows():
    report = parse_flex_xml_file(FIXTURE)
    normalized = normalize_transactions([*report.rows("Trade"), *report.rows("OptionEAE")])

    assert len(normalized) == 3
    option_trade = normalized[0]
    stock_trade = normalized[1]
    assignment = normalized[2]

    assert option_trade.source_section == "Trade"
    assert option_trade.asset_category == "OPT"
    assert option_trade.underlying_symbol == "ABC"
    assert option_trade.trade_date == "2026-01-15"
    assert option_trade.date_time == "2026-01-15T15:45:00"
    assert option_trade.expiry == "2026-01-17"
    assert option_trade.strike == 100.0
    assert option_trade.put_call == "P"
    assert option_trade.buy_sell == "SELL"
    assert option_trade.quantity == -1.0
    assert option_trade.net_cash == 249.0
    assert option_trade.ib_commission == -1.0

    assert stock_trade.asset_category == "STK"
    assert stock_trade.symbol == "ABC"
    assert stock_trade.quantity == 100.0
    assert stock_trade.multiplier == 1.0

    assert assignment.source_section == "OptionEAE"
    assert assignment.transaction_type == "Assignment"
    assert assignment.realized_pnl == 249.0


def test_redacted_preview_hides_identifiers():
    trade = parse_flex_xml_file(FIXTURE).rows("Trade")[0]
    preview = redacted_preview(normalize_transactions([trade])[0])

    assert preview["account_id"] == "<redacted>"
    assert preview["symbol"] == "<redacted>"
    assert preview["underlying_symbol"] == "<redacted>"
    assert preview["transaction_id"] == "<redacted>"
    assert preview["dedupe_key"] == "<redacted>"
    assert preview["asset_category"] == "OPT"


def test_options_dataframe_from_report_maps_to_current_pipeline_shape():
    report = parse_flex_xml_file(FIXTURE)
    df = options_dataframe_from_report(report)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["trans_date"].isoformat() == "2026-01-15T00:00:00"
    assert row["ticker"] == "ABC"
    assert row["type"] == "Put"
    assert row["action"] == "Sell"
    assert row["expiration"].isoformat() == "2026-01-17T00:00:00"
    assert row["strike"] == 100.0
    assert row["qty"] == 1.0
    assert row["amount"] == 250.0
    assert row["commission"] == 1.0
    assert row["total_pnl"] == 249.0
    assert row["assigned_flag"] == 1.0
    assert row["source_sheet"] == "IBKR Flex 2026"


def test_summarize_options_frame_reports_counts_and_sums():
    summary = summarize_options_frame(options_dataframe_from_report(parse_flex_xml_file(FIXTURE)))

    assert summary["rows"] == 1
    assert summary["date_min"] == "2026-01-15"
    assert summary["date_max"] == "2026-01-15"
    assert summary["assigned_rows"] == 1
    assert summary["action_type_counts"] == [{"action": "Sell", "type": "Put", "rows": 1}]
    assert summary["action_sums"] == [
        {"action": "Sell", "qty": 1.0, "amount": 250.0, "commission": 1.0, "total_pnl": 249.0}
    ]


def test_plan_backfill_ranges_uses_365_day_chunks():
    ranges = plan_backfill_ranges(date(2022, 1, 1), date(2026, 5, 9))

    assert [r.days_inclusive for r in ranges] == [365, 365, 365, 365, 130]
    assert ranges[0] == DateRange(date(2022, 1, 1), date(2022, 12, 31))
    assert ranges[-1] == DateRange(date(2025, 12, 31), date(2026, 5, 9))
    assert ranges[-1].fd == "20251231"
    assert ranges[-1].td == "20260509"


def test_plan_backfill_ranges_rejects_reversed_dates():
    try:
        plan_backfill_ranges(date(2026, 1, 2), date(2026, 1, 1))
    except ValueError as exc:
        assert "end must be on or after start" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_processing_message_recognizes_ibkr_generation_in_progress():
    data = b"""
    <FlexStatementResponse>
      <Status>Fail</Status>
      <ErrorCode>1019</ErrorCode>
      <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
    </FlexStatementResponse>
    """

    assert processing_message(data) == "1019: Statement generation in progress. Please try again shortly."


def test_import_job_explicit_date_range_overrides_rolling_env(monkeypatch):
    monkeypatch.setenv("IBKR_IMPORT_LAST_DAYS", "14")

    date_range = _date_range_from_args(
        SimpleNamespace(
            from_date="2026-05-01",
            to_date="2026-05-08",
            last_days=None,
            to_offset_days=None,
        )
    )

    assert date_range == DateRange(date(2026, 5, 1), date(2026, 5, 8))


def test_import_job_rejects_partial_explicit_date_range():
    try:
        _date_range_from_args(
            SimpleNamespace(from_date="2026-05-01", to_date=None, last_days=None, to_offset_days=None)
        )
    except RuntimeError as exc:
        assert "Set both" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_import_job_can_build_rolling_range(monkeypatch):
    class FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            import datetime as dt

            return dt.datetime(2026, 5, 9, tzinfo=tz)

    monkeypatch.setattr("portfolio_backend.ibkr.import_job.datetime", FixedDateTime)

    date_range = _date_range_from_args(
        SimpleNamespace(from_date=None, to_date=None, last_days=14, to_offset_days=1)
    )

    assert date_range == DateRange(date(2026, 4, 25), date(2026, 5, 8))


def test_import_job_can_build_auto_target_range(monkeypatch):
    class FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            import datetime as dt

            return dt.datetime(2026, 5, 9, tzinfo=tz)

    monkeypatch.setattr("portfolio_backend.ibkr.import_job.datetime", FixedDateTime)

    date_range = _auto_target_range_from_args(
        SimpleNamespace(
            inception_date="2022-11-01",
            to_offset_days=1,
        )
    )

    assert date_range == DateRange(date(2022, 11, 1), date(2026, 5, 8))


def test_plan_missing_import_ranges_backfills_gaps_and_recent_overlap():
    planned = plan_missing_import_ranges(
        target=DateRange(date(2022, 11, 1), date(2026, 5, 8)),
        existing=[
            DateRange(date(2022, 11, 1), date(2023, 10, 31)),
            DateRange(date(2024, 11, 1), date(2026, 5, 1)),
        ],
        recent_overlap_days=14,
    )

    assert planned == [
        DateRange(date(2023, 11, 1), date(2024, 10, 30)),
        DateRange(date(2024, 10, 31), date(2024, 10, 31)),
        DateRange(date(2026, 4, 25), date(2026, 5, 1)),
        DateRange(date(2026, 5, 2), date(2026, 5, 8)),
    ]


def test_plan_missing_import_ranges_backfills_full_inception_after_reset():
    planned = plan_missing_import_ranges(
        target=DateRange(date(2022, 11, 1), date(2026, 5, 8)),
        existing=[],
        recent_overlap_days=14,
    )

    assert planned == [
        DateRange(date(2022, 11, 1), date(2023, 10, 31)),
        DateRange(date(2023, 11, 1), date(2024, 10, 30)),
        DateRange(date(2024, 10, 31), date(2025, 10, 30)),
        DateRange(date(2025, 10, 31), date(2026, 5, 8)),
    ]


def test_plan_missing_import_ranges_only_recent_overlap_when_coverage_complete():
    planned = plan_missing_import_ranges(
        target=DateRange(date(2022, 11, 1), date(2026, 5, 8)),
        existing=[DateRange(date(2022, 1, 1), date(2026, 5, 8))],
        recent_overlap_days=14,
    )

    assert planned == [DateRange(date(2026, 4, 25), date(2026, 5, 8))]


def test_backfill_split_helpers_isolate_unavailable_ranges():
    chunk = DateRange(date(2025, 6, 24), date(2025, 12, 20))
    left, right = _split_range(chunk)

    assert _is_statement_unavailable("IBKR SendRequest error: 1003: Statement is not available.")
    assert left == DateRange(date(2025, 6, 24), date(2025, 9, 21))
    assert right == DateRange(date(2025, 9, 22), date(2025, 12, 20))
    assert left.days_inclusive + right.days_inclusive == chunk.days_inclusive


def test_combined_report_dedupes_overlapping_files():
    combined = combine_flex_reports([FIXTURE, FIXTURE])

    assert combined.metadata["sourceFiles"] == "2"
    assert len(combined.rows("Trade")) == 2
    assert len(combined.rows("OptionEAE")) == 1


def test_resolve_local_flex_xml_paths_supports_explicit_directory(tmp_path):
    target = tmp_path / "report.xml"
    target.write_bytes(FIXTURE.read_bytes())

    assert resolve_local_flex_xml_paths(xml_dir=str(tmp_path), env_path=None) == [target]


def test_local_ibkr_import_service_writes_raw_rows_and_transactions(tmp_path):
    service = IbkrImportService(
        LocalRawReportStore(tmp_path / "raw"),
        LocalJsonImportStore(tmp_path / "firestore_sim"),
    )

    first = service.import_xml(FIXTURE.read_bytes(), query_id="1503002", run_id="run-1")
    second = service.import_xml(FIXTURE.read_bytes(), query_id="1503002", run_id="run-2")

    assert first.write_summary.inserted_raw_rows == 6
    assert first.write_summary.updated_raw_rows == 0
    assert first.write_summary.inserted_transactions == 3
    assert first.write_summary.updated_transactions == 0
    assert second.write_summary.inserted_raw_rows == 0
    assert second.write_summary.updated_raw_rows == 6
    assert second.write_summary.inserted_transactions == 0
    assert second.write_summary.updated_transactions == 3
    assert first.raw_report.local_path is not None
    assert Path(first.raw_report.local_path).exists()
    assert len(list((tmp_path / "firestore_sim" / "ibkr_raw_rows").glob("*.json"))) == 6
    assert len(list((tmp_path / "firestore_sim" / "ibkr_transactions").glob("*.json"))) == 3


def test_local_import_store_reports_successful_import_ranges(tmp_path):
    service = IbkrImportService(
        LocalRawReportStore(tmp_path / "raw"),
        LocalJsonImportStore(tmp_path / "firestore_sim"),
    )
    service.import_xml(FIXTURE.read_bytes(), query_id="1503002", run_id="run-1")

    ranges = LocalJsonImportStore(tmp_path / "firestore_sim").successful_import_ranges(query_id="1503002")

    assert ranges == [DateRange(date(2026, 1, 1), date(2026, 1, 31))]


def test_local_persisted_report_repository_rebuilds_flex_report(tmp_path):
    service = IbkrImportService(
        LocalRawReportStore(tmp_path / "raw"),
        LocalJsonImportStore(tmp_path / "firestore_sim"),
    )
    service.import_xml(FIXTURE.read_bytes(), query_id="1503002", run_id="run-1")

    report = LocalJsonFlexReportRepository(tmp_path / "firestore_sim").load_report(query_id="1503002")
    option_only = LocalJsonFlexReportRepository(tmp_path / "firestore_sim").load_report(
        query_id="1503002",
        sections=["Trade"],
    )

    assert len(report.rows("Trade")) == 2
    assert len(report.rows("OptionEAE")) == 1
    assert len(report.rows("CashTransaction")) == 1
    assert report.metadata["queryId"] == "1503002"
    assert report.metadata["fromDate"] == "20260101"
    assert report.metadata["toDate"] == "20260131"
    assert option_only.section_counts == {"Trade": 2}


def test_local_persisted_report_repository_fails_loudly_for_missing_rows(tmp_path):
    with pytest.raises(FileNotFoundError, match="No persisted IBKR raw rows"):
        LocalJsonFlexReportRepository(tmp_path / "firestore_sim").load_report(query_id="missing")


def test_load_flex_report_from_env_can_use_local_json_store(tmp_path, monkeypatch):
    service = IbkrImportService(
        LocalRawReportStore(tmp_path / "raw"),
        LocalJsonImportStore(tmp_path / "firestore_sim"),
    )
    service.import_xml(FIXTURE.read_bytes(), query_id="1503002", run_id="run-1")
    monkeypatch.setenv("IBKR_REPORT_SOURCE", "local_json")
    monkeypatch.setenv("IBKR_IMPORT_JSON_DIR", str(tmp_path / "firestore_sim"))
    monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "1503002")

    report = load_flex_report_from_env(env_path=None)

    assert len(report.rows("Trade")) == 2
    assert len(report.rows("OptionEAE")) == 1
