#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.dedupe import dedupe_key  # noqa: E402
from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow, parse_flex_xml_file  # noqa: E402
from portfolio_backend.ibkr.option_accounting import (  # noqa: E402
    cashflow_summary,
    filter_executions,
    option_executions_from_report,
)
from portfolio_backend.ibkr.source_adapter import options_dataframe_from_report, summarize_options_frame  # noqa: E402
from portfolio_backend.pipeline import build_pipeline_without_live_prices  # noqa: E402


def _load_streamlit_app():
    with contextlib.redirect_stderr(io.StringIO()):
        import streamlit_app as app

    return app


def _state_summary(state) -> dict:
    yearly = state.yearly.copy()
    current_year = int(state.as_of.year)
    year_rows = yearly[yearly["year"].eq(current_year)].to_dict(orient="records") if not yearly.empty else []
    return {
        "df_opts_rows": int(len(state.df_opts)),
        "lots": int(len(state.lots)),
        "open_options_rows": int(len(state.open_options)),
        "realized_option_events": int(len(state.realized_option_events)),
        "stock_txns": int(len(state.stock_txns)),
        "realized_sales": int(len(state.realized_sales)),
        "ending_inventory": int(len(state.ending_inventory)),
        "cumulative_realized": round(float(state.cumulative_realized), 6),
        "grand_total": round(float(state.grand_total), 6),
        "issues_count": int(len(state.issues)),
        "first_issues": state.issues[:5],
        f"year_{current_year}": year_rows[:1],
    }


def _load_combined_report(paths: list[Path]) -> IbkrFlexReport:
    rows_by_section: dict[str, list[IbkrRawRow]] = {}
    seen: set[tuple[str, str]] = set()
    section_counts: dict[str, int] = {}
    from_dates: list[str] = []
    to_dates: list[str] = []
    for path in paths:
        report = parse_flex_xml_file(path)
        if report.metadata.get("fromDate"):
            from_dates.append(report.metadata["fromDate"])
        if report.metadata.get("toDate"):
            to_dates.append(report.metadata["toDate"])
        for section, rows in report.rows_by_section.items():
            for row in rows:
                key = (section, dedupe_key(row.section, row.attrs))
                if key in seen:
                    continue
                seen.add(key)
                rows_by_section.setdefault(section, []).append(row)
    for section, rows in rows_by_section.items():
        section_counts[section] = len(rows)
    metadata = {}
    if from_dates:
        metadata["fromDate"] = min(from_dates)
    if to_dates:
        metadata["toDate"] = max(to_dates)
    metadata["sourceFiles"] = str(len(paths))
    return IbkrFlexReport(
        root_tag="CombinedFlexReports",
        metadata=metadata,
        rows_by_section=rows_by_section,
        section_counts=section_counts,
    )


def _resolve_xml_paths(xml_path: str | None, xml_dir: str | None) -> list[Path]:
    if xml_dir:
        paths = sorted(Path(xml_dir).expanduser().glob("*.xml"))
        if not paths:
            raise FileNotFoundError(f"No XML files found in {xml_dir}")
        return paths
    return [Path(xml_path or "/tmp/ibkr-flex-1503002.xml").expanduser()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare current Google Sheet pipeline output with IBKR-derived option rows."
    )
    parser.add_argument("xml_path", nargs="?", default=None)
    parser.add_argument(
        "--xml-dir",
        help="Directory of IBKR Flex XML files to combine with section-level dedupe.",
    )
    parser.add_argument("--as-of", required=True, help="End date for the comparison window, as YYYY-MM-DD.")
    parser.add_argument(
        "--since",
        default=None,
        help="Only include IBKR adapter rows with trans_date on or after this YYYY-MM-DD date.",
    )
    parser.add_argument(
        "--sheet",
        action="append",
        dest="sheets",
        default=[],
        help="Google Sheet tab to include. Repeatable. Defaults to Options 2024/2025/2026.",
    )
    args = parser.parse_args()

    app = _load_streamlit_app()
    sheets = args.sheets or ["Options 2024", "Options 2025", "Options 2026"]
    xml_paths = _resolve_xml_paths(args.xml_path, args.xml_dir)
    report = _load_combined_report(xml_paths)
    ibkr_df = options_dataframe_from_report(report)
    ibkr_executions = option_executions_from_report(report)
    through_ts = pd.Timestamp(date.fromisoformat(args.as_of))
    if args.since:
        since_ts = pd.Timestamp(date.fromisoformat(args.since))
        ibkr_df = ibkr_df[pd.to_datetime(ibkr_df["trans_date"], errors="coerce") >= since_ts].copy()
    else:
        since_ts = None
    ibkr_df = ibkr_df[pd.to_datetime(ibkr_df["trans_date"], errors="coerce") <= through_ts].copy()
    ibkr_executions = filter_executions(ibkr_executions, since=since_ts, through=through_ts)
    as_of = date.fromisoformat(args.as_of)

    def load_ibkr(_sheet_id, _sheets):
        return ibkr_df.copy()

    def load_sheet(sheet_id, selected_sheets):
        return app.load_options(sheet_id, selected_sheets)

    sheet_state = build_pipeline_without_live_prices(
        app.SHEET_ID,
        as_of,
        False,
        sheets,
        load_sheet,
        app.fetch_price_history_yf,
        app.collect_dividend_cashflows,
        app.align_benchmarks_monthly,
    )
    ibkr_state = build_pipeline_without_live_prices(
        "ibkr",
        as_of,
        False,
        ["IBKR Flex"],
        load_ibkr,
        app.fetch_price_history_yf,
        app.collect_dividend_cashflows,
        app.align_benchmarks_monthly,
    )

    print(
        json.dumps(
            {
                "as_of": args.as_of,
                "since": args.since,
                "sheets": sheets,
                "ibkr_report": {
                    "source_files": len(xml_paths),
                    "metadata": report.metadata,
                    "section_counts": report.section_counts,
                    "options_adapter": summarize_options_frame(ibkr_df),
                    "option_cashflow": cashflow_summary(ibkr_executions),
                },
                "sheet": _state_summary(sheet_state),
                "ibkr_option_adapter_only": _state_summary(ibkr_state),
                "notes": [
                    "IBKR comparison currently feeds only option-shaped rows into the existing pipeline.",
                    "IBKR option_cashflow is the execution-date cash view: short option opening credits minus close debits and commissions.",
                    "For rolled options, option_cashflow is the better comparison to the manual sheet's strategy P&L than close-only lifecycle realized P&L.",
                    "Current sheet pipeline derives stock transactions from assigned option rows.",
                    "IBKR contains explicit stock trades and OptionEAE stock rows; those need a dedicated pipeline extension before final cutover.",
                ],
            },
            indent=2,
            default=str,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
