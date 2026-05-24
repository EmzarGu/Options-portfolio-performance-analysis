#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_sources import download_excel_workbook  # noqa: E402
from portfolio_backend.gcp import service_account_credentials_from_config  # noqa: E402
from portfolio_backend.ibkr.persisted_report import (  # noqa: E402
    FirestoreFlexReportRepository,
    LocalJsonFlexReportRepository,
)
from portfolio_backend.option_market.models import now_iso  # noqa: E402
from portfolio_backend.option_market.store import (  # noqa: E402
    FirestoreOptionMarketStore,
    LocalJsonOptionMarketStore,
    MemoryOptionMarketStore,
)
from portfolio_backend.option_market.validation import (  # noqa: E402
    build_probability_history_import,
    candidates_from_ibkr_report,
    extract_sheet_probability_rows,
)


DEFAULT_SHEET_ID = "19LhrZai3cbJ1GbPE1iTquYHUeXfpIxXFX1amF5eWi_g"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Google Sheet historical option probability rows and match them to IBKR trades."
    )
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--sheet-id", default=os.getenv("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID))
    parser.add_argument("--sheet", action="append", dest="sheets", default=[])
    parser.add_argument("--store", choices=["local-json", "firestore", "memory"], default="local-json")
    parser.add_argument("--local-json-dir", default="tmp/option_probability_history/firestore_sim")
    parser.add_argument(
        "--ibkr-report-source",
        choices=["firestore", "local-json"],
        default=os.getenv("IBKR_REPORT_SOURCE", "firestore"),
    )
    parser.add_argument(
        "--ibkr-local-json-dir",
        default=os.getenv("IBKR_IMPORT_JSON_DIR", "tmp/ibkr_import/firestore_sim"),
    )
    parser.add_argument("--query-id", default=os.getenv("IBKR_FLEX_QUERY_ID"))
    parser.add_argument("--matched-only", action="store_true")
    parser.add_argument("--output-dir", default="tmp/option_probability_history")
    args = parser.parse_args()

    if args.end_year < args.start_year:
        raise SystemExit("--end-year must be greater than or equal to --start-year")

    years = list(range(args.start_year, args.end_year + 1))
    sheets = args.sheets or [f"Options {year}" for year in years]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report = _load_ibkr_report(args)
    sheet_df = _load_sheet_options(args.sheet_id, sheets)
    candidates = []
    probability_rows = []
    for year in years:
        candidates.extend(candidates_from_ibkr_report(report, year=year))
        probability_rows.extend(extract_sheet_probability_rows(sheet_df, year=year))

    imported = build_probability_history_import(candidates, probability_rows)
    matches = imported.trade_matches
    if args.matched_only:
        matches = [match for match in matches if match.matched]
    summary = {
        **imported.summary,
        "persisted_trade_match_count": len(matches),
        "start_year": args.start_year,
        "end_year": args.end_year,
        "sheets": sheets,
    }

    store = _build_store(args)
    run_id = datetime.now(timezone.utc).strftime("option-probability-history-%Y%m%dT%H%M%S%fZ")
    run_doc = {
        "run_id": run_id,
        "status": "running",
        "started_at": now_iso(),
        "source": "google_sheet",
        "sheet_id": args.sheet_id,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "sheets": sheets,
        "query_id": args.query_id,
    }
    store.begin_probability_import_run(run_doc)
    store.save_probability_history(imported.probability_rows, matches)
    finish_doc = {
        "status": "succeeded",
        "finished_at": now_iso(),
        "summary": summary,
        "probability_row_ids": [row.row_id for row in imported.probability_rows],
        "trade_match_ids": [match.match_id for match in matches],
        "unmatched_probability_row_ids": [row.row_id for row in imported.unmatched_probability_rows],
    }
    store.finish_probability_import_run(run_id, finish_doc)

    _write_outputs(output_dir, run_id, imported.probability_rows, matches, imported.unmatched_probability_rows, summary)
    print(json.dumps({**run_doc, **finish_doc}, indent=2, sort_keys=True, default=str))
    return 0


def _load_ibkr_report(args):
    if args.ibkr_report_source == "local-json":
        return LocalJsonFlexReportRepository(args.ibkr_local_json_dir).load_report(
            query_id=args.query_id,
            sections=["Trade"],
        )
    return FirestoreFlexReportRepository().load_report(query_id=args.query_id, sections=["Trade"])


def _load_sheet_options(sheet_id: str, sheets: list[str]) -> pd.DataFrame:
    credentials, _ = service_account_credentials_from_config()
    download = download_excel_workbook(sheet_id, credentials=credentials)
    frames = []
    for sheet in sheets:
        raw = pd.read_excel(io.BytesIO(download.content), sheet_name=sheet, header=1)
        raw["source_sheet"] = sheet
        raw["source_row_number"] = raw.index + 3
        frames.append(raw)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_store(args):
    if args.store == "firestore":
        return FirestoreOptionMarketStore()
    if args.store == "memory":
        return MemoryOptionMarketStore()
    return LocalJsonOptionMarketStore(args.local_json_dir)


def _write_outputs(output_dir: Path, run_id: str, rows, matches, unmatched_rows, summary: dict[str, Any]) -> None:
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row.as_dict() for row in rows]).to_csv(run_dir / "probability_rows.csv", index=False)
    pd.DataFrame([match.as_dict() for match in matches]).to_csv(run_dir / "probability_trade_matches.csv", index=False)
    pd.DataFrame([row.as_dict() for row in unmatched_rows]).to_csv(run_dir / "unmatched_probability_rows.csv", index=False)
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
