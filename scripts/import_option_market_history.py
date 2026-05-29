#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import datetime
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
from portfolio_backend.option_market.cutemarkets import CuteMarketsClient  # noqa: E402
from portfolio_backend.option_market.history import run_historical_option_enrichment  # noqa: E402
from portfolio_backend.option_market.store import (  # noqa: E402
    FirestoreOptionMarketStore,
    LocalJsonOptionMarketStore,
    MemoryOptionMarketStore,
)
from portfolio_backend.option_market.validation import (  # noqa: E402
    attach_sheet_probabilities,
    candidates_from_ibkr_report,
    extract_sheet_probability_rows,
)


DEFAULT_SHEET_ID = "19LhrZai3cbJ1GbPE1iTquYHUeXfpIxXFX1amF5eWi_g"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Persist CuteMarkets historical option facts for actual IBKR short-option opening trades."
    )
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--store", choices=["local-json", "firestore", "memory"], default="local-json")
    parser.add_argument("--local-json-dir", default="tmp/option_market_history/firestore_sim")
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
    parser.add_argument("--sheet-id", default=os.getenv("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID))
    parser.add_argument("--sheet", action="append", dest="sheets", default=[])
    parser.add_argument("--skip-google-sheet", action="store_true")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--max-provider-calls", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="tmp/option_market_history")
    args = parser.parse_args()

    if args.end_year < args.start_year:
        raise SystemExit("--end-year must be greater than or equal to --start-year")

    years = list(range(args.start_year, args.end_year + 1))
    report = _load_ibkr_report(args)
    candidates = []
    for year in years:
        candidates.extend(candidates_from_ibkr_report(report, year=year))

    sheet_rows: list[dict[str, Any]] = []
    if not args.skip_google_sheet:
        sheet_rows = _load_sheet_probability_rows(args, years)
    candidates, unmatched_sheet_rows = attach_sheet_probabilities(candidates, sheet_rows)

    summary = {
        "start_year": args.start_year,
        "end_year": args.end_year,
        "candidate_count": len(candidates),
        "sheet_probability_row_count": len(sheet_rows),
        "candidates_with_sheet_probability": sum(1 for candidate in candidates if candidate.profit_probability is not None),
        "unmatched_sheet_probability_row_count": len(unmatched_sheet_rows),
        "missing_only": not args.refresh_existing,
    }
    if args.dry_run:
        print(json.dumps({**summary, "status": "dry_run"}, indent=2, sort_keys=True, default=str))
        return 0

    provider = CuteMarketsClient()
    if not provider.configured:
        raise SystemExit("CUTEMARKETS_API_KEY is not configured")

    store = _build_store(args)
    result = run_historical_option_enrichment(
        candidates,
        store=store,
        provider=provider,
        missing_only=not args.refresh_existing,
        max_provider_calls=args.max_provider_calls,
    )
    _write_outputs(Path(args.output_dir), result.run_doc, result.enrichments, summary)
    print(json.dumps({**summary, **result.run_doc}, indent=2, sort_keys=True, default=str))
    return 0


def _load_ibkr_report(args):
    if args.ibkr_report_source == "local-json":
        return LocalJsonFlexReportRepository(args.ibkr_local_json_dir).load_report(
            query_id=args.query_id,
            sections=["Trade"],
        )
    return FirestoreFlexReportRepository().load_report(query_id=args.query_id, sections=["Trade"])


def _load_sheet_probability_rows(args, years: list[int]) -> list[dict[str, Any]]:
    sheets = args.sheets or [f"Options {year}" for year in years]
    try:
        credentials, _ = service_account_credentials_from_config()
        download = download_excel_workbook(args.sheet_id, credentials=credentials)
    except Exception as exc:
        print(f"warning: Google Sheet probability history unavailable: {exc}", file=sys.stderr)
        return []

    rows: list[dict[str, Any]] = []
    for sheet in sheets:
        try:
            raw = pd.read_excel(io.BytesIO(download.content), sheet_name=sheet, header=1)
        except Exception as exc:
            print(f"warning: sheet {sheet!r} unavailable: {exc}", file=sys.stderr)
            continue
        raw["source_sheet"] = sheet
        raw["source_row_number"] = raw.index + 3
        for year in years:
            rows.extend(extract_sheet_probability_rows(raw, year=year))
    return rows


def _build_store(args):
    if args.store == "firestore":
        return FirestoreOptionMarketStore()
    if args.store == "memory":
        return MemoryOptionMarketStore()
    return LocalJsonOptionMarketStore(args.local_json_dir)


def _write_outputs(output_dir: Path, run_doc: dict[str, Any], enrichments, summary: dict[str, Any]) -> None:
    run_id = str(run_doc["run_id"])
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row.as_dict() for row in enrichments]).to_json(
        run_dir / "historical_enrichments.json",
        orient="records",
        indent=2,
        date_format="iso",
    )
    (run_dir / "summary.json").write_text(
        json.dumps({**summary, **run_doc}, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
