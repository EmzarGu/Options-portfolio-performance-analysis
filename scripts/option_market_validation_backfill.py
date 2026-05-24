#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

from portfolio_backend.ibkr.persisted_report import (  # noqa: E402
    FirestoreFlexReportRepository,
    LocalJsonFlexReportRepository,
)
from data_sources import download_excel_workbook, load_options_from_excel_bytes  # noqa: E402
from portfolio_backend.gcp import service_account_credentials_from_config  # noqa: E402
from portfolio_backend.option_market.models import contract_from_dict, now_iso  # noqa: E402
from portfolio_backend.option_market.store import (  # noqa: E402
    FirestoreOptionMarketStore,
    LocalJsonOptionMarketStore,
    MemoryOptionMarketStore,
)
from portfolio_backend.option_market.validation import (  # noqa: E402
    attach_sheet_probabilities,
    build_validation_report,
    candidates_from_ibkr_report,
    dedupe_chain_requests,
    extract_sheet_probability_rows,
    match_trade_to_contract,
)

DEFAULT_SHEET_ID = "19LhrZai3cbJ1GbPE1iTquYHUeXfpIxXFX1amF5eWi_g"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill and validate trade-scoped historical option market data."
    )
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--provider", default="none")
    parser.add_argument("--store", choices=["local-json", "firestore", "memory"], default="local-json")
    parser.add_argument("--local-json-dir", default="tmp/option_market_validation/firestore_sim")
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
    parser.add_argument("--include-without-probability", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-refetch", action="store_true")
    parser.add_argument("--max-requests", type=int, default=None)
    parser.add_argument("--output-dir", default="tmp/option_market_validation")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report = _load_ibkr_report(args)
    sheet_df = _load_sheet_options(args.sheet_id, args.sheets or [f"Options {args.year}"])
    sheet_prob_rows = extract_sheet_probability_rows(sheet_df, year=args.year)
    candidates, unmatched_sheet_rows = attach_sheet_probabilities(
        candidates_from_ibkr_report(report, year=args.year),
        sheet_prob_rows,
    )
    if not args.include_without_probability:
        candidates = [candidate for candidate in candidates if candidate.profit_probability is not None]

    requests = dedupe_chain_requests(candidates, provider=args.provider)
    if args.max_requests is not None:
        requests = requests[: args.max_requests]

    store = _build_store(args)
    run_id = datetime.now(timezone.utc).strftime("option-market-%Y%m%dT%H%M%S%fZ")
    run_doc = {
        "run_id": run_id,
        "provider": args.provider,
        "year": args.year,
        "status": "dry_run" if args.dry_run else "running",
        "started_at": now_iso(),
        "candidate_count": len(candidates),
        "request_count": len(requests),
        "query_id": args.query_id,
    }
    store.begin_fetch_run(run_doc)

    if args.dry_run:
        _write_outputs(output_dir, run_id, candidates, [], build_validation_report([], unmatched_sheet_rows))
        store.finish_fetch_run(run_id, {"status": "dry_run", "finished_at": now_iso()})
        print(json.dumps({**run_doc, "requests": [request.as_dict() for request in requests]}, indent=2))
        return 0

    provider = _build_provider(args)
    contracts_by_request: dict[str, list[Any]] = {}
    fetched = 0
    reused = 0
    failed = 0
    for request in requests:
        snapshot = None if args.force_refetch else store.load_chain_snapshot(request)
        if snapshot:
            contract_docs = store.load_contracts(request)
            contracts_by_request[request.request_id] = [contract_from_dict(doc) for doc in contract_docs]
            reused += 1
            continue
        result = provider.fetch_historical_chain(
            ticker=request.ticker,
            trade_date=request.trade_date,
            expiry=request.expiry,
            put_call=request.put_call,
        )
        store.save_chain_snapshot(result)
        contracts_by_request[request.request_id] = result.contracts
        fetched += 1
        failed += int(bool(result.error))

    request_by_key = {request.request_id: request for request in requests}
    request_id_by_trade_key = {
        (request.ticker, request.trade_date, request.expiry, request.put_call): request.request_id
        for request in requests
    }
    matches = []
    for candidate in candidates:
        request_id = request_id_by_trade_key.get(candidate.request_key)
        if request_id is None:
            continue
        matches.append(
            match_trade_to_contract(
                candidate,
                request_id,
                contracts_by_request.get(request_id, []),
            )
        )
    store.save_trade_matches(matches)
    validation_report = build_validation_report(matches, unmatched_sheet_rows)
    _write_outputs(output_dir, run_id, candidates, matches, validation_report)
    finish_doc = {
        "status": "succeeded" if failed == 0 else "completed_with_errors",
        "finished_at": now_iso(),
        "fetched_requests": fetched,
        "reused_requests": reused,
        "failed_requests": failed,
        "summary": validation_report.summary,
        "request_ids": sorted(request_by_key),
    }
    store.finish_fetch_run(run_id, finish_doc)
    print(json.dumps({**run_doc, **finish_doc}, indent=2, sort_keys=True))
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
    return load_options_from_excel_bytes(download.content, sheets)


def _build_store(args):
    if args.store == "firestore":
        return FirestoreOptionMarketStore()
    if args.store == "memory":
        return MemoryOptionMarketStore()
    return LocalJsonOptionMarketStore(args.local_json_dir)


def _build_provider(args):
    raise RuntimeError("No active option market provider configured.")


def _write_outputs(output_dir: Path, run_id: str, candidates, matches, report) -> None:
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([candidate.as_dict() for candidate in candidates]).to_csv(run_dir / "trade_candidates.csv", index=False)
    pd.DataFrame([match.as_dict() for match in matches]).to_csv(run_dir / "trade_matches.csv", index=False)
    pd.DataFrame(report.bucket_summary).to_csv(run_dir / "risk_bucket_summary.csv", index=False)
    (run_dir / "summary.json").write_text(
        json.dumps(report.summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (run_dir / "report.md").write_text(_markdown_report(report), encoding="utf-8")


def _markdown_report(report) -> str:
    summary = report.summary
    lines = [
        "# Option Market Validation Report",
        "",
        f"- Trades evaluated: {summary.get('trade_count', 0)}",
        f"- Matched provider contracts: {summary.get('matched_contract_count', 0)} "
        f"({summary.get('matched_contract_rate')})",
        f"- Matched rows with delta: {summary.get('matched_delta_count', 0)} "
        f"({summary.get('matched_delta_rate')})",
        f"- Unmatched sheet probability rows: {summary.get('unmatched_sheet_probability_rows', 0)}",
        f"- Warning types: {', '.join(summary.get('warning_types') or []) or 'none'}",
        "",
        "## Risk Buckets",
        "",
        "| Bucket | Trades | Matched | Avg sheet risk | Avg delta risk | Avg mark-fill |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in report.bucket_summary:
        lines.append(
            "| {bucket} | {trades} | {matched} | {sheet} | {delta} | {mark} |".format(
                bucket=row["bucket"],
                trades=row["trades"],
                matched=row["matched"],
                sheet=_fmt(row.get("avg_sheet_assignment_risk_proxy")),
                delta=_fmt(row.get("avg_provider_delta_risk")),
                mark=_fmt(row.get("avg_mark_minus_fill")),
            )
        )
    lines.extend(
        [
            "",
            "This report is validation-only. It does not change accounting, mobile payloads, or dashboard output.",
        ]
    )
    return "\n".join(lines) + "\n"


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
