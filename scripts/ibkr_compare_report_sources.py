#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.mobile_service import build_ibkr_mobile_payload_context  # noqa: E402
from portfolio_backend.ibkr.repository import load_flex_report_from_env, load_local_flex_report  # noqa: E402
from portfolio_backend.mobile_api_service import (  # noqa: E402
    MobilePayloadRequest,
    MobileServiceDependencies,
    build_mobile_dashboard_payload,
    build_mobile_issues_payload,
    build_mobile_monthly_payload,
    build_mobile_open_option_shorts_payload,
    build_mobile_positions_payload,
    build_mobile_tickers_payload,
    build_mobile_yearly_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare IBKR mobile payloads from local XML and a persisted report source."
    )
    parser.add_argument("--xml-dir", default=str(REPO_ROOT / "tmp" / "ibkr_backfill" / "query-1503002"))
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--query-id", default=os.environ.get("IBKR_FLEX_QUERY_ID", "1503002"))
    parser.add_argument(
        "--persisted-source",
        choices=("local_json", "firestore"),
        default="local_json",
    )
    parser.add_argument("--json-dir", default=str(REPO_ROOT / "tmp" / "ibkr_import_full" / "firestore_sim"))
    args = parser.parse_args()

    os.environ["IBKR_REPORT_SOURCE"] = args.persisted_source
    os.environ["IBKR_FLEX_QUERY_ID"] = args.query_id
    if args.persisted_source == "local_json":
        os.environ["IBKR_IMPORT_JSON_DIR"] = args.json_dir

    request = MobilePayloadRequest("ibkr-flex", date.fromisoformat(args.as_of), ["IBKR Flex"], True, cache_bust=1)
    dependencies = MobileServiceDependencies(
        load_options=lambda *_: None,
        fetch_price_history=lambda tickers, start, end: ({}, [], {"requested": len(tickers), "fetched": 0}),
        collect_dividend_cashflows=lambda *_: None,
        align_benchmarks_monthly=lambda tickers, idx: {},
        fetch_current_prices=None,
    )
    xml_report = load_local_flex_report(xml_dir=args.xml_dir, query_id=args.query_id, env_path=None)
    persisted_report = load_flex_report_from_env(env_path=None)
    xml_context = build_ibkr_mobile_payload_context(
        request,
        dependencies,
        xml_report,
        available_sheets=["IBKR Flex"],
        source_metadata={"pipeline_built_at": "fixed"},
    )
    persisted_context = build_ibkr_mobile_payload_context(
        request,
        dependencies,
        persisted_report,
        available_sheets=["IBKR Flex"],
        source_metadata={"pipeline_built_at": "fixed"},
    )

    builders = {
        "dashboard": build_mobile_dashboard_payload,
        "positions": build_mobile_positions_payload,
        "open_option_shorts": lambda context: build_mobile_open_option_shorts_payload(context),
        "tickers": lambda context: build_mobile_tickers_payload(context, include_history=True),
        "monthly": build_mobile_monthly_payload,
        "yearly": build_mobile_yearly_payload,
        "issues": build_mobile_issues_payload,
    }
    payload_results = {}
    for name, builder in builders.items():
        payload_results[name] = _scrub(builder(xml_context)) == _scrub(builder(persisted_context))

    result = {
        "as_of": args.as_of,
        "xml_section_counts": xml_report.section_counts,
        "persisted_section_counts": persisted_report.section_counts,
        "xml_state_counts": _state_counts(xml_context.state),
        "persisted_state_counts": _state_counts(persisted_context.state),
        "payload_matches": payload_results,
        "all_match": all(payload_results.values()),
    }
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result["all_match"] else 1


def _state_counts(state) -> dict[str, int]:
    return {
        "df_opts": len(state.df_opts),
        "open_options": len(state.open_options),
        "stock_txns": len(state.stock_txns),
        "realized_sales": len(state.realized_sales),
        "dividends": len(state.div_df),
        "issues": len(state.issues),
    }


def _scrub(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _scrub(item) for key, item in value.items() if key not in {"pipeline_built_at", "prices_updated_at"}}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
