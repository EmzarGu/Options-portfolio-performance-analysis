#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.flex_client import DateRange, FlexClient, load_env, parse_iso_date  # noqa: E402
from portfolio_backend.ibkr.importer import (  # noqa: E402
    FirestoreImportStore,
    GcsRawReportStore,
    IbkrImportService,
    LocalJsonImportStore,
    LocalRawReportStore,
)
from portfolio_backend.ibkr.repository import resolve_local_flex_xml_paths  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import one or more IBKR Flex Activity XML reports into local JSON docs or GCS/Firestore."
    )
    parser.add_argument("--env", default=str(REPO_ROOT / ".env"), help="Path to .env with IBKR settings")
    parser.add_argument("--query-id", default=None, help="IBKR Flex query ID. Defaults to IBKR_FLEX_QUERY_ID.")
    parser.add_argument("--xml-path", default=None, help="Import a single existing XML file")
    parser.add_argument("--xml-dir", default=None, help="Import every XML file in a directory")
    parser.add_argument("--from", dest="from_date", default=None, help="Fetch this date range start, YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=None, help="Fetch this date range end, YYYY-MM-DD")
    parser.add_argument("--polls", type=int, default=30)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument(
        "--store",
        choices=("local", "gcp"),
        default="local",
        help="local writes JSON docs under tmp; gcp writes raw XML to GCS and rows to Firestore.",
    )
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "tmp" / "ibkr_import"))
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize but do not write stores")
    args = parser.parse_args()

    load_env(args.env)
    query_id = args.query_id or _env_required("IBKR_FLEX_QUERY_ID")
    inputs = _load_inputs(args, query_id)

    if args.dry_run:
        from portfolio_backend.ibkr.flex_parser import parse_flex_xml

        print(
            json.dumps(
                [
                    {
                        "source": source,
                        "bytes": len(xml_bytes),
                        "metadata": parse_flex_xml(xml_bytes).metadata,
                        "section_counts": parse_flex_xml(xml_bytes).section_counts,
                    }
                    for source, xml_bytes in inputs
                ],
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.store == "gcp":
        service = IbkrImportService(GcsRawReportStore.from_env(), FirestoreImportStore())
    else:
        output_root = Path(args.output_dir).expanduser()
        service = IbkrImportService(
            LocalRawReportStore(output_root / "raw"),
            LocalJsonImportStore(output_root / "firestore_sim"),
        )

    results = []
    for source, xml_bytes in inputs:
        result = service.import_xml(xml_bytes, query_id=query_id)
        result_doc = result.as_dict()
        result_doc["source"] = source
        results.append(result_doc)

    print(json.dumps(results, indent=2, sort_keys=True, default=str))
    return 0


def _load_inputs(args, query_id: str) -> list[tuple[str, bytes]]:
    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            raise ValueError("--from and --to must be provided together")
        client = FlexClient.from_env(env_path=args.env)
        date_range = DateRange(parse_iso_date(args.from_date), parse_iso_date(args.to_date))
        fetched = client.fetch_statement(date_range, polls=args.polls, poll_interval=args.poll_interval)
        return [(f"ibkr-flex:{date_range.label}:reference-{fetched.reference_code}", fetched.report)]

    paths = resolve_local_flex_xml_paths(
        xml_path=args.xml_path,
        xml_dir=args.xml_dir,
        query_id=query_id,
        env_path=args.env,
    )
    return [(str(path), path.read_bytes()) for path in paths]


def _env_required(name: str) -> str:
    import os

    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
