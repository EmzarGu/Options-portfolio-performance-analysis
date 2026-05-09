from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from portfolio_backend.ibkr.flex_client import DateRange, FlexClient, parse_iso_date
from portfolio_backend.ibkr.importer import FirestoreImportStore, GcsRawReportStore, IbkrImportService


SOURCE = "ibkr_flex"
REPORT_TYPE = "activity"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None or not str(value).strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(value).strip()


def _project_id() -> Optional[str]:
    return os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")


def _date_range_from_args(args: argparse.Namespace) -> Optional[DateRange]:
    start_value = args.from_date or os.environ.get("IBKR_IMPORT_FROM")
    end_value = args.to_date or os.environ.get("IBKR_IMPORT_TO")
    last_days_value = args.last_days or os.environ.get("IBKR_IMPORT_LAST_DAYS")
    to_offset_value = args.to_offset_days
    if to_offset_value is None:
        to_offset_value = int(os.environ.get("IBKR_IMPORT_TO_OFFSET_DAYS", "1"))
    if start_value or end_value:
        if not start_value or not end_value:
            raise RuntimeError("Set both --from/IBKR_IMPORT_FROM and --to/IBKR_IMPORT_TO, or neither.")
        return DateRange(parse_iso_date(start_value), parse_iso_date(end_value))
    if not last_days_value:
        return None
    last_days = int(last_days_value)
    to_offset_days = int(to_offset_value)
    if last_days < 1:
        raise RuntimeError("--last-days/IBKR_IMPORT_LAST_DAYS must be at least 1.")
    if to_offset_days < 0:
        raise RuntimeError("--to-offset-days/IBKR_IMPORT_TO_OFFSET_DAYS must be greater than or equal to 0.")
    end = datetime.now(timezone.utc).date() - timedelta(days=to_offset_days)
    start = end - timedelta(days=last_days - 1)
    return DateRange(start, end)


def _run_id(query_id: str, started_at: datetime, date_range: Optional[DateRange]) -> str:
    suffix = "default"
    if date_range is not None:
        suffix = date_range.label
    return f"ibkr-flex-{query_id}-{started_at.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"


def run_import(args: argparse.Namespace) -> dict[str, Any]:
    from google.cloud import firestore, storage

    started_at = _utc_now()
    started_iso = _utc_iso(started_at)
    query_id = _env("IBKR_FLEX_QUERY_ID")
    bucket_name = _env("IBKR_RAW_BUCKET")
    date_range = _date_range_from_args(args)
    run_id = args.run_id or _run_id(query_id, started_at, date_range)

    db = firestore.Client(project=_project_id())
    storage_client = storage.Client(project=_project_id())
    import_service = IbkrImportService(
        GcsRawReportStore(bucket_name, client=storage_client),
        FirestoreImportStore(client=db),
    )

    try:
        client = FlexClient.from_env(env_path=None)
        fetched = client.fetch_statement(date_range, polls=args.polls, poll_interval=args.poll_interval)
        result = import_service.import_xml(fetched.report, query_id=query_id, run_id=run_id)
        finished_at = _utc_iso(_utc_now())
        db.collection("refresh_runs").document(run_id).set(
            {
                "run_id": run_id,
                "source": SOURCE,
                "status": "succeeded",
                "started_at": started_iso,
                "finished_at": finished_at,
                "ibkr_import_run_id": run_id,
                "raw_bucket": result.raw_report.bucket,
                "raw_object": result.raw_report.object_name,
                "section_counts": result.section_counts,
                "schema_version": 1,
            },
            merge=True,
        )
        return {
            "run_id": run_id,
            "status": "succeeded",
            "raw_object": result.raw_report.uri,
            "xml_bytes": result.raw_report.byte_count,
            "inserted_raw_rows": result.write_summary.inserted_raw_rows,
            "updated_raw_rows": result.write_summary.updated_raw_rows,
            "inserted_transactions": result.write_summary.inserted_transactions,
            "updated_transactions": result.write_summary.updated_transactions,
            "reference_code": fetched.reference_code,
            "polls": fetched.polls,
        }
    except Exception as exc:
        finished_at = _utc_iso(_utc_now())
        db.collection("refresh_runs").document(run_id).set(
            {
                "run_id": run_id,
                "source": SOURCE,
                "status": "failed",
                "started_at": started_iso,
                "finished_at": finished_at,
                "ibkr_import_run_id": run_id,
                "query_id": query_id,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:1000],
                "schema_version": 1,
            },
            merge=True,
        )
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch an IBKR Flex report and persist raw/import state.")
    parser.add_argument("--from", dest="from_date", help="Optional override start date, YYYY-MM-DD.")
    parser.add_argument("--to", dest="to_date", help="Optional override end date, YYYY-MM-DD.")
    parser.add_argument(
        "--last-days",
        type=int,
        default=None,
        help="Optional rolling inclusive window length ending at UTC today minus --to-offset-days.",
    )
    parser.add_argument(
        "--to-offset-days",
        type=int,
        default=None,
        help="Days before UTC today to use as rolling window end. Defaults to 1 for --last-days.",
    )
    parser.add_argument("--run-id", help="Optional deterministic import run id.")
    parser.add_argument("--polls", type=int, default=int(os.environ.get("IBKR_FLEX_POLLS", "30")))
    parser.add_argument("--poll-interval", type=float, default=float(os.environ.get("IBKR_FLEX_POLL_INTERVAL", "5")))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_import(args)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
