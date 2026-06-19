from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from portfolio_backend.ibkr.flex_client import DateRange, FlexClient, parse_iso_date, plan_backfill_ranges
from portfolio_backend.ibkr.importer import FirestoreImportStore, GcsRawReportStore, IbkrImportService


SOURCE = "ibkr_flex"
REPORT_TYPE = "activity"
DEFAULT_RECENT_OVERLAP_DAYS = 14


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


def _auto_target_range_from_args(args: argparse.Namespace) -> Optional[DateRange]:
    start_value = args.inception_date or os.environ.get("IBKR_IMPORT_INCEPTION_DATE")
    if not start_value:
        return None
    to_offset_value = args.to_offset_days
    if to_offset_value is None:
        to_offset_value = int(os.environ.get("IBKR_IMPORT_TO_OFFSET_DAYS", "1"))
    to_offset_days = int(to_offset_value)
    if to_offset_days < 0:
        raise RuntimeError("--to-offset-days/IBKR_IMPORT_TO_OFFSET_DAYS must be greater than or equal to 0.")
    start = parse_iso_date(start_value)
    end = datetime.now(timezone.utc).date() - timedelta(days=to_offset_days)
    if end < start:
        raise RuntimeError("IBKR auto import target end is before IBKR_IMPORT_INCEPTION_DATE.")
    return DateRange(start, end)


def _recent_overlap_days_from_args(args: argparse.Namespace) -> int:
    value = args.recent_overlap_days
    if value is None:
        value = int(os.environ.get("IBKR_IMPORT_RECENT_OVERLAP_DAYS", str(DEFAULT_RECENT_OVERLAP_DAYS)))
    overlap_days = int(value)
    if overlap_days < 0:
        raise RuntimeError("--recent-overlap-days/IBKR_IMPORT_RECENT_OVERLAP_DAYS must be greater than or equal to 0.")
    return overlap_days


def plan_missing_import_ranges(
    *,
    target: DateRange,
    existing: Iterable[DateRange],
    recent_overlap_days: int = DEFAULT_RECENT_OVERLAP_DAYS,
    max_days: int = 365,
) -> list[DateRange]:
    covered = _merge_ranges(_clip_ranges(existing, target))
    missing = _missing_ranges(target, covered)
    planned: list[DateRange] = []
    for date_range in missing:
        business_range = _trim_weekend_edges(date_range)
        if business_range is not None:
            planned.extend(plan_backfill_ranges(business_range.start, business_range.end, max_days=max_days))
    if recent_overlap_days > 0:
        overlap_start = max(target.start, target.end - timedelta(days=recent_overlap_days - 1))
        recent_target = DateRange(overlap_start, target.end)
        for date_range in _clip_ranges(covered, recent_target):
            planned.extend(plan_backfill_ranges(date_range.start, date_range.end, max_days=max_days))
    return _dedupe_ranges(planned)


def _trim_weekend_edges(date_range: DateRange) -> DateRange | None:
    """Avoid standalone weekend gaps for Activity Flex statements.

    IBKR Activity Flex statements are business-day based. Once coverage reaches
    a Friday, the following Saturday/Sunday should not be treated as missing
    import coverage; otherwise the daily job repeatedly requests unavailable
    weekend-only statements.
    """
    start = date_range.start
    end = date_range.end
    while start <= end and start.weekday() >= 5:
        start += timedelta(days=1)
    while end >= start and end.weekday() >= 5:
        end -= timedelta(days=1)
    if start > end:
        return None
    return DateRange(start, end)


def split_trailing_target_day(ranges: Iterable[DateRange], target_end: date) -> list[DateRange]:
    """Split the target end date into a one-day chunk.

    IBKR often has all prior data available while the latest requested calendar
    day is still unavailable. Keeping the trailing day isolated lets auto mode
    import everything else and defer only that day.
    """
    split: list[DateRange] = []
    for date_range in ranges:
        if date_range.start < target_end <= date_range.end:
            split.append(DateRange(date_range.start, target_end - timedelta(days=1)))
            split.append(DateRange(target_end, target_end))
        else:
            split.append(date_range)
    return _dedupe_ranges(split)


def _clip_ranges(ranges: Iterable[DateRange], target: DateRange) -> list[DateRange]:
    clipped = []
    for date_range in ranges:
        start = max(date_range.start, target.start)
        end = min(date_range.end, target.end)
        if start <= end:
            clipped.append(DateRange(start, end))
    return clipped


def _merge_ranges(ranges: Iterable[DateRange]) -> list[DateRange]:
    ordered = sorted(ranges, key=lambda item: (item.start, item.end))
    merged: list[DateRange] = []
    for date_range in ordered:
        if not merged:
            merged.append(date_range)
            continue
        previous = merged[-1]
        if date_range.start <= previous.end + timedelta(days=1):
            merged[-1] = DateRange(previous.start, max(previous.end, date_range.end))
        else:
            merged.append(date_range)
    return merged


def _missing_ranges(target: DateRange, covered: Iterable[DateRange]) -> list[DateRange]:
    missing: list[DateRange] = []
    cursor = target.start
    for date_range in covered:
        if cursor < date_range.start:
            missing.append(DateRange(cursor, date_range.start - timedelta(days=1)))
        cursor = max(cursor, date_range.end + timedelta(days=1))
    if cursor <= target.end:
        missing.append(DateRange(cursor, target.end))
    return missing


def _dedupe_ranges(ranges: Iterable[DateRange]) -> list[DateRange]:
    seen: set[tuple[date, date]] = set()
    deduped: list[DateRange] = []
    for date_range in sorted(ranges, key=lambda item: (item.start, item.end)):
        key = (date_range.start, date_range.end)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(date_range)
    return deduped


def _run_id(query_id: str, started_at: datetime, date_range: Optional[DateRange]) -> str:
    suffix = "default"
    if date_range is not None:
        suffix = date_range.label
    return f"ibkr-flex-{query_id}-{started_at.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"


def _import_range(
    *,
    args: argparse.Namespace,
    query_id: str,
    date_range: Optional[DateRange],
    db,
    import_service: IbkrImportService,
    client: FlexClient,
    run_id: Optional[str] = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    started_iso = _utc_iso(started_at)
    resolved_run_id = run_id or args.run_id or _run_id(query_id, started_at, date_range)

    try:
        fetched = client.fetch_statement(date_range, polls=args.polls, poll_interval=args.poll_interval)
        result = import_service.import_xml(fetched.report, query_id=query_id, run_id=resolved_run_id)
        finished_at = _utc_iso(_utc_now())
        db.collection("refresh_runs").document(resolved_run_id).set(
            {
                "run_id": resolved_run_id,
                "source": SOURCE,
                "status": "succeeded",
                "started_at": started_iso,
                "finished_at": finished_at,
                "ibkr_import_run_id": resolved_run_id,
                "raw_bucket": result.raw_report.bucket,
                "raw_object": result.raw_report.object_name,
                "section_counts": result.section_counts,
                "schema_version": 1,
            },
            merge=True,
        )
        return {
            "run_id": resolved_run_id,
            "status": "succeeded",
            "from_date": date_range.start.isoformat() if date_range is not None else None,
            "to_date": date_range.end.isoformat() if date_range is not None else None,
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
        db.collection("refresh_runs").document(resolved_run_id).set(
            {
                "run_id": resolved_run_id,
                "source": SOURCE,
                "status": "failed",
                "started_at": started_iso,
                "finished_at": finished_at,
                "ibkr_import_run_id": resolved_run_id,
                "query_id": query_id,
                "from_date": date_range.start.isoformat() if date_range is not None else None,
                "to_date": date_range.end.isoformat() if date_range is not None else None,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:1000],
                "schema_version": 1,
            },
            merge=True,
        )
        raise


def _is_statement_unavailable_error(error: str) -> bool:
    lowered = str(error or "").lower()
    return (
        ("1003" in lowered and "statement is not available" in lowered)
        or ("1004" in lowered and "statement is incomplete" in lowered)
    )


def _is_deferable_trailing_unavailable(exc: Exception, date_range: DateRange, auto_target: DateRange) -> bool:
    return (
        date_range.start == date_range.end == auto_target.end
        and _is_statement_unavailable_error(str(exc))
    )


def _mark_deferred_refresh_run(
    *,
    db,
    run_id: str,
    query_id: str,
    date_range: DateRange,
    exc: Exception,
) -> None:
    db.collection("refresh_runs").document(run_id).set(
        {
            "run_id": run_id,
            "source": SOURCE,
            "status": "deferred",
            "finished_at": _utc_iso(_utc_now()),
            "ibkr_import_run_id": run_id,
            "query_id": query_id,
            "from_date": date_range.start.isoformat(),
            "to_date": date_range.end.isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:1000],
            "defer_reason": "trailing_statement_unavailable",
            "schema_version": 1,
        },
        merge=True,
    )


def run_import(args: argparse.Namespace) -> dict[str, Any]:
    from google.cloud import firestore, storage

    query_id = _env("IBKR_FLEX_QUERY_ID")
    bucket_name = _env("IBKR_RAW_BUCKET")
    db = firestore.Client(project=_project_id())
    storage_client = storage.Client(project=_project_id())
    record_store = FirestoreImportStore(client=db)
    import_service = IbkrImportService(
        GcsRawReportStore(bucket_name, client=storage_client),
        record_store,
    )
    client = FlexClient.from_env(env_path=None)
    explicit_range = _date_range_from_args(args)
    auto_target = None if explicit_range is not None else _auto_target_range_from_args(args)
    if auto_target is None:
        return _import_range(
            args=args,
            query_id=query_id,
            date_range=explicit_range,
            db=db,
            import_service=import_service,
            client=client,
        )

    existing = record_store.successful_import_ranges(query_id=query_id)
    planned = plan_missing_import_ranges(
        target=auto_target,
        existing=existing,
        recent_overlap_days=_recent_overlap_days_from_args(args),
    )
    planned = split_trailing_target_day(planned, auto_target.end)
    _emit_progress(
        {
            "event": "auto_plan",
            "query_id": query_id,
            "target_from": auto_target.start.isoformat(),
            "target_to": auto_target.end.isoformat(),
            "existing_ranges": [_range_dict(item) for item in _merge_ranges(_clip_ranges(existing, auto_target))],
            "planned_ranges": [_range_dict(item) for item in planned],
        }
    )
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    batch_started_at = _utc_now()
    for index, date_range in enumerate(planned, start=1):
        chunk_run_id = _run_id(query_id, batch_started_at, date_range)
        _emit_progress(
            {
                "event": "chunk_started",
                "index": index,
                "total": len(planned),
                "run_id": chunk_run_id,
                **_range_dict(date_range),
            }
        )
        try:
            result = _import_range(
                args=args,
                query_id=query_id,
                date_range=date_range,
                db=db,
                import_service=import_service,
                client=client,
                run_id=chunk_run_id,
            )
        except Exception as exc:
            if _is_deferable_trailing_unavailable(exc, date_range, auto_target):
                _mark_deferred_refresh_run(
                    db=db,
                    run_id=chunk_run_id,
                    query_id=query_id,
                    date_range=date_range,
                    exc=exc,
                )
                deferred_chunk = {
                    "event": "chunk_deferred",
                    "index": index,
                    "total": len(planned),
                    "run_id": chunk_run_id,
                    **_range_dict(date_range),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                    "defer_reason": "trailing_statement_unavailable",
                }
                deferred.append(deferred_chunk)
                _emit_progress(deferred_chunk)
                continue
            failure = {
                "event": "chunk_failed",
                "index": index,
                "total": len(planned),
                "run_id": chunk_run_id,
                **_range_dict(date_range),
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:1000],
            }
            failures.append(failure)
            _emit_progress(failure)
            continue
        results.append(result)
        _emit_progress({"event": "chunk_succeeded", "index": index, "total": len(planned), **result})

    summary = _auto_summary(auto_target=auto_target, planned=planned, results=results, failures=failures, deferred=deferred)
    _emit_progress({"event": "auto_summary", **summary})
    if failures:
        raise RuntimeError(f"IBKR auto import failed for {len(failures)} of {len(planned)} chunks")
    return summary


def _range_dict(date_range: DateRange) -> dict[str, Any]:
    return {
        "from_date": date_range.start.isoformat(),
        "to_date": date_range.end.isoformat(),
        "days": date_range.days_inclusive,
    }


def _emit_progress(event: dict[str, Any]) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def _auto_summary(
    *,
    auto_target: DateRange,
    planned: list[DateRange],
    results: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    deferred: list[dict[str, Any]],
) -> dict[str, Any]:
    status = "failed" if failures else "succeeded_with_deferred" if deferred else "succeeded"
    return {
        "status": status,
        "mode": "auto",
        "target_from": auto_target.start.isoformat(),
        "target_to": auto_target.end.isoformat(),
        "planned_chunks": len(planned),
        "succeeded_chunks": len(results),
        "failed_chunks": len(failures),
        "deferred_chunks": len(deferred),
        "inserted_raw_rows": sum(int(item.get("inserted_raw_rows") or 0) for item in results),
        "updated_raw_rows": sum(int(item.get("updated_raw_rows") or 0) for item in results),
        "inserted_transactions": sum(int(item.get("inserted_transactions") or 0) for item in results),
        "updated_transactions": sum(int(item.get("updated_transactions") or 0) for item in results),
        "ranges": [_range_dict(item) for item in planned],
        "failures": failures,
        "deferred": deferred,
    }


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
    parser.add_argument(
        "--inception-date",
        help=(
            "Earliest date to guarantee in Firestore coverage, YYYY-MM-DD. "
            "When set and no explicit --from/--to range is provided, the job auto-imports missing coverage."
        ),
    )
    parser.add_argument(
        "--recent-overlap-days",
        type=int,
        default=None,
        help="Recent days to re-import during auto mode to catch late IBKR corrections. Defaults to 14.",
    )
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
