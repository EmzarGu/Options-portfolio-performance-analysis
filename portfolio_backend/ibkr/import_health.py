"""IBKR import marker parsing and unresolved import-health classification.

These rules are shared by web and mobile through the context runtime.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from portfolio_backend.market_calendar import previous_us_market_trading_day

logger = logging.getLogger("uvicorn.error")

DEFAULT_IBKR_IMPORT_STALE_DAYS = 3


def _ibkr_import_stale_days() -> int:
    value = os.getenv("IBKR_IMPORT_STALE_DAYS", str(DEFAULT_IBKR_IMPORT_STALE_DAYS)).strip()
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return DEFAULT_IBKR_IMPORT_STALE_DAYS


def _ibkr_import_marker_from_doc(
    doc: Dict[str, Any],
    *,
    fallback_id: str,
    query_id: str,
) -> Optional[Dict[str, Any]]:
    if str(doc.get("query_id") or query_id) != str(query_id):
        return None
    marker = {
        "import_run_id": str(doc.get("run_id") or doc.get("import_run_id") or fallback_id),
        "query_id": str(doc.get("query_id") or query_id),
        "status": str(doc.get("status") or "succeeded"),
        "finished_at": doc.get("finished_at"),
        "from_date": doc.get("from_date"),
        "to_date": doc.get("to_date"),
        "inserted_raw_rows": doc.get("inserted_raw_rows"),
        "updated_raw_rows": doc.get("updated_raw_rows"),
        "inserted_transactions": doc.get("inserted_transactions"),
        "updated_transactions": doc.get("updated_transactions"),
    }
    marker["source_snapshot_id"] = (
        f"ibkr-flex:{marker['query_id']}:{marker.get('finished_at') or marker['import_run_id']}"
    )
    return marker


def _with_ibkr_import_health(client, query_id: str, marker: Dict[str, Any]) -> Dict[str, Any]:
    marker = dict(marker)
    marker["import_health"] = _ibkr_import_health(client, query_id, marker)
    return marker


def _ibkr_import_health(client, query_id: str, latest_success_marker: Dict[str, Any]) -> Dict[str, Any]:
    """Detect unresolved IBKR import failures/deferred trailing statements.

    Pipeline data is loaded from successful raw-row imports. A trailing-day
    statement can fail/defer after the last successful import; without checking
    refresh_runs explicitly, the apps can incorrectly show data quality as OK
    while yesterday's assignment/expiration rows are still missing.
    """
    latest_success_finished = str(latest_success_marker.get("finished_at") or "")
    latest_success_to_date = _parse_iso_date(latest_success_marker.get("to_date"))
    successful_imports = _ibkr_successful_import_docs(client, query_id)
    stale_issue = _ibkr_stale_import_issue(latest_success_marker, today=date.today())
    unresolved_by_range: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    try:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            docs = client.collection("refresh_runs").where(filter=FieldFilter("source", "==", "ibkr_flex")).stream()
        except Exception:
            docs = client.collection("refresh_runs").where("source", "==", "ibkr_flex").stream()
        for snap in docs:
            doc = snap.to_dict() or {}
            status = str(doc.get("status") or "")
            if status not in {"failed", "deferred"}:
                continue
            doc_query_id = str(doc.get("query_id") or query_id)
            if doc_query_id != str(query_id):
                continue
            finished_at = str(doc.get("finished_at") or "")
            issue_from_date = _parse_iso_date(doc.get("from_date"))
            issue_to_date = _parse_iso_date(doc.get("to_date"))
            if _ibkr_import_issue_resolved_by_import_ranges(
                successful_imports,
                query_id=str(query_id),
                issue_finished_at=finished_at,
                issue_from_date=issue_from_date,
                issue_to_date=issue_to_date,
            ) or _ibkr_import_issue_resolved(
                latest_success_finished=latest_success_finished,
                latest_success_to_date=latest_success_to_date,
                issue_finished_at=finished_at,
                issue_to_date=issue_to_date,
            ):
                continue
            from_date = doc.get("from_date")
            to_date = doc.get("to_date")
            reason = str(doc.get("defer_reason") or doc.get("error_message") or status)
            label = str(to_date or from_date or "latest requested date")
            if from_date and to_date and from_date != to_date:
                label = f"{from_date} to {to_date}"
            if reason == "trailing_statement_unavailable":
                message = f"IBKR import deferred for {label}: statement was not available yet."
            else:
                message = f"IBKR import {status} for {label}: {reason}"
            issue = {
                "category": "import",
                "severity": "warning",
                "status": status,
                "from_date": from_date,
                "to_date": to_date,
                "finished_at": finished_at,
                "message": message,
                "action": "retry_import",
            }
            key = (str(from_date or ""), str(to_date or ""), _statement_unavailable_issue_key(reason) or reason)
            previous = unresolved_by_range.get(key)
            if previous is None or str(issue.get("finished_at") or "") > str(previous.get("finished_at") or ""):
                unresolved_by_range[key] = issue
    except Exception as exc:
        logger.warning("ibkr_import_health_check_failed error=%s", exc)
    issues: List[Dict[str, Any]] = list(unresolved_by_range.values())
    if stale_issue is not None and not issues:
        issues.append(stale_issue)
    issues.sort(key=lambda item: str(item.get("finished_at") or ""), reverse=True)
    return {
        "status": "warning" if issues else "ok",
        "issues": issues[:5],
        "unresolved_count": len(issues),
        "latest_success_finished_at": latest_success_marker.get("finished_at"),
        "latest_success_to_date": latest_success_marker.get("to_date"),
    }


def _ibkr_successful_import_docs(client, query_id: str) -> List[Dict[str, Any]]:
    try:
        try:
            from google.cloud.firestore_v1 import FieldFilter

            docs = (
                client.collection("ibkr_import_runs")
                .where(filter=FieldFilter("query_id", "==", str(query_id)))
                .stream()
            )
        except Exception:
            docs = client.collection("ibkr_import_runs").where("query_id", "==", str(query_id)).stream()
        successful: List[Dict[str, Any]] = []
        for snap in docs:
            doc = snap.to_dict() or {}
            if str(doc.get("status")) == "succeeded" and str(doc.get("query_id") or query_id) == str(query_id):
                successful.append(dict(doc))
        return successful
    except Exception as exc:
        logger.warning("ibkr_successful_import_lookup_failed error=%s", exc)
        return []


def _statement_unavailable_issue_key(reason: str) -> Optional[str]:
    lowered = str(reason or "").lower()
    if reason == "trailing_statement_unavailable":
        return "statement_unavailable"
    if "statement is incomplete" in lowered or "statement is not available" in lowered:
        return "statement_unavailable"
    return None


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    compact = text[:8]
    if len(compact) == 8 and compact.isdigit():
        try:
            return datetime.strptime(compact, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _ibkr_stale_import_issue(latest_success_marker: Dict[str, Any], *, today: date) -> Optional[Dict[str, Any]]:
    stale_days = _ibkr_import_stale_days()
    if stale_days <= 0:
        return None
    to_date = _parse_iso_date(latest_success_marker.get("to_date"))
    if to_date is None:
        return {
            "category": "import",
            "severity": "warning",
            "status": "stale",
            "from_date": latest_success_marker.get("from_date"),
            "to_date": latest_success_marker.get("to_date"),
            "finished_at": latest_success_marker.get("finished_at"),
            "message": "IBKR import freshness cannot be verified: latest successful import has no valid statement end date.",
            "action": "retry_import",
        }
    latest_expected = previous_us_market_trading_day(today - timedelta(days=stale_days))
    if to_date >= latest_expected:
        return None
    return {
        "category": "import",
        "severity": "warning",
        "status": "stale",
        "from_date": latest_success_marker.get("from_date"),
        "to_date": latest_success_marker.get("to_date"),
        "finished_at": latest_success_marker.get("finished_at"),
        "message": (
            f"IBKR import stale: latest successful statement ends {to_date.isoformat()}, "
            f"behind the latest expected trading day {latest_expected.isoformat()}."
        ),
        "action": "retry_import",
    }


def _ibkr_import_issue_resolved(
    *,
    latest_success_finished: str,
    latest_success_to_date: Optional[date],
    issue_finished_at: str,
    issue_to_date: Optional[date],
) -> bool:
    if latest_success_finished and issue_finished_at and latest_success_finished > issue_finished_at:
        if issue_to_date is None or latest_success_to_date is None:
            return True
        return latest_success_to_date >= issue_to_date
    return False


def _ibkr_import_issue_resolved_by_import_ranges(
    successful_imports: List[Dict[str, Any]],
    *,
    query_id: str,
    issue_finished_at: str,
    issue_from_date: Optional[date],
    issue_to_date: Optional[date],
) -> bool:
    if not issue_finished_at:
        return False
    issue_start = issue_from_date or issue_to_date
    issue_end = issue_to_date or issue_from_date
    if issue_start is None or issue_end is None:
        return False
    if issue_start > issue_end:
        issue_start, issue_end = issue_end, issue_start

    intervals: List[Tuple[date, date]] = []
    for doc in successful_imports:
        if str(doc.get("status")) != "succeeded":
            continue
        if str(doc.get("query_id") or query_id) != str(query_id):
            continue
        finished_at = str(doc.get("finished_at") or "")
        if not finished_at:
            continue
        run_start = _parse_iso_date(doc.get("from_date")) or _parse_iso_date(doc.get("to_date"))
        run_end = _parse_iso_date(doc.get("to_date")) or run_start
        if run_start is None or run_end is None:
            continue
        if run_start > run_end:
            run_start, run_end = run_end, run_start
        if run_end < issue_start or run_start > issue_end:
            continue
        intervals.append((run_start, run_end))

    if not intervals:
        return False
    intervals.sort(key=lambda pair: pair[0])
    covered_until: Optional[date] = None
    for start, end in intervals:
        if covered_until is None:
            if start > issue_start:
                return False
            covered_until = end
        elif start <= covered_until + timedelta(days=1):
            if end > covered_until:
                covered_until = end
        else:
            return False
        if covered_until >= issue_end:
            return True
    return False
