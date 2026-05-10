from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow


def report_from_raw_row_documents(
    docs: Iterable[dict[str, Any]],
    *,
    metadata: Optional[dict[str, str]] = None,
) -> IbkrFlexReport:
    rows_by_section: dict[str, list[IbkrRawRow]] = {}
    source_runs: set[str] = set()
    query_ids: set[str] = set()
    report_from_dates: list[str] = []
    report_to_dates: list[str] = []
    row_dates: list[str] = []

    for doc in docs:
        section = str(doc.get("section") or "")
        raw = doc.get("raw")
        if not section or not isinstance(raw, dict):
            continue
        rows_by_section.setdefault(section, []).append(IbkrRawRow(section=section, attrs={str(k): str(v) for k, v in raw.items()}))
        if doc.get("run_id"):
            source_runs.add(str(doc["run_id"]))
        if doc.get("query_id"):
            query_ids.add(str(doc["query_id"]))
        if doc.get("report_from_date"):
            report_from_dates.append(str(doc["report_from_date"]))
        if doc.get("report_to_date"):
            report_to_dates.append(str(doc["report_to_date"]))
        trade_date = raw.get("tradeDate") or raw.get("date") or raw.get("reportDate")
        if isinstance(trade_date, str) and len(trade_date) == 8 and trade_date.isdigit():
            row_dates.append(trade_date)

    resolved_metadata = dict(metadata or {})
    from_dates = report_from_dates or row_dates
    to_dates = report_to_dates or row_dates
    if from_dates:
        resolved_metadata.setdefault("fromDate", min(from_dates))
    if to_dates:
        resolved_metadata.setdefault("toDate", max(to_dates))
    if source_runs:
        resolved_metadata.setdefault("sourceRuns", str(len(source_runs)))
    if len(query_ids) == 1:
        resolved_metadata.setdefault("queryId", next(iter(query_ids)))

    return IbkrFlexReport(
        root_tag="PersistedFlexRows",
        metadata=resolved_metadata,
        rows_by_section=rows_by_section,
        section_counts={section: len(rows) for section, rows in rows_by_section.items()},
    )


class LocalJsonFlexReportRepository:
    def __init__(self, root_dir: str | Path = "tmp/ibkr_import/firestore_sim") -> None:
        self.root_dir = Path(root_dir)

    def load_report(
        self,
        *,
        query_id: Optional[str] = None,
        sections: Optional[Iterable[str]] = None,
    ) -> IbkrFlexReport:
        wanted_sections = {str(section) for section in sections} if sections is not None else None
        docs = []
        for path in sorted((self.root_dir / "ibkr_raw_rows").glob("*.json")):
            doc = json.loads(path.read_text(encoding="utf-8"))
            if query_id is not None and str(doc.get("query_id")) != str(query_id):
                continue
            if wanted_sections is not None and str(doc.get("section")) not in wanted_sections:
                continue
            docs.append(doc)
        if not docs:
            raise FileNotFoundError(_empty_report_message(query_id=query_id, sections=wanted_sections))
        return report_from_raw_row_documents(docs)


class FirestoreFlexReportRepository:
    def __init__(
        self,
        *,
        client: Any = None,
        raw_rows_collection: str = "ibkr_raw_rows",
    ) -> None:
        self.client = client or _firestore_client()
        self.raw_rows_collection = raw_rows_collection

    def load_report(
        self,
        *,
        query_id: Optional[str] = None,
        sections: Optional[Iterable[str]] = None,
    ) -> IbkrFlexReport:
        wanted_sections = {str(section) for section in sections} if sections is not None else None
        query = self.client.collection(self.raw_rows_collection)
        if query_id is not None:
            try:
                from google.cloud.firestore_v1 import FieldFilter

                query = query.where(filter=FieldFilter("query_id", "==", str(query_id)))
            except Exception:
                query = query.where("query_id", "==", str(query_id))
        docs = []
        for snap in query.stream():
            doc = snap.to_dict() or {}
            if wanted_sections is not None and str(doc.get("section")) not in wanted_sections:
                continue
            docs.append(doc)
        if not docs:
            raise FileNotFoundError(_empty_report_message(query_id=query_id, sections=wanted_sections))
        return report_from_raw_row_documents(docs)


class FirestoreRestFlexReportRepository:
    def __init__(
        self,
        *,
        project: Optional[str] = None,
        database: str = "(default)",
        raw_rows_collection: str = "ibkr_raw_rows",
        session: Any = None,
        timeout: int = 60,
    ) -> None:
        self.project = project
        self.database = database
        self.raw_rows_collection = raw_rows_collection
        self.session = session or _firestore_rest_session()
        self.timeout = timeout

    def load_report(
        self,
        *,
        query_id: Optional[str] = None,
        sections: Optional[Iterable[str]] = None,
    ) -> IbkrFlexReport:
        wanted_sections = {str(section) for section in sections} if sections is not None else None
        project = self.project or _firestore_project()
        url = (
            f"https://firestore.googleapis.com/v1/projects/{quote(project)}/"
            f"databases/{quote(self.database, safe='')}/documents:runQuery"
        )
        structured_query: dict[str, Any] = {
            "from": [{"collectionId": self.raw_rows_collection}],
        }
        if query_id is not None:
            structured_query["where"] = {
                "fieldFilter": {
                    "field": {"fieldPath": "query_id"},
                    "op": "EQUAL",
                    "value": {"stringValue": str(query_id)},
                }
            }
        response = self.session.post(
            url,
            json={"structuredQuery": structured_query},
            timeout=self.timeout,
        )
        response.raise_for_status()
        docs = []
        for item in response.json():
            fields = item.get("document", {}).get("fields")
            if not fields:
                continue
            doc = _decode_firestore_fields(fields)
            if wanted_sections is not None and str(doc.get("section")) not in wanted_sections:
                continue
            docs.append(doc)
        if not docs:
            raise FileNotFoundError(_empty_report_message(query_id=query_id, sections=wanted_sections))
        return report_from_raw_row_documents(docs)


def _firestore_client():
    try:
        from portfolio_backend.gcp import firestore_client
    except ImportError as exc:
        raise RuntimeError("Install google-cloud-firestore to load IBKR reports from Firestore.") from exc
    return firestore_client()


def _firestore_rest_session():
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        from portfolio_backend.gcp import service_account_credentials_from_config
    except ImportError as exc:
        raise RuntimeError("Install google-auth to load IBKR reports from Firestore REST.") from exc

    credentials, _ = service_account_credentials_from_config()
    if credentials is None:
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def _firestore_project() -> str:
    try:
        from portfolio_backend.gcp import service_account_credentials_from_config

        _, credential_project = service_account_credentials_from_config()
    except Exception:
        credential_project = None
    project = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or credential_project
    if not project:
        raise RuntimeError("FIRESTORE_PROJECT_ID is required for Firestore REST IBKR report loading.")
    return str(project)


def _decode_firestore_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return {key: _decode_firestore_value(value) for key, value in fields.items()}


def _decode_firestore_value(value: dict[str, Any]) -> Any:
    if "stringValue" in value:
        return value["stringValue"]
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "booleanValue" in value:
        return bool(value["booleanValue"])
    if "nullValue" in value:
        return None
    if "timestampValue" in value:
        return value["timestampValue"]
    if "mapValue" in value:
        return _decode_firestore_fields(value.get("mapValue", {}).get("fields", {}))
    if "arrayValue" in value:
        return [_decode_firestore_value(item) for item in value.get("arrayValue", {}).get("values", [])]
    return None


def _empty_report_message(*, query_id: Optional[str], sections: Optional[set[str]]) -> str:
    details = []
    if query_id:
        details.append(f"query_id={query_id}")
    if sections:
        details.append("sections=" + ",".join(sorted(sections)))
    suffix = " (" + "; ".join(details) + ")" if details else ""
    return f"No persisted IBKR raw rows were found{suffix}."
