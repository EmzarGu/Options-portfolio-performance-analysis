from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Protocol

from portfolio_backend.ibkr.dedupe import canonical_row_hash, dedupe_key, raw_row_id
from portfolio_backend.ibkr.flex_client import DateRange
from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow, parse_flex_xml
from portfolio_backend.ibkr.normalization import IbkrNormalizedTransaction, normalize_transactions


RELEVANT_TRANSACTION_SECTIONS = ("Trade", "OptionEAE")


@dataclass(frozen=True)
class RawReportRef:
    uri: str
    bucket: Optional[str]
    object_name: Optional[str]
    local_path: Optional[str]
    sha256: str
    byte_count: int


@dataclass(frozen=True)
class ImportWriteSummary:
    inserted_raw_rows: int
    updated_raw_rows: int
    inserted_transactions: int
    updated_transactions: int


@dataclass(frozen=True)
class IbkrImportResult:
    run_id: str
    query_id: str
    status: str
    started_at: str
    finished_at: str
    raw_report: RawReportRef
    metadata: dict[str, Any]
    section_counts: dict[str, int]
    normalized_transactions: int
    write_summary: ImportWriteSummary

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        return data


class RawReportStore(Protocol):
    def put_xml(
        self,
        *,
        run_id: str,
        query_id: str,
        xml_bytes: bytes,
        metadata: dict[str, Any],
    ) -> RawReportRef:
        ...


class ImportRecordStore(Protocol):
    def begin_run(self, run_doc: dict[str, Any]) -> None:
        ...

    def upsert_raw_rows(self, *, run_id: str, query_id: str, report: IbkrFlexReport) -> tuple[int, int]:
        ...

    def upsert_transactions(
        self,
        *,
        run_id: str,
        query_id: str,
        transactions: Iterable[IbkrNormalizedTransaction],
    ) -> tuple[int, int]:
        ...

    def finish_run(self, run_id: str, updates: dict[str, Any]) -> None:
        ...

    def mark_latest_successful_run(self, *, query_id: str, run_doc: dict[str, Any]) -> None:
        ...

    def successful_import_ranges(self, *, query_id: str) -> list[DateRange]:
        ...


class LocalRawReportStore:
    def __init__(self, root_dir: str | Path = "tmp/ibkr_import/raw") -> None:
        self.root_dir = Path(root_dir)

    def put_xml(
        self,
        *,
        run_id: str,
        query_id: str,
        xml_bytes: bytes,
        metadata: dict[str, Any],
    ) -> RawReportRef:
        from_date = metadata.get("fromDate") or "unknown"
        to_date = metadata.get("toDate") or "unknown"
        target = self.root_dir / f"query-{query_id}" / f"run-{run_id}" / f"{from_date}-{to_date}.xml"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(xml_bytes)
        digest = _sha256(xml_bytes)
        return RawReportRef(
            uri=str(target),
            bucket=None,
            object_name=None,
            local_path=str(target),
            sha256=digest,
            byte_count=len(xml_bytes),
        )


class LocalJsonImportStore:
    def __init__(self, root_dir: str | Path = "tmp/ibkr_import/firestore_sim") -> None:
        self.root_dir = Path(root_dir)

    def begin_run(self, run_doc: dict[str, Any]) -> None:
        self._write_doc("ibkr_import_runs", run_doc["run_id"], run_doc)

    def upsert_raw_rows(self, *, run_id: str, query_id: str, report: IbkrFlexReport) -> tuple[int, int]:
        inserted = 0
        updated = 0
        for section, rows in report.rows_by_section.items():
            for row in rows:
                doc_id = raw_row_id(row.section, row.attrs)
                existed = self._doc_path("ibkr_raw_rows", doc_id).exists()
                self._write_doc("ibkr_raw_rows", doc_id, raw_row_document(run_id, query_id, row, report.metadata))
                inserted += int(not existed)
                updated += int(existed)
        return inserted, updated

    def upsert_transactions(
        self,
        *,
        run_id: str,
        query_id: str,
        transactions: Iterable[IbkrNormalizedTransaction],
    ) -> tuple[int, int]:
        inserted = 0
        updated = 0
        for txn in transactions:
            doc_id = txn.transaction_id
            existed = self._doc_path("ibkr_transactions", doc_id).exists()
            self._write_doc("ibkr_transactions", doc_id, transaction_document(run_id, query_id, txn))
            inserted += int(not existed)
            updated += int(existed)
        return inserted, updated

    def finish_run(self, run_id: str, updates: dict[str, Any]) -> None:
        path = self._doc_path("ibkr_import_runs", run_id)
        existing = _read_json(path) if path.exists() else {"run_id": run_id}
        self._write_doc("ibkr_import_runs", run_id, {**existing, **updates})

    def mark_latest_successful_run(self, *, query_id: str, run_doc: dict[str, Any]) -> None:
        doc_id = f"ibkr_latest_import_{query_id}"
        self._write_doc("app_metadata", doc_id, run_doc)
        self._write_doc("app_metadata", "ibkr_latest_import", run_doc)

    def successful_import_ranges(self, *, query_id: str) -> list[DateRange]:
        ranges: list[DateRange] = []
        for path in sorted((self.root_dir / "ibkr_import_runs").glob("*.json")):
            doc = _read_json(path)
            if str(doc.get("query_id")) != str(query_id):
                continue
            if str(doc.get("status")) != "succeeded":
                continue
            date_range = _date_range_from_doc(doc)
            if date_range is not None:
                ranges.append(date_range)
        return ranges

    def _doc_path(self, collection: str, doc_id: str) -> Path:
        return self.root_dir / collection / f"{doc_id}.json"

    def _write_doc(self, collection: str, doc_id: str, data: dict[str, Any]) -> None:
        path = self._doc_path(collection, doc_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str), encoding="utf-8")


class GcsRawReportStore:
    def __init__(
        self,
        bucket_name: str,
        *,
        prefix: str = "ibkr/flex/activity",
        client: Any = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")
        self.client = client or _storage_client()

    @classmethod
    def from_env(cls) -> "GcsRawReportStore":
        bucket = os.environ.get("IBKR_RAW_BUCKET", "").strip()
        if not bucket:
            raise ValueError("IBKR_RAW_BUCKET is required for GCS raw report storage")
        return cls(bucket)

    def put_xml(
        self,
        *,
        run_id: str,
        query_id: str,
        xml_bytes: bytes,
        metadata: dict[str, Any],
    ) -> RawReportRef:
        run_date = _today_utc()
        object_name = (
            f"{self.prefix}/query-{query_id}/run_date={run_date}/"
            f"run_id={run_id}/activity.xml"
        )
        digest = _sha256(xml_bytes)
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(object_name)
        blob.metadata = {
            "run_id": run_id,
            "query_id": query_id,
            "sha256": digest,
            "from_date": str(metadata.get("fromDate") or ""),
            "to_date": str(metadata.get("toDate") or ""),
        }
        blob.upload_from_string(xml_bytes, content_type="application/xml")
        return RawReportRef(
            uri=f"gs://{self.bucket_name}/{object_name}",
            bucket=self.bucket_name,
            object_name=object_name,
            local_path=None,
            sha256=digest,
            byte_count=len(xml_bytes),
        )


class FirestoreImportStore:
    def __init__(
        self,
        *,
        client: Any = None,
        import_runs_collection: str = "ibkr_import_runs",
        raw_rows_collection: str = "ibkr_raw_rows",
        transactions_collection: str = "ibkr_transactions",
    ) -> None:
        self.client = client or _firestore_client()
        self.import_runs_collection = import_runs_collection
        self.raw_rows_collection = raw_rows_collection
        self.transactions_collection = transactions_collection

    def begin_run(self, run_doc: dict[str, Any]) -> None:
        self.client.collection(self.import_runs_collection).document(run_doc["run_id"]).set(run_doc, merge=True)

    def upsert_raw_rows(self, *, run_id: str, query_id: str, report: IbkrFlexReport) -> tuple[int, int]:
        docs = [
            (raw_row_id(row.section, row.attrs), raw_row_document(run_id, query_id, row, report.metadata))
            for rows in report.rows_by_section.values()
            for row in rows
        ]
        return self._upsert_docs(self.raw_rows_collection, docs)

    def upsert_transactions(
        self,
        *,
        run_id: str,
        query_id: str,
        transactions: Iterable[IbkrNormalizedTransaction],
    ) -> tuple[int, int]:
        docs = [
            (txn.transaction_id, transaction_document(run_id, query_id, txn))
            for txn in transactions
        ]
        return self._upsert_docs(self.transactions_collection, docs)

    def finish_run(self, run_id: str, updates: dict[str, Any]) -> None:
        self.client.collection(self.import_runs_collection).document(run_id).set(updates, merge=True)

    def mark_latest_successful_run(self, *, query_id: str, run_doc: dict[str, Any]) -> None:
        metadata = self.client.collection("app_metadata")
        metadata.document(f"ibkr_latest_import_{query_id}").set(run_doc, merge=True)
        metadata.document("ibkr_latest_import").set(run_doc, merge=True)

    def successful_import_ranges(self, *, query_id: str) -> list[DateRange]:
        query = self.client.collection(self.import_runs_collection).where("query_id", "==", str(query_id))
        ranges: list[DateRange] = []
        for snap in query.stream():
            doc = snap.to_dict() or {}
            if str(doc.get("status")) != "succeeded":
                continue
            date_range = _date_range_from_doc(doc)
            if date_range is not None:
                ranges.append(date_range)
        return ranges

    def _upsert_docs(self, collection: str, docs: list[tuple[str, dict[str, Any]]]) -> tuple[int, int]:
        inserted = 0
        updated = 0
        for chunk in _chunks(docs, 400):
            refs = [self.client.collection(collection).document(doc_id) for doc_id, _ in chunk]
            existing_ids = {snap.id for snap in self.client.get_all(refs) if snap.exists}
            batch = self.client.batch()
            for doc_id, data in chunk:
                if doc_id in existing_ids:
                    updated += 1
                else:
                    inserted += 1
                batch.set(self.client.collection(collection).document(doc_id), data, merge=True)
            batch.commit()
        return inserted, updated


class IbkrImportService:
    def __init__(self, raw_store: RawReportStore, record_store: ImportRecordStore) -> None:
        self.raw_store = raw_store
        self.record_store = record_store

    def import_xml(
        self,
        xml_bytes: bytes,
        *,
        query_id: str,
        run_id: Optional[str] = None,
    ) -> IbkrImportResult:
        started_at = _now_iso()
        resolved_run_id = run_id or _run_id()
        report = parse_flex_xml(xml_bytes)
        raw_ref = self.raw_store.put_xml(
            run_id=resolved_run_id,
            query_id=query_id,
            xml_bytes=xml_bytes,
            metadata=report.metadata,
        )
        run_doc = {
            "run_id": resolved_run_id,
            "query_id": query_id,
            "source": "ibkr_flex",
            "report_type": "activity",
            "status": "running",
            "started_at": started_at,
            "from_date": report.metadata.get("fromDate"),
            "to_date": report.metadata.get("toDate"),
            "period": report.metadata.get("period"),
            "raw_bucket": raw_ref.bucket,
            "raw_object": raw_ref.object_name,
            "raw_uri": raw_ref.uri,
            "xml_bytes": raw_ref.byte_count,
            "xml_sha256": raw_ref.sha256,
            "section_counts": report.section_counts,
        }
        self.record_store.begin_run(run_doc)

        normalized = normalize_transactions(
            row
            for section in RELEVANT_TRANSACTION_SECTIONS
            for row in report.rows(section)
        )
        inserted_raw, updated_raw = self.record_store.upsert_raw_rows(
            run_id=resolved_run_id,
            query_id=query_id,
            report=report,
        )
        inserted_txn, updated_txn = self.record_store.upsert_transactions(
            run_id=resolved_run_id,
            query_id=query_id,
            transactions=normalized,
        )
        finished_at = _now_iso()
        summary = ImportWriteSummary(
            inserted_raw_rows=inserted_raw,
            updated_raw_rows=updated_raw,
            inserted_transactions=inserted_txn,
            updated_transactions=updated_txn,
        )
        finish_doc = {
            "status": "succeeded",
            "finished_at": finished_at,
            "inserted_raw_rows": inserted_raw,
            "updated_raw_rows": updated_raw,
            "inserted_transactions": inserted_txn,
            "updated_transactions": updated_txn,
            "skipped_duplicates": updated_raw + updated_txn,
        }
        self.record_store.finish_run(resolved_run_id, finish_doc)
        self.record_store.mark_latest_successful_run(
            query_id=query_id,
            run_doc={
                **run_doc,
                **finish_doc,
                "updated_at": finished_at,
                "schema_version": 1,
            },
        )
        return IbkrImportResult(
            run_id=resolved_run_id,
            query_id=query_id,
            status="succeeded",
            started_at=started_at,
            finished_at=finished_at,
            raw_report=raw_ref,
            metadata=report.metadata,
            section_counts=report.section_counts,
            normalized_transactions=len(normalized),
            write_summary=summary,
        )


def raw_row_document(
    run_id: str,
    query_id: str,
    row: IbkrRawRow,
    report_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    attrs = dict(row.attrs)
    metadata = report_metadata or {}
    return {
        "raw_row_id": raw_row_id(row.section, attrs),
        "run_id": run_id,
        "query_id": query_id,
        "report_from_date": metadata.get("fromDate"),
        "report_to_date": metadata.get("toDate"),
        "report_period": metadata.get("period"),
        "section": row.section,
        "source_row_hash": canonical_row_hash(row.section, attrs),
        "natural_key": dedupe_key(row.section, attrs),
        "account_id": attrs.get("accountId"),
        "report_date": attrs.get("reportDate"),
        "trade_date": attrs.get("tradeDate") or attrs.get("date"),
        "date_time": attrs.get("dateTime"),
        "conid": attrs.get("conid"),
        "symbol": attrs.get("symbol"),
        "underlying_symbol": attrs.get("underlyingSymbol"),
        "transaction_id": attrs.get("transactionID"),
        "trade_id": attrs.get("tradeID"),
        "ib_exec_id": attrs.get("ibExecID"),
        "raw": attrs,
        "updated_at": _now_iso(),
    }


def transaction_document(
    run_id: str,
    query_id: str,
    transaction: IbkrNormalizedTransaction,
) -> dict[str, Any]:
    data = transaction.as_dict()
    data.update(
        {
            "run_id": run_id,
            "query_id": query_id,
            "updated_at": _now_iso(),
        }
    )
    return data


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _date_range_from_doc(doc: dict[str, Any]) -> Optional[DateRange]:
    start = _date_from_ibkr_value(doc.get("from_date") or doc.get("report_from_date"))
    end = _date_from_ibkr_value(doc.get("to_date") or doc.get("report_to_date"))
    if start is None or end is None or end < start:
        return None
    return DateRange(start, end)


def _date_from_ibkr_value(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _chunks(items: list[Any], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _storage_client():
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise RuntimeError("Install google-cloud-storage to use GCS IBKR raw report storage.") from exc
    return storage.Client()


def _firestore_client():
    try:
        from portfolio_backend.gcp import firestore_client
    except ImportError as exc:
        raise RuntimeError("Install google-cloud-firestore to use Firestore IBKR import storage.") from exc
    return firestore_client()
