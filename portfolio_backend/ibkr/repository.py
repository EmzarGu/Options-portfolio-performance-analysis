from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional

from portfolio_backend.ibkr.dedupe import dedupe_key
from portfolio_backend.ibkr.flex_client import load_env
from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow, parse_flex_xml_file
from portfolio_backend.ibkr.persisted_report import (
    FirestoreFlexReportRepository,
    FirestoreRestFlexReportRepository,
    LocalJsonFlexReportRepository,
)


def combine_flex_reports(paths: Iterable[str | Path]) -> IbkrFlexReport:
    rows_by_section: dict[str, list[IbkrRawRow]] = {}
    seen: set[tuple[str, str]] = set()
    from_dates: list[str] = []
    to_dates: list[str] = []
    source_paths = [Path(path).expanduser() for path in paths]
    if not source_paths:
        raise FileNotFoundError("No IBKR Flex XML reports were provided.")

    for path in source_paths:
        report = parse_flex_xml_file(path)
        if report.metadata.get("fromDate"):
            from_dates.append(report.metadata["fromDate"])
        if report.metadata.get("toDate"):
            to_dates.append(report.metadata["toDate"])
        for section, rows in report.rows_by_section.items():
            for row in rows:
                key = (section, dedupe_key(row.section, row.attrs))
                if key in seen:
                    continue
                seen.add(key)
                rows_by_section.setdefault(section, []).append(row)

    metadata = {"sourceFiles": str(len(source_paths))}
    if from_dates:
        metadata["fromDate"] = min(from_dates)
    if to_dates:
        metadata["toDate"] = max(to_dates)
    section_counts = {section: len(rows) for section, rows in rows_by_section.items()}
    return IbkrFlexReport(
        root_tag="CombinedFlexReports",
        metadata=metadata,
        rows_by_section=rows_by_section,
        section_counts=section_counts,
    )


def resolve_local_flex_xml_paths(
    *,
    xml_path: Optional[str] = None,
    xml_dir: Optional[str] = None,
    query_id: Optional[str] = None,
    env_path: Optional[str | Path] = None,
) -> list[Path]:
    load_env(env_path)
    explicit_path = xml_path or os.environ.get("IBKR_FLEX_XML_PATH")
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"IBKR_FLEX_XML_PATH does not exist: {path}")
        return [path]

    explicit_dir = xml_dir or os.environ.get("IBKR_FLEX_REPORT_DIR")
    if explicit_dir:
        paths = sorted(Path(explicit_dir).expanduser().glob("*.xml"))
        if not paths:
            raise FileNotFoundError(f"No IBKR Flex XML files found in {explicit_dir}")
        return paths

    resolved_query_id = query_id or os.environ.get("IBKR_FLEX_QUERY_ID")
    if resolved_query_id:
        default_dir = Path("tmp") / "ibkr_backfill" / f"query-{resolved_query_id}"
        paths = sorted(default_dir.glob("*.xml"))
        if paths:
            return paths

    raise FileNotFoundError(
        "No IBKR Flex XML reports found. Set IBKR_FLEX_XML_PATH, IBKR_FLEX_REPORT_DIR, "
        "or IBKR_FLEX_QUERY_ID with reports under tmp/ibkr_backfill/query-{id}."
    )


def load_local_flex_report(
    *,
    xml_path: Optional[str] = None,
    xml_dir: Optional[str] = None,
    query_id: Optional[str] = None,
    env_path: Optional[str | Path] = ".env",
) -> IbkrFlexReport:
    return combine_flex_reports(
        resolve_local_flex_xml_paths(
            xml_path=xml_path,
            xml_dir=xml_dir,
            query_id=query_id,
            env_path=env_path,
        )
    )


def load_flex_report_from_env(*, env_path: Optional[str | Path] = ".env") -> IbkrFlexReport:
    load_env(env_path)
    default_source = "firestore" if os.environ.get("OPTIONS_DATA_SOURCE", "").strip().lower() in {"ibkr", "ibkr_flex"} else "local_xml"
    source = _clean_env_token(os.environ.get("IBKR_REPORT_SOURCE", default_source)).lower()
    query_id = os.environ.get("IBKR_FLEX_QUERY_ID")
    if source in {"local_json", "json", "firestore_sim"}:
        root_dir = os.environ.get("IBKR_IMPORT_JSON_DIR", "tmp/ibkr_import/firestore_sim")
        return LocalJsonFlexReportRepository(root_dir).load_report(query_id=query_id)
    if source in {"firestore", "gcp"}:
        return FirestoreFlexReportRepository().load_report(query_id=query_id)
    if source in {"firestore_rest", "firestore-rest", "rest"}:
        return FirestoreRestFlexReportRepository().load_report(query_id=query_id)
    return load_local_flex_report(query_id=query_id, env_path=env_path)


def _clean_env_token(value: object) -> str:
    text = str(value or "").strip()
    for quote in ("'", '"'):
        if text.startswith(quote) and text.endswith(quote) and len(text) >= 2:
            text = text[1:-1].strip()
    return text
