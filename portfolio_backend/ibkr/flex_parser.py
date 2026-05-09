from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional


RELEVANT_SECTIONS = {
    "Trade",
    "OptionEAE",
    "CashTransaction",
    "OpenPosition",
    "SecurityInfo",
}


@dataclass(frozen=True)
class IbkrRawRow:
    section: str
    attrs: Dict[str, str]


@dataclass(frozen=True)
class IbkrFlexReport:
    root_tag: str
    metadata: Dict[str, str]
    rows_by_section: Dict[str, List[IbkrRawRow]]
    section_counts: Dict[str, int]

    def rows(self, section: str) -> List[IbkrRawRow]:
        return list(self.rows_by_section.get(section, []))


def strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_root(xml_bytes: bytes) -> ET.Element:
    try:
        return ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        preview = xml_bytes[:500].decode("utf-8", errors="replace")
        raise ValueError(f"Could not parse IBKR Flex XML: {preview}") from exc


def _statement_metadata(root: ET.Element) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    for elem in root.iter():
        if strip_namespace(elem.tag) != "FlexStatement":
            continue
        for key in ("fromDate", "toDate", "period", "whenGenerated"):
            value = elem.attrib.get(key)
            if value:
                metadata[key] = value
        break
    return metadata


def parse_flex_xml(xml_bytes: bytes, sections: Optional[Iterable[str]] = None) -> IbkrFlexReport:
    """Parse IBKR Flex XML into section-addressable raw rows.

    The parser keeps XML attributes as strings. Type coercion belongs in the
    normalization layer so raw rows remain a faithful audit representation.
    """
    root = _parse_root(xml_bytes)
    wanted = set(sections or RELEVANT_SECTIONS)
    rows_by_section: Dict[str, List[IbkrRawRow]] = defaultdict(list)
    section_counts: Counter[str] = Counter()

    for elem in root.iter():
        section = strip_namespace(elem.tag)
        if not elem.attrib:
            continue
        section_counts[section] += 1
        if section in wanted:
            rows_by_section[section].append(IbkrRawRow(section=section, attrs=dict(elem.attrib)))

    return IbkrFlexReport(
        root_tag=strip_namespace(root.tag),
        metadata=_statement_metadata(root),
        rows_by_section=dict(rows_by_section),
        section_counts=dict(sorted(section_counts.items())),
    )


def parse_flex_xml_file(path: str | Path, sections: Optional[Iterable[str]] = None) -> IbkrFlexReport:
    return parse_flex_xml(Path(path).read_bytes(), sections=sections)


def count_trade_asset_categories(rows: Iterable[IbkrRawRow]) -> Dict[str, int]:
    counts = Counter(row.attrs.get("assetCategory", "") for row in rows)
    return dict(sorted(counts.items()))


def collect_section_fields(rows_by_section: Mapping[str, Iterable[IbkrRawRow]]) -> Dict[str, List[str]]:
    fields: Dict[str, set[str]] = defaultdict(set)
    for section, rows in rows_by_section.items():
        for row in rows:
            fields[section].update(row.attrs)
    return {section: sorted(names) for section, names in sorted(fields.items())}
