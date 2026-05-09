#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.flex_parser import (  # noqa: E402
    RELEVANT_SECTIONS,
    collect_section_fields,
    count_trade_asset_categories,
    parse_flex_xml_file,
)
from portfolio_backend.ibkr.normalization import normalize_transactions, redacted_preview  # noqa: E402


def build_summary(xml_path: Path, preview_limit: int) -> dict:
    report = parse_flex_xml_file(xml_path, sections=RELEVANT_SECTIONS)
    trade_rows = report.rows("Trade")
    option_eae_rows = report.rows("OptionEAE")
    normalized = normalize_transactions([*trade_rows, *option_eae_rows])
    normalized_counts = Counter(txn.source_section for txn in normalized)
    normalized_asset_counts = Counter(
        f"{txn.source_section}:{txn.asset_category or 'unknown'}" for txn in normalized
    )

    return {
        "xml_path": str(xml_path),
        "xml_bytes": xml_path.stat().st_size,
        "root_tag": report.root_tag,
        "metadata": report.metadata,
        "section_counts": {
            section: report.section_counts.get(section, 0)
            for section in sorted(RELEVANT_SECTIONS)
        },
        "trade_asset_categories": count_trade_asset_categories(trade_rows),
        "normalized_counts": dict(sorted(normalized_counts.items())),
        "normalized_asset_counts": dict(sorted(normalized_asset_counts.items())),
        "section_fields": collect_section_fields(report.rows_by_section),
        "redacted_previews": [
            redacted_preview(txn)
            for txn in normalized[: max(0, preview_limit)]
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run parse a local IBKR Flex XML report without writing remote state."
    )
    parser.add_argument(
        "xml_path",
        nargs="?",
        default="/tmp/ibkr-flex-1503002.xml",
        help="Path to an IBKR Flex XML file. Defaults to the latest local test pull.",
    )
    parser.add_argument("--preview-limit", type=int, default=5)
    args = parser.parse_args()

    xml_path = Path(args.xml_path).expanduser()
    if not xml_path.exists():
        raise SystemExit(f"IBKR Flex XML file not found: {xml_path}")
    print(json.dumps(build_summary(xml_path, args.preview_limit), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
