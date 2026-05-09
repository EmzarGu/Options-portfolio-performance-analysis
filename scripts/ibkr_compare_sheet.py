#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.flex_parser import parse_flex_xml_file  # noqa: E402
from portfolio_backend.ibkr.source_adapter import options_dataframe_from_report, summarize_options_frame  # noqa: E402


def _load_sheet_options(selected_sheets: list[str]) -> pd.DataFrame:
    # Importing Streamlit app outside Streamlit emits runtime warnings. Keep the
    # comparison output machine-readable by swallowing those warnings.
    with contextlib.redirect_stderr(io.StringIO()):
        import streamlit_app as app

        return app.load_options(app.SHEET_ID, selected_sheets)


def _filter_since(df: pd.DataFrame, since: str) -> pd.DataFrame:
    if not since or df.empty:
        return df.copy()
    dates = pd.to_datetime(df["trans_date"], errors="coerce")
    return df.loc[dates >= pd.Timestamp(since)].copy()


def _comparable_sums(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["action", "type", "rows", "qty", "amount", "commission", "total_pnl"])
    out = (
        df.groupby(["action", "type"])[["qty", "amount", "commission", "total_pnl"]]
        .sum(numeric_only=True)
        .reset_index()
    )
    counts = df.groupby(["action", "type"]).size().rename("rows").reset_index()
    out = counts.merge(out, on=["action", "type"], how="left")
    return out.sort_values(["action", "type"]).reset_index(drop=True)


def _diff_sums(sheet_df: pd.DataFrame, ibkr_df: pd.DataFrame) -> list[dict]:
    sheet = _comparable_sums(sheet_df).rename(
        columns={col: f"sheet_{col}" for col in ("rows", "qty", "amount", "commission", "total_pnl")}
    )
    ibkr = _comparable_sums(ibkr_df).rename(
        columns={col: f"ibkr_{col}" for col in ("rows", "qty", "amount", "commission", "total_pnl")}
    )
    merged = sheet.merge(ibkr, on=["action", "type"], how="outer").fillna(0)
    for col in ("rows", "qty", "amount", "commission", "total_pnl"):
        merged[f"diff_{col}"] = merged[f"ibkr_{col}"] - merged[f"sheet_{col}"]
    return merged.round(6).to_dict(orient="records")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare current Google Sheet option rows to IBKR-derived option rows.")
    parser.add_argument("xml_path", nargs="?", default="/tmp/ibkr-flex-1503002.xml")
    parser.add_argument("--since", default="2025-05-09")
    parser.add_argument(
        "--sheet",
        action="append",
        dest="sheets",
        default=[],
        help="Google Sheet tab to include. Repeatable. Defaults to Options 2024/2025/2026.",
    )
    parser.add_argument(
        "--include-long-legs",
        action="store_true",
        help="Include IBKR long option opens/closes. Default compares only short-strategy-compatible rows.",
    )
    args = parser.parse_args()

    sheets = args.sheets or ["Options 2024", "Options 2025", "Options 2026"]
    report = parse_flex_xml_file(args.xml_path)
    ibkr_df = _filter_since(
        options_dataframe_from_report(report, short_strategy_only=not args.include_long_legs),
        args.since,
    )
    sheet_df = _filter_since(_load_sheet_options(sheets), args.since)

    summary = {
        "since": args.since,
        "sheets": sheets,
        "sheet_summary": summarize_options_frame(sheet_df),
        "ibkr_summary": summarize_options_frame(ibkr_df),
        "action_type_diffs": _diff_sums(sheet_df, ibkr_df),
        "notes": [
            "This compares IBKR option trades in the 365-day Flex report.",
            "By default IBKR long option legs are excluded because the current pipeline models short option lots.",
            "Differences are expected until filtering and strategy mapping are reconciled.",
            "IBKR includes stock trades separately; this comparison covers option-shaped rows only.",
        ],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
