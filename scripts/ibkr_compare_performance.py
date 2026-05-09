#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.performance import yearly_performance_from_report  # noqa: E402
from portfolio_backend.pipeline import build_pipeline_without_live_prices  # noqa: E402
from scripts.ibkr_compare_pipeline import _load_combined_report, _resolve_xml_paths  # noqa: E402


def _load_streamlit_app():
    with contextlib.redirect_stderr(io.StringIO()):
        import streamlit_app as app

    return app


def _sheet_yearly(app, *, as_of: date, sheets: list[str]) -> pd.DataFrame:
    state = build_pipeline_without_live_prices(
        app.SHEET_ID,
        as_of,
        False,
        sheets,
        app.load_options,
        app.fetch_price_history_yf,
        app.collect_dividend_cashflows,
        app.align_benchmarks_monthly,
    )
    cols = ["year", "realized_options_pnl", "realized_stock_pnl", "dividends", "total_realized_pnl"]
    out = state.yearly[[col for col in cols if col in state.yearly.columns]].copy()
    out = out.rename(
        columns={
            "realized_options_pnl": "sheet_option_pnl",
            "realized_stock_pnl": "sheet_stock_pnl",
            "dividends": "sheet_dividends",
            "total_realized_pnl": "sheet_total_realized_pnl",
        }
    )
    return out


def _comparison_table(sheet: pd.DataFrame, ibkr: pd.DataFrame) -> pd.DataFrame:
    out = sheet.merge(ibkr, on="year", how="outer").fillna(0)
    out["diff_option_ibkr_minus_sheet"] = out["option_cashflow_pnl"] - out["sheet_option_pnl"]
    out["diff_stock_ibkr_minus_sheet"] = out["stock_realized_pnl"] - out["sheet_stock_pnl"]
    out["diff_dividends_ibkr_minus_sheet"] = out["dividends_net"] - out["sheet_dividends"]
    out["diff_total_ibkr_minus_sheet"] = out["realized_strategy_cash_pnl"] - out["sheet_total_realized_pnl"]
    return out.sort_values("year").round(6)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare IBKR-native yearly performance to current sheet pipeline.")
    parser.add_argument("xml_path", nargs="?", default=None)
    parser.add_argument("--xml-dir", default=str(REPO_ROOT / "tmp" / "ibkr_backfill" / "query-1503002"))
    parser.add_argument("--since", default="2024-01-01")
    parser.add_argument("--as-of", default="2026-05-08")
    parser.add_argument(
        "--sheet",
        action="append",
        dest="sheets",
        default=[],
        help="Google Sheet tab to include. Repeatable. Defaults to Options 2024/2025/2026.",
    )
    args = parser.parse_args()

    app = _load_streamlit_app()
    sheets = args.sheets or ["Options 2024", "Options 2025", "Options 2026"]
    report = _load_combined_report(_resolve_xml_paths(args.xml_path, args.xml_dir))
    since = pd.Timestamp(date.fromisoformat(args.since))
    as_of = pd.Timestamp(date.fromisoformat(args.as_of))
    ibkr = yearly_performance_from_report(report, since=since, through=as_of)
    sheet = _sheet_yearly(app, as_of=as_of.date(), sheets=sheets)
    comparison = _comparison_table(sheet, ibkr)

    print(
        json.dumps(
            {
                "since": args.since,
                "as_of": args.as_of,
                "sheets": sheets,
                "comparison": comparison.to_dict(orient="records"),
                "notes": [
                    "IBKR option_cashflow_pnl is execution-date option premium/close cash including commissions.",
                    "IBKR option_cashflow_pnl is wheel-scoped: puts are included, but calls are included only while prior put-assignment stock inventory is held.",
                    "IBKR stock_realized_pnl is wheel-scoped: assigned-put stock buys matched FIFO to assigned-call stock sells from OptionEAE stock-side rows.",
                    "IBKR dividends_net is wheel-scoped: dividend/payment-in-lieu/withholding cash is included only while assignment-derived shares are held.",
                    "Deposits/withdrawals, interest, and account fees are reported separately and excluded from realized_strategy_cash_pnl.",
                ],
                "stock_issues": ibkr.attrs.get("stock_issues", []),
                "option_issues": ibkr.attrs.get("option_issues", []),
            },
            indent=2,
            default=str,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
