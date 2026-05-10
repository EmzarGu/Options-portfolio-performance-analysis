#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.option_accounting import (  # noqa: E402
    executions_to_dataframe,
    filter_executions,
    option_executions_from_report,
)
from portfolio_backend.ibkr.performance import (  # noqa: E402
    DIVIDEND_CASH_TYPES,
    cashflows_from_report,
    cashflows_to_dataframe,
    compute_wheel_stock_realized_and_segments,
    wheel_dividend_cashflows,
    wheel_option_executions,
    wheel_stock_movements_from_report,
    yearly_performance_from_report,
)
from portfolio_backend.ibkr.source_adapter import options_dataframe_from_report  # noqa: E402
from portfolio_backend.calculations import build_option_trades, process_option_positions  # noqa: E402
from scripts.ibkr_compare_performance import _comparison_table, _load_streamlit_app, _sheet_yearly  # noqa: E402
from scripts.ibkr_compare_pipeline import _load_combined_report, _resolve_xml_paths  # noqa: E402


def _write_frame(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _sheet_option_events(app, *, as_of: pd.Timestamp, sheets: list[str]) -> pd.DataFrame:
    df = app.load_options(app.SHEET_ID, sheets)
    trades = build_option_trades(df, [])
    events, *_ = process_option_positions(trades, as_of)
    rows = [event.__dict__ for event in events]
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["date", "ticker", "pnl"])
    out["date"] = pd.to_datetime(out["date"])
    out["year"] = out["date"].dt.year
    return out


def _ticker_diffs(sheet_events: pd.DataFrame, option_executions: pd.DataFrame) -> pd.DataFrame:
    sheet = sheet_events.groupby(["year", "ticker"])["pnl"].sum().rename("sheet_option_pnl").reset_index()
    ibkr = option_executions.groupby(["year", "ticker"])["net_cash"].sum().rename("ibkr_option_cashflow_pnl").reset_index()
    out = sheet.merge(ibkr, on=["year", "ticker"], how="outer").fillna(0)
    out["diff_ibkr_minus_sheet"] = out["ibkr_option_cashflow_pnl"] - out["sheet_option_pnl"]
    out["abs_diff"] = out["diff_ibkr_minus_sheet"].abs()
    return out.sort_values(["year", "abs_diff"], ascending=[True, False]).round(6)


def _open_options(report, *, since: pd.Timestamp, as_of: pd.Timestamp) -> pd.DataFrame:
    df = options_dataframe_from_report(report)
    df = df[(pd.to_datetime(df["trans_date"]) >= since) & (pd.to_datetime(df["trans_date"]) <= as_of)].copy()
    trades = build_option_trades(df, [])
    _, open_lots, _, _, _ = process_option_positions(trades, as_of)
    rows = [lot.__dict__ for lot in open_lots]
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["ticker", "otype", "strike", "qty", "open_date", "expiration", "open_price"])
    out["open_premium_remaining"] = out["qty"] * out["open_price"] * 100
    return out.sort_values(["expiration", "ticker", "otype", "strike"]).round(6)


def _as_frame(rows) -> pd.DataFrame:
    return pd.DataFrame([row.as_dict() for row in rows])


def _dividend_reconciliation(all_cashflows, included_cashflows) -> pd.DataFrame:
    all_df = cashflows_to_dataframe([row for row in all_cashflows if row.cash_type in DIVIDEND_CASH_TYPES])
    included_df = cashflows_to_dataframe(included_cashflows)
    if all_df.empty:
        return pd.DataFrame(columns=["date", "ticker", "cash_type", "transaction_id", "action_id", "amount", "included_amount", "excluded_amount"])
    group_cols = ["date", "ticker", "cash_type", "transaction_id", "action_id"]
    all_grouped = all_df.groupby(group_cols, dropna=False)["amount"].sum().rename("amount").reset_index()
    if included_df.empty:
        all_grouped["included_amount"] = 0.0
    else:
        inc_grouped = included_df.groupby(group_cols, dropna=False)["amount"].sum().rename("included_amount").reset_index()
        all_grouped = all_grouped.merge(inc_grouped, on=group_cols, how="left")
        all_grouped["included_amount"] = all_grouped["included_amount"].fillna(0.0)
    all_grouped["excluded_amount"] = all_grouped["amount"] - all_grouped["included_amount"]
    return all_grouped.sort_values(["date", "ticker", "cash_type"]).round(6)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate IBKR wheel reconciliation CSV/JSON artifacts.")
    parser.add_argument("xml_path", nargs="?", default=None)
    parser.add_argument("--xml-dir", default=str(REPO_ROOT / "tmp" / "ibkr_backfill" / "query-1503002"))
    parser.add_argument("--since", required=True, help="Start date for reconciliation artifacts, as YYYY-MM-DD.")
    parser.add_argument("--as-of", required=True, help="End date for reconciliation artifacts, as YYYY-MM-DD.")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "tmp" / "ibkr_reconciliation"))
    parser.add_argument("--sheet", action="append", dest="sheets", default=[])
    args = parser.parse_args()

    since = pd.Timestamp(date.fromisoformat(args.since))
    as_of = pd.Timestamp(date.fromisoformat(args.as_of))
    output_dir = Path(args.output_dir)
    app = _load_streamlit_app()
    sheets = args.sheets or ["Options 2024", "Options 2025", "Options 2026"]
    report = _load_combined_report(_resolve_xml_paths(args.xml_path, args.xml_dir))

    yearly = yearly_performance_from_report(report, since=since, through=as_of)
    sheet = _sheet_yearly(app, as_of=as_of.date(), sheets=sheets)
    comparison = _comparison_table(sheet, yearly)

    all_option_executions = filter_executions(option_executions_from_report(report, short_strategy_only=False), since=since, through=as_of)
    movements = wheel_stock_movements_from_report(report)
    realized, segments, stock_issues = compute_wheel_stock_realized_and_segments(movements, as_of=as_of)
    option_executions, excluded_option_executions, option_issues = wheel_option_executions(all_option_executions, segments)
    option_df = executions_to_dataframe(option_executions)
    if not option_df.empty:
        option_df["year"] = pd.to_datetime(option_df["date"]).dt.year
    excluded_option_df = executions_to_dataframe(excluded_option_executions)
    if not excluded_option_df.empty:
        excluded_option_df["year"] = pd.to_datetime(excluded_option_df["date"]).dt.year
    sheet_events = _sheet_option_events(app, as_of=as_of, sheets=sheets)
    ticker_diffs = _ticker_diffs(sheet_events, option_df)

    movement_df = _as_frame([row for row in movements if since <= row.date <= as_of])
    realized_df = _as_frame([row for row in realized if since <= row.date <= as_of])
    segment_df = _as_frame([row for row in segments if row.end >= since and row.start <= as_of])

    all_cashflows = cashflows_from_report(report)
    included_dividends = wheel_dividend_cashflows(
        all_cashflows,
        segments,
        raw_cash_rows=report.rows("CashTransaction"),
    )
    included_dividend_df = cashflows_to_dataframe([row for row in included_dividends if since <= row.date <= as_of])
    dividend_recon = _dividend_reconciliation(
        [row for row in all_cashflows if since <= row.date <= as_of],
        [row for row in included_dividends if since <= row.date <= as_of],
    )
    open_options = _open_options(report, since=since, as_of=as_of)

    outputs = {
        "yearly_comparison.csv": comparison,
        "ticker_option_diffs.csv": ticker_diffs,
        "ibkr_option_cashflows.csv": option_df,
        "ibkr_option_cashflows_excluded.csv": excluded_option_df,
        "wheel_stock_movements.csv": movement_df,
        "wheel_stock_realized.csv": realized_df,
        "wheel_holding_segments.csv": segment_df,
        "wheel_dividends_included.csv": included_dividend_df,
        "wheel_dividend_reconciliation.csv": dividend_recon,
        "open_options.csv": open_options,
    }
    for filename, frame in outputs.items():
        _write_frame(frame, output_dir / filename)

    summary = {
        "since": args.since,
        "as_of": args.as_of,
        "output_dir": str(output_dir),
        "files": sorted(outputs),
        "yearly_comparison": _records(comparison),
        "top_option_diffs": _records(ticker_diffs.head(30)),
        "stock_issues": stock_issues,
        "option_issues": option_issues,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
