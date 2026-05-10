import json
import logging
import os
import subprocess
import uuid
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple
try:
    import tomllib  # py311+
except ModuleNotFoundError:  # py3.9/3.10
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError:
        tomllib = None

import pandas as pd
import streamlit as st
import altair as alt
from google.oauth2 import service_account
from data_sources import (
    DividendFetchResult,
    SheetDownload,
    YFinanceDividendProvider,
    align_benchmarks_monthly as _align_benchmarks_monthly,
    clear_dividend_history_cache,
    collect_dividend_cashflows as _collect_dividend_cashflows,
    download_excel_workbook,
    fetch_current_prices_yf as _fetch_current_prices_yf,
    fetch_drive_file_metadata as _data_source_fetch_drive_file_metadata,
    fetch_price_history_yf as _fetch_price_history_yf,
    load_options_from_excel_bytes,
    option_sheet_names_from_excel_bytes,
)

logger = logging.getLogger(__name__)
from portfolio_backend.calculations import (
    assess_capital_history_coverage,
    build_capital_timeline,
    build_holding_segments,
    build_option_trades,
    compute_stock_realized_and_inventory,
    daterange_days,
    infer_mixed_short_leg,
    parse_strike_pair,
    process_option_positions,
    resolve_capital_price_on_day,
)
from portfolio_backend.charts import (
    build_benchmark_growth_chart_data,
    build_options_cycle_chart_data,
    clean_monthly_return_series as _clean_monthly_return_series,
    select_chart_return_window as _select_chart_return_window,
)
from portfolio_backend.constants import CONTRACT_MULTIPLIER
from portfolio_backend.issue_classification import split_actionable_and_audit_issues
from portfolio_backend.models import (
    ChainOutcome,
    HoldSeg,
    OpenLot,
    OptionLot,
    OptionPnLEvent,
    OptionTrade,
    PipelineState,
    RealizedSale,
    StockTxn,
)
from portfolio_backend.performance import (
    build_chains,
    build_covered_return_series,
    build_dashboard_unrealized_adjusted_return_series,
    build_dashboard_unrealized_snapshot,
    build_per_ticker_totals,
    build_monthly_summary,
    build_yearly_with_dashboard_unrealized,
    calculate_performance_metrics,
    calculate_performance_metrics_if_complete,
    calculate_unrealized_positions,
    capital_stats_by_year,
    expectancies,
    period_returns,
    per_ticker_yearly_from_realized,
    realized_option_pnl_by_year,
    realized_stock_pnl_by_year,
    twr_annualized_by_year,
    yearly_summary_from_monthly,
)
from portfolio_backend.pipeline import (
    apply_live_price_overlay as _backend_apply_live_price_overlay,
    apply_unrealized_adjusted_display,
    build_base_pipeline as _backend_build_base_pipeline,
    build_pipeline_without_live_prices as _backend_build_pipeline_without_live_prices,
    current_price_tickers_for_state as _backend_current_price_tickers_for_state,
    issues_without_current_price_messages as _backend_issues_without_current_price_messages,
    normalize_dividend_fetch_result,
    open_option_lots_for_state as _backend_open_option_lots_for_state,
)
from portfolio_backend.ibkr.pipeline import build_ibkr_base_pipeline as _backend_build_ibkr_base_pipeline
from portfolio_backend.ibkr.repository import load_flex_report_from_env
from portfolio_backend.tables import (
    build_assigned_holdings_frame,
    build_open_option_shorts_frame,
    filter_df_to_range,
)
from portfolio_backend.view_models import build_dashboard_view_model

# Lazy import to avoid startup delays if not used
try:
    import yfinance as yf
except Exception:  # pragma: no cover - Streamlit Cloud will install from requirements
    yf = None


# ------------------------------------------------------------
# Page config / styling
# ------------------------------------------------------------
st.set_page_config(page_title="Options ROI Dashboard", layout="wide")
st.markdown(
    """
    <style>
        .metric-card {background: #0b132b; color: #e0e6ed; padding: 16px; border-radius: 14px; border: 1px solid #1f2a44;}
        .metric-value {font-size: 26px; font-weight: 700; margin: 0;}
        .metric-label {font-size: 12px; color: #9fb3c8; margin: 0;}
        .section-title {margin-top: 12px; margin-bottom: 4px;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
SHEET_ID = "19LhrZai3cbJ1GbPE1iTquYHUeXfpIxXFX1amF5eWi_g"
SHEETS = ["Options 2024", "Options 2025", "Options 2026"]
DATA_SOURCE_GOOGLE_SHEETS = "google_sheets"
DATA_SOURCE_IBKR = "ibkr"
IBKR_SOURCE_LABEL = "IBKR Flex"
DEFAULT_FIRESTORE_PROJECT_ID = "options-performance-dashboard"
DEFAULT_IBKR_FLEX_QUERY_ID = "1503002"
STREAMLIT_ENV_SECRET_KEYS = (
    "OPTIONS_DATA_SOURCE",
    "IBKR_REPORT_SOURCE",
    "IBKR_FLEX_QUERY_ID",
    "FIRESTORE_PROJECT_ID",
    "FIRESTORE_DATABASE",
    "FIRESTORE_SERVICE_ACCOUNT_JSON",
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "PRICE_HISTORY_STORE",
    "DIVIDEND_HISTORY_STORE",
    "AUDIT_STORE",
)
# Keep the original on-disk prefs path for local runs; add a home-dir fallback for rebuilds.
PREFS_PATH = Path(".streamlit_user_prefs.json")
PREFS_HOME_PATH = Path.home() / ".options_roi_prefs.json"
PIPELINE_RELOAD_TOKEN_KEY = "pipeline_reload_token"
PRICE_SESSION_ID_KEY = "price_session_id"
PRICE_REFRESH_TOKEN_KEY = "price_refresh_token"
UNREALIZED_ADJUSTED_TOGGLE_LABEL = "Add heuristic unrealized to current-year totals and TWR"
UNREALIZED_ADJUSTED_EXPLANATION = (
    "Uses the current unrealized snapshot to adjust current-year totals and TWR. "
    "This is not a full option mark-to-market calculation."
)
UNREALIZED_ADJUSTED_TWR_LABEL = "YTD Annualized TWR (unrealized-adjusted)"
UNREALIZED_ADJUSTED_VIEW_LABEL = "##### Comprehensive Yearly Performance (Unrealized-adjusted view)"
UNREALIZED_ADJUSTED_TOTAL_LABEL = "Total P&L (incl heuristic unrealized)"
UNREALIZED_ADJUSTED_TWR_COLUMN_LABEL = "Ann. TWR (unrealized-adjusted)"
CURRENT_UNREALIZED_SNAPSHOT_LABEL = "Current unrealized snapshot"


# ------------------------------------------------------------
# Secrets / credentials
# ------------------------------------------------------------
def _load_credentials():
    def parse(raw_val):
        if isinstance(raw_val, dict):
            return raw_val
        if isinstance(raw_val, str):
            txt = raw_val.strip()
            for triple in ('"""', "'''"):
                if txt.startswith(triple) and txt.endswith(triple):
                    txt = txt[len(triple) : -len(triple)]
                    txt = txt.strip()
            # 1) normal JSON
            try:
                return json.loads(txt)
            except json.JSONDecodeError:
                # If TOML basic string expanded \n into real newlines inside private_key,
                # re-escape newlines inside that value and retry.
                try:
                    import re

                    def _fix_pk(match):
                        val = match.group(1)
                        val_fixed = val.replace("\r\n", "\n").replace("\n", "\\n")
                        return f'"private_key": "{val_fixed}"'

                    txt_esc = re.sub(r'"private_key"\s*:\s*"(.*?)"', _fix_pk, txt, flags=re.DOTALL)
                    return json.loads(txt_esc)
                except Exception:
                    pass
            # 2) single-quoted JSON (naive)
            try:
                return json.loads(txt.replace("'", '"'))
            except Exception:
                pass
            # 3) literal_eval for TOML-ish dicts
            try:
                import ast

                val = ast.literal_eval(txt)
                if isinstance(val, dict):
                    return val
            except Exception:
                pass
        raise RuntimeError("Could not parse GOOGLE_SERVICE_ACCOUNT_JSON; please paste raw JSON for the service account.")

    # Priority: st.secrets -> env var -> local secrets file -> fallback keys in st.secrets
    raw = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw is None:
        env_val = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if env_val:
            raw = env_val
    if raw is None:
        secrets_path = os.getenv("LOCAL_SECRETS_PATH")
        if secrets_path:
            p = Path(secrets_path).expanduser()
            if not p.exists():
                raise RuntimeError(f"LOCAL_SECRETS_PATH is set but file not found: {p}")
            if p.suffix.lower() == ".toml":
                if tomllib is None:
                    raise RuntimeError("tomllib/tomli not available; install tomli or use JSON secrets.")
                data = tomllib.loads(p.read_text())
                raw = (
                    data.get("GOOGLE_SERVICE_ACCOUNT_JSON")
                    or data.get("google_service_account_json")
                    or data.get("service_account")
                )
            else:
                raw = p.read_text()
    if raw is None:
        for key in ("gcp_service_account", "service_account"):
            if key in st.secrets:
                raw = st.secrets[key]
                break
    if raw is None:
        raise RuntimeError("Secret GOOGLE_SERVICE_ACCOUNT_JSON is missing in Streamlit secrets, env var, or LOCAL_SECRETS_PATH.")

    info = parse(raw)
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def _coerce_bool(val) -> bool:
    """Convert query-string-ish values into a boolean."""
    if isinstance(val, list) and val:
        val = val[-1]
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() not in ("0", "false", "no", "off")


def _get_query_params() -> Dict[str, List[str]]:
    """Return query params in the legacy dict[str, list[str]] shape."""
    try:
        query_params = getattr(st, "query_params", None)
        if query_params is not None:
            return {key: query_params.get_all(key) for key in query_params}
        return st.experimental_get_query_params()
    except Exception:
        return {}


def _set_query_params(params: Dict[str, List[str]]) -> None:
    """Persist query params while preserving repeated-key semantics."""
    try:
        query_params = getattr(st, "query_params", None)
        if query_params is not None:
            query_params.from_dict(params)
        else:
            st.experimental_set_query_params(**params)
    except Exception:
        pass


def _load_query_prefs() -> Dict:
    """Restore prefs from the browser URL so they survive app sleep/restarts."""
    params = _get_query_params()
    if not params:
        return {}
    prefs: Dict[str, object] = {}
    if "include_unrealized" in params:
        prefs["include_unrealized"] = _coerce_bool(params["include_unrealized"])
    if "selected_sheets" in params:
        prefs["selected_sheets"] = params.get("selected_sheets", [])
    return prefs


def _read_prefs_file(path: Path) -> Dict:
    try:
        data = json.loads(path.read_text())
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def load_prefs():
    # Priority: repo-local file -> home file -> query params (most recent user choices)
    prefs = _read_prefs_file(PREFS_PATH)
    prefs.update(_read_prefs_file(PREFS_HOME_PATH))
    prefs.update(_load_query_prefs())
    return prefs


def _persist_query_params(prefs: Dict) -> None:
    """Encode prefs into the URL so they survive Streamlit sleep/restarts."""
    current = _get_query_params()
    has_query_params = hasattr(st, "query_params")
    has_experimental = hasattr(st, "experimental_get_query_params") and hasattr(st, "experimental_set_query_params")
    if not current and not has_query_params and not has_experimental:
        return

    desired = dict(current)
    desired["include_unrealized"] = ["1"] if prefs.get("include_unrealized") else ["0"]
    sheets = [str(s) for s in prefs.get("selected_sheets") or [] if s]
    if sheets:
        desired["selected_sheets"] = sheets
    elif "selected_sheets" in desired:
        desired.pop("selected_sheets")

    if desired != current:
        _set_query_params(desired)


def save_prefs(prefs: Dict):
    # Best-effort write to both locations so preferences survive rebuilds/sleep.
    for path in (PREFS_PATH, PREFS_HOME_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(prefs, indent=2))
        except Exception:
            pass
    _persist_query_params(prefs)


def _rerun_app():
    """Streamlit rerun helper compatible with new and old versions."""
    if hasattr(st, "rerun"):
        st.rerun()
    else:  # pragma: no cover - older Streamlit fallback
        st.experimental_rerun()


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_ts(ts: Optional[str]) -> str:
    dt = _parse_iso(ts)
    if not dt:
        return "n/a"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _fetch_drive_file_metadata(sheet_id: str) -> Dict[str, str]:
    return _data_source_fetch_drive_file_metadata(sheet_id, _load_credentials())


@st.cache_data(show_spinner=False, ttl=120)
def get_drive_file_metadata(sheet_id: str) -> Dict[str, str]:
    return _fetch_drive_file_metadata(sheet_id)


@st.cache_data(show_spinner=False)
def _download_excel(sheet_id: str) -> SheetDownload:
    override = os.getenv("LOCAL_EXCEL_PATH")
    if override:
        return download_excel_workbook(sheet_id, local_excel_path=override)

    creds = None
    creds_err = None
    try:
        creds = _load_credentials()
    except Exception as exc:
        creds_err = exc

    try:
        return download_excel_workbook(sheet_id, credentials=creds)
    except Exception as exc:
        if creds_err:
            raise RuntimeError(f"{creds_err} {exc}") from exc
        raise


@st.cache_data(show_spinner=True)
def list_option_sheets(sheet_id: str) -> List[str]:
    try:
        download = _download_excel(sheet_id)
        return option_sheet_names_from_excel_bytes(download.content)
    except Exception:
        return []


@st.cache_data(show_spinner=True)
def load_options(sheet_id: str, sheets: List[str]) -> pd.DataFrame:
    download = _download_excel(sheet_id)
    return load_options_from_excel_bytes(download.content, sheets)


def _clear_data_caches() -> None:
    _download_excel.clear()
    list_option_sheets.clear()
    load_options.clear()
    get_drive_file_metadata.clear()
    clear_dividend_history_cache()
    try:
        get_cached_pipeline.clear()
    except NameError:
        pass
    try:
        get_cached_ibkr_pipeline_resource.clear()
    except NameError:
        pass
    try:
        get_cached_current_prices.clear()
    except NameError:
        pass


def align_benchmarks_monthly(tickers: Dict[str, str], idx: pd.DatetimeIndex):
    return _align_benchmarks_monthly(tickers, idx, yf)


def resolve_build_version() -> str:
    for env_key in ("APP_BUILD_VERSION", "BUILD_VERSION"):
        env_val = os.getenv(env_key)
        if env_val:
            return env_val.strip()

    repo_root = Path(__file__).resolve().parent
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        committed_at = subprocess.run(
            ["git", "show", "-s", "--format=%cI", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if sha and committed_at:
            return f"git:{sha} ({committed_at})"
        if sha:
            return f"git:{sha}"
    except Exception:
        pass
    return "unknown"


APP_BUILD_VERSION = resolve_build_version()


def sync_streamlit_secrets_to_env() -> None:
    try:
        secrets = st.secrets
        for key in STREAMLIT_ENV_SECRET_KEYS:
            if os.getenv(key):
                continue
            if key not in secrets:
                continue
            value = secrets.get(key)
            if value is not None:
                os.environ[key] = str(value)
    except Exception:
        pass


def _set_env_default_if_blank(key: str, value: str) -> None:
    if not str(os.getenv(key, "")).strip():
        os.environ[key] = value


def data_source_mode() -> str:
    sync_streamlit_secrets_to_env()
    value = os.getenv("OPTIONS_DATA_SOURCE", DATA_SOURCE_GOOGLE_SHEETS).strip().lower()
    if value in {DATA_SOURCE_IBKR, "ibkr_flex"}:
        return DATA_SOURCE_IBKR
    if value in {DATA_SOURCE_GOOGLE_SHEETS, "sheets", "sheet", "google"}:
        return DATA_SOURCE_GOOGLE_SHEETS
    return DATA_SOURCE_GOOGLE_SHEETS


def streamlit_app_source_mode() -> str:
    sync_streamlit_secrets_to_env()
    if not os.getenv("OPTIONS_DATA_SOURCE"):
        _set_env_default_if_blank("IBKR_REPORT_SOURCE", "firestore")
        _set_env_default_if_blank("FIRESTORE_PROJECT_ID", DEFAULT_FIRESTORE_PROJECT_ID)
        _set_env_default_if_blank("IBKR_FLEX_QUERY_ID", DEFAULT_IBKR_FLEX_QUERY_ID)
        return DATA_SOURCE_IBKR
    mode = data_source_mode()
    if mode == DATA_SOURCE_IBKR:
        _set_env_default_if_blank("IBKR_REPORT_SOURCE", "firestore")
        _set_env_default_if_blank("FIRESTORE_PROJECT_ID", DEFAULT_FIRESTORE_PROJECT_ID)
        _set_env_default_if_blank("IBKR_FLEX_QUERY_ID", DEFAULT_IBKR_FLEX_QUERY_ID)
    return mode


def is_ibkr_source_mode() -> bool:
    return data_source_mode() == DATA_SOURCE_IBKR


def source_label_for_mode(mode: Optional[str] = None) -> str:
    return IBKR_SOURCE_LABEL if (mode or data_source_mode()) == DATA_SOURCE_IBKR else "Google Sheets"


def available_sources_for_mode(mode: Optional[str] = None) -> List[str]:
    if (mode or data_source_mode()) == DATA_SOURCE_IBKR:
        return [IBKR_SOURCE_LABEL]
    return list_option_sheets(SHEET_ID)


def normalize_selected_sheets_for_mode(selected_sheets: Optional[List[str]], available_sheets: List[str], mode: Optional[str] = None) -> List[str]:
    if (mode or data_source_mode()) == DATA_SOURCE_IBKR:
        return [IBKR_SOURCE_LABEL]
    return list(selected_sheets or [])


def fetch_current_prices_yf(tickers) -> Tuple[Dict[str, float], List[str], Dict[str, int]]:
    return _fetch_current_prices_yf(tickers, yf)


def fetch_price_history_yf(
    tickers,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]:
    return _fetch_price_history_yf(tickers, start, end, yf)


def _format_df(df: pd.DataFrame, currency_cols=None, pct_cols=None, int_cols=None, float_cols=None, hide_index=False, na_rep=None):
    df = df.copy()
    numeric_cols = set(currency_cols or []).union(pct_cols or [], int_cols or [], float_cols or [])
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    formatter = {}
    if currency_cols:
        formatter.update({c: "{:,.0f}".format for c in currency_cols if c in df.columns})
    if pct_cols:
        formatter.update({c: "{:.1%}".format for c in pct_cols if c in df.columns})
    if int_cols:
        formatter.update({c: "{:.0f}".format for c in int_cols if c in df.columns})
    if float_cols:
        formatter.update({c: "{:.2f}".format for c in float_cols if c in df.columns})
    styler = df.style.format(formatter, na_rep=na_rep).set_properties(**{"text-align": "right"})
    if hide_index:
        try:
            styler = styler.hide(axis="index")
        except Exception:
            styler = styler.hide_index()
    return styler


def metric_card(label, value, delta=None):
    delta_txt = (
        f"<span style='color:#4ade80'>▲ {delta}</span>"
        if delta is not None and delta >= 0
        else f"<span style='color:#f87171'>▼ {abs(delta)}</span>" if delta is not None else ""
    )
    st.markdown(
        f"""
        <div class="metric-card">
            <p class="metric-label">{label}</p>
            <p class="metric-value">{value}</p>
            <p class="metric-label">{delta_txt}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_data_status(sheet_id: str) -> None:
    try:
        download_info = _download_excel(sheet_id)
    except Exception:
        download_info = None

    if download_info and download_info.source == "local":
        override = os.getenv("LOCAL_EXCEL_PATH")
        label = "Local file"
        if override:
            label = f"Local file: {Path(override).expanduser().name}"
        st.caption(f"Source: {label}")
        st.caption(f"File modified: {_format_ts(download_info.file_modified_at)}")
        st.caption(f"Data loaded: {_format_ts(download_info.downloaded_at)}")
        return
    if download_info and download_info.source == "public":
        st.caption("Source: Public Google Sheet (no auth)")
        if download_info.file_modified_at:
            st.caption(f"File modified: {_format_ts(download_info.file_modified_at)}")
        st.caption(f"Data loaded: {_format_ts(download_info.downloaded_at)}")
        return

    try:
        current_meta = get_drive_file_metadata(sheet_id)
    except Exception:
        current_meta = None

    if download_info is None and current_meta is None:
        st.caption("Data status unavailable.")
        return

    drive_name = None
    if download_info and download_info.file_name:
        drive_name = download_info.file_name
    elif current_meta:
        drive_name = current_meta.get("name")

    if drive_name:
        st.caption(f"Drive file: {drive_name}")
    else:
        st.caption(f"Drive file id: {sheet_id}")

    used_modified = download_info.file_modified_at if download_info else None
    st.caption(f"File modified (used): {_format_ts(used_modified)}")
    if download_info and download_info.file_version:
        st.caption(f"Drive version (used): {download_info.file_version}")
    st.caption(f"Data downloaded: {_format_ts(download_info.downloaded_at if download_info else None)}")

    current_modified = current_meta.get("modifiedTime") if current_meta else None
    if current_modified:
        st.caption(f"File modified (current): {_format_ts(current_modified)}")

    used_dt = _parse_iso(used_modified) if used_modified else None
    current_dt = _parse_iso(current_modified) if current_modified else None
    if used_dt and current_dt and current_dt > used_dt:
        st.warning("Drive file is newer than cached data. Click Reload to fetch the latest file.")


def collect_dividend_cashflows(stock_txns: List[StockTxn], as_of: pd.Timestamp) -> DividendFetchResult:
    provider = YFinanceDividendProvider(yf) if yf is not None else None
    return _collect_dividend_cashflows(stock_txns, as_of, build_holding_segments, provider)


def render_issue_status_banner(issues: List[str], price_errors: List[str], price_summary: Dict[str, int]) -> None:
    severity = "success"
    actionable_issues, audit_issues = split_actionable_and_audit_issues(issues)
    coverage_problem = price_summary and (
        price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
    )
    if actionable_issues or price_errors:
        severity = "error"
    elif coverage_problem:
        severity = "warning"

    total_issues = len(actionable_issues) + len(price_errors) + (1 if coverage_problem else 0)
    if total_issues == 0:
        msg = "0 actionable issues detected (Diagnostics tab)"
        if audit_issues:
            msg += f" · {len(audit_issues)} audit note(s)"
    else:
        msg = f"{total_issues} actionable issue(s) detected — check Diagnostics tab"

    color = {"success": "#22c55e", "warning": "#f59e0b", "error": "#ef4444"}[severity]
    st.markdown(f"<div style='font-weight:600; color:{color}; margin: 4px 0;'>{msg}</div>", unsafe_allow_html=True)


def _build_pipeline_uncached(as_of: date, include_unrealized_current_year: bool, selected_sheets: List[str], cache_bust: int = 1):
    source_mode = data_source_mode()
    selected_sheets = normalize_selected_sheets_for_mode(selected_sheets, [IBKR_SOURCE_LABEL], source_mode)
    if source_mode == DATA_SOURCE_IBKR:
        base_state = build_base_pipeline(as_of, selected_sheets, cache_bust=cache_bust, source_mode=source_mode)
        return apply_unrealized_adjusted_display(base_state, include_unrealized_current_year)
    return _backend_build_pipeline_without_live_prices(
        SHEET_ID,
        as_of,
        include_unrealized_current_year,
        selected_sheets,
        load_options,
        fetch_price_history_yf,
        collect_dividend_cashflows,
        align_benchmarks_monthly,
        cache_bust=cache_bust,
    )


def build_base_pipeline(
    as_of: date,
    selected_sheets: List[str],
    cache_bust: int = 1,
    source_mode: Optional[str] = None,
) -> PipelineState:
    sync_streamlit_secrets_to_env()
    source_mode = source_mode or data_source_mode()
    def record_pipeline_phase(phase: str, elapsed_ms: float) -> None:
        logger.warning(
            "streamlit_pipeline_phase source=%s phase=%s elapsed_ms=%.1f",
            source_mode,
            phase,
            elapsed_ms,
        )

    if source_mode == DATA_SOURCE_IBKR:
        started_at = perf_counter()
        report = load_flex_report_from_env()
        record_pipeline_phase("ibkr_load_report_ms", (perf_counter() - started_at) * 1000)
        return _backend_build_ibkr_base_pipeline(
            report,
            as_of=as_of,
            fetch_price_history_fn=fetch_price_history_yf,
            align_benchmarks_monthly_fn=align_benchmarks_monthly,
            selected_sheets=[IBKR_SOURCE_LABEL],
            cache_bust=cache_bust,
            timing_recorder=record_pipeline_phase,
        )
    return _backend_build_base_pipeline(
        SHEET_ID,
        as_of,
        selected_sheets,
        load_options,
        fetch_price_history_yf,
        collect_dividend_cashflows,
        align_benchmarks_monthly,
        cache_bust=cache_bust,
        timing_recorder=record_pipeline_phase,
    )


def build_pipeline(
    as_of: date,
    include_unrealized_current_year: bool,
    selected_sheets: List[str],
    cache_bust: int = 1,
    price_refresh_token=0,
):
    selected_sheets = normalize_selected_sheets_for_mode(selected_sheets, [IBKR_SOURCE_LABEL] if is_ibkr_source_mode() else selected_sheets)
    base_state = build_base_pipeline(as_of, selected_sheets, cache_bust=cache_bust)
    priced_state = apply_live_price_overlay(base_state, price_refresh_token)
    return apply_unrealized_adjusted_display(priced_state, include_unrealized_current_year)


def build_pipeline_cache_key(
    as_of: date,
    include_unrealized_current_year: bool,
    selected_sheets: List[str],
    reload_token: int,
    source_mode: Optional[str] = None,
) -> Tuple[str, str, Tuple[str, ...], int]:
    source_mode = source_mode or data_source_mode()
    as_of_key = pd.to_datetime(as_of).date().isoformat()
    selected_sheets = normalize_selected_sheets_for_mode(selected_sheets, [IBKR_SOURCE_LABEL], source_mode)
    selected_sheets_key = tuple(str(sheet) for sheet in (selected_sheets or []))
    return (
        source_mode,
        as_of_key,
        selected_sheets_key,
        int(reload_token or 0),
    )


@st.cache_data(show_spinner="Building portfolio pipeline...")
def get_cached_pipeline(
    source_mode: str,
    as_of_key: str,
    selected_sheets_key: Tuple[str, ...],
    reload_token: int,
) -> PipelineState:
    return build_base_pipeline(
        date.fromisoformat(as_of_key),
        list(selected_sheets_key),
        cache_bust=reload_token,
        source_mode=source_mode,
    )


@st.cache_resource(show_spinner=False, ttl=300)
def get_cached_ibkr_pipeline_resource(
    source_mode: str,
    as_of_key: str,
    selected_sheets_key: Tuple[str, ...],
    reload_token: int,
) -> PipelineState:
    return build_base_pipeline(
        date.fromisoformat(as_of_key),
        list(selected_sheets_key),
        cache_bust=reload_token,
        source_mode=source_mode,
    )


def _open_option_lots_for_state(state: PipelineState) -> List[OptionLot]:
    return _backend_open_option_lots_for_state(state)


def _current_price_tickers_for_state(state: PipelineState) -> Tuple[str, ...]:
    return _backend_current_price_tickers_for_state(state)


@st.cache_data(show_spinner=False)
def get_cached_current_prices(
    price_session_id: str,
    tickers_key: Tuple[str, ...],
    price_refresh_counter: Any,
) -> Tuple[Dict[str, float], List[str], Dict[str, int], str]:
    live_prices, stock_price_errors, stock_summary = fetch_current_prices_yf(list(tickers_key))
    price_updated_at = datetime.now().strftime("%H:%M:%S")
    return live_prices, stock_price_errors, stock_summary, price_updated_at


def _issues_without_current_price_messages(issues: List[str]) -> List[str]:
    return _backend_issues_without_current_price_messages(issues)


def apply_live_price_overlay(base_state: PipelineState, price_refresh_token=0) -> PipelineState:
    tickers_key = _current_price_tickers_for_state(base_state)
    if isinstance(price_refresh_token, tuple) and len(price_refresh_token) == 2:
        price_session_id, price_refresh_counter = price_refresh_token
    else:
        price_session_id, price_refresh_counter = "legacy", price_refresh_token
    live_prices, stock_price_errors, stock_summary, price_updated_at = get_cached_current_prices(
        str(price_session_id),
        tickers_key,
        price_refresh_counter,
    )
    return _backend_apply_live_price_overlay(
        base_state,
        live_prices,
        stock_price_errors,
        stock_summary,
        price_updated_at,
    )


def _get_pipeline_reload_token() -> int:
    return int(st.session_state.get(PIPELINE_RELOAD_TOKEN_KEY, 0) or 0)


def _increment_pipeline_reload_token() -> int:
    token = _get_pipeline_reload_token() + 1
    st.session_state[PIPELINE_RELOAD_TOKEN_KEY] = token
    return token


def _get_price_session_id() -> str:
    session_id = st.session_state.get(PRICE_SESSION_ID_KEY)
    if not session_id:
        session_id = uuid.uuid4().hex
        st.session_state[PRICE_SESSION_ID_KEY] = session_id
    return str(session_id)


def _get_explicit_price_refresh_token() -> int:
    return int(st.session_state.get(PRICE_REFRESH_TOKEN_KEY, 0) or 0)


def _increment_price_refresh_token() -> int:
    token = _get_explicit_price_refresh_token() + 1
    st.session_state[PRICE_REFRESH_TOKEN_KEY] = token
    return token


def _get_price_refresh_cache_token(
    as_of: date,
    include_unrealized_current_year: bool,
    selected_sheets: List[str],
) -> Tuple[str, int]:
    return (_get_price_session_id(), _get_explicit_price_refresh_token())


def _format_price_status(state: PipelineState) -> str:
    summary = state.price_summary or {}
    requested = int(summary.get("stocks_requested", 0) or 0)
    fetched = int(summary.get("stocks_fetched", 0) or 0)
    updated_at = state.price_updated_at or "n/a"
    return f"Prices updated: {updated_at} · {fetched}/{requested} tickers priced"


def _render_price_refresh_button(key: str) -> None:
    if st.button("Refresh prices", key=key):
        _increment_price_refresh_token()
        _rerun_app()


def _sanitize_sheet_defaults(defaults: List[str], available_sheets: List[str], fallback_sheets: List[str]) -> List[str]:
    selected_defaults = [sheet for sheet in (defaults or []) if sheet in available_sheets]
    if selected_defaults:
        return selected_defaults
    return [sheet for sheet in fallback_sheets if sheet in available_sheets]


def _render_config_tab(available_sheets: List[str], default_sheets: List[str], source_mode: str) -> None:
    st.markdown("##### Data source")
    col_refresh, col_status = st.columns([1, 2])
    with col_refresh:
        if st.button("Refresh data / Rebuild pipeline", key="refresh_rebuild_pipeline"):
            _increment_pipeline_reload_token()
            _clear_data_caches()
            _rerun_app()
        st.caption("Clears cached source and pipeline data. Press after source edits/imports to fetch fresh data and rebuild.")
    with col_status:
        if source_mode == DATA_SOURCE_IBKR:
            st.caption(f"Source: {IBKR_SOURCE_LABEL}")
            st.caption("Configured by `OPTIONS_DATA_SOURCE=ibkr`; Google Sheet tab selection is disabled in this mode.")
            st.session_state["selected_sheets"] = [IBKR_SOURCE_LABEL]
        else:
            _render_data_status(SHEET_ID)
    if source_mode == DATA_SOURCE_IBKR:
        st.selectbox(
            "Data source",
            options=available_sheets,
            index=0,
            key="ibkr_source_display",
            disabled=True,
        )
    else:
        selected_defaults = _sanitize_sheet_defaults(
            st.session_state.get("selected_sheets", default_sheets) or [],
            available_sheets,
            default_sheets,
        )
        selected_sheets = st.multiselect(
            "Sheets to include (Options YYYY):",
            options=available_sheets,
            default=selected_defaults,
            key="selected_sheets",
        )
        if not selected_sheets:
            st.warning("Select at least one sheet to run the dashboard.")
        st.caption("Any sheet named like `Options 2022`, `Options 2023`, etc., can be included.")


def _render_snapshot(
    col_main,
    state: PipelineState,
    include_unrealized: bool,
    unrealized_blocked: bool,
    ytd_total: float,
    realized_total: float,
    as_of_year: int,
    capital_history_affected_years: set,
    ytd_twr,
    missing_required_price_tickers: List[str],
    capital_history_incomplete: bool,
    capital_history_coverage_issues: List[Dict[str, Any]],
    dividend_warning_note: Optional[str],
    issues: List[str],
    price_errors: List[str],
    price_summary: Dict[str, int],
) -> None:
    with col_main:
        st.markdown("#### Portfolio Snapshot")
        price_col, price_status_col = st.columns([1, 4])
        with price_col:
            _render_price_refresh_button("refresh_prices_snapshot")
        with price_status_col:
            st.caption(_format_price_status(state))
        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            total_pnl_value = "n/a" if include_unrealized and unrealized_blocked else f"${ytd_total:,.0f}"
            metric_card("YTD Total P&L", total_pnl_value, delta=None)
        with mc2:
            metric_card("YTD Realized P&L (w/ div)", f"${realized_total:,.0f}")
        with mc3:
            unrealized_snapshot_value = (
                "incomplete"
                if unrealized_blocked
                else f"${state.total_unreal:,.0f} (opt ${state.option_unreal:,.0f} / stk ${state.stock_unreal:,.0f})"
            )
            metric_card(
                CURRENT_UNREALIZED_SNAPSHOT_LABEL,
                unrealized_snapshot_value,
            )
        with mc4:
            unrealized_adjusted_twr_value = (
                "n/a"
                if (include_unrealized and unrealized_blocked) or (as_of_year in capital_history_affected_years)
                else f"{float(ytd_twr):.1%}" if pd.notna(ytd_twr) else "n/a"
            )
            metric_card(
                UNREALIZED_ADJUSTED_TWR_LABEL if include_unrealized else "YTD Annualized TWR",
                unrealized_adjusted_twr_value,
            )
        if unrealized_blocked and missing_required_price_tickers:
            st.warning(
                "Current unrealized snapshot is incomplete: missing required prices for "
                + ", ".join(missing_required_price_tickers)
                + ". Unrealized-adjusted totals and TWR are suppressed."
            )
        if capital_history_incomplete and capital_history_coverage_issues:
            coverage_summary = "; ".join(
                f"{item['ticker']} ({pd.to_datetime(item['start_date']).date()} to {pd.to_datetime(item['end_date']).date()})"
                for item in capital_history_coverage_issues
            )
            st.warning(
                "Historical capital price coverage is incomplete for "
                + coverage_summary
                + ". RoAC/RoPC/TWR-based metrics are suppressed for affected periods."
            )
        if dividend_warning_note:
            st.warning(dividend_warning_note)
    render_issue_status_banner(issues, price_errors, price_summary)
    st.divider()


def _render_yearly_tab(
    state: PipelineState,
    yearly: pd.DataFrame,
    include_unrealized: bool,
    dividend_warning_note: Optional[str],
    covered_period_note: Optional[str],
    capital_history_incomplete: bool,
    monthly_returns_covered: pd.Series,
    return_series_truncated: bool,
) -> None:
    # Comprehensive Yearly Performance (Realized View)
    st.markdown("##### Comprehensive Yearly Performance (Realized View)")
    if dividend_warning_note:
        st.warning(dividend_warning_note)
    realized_cols = [
        "year",
        "realized_options_pnl",
        "realized_stock_pnl",
        "dividends",
        "total_realized_pnl",
        "avg_capital",
        "peak_capital",
        "roac_year",
        "ropc_year",
        "ann_roac",
        "ann_ropc",
        "annualized_return_twr",
        "annualized_return_twr_active",
    ]
    realized_map = {
        "year": "Year",
        "realized_options_pnl": "Options P&L",
        "realized_stock_pnl": "Stock P&L",
        "dividends": "Dividends",
        "total_realized_pnl": "Realized P&L",
        "avg_capital": "Avg capital",
        "peak_capital": "Peak capital",
        "roac_year": "RoAC",
        "ropc_year": "RoPC",
        "ann_roac": "Ann. RoAC",
        "ann_ropc": "Ann. RoPC",
        "annualized_return_twr": "Ann. TWR",
        "annualized_return_twr_active": "Ann. TWR (active)",
    }
    realized_display = yearly[[c for c in realized_cols if c in yearly.columns]].rename(columns=realized_map)
    st.dataframe(
        _format_df(
            realized_display.reset_index(drop=True),
            currency_cols=["Options P&L", "Stock P&L", "Dividends", "Realized P&L", "Avg capital", "Peak capital"],
            pct_cols=["RoAC", "RoPC", "Ann. RoAC", "Ann. RoPC", "Ann. TWR", "Ann. TWR (active)"],
            int_cols=["Year"],
            hide_index=True,
        ),
        width="stretch",
    )

    # Comprehensive Yearly Performance (unrealized-adjusted view)
    st.markdown(UNREALIZED_ADJUSTED_VIEW_LABEL)
    st.caption(UNREALIZED_ADJUSTED_EXPLANATION)
    unrealized_adjusted_cols = [
        "year",
        "total_realized_pnl",
        "total_pnl_incl_unreal",
        "ann_roac",
        "ann_ropc",
        "annualized_return_twr",
        "annualized_return_twr_unrealized_adjusted",
    ]
    unrealized_adjusted_map = {
        "year": "Year",
        "total_realized_pnl": "Realized P&L",
        "total_pnl_incl_unreal": UNREALIZED_ADJUSTED_TOTAL_LABEL,
        "ann_roac": "Ann. return on avg",
        "ann_ropc": "Ann. return on peak",
        "annualized_return_twr": "Ann. TWR",
        "annualized_return_twr_unrealized_adjusted": UNREALIZED_ADJUSTED_TWR_COLUMN_LABEL,
    }
    unrealized_adjusted_source = state["yearly_with_unreal"] if include_unrealized else state["yearly"]
    unrealized_adjusted_display = unrealized_adjusted_source[
        [c for c in unrealized_adjusted_cols if c in unrealized_adjusted_source.columns]
    ].rename(columns=unrealized_adjusted_map)
    st.dataframe(
        _format_df(
            unrealized_adjusted_display.reset_index(drop=True),
            currency_cols=["Realized P&L", UNREALIZED_ADJUSTED_TOTAL_LABEL],
            pct_cols=["Ann. return on avg", "Ann. return on peak", "Ann. TWR", UNREALIZED_ADJUSTED_TWR_COLUMN_LABEL],
            int_cols=["Year"],
            hide_index=True,
        ),
        width="stretch",
    )

    # Expectancy Analysis
    st.markdown("##### Expectancy Analysis")
    exp_df = expectancies(state.get("realized_option_events", []), state.get("realized_sales", []), state["monthly_cycles"], state.get("chain_outcomes", []))
    st.dataframe(
        _format_df(
            exp_df,
            currency_cols=["Avg win", "Avg loss", "Expectancy", "Total P&L"],
            pct_cols=["Win rate"],
            int_cols=["Count"],
            hide_index=True,
        ),
        width="stretch",
    )

    # Benchmark metrics
    st.markdown("##### Key Performance Metrics (vs. Benchmarks)")
    st.caption("Sortino is unavailable when there are no downside months versus the monthly risk-free hurdle.")
    bench_df = state.get("benchmark_metrics", pd.DataFrame())
    if covered_period_note:
        st.info(covered_period_note)
    if capital_history_incomplete and monthly_returns_covered.empty:
        st.info("Return-based strategy metrics are unavailable because no complete covered comparison period exists.")
    elif not bench_df.empty:
        bench_display = bench_df.copy()
        bench_display = bench_display.rename(columns={
            "CAGR": "CAGR",
            "Volatility": "Volatility",
            "Sharpe": "Sharpe",
            "Sortino": "Sortino",
            "Max Drawdown": "Max drawdown",
            "Return 3M": "Return 3M",
            "Return 6M": "Return 6M",
            "Return YTD": "Return YTD",
            "Return 1Y": "Return 1Y",
            "Return SI": "Return SI",
        })
        st.dataframe(
            _format_df(
                bench_display,
                pct_cols=["CAGR", "Volatility", "Max drawdown", "Return 3M", "Return 6M", "Return YTD", "Return 1Y", "Return SI"],
                float_cols=["Sharpe", "Sortino"],
                hide_index=True,
                na_rep="n/a",
            ),
            width="stretch",
        )
    else:
        st.info("Benchmark data unavailable (yfinance fetch failed).")

    # Charts
    st.markdown("##### Charts")
    range_options = ["3M", "6M", "YTD", "1Y", "Since inception"]
    range_choice = st.radio("Range", range_options, index=range_options.index("YTD"), key="chart_range", horizontal=True)
    if return_series_truncated and covered_period_note:
        st.info(covered_period_note)

    if capital_history_incomplete and monthly_returns_covered.empty:
        st.info("Return-based charts are unavailable because no complete covered return period exists.")
    else:
        eq_df = build_benchmark_growth_chart_data(
            monthly_returns_covered,
            state.get("aligned_bench_returns", {}),
            range_choice,
            state["as_of"],
        )
        if not eq_df.empty:
            eq_df = eq_df.sort_values(["Series", "Date"])
            if not eq_df.empty:
                g_min = float(eq_df["Growth"].min())
                g_max = float(eq_df["Growth"].max())
                pad = (g_max - g_min) * 0.1 if g_max > g_min else 0.05
                y_domain = [g_min - pad, g_max + pad]
                chart = (
                    alt.Chart(eq_df)
                    .mark_line()
                    .encode(
                        x=alt.X("Date:T", title="Date"),
                        y=alt.Y("Growth:Q", title="Cumulative growth of $1", scale=alt.Scale(domain=y_domain, nice=True)),
                        color=alt.Color("Series:N", title="Series"),
                        tooltip=["Date:T", "Series:N", alt.Tooltip("Growth:Q", format=".3f")],
                    )
                    .properties(height=260, title="Cumulative Growth vs Benchmarks")
                )
                st.altair_chart(chart, width="stretch")

    # P&L by options cycle
    pnl_df = build_options_cycle_chart_data(state["monthly_cycles"])
    if not pnl_df.empty:
        pnl_df = filter_df_to_range(pnl_df, "Date", state["as_of"], range_choice)
        if not pnl_df.empty:
            bar = (
                alt.Chart(pnl_df)
                .mark_bar()
                .encode(
                    x=alt.X("Date:T", title="Option cycle"),
                    y=alt.Y("pnl:Q", title="P&L ($)"),
                    color=alt.Color("color:N", scale=alt.Scale(domain=["Positive", "Negative"], range=["#22c55e", "#ef4444"]), legend=None),
                    tooltip=["Date:T", alt.Tooltip("pnl:Q", format=",.0f")],
                )
                .properties(height=260, title="P&L by Options Cycle")
            )
            st.altair_chart(bar, width="stretch")

    # Monthly return line (strategy only)
    if not monthly_returns_covered.empty:
        ret_df = pd.DataFrame({"Date": monthly_returns_covered.index, "Return": monthly_returns_covered.values})
        ret_df = filter_df_to_range(ret_df, "Date", state["as_of"], range_choice)
        if not ret_df.empty:
            y_min = float(ret_df["Return"].min())
            y_max = float(ret_df["Return"].max())
            pad = (y_max - y_min) * 0.1 if y_max > y_min else 0.01
            y_domain = [y_min - pad, y_max + pad]
            bands = pd.DataFrame(
                [
                    {"band": "negative", "y0": min(y_min, 0.0) - pad, "y1": 0.0},
                    {"band": "neutral", "y0": 0.0, "y1": 0.015},
                    {"band": "positive", "y0": 0.015, "y1": max(y_max, 0.02) + pad},
                ]
            )
            band_colors = alt.Color(
                "band:N",
                scale=alt.Scale(
                    domain=["negative", "neutral", "positive"],
                    range=["#ef444480", "#facc1580", "#22c55e80"],
                ),
                legend=None,
            )
            x_min = pd.to_datetime(ret_df["Date"]).min()
            x_max = pd.to_datetime(ret_df["Date"]).max()
            x_domain = [x_min, x_max]
            bands_chart = (
                alt.Chart(bands.assign(Date=x_min, Date2=x_max))
                .mark_rect()
                .encode(
                    x=alt.X("Date:T", title="Date", scale=alt.Scale(domain=x_domain)),
                    x2="Date2:T",
                    y="y0:Q",
                    y2="y1:Q",
                    color=band_colors,
                )
            )
            line_chart = (
                alt.Chart(ret_df)
                .mark_line(point=True, color="#60a5fa")
                .encode(
                    x=alt.X("Date:T", title="Date", scale=alt.Scale(domain=x_domain)),
                    y=alt.Y("Return:Q", title="Monthly return", axis=alt.Axis(format="%", grid=True), scale=alt.Scale(domain=y_domain)),
                    tooltip=["Date:T", alt.Tooltip("Return:Q", format=".2%")],
                )
            )
            ret_chart = alt.layer(bands_chart, line_chart).properties(height=220, title="Monthly Returns (RoAC)")
            st.altair_chart(ret_chart, width="stretch")
    elif capital_history_incomplete:
        st.info("No complete monthly return segment is available to chart.")


def _render_monthly_tab(
    monthly_cycles: pd.DataFrame,
    monthly_returns_covered: pd.Series,
    dividend_warning_note: Optional[str],
    return_series_truncated: bool,
    covered_period_note: Optional[str],
    capital_history_incomplete: bool,
) -> None:
    st.markdown("##### Monthly performance (calendar months)")
    if dividend_warning_note:
        st.warning(dividend_warning_note)
    col_map = {
        "index": "Month",
        "month": "Month",
        "realized_options_pnl": "Options P&L",
        "realized_stock_pnl": "Stock P&L",
        "dividends": "Dividends",
        "total_realized_pnl": "Total P&L (w/ div)",
        "avg_capital": "Avg capital",
        "peak_capital": "Peak capital",
        "roac": "Return (RoAC)",
        "ropc": "Return (RoPC)",
    }
    show_cols = ["Month", "Options P&L", "Stock P&L", "Dividends", "Total P&L (w/ div)", "Avg capital", "Peak capital", "Return (RoAC)", "Return (RoPC)"]
    monthly_table = monthly_cycles.reset_index().rename(columns=col_map)
    if "Month" in monthly_table.columns:
        monthly_table["Month"] = pd.to_datetime(monthly_table["Month"]).dt.strftime("%Y-%m-%d")
    monthly_table = monthly_table[[c for c in show_cols if c in monthly_table.columns]]
    st.dataframe(
        _format_df(
            monthly_table,
            currency_cols=["Options P&L", "Stock P&L", "Dividends", "Total P&L (w/ div)", "Avg capital", "Peak capital"],
            pct_cols=["Return (RoAC)", "Return (RoPC)"],
            hide_index=True,
        ),
        width="stretch",
    )
    if return_series_truncated and covered_period_note:
        st.info(covered_period_note)
    if capital_history_incomplete and monthly_returns_covered.empty:
        st.info("Cumulative return chart is unavailable because no complete covered return period exists.")
    elif not monthly_returns_covered.empty:
        equity_curve = (1 + monthly_returns_covered).cumprod()
        curve_df = pd.DataFrame(
            {
                "Month": monthly_returns_covered.index,
                "Growth": equity_curve.values,
            }
        )
        y_min = float(curve_df["Growth"].min() * 0.98)
        y_max = float(curve_df["Growth"].max() * 1.02)
        curve_chart = (
            alt.Chart(curve_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Month:T", title="Month"),
                y=alt.Y("Growth:Q", title="Cumulative growth of $1", scale=alt.Scale(domain=[y_min, y_max], nice=True)),
                tooltip=["Month:T", alt.Tooltip("Growth:Q", format=".3f")],
            )
            .properties(height=260, title="Cumulative growth by month")
        )
        st.altair_chart(curve_chart, width="stretch")


def _render_ticker_tab(state: PipelineState, unrealized_blocked: bool) -> None:
    st.markdown("##### Per-ticker P&L (realized)")
    realized_map = {
        "year": "Year",
        "ticker": "Ticker",
        "options_pnl": "Options P&L",
        "stock_realized_pnl": "Stock P&L",
        "combined_realized": "Total realized P&L",
    }
    realized_df = state["per_ticker"].copy()
    if not realized_df.empty:
        realized_df = (
            realized_df.groupby(["year", "ticker"], as_index=False)[["options_pnl", "stock_realized_pnl", "combined_realized"]]
            .sum()
            .rename(columns=realized_map)
        )
    else:
        realized_df = realized_df.rename(columns=realized_map)
    st.dataframe(
        _format_df(
            realized_df,
            currency_cols=["Options P&L", "Stock P&L", "Total realized P&L"],
            int_cols=["Year"],
            hide_index=True,
        ),
        width="stretch",
    )
    totals_df = state.get("per_ticker_totals", pd.DataFrame())
    if unrealized_blocked:
        st.info("Per-ticker realized + unrealized totals are suppressed because the current unrealized snapshot is incomplete.")
    elif not totals_df.empty:
        st.markdown("##### Per-ticker P&L (realized + unrealized)")
        totals_map = {
            "ticker": "Ticker",
            "options_pnl": "Options P&L",
            "stock_realized_pnl": "Stock P&L",
            "combined_realized": "Total realized P&L",
            "unrealized_pnl": "Unrealized P&L",
            "total_pnl": "Total P&L",
        }
        totals_display = totals_df.rename(columns=totals_map)
        st.dataframe(
            _format_df(
                totals_display,
                currency_cols=["Options P&L", "Stock P&L", "Total realized P&L", "Unrealized P&L", "Total P&L"],
                hide_index=True,
            ),
            width="stretch",
        )


def _highlight_short_option_price(row: pd.Series):
    """Color-code short options by moneyness bands."""
    styles = [""] * len(row)
    try:
        moneyness = pd.to_numeric(row.get("Moneyness %"), errors="coerce")
        style = ""
        if pd.notna(moneyness):
            if moneyness > 0:
                style = "background-color: rgba(220, 38, 38, 0.50); color: #ffffff;"
            elif -0.01 <= moneyness <= 0:
                style = "background-color: rgba(249, 115, 22, 0.45); color: #111827;"
            elif -0.05 <= moneyness < -0.01:
                style = "background-color: rgba(250, 204, 21, 0.45); color: #111827;"
            elif moneyness < -0.10:
                style = "background-color: rgba(37, 99, 235, 0.40); color: #ffffff;"
        if style:
            for col in ("Current price", "Moneyness %"):
                if col in row.index:
                    styles[row.index.get_loc(col)] = style
    except Exception:
        pass
    return styles


def build_open_options_positions_frame(open_options: pd.DataFrame, stock_prices: Dict[str, float]) -> pd.DataFrame:
    return build_open_option_shorts_frame(open_options, stock_prices)


def _render_positions_tab(state: PipelineState) -> None:
    col_refresh, col_status = st.columns([1, 4])
    with col_refresh:
        _render_price_refresh_button("refresh_prices_positions")
    with col_status:
        st.caption(_format_price_status(state))
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("##### Assigned holdings (inventory)")
        inv_df = build_assigned_holdings_frame(state["inv_df"])
        if "buy_date" in inv_df.columns:
            inv_df["buy_date"] = pd.to_datetime(inv_df["buy_date"]).dt.strftime("%Y-%m-%d")
        inv_df = inv_df.rename(
            columns={
                "ticker": "Ticker",
                "buy_date": "Buy date",
                "shares": "Shares",
                "cost_per_share": "Cost/share",
                "current_price": "Current price",
                "covered_shares": "Covered shares",
                "covered_strike": "Covered strike",
                "unrealized_pnl": "Unrealized P&L",
                "source": "Source",
            }
        )
        st.dataframe(
            _format_df(
                inv_df,
                currency_cols=["Unrealized P&L"],
                float_cols=["Cost/share", "Current price", "Covered strike"],
                int_cols=["Shares", "Covered shares"],
            ),
            width="stretch",
        )
    with c2:
        st.markdown("##### Open option shorts")
        if state["open_options"].empty:
            st.info("No open short options.")
        else:
            oo = build_open_options_positions_frame(state["open_options"], state.get("stock_prices") or {})
            oo = oo[["ticker", "type", "strike", "current_price", "moneyness_pct", "qty", "expiration", "trans_date", "open_price"]].copy()
            for dcol in ["expiration", "trans_date"]:
                if dcol in oo.columns:
                    oo[dcol] = pd.to_datetime(oo[dcol]).dt.strftime("%Y-%m-%d")
            oo = oo.rename(
                columns={
                    "ticker": "Ticker",
                    "type": "Type",
                    "strike": "Strike",
                    "current_price": "Current price",
                    "moneyness_pct": "Moneyness %",
                    "qty": "Qty",
                    "expiration": "Expiration",
                    "trans_date": "Opened",
                    "open_price": "Open price",
                }
            )
            st.caption("Color key (short-option moneyness): red > 0%; orange -1% to 0%; yellow -5% to -1%; none -10% to -5%; blue < -10%.")
            st.dataframe(
                _format_df(
                    oo,
                    pct_cols=["Moneyness %"],
                    float_cols=["Strike", "Open price", "Current price"],
                    int_cols=["Qty"],
                ).apply(_highlight_short_option_price, axis=1),
                width="stretch",
            )


def _render_logs_tab(
    state: PipelineState,
    issues: List[str],
    price_errors: List[str],
    price_summary: Dict[str, int],
    unrealized_blocked: bool,
    capital_history_incomplete: bool,
    capital_history_coverage_issues: List[Dict[str, Any]],
    dividend_coverage_complete: bool,
    dividend_errors: List[str],
    dividend_warning_note: Optional[str],
) -> None:
    st.markdown("##### Data source / connectivity issues")
    st.write(f"Build version: {APP_BUILD_VERSION}")
    if data_source_mode() == DATA_SOURCE_IBKR:
        st.caption(
            "Active source: `IBKR Flex` from Firestore. Price history, imported IBKR rows, and audit diagnostics are backend-owned."
        )
    else:
        st.caption(
            "Secrets key used for Google Sheets mode: `GOOGLE_SERVICE_ACCOUNT_JSON`. Public sheets load without credentials; "
            "private sheets need a service account. Offline fallback for Sheets mode: set env "
            "`LOCAL_EXCEL_PATH=/full/path/to/IBKR_Portfolio_sheets.xlsx` when running locally."
        )
    actionable_issues, audit_issues = split_actionable_and_audit_issues(issues)
    coverage_problem = price_summary and (
        price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
    )
    if actionable_issues or price_errors or coverage_problem or (not dividend_coverage_complete):
        if actionable_issues:
            st.warning("Actionable issues:")
            st.dataframe(pd.DataFrame({"message": actionable_issues}), width="stretch")
        if price_summary:
            st.write("Price fetch coverage:")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "asset": "stocks",
                            "requested": price_summary.get("stocks_requested", 0),
                            "fetched": price_summary.get("stocks_fetched", 0),
                        },
                    ]
                ),
                width="stretch",
            )
        historical_price_summary = state.get("historical_price_summary", {})
        if historical_price_summary:
            st.write("Historical price fetch coverage:")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "asset": "historical_stocks",
                            "requested": historical_price_summary.get("requested", 0),
                            "fetched": historical_price_summary.get("fetched", 0),
                        },
                    ]
                ),
                width="stretch",
            )
        if price_errors:
            st.write("Price fetch issues:")
            st.dataframe(pd.DataFrame({"error": price_errors}), width="stretch")
        historical_price_errors = state.get("historical_price_errors", [])
        if historical_price_errors:
            st.write("Historical price fetch issues:")
            st.dataframe(pd.DataFrame({"error": historical_price_errors}), width="stretch")
        if unrealized_blocked:
            st.info("Unrealized P&L and related metrics were suppressed due to missing prices.")
        if capital_history_incomplete and capital_history_coverage_issues:
            coverage_df = pd.DataFrame(capital_history_coverage_issues).rename(
                columns={
                    "ticker": "ticker",
                    "start_date": "start_date",
                    "end_date": "end_date",
                    "reason": "reason",
                }
            )
            st.write("Historical capital price coverage issues:")
            st.dataframe(coverage_df, width="stretch")
            st.info("RoAC, RoPC, annualized return metrics, and return-based charts were suppressed for affected periods.")
        dividend_summary = state.get("dividend_summary", {})
        if dividend_summary:
            st.write("Dividend fetch coverage:")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "asset": "dividends",
                            "attempted_tickers": dividend_summary.get("attempted", 0),
                            "failed_tickers": dividend_summary.get("failed", 0),
                        }
                    ]
                ),
                width="stretch",
            )
        if dividend_errors:
            st.write("Dividend fetch issues:")
            st.dataframe(pd.DataFrame({"error": dividend_errors}), width="stretch")
        if dividend_warning_note:
            st.info(dividend_warning_note)
    else:
        st.success("No actionable issues detected.")
    if audit_issues:
        st.info("Wheel audit notes: expected IBKR exclusions that are not counted as data-health warnings.")
        with st.expander(f"Show {len(audit_issues)} audit notes", expanded=False):
            st.dataframe(pd.DataFrame({"message": audit_issues}), width="stretch")
    if state.get("stock_prices"):
        st.write("Stock prices used:")
        st.dataframe(
            pd.DataFrame(
                [{"ticker": k, "price": v} for k, v in state["stock_prices"].items()]
            ).sort_values("ticker"),
            width="stretch",
        )
    if state.get("advanced_unreal") is not None and not getattr(state.get("advanced_unreal"), "empty", True):
        st.write("Unrealized by ticker (options/stocks):")
        adv_df = state["advanced_unreal"].reset_index()
        adv_df.columns = ["ticker", "unrealized_pnl"]
        st.dataframe(_format_df(adv_df, currency_cols=["unrealized_pnl"]), width="stretch")
    if state.get("sheet_counts") is not None:
        st.markdown("##### Loaded rows by data source")
        st.dataframe(state["sheet_counts"], width="stretch")
    if data_source_mode() == DATA_SOURCE_IBKR:
        st.markdown("##### IBKR reconciliation notes")
        st.caption("Latest documented sheet-vs-IBKR reconciliation found no accounting blockers before switching Streamlit.")
        st.write(
            pd.DataFrame(
                [
                    {"case": "FTNT", "status": "Matched", "explanation": "Open 95C expiring 2026-09-18 caps assigned holding at 100 covered shares."},
                    {"case": "CCJ/NVDA April", "status": "Expected difference", "explanation": "IBKR uses lifecycle-date roll/close recognition; sheet periodization differs."},
                    {"case": "AAPL April", "status": "Expected difference", "explanation": "IBKR realized assignment economics in prior roll events instead of April assignment date."},
                    {"case": "SPY/ABR", "status": "Excluded", "explanation": "Excluded from wheel P&L by accounting rules."},
                    {"case": "ZM monthly premium", "status": "Matched semantics", "explanation": "Incremental open premium is separate from roll-adjusted display premium."},
                ]
            )
        )
    st.markdown("---")
    st.markdown("##### Debug / raw data")
    st.write("Options raw", state["df_opts"].head())
    st.write("Capital daily tail", state["capital_daily"].tail())
    st.write("Dividends", state["div_df"].head())


def _render_methodology_tab() -> None:
    st.markdown("##### How we compute the numbers")
    st.markdown(
        """
**Scope & sources**
- Data source: Google Sheets mode uses whichever `Options YYYY` tabs you pick in Config; IBKR mode uses the imported Flex data source. Rows outside the active source are ignored.
- Actions processed: `Sell/Buy` (plus `Bought` → `Buy`). Blank rows are skipped.

**Capital & P&L**
- Capital base: short-put reserve at strike*100*contracts; shares marked to latest close if fetched, else cost. Open options are not fully marked to market.
- Realized P&L: option premia + stock sales + dividends.
- Current unrealized snapshot: stock unrealized plus the dashboard's current heuristic option treatment. If enabled, that snapshot is added to current-year totals and the unrealized-adjusted TWR summary.

**Returns**
- Monthly returns (RoAC/RoPC) = monthly realized P&L ÷ monthly avg/peak capital (calendar months).
- Annualized TWR (realized) = geometric product of monthly returns; “active” drops months with zero option P&L.
- Unrealized-adjusted TWR applies the current unrealized snapshot to the current month for the current-year summary. Growth charts remain based on realized returns.

**Assignments & inventory**
- Put rows marked “assigned” create stock lots; covered-call assignments reduce inventory FIFO. If quantities or flags don’t line up, calls/puts can appear unmatched.

**Benchmarks & prices**
- Benchmarks from yfinance monthly prices; missing prices fall back to last available or cost. Coverage issues surface in Logs.

**Limitations / edge cases**
- No true option MTM; no external deposit/withdrawal modeling.
- Date parsing depends on source date fields being parseable; bad rows go to Issues.
- Mixed legs (“Put/Call”, “Call/Put”) infer the short leg via type/comment heuristics.
        """
    )


def _load_dashboard_state_for_ui(
    *,
    as_of_input: date,
    include_unrealized: bool,
    selected_sheets: List[str],
    source_mode: str,
) -> Tuple[PipelineState, Any]:
    reload_token = _get_pipeline_reload_token()
    pipeline_cache_key = build_pipeline_cache_key(
        as_of_input,
        include_unrealized,
        selected_sheets,
        reload_token,
        source_mode=source_mode,
    )
    price_refresh_token = _get_price_refresh_cache_token(as_of_input, include_unrealized, selected_sheets)
    if source_mode == DATA_SOURCE_IBKR:
        with st.spinner("Building portfolio pipeline..."):
            base_state = get_cached_ibkr_pipeline_resource(*pipeline_cache_key)
    else:
        base_state = get_cached_pipeline(*pipeline_cache_key)
    priced_state = apply_live_price_overlay(base_state, price_refresh_token)
    state = apply_unrealized_adjusted_display(priced_state, include_unrealized)
    return state, build_dashboard_view_model(state, include_unrealized)


def _render_ibkr_app(source_mode: str) -> None:
    st.title("Options ROI Dashboard")
    st.caption("Live from IBKR Flex with Streamlit")

    prefs = load_prefs()
    top_left, top_mid, top_right = st.columns([1.2, 1.4, 2.4])
    with top_left:
        as_of_input = st.date_input("As of date", value=date.today(), key="ibkr_as_of")
    with top_mid:
        include_unrealized = st.checkbox(
            "Include current unrealized snapshot",
            value=bool(prefs.get("include_unrealized", True)),
            key="include_unrealized",
        )
    with top_right:
        st.caption("Source: IBKR Flex")
        st.caption("Google Sheet selection is disabled in IBKR mode. Historical prices and IBKR rows are read from Firestore.")

    selected_sheets = [IBKR_SOURCE_LABEL]
    new_prefs = {
        "include_unrealized": bool(st.session_state.get("include_unrealized", False)),
        "selected_sheets": prefs.get("selected_sheets", []),
    }
    if new_prefs != prefs:
        save_prefs(new_prefs)

    try:
        state, view_model = _load_dashboard_state_for_ui(
            as_of_input=as_of_input,
            include_unrealized=include_unrealized,
            selected_sheets=selected_sheets,
            source_mode=source_mode,
        )
    except Exception as e:
        st.error(
            "Could not load IBKR Flex data from Firestore. Confirm Streamlit secrets include "
            "`GOOGLE_SERVICE_ACCOUNT_JSON` or `FIRESTORE_SERVICE_ACCOUNT_JSON`, and that "
            "`FIRESTORE_PROJECT_ID` is set to `options-performance-dashboard`. "
            f"Details: {e}"
        )
        st.stop()

    _render_snapshot(
        st.container(),
        state,
        include_unrealized,
        view_model.unrealized_blocked,
        view_model.ytd_total,
        view_model.realized_total,
        view_model.as_of_year,
        view_model.capital_history_affected_years,
        view_model.ytd_twr,
        view_model.missing_required_price_tickers,
        view_model.capital_history_incomplete,
        view_model.capital_history_coverage_issues,
        view_model.dividend_warning_note,
        view_model.issues,
        view_model.price_errors,
        view_model.price_summary,
    )

    tabs = ["Dashboard", "Monthly", "Tickers", "Positions", "Diagnostics", "Methodology"]
    tab_dashboard, tab_monthly, tab_ticker, tab_positions, tab_logs, tab_method = st.tabs(tabs)

    with tab_dashboard:
        _render_yearly_tab(
            state,
            view_model.yearly,
            include_unrealized,
            view_model.dividend_warning_note,
            view_model.covered_period_note,
            view_model.capital_history_incomplete,
            view_model.monthly_returns_covered,
            view_model.return_series_truncated,
        )

    with tab_monthly:
        _render_monthly_tab(
            view_model.monthly_cycles,
            view_model.monthly_returns_covered,
            view_model.dividend_warning_note,
            view_model.return_series_truncated,
            view_model.covered_period_note,
            view_model.capital_history_incomplete,
        )

    with tab_ticker:
        _render_ticker_tab(state, view_model.unrealized_blocked)

    with tab_positions:
        _render_positions_tab(state)

    with tab_logs:
        col_refresh, col_status = st.columns([1, 3])
        with col_refresh:
            if st.button("Rebuild cached data", key="ibkr_rebuild_pipeline"):
                _increment_pipeline_reload_token()
                _clear_data_caches()
                _rerun_app()
        with col_status:
            st.caption(f"Build version: {APP_BUILD_VERSION}")
            st.caption(_format_price_status(state))
        _render_logs_tab(
            state,
            view_model.issues,
            view_model.price_errors,
            view_model.price_summary,
            view_model.unrealized_blocked,
            view_model.capital_history_incomplete,
            view_model.capital_history_coverage_issues,
            view_model.dividend_coverage_complete,
            view_model.dividend_errors,
            view_model.dividend_warning_note,
        )

    with tab_method:
        _render_methodology_tab()


def main():
    source_mode = streamlit_app_source_mode()
    if source_mode == DATA_SOURCE_IBKR:
        _render_ibkr_app(source_mode)
        return

    st.title("Options ROI Dashboard")
    st.caption(f"Live from {source_label_for_mode(source_mode)} with Streamlit")

    col_side, col_main = st.columns([1, 4])
    prefs = load_prefs()
    with col_side:
        as_of_input = st.date_input("As of date", value=date.today())
        include_unrealized = st.checkbox(
            UNREALIZED_ADJUSTED_TOGGLE_LABEL,
            value=bool(prefs.get("include_unrealized", False)),
            key="include_unrealized",
        )
        st.caption(UNREALIZED_ADJUSTED_EXPLANATION)

    snapshot_area = st.container()
    tabs_area = st.container()

    available_sheets = available_sources_for_mode(source_mode)
    if source_mode == DATA_SOURCE_IBKR:
        default_sheets = [IBKR_SOURCE_LABEL]
        st.session_state["selected_sheets"] = [IBKR_SOURCE_LABEL]
    else:
        saved_sheets = prefs.get("selected_sheets") or []
        saved_sheets = [s for s in saved_sheets if s in available_sheets]
        default_sheets = saved_sheets or [s for s in available_sheets if s in SHEETS] or available_sheets
        st.session_state["selected_sheets"] = st.session_state.get("selected_sheets", default_sheets)
    with tabs_area:
        tabs = ["Yearly", "Monthly cycles", "Per ticker", "Positions", "Config", "Logs / data issues", "Methodology"]
        tab_yearly, tab_monthly, tab_ticker, tab_positions, tab_config, tab_logs, tab_method = st.tabs(tabs)
        with tab_config:
            _render_config_tab(available_sheets, default_sheets, source_mode)
    selected_sheets = normalize_selected_sheets_for_mode(
        st.session_state.get("selected_sheets", default_sheets) or default_sheets,
        available_sheets,
        source_mode,
    )

    # Persist prefs if changed
    new_prefs = {
        "include_unrealized": bool(st.session_state.get("include_unrealized", False)),
        "selected_sheets": prefs.get("selected_sheets", []) if source_mode == DATA_SOURCE_IBKR else selected_sheets,
    }
    if new_prefs != prefs:
        save_prefs(new_prefs)

    reload_token = _get_pipeline_reload_token()
    pipeline_cache_key = build_pipeline_cache_key(
        as_of_input,
        include_unrealized,
        selected_sheets,
        reload_token,
        source_mode=source_mode,
    )
    price_refresh_token = _get_price_refresh_cache_token(as_of_input, include_unrealized, selected_sheets)
    try:
        base_state = get_cached_pipeline(*pipeline_cache_key)
        priced_state = apply_live_price_overlay(base_state, price_refresh_token)
        state = apply_unrealized_adjusted_display(priced_state, include_unrealized)
    except Exception as e:
        if source_mode == DATA_SOURCE_IBKR:
            st.error(
                "Could not load IBKR Flex data from Firestore. Confirm Streamlit secrets include "
                "`GOOGLE_SERVICE_ACCOUNT_JSON` or `FIRESTORE_SERVICE_ACCOUNT_JSON`, and that "
                "`FIRESTORE_PROJECT_ID` is set to `options-performance-dashboard`. "
                f"Details: {e}"
            )
        else:
            st.error(
                "Could not load data. If the sheet is private, set `GOOGLE_SERVICE_ACCOUNT_JSON` (or `LOCAL_SECRETS_PATH`); "
                "if it's public, ensure sharing is enabled. You can also set `LOCAL_EXCEL_PATH` to a local workbook. "
                f"Details: {e}"
            )
        st.stop()
    view_model = build_dashboard_view_model(state, include_unrealized)

    with snapshot_area:
        _render_snapshot(
            col_main,
            state,
            include_unrealized,
            view_model.unrealized_blocked,
            view_model.ytd_total,
            view_model.realized_total,
            view_model.as_of_year,
            view_model.capital_history_affected_years,
            view_model.ytd_twr,
            view_model.missing_required_price_tickers,
            view_model.capital_history_incomplete,
            view_model.capital_history_coverage_issues,
            view_model.dividend_warning_note,
            view_model.issues,
            view_model.price_errors,
            view_model.price_summary,
        )

    with tab_yearly:
        _render_yearly_tab(
            state,
            view_model.yearly,
            include_unrealized,
            view_model.dividend_warning_note,
            view_model.covered_period_note,
            view_model.capital_history_incomplete,
            view_model.monthly_returns_covered,
            view_model.return_series_truncated,
        )

    with tab_monthly:
        _render_monthly_tab(
            view_model.monthly_cycles,
            view_model.monthly_returns_covered,
            view_model.dividend_warning_note,
            view_model.return_series_truncated,
            view_model.covered_period_note,
            view_model.capital_history_incomplete,
        )

    with tab_ticker:
        _render_ticker_tab(state, view_model.unrealized_blocked)

    with tab_positions:
        _render_positions_tab(state)

    with tab_logs:
        _render_logs_tab(
            state,
            view_model.issues,
            view_model.price_errors,
            view_model.price_summary,
            view_model.unrealized_blocked,
            view_model.capital_history_incomplete,
            view_model.capital_history_coverage_issues,
            view_model.dividend_coverage_complete,
            view_model.dividend_errors,
            view_model.dividend_warning_note,
        )

    with tab_method:
        _render_methodology_tab()


if __name__ == "__main__":
    main()
