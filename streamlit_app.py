import io
import json
import math
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
try:
    import tomllib  # py311+
except ModuleNotFoundError:  # py3.9/3.10
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError:
        tomllib = None

import numpy as np
import pandas as pd
import requests
import streamlit as st
import altair as alt
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from data_sources import DividendFetchResult, YFinanceDividendProvider, collect_dividend_cashflows as _collect_dividend_cashflows
from reporting import build_open_options_frame, filter_df_to_range

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
CONTRACT_MULTIPLIER = 100
# Keep the original on-disk prefs path for local runs; add a home-dir fallback for rebuilds.
PREFS_PATH = Path(".streamlit_user_prefs.json")
PREFS_HOME_PATH = Path.home() / ".options_roi_prefs.json"
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


@dataclass(frozen=True)
class SheetDownload:
    content: bytes
    downloaded_at: str
    source: str
    file_name: Optional[str] = None
    file_modified_at: Optional[str] = None
    file_version: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    creds = _load_credentials()
    authed = AuthorizedSession(creds)
    url = f"https://www.googleapis.com/drive/v3/files/{sheet_id}"
    resp = authed.get(url, params={"fields": "id,name,modifiedTime,version"}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        return data
    return {}


@st.cache_data(show_spinner=False, ttl=120)
def get_drive_file_metadata(sheet_id: str) -> Dict[str, str]:
    return _fetch_drive_file_metadata(sheet_id)


@st.cache_data(show_spinner=False)
def _download_excel(sheet_id: str) -> SheetDownload:
    downloaded_at = _now_iso()
    override = os.getenv("LOCAL_EXCEL_PATH")
    if override:
        p = Path(override).expanduser()
        if not p.exists():
            raise RuntimeError(f"LOCAL_EXCEL_PATH is set but file not found: {p}")
        modified_at = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).isoformat()
        return SheetDownload(
            content=p.read_bytes(),
            downloaded_at=downloaded_at,
            source="local",
            file_name=p.name,
            file_modified_at=modified_at,
        )

    meta = {}
    try:
        meta = _fetch_drive_file_metadata(sheet_id)
    except Exception:
        meta = {}
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

    creds = None
    creds_err = None
    try:
        creds = _load_credentials()
    except Exception as exc:
        creds_err = exc

    if creds:
        authed = AuthorizedSession(creds)
        resp = authed.get(url, timeout=15)
        resp.raise_for_status()
        if not resp.content.startswith(b"PK"):
            raise RuntimeError("Google Sheets export returned non-XLSX content; check sharing settings.")
        return SheetDownload(
            content=resp.content,
            downloaded_at=downloaded_at,
            source="drive",
            file_name=meta.get("name"),
            file_modified_at=meta.get("modifiedTime"),
            file_version=meta.get("version"),
        )

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        content = resp.content
        if not content.startswith(b"PK"):
            raise RuntimeError("Public sheet export returned non-XLSX content; check sharing settings.")
    except Exception as public_exc:
        msg = (
            "Public sheet download failed and no service account credentials were found. "
            "Share the sheet publicly or set GOOGLE_SERVICE_ACCOUNT_JSON / LOCAL_SECRETS_PATH / LOCAL_EXCEL_PATH."
        )
        if creds_err:
            msg = f"{creds_err} {msg}"
        raise RuntimeError(msg) from public_exc

    return SheetDownload(
        content=content,
        downloaded_at=downloaded_at,
        source="public",
        file_name=meta.get("name"),
        file_modified_at=meta.get("modifiedTime"),
    )


@st.cache_data(show_spinner=True)
def list_option_sheets(sheet_id: str) -> List[str]:
    try:
        download = _download_excel(sheet_id)
        excel_bytes = download.content
        xls = pd.ExcelFile(io.BytesIO(excel_bytes))
        names = [n for n in xls.sheet_names if pd.notna(n) and str(n).startswith("Options ")]
        return sorted(names)
    except Exception:
        return []


@st.cache_data(show_spinner=True)
def load_options(sheet_id: str, sheets: List[str]) -> pd.DataFrame:
    download = _download_excel(sheet_id)
    excel_bytes = download.content
    frames = []
    for sh in sheets:
        bio = io.BytesIO(excel_bytes)
        raw = pd.read_excel(bio, sheet_name=sh, header=1)
        df = raw.rename(
            columns={
                "Trans date": "trans_date",
                "Tiker": "ticker",
                "Type": "type",
                "Action": "action",
                "Expiration": "expiration",
                "Strike": "strike",
                "Qty": "qty",
                "Amount": "amount",
                "Comission": "commission",
                "Total P&L": "total_pnl",
                "Assigned": "assigned_flag",
                "Comment": "comment",
            }
        )
        for d in ["trans_date", "expiration"]:
            # Accept European-style dd/mm/yyyy as well as ISO; dayfirst=True prevents dropping rows like 25/08/2025.
            df[d] = pd.to_datetime(df[d], errors="coerce", dayfirst=True).dt.tz_localize(None)
        for n in ["strike", "qty", "amount", "commission", "total_pnl"]:
            df[n] = pd.to_numeric(df[n], errors="coerce")
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["action"] = df["action"].astype(str).str.title().str.strip()
        df["action"] = df["action"].replace({"Bought": "Buy"})
        df["type"] = df["type"].astype(str).str.title().str.strip()
        df["comment"] = df["comment"].astype(str)
        if "assigned_flag" in df.columns:
            df["assigned_flag"] = pd.to_numeric(df["assigned_flag"], errors="coerce").fillna(0).astype(float)
        df["source_sheet"] = sh
        frames.append(df)
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all[df_all["action"].isin(["Sell", "Buy"])]
    return df_all


def _clear_data_caches() -> None:
    _download_excel.clear()
    list_option_sheets.clear()
    load_options.clear()
    get_drive_file_metadata.clear()


# ------------------------------------------------------------
# Domain models
# ------------------------------------------------------------
@dataclass
class OptionTrade:
    date: pd.Timestamp
    ticker: str
    otype: str  # "Put" or "Call"
    action: str  # "Sell" (open short) or "Buy" (close)
    strike: float
    expiration: pd.Timestamp
    qty: int
    price: float  # per-share net price (after commission; always positive)
    comment: str
    assigned: bool


@dataclass
class OptionLot:
    ticker: str
    otype: str
    strike: float
    qty: int
    open_date: pd.Timestamp
    expiration: pd.Timestamp
    open_price: float  # per-share net credit/debit when opened
    comment: str
    assigned: bool
    close_date: Optional[pd.Timestamp] = None
    close_price: Optional[float] = None
    close_reason: Optional[str] = None


@dataclass
class OptionPnLEvent:
    date: pd.Timestamp
    ticker: str
    otype: str
    strike: float
    qty: int
    pnl: float
    p_open: float
    p_close: float
    reason: str  # close | expiration | assignment


@dataclass
class StockTxn:
    date: pd.Timestamp
    ticker: str
    side: str  # "BUY" or "SELL"
    shares: int
    price: float
    source: str  # "Assigned"


@dataclass
class RealizedSale:
    date: pd.Timestamp
    ticker: str
    shares: int
    proceeds: float
    cost: float
    pnl: float
    source: str = ""


@dataclass
class OpenLot:
    ticker: str
    buy_date: pd.Timestamp
    shares_remaining: int
    cost_per_share: float


@dataclass
class HoldSeg:
    ticker: str
    start: pd.Timestamp
    end: pd.Timestamp
    shares: int
    cost_per_share: float


@dataclass
class ChainOutcome:
    ticker: str
    start: pd.Timestamp
    end: Optional[pd.Timestamp]
    option_pnl: float
    stock_pnl: float
    total_pnl: float


# ------------------------------------------------------------
# Transformations
# ------------------------------------------------------------
def parse_strike_pair(s: str) -> Tuple[float, float]:
    try:
        a, b = str(s).split("/")
        return float(a), float(b)
    except Exception:
        return math.nan, math.nan


MIXED_SHORT_PUT_PHRASES = ("short put", "sold put", "written put")
MIXED_SHORT_CALL_PHRASES = ("short call", "sold call", "written call")


def infer_mixed_short_leg(row: pd.Series) -> Tuple[Optional[str], float]:
    t_low = str(row.get("type", "")).lower()
    c_low = str(row.get("comment", "")).lower()
    a, b = parse_strike_pair(row.get("strike", ""))
    put_strike = call_strike = math.nan
    if "put/call" in t_low:
        put_strike, call_strike = a, b
    elif "call/put" in t_low:
        call_strike, put_strike = a, b
    put_hint = any(phrase in c_low for phrase in MIXED_SHORT_PUT_PHRASES)
    call_hint = any(phrase in c_low for phrase in MIXED_SHORT_CALL_PHRASES)
    if put_hint and not call_hint:
        return "Put", put_strike
    if call_hint and not put_hint:
        return "Call", call_strike
    return None, math.nan


def _mixed_leg_parse_issue(row) -> str:
    row_dict = row._asdict() if hasattr(row, "_asdict") else row
    ticker = str(row_dict.get("ticker", "")).upper().strip()
    trans_date = pd.to_datetime(row_dict.get("trans_date", pd.NaT), errors="coerce")
    date_text = trans_date.date() if pd.notna(trans_date) else "unknown date"
    comment = row_dict.get("comment", "")
    return (
        f"Mixed-leg option row for {ticker or 'unknown ticker'} on {date_text} "
        f"has ambiguous short leg. Type={row_dict.get('type', '')}, strike={row_dict.get('strike', '')}, "
        f"comment={comment!r}. Add one of: short put, sold put, written put, short call, sold call, written call."
    )


def _price_per_share(row: pd.Series) -> float:
    accessor = row.get if hasattr(row, "get") else lambda k, default=None: getattr(row, k, default)
    qty_raw = accessor("qty", 0)
    qty = abs(float(qty_raw) if pd.notna(qty_raw) else 0.0)
    if qty == 0:
        return 0.0
    pnl_val = accessor("total_pnl", None)
    amount_val = accessor("amount", None)
    commission_val = accessor("commission", 0.0) or 0.0
    net_cash = None
    if pd.notna(pnl_val):
        net_cash = float(pnl_val)
    elif pd.notna(amount_val):
        net_cash = float(amount_val) - float(commission_val)
    if net_cash is None:
        return 0.0
    return net_cash / (qty * CONTRACT_MULTIPLIER)


def build_option_trades(df: pd.DataFrame, issues: Optional[List[str]] = None) -> List[OptionTrade]:
    trades: List[OptionTrade] = []
    rows = df.sort_values(["ticker", "trans_date"]).reset_index(drop=True)
    # Pre-count sells per option key to ignore standalone long buys (protective hedges)
    sell_counts: Dict[Tuple, int] = defaultdict(int)
    for r in rows.itertuples(index=False):
        t_raw = str(r.type).strip()
        action = r.action
        otype = None
        if t_raw in ("Put", "Call"):
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
            else:
                strike_val = math.nan
        else:
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        if action == "Sell" and otype is not None and not pd.isna(strike_val):
            key = (str(r.ticker).upper().strip(), otype, strike_val, pd.to_datetime(r.expiration).normalize())
            sell_counts[key] += 1

    for r in rows.itertuples(index=False):
        action = r.action
        if action not in ("Sell", "Buy"):
            continue
        t_raw = str(r.type).strip()
        cmt = r.comment if pd.notna(r.comment) else ""
        assigned_flag = False
        if hasattr(r, "assigned_flag"):
            try:
                assigned_flag = float(getattr(r, "assigned_flag")) > 0
            except Exception:
                assigned_flag = False
        assigned = assigned_flag or ("assigned" in cmt.lower())
        otype = None
        if t_raw in ("Put", "Call"):
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
            else:
                strike_val = math.nan
                if issues is not None:
                    issues.append(_mixed_leg_parse_issue(r))
        else:
            strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        if otype is None or pd.isna(strike_val):
            continue
        key = (str(r.ticker).upper().strip(), otype, strike_val, pd.to_datetime(r.expiration).normalize())
        if action == "Buy" and sell_counts.get(key, 0) == 0:
            # Ignore standalone protective longs
            continue
        price = _price_per_share(r)
        if action == "Buy":
            price = abs(price)
        qty = int(round(float(r.qty))) if pd.notna(r.qty) else 0
        trades.append(
            OptionTrade(
                date=pd.to_datetime(r.trans_date),
                ticker=r.ticker,
                otype=otype,
                action=action,
                strike=strike_val,
                expiration=pd.to_datetime(r.expiration),
                qty=qty,
                price=price,
                comment=cmt,
                assigned=assigned,
            )
        )
    return trades


def process_option_positions(trades: List[OptionTrade], as_of: pd.Timestamp):
    open_map: Dict[Tuple, List[OptionLot]] = defaultdict(list)
    realized_events: List[OptionPnLEvent] = []
    stock_txns: List[StockTxn] = []
    issues: List[str] = []
    all_lots: List[OptionLot] = []

    def snapshot_lot(
        lot: OptionLot,
        qty: Optional[int] = None,
        close_date: Optional[pd.Timestamp] = None,
        close_price: Optional[float] = None,
        close_reason: Optional[str] = None,
    ) -> OptionLot:
        return OptionLot(
            ticker=lot.ticker,
            otype=lot.otype,
            strike=lot.strike,
            qty=lot.qty if qty is None else qty,
            open_date=lot.open_date,
            expiration=lot.expiration,
            open_price=lot.open_price,
            comment=lot.comment,
            assigned=lot.assigned,
            close_date=close_date,
            close_price=close_price,
            close_reason=close_reason,
        )

    for t in sorted(trades, key=lambda x: (x.date, x.ticker)):
        key = (t.ticker, t.otype, t.strike, pd.to_datetime(t.expiration).normalize())
        if t.action == "Sell":
            lot = OptionLot(
                ticker=t.ticker,
                otype=t.otype,
                strike=t.strike,
                qty=t.qty,
                open_date=pd.to_datetime(t.date),
                expiration=pd.to_datetime(t.expiration),
                open_price=t.price,
                comment=t.comment,
                assigned=t.assigned,
            )
            open_map[key].append(lot)
        else:
            qty_to_close = t.qty
            buckets = open_map.get(key, [])
            if qty_to_close > 0 and not buckets:
                issues.append(f"Buy {t.ticker} {t.otype} {t.strike} on {t.date.date()} had no open short to close.")
            while qty_to_close > 0 and buckets:
                lot = buckets[0]
                take = min(qty_to_close, lot.qty)
                pnl = (lot.open_price - t.price) * take * CONTRACT_MULTIPLIER
                realized_events.append(
                    OptionPnLEvent(
                        date=pd.to_datetime(t.date),
                        ticker=t.ticker,
                        otype=t.otype,
                        strike=t.strike,
                        qty=take,
                        pnl=pnl,
                        p_open=lot.open_price,
                        p_close=t.price,
                        reason="close",
                    )
                )
                all_lots.append(
                    snapshot_lot(
                        lot,
                        qty=take,
                        close_date=pd.to_datetime(t.date),
                        close_price=t.price,
                        close_reason="close",
                    )
                )
                lot.qty -= take
                qty_to_close -= take
                if lot.qty == 0:
                    buckets.pop(0)
            if qty_to_close > 0:
                issues.append(f"Unmatched buy quantity for {t.ticker} {t.otype} {t.strike} on {t.date.date()}: {qty_to_close} remaining.")
            open_map[key] = buckets

    open_lots: List[OptionLot] = []
    for buckets in open_map.values():
        for lot in buckets:
            if pd.isna(lot.expiration):
                continue
            if as_of.normalize() >= pd.to_datetime(lot.expiration).normalize():
                close_date = pd.to_datetime(lot.expiration).normalize()
                pnl = (lot.open_price - 0.0) * lot.qty * CONTRACT_MULTIPLIER
                reason = "assignment" if lot.assigned else "expiration"
                realized_events.append(
                    OptionPnLEvent(
                        date=close_date,
                        ticker=lot.ticker,
                        otype=lot.otype,
                        strike=lot.strike,
                        qty=lot.qty,
                        pnl=pnl,
                        p_open=lot.open_price,
                        p_close=0.0,
                        reason=reason,
                    )
                )
                all_lots.append(
                    snapshot_lot(
                        lot,
                        close_date=close_date,
                        close_price=0.0,
                        close_reason=reason,
                    )
                )
                shares = int(round(lot.qty * CONTRACT_MULTIPLIER))
                if lot.assigned and shares > 0:
                    if lot.otype == "Put":
                        stock_txns.append(
                            StockTxn(close_date, lot.ticker, "BUY", shares, lot.strike, "Assigned Put")
                        )
                    else:
                        stock_txns.append(
                            StockTxn(close_date, lot.ticker, "SELL", shares, lot.strike, "Assigned Call")
                        )
            else:
                open_snapshot = snapshot_lot(lot)
                open_lots.append(open_snapshot)
                all_lots.append(open_snapshot)
    return realized_events, open_lots, stock_txns, issues, all_lots


def compute_stock_realized_and_inventory(txns: List[StockTxn], issues: Optional[List[str]] = None):
    by_ticker: Dict[str, List[OpenLot]] = defaultdict(list)
    realized: List[RealizedSale] = []
    for t in sorted(txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            by_ticker[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty_to_sell = t.shares
            cost_accum = 0.0
            while qty_to_sell > 0 and by_ticker[t.ticker]:
                lot = by_ticker[t.ticker][0]
                take = min(qty_to_sell, lot.shares_remaining)
                cost_accum += take * lot.cost_per_share
                lot.shares_remaining -= take
                qty_to_sell -= take
                if lot.shares_remaining == 0:
                    by_ticker[t.ticker].pop(0)
            if qty_to_sell > 0:
                # Not enough inventory; assume pre-owned shares for assigned calls -> zero P&L on uncovered portion
                if issues is not None and t.source != "Assigned Call":
                    issues.append(f"Selling {t.shares} shares of {t.ticker} on {t.date.date()} exceeded inventory by {qty_to_sell}.")
                cost_accum += qty_to_sell * t.price
                qty_to_sell = 0
            proceeds = t.shares * t.price
            cost = cost_accum
            realized.append(RealizedSale(t.date, t.ticker, t.shares, proceeds, cost, proceeds - cost, t.source))
    inventory: List[OpenLot] = []
    for _, lots_list in by_ticker.items():
        for lot in lots_list:
            if lot.shares_remaining > 0:
                inventory.append(lot)
    return realized, inventory


def build_holding_segments(txns: List[StockTxn], as_of: pd.Timestamp) -> List[HoldSeg]:
    open_buys: Dict[str, List[OpenLot]] = defaultdict(list)
    segs: List[HoldSeg] = []
    for t in sorted(txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            open_buys[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty = t.shares
            while qty > 0 and open_buys[t.ticker]:
                lot = open_buys[t.ticker][0]
                used = min(qty, lot.shares_remaining)
                segs.append(
                    HoldSeg(
                        t.ticker,
                        lot.buy_date.normalize(),
                        min(t.date.normalize(), as_of),
                        int(used),
                        lot.cost_per_share,
                    )
                )
                lot.shares_remaining -= used
                qty -= used
                if lot.shares_remaining == 0:
                    open_buys[t.ticker].pop(0)
    for tk, lots_list in open_buys.items():
        for lot in lots_list:
            if lot.shares_remaining > 0:
                segs.append(HoldSeg(tk, lot.buy_date.normalize(), as_of, int(lot.shares_remaining), lot.cost_per_share))
    return segs


def daterange_days(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if pd.isna(start) or pd.isna(end):
        return pd.DatetimeIndex([])
    start = start.normalize()
    end = end.normalize()
    if end <= start:
        end = start + pd.Timedelta(days=1)
    return pd.date_range(start, end, freq="D", inclusive="left")


def resolve_capital_price_on_day(
    px_series: Optional[pd.Series],
    valuation_date: pd.Timestamp,
    fallback_price: float,
) -> float:
    """Resolve a price for capital-denominator use: same-day close, else last prior close, else fallback."""
    if px_series is None:
        return fallback_price
    try:
        prices = px_series.dropna().copy()
        if prices.empty:
            return fallback_price
        prices.index = pd.to_datetime(prices.index, errors="coerce")
        prices = prices[prices.index.notna()].sort_index()
        if prices.empty:
            return fallback_price
        valuation_date = pd.to_datetime(valuation_date).normalize()
        exact_price = prices.get(valuation_date, np.nan)
        if pd.notna(exact_price):
            return float(exact_price)
        prior_prices = prices.loc[prices.index <= valuation_date]
        if not prior_prices.empty:
            return float(prior_prices.iloc[-1])
    except Exception:
        return fallback_price
    return fallback_price


def build_capital_timeline(
    option_lots: List[OptionLot],
    txns: List[StockTxn],
    as_of: pd.Timestamp,
    df_opts: pd.DataFrame,
    price_history: Dict[str, pd.Series],
) -> pd.DataFrame:
    rows = []
    for lot in option_lots:
        if lot.otype != "Put":
            continue
        open_d = pd.to_datetime(lot.open_date).normalize()
        close_candidate = lot.close_date if lot.close_date is not None else lot.expiration
        close_d = pd.to_datetime(close_candidate if pd.notna(close_candidate) else as_of).normalize()
        close_d = min(close_d, as_of.normalize())
        if pd.isna(open_d) or pd.isna(close_d):
            continue
        reserve = lot.strike * CONTRACT_MULTIPLIER * int(round(lot.qty))
        for d in daterange_days(open_d, close_d):
            rows.append((d, "puts_reserve", reserve))

    segs = build_holding_segments(txns, as_of)
    for seg in segs:
        px_series = price_history.get(seg.ticker)
        for d in daterange_days(seg.start, seg.end):
            price_on_day = resolve_capital_price_on_day(px_series, d, seg.cost_per_share)
            invested = seg.shares * price_on_day
            rows.append((d, "shares_invested", invested))

    cap = pd.DataFrame(rows, columns=["date", "component", "amount"])
    if cap.empty:
        start_date = df_opts["trans_date"].min().normalize() if not df_opts.empty else as_of.normalize()
        idx = pd.date_range(start_date, as_of, freq="D")
        cap = pd.DataFrame({"date": idx, "component": ["puts_reserve"] * len(idx), "amount": [0.0] * len(idx)})
    daily = cap.groupby(["date", "component"])["amount"].sum().unstack(fill_value=0.0)
    daily["total"] = daily.sum(axis=1)
    return daily


def _count_business_days(start: pd.Timestamp, end: pd.Timestamp) -> int:
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if pd.isna(start) or pd.isna(end) or start > end:
        return 0
    return len(pd.bdate_range(start, end))


def assess_capital_history_coverage(
    holding_segments: List[HoldSeg],
    price_history: Dict[str, pd.Series],
) -> Dict[str, object]:
    """
    Identify denominator-history gaps that should block return metrics.

    Cost-basis fallback remains acceptable only before a position has any prior
    fetched close. Once a segment should have durable historical coverage, stale
    or missing history marks the capital timeline as incomplete.
    """
    coverage_issues: List[Dict[str, object]] = []
    affected_months = set()
    affected_years = set()
    affected_tickers = set()

    def add_issue(ticker: str, start_date: pd.Timestamp, end_date: pd.Timestamp, reason: str) -> None:
        start_ts = pd.to_datetime(start_date).normalize()
        end_ts = pd.to_datetime(end_date).normalize()
        if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
            return
        coverage_issues.append(
            {
                "ticker": ticker,
                "start_date": start_ts,
                "end_date": end_ts,
                "reason": reason,
            }
        )
        affected_tickers.add(ticker)
        for d in pd.date_range(start_ts, end_ts, freq="D"):
            affected_months.add(d.to_period("M").to_timestamp("M"))
            affected_years.add(d.year)

    for seg in holding_segments:
        valuation_days = daterange_days(seg.start, seg.end)
        if valuation_days.empty:
            continue

        px_series = price_history.get(seg.ticker)
        if px_series is None:
            if len(valuation_days) > 1:
                add_issue(seg.ticker, valuation_days[1], valuation_days[-1], "missing_history")
            continue

        prices = px_series.dropna().copy()
        if prices.empty:
            if len(valuation_days) > 1:
                add_issue(seg.ticker, valuation_days[1], valuation_days[-1], "missing_history")
            continue

        prices.index = pd.to_datetime(prices.index, errors="coerce")
        prices = prices[prices.index.notna()].sort_index()
        if prices.empty:
            if len(valuation_days) > 1:
                add_issue(seg.ticker, valuation_days[1], valuation_days[-1], "missing_history")
            continue
        prices.index = prices.index.normalize()

        first_price_date = prices.index.min()
        last_price_date = prices.index.max()

        early_days = valuation_days[valuation_days < first_price_date]
        if len(early_days) > 1:
            missing_bdays_before_first = _count_business_days(
                early_days[0] + pd.Timedelta(days=1),
                first_price_date - pd.Timedelta(days=1),
            )
            if missing_bdays_before_first > 1:
                add_issue(seg.ticker, early_days[1], early_days[-1], "missing_before_first_close")

        in_range_prices = prices[(prices.index >= first_price_date) & (prices.index <= last_price_date)]
        if len(in_range_prices.index) > 1:
            for prev_date, next_date in zip(in_range_prices.index[:-1], in_range_prices.index[1:]):
                gap_start = prev_date + pd.Timedelta(days=1)
                gap_end = next_date - pd.Timedelta(days=1)
                if _count_business_days(gap_start, gap_end) > 1:
                    add_issue(seg.ticker, gap_start, gap_end, "internal_gap")

        late_days = valuation_days[valuation_days > last_price_date]
        if len(late_days) > 0:
            missing_bdays_after_last = _count_business_days(
                last_price_date + pd.Timedelta(days=1),
                late_days[-1],
            )
            if missing_bdays_after_last > 1:
                add_issue(seg.ticker, late_days[0], late_days[-1], "stale_tail")

    return {
        "capital_history_incomplete": bool(coverage_issues),
        "capital_history_coverage_issues": coverage_issues,
        "capital_history_affected_months": sorted(affected_months),
        "capital_history_affected_years": sorted(affected_years),
        "capital_history_affected_tickers": sorted(affected_tickers),
    }


def build_monthly_summary(
    realized_option_events: List[OptionPnLEvent],
    realized_sales: List[RealizedSale],
    capital_daily: pd.DataFrame,
    dividends_df: pd.DataFrame,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    def month_end(d):
        return pd.to_datetime(d).to_period("M").to_timestamp("M")

    opt_series = pd.Series(dtype=float, name="realized_options_pnl")
    if realized_option_events:
        df = pd.DataFrame(
            [{"date": e.date, "pnl": e.pnl} for e in realized_option_events if pd.to_datetime(e.date) <= as_of]
        )
        if not df.empty:
            opt_series = df.groupby(df["date"].apply(month_end))["pnl"].sum().rename("realized_options_pnl")

    stock_series = pd.Series(dtype=float, name="realized_stock_pnl")
    if realized_sales:
        rs_df = pd.DataFrame(
            [{"date": r.date, "pnl": r.pnl} for r in realized_sales if pd.to_datetime(r.date) <= as_of]
        )
        if not rs_df.empty:
            stock_series = rs_df.groupby(rs_df["date"].apply(month_end))["pnl"].sum().rename("realized_stock_pnl")

    div_series = pd.Series(dtype=float, name="dividends")
    if dividends_df is not None and not dividends_df.empty:
        div_filtered = dividends_df[dividends_df["pay_date"] <= as_of] if "pay_date" in dividends_df else dividends_df.copy()
        if not div_filtered.empty:
            date_col = "pay_date" if "pay_date" in div_filtered else "ex_date"
            div_series = div_filtered.groupby(div_filtered[date_col].apply(month_end))["cash"].sum().rename("dividends")

    cap = capital_daily.copy()
    cap.index = pd.to_datetime(cap.index).normalize()
    cap["month"] = cap.index.to_series().apply(month_end)
    avg_cap = cap.groupby("month")["total"].mean().rename("avg_capital")
    peak_cap = cap.groupby("month")["total"].max().rename("peak_capital")

    combined = pd.concat([opt_series, stock_series, div_series, avg_cap, peak_cap], axis=1).fillna(0.0)
    combined["total_realized_pnl"] = combined["realized_options_pnl"] + combined["realized_stock_pnl"] + combined["dividends"]
    combined["roac"] = np.where(combined["avg_capital"] > 0, combined["total_realized_pnl"] / combined["avg_capital"], np.nan)
    combined["ropc"] = np.where(combined["peak_capital"] > 0, combined["total_realized_pnl"] / combined["peak_capital"], np.nan)
    combined.index.name = "month"
    as_of_month_end = pd.to_datetime(as_of).to_period("M").to_timestamp("M")
    combined = combined[combined.index <= as_of_month_end].sort_index()
    return combined


def yearly_summary_from_monthly(monthly_df: pd.DataFrame, capital_daily: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    if monthly_df is None or monthly_df.empty:
        return pd.DataFrame(
            columns=[
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
            ]
        )
    m = monthly_df.copy()
    # Guard against object indexes (e.g., strings) that break .index.year
    m.index = pd.to_datetime(m.index, errors="coerce")
    m = m[m.index.notna()]
    if m.empty:
        return pd.DataFrame(
            columns=[
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
            ]
        )
    m["year"] = m.index.year
    agg = (
        m.groupby("year")
        .agg(
            realized_options_pnl=("realized_options_pnl", "sum"),
            realized_stock_pnl=("realized_stock_pnl", "sum"),
            dividends=("dividends", "sum"),
            total_realized_pnl=("total_realized_pnl", "sum"),
            roac_year=("roac", lambda s: (1 + s.dropna()).prod() - 1 if len(s.dropna()) else np.nan),
            ropc_year=("ropc", lambda s: (1 + s.dropna()).prod() - 1 if len(s.dropna()) else np.nan),
        )
        .reset_index()
    )
    cap_stats = capital_stats_by_year(capital_daily)
    agg = agg.merge(cap_stats, on="year", how="left")
    month_counts = m.groupby("year").size()
    days_elapsed = (
        capital_daily.reset_index()
        .assign(year=lambda d: pd.to_datetime(d["date"]).dt.year)
        .groupby("year")["date"]
        .nunique()
    )
    agg["ann_roac"] = agg["roac_year"]
    agg["ann_ropc"] = agg["ropc_year"]
    for idx, row in agg.iterrows():
        year = row["year"]
        months = month_counts.get(year, 0)
        if months == 12:
            continue
        days = days_elapsed.get(year, np.nan)
        if pd.notna(row["roac_year"]) and pd.notna(days) and days > 0:
            agg.at[idx, "ann_roac"] = (1 + row["roac_year"]) ** (365.0 / days) - 1
        if pd.notna(row["ropc_year"]) and pd.notna(days) and days > 0:
            agg.at[idx, "ann_ropc"] = (1 + row["ropc_year"]) ** (365.0 / days) - 1
    agg = agg.sort_values("year")
    return agg


def realized_option_pnl_by_year(realized_option_events: List[OptionPnLEvent]) -> pd.DataFrame:
    if not realized_option_events:
        return pd.DataFrame(columns=["year", "options_pnl"])
    df = pd.DataFrame([{"date": e.date, "pnl": e.pnl} for e in realized_option_events])
    df["year"] = pd.to_datetime(df["date"]).dt.year
    return df.groupby("year")["pnl"].sum().rename("options_pnl").reset_index()


def realized_stock_pnl_by_year(realized_sales: List[RealizedSale]) -> pd.DataFrame:
    if not realized_sales:
        return pd.DataFrame(columns=["year", "stock_realized_pnl"])
    df = pd.DataFrame([{"date": r.date, "pnl": r.pnl} for r in realized_sales])
    df["year"] = pd.to_datetime(df["date"]).dt.year
    return df.groupby("year")["pnl"].sum().rename("stock_realized_pnl").reset_index()


def per_ticker_yearly_from_realized(
    realized_option_events: List[OptionPnLEvent],
    realized_sales: List[RealizedSale],
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    opt_df = pd.DataFrame(
        [
            {"year": pd.to_datetime(e.date).year, "ticker": e.ticker, "options_pnl": e.pnl}
            for e in realized_option_events
            if pd.to_datetime(e.date) <= as_of
        ]
    )
    stock_df = pd.DataFrame(
        [
            {"year": pd.to_datetime(r.date).year, "ticker": r.ticker, "stock_realized_pnl": r.pnl}
            for r in realized_sales or []
            if pd.to_datetime(r.date) <= as_of
        ]
    )
    if not opt_df.empty:
        opt_df = opt_df.groupby(["year", "ticker"])["options_pnl"].sum().reset_index()
    else:
        opt_df = pd.DataFrame(columns=["year", "ticker", "options_pnl"])
    if not stock_df.empty:
        stock_df = stock_df.groupby(["year", "ticker"])["stock_realized_pnl"].sum().reset_index()
    else:
        stock_df = pd.DataFrame(columns=["year", "ticker", "stock_realized_pnl"])
    out = opt_df.merge(stock_df, on=["year", "ticker"], how="outer").fillna(0.0)
    out["combined_realized"] = out["options_pnl"] + out["stock_realized_pnl"]
    return out.sort_values(["year", "combined_realized"], ascending=[True, False])


def build_per_ticker_totals(per_ticker_realized: pd.DataFrame, per_ticker_unreal: pd.Series) -> pd.DataFrame:
    realized_cols = ["options_pnl", "stock_realized_pnl", "combined_realized"]
    if per_ticker_realized is not None and not per_ticker_realized.empty:
        realized_totals = (
            per_ticker_realized.groupby("ticker")[realized_cols]
            .sum()
            .reset_index()
        )
    else:
        realized_totals = pd.DataFrame(columns=["ticker", *realized_cols])

    if per_ticker_unreal is not None and not per_ticker_unreal.empty:
        unreal_totals = per_ticker_unreal.rename_axis("ticker").reset_index(name="unrealized_pnl")
    else:
        unreal_totals = pd.DataFrame(columns=["ticker", "unrealized_pnl"])

    all_tickers = sorted(set(realized_totals.get("ticker", pd.Series(dtype=str))).union(unreal_totals.get("ticker", pd.Series(dtype=str))))
    if not all_tickers:
        return pd.DataFrame(columns=["ticker", *realized_cols, "unrealized_pnl", "total_pnl"])

    out = pd.DataFrame({"ticker": all_tickers})
    out = out.merge(realized_totals, on="ticker", how="left").merge(unreal_totals, on="ticker", how="left")
    for col in [*realized_cols, "unrealized_pnl"]:
        if col not in out.columns:
            out[col] = 0.0
    out[realized_cols + ["unrealized_pnl"]] = out[realized_cols + ["unrealized_pnl"]].fillna(0.0)
    out["total_pnl"] = out["combined_realized"] + out["unrealized_pnl"]
    return out


def twr_annualized_by_year(ret_series):
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return pd.Series(dtype=float)
    rs = ret_series.copy()
    rs.index = pd.to_datetime(rs.index, errors="coerce")
    rs = rs[rs.index.notna()]
    if rs.empty:
        return pd.Series(dtype=float)
    grouped = ret_series.groupby(ret_series.index.year)
    return grouped.apply(lambda r: (1 + r).prod() ** (12 / len(r)) - 1)


def expectancies(realized_option_events: List[OptionPnLEvent], realized_sales: List[RealizedSale], monthly_summary: pd.DataFrame, chain_outcomes: List["ChainOutcome"]):
    rows = []
    def add_row(name, pnls):
        if len(pnls) == 0:
            return
        pnls = np.array([p for p in pnls if pd.notna(p)], dtype=float)
        wins = pnls[pnls > 0]
        losses = pnls[pnls < 0]
        win_rate = (pnls > 0).mean() if len(pnls) else np.nan
        avg_win = wins.mean() if len(wins) else 0.0
        avg_loss = losses.mean() if len(losses) else 0.0
        expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss
        rows.append(
            {
                "Category": name,
                "Count": len(pnls),
                "Win rate": win_rate,
                "Avg win": avg_win,
                "Avg loss": avg_loss,
                "Expectancy": expectancy,
                "Total P&L": pnls.sum(),
            }
        )

    add_row("Options Trades", [e.pnl for e in realized_option_events])
    add_row("Stock Trades", [r.pnl for r in realized_sales])
    if monthly_summary is not None and not monthly_summary.empty and "total_realized_pnl" in monthly_summary:
        add_row("Monthly Totals", monthly_summary["total_realized_pnl"].tolist())
    if chain_outcomes:
        add_row("Chains", [c.total_pnl for c in chain_outcomes if c.end is not None])

    return pd.DataFrame(rows)


def calculate_performance_metrics(ret_series: pd.Series, rf: float = 0.04):
    ret_series = ret_series.dropna()
    if ret_series.empty:
        return {}
    m = len(ret_series)
    ec = (1 + ret_series).cumprod()
    cagr = ec.iloc[-1] ** (12 / m) - 1 if m > 0 else 0.0
    ann_vol = ret_series.std() * np.sqrt(12)
    rf_m = (1 + rf) ** (1 / 12) - 1
    excess = ret_series - rf_m
    sharpe = (excess.mean() / ret_series.std()) * np.sqrt(12) if ret_series.std() != 0 else np.nan
    down_std = np.sqrt((excess[excess < 0] ** 2).sum() / m) if m > 0 else 0
    sortino = (excess.mean() / down_std) * np.sqrt(12) if down_std != 0 else np.nan
    # prepend baseline equity of 1.0 so an initial negative month counts as a drawdown
    ec_dd = pd.concat([pd.Series([1.0]), ec.reset_index(drop=True)], ignore_index=True)
    max_dd = (ec_dd / ec_dd.cummax() - 1).min()
    return {"CAGR": cagr, "Volatility": ann_vol, "Sharpe": sharpe, "Sortino": sortino, "Max Drawdown": max_dd}


def align_benchmarks_monthly(tickers: Dict[str, str], idx: pd.DatetimeIndex):
    """Return dict name->Series of monthly returns aligned to given month-end index."""
    if yf is None or len(idx) == 0:
        return {}
    start = idx.min() - pd.DateOffset(months=2)
    end = idx.max() + pd.DateOffset(days=1)
    all_tickers_list = list(tickers.values())
    try:
        px_data = yf.download(all_tickers_list, start=start, end=end, progress=False, auto_adjust=True)
        if px_data.empty:
            return {}
        px_data = px_data["Close"] if "Close" in px_data.columns else px_data
    except Exception:
        return {}
    aligned = {}
    for name, ticker in tickers.items():
        try:
            px = px_data[ticker] if len(all_tickers_list) > 1 else px_data
            px = px.dropna()
            if px.empty:
                continue
            # resample to month-end to match strategy returns
            monthly_px = px.resample("ME").last()
            monthly_ret = monthly_px.pct_change(fill_method=None)
            monthly_ret = monthly_ret.reindex(idx)
            aligned[name] = monthly_ret
        except Exception:
            continue
    return aligned


def period_returns(ret_series: pd.Series):
    out = {}
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return out
    srt = ret_series.copy()
    srt.index = pd.to_datetime(srt.index, errors="coerce")
    srt = srt[srt.index.notna()].sort_index()
    if srt.empty:
        return out
    def compound_if_complete(sub):
        return (1 + sub).prod() - 1 if len(sub) and sub.notna().all() else np.nan

    def trailing_n(n):
        sub = srt.tail(n)
        return compound_if_complete(sub) if len(sub) == n else np.nan

    out["Return 3M"] = trailing_n(3)
    out["Return 6M"] = trailing_n(6)
    out["Return 1Y"] = trailing_n(12)
    latest_year = srt.index.max().year
    ytd = srt[srt.index.year == latest_year]
    out["Return YTD"] = compound_if_complete(ytd)
    out["Return SI"] = compound_if_complete(srt)
    return out


def capital_stats_by_year(capital_daily: pd.DataFrame) -> pd.DataFrame:
    df = capital_daily.reset_index()
    df["year"] = df["date"].dt.year
    return df.groupby("year").agg(avg_capital=("total", "mean"), peak_capital=("total", "max")).reset_index()


def build_covered_return_series(
    monthly_returns: pd.Series,
    affected_months: List[pd.Timestamp],
) -> Dict[str, object]:
    """
    Return the contiguous fully covered prefix for return-based charts/metrics.

    Once denominator incompleteness starts, later months are excluded from
    cumulative return displays and benchmark comparisons.
    """
    covered = monthly_returns.copy()
    covered.index = pd.to_datetime(covered.index, errors="coerce")
    covered = covered[covered.index.notna()].sort_index()
    affected = sorted(pd.to_datetime(m).to_period("M").to_timestamp("M") for m in affected_months if pd.notna(m))
    first_incomplete_month = affected[0] if affected else None
    if first_incomplete_month is not None:
        covered = covered[covered.index < first_incomplete_month]
    last_complete_month = covered.index.max() if not covered.empty else None
    return {
        "covered_returns": covered,
        "first_incomplete_month": first_incomplete_month,
        "last_complete_month": last_complete_month,
        "truncated": first_incomplete_month is not None and last_complete_month is not None,
    }


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


def fetch_current_prices_yf(tickers) -> Tuple[Dict[str, float], List[str], Dict[str, int]]:
    """Fetch latest stock prices; return prices, error messages, and coverage summary."""
    errors: List[str] = []
    summary = {"requested": 0, "fetched": 0}
    if yf is None:
        errors.append("yfinance not installed; cannot fetch live stock prices.")
        return {}, errors, summary
    tickers = sorted({str(t).upper().strip() for t in tickers if isinstance(t, str) and t.strip()})
    summary["requested"] = len(tickers)
    prices: Dict[str, float] = {}
    if not tickers:
        return prices, errors, summary
    try:
        data = yf.download(tickers=tickers, period="5d", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=True)
        if isinstance(data.columns, pd.MultiIndex):
            for t in tickers:
                for col in ("Adj Close", "Close"):
                    try:
                        series = data[(t, col)].dropna()
                        if not series.empty:
                            prices[t] = float(series.iloc[-1])
                            break
                    except KeyError:
                        continue
        else:
            series = data["Adj Close"].dropna() if "Adj Close" in data else data["Close"].dropna()
            if not series.empty and len(tickers) == 1:
                prices[tickers[0]] = float(series.iloc[-1])
    except Exception as exc:
        errors.append(f"Primary price download failed: {exc}")
    missing = [t for t in tickers if t not in prices]
    for t in missing:
        try:
            tk = yf.Ticker(t)
            hist = tk.history(period="5d", interval="1d")
            if not hist.empty:
                prices[t] = float(hist["Close"].iloc[-1])
                continue
            p = getattr(tk.fast_info, "last_price", None)
            if p:
                prices[t] = float(p)
        except Exception as exc:
            errors.append(f"{t}: {exc}")
    still_missing = [t for t in tickers if t not in prices]
    summary["fetched"] = len(prices)
    if still_missing:
        errors.append(f"Missing prices for tickers: {', '.join(still_missing)}")
    return prices, errors, summary


def fetch_price_history_yf(
    tickers,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[Dict[str, pd.Series], List[str], Dict[str, int]]:
    """Daily close prices per ticker between start and end (inclusive end)."""
    history: Dict[str, pd.Series] = {}
    errors: List[str] = []
    summary = {"requested": 0, "fetched": 0}
    if yf is None:
        errors.append("yfinance not installed; cannot fetch historical stock prices.")
        return history, errors, summary
    tickers = sorted({t for t in tickers if t})
    summary["requested"] = len(tickers)
    if not tickers or pd.isna(start) or pd.isna(end):
        return history, errors, summary
    try:
        data = yf.download(
            tickers=tickers,
            start=start,
            end=end + pd.Timedelta(days=1),
            progress=False,
            auto_adjust=False,
            group_by="ticker",
        )
        if isinstance(data.columns, pd.MultiIndex):
            for t in tickers:
                try:
                    series = data[(t, "Adj Close")].dropna() if (t, "Adj Close") in data else data[(t, "Close")].dropna()
                    if not series.empty:
                        history[t] = series.tz_localize(None).rename(t)
                except Exception:
                    continue
        else:
            series = data["Adj Close"].dropna() if "Adj Close" in data else data.get("Close", pd.Series(dtype=float)).dropna()
            if not series.empty and len(tickers) == 1:
                history[tickers[0]] = series.tz_localize(None).rename(tickers[0])
    except Exception as exc:
        errors.append(f"Historical price download failed: {exc}")
        return history, errors, summary
    # normalize date index
    for t, s in list(history.items()):
        s.index = pd.to_datetime(s.index).normalize()
        history[t] = s
    summary["fetched"] = len(history)
    missing_tickers = [t for t in tickers if t not in history]
    if missing_tickers:
        errors.append(f"Missing historical price series for tickers: {', '.join(missing_tickers)}")
    return history, errors, summary


def calculate_unrealized_positions(
    open_options: List[OptionLot],
    inventory: List[OpenLot],
    prices: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.Series, float]:
    """Compute unrealized P&L by ticker using rules for short options and covered calls."""
    per_ticker = defaultdict(float)
    stock_rows = []
    # Build coverage map for open calls (shares capped at strike)
    coverage: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for lot in open_options:
        if lot.otype == "Call" and lot.qty > 0:
            coverage[lot.ticker].append({"strike": lot.strike, "shares": lot.qty * CONTRACT_MULTIPLIER})
    for cov_list in coverage.values():
        cov_list.sort(key=lambda x: x["strike"])  # use lowest strikes first

    # Option unrealized (premium received) + short put stock component
    for lot in open_options:
        premium_total = lot.open_price * lot.qty * CONTRACT_MULTIPLIER
        per_ticker[lot.ticker] += premium_total
        if lot.otype == "Put":
            px = prices.get(lot.ticker)
            if px is not None and not pd.isna(px) and px < lot.strike:
                stock_component = (px - lot.strike) * lot.qty * CONTRACT_MULTIPLIER
                per_ticker[lot.ticker] += stock_component
                stock_rows.append(
                    {
                        "ticker": lot.ticker,
                        "buy_date": None,
                        "shares": lot.qty * CONTRACT_MULTIPLIER,
                        "cost_per_share": lot.strike,
                        "current_price": px,
                        "covered_shares": 0,
                        "covered_strike": lot.strike,
                        "unrealized_pnl": stock_component,
                        "source": "put_gap",
                    }
                )

    # Stock inventory unrealized with covered call cap
    for lot in inventory:
        px = prices.get(lot.ticker)
        if px is None or pd.isna(px):
            continue
        shares_remaining = lot.shares_remaining
        lot_pnl = 0.0
        covered_used = 0
        covered_strike_min = None
        cov_list = coverage.get(lot.ticker, [])
        while shares_remaining > 0:
            if cov_list:
                leg = cov_list[0]
                use = min(shares_remaining, leg["shares"])
                effective_px = min(px, leg["strike"])
                lot_pnl += (effective_px - lot.cost_per_share) * use
                covered_used += use
                covered_strike_min = leg["strike"] if covered_strike_min is None else min(covered_strike_min, leg["strike"])
                leg["shares"] -= use
                shares_remaining -= use
                if leg["shares"] == 0:
                    cov_list.pop(0)
            else:
                lot_pnl += (px - lot.cost_per_share) * shares_remaining
                shares_remaining = 0
        per_ticker[lot.ticker] += lot_pnl
        stock_rows.append(
            {
                "ticker": lot.ticker,
                "buy_date": lot.buy_date,
                "shares": lot.shares_remaining,
                "cost_per_share": lot.cost_per_share,
                "current_price": px,
                "covered_shares": covered_used,
                "covered_strike": covered_strike_min,
                "unrealized_pnl": lot_pnl,
                "source": "stock_lot",
            }
        )

    inv_df = pd.DataFrame(stock_rows)
    per_ticker_series = pd.Series(per_ticker, dtype=float)
    total_unreal = float(per_ticker_series.sum()) if not per_ticker_series.empty else 0.0
    return inv_df, per_ticker_series, total_unreal


def build_dashboard_unrealized_snapshot(
    open_option_lots: List[OptionLot],
    ending_inventory: List[OpenLot],
    live_prices: Dict[str, float],
) -> Dict[str, object]:
    """Return the current dashboard unrealized snapshot without changing its legacy behavior."""
    required_price_tickers = {lot.ticker for lot in ending_inventory}
    required_price_tickers.update(lot.ticker for lot in open_option_lots if lot.otype == "Put")
    missing_required_price_tickers = sorted(
        ticker
        for ticker in required_price_tickers
        if ticker not in live_prices or pd.isna(live_prices.get(ticker))
    )
    inv_df, per_ticker_unreal, total_unreal = calculate_unrealized_positions(
        open_option_lots,
        ending_inventory,
        live_prices,
    )
    stock_unreal = float(inv_df["unrealized_pnl"].sum()) if not inv_df.empty else 0.0
    option_unreal = total_unreal - stock_unreal
    return {
        "inv_df": inv_df,
        "per_ticker_unreal": per_ticker_unreal,
        "total_unreal": total_unreal,
        "stock_unreal": stock_unreal,
        "option_unreal": option_unreal,
        "unrealized_blocked": bool(missing_required_price_tickers),
        "missing_required_price_tickers": missing_required_price_tickers,
    }


def build_dashboard_unrealized_adjusted_return_series(
    monthly_returns: pd.Series,
    capital_daily: pd.DataFrame,
    as_of_ts: pd.Timestamp,
    include_unrealized_current_year: bool,
    total_unreal: float,
    unrealized_blocked: bool = False,
) -> pd.Series:
    """Apply the dashboard's current unrealized-adjusted return treatment."""
    monthly_returns_unrealized_adjusted = monthly_returns.copy()
    if unrealized_blocked:
        return monthly_returns_unrealized_adjusted
    if include_unrealized_current_year and total_unreal != 0:
        cap_year_stats = capital_stats_by_year(capital_daily)
        cap_curr_year = cap_year_stats.loc[cap_year_stats["year"] == as_of_ts.year, "avg_capital"]
        cap_basis = float(cap_curr_year.iloc[0]) if not cap_curr_year.empty else np.nan
        if pd.notna(cap_basis) and cap_basis > 0:
            unrealized_return_component = total_unreal / cap_basis
            month_end = pd.to_datetime(as_of_ts).to_period("M").to_timestamp("M")
            base_ret = (
                monthly_returns_unrealized_adjusted.loc[month_end]
                if month_end in monthly_returns_unrealized_adjusted.index
                else 0.0
            )
            monthly_returns_unrealized_adjusted.loc[month_end] = base_ret + unrealized_return_component
    return monthly_returns_unrealized_adjusted


def build_yearly_with_dashboard_unrealized(
    yearly: pd.DataFrame,
    include_unrealized_current_year: bool,
    total_unreal: float,
    as_of_ts: pd.Timestamp,
    unrealized_blocked: bool = False,
) -> pd.DataFrame:
    """Apply the dashboard's current unrealized total to the yearly summary."""
    yearly_with_unreal = yearly.copy()
    yearly_with_unreal["total_pnl_incl_unreal"] = yearly_with_unreal.get("total_realized_pnl", pd.Series(dtype=float))
    if unrealized_blocked and not yearly_with_unreal.empty:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        yearly_with_unreal.loc[mask_curr, "total_pnl_incl_unreal"] = np.nan
        return yearly_with_unreal
    if include_unrealized_current_year and total_unreal != 0 and not yearly_with_unreal.empty:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        yearly_with_unreal.loc[mask_curr, "total_pnl_incl_unreal"] = (
            yearly_with_unreal.loc[mask_curr, "total_realized_pnl"] + total_unreal
        )
    return yearly_with_unreal


def _chain_stock_realized(stock_txns: List[StockTxn]) -> float:
    by_ticker: Dict[str, List[OpenLot]] = defaultdict(list)
    realized = 0.0
    for t in sorted(stock_txns, key=lambda x: (x.date, x.ticker)):
        if t.side == "BUY":
            by_ticker[t.ticker].append(OpenLot(t.ticker, t.date, t.shares, t.price))
        else:
            qty = t.shares
            cost_accum = 0.0
            while qty > 0 and by_ticker[t.ticker]:
                lot = by_ticker[t.ticker][0]
                take = min(qty, lot.shares_remaining)
                cost_accum += take * lot.cost_per_share
                lot.shares_remaining -= take
                qty -= take
                if lot.shares_remaining == 0:
                    by_ticker[t.ticker].pop(0)
            # uncovered sells assume zero profit (pre-owned)
            cost_accum += qty * t.price
            realized += t.shares * t.price - cost_accum
    return realized


def build_chains(stock_txns: List[StockTxn], option_events: List[OptionPnLEvent], as_of: pd.Timestamp) -> List[ChainOutcome]:
    chains: Dict[str, List[Dict]] = defaultdict(list)
    balances: Dict[str, int] = defaultdict(int)
    # Build chains from stock txn flow
    for t in sorted(stock_txns, key=lambda x: (x.date, x.ticker)):
        tk = t.ticker
        cur_balance = balances[tk]
        active = chains[tk][-1] if chains[tk] else None
        if active is None:
            active = {"start": t.date, "end": None, "txns": [], "option_events": []}
            chains[tk].append(active)
        active["txns"].append(t)
        if t.side == "BUY":
            cur_balance += t.shares
        else:
            cur_balance = max(0, cur_balance - t.shares)
        balances[tk] = cur_balance
        if cur_balance == 0:
            active["end"] = t.date
    # Attach option events to chains by ticker and date window
    for ev in sorted(option_events, key=lambda x: (x.date, x.ticker)):
        tk = ev.ticker
        assigned_chain = None
        for ch in chains.get(tk, []):
            end_date = ch["end"] if ch["end"] is not None else as_of
            if ch["start"] <= ev.date <= end_date:
                assigned_chain = ch
                break
        if assigned_chain is None:
            # standalone option chain with no stock flow; treat any realized event as a closed chain
            ch = {"start": ev.date, "end": ev.date, "txns": [], "option_events": [ev]}
            chains[tk].append(ch)
        else:
            assigned_chain["option_events"].append(ev)

    outcomes: List[ChainOutcome] = []
    for tk, ch_list in chains.items():
        for ch in ch_list:
            stock_pnl = _chain_stock_realized(ch["txns"])
            option_pnl = sum(e.pnl for e in ch["option_events"])
            outcomes.append(
                ChainOutcome(
                    ticker=tk,
                    start=pd.to_datetime(ch["start"]),
                    end=pd.to_datetime(ch["end"]) if ch["end"] is not None else None,
                    option_pnl=option_pnl,
                    stock_pnl=stock_pnl,
                    total_pnl=option_pnl + stock_pnl,
                )
            )
    return outcomes


def build_options_cycle_chart_data(monthly_summary: pd.DataFrame) -> pd.DataFrame:
    if monthly_summary is None or monthly_summary.empty or "total_realized_pnl" not in monthly_summary.columns:
        return pd.DataFrame(columns=["Date", "pnl", "color"])
    pnl_df = monthly_summary[["total_realized_pnl"]].reset_index()
    first_col = pnl_df.columns[0]
    pnl_df = pnl_df.rename(columns={first_col: "Date", "total_realized_pnl": "pnl"})
    pnl_df["color"] = np.where(pnl_df["pnl"] >= 0, "Positive", "Negative")
    return pnl_df


def _format_df(df: pd.DataFrame, currency_cols=None, pct_cols=None, int_cols=None, float_cols=None, hide_index=False):
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
    styler = df.style.format(formatter).set_properties(**{"text-align": "right"})
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


def normalize_dividend_fetch_result(result) -> DividendFetchResult:
    if isinstance(result, DividendFetchResult):
        return result
    if isinstance(result, pd.DataFrame):
        return DividendFetchResult(
            cashflows=result,
            coverage_complete=True,
            attempted_tickers=[],
            failed_tickers=[],
            errors=[],
        )
    return DividendFetchResult(
        cashflows=pd.DataFrame(columns=["ticker", "ex_date", "pay_date", "per_share", "shares", "cash"]),
        coverage_complete=False,
        attempted_tickers=[],
        failed_tickers=[],
        errors=["Dividend fetch returned an unexpected result shape."],
    )


def collect_dividend_cashflows(stock_txns: List[StockTxn], as_of: pd.Timestamp) -> DividendFetchResult:
    provider = YFinanceDividendProvider(yf) if yf is not None else None
    return _collect_dividend_cashflows(stock_txns, as_of, build_holding_segments, provider)


def render_issue_status_banner(issues: List[str], price_errors: List[str], price_summary: Dict[str, int]) -> None:
    severity = "success"
    coverage_problem = price_summary and (
        price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
    )
    if issues or price_errors:
        severity = "error"
    elif coverage_problem:
        severity = "warning"

    total_issues = len(issues) + len(price_errors) + (1 if coverage_problem else 0)
    if total_issues == 0:
        msg = "0 issues detected (Logs tab)"
    else:
        msg = f"{total_issues} issue(s) detected — check Logs tab"

    color = {"success": "#22c55e", "warning": "#f59e0b", "error": "#ef4444"}[severity]
    st.markdown(f"<div style='font-weight:600; color:{color}; margin: 4px 0;'>{msg}</div>", unsafe_allow_html=True)


def build_pipeline(as_of: date, include_unrealized_current_year: bool, selected_sheets: List[str], cache_bust: int = 1):
    df_opts = load_options(SHEET_ID, selected_sheets)
    sheet_counts = df_opts.groupby("source_sheet").size().rename("rows").reset_index()
    today_norm = pd.Timestamp.today().normalize()
    as_of_ts = min(pd.Timestamp(as_of), today_norm)
    issues: List[str] = []
    price_errors: List[str] = []
    historical_price_errors: List[str] = []

    df_opts = df_opts[df_opts["trans_date"] <= as_of_ts].copy()

    trades = build_option_trades(df_opts, issues)
    realized_option_events, open_option_lots, stock_txns, trade_issues, all_option_lots = process_option_positions(trades, as_of_ts)
    issues.extend(trade_issues)
    realized_sales, ending_inventory = compute_stock_realized_and_inventory(stock_txns, issues)
    chain_outcomes = build_chains(stock_txns, realized_option_events, as_of_ts)
    start_date = df_opts["trans_date"].min() if not df_opts.empty else as_of_ts
    if pd.notna(start_date):
        price_history, historical_price_errors, historical_price_summary = fetch_price_history_yf(
            {t.ticker for t in stock_txns},
            pd.to_datetime(start_date).normalize(),
            as_of_ts.normalize(),
        )
    else:
        price_history, historical_price_errors, historical_price_summary = {}, [], {"requested": 0, "fetched": 0}
    capital_daily = build_capital_timeline(all_option_lots, stock_txns, as_of_ts, df_opts, price_history)
    holding_segments = build_holding_segments(stock_txns, as_of_ts)
    capital_history_state = assess_capital_history_coverage(holding_segments, price_history)

    dividend_fetch_result = normalize_dividend_fetch_result(collect_dividend_cashflows(stock_txns, as_of_ts))
    div_df = dividend_fetch_result.cashflows
    dividend_coverage_complete = bool(dividend_fetch_result.coverage_complete)
    dividend_attempted_tickers = dividend_fetch_result.attempted_tickers
    dividend_failed_tickers = dividend_fetch_result.failed_tickers
    dividend_affected_tickers = dividend_failed_tickers or dividend_attempted_tickers
    dividend_errors = dividend_fetch_result.errors
    dividend_summary = {
        "attempted": len(dividend_attempted_tickers),
        "failed": len(dividend_failed_tickers),
    }

    monthly_summary = build_monthly_summary(realized_option_events, realized_sales, capital_daily, div_df, as_of_ts)
    affected_months = capital_history_state["capital_history_affected_months"]
    if capital_history_state["capital_history_incomplete"] and not monthly_summary.empty:
        affected_month_mask = monthly_summary.index.isin(affected_months)
        for col in ("roac", "ropc"):
            if col in monthly_summary.columns:
                monthly_summary.loc[affected_month_mask, col] = np.nan
    monthly_returns = monthly_summary["roac"].dropna() if "roac" in monthly_summary else pd.Series(dtype=float)
    monthly_returns.index = pd.to_datetime(monthly_returns.index, errors="coerce")
    monthly_returns = monthly_returns[monthly_returns.index.notna()]
    covered_return_state = build_covered_return_series(
        monthly_returns,
        capital_history_state["capital_history_affected_months"],
    )
    monthly_returns_covered = covered_return_state["covered_returns"]
    monthly_returns_unrealized_adjusted = monthly_returns.copy()
    # Active months: exclude months with zero options P&L (i.e., no option trades)
    if "realized_options_pnl" in monthly_summary and "roac" in monthly_summary:
        monthly_returns_active = monthly_summary.loc[monthly_summary["realized_options_pnl"] != 0, "roac"].dropna()
    else:
        monthly_returns_active = pd.Series(dtype=float)

    open_options_df = build_open_options_frame(open_option_lots)

    tickers_to_price = sorted({lot.ticker for lot in ending_inventory}.union({lot.ticker for lot in open_option_lots}))
    live_prices, stock_price_errors, stock_summary = fetch_current_prices_yf(tickers_to_price)
    price_errors.extend(stock_price_errors)
    price_summary = {
        "stocks_requested": stock_summary.get("requested", 0),
        "stocks_fetched": stock_summary.get("fetched", 0),
    }

    unrealized_snapshot = build_dashboard_unrealized_snapshot(open_option_lots, ending_inventory, live_prices)
    inv_df = unrealized_snapshot["inv_df"]
    per_ticker_unreal = unrealized_snapshot["per_ticker_unreal"]
    total_unreal = unrealized_snapshot["total_unreal"]
    stock_unreal = unrealized_snapshot["stock_unreal"]
    option_unreal = unrealized_snapshot["option_unreal"]
    missing_required_price_tickers = unrealized_snapshot["missing_required_price_tickers"]
    monthly_returns_unrealized_adjusted = build_dashboard_unrealized_adjusted_return_series(
        monthly_returns,
        capital_daily,
        as_of_ts,
        include_unrealized_current_year,
        total_unreal,
        unrealized_snapshot["unrealized_blocked"],
    )

    coverage_gaps = []
    if price_summary["stocks_fetched"] < price_summary["stocks_requested"]:
        coverage_gaps.append(f"Stocks priced: {price_summary['stocks_fetched']}/{price_summary['stocks_requested']}")
    if coverage_gaps:
        issues.append("Price coverage incomplete: " + "; ".join(coverage_gaps))
    if missing_required_price_tickers:
        issues.append(
            "Current unrealized snapshot incomplete: missing required prices for "
            + ", ".join(missing_required_price_tickers)
        )
    if historical_price_errors:
        issues.extend([f"Historical price error: {e}" for e in historical_price_errors])
    if capital_history_state["capital_history_incomplete"]:
        capital_history_issue_text = "; ".join(
            f"{item['ticker']} ({pd.to_datetime(item['start_date']).date()} to {pd.to_datetime(item['end_date']).date()})"
            for item in capital_history_state["capital_history_coverage_issues"]
        )
        issues.append(
            "Historical capital price coverage incomplete: "
            + capital_history_issue_text
            + ". Denominator-based return metrics are suppressed for affected periods."
        )
    if not dividend_coverage_complete:
        if dividend_affected_tickers:
            issues.append(
                "Dividend data incomplete for "
                + ", ".join(dividend_affected_tickers)
                + ". Realized P&L and return metrics remain visible but may understate dividends."
            )
        else:
            issues.append(
                "Dividend data incomplete. Realized P&L and return metrics remain visible but may understate dividends."
            )
    if price_errors:
        issues.extend([f"Price error: {e}" for e in price_errors])

    yearly = yearly_summary_from_monthly(monthly_summary, capital_daily, as_of_ts)
    twr_annualized = twr_annualized_by_year(monthly_returns.dropna())
    if not twr_annualized.empty:
        yearly = yearly.merge(twr_annualized.rename("annualized_return_twr"), left_on="year", right_index=True, how="left")
    twr_annualized_unrealized_adjusted = (
        twr_annualized_by_year(monthly_returns_unrealized_adjusted.dropna())
        if hasattr(monthly_returns_unrealized_adjusted.index, "year")
        else pd.Series(dtype=float)
    )
    if not unrealized_snapshot["unrealized_blocked"] and not twr_annualized_unrealized_adjusted.empty:
        yearly = yearly.merge(
            twr_annualized_unrealized_adjusted.rename("annualized_return_twr_unrealized_adjusted"),
            left_on="year",
            right_index=True,
            how="left",
        )
    elif include_unrealized_current_year:
        yearly["annualized_return_twr_unrealized_adjusted"] = np.nan
    twr_active = twr_annualized_by_year(monthly_returns_active.dropna())
    if not twr_active.empty:
        yearly = yearly.merge(twr_active.rename("annualized_return_twr_active"), left_on="year", right_index=True, how="left")
    if capital_history_state["capital_history_incomplete"] and not yearly.empty:
        affected_years = set(capital_history_state["capital_history_affected_years"])
        affected_year_mask = yearly["year"].isin(affected_years)
        for col in (
            "roac_year",
            "ropc_year",
            "ann_roac",
            "ann_ropc",
            "annualized_return_twr",
            "annualized_return_twr_active",
            "annualized_return_twr_unrealized_adjusted",
        ):
            if col not in yearly.columns:
                yearly[col] = np.nan
            yearly.loc[affected_year_mask, col] = np.nan

    yearly_with_unreal = build_yearly_with_dashboard_unrealized(
        yearly,
        include_unrealized_current_year,
        total_unreal,
        as_of_ts,
        unrealized_snapshot["unrealized_blocked"],
    )

    per_ticker = per_ticker_yearly_from_realized(realized_option_events, realized_sales, as_of_ts)
    per_ticker_totals = build_per_ticker_totals(per_ticker, per_ticker_unreal)

    cumulative_realized = float(monthly_summary["total_realized_pnl"].sum()) if not monthly_summary.empty else 0.0
    grand_total = cumulative_realized + total_unreal

    # Benchmarks using monthly returns alignment (clip to as_of)
    benchmark_tickers = {"Cboe BXM": "^BXM", "PUTW ETF": "PUTW", "SCHD ETF": "SCHD"}
    strat_rets = monthly_returns_covered.copy()
    if not strat_rets.empty:
        strat_rets.index = pd.to_datetime(strat_rets.index).to_period("M").to_timestamp("M")
        strat_rets = strat_rets[strat_rets.index <= as_of_ts.normalize()]
    aligned_bench_returns = align_benchmarks_monthly(benchmark_tickers, strat_rets.index if not strat_rets.empty else pd.DatetimeIndex([]))
    benchmark_metrics_rows = []
    # Limit to last 12 months for risk metrics (Sharpe/Vol/Sortino/DD)
    strat_for_metrics = strat_rets.tail(12) if not strat_rets.empty else strat_rets
    strat_full = calculate_performance_metrics(strat_rets)
    strat_risk = calculate_performance_metrics(strat_for_metrics)
    strategy_row = {"Series": "My Strategy", **strat_full, **period_returns(strat_rets)}
    # Override risk fields with last-12m values
    for key in ["Volatility", "Sharpe", "Sortino", "Max Drawdown"]:
        if key in strat_risk:
            strategy_row[key] = strat_risk[key]
    benchmark_metrics_rows.append(strategy_row)
    for name, rets in aligned_bench_returns.items():
        rets = rets.copy()
        rets.index = pd.to_datetime(rets.index, errors="coerce")
        rets = rets[rets.index.notna()].sort_index()
        observed_rets = rets.dropna()
        full = calculate_performance_metrics(observed_rets)
        risk = calculate_performance_metrics(rets.tail(12).dropna())
        row = {"Series": name, **full, **period_returns(rets)}
        for key in ["Volatility", "Sharpe", "Sortino", "Max Drawdown"]:
            if key in risk:
                row[key] = risk[key]
        benchmark_metrics_rows.append(row)
    benchmark_metrics_df = pd.DataFrame(benchmark_metrics_rows)

    return {
        "df_opts": df_opts,
        "lots": all_option_lots,
        "stock_txns": stock_txns,
        "realized_sales": realized_sales,
        "ending_inventory": ending_inventory,
        "capital_daily": capital_daily,
        "monthly_cycles": monthly_summary,
        "monthly_returns_w_div": monthly_returns,
        "monthly_returns_covered": monthly_returns_covered,
        "monthly_returns_unrealized_adjusted": monthly_returns_unrealized_adjusted,
        "monthly_returns_active": monthly_returns_active,
        "open_options": open_options_df,
        "live_prices": live_prices,
        "inv_df": inv_df,
        "total_unreal": total_unreal,
        "option_unreal": option_unreal,
        "stock_unreal": stock_unreal,
        "advanced_unreal": per_ticker_unreal,
        "yearly": yearly,
        "yearly_with_unreal": yearly_with_unreal,
        "per_ticker": per_ticker,
        "div_df": div_df,
        "as_of": as_of_ts,
        "issues": issues,
        "price_errors": price_errors,
        "unrealized_blocked": unrealized_snapshot["unrealized_blocked"],
        "missing_required_price_tickers": missing_required_price_tickers,
        "price_summary": price_summary,
        "historical_price_summary": historical_price_summary,
        "historical_price_errors": historical_price_errors,
        "dividend_coverage_complete": dividend_coverage_complete,
        "dividend_attempted_tickers": dividend_attempted_tickers,
        "dividend_failed_tickers": dividend_failed_tickers,
        "dividend_affected_tickers": dividend_affected_tickers,
        "dividend_errors": dividend_errors,
        "dividend_summary": dividend_summary,
        "stock_prices": live_prices,
        "benchmark_metrics": benchmark_metrics_df,
        "aligned_bench_returns": aligned_bench_returns,
        "per_ticker_totals": per_ticker_totals,
        "grand_total": grand_total,
        "cumulative_realized": cumulative_realized,
        "realized_option_events": realized_option_events,
        "chain_outcomes": chain_outcomes,
        "sheet_counts": sheet_counts,
        "capital_history_incomplete": capital_history_state["capital_history_incomplete"],
        "capital_history_coverage_issues": capital_history_state["capital_history_coverage_issues"],
        "capital_history_affected_months": capital_history_state["capital_history_affected_months"],
        "capital_history_affected_years": capital_history_state["capital_history_affected_years"],
        "capital_history_affected_tickers": capital_history_state["capital_history_affected_tickers"],
        "first_incomplete_return_month": covered_return_state["first_incomplete_month"],
        "last_complete_return_month": covered_return_state["last_complete_month"],
        "return_series_truncated": covered_return_state["truncated"],
    }


def main():
    st.title("Options ROI Dashboard")
    st.caption("Live from Google Sheets with Streamlit")

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

    available_sheets = list_option_sheets(SHEET_ID)
    saved_sheets = prefs.get("selected_sheets") or []
    saved_sheets = [s for s in saved_sheets if s in available_sheets]
    default_sheets = saved_sheets or [s for s in available_sheets if s in SHEETS] or available_sheets
    st.session_state["selected_sheets"] = st.session_state.get("selected_sheets", default_sheets)
    with tabs_area:
        tabs = ["Yearly", "Monthly cycles", "Per ticker", "Positions", "Config", "Logs / data issues", "Methodology"]
        tab_yearly, tab_monthly, tab_ticker, tab_positions, tab_config, tab_logs, tab_method = st.tabs(tabs)
        with tab_config:
            st.markdown("##### Data sources")
            col_refresh, col_status = st.columns([1, 2])
            with col_refresh:
                if st.button("Reload data from Google Drive", key="reload_drive_data"):
                    _clear_data_caches()
                    _rerun_app()
                st.caption("Clears cached sheet data and forces a fresh download.")
            with col_status:
                _render_data_status(SHEET_ID)
            selected_sheets = st.multiselect(
                "Sheets to include (Options YYYY):",
                options=available_sheets,
                default=st.session_state.get("selected_sheets", default_sheets),
                key="selected_sheets",
            )
            if not selected_sheets:
                st.warning("Select at least one sheet to run the dashboard.")
            st.caption("Any sheet named like `Options 2022`, `Options 2023`, etc., can be included.")
    selected_sheets = st.session_state.get("selected_sheets", default_sheets) or default_sheets

    # Persist prefs if changed
    new_prefs = {
        "include_unrealized": bool(st.session_state.get("include_unrealized", False)),
        "selected_sheets": selected_sheets,
    }
    if new_prefs != prefs:
        save_prefs(new_prefs)

    # cache_bust is kept for API compatibility; build_pipeline no longer cached
    try:
        state = build_pipeline(as_of_input, include_unrealized, selected_sheets, cache_bust=4)
    except Exception as e:
        st.error(
            "Could not load data. If the sheet is private, set `GOOGLE_SERVICE_ACCOUNT_JSON` (or `LOCAL_SECRETS_PATH`); "
            "if it's public, ensure sharing is enabled. You can also set `LOCAL_EXCEL_PATH` to a local workbook. "
            f"Details: {e}"
        )
        st.stop()
    yearly = state["yearly_with_unreal"] if include_unrealized else state["yearly"]
    monthly_cycles = state["monthly_cycles"]

    as_of_year = state["as_of"].year
    ytd_row = yearly[yearly["year"] == as_of_year]
    ytd_row = ytd_row.iloc[0] if not ytd_row.empty else pd.Series(
        {
            "total_realized_pnl": 0.0,
            "ann_roac": pd.NA,
            "annualized_return_twr": pd.NA,
        }
    )
    realized_total = float(ytd_row.get("total_realized_pnl", 0.0) or 0.0)
    ytd_total = realized_total + (state["total_unreal"] if include_unrealized else 0.0)
    twr_field = "annualized_return_twr_unrealized_adjusted" if include_unrealized else "annualized_return_twr"
    ytd_twr = ytd_row.get(twr_field, pd.NA)
    issues = state.get("issues", [])
    price_errors = state.get("price_errors", [])
    unrealized_blocked = state.get("unrealized_blocked", False)
    missing_required_price_tickers = state.get("missing_required_price_tickers", [])
    price_summary = state.get("price_summary", {})
    capital_history_incomplete = state.get("capital_history_incomplete", False)
    capital_history_coverage_issues = state.get("capital_history_coverage_issues", [])
    capital_history_affected_years = set(state.get("capital_history_affected_years", []))
    dividend_coverage_complete = state.get("dividend_coverage_complete", True)
    dividend_affected_tickers = state.get("dividend_affected_tickers", [])
    dividend_errors = state.get("dividend_errors", [])
    monthly_returns_covered = state.get("monthly_returns_covered", pd.Series(dtype=float))
    first_incomplete_return_month = state.get("first_incomplete_return_month")
    last_complete_return_month = state.get("last_complete_return_month")
    return_series_truncated = state.get("return_series_truncated", False)
    covered_period_note = None
    if return_series_truncated and pd.notna(last_complete_return_month) and pd.notna(first_incomplete_return_month):
        covered_period_note = (
            "Return-based charts and benchmark metrics are shown through "
            f"{pd.to_datetime(last_complete_return_month).date()} only. "
            "Later periods are incomplete due to missing historical capital prices and are excluded."
        )
    elif capital_history_incomplete and monthly_returns_covered.empty:
        covered_period_note = (
            "No fully covered return period is available because historical capital price coverage is incomplete."
        )
    dividend_warning_note = None
    if not dividend_coverage_complete:
        if dividend_affected_tickers:
            dividend_warning_note = (
                "Dividend data is incomplete for "
                + ", ".join(dividend_affected_tickers)
                + ". Realized P&L and return metrics remain visible but may understate dividends."
            )
        else:
            dividend_warning_note = (
                "Dividend data is incomplete. Realized P&L and return metrics remain visible but may understate dividends."
            )

    with snapshot_area:
        with col_main:
            st.markdown("#### Portfolio Snapshot")
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
                    else f"${state['total_unreal']:,.0f} (opt ${state.get('option_unreal', 0.0):,.0f} / stk ${state.get('stock_unreal', 0.0):,.0f})"
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

    with tab_yearly:
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
            use_container_width=True,
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
            use_container_width=True,
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
            use_container_width=True,
        )

        # Benchmark metrics
        st.markdown("##### Key Performance Metrics (vs. Benchmarks)")
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
                ),
                use_container_width=True,
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
            aligned_bench = state.get("aligned_bench_returns", {})
            strat_curve = (1 + monthly_returns_covered).cumprod() if not monthly_returns_covered.empty else pd.Series(dtype=float)
            if not strat_curve.empty:
                strat_curve.index = pd.to_datetime(strat_curve.index).to_period("M").to_timestamp("M")
            curves = []
            if not strat_curve.empty:
                curves.append(pd.DataFrame({"Date": strat_curve.index, "Series": "My Strategy", "Growth": strat_curve.values}))
            for name, series in aligned_bench.items():
                if not series.empty:
                    curves.append(pd.DataFrame({"Date": series.index, "Series": name, "Growth": (1 + series.fillna(0)).cumprod().values}))
            if curves:
                eq_df = pd.concat(curves, ignore_index=True)
                eq_df = filter_df_to_range(eq_df, "Date", state["as_of"], range_choice)
                if not eq_df.empty:
                    eq_df = eq_df.sort_values(["Series", "Date"])
                    eq_df["Growth"] = eq_df["Growth"] / eq_df.groupby("Series")["Growth"].transform(lambda s: s.iloc[0] if len(s) else np.nan)
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
                    st.altair_chart(chart, use_container_width=True)

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
                st.altair_chart(bar, use_container_width=True)

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
                st.altair_chart(ret_chart, use_container_width=True)
        elif capital_history_incomplete:
            st.info("No complete monthly return segment is available to chart.")

    with tab_monthly:
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
            use_container_width=True,
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
            st.altair_chart(curve_chart, use_container_width=True)

    with tab_ticker:
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
            use_container_width=True,
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
                use_container_width=True,
            )

    with tab_positions:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### Assigned holdings (inventory)")
            inv_df = state["inv_df"].copy()
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
            if "Source" in inv_df.columns:
                inv_df = inv_df[inv_df["Source"].isin(["stock_lot", "put_gap"])]
            st.dataframe(
                _format_df(
                    inv_df,
                    currency_cols=["Unrealized P&L"],
                    float_cols=["Cost/share", "Current price", "Covered strike"],
                    int_cols=["Shares", "Covered shares"],
                ),
                use_container_width=True,
            )
        with c2:
            st.markdown("##### Open option shorts")
            if state["open_options"].empty:
                st.info("No open short options.")
            else:
                oo = state["open_options"].copy()
                stock_prices = state.get("stock_prices") or {}
                oo["current_price"] = oo["ticker"].map(stock_prices)
                strike_num = pd.to_numeric(oo["strike"], errors="coerce")
                current_num = pd.to_numeric(oo["current_price"], errors="coerce")
                valid_moneyness = strike_num.notna() & current_num.notna() & (strike_num != 0)
                oo["moneyness_pct"] = np.nan
                put_mask = (oo["type"] == "Put") & valid_moneyness
                call_mask = (oo["type"] == "Call") & valid_moneyness
                oo.loc[put_mask, "moneyness_pct"] = (strike_num[put_mask] - current_num[put_mask]) / strike_num[put_mask]
                oo.loc[call_mask, "moneyness_pct"] = (current_num[call_mask] - strike_num[call_mask]) / strike_num[call_mask]
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
                st.dataframe(
                    _format_df(
                        oo,
                        pct_cols=["Moneyness %"],
                        float_cols=["Strike", "Open price", "Current price"],
                        int_cols=["Qty"],
                    ).apply(_highlight_short_option_price, axis=1),
                    use_container_width=True,
                )

    with tab_logs:
        st.markdown("##### Data / connectivity issues")
        st.write(f"Build version: {APP_BUILD_VERSION}")
        st.caption(
            "Secrets key used: `GOOGLE_SERVICE_ACCOUNT_JSON`. Public sheets load without credentials; "
            "private sheets need a service account. Offline fallback: set env "
            "`LOCAL_EXCEL_PATH=/full/path/to/IBKR_Portfolio_sheets.xlsx` when running locally."
        )
        coverage_problem = price_summary and (
            price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
        )
        if issues or price_errors or coverage_problem or (not dividend_coverage_complete):
            if issues:
                st.warning("Issues:")
                st.dataframe(pd.DataFrame({"message": issues}), use_container_width=True)
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
                    use_container_width=True,
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
                    use_container_width=True,
                )
            if price_errors:
                st.write("Price fetch issues:")
                st.dataframe(pd.DataFrame({"error": price_errors}), use_container_width=True)
            historical_price_errors = state.get("historical_price_errors", [])
            if historical_price_errors:
                st.write("Historical price fetch issues:")
                st.dataframe(pd.DataFrame({"error": historical_price_errors}), use_container_width=True)
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
                st.dataframe(coverage_df, use_container_width=True)
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
                    use_container_width=True,
                )
            if dividend_errors:
                st.write("Dividend fetch issues:")
                st.dataframe(pd.DataFrame({"error": dividend_errors}), use_container_width=True)
            if dividend_warning_note:
                st.info(dividend_warning_note)
        else:
            st.success("No issues detected.")
        if state.get("stock_prices"):
            st.write("Stock prices used:")
            st.dataframe(
                pd.DataFrame(
                    [{"ticker": k, "price": v} for k, v in state["stock_prices"].items()]
                ).sort_values("ticker"),
                use_container_width=True,
            )
        if state.get("advanced_unreal") is not None and not getattr(state.get("advanced_unreal"), "empty", True):
            st.write("Unrealized by ticker (options/stocks):")
            adv_df = state["advanced_unreal"].reset_index()
            adv_df.columns = ["ticker", "unrealized_pnl"]
            st.dataframe(_format_df(adv_df, currency_cols=["unrealized_pnl"]), use_container_width=True)
        if state.get("sheet_counts") is not None:
            st.markdown("##### Loaded rows by sheet")
            st.dataframe(state["sheet_counts"], use_container_width=True)
        st.markdown("---")
        st.markdown("##### Debug / raw data")
        st.write("Options raw", state["df_opts"].head())
        st.write("Capital daily tail", state["capital_daily"].tail())
        st.write("Dividends", state["div_df"].head())

    with tab_method:
        st.markdown("##### How we compute the numbers")
        st.markdown(
            """
**Scope & sources**
- Sheets included: whichever `Options YYYY` tabs you pick in Config. Rows outside those sheets are ignored.
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
- Date parsing depends on sheet date fields being parseable; bad rows go to Issues.
- Mixed legs (“Put/Call”, “Call/Put”) infer the short leg via type/comment heuristics.
            """
        )


if __name__ == "__main__":
    main()
