import io
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
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
import streamlit as st
import altair as alt
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

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
SHEETS = ["Options 2024", "Options 2025"]
CONTRACT_MULTIPLIER = 100


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


@st.cache_data(show_spinner=False)
def _download_excel(sheet_id: str) -> bytes:
    override = os.getenv("LOCAL_EXCEL_PATH")
    if override:
        p = Path(override).expanduser()
        if not p.exists():
            raise RuntimeError(f"LOCAL_EXCEL_PATH is set but file not found: {p}")
        return p.read_bytes()

    creds = _load_credentials()
    authed = AuthorizedSession(creds)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    resp = authed.get(url)
    resp.raise_for_status()
    return resp.content


@st.cache_data(show_spinner=True)
def load_options(sheet_id: str, sheets: List[str]) -> pd.DataFrame:
    excel_bytes = _download_excel(sheet_id)
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
            df[d] = pd.to_datetime(df[d], errors="coerce").dt.tz_localize(None)
        for n in ["strike", "qty", "amount", "commission", "total_pnl"]:
            df[n] = pd.to_numeric(df[n], errors="coerce")
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["action"] = df["action"].astype(str).str.title().str.strip()
        df["type"] = df["type"].astype(str).str.title().str.strip()
        df["comment"] = df["comment"].astype(str)
        if "assigned_flag" in df.columns:
            df["assigned_flag"] = pd.to_numeric(df["assigned_flag"], errors="coerce").fillna(0).astype(float)
        df["source_sheet"] = sh
        frames.append(df)
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all[df_all["action"].isin(["Sell", "Buy"])]
    return df_all


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


def infer_mixed_short_leg(row: pd.Series) -> Tuple[str, float]:
    t_low = str(row.get("type", "")).lower()
    c_low = str(row.get("comment", "")).lower()
    a, b = parse_strike_pair(row.get("strike", ""))
    put_strike = call_strike = math.nan
    if "put/call" in t_low:
        put_strike, call_strike = a, b
    elif "call/put" in t_low:
        call_strike, put_strike = a, b
    if ("short call" in c_low) or ("long put" in c_low):
        return "Call", call_strike
    return "Put", put_strike


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


def build_option_trades(df: pd.DataFrame) -> List[OptionTrade]:
    trades: List[OptionTrade] = []
    rows = df.sort_values(["ticker", "trans_date"]).reset_index(drop=True)
    # Pre-count sells per option key to ignore standalone long buys (protective hedges)
    sell_counts: Dict[Tuple, int] = defaultdict(int)
    for r in rows.itertuples(index=False):
        t_raw = str(r.type).strip()
        action = r.action
        strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        otype = None
        if t_raw in ("Put", "Call"):
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
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
        strike_val = float(r.strike) if pd.notna(r.strike) else math.nan
        otype = None
        if t_raw in ("Put", "Call"):
            otype = t_raw
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            leg, inferred_strike = infer_mixed_short_leg(r._asdict())
            if pd.notna(inferred_strike):
                otype = leg
                strike_val = float(inferred_strike)
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
            all_lots.append(lot)
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
                lot.qty -= take
                lot.close_date = pd.to_datetime(t.date)
                lot.close_price = t.price
                lot.close_reason = "close"
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
                lot.close_date = close_date
                lot.close_price = 0.0
                lot.close_reason = reason
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
                open_lots.append(lot)
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
            price_on_day = None
            if px_series is not None:
                try:
                    price_on_day = float(px_series.get(d, np.nan))
                except Exception:
                    price_on_day = np.nan
            if pd.isna(price_on_day):
                price_on_day = seg.cost_per_share
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
    combined = combined[combined.index <= as_of.normalize()].sort_index()
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


def twr_annualized_by_year(ret_series):
    if ret_series.empty or not hasattr(ret_series.index, "year"):
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
            monthly_px = px.resample("M").last()
            monthly_ret = monthly_px.pct_change().dropna()
            monthly_ret = monthly_ret.reindex(idx, method="ffill")
            aligned[name] = monthly_ret
        except Exception:
            continue
    return aligned


def period_returns(ret_series: pd.Series):
    out = {}
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return out
    srt = ret_series.sort_index()
    def trailing_n(n):
        sub = srt.tail(n)
        return (1 + sub).prod() - 1 if len(sub) else np.nan
    out["Return 3M"] = trailing_n(3)
    out["Return 6M"] = trailing_n(6)
    out["Return 1Y"] = trailing_n(12)
    latest_year = srt.index.max().year
    ytd = srt[srt.index.year == latest_year]
    out["Return YTD"] = (1 + ytd).prod() - 1 if not ytd.empty else np.nan
    out["Return SI"] = (1 + srt).prod() - 1 if len(srt) else np.nan
    return out


def capital_stats_by_year(capital_daily: pd.DataFrame) -> pd.DataFrame:
    df = capital_daily.reset_index()
    df["year"] = df["date"].dt.year
    return df.groupby("year").agg(avg_capital=("total", "mean"), peak_capital=("total", "max")).reset_index()


APP_BUILD_VERSION = "2025-11-30T22:42:00Z"


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


def fetch_price_history_yf(tickers, start: pd.Timestamp, end: pd.Timestamp) -> Dict[str, pd.Series]:
    """Daily close prices per ticker between start and end (inclusive end)."""
    history: Dict[str, pd.Series] = {}
    if yf is None:
        return history
    tickers = sorted({t for t in tickers if t})
    if not tickers or pd.isna(start) or pd.isna(end):
        return history
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
    except Exception:
        return history
    # normalize date index
    for t, s in list(history.items()):
        s.index = pd.to_datetime(s.index).normalize()
        history[t] = s
    return history


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


def unrealized_as_of(inventory: List[OpenLot], prices: Dict[str, float]) -> Tuple[pd.DataFrame, float]:
    rows = []
    total_unreal = 0.0
    for lot in inventory:
        px = prices.get(lot.ticker)
        if px is None or pd.isna(px) or px <= 0:
            continue
        unreal = (px - lot.cost_per_share) * lot.shares_remaining
        rows.append(
            {
                "ticker": lot.ticker,
                "buy_date": lot.buy_date.date(),
                "shares": lot.shares_remaining,
                "cost_per_share": lot.cost_per_share,
                "current_price": px,
                "unrealized_pnl": unreal,
            }
        )
        total_unreal += unreal
    return pd.DataFrame(rows), total_unreal


def find_open_options(df_opts: pd.DataFrame, as_of_date: pd.Timestamp):
    df_opts_clean = df_opts[pd.to_numeric(df_opts["strike"], errors="coerce").notna()].copy()
    df_opts_clean["trans_date"] = pd.to_datetime(df_opts_clean["trans_date"])
    df_opts_clean["expiration"] = pd.to_datetime(df_opts_clean["expiration"])
    sells = df_opts_clean[df_opts_clean["action"] == "Sell"].copy()
    buys = df_opts_clean[df_opts_clean["action"] == "Buy"].copy()
    open_options = []
    for _, sell in sells.iterrows():
        if sell["expiration"] > as_of_date:
            matching_buy = buys[
                (buys["ticker"] == sell["ticker"])
                & (buys["type"] == sell["type"])
                & (buys["strike"] == sell["strike"])
                & (buys["expiration"] == sell["expiration"])
                & (buys["trans_date"] > sell["trans_date"])
            ]
            if matching_buy.empty:
                open_options.append(sell)
    return pd.DataFrame(open_options)


def fetch_current_option_prices_yf(open_opts_df):
    """Fetch option prices for open short positions; return prices, errors, and coverage summary."""
    if yf is None:
        return {}, ["yfinance not installed; cannot fetch live option prices."], {"requested": 0, "fetched": 0}
    if open_opts_df.empty:
        return {}, [], {"requested": 0, "fetched": 0}
    prices = {}
    errors: List[str] = []
    symbols = []
    for _, opt in open_opts_df.iterrows():
        if pd.isna(opt["strike"]):
            continue
        try:
            ticker = opt["ticker"]
            exp_str = opt["expiration"].strftime("%y%m%d")
            opt_type = opt["type"][0].upper()
            strike_str = f"{int(opt['strike'] * 1000):08d}"
            option_symbol = f"{ticker}{exp_str}{opt_type}{strike_str}"
            symbols.append(option_symbol)
            price = yf.Ticker(option_symbol).fast_info.get("lastPrice")
            if price is not None and price > 0:
                prices[option_symbol] = price
        except Exception as exc:
            errors.append(f"{opt.get('ticker', 'UNK')}: {exc}")
    missing = [s for s in symbols if s not in prices]
    if missing:
        errors.append(f"Missing option prices for: {', '.join(missing[:10])}" + (" ..." if len(missing) > 10 else ""))
    summary = {"requested": len(symbols), "fetched": len(prices)}
    return prices, errors, summary


def calculate_advanced_unrealized_pnl(ending_inventory, open_options, live_stock_prices, live_option_prices):
    unrealized_rows = []
    for lot in ending_inventory:
        current_price = live_stock_prices.get(lot.ticker, 0)
        if current_price > 0:
            pnl = (current_price - lot.cost_per_share) * lot.shares_remaining
            unrealized_rows.append({"ticker": lot.ticker, "unrealized_pnl": pnl})
    for _, opt in open_options.iterrows():
        if pd.isna(opt["strike"]):
            continue
        ticker = opt["ticker"]
        exp_str = opt["expiration"].strftime("%y%m%d")
        opt_type = opt["type"][0].upper()
        strike_str = f"{int(opt['strike'] * 1000):08d}"
        option_symbol = f"{ticker}{exp_str}{opt_type}{strike_str}"
        current_option_price_per_share = live_option_prices.get(option_symbol, 0)
        if current_option_price_per_share > 0:
            pnl = -(current_option_price_per_share * CONTRACT_MULTIPLIER * opt["qty"])
            unrealized_rows.append({"ticker": opt["ticker"], "unrealized_pnl": pnl})
    if not unrealized_rows:
        return pd.Series(dtype=float)
    return pd.DataFrame(unrealized_rows).groupby("ticker")["unrealized_pnl"].sum()


def _format_df(df: pd.DataFrame, currency_cols=None, pct_cols=None, int_cols=None, hide_index=False):
    df = df.copy()
    formatter = {}
    if currency_cols:
        formatter.update({c: "{:,.0f}".format for c in currency_cols if c in df.columns})
    if pct_cols:
        formatter.update({c: "{:.1%}".format for c in pct_cols if c in df.columns})
    if int_cols:
        formatter.update({c: "{:.0f}".format for c in int_cols if c in df.columns})
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


def build_pipeline(as_of: date, include_unrealized_current_year: bool, cache_bust: int = 1):
    df_opts = load_options(SHEET_ID, SHEETS)
    today_norm = pd.Timestamp.today().normalize()
    as_of_ts = min(pd.Timestamp(as_of), today_norm)
    issues: List[str] = []
    price_errors: List[str] = []

    df_opts = df_opts[df_opts["trans_date"] <= as_of_ts].copy()

    trades = build_option_trades(df_opts)
    realized_option_events, open_option_lots, stock_txns, trade_issues, all_option_lots = process_option_positions(trades, as_of_ts)
    issues.extend(trade_issues)
    realized_sales, ending_inventory = compute_stock_realized_and_inventory(stock_txns, issues)
    chain_outcomes = build_chains(stock_txns, realized_option_events, as_of_ts)
    start_date = df_opts["trans_date"].min() if not df_opts.empty else as_of_ts
    price_history = fetch_price_history_yf({t.ticker for t in stock_txns}, pd.to_datetime(start_date).normalize(), as_of_ts.normalize()) if pd.notna(start_date) else {}
    capital_daily = build_capital_timeline(all_option_lots, stock_txns, as_of_ts, df_opts, price_history)

    div_df = pd.DataFrame()
    if yf is not None:
        try:
            segs = build_holding_segments(stock_txns, as_of_ts)
            if segs:
                by_ticker = defaultdict(list)
                for s in segs:
                    by_ticker[s.ticker].append((pd.to_datetime(s.start).normalize(), pd.to_datetime(s.end).normalize(), s.shares))
                div_rows = []
                for ticker, seg_list in by_ticker.items():
                    try:
                        div_hist = yf.Ticker(ticker).dividends
                        if div_hist.empty:
                            continue
                        div_hist.index = pd.to_datetime(div_hist.index).tz_localize(None).normalize()
                        for start, end, shares in seg_list:
                            divs_in_period = div_hist[(div_hist.index >= start) & (div_hist.index < end)]
                            for pay_date, per_share in divs_in_period.items():
                                div_rows.append(
                                    {
                                        "ticker": ticker,
                                        "ex_date": pay_date,
                                        "pay_date": pay_date,
                                        "per_share": per_share,
                                        "shares": shares,
                                        "cash": per_share * shares,
                                    }
                                )
                    except Exception:
                        continue
                div_df = pd.DataFrame(div_rows)
        except Exception:
            div_df = pd.DataFrame()

    monthly_summary = build_monthly_summary(realized_option_events, realized_sales, capital_daily, div_df, as_of_ts)
    monthly_returns = monthly_summary["roac"].dropna() if "roac" in monthly_summary else pd.Series(dtype=float)

    open_options_df = pd.DataFrame(
        [
            {
                "ticker": lot.ticker,
                "type": lot.otype,
                "strike": lot.strike,
                "qty": lot.qty,
                "expiration": lot.expiration,
                "trans_date": lot.open_date,
                "open_price": lot.open_price,
            }
            for lot in open_option_lots
        ]
    )

    tickers_to_price = sorted({lot.ticker for lot in ending_inventory}.union({lot.ticker for lot in open_option_lots}))
    live_prices, stock_price_errors, stock_summary = fetch_current_prices_yf(tickers_to_price)
    price_errors.extend(stock_price_errors)
    price_summary = {
        "stocks_requested": stock_summary.get("requested", 0),
        "stocks_fetched": stock_summary.get("fetched", 0),
    }

    inv_df, per_ticker_unreal, total_unreal = calculate_unrealized_positions(open_option_lots, ending_inventory, live_prices)
    stock_unreal = float(inv_df["unrealized_pnl"].sum()) if not inv_df.empty else 0.0
    option_unreal = total_unreal - stock_unreal

    coverage_gaps = []
    if price_summary["stocks_fetched"] < price_summary["stocks_requested"]:
        coverage_gaps.append(f"Stocks priced: {price_summary['stocks_fetched']}/{price_summary['stocks_requested']}")
    if coverage_gaps:
        issues.append("Price coverage incomplete: " + "; ".join(coverage_gaps))
    if price_errors:
        issues.extend([f"Price error: {e}" for e in price_errors])

    yearly = yearly_summary_from_monthly(monthly_summary, capital_daily, as_of_ts)
    twr_annualized = twr_annualized_by_year(monthly_returns.dropna())
    if not twr_annualized.empty:
        yearly = yearly.merge(twr_annualized.rename("annualized_return_twr"), left_on="year", right_index=True, how="left")

    yearly_with_unreal = yearly.copy()
    yearly_with_unreal["total_pnl_incl_unreal"] = yearly_with_unreal.get("total_realized_pnl", pd.Series(dtype=float))
    if include_unrealized_current_year and total_unreal != 0 and not yearly_with_unreal.empty:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        yearly_with_unreal.loc[mask_curr, "total_pnl_incl_unreal"] = yearly_with_unreal.loc[mask_curr, "total_realized_pnl"] + total_unreal

    per_ticker = per_ticker_yearly_from_realized(realized_option_events, realized_sales, as_of_ts)
    per_ticker_totals = (
        per_ticker.groupby("ticker")[["options_pnl", "stock_realized_pnl", "combined_realized"]]
        .sum()
        .reset_index()
    )
    unreal_series = per_ticker_unreal.reindex(per_ticker_totals["ticker"]).fillna(0.0) if not per_ticker_unreal.empty else 0.0
    per_ticker_totals["unrealized_pnl"] = unreal_series.values if hasattr(unreal_series, "values") else unreal_series
    per_ticker_totals["total_pnl"] = per_ticker_totals["combined_realized"] + per_ticker_totals["unrealized_pnl"]

    cumulative_realized = float(monthly_summary["total_realized_pnl"].sum()) if not monthly_summary.empty else 0.0
    grand_total = cumulative_realized + total_unreal

    # Benchmarks using monthly returns alignment (clip to as_of)
    benchmark_tickers = {"Cboe BXM": "^BXM", "PUTW ETF": "PUTW", "SCHD ETF": "SCHD"}
    strat_rets = monthly_returns.copy()
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
        rets_clean = rets.dropna()
        full = calculate_performance_metrics(rets_clean)
        risk = calculate_performance_metrics(rets_clean.tail(12))
        row = {"Series": name, **full, **period_returns(rets_clean)}
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
        "open_options": open_options_df,
        "live_prices": live_prices,
        "live_option_prices": {},
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
        "unrealized_blocked": False,
        "price_summary": price_summary,
        "stock_prices": live_prices,
        "option_prices": {},
        "benchmark_metrics": benchmark_metrics_df,
        "aligned_bench_returns": aligned_bench_returns,
        "per_ticker_totals": per_ticker_totals,
        "grand_total": grand_total,
        "cumulative_realized": cumulative_realized,
        "realized_option_events": realized_option_events,
        "chain_outcomes": chain_outcomes,
    }


def main():
    st.title("Options ROI Dashboard")
    st.caption("Live from Google Sheets with Streamlit")

    col_side, col_main = st.columns([1, 4])
    with col_side:
        as_of_input = st.date_input("As of date", value=date.today())
        include_unrealized = st.checkbox("Include unrealized in current year", value=True)
        st.markdown("Secrets key used: `GOOGLE_SERVICE_ACCOUNT_JSON`")
        st.caption("Offline fallback: set env `LOCAL_EXCEL_PATH=/full/path/to/IBKR_Portfolio_sheets.xlsx` when running locally.")

    # cache_bust is kept for API compatibility; build_pipeline no longer cached
    state = build_pipeline(as_of_input, include_unrealized, cache_bust=4)
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
    ytd_twr = ytd_row.get("annualized_return_twr", pd.NA)
    issues = state.get("issues", [])
    price_errors = state.get("price_errors", [])
    unrealized_blocked = state.get("unrealized_blocked", False)
    price_summary = state.get("price_summary", {})

    if issues:
        st.warning(f"Issues detected: {len(issues)} (see Logs tab)")
    if price_errors or (
        price_summary
        and (
            price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
        )
    ):
        st.error("Price fetch issues detected. See Logs tab for details.")

    with col_main:
        st.markdown("#### Portfolio Snapshot")
        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            metric_card("YTD Total P&L", f"${ytd_total:,.0f}", delta=None)
        with mc2:
            metric_card("YTD Realized P&L (w/ div)", f"${realized_total:,.0f}")
        with mc3:
            metric_card(
                "Unrealized P&L",
                f"${state['total_unreal']:,.0f} (opt ${state.get('option_unreal', 0.0):,.0f} / stk ${state.get('stock_unreal', 0.0):,.0f})",
            )
        with mc4:
            metric_card(
                "YTD Annualized TWR",
                f"{float(ytd_twr):.1%}" if pd.notna(ytd_twr) else "n/a",
            )

    tab_yearly, tab_monthly, tab_ticker, tab_positions, tab_logs = st.tabs(["Yearly", "Monthly cycles", "Per ticker", "Positions", "Logs / data issues"])

    with tab_yearly:
        # Comprehensive Yearly Performance (Realized View)
        st.markdown("##### Comprehensive Yearly Performance (Realized View)")
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
        }
        realized_display = yearly[[c for c in realized_cols if c in yearly.columns]].rename(columns=realized_map)
        st.dataframe(
            _format_df(
                realized_display.reset_index(drop=True),
                currency_cols=["Options P&L", "Stock P&L", "Dividends", "Realized P&L", "Avg capital", "Peak capital"],
                pct_cols=["RoAC", "RoPC", "Ann. RoAC", "Ann. RoPC", "Ann. TWR"],
                int_cols=["Year"],
                hide_index=True,
            ),
            use_container_width=True,
        )

        # Comprehensive Yearly Performance (MTM view)
        st.markdown("##### Comprehensive Yearly Performance (MTM view)")
        mtm_cols = [
            "year",
            "total_realized_pnl",
            "total_pnl_incl_unreal",
            "ann_roac",
            "ann_ropc",
            "annualized_return_twr",
        ]
        mtm_map = {
            "year": "Year",
            "total_realized_pnl": "Realized P&L",
            "total_pnl_incl_unreal": "Total P&L (incl unreal)",
            "ann_roac": "Ann. return on avg",
            "ann_ropc": "Ann. return on peak",
            "annualized_return_twr": "Ann. TWR",
        }
        mtm_source = state["yearly_with_unreal"] if include_unrealized else state["yearly"]
        mtm_display = mtm_source[[c for c in mtm_cols if c in mtm_source.columns]].rename(columns=mtm_map)
        st.dataframe(
            _format_df(
                mtm_display.reset_index(drop=True),
                currency_cols=["Realized P&L", "Total P&L (incl unreal)"],
                pct_cols=["Ann. return on avg", "Ann. return on peak", "Ann. TWR"],
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
        if not bench_df.empty:
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
                    hide_index=True,
                ),
                use_container_width=True,
            )
        else:
            st.info("Benchmark data unavailable (yfinance fetch failed).")

        # Charts
        st.markdown("##### Charts")
        aligned_bench = state.get("aligned_bench_returns", {})
        strat_curve = (1 + state["monthly_returns_w_div"]).cumprod() if not state["monthly_returns_w_div"].empty else pd.Series(dtype=float)
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
            chart = (
                alt.Chart(eq_df)
                .mark_line()
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Growth:Q", title="Cumulative growth of $1", scale=alt.Scale(nice=True)),
                    color=alt.Color("Series:N", title="Series"),
                    tooltip=["Date:T", "Series:N", alt.Tooltip("Growth:Q", format=".3f")],
                )
                .properties(height=260, title="Cumulative Growth vs Benchmarks")
            )
            st.altair_chart(chart, use_container_width=True)

        # P&L by options cycle
        if "combined_realized_m_w_div" in state["monthly_cycles"]:
            pnl_df = state["monthly_cycles"][["combined_realized_m_w_div"]].reset_index().rename(columns={"index": "cycle"})
            pnl_df = pnl_df.rename(columns={"combined_realized_m_w_div": "pnl"})
            pnl_df["color"] = np.where(pnl_df["pnl"] >= 0, "Positive", "Negative")
            bar = (
                alt.Chart(pnl_df)
                .mark_bar()
                .encode(
                    x=alt.X("cycle:T", title="Option cycle"),
                    y=alt.Y("pnl:Q", title="P&L ($)"),
                    color=alt.Color("color:N", scale=alt.Scale(domain=["Positive", "Negative"], range=["#22c55e", "#ef4444"]), legend=None),
                    tooltip=["cycle:T", alt.Tooltip("pnl:Q", format=",.0f")],
                )
                .properties(height=260, title="P&L by Options Cycle")
            )
            st.altair_chart(bar, use_container_width=True)

        # Monthly return line (strategy only)
        if not state["monthly_returns_w_div"].empty:
            ret_df = pd.DataFrame({"Date": state["monthly_returns_w_div"].index, "Return": state["monthly_returns_w_div"].values})
            ret_chart = (
                alt.Chart(ret_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Return:Q", title="Monthly return", axis=alt.Axis(format="%")),
                    tooltip=["Date:T", alt.Tooltip("Return:Q", format=".2%")],
                )
                .properties(height=220, title="Monthly Returns (RoAC)")
            )
            st.altair_chart(ret_chart, use_container_width=True)

    with tab_monthly:
        st.markdown("##### Monthly performance (calendar months)")
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
        if not state["monthly_returns_w_div"].empty:
            equity_curve = (1 + state["monthly_returns_w_div"]).cumprod()
            curve_df = pd.DataFrame(
                {
                    "Month": state["monthly_returns_w_div"].index,
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
        if not totals_df.empty:
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
                    currency_cols=["Cost/share", "Current price", "Covered strike", "Unrealized P&L"],
                    int_cols=["Shares", "Covered shares"],
                ),
                use_container_width=True,
            )
        with c2:
            st.markdown("##### Open option shorts")
            if state["open_options"].empty:
                st.info("No open short options.")
            else:
                oo = state["open_options"][["ticker", "type", "strike", "qty", "expiration", "trans_date", "open_price"]].copy()
                for dcol in ["expiration", "trans_date"]:
                    if dcol in oo.columns:
                        oo[dcol] = pd.to_datetime(oo[dcol]).dt.strftime("%Y-%m-%d")
                oo = oo.rename(
                    columns={
                        "ticker": "Ticker",
                        "type": "Type",
                        "strike": "Strike",
                        "qty": "Qty",
                        "expiration": "Expiration",
                        "trans_date": "Opened",
                        "open_price": "Open price",
                    }
                )
                st.dataframe(
                    _format_df(
                        oo,
                        currency_cols=["Strike", "Open price"],
                        int_cols=["Qty"],
                    ),
                    use_container_width=True,
                )

    with tab_logs:
        st.markdown("##### Data / connectivity issues")
        st.write(f"Build version: {APP_BUILD_VERSION}")
        coverage_problem = price_summary and (
            price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
        )
        if issues or price_errors or coverage_problem:
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
            if price_errors:
                st.write("Price fetch issues:")
                st.dataframe(pd.DataFrame({"error": price_errors}), use_container_width=True)
            if unrealized_blocked:
                st.info("Unrealized P&L and related metrics were suppressed due to missing prices.")
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
        if state.get("option_prices"):
            st.write("Option prices used:")
            st.dataframe(
                pd.DataFrame(
                    [{"symbol": k, "price": v} for k, v in state["option_prices"].items()]
                ).sort_values("symbol").head(50),
                use_container_width=True,
            )
        if state.get("advanced_unreal") is not None and not getattr(state.get("advanced_unreal"), "empty", True):
            st.write("Unrealized by ticker (options/stocks):")
            adv_df = state["advanced_unreal"].reset_index()
            adv_df.columns = ["ticker", "unrealized_pnl"]
            st.dataframe(_format_df(adv_df, currency_cols=["unrealized_pnl"]), use_container_width=True)
        st.markdown("---")
        st.markdown("##### Debug / raw data")
        st.write("Options raw", state["df_opts"].head())
        st.write("Capital daily tail", state["capital_daily"].tail())
        st.write("Dividends", state["div_df"].head())


if __name__ == "__main__":
    main()
