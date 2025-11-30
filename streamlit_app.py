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
        df["source_sheet"] = sh
        frames.append(df)
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all[df_all["action"].isin(["Sell", "Buy"])]
    return df_all


# ------------------------------------------------------------
# Domain models
# ------------------------------------------------------------
@dataclass
class OptionLot:
    ticker: str
    otype: str  # "Put" or "Call" (short leg)
    strike: float
    qty: float
    open_date: pd.Timestamp
    expiration: pd.Timestamp
    premium_net: float
    comment: str
    assigned: bool
    close_date: pd.Timestamp
    close_reason: str  # "expiration" | "closed_early" | "assigned_expiration"


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


def build_short_lots_from_rows(df: pd.DataFrame) -> List[OptionLot]:
    lots: List[OptionLot] = []
    rows = df.sort_values(["ticker", "trans_date"]).reset_index(drop=True)
    for r in rows.itertuples(index=False):
        t_raw = str(r.type).strip()
        action = r.action
        cmt = r.comment if pd.notna(r.comment) else ""
        assigned = "assigned" in cmt.lower()

        if t_raw in ("Put", "Call"):
            if action != "Sell":
                continue
            lot = OptionLot(
                ticker=r.ticker,
                otype=t_raw,
                strike=float(r.strike) if pd.notna(r.strike) else math.nan,
                qty=float(r.qty) if pd.notna(r.qty) else 0.0,
                open_date=r.trans_date,
                expiration=r.expiration,
                premium_net=float(r.total_pnl) if pd.notna(r.total_pnl) else 0.0,
                comment=cmt,
                assigned=assigned,
                close_date=r.expiration,
                close_reason="assigned_expiration" if assigned else "expiration",
            )
            lots.append(lot)
        elif ("put/call" in t_raw.lower()) or ("call/put" in t_raw.lower()):
            short_leg, short_strike = infer_mixed_short_leg(r._asdict())
            if pd.isna(short_strike):
                continue
            lot = OptionLot(
                ticker=r.ticker,
                otype=short_leg,
                strike=float(short_strike),
                qty=float(r.qty) if pd.notna(r.qty) else 0.0,
                open_date=r.trans_date,
                expiration=r.expiration,
                premium_net=float(r.total_pnl) if pd.notna(r.total_pnl) else 0.0,
                comment=cmt,
                assigned=assigned,
                close_date=r.expiration,
                close_reason="assigned_expiration" if assigned else "expiration",
            )
            lots.append(lot)
    return lots


def apply_buy_to_close_closeouts(lots: List[OptionLot], df: pd.DataFrame) -> None:
    key_to_indices: Dict[Tuple, List[int]] = defaultdict(list)
    for i, lot in enumerate(lots):
        exp = pd.to_datetime(lot.expiration)
        exp = exp.normalize() if pd.notna(exp) else pd.NaT
        key = (lot.ticker, lot.otype, lot.strike, exp)
        key_to_indices[key].append(i)

    buys = df[(df["action"] == "Buy") & (df["type"].isin(["Put", "Call"]))].copy()
    buys = buys.sort_values("trans_date")
    for _, b in buys.iterrows():
        exp = pd.to_datetime(b["expiration"])
        exp = exp.normalize() if pd.notna(exp) else pd.NaT
        key = (
            str(b["ticker"]).upper().strip(),
            str(b["type"]),
            float(b["strike"]),
            exp,
        )
        indices = key_to_indices.get(key, [])
        if not indices:
            continue
        buy_date = b["trans_date"]
        for idx in indices:
            lot = lots[idx]
            if lot.open_date <= buy_date < lot.close_date:
                lot.close_date = buy_date
                lot.close_reason = "closed_early"
                break


def stock_txns_from_assigned_lots(lots: List[OptionLot]) -> List[StockTxn]:
    txns: List[StockTxn] = []
    for lot in lots:
        if not lot.assigned:
            continue
        shares = int(round(lot.qty * CONTRACT_MULTIPLIER))
        if shares == 0:
            continue
        if lot.otype == "Put":
            txns.append(
                StockTxn(lot.close_date.normalize(), lot.ticker, "BUY", shares, lot.strike, "Assigned")
            )
        else:
            txns.append(
                StockTxn(lot.close_date.normalize(), lot.ticker, "SELL", shares, lot.strike, "Assigned")
            )
    return txns


def compute_stock_realized_and_inventory(txns: List[StockTxn]):
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
            proceeds = t.shares * t.price
            cost = cost_accum + (qty_to_sell * t.price if qty_to_sell > 0 else 0.0)
            realized.append(RealizedSale(t.date, t.ticker, t.shares, proceeds, cost, proceeds - cost))
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


def build_capital_timeline(lots: List[OptionLot], txns: List[StockTxn], as_of: pd.Timestamp, df_opts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for lot in lots:
        if lot.otype == "Put":
            open_d = lot.open_date
            close_d = pd.to_datetime(lot.close_date)
            if pd.isna(close_d):
                close_d = as_of
            else:
                close_d = min(close_d, as_of)
            reserve = lot.strike * CONTRACT_MULTIPLIER * int(round(lot.qty))
            for d in daterange_days(open_d, close_d):
                rows.append((d, "puts_reserve", reserve))
    segs = build_holding_segments(txns, as_of)
    for seg in segs:
        invested = seg.shares * seg.cost_per_share
        for d in daterange_days(seg.start, seg.end):
            rows.append((d, "shares_invested", invested))
    cap = pd.DataFrame(rows, columns=["date", "component", "amount"])
    if cap.empty:
        idx = pd.date_range(df_opts["trans_date"].min().normalize(), as_of, freq="D")
        cap = pd.DataFrame({"date": idx, "component": ["puts_reserve"] * len(idx), "amount": [0.0] * len(idx)})
    daily = cap.groupby(["date", "component"])["amount"].sum().unstack(fill_value=0.0)
    daily["total"] = daily.sum(axis=1)
    return daily


def _third_friday(dt_val):
    dt_val = pd.Timestamp(dt_val)
    return dt_val.replace(day=1) + pd.offsets.WeekOfMonth(week=2, weekday=4)


def _cycle_end(dt_val):
    dt_val = pd.Timestamp(dt_val)
    exp = _third_friday(dt_val)
    if dt_val > exp:
        exp = _third_friday(dt_val + pd.offsets.MonthBegin(1))
    return exp


def build_monthly_cycles(df_opts: pd.DataFrame, realized_sales: List[RealizedSale], capital_daily: pd.DataFrame, dividends_df: pd.DataFrame):
    def cyc(d):
        return _cycle_end(d)

    opts_cycle = df_opts.groupby(df_opts["trans_date"].apply(cyc))["total_pnl"].sum().rename("options_pnl_m")
    if realized_sales:
        rs_df = pd.DataFrame({"date": [r.date for r in realized_sales], "pnl": [r.pnl for r in realized_sales]})
        stock_cycle = rs_df.groupby(rs_df["date"].apply(cyc))["pnl"].sum().rename("stock_realized_pnl_m")
    else:
        stock_cycle = pd.Series(dtype=float, name="stock_realized_pnl_m")
    div_cycle = dividends_df.groupby(dividends_df["ex_date"].apply(cyc))["cash"].sum().rename("dividends_m") if not dividends_df.empty else pd.Series(dtype=float, name="dividends_m")
    combined = pd.concat([opts_cycle, stock_cycle, div_cycle], axis=1).fillna(0.0)
    combined["combined_realized_m"] = combined["options_pnl_m"] + combined["stock_realized_pnl_m"]
    combined["combined_realized_m_w_div"] = combined["combined_realized_m"] + combined["dividends_m"]
    cap = capital_daily.copy()
    cap["cycle"] = cap.index.to_series().apply(cyc)
    avg_cap = cap.groupby("cycle")["total"].mean().rename("avg_capital_m")
    combined = combined.join(avg_cap, how="left").fillna(0.0)
    combined["return_m"] = np.where(combined["avg_capital_m"] > 0, combined["combined_realized_m"] / combined["avg_capital_m"], np.nan)
    combined["return_m_w_div"] = np.where(combined["avg_capital_m"] > 0, combined["combined_realized_m_w_div"] / combined["avg_capital_m"], np.nan)
    return combined.sort_index()


def twr_annualized_by_year(ret_series):
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return pd.Series(dtype=float)
    grouped = ret_series.groupby(ret_series.index.year)
    return grouped.apply(lambda r: (1 + r).prod() ** (12 / len(r)) - 1)


def options_pnl_by_year(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp["year"] = tmp["trans_date"].dt.year
    return tmp.groupby("year")["total_pnl"].sum().rename("options_pnl").reset_index()


def realized_stock_pnl_by_year(realized_sales: List[RealizedSale]) -> pd.DataFrame:
    if not realized_sales:
        return pd.DataFrame(columns=["year", "stock_realized_pnl"])
    df = pd.DataFrame([{"date": r.date, "ticker": r.ticker, "shares": r.shares, "pnl": r.pnl} for r in realized_sales])
    df["year"] = df["date"].dt.year
    return df.groupby("year")["pnl"].sum().rename("stock_realized_pnl").reset_index()


def capital_stats_by_year(capital_daily: pd.DataFrame) -> pd.DataFrame:
    df = capital_daily.reset_index()
    df["year"] = df["date"].dt.year
    return df.groupby("year").agg(avg_capital=("total", "mean"), peak_capital=("total", "max")).reset_index()


def combine_yearly(options_df, realized_df, capital_df, as_of, capital_daily):
    years = sorted(set(capital_df["year"]).union(options_df["year"]).union(realized_df["year"]))
    out = pd.DataFrame({"year": years})
    out = (
        out.merge(options_df, on="year", how="left")
        .merge(realized_df, on="year", how="left")
        .merge(capital_df, on="year", how="left")
    )
    out = out.fillna(0.0)
    out["combined_realized"] = out["options_pnl"] + out["stock_realized_pnl"]
    elapsed_days = (
        pd.DataFrame({"date": capital_daily.index})
        .assign(year=lambda d: d["date"].dt.year)
        .groupby("year")["date"]
        .nunique()
        .rename("days_elapsed")
        .reset_index()
    )
    out = out.merge(elapsed_days, on="year", how="left").fillna({"days_elapsed": 365})
    out["return_on_avg"] = out["combined_realized"] / out["avg_capital"].replace({0: pd.NA})
    out["return_on_peak"] = out["combined_realized"] / out["peak_capital"].replace({0: pd.NA})
    current_year = as_of.year
    mask_curr = (out["year"] == current_year) & (out["days_elapsed"] < 365)
    factor = 365.0 / out["days_elapsed"]
    out["annualized_return_on_avg"] = out["return_on_avg"]
    out.loc[mask_curr, "annualized_return_on_avg"] = out.loc[mask_curr, "return_on_avg"] * factor[mask_curr]
    out["annualized_return_on_peak"] = out["return_on_peak"]
    out.loc[mask_curr, "annualized_return_on_peak"] = out.loc[mask_curr, "return_on_peak"] * factor[mask_curr]
    return out


def per_ticker_yearly(df_opts: pd.DataFrame, realized_sales: List[RealizedSale]) -> pd.DataFrame:
    o = df_opts.copy()
    o["year"] = o["trans_date"].dt.year
    o_t = o.groupby(["year", "ticker"])["total_pnl"].sum().rename("options_pnl").reset_index()
    if realized_sales:
        s = pd.DataFrame([{"date": r.date, "ticker": r.ticker, "pnl": r.pnl} for r in realized_sales])
        s["year"] = s["date"].dt.year
        s_t = s.groupby(["year", "ticker"])["pnl"].sum().rename("stock_realized_pnl").reset_index()
    else:
        s_t = pd.DataFrame(columns=["year", "ticker", "stock_realized_pnl"])
    out = o_t.merge(s_t, on=["year", "ticker"], how="outer").fillna(0.0)
    out["combined_realized"] = out["options_pnl"] + out["stock_realized_pnl"]
    return out.sort_values(["year", "combined_realized"], ascending=[True, False])


APP_BUILD_VERSION = "2025-11-30T18:55:00Z"


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


def _format_df(df: pd.DataFrame, currency_cols=None, pct_cols=None, int_cols=None):
    df = df.copy()
    formatter = {}
    if currency_cols:
        formatter.update({c: "{:,.0f}".format for c in currency_cols if c in df.columns})
    if pct_cols:
        formatter.update({c: "{:.1%}".format for c in pct_cols if c in df.columns})
    if int_cols:
        formatter.update({c: "{:,.0f}".format for c in int_cols if c in df.columns})
    return df.style.format(formatter).set_properties(**{"text-align": "right"})


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


@st.cache_data(show_spinner=True)
def build_pipeline(as_of: date, include_unrealized_current_year: bool, cache_bust: int = 1):
    df_opts = load_options(SHEET_ID, SHEETS)
    as_of_ts = pd.Timestamp(as_of)
    issues: List[str] = []
    price_errors: List[str] = []

    lots = build_short_lots_from_rows(df_opts)
    apply_buy_to_close_closeouts(lots, df_opts)
    stock_txns = stock_txns_from_assigned_lots(lots)
    realized_sales, ending_inventory = compute_stock_realized_and_inventory(stock_txns)
    capital_daily = build_capital_timeline(lots, stock_txns, as_of_ts, df_opts)

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
                            for ex_date, per_share in divs_in_period.items():
                                div_rows.append(
                                    {
                                        "ticker": ticker,
                                        "ex_date": ex_date,
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

    monthly_cycles = build_monthly_cycles(df_opts, realized_sales, capital_daily, div_df)
    final_monthly_returns_w_div = monthly_cycles["return_m_w_div"].dropna()
    open_options_df = find_open_options(df_opts, as_of_ts)

    tickers_to_price = sorted({lot.ticker for lot in ending_inventory})
    live_prices, stock_price_errors, stock_summary = fetch_current_prices_yf(tickers_to_price)
    live_option_prices, option_price_errors, opt_summary = fetch_current_option_prices_yf(open_options_df)
    price_errors.extend(stock_price_errors)
    price_errors.extend(option_price_errors)
    price_summary = {
        "stocks_requested": stock_summary.get("requested", 0),
        "stocks_fetched": stock_summary.get("fetched", 0),
        "options_requested": opt_summary.get("requested", 0),
        "options_fetched": opt_summary.get("fetched", 0),
    }

    # If any price errors or missing coverage, do not trust unrealized; zero it and log.
    unrealized_blocked = bool(price_errors)
    coverage_incomplete = (
        price_summary["stocks_fetched"] < price_summary["stocks_requested"]
        or price_summary["options_fetched"] < price_summary["options_requested"]
    )
    if coverage_incomplete:
        unrealized_blocked = True
    inv_df = pd.DataFrame()
    advanced_unreal = pd.Series(dtype=float)
    total_unreal = 0.0
    if not unrealized_blocked:
        inv_df, total_unreal = unrealized_as_of(ending_inventory, live_prices)
        advanced_unreal = calculate_advanced_unrealized_pnl(ending_inventory, open_options_df, live_prices, live_option_prices)
        # If we still ended up with no unrealized but had positions, treat as blocked
        if (ending_inventory and inv_df.empty) or (not open_options_df.empty and advanced_unreal.empty):
            unrealized_blocked = True
            total_unreal = 0.0
            advanced_unreal = pd.Series(dtype=float)

    if unrealized_blocked:
        issues.append("Live prices unavailable or incomplete; unrealized P&L suppressed. See Logs tab.")

    opts_year = options_pnl_by_year(df_opts)
    stock_year = realized_stock_pnl_by_year(realized_sales)
    cap_year = capital_stats_by_year(capital_daily)
    yearly = combine_yearly(opts_year, stock_year, cap_year, as_of_ts, capital_daily)
    twr_annualized = twr_annualized_by_year(monthly_cycles["return_m"].dropna())
    yearly = yearly.merge(twr_annualized.rename("annualized_return_twr"), left_on="year", right_index=True, how="left")

    yearly_with_unreal = yearly.copy()
    yearly_with_unreal["combined_incl_unreal"] = yearly_with_unreal["combined_realized"]
    yearly_with_unreal["return_on_avg_incl_unreal"] = yearly_with_unreal["return_on_avg"]
    yearly_with_unreal["return_on_peak_incl_unreal"] = yearly_with_unreal["return_on_peak"]
    yearly_with_unreal["annualized_return_on_avg_incl_unreal"] = yearly_with_unreal["annualized_return_on_avg"]
    yearly_with_unreal["annualized_return_on_peak_incl_unreal"] = yearly_with_unreal["annualized_return_on_peak"]

    if include_unrealized_current_year and total_unreal != 0:
        mask_curr = yearly_with_unreal["year"].eq(as_of_ts.year)
        if mask_curr.any():
            yearly_with_unreal.loc[mask_curr, "combined_incl_unreal"] = yearly_with_unreal.loc[mask_curr, "combined_realized"] + total_unreal
            yearly_with_unreal.loc[mask_curr, "return_on_avg_incl_unreal"] = yearly_with_unreal.loc[mask_curr, "combined_incl_unreal"] / yearly_with_unreal.loc[mask_curr, "avg_capital"].replace({0: pd.NA})
            yearly_with_unreal.loc[mask_curr, "return_on_peak_incl_unreal"] = yearly_with_unreal.loc[mask_curr, "combined_incl_unreal"] / yearly_with_unreal.loc[mask_curr, "peak_capital"].replace({0: pd.NA})
            mask = mask_curr & yearly_with_unreal["days_elapsed"].lt(365)
            factor = 365.0 / yearly_with_unreal.loc[mask, "days_elapsed"]
            yearly_with_unreal.loc[mask, "annualized_return_on_avg_incl_unreal"] = yearly_with_unreal.loc[mask, "return_on_avg_incl_unreal"] * factor
            yearly_with_unreal.loc[mask, "annualized_return_on_peak_incl_unreal"] = yearly_with_unreal.loc[mask, "return_on_peak_incl_unreal"] * factor

    per_ticker = per_ticker_yearly(df_opts, realized_sales)

    return {
        "df_opts": df_opts,
        "lots": lots,
        "stock_txns": stock_txns,
        "realized_sales": realized_sales,
        "ending_inventory": ending_inventory,
        "capital_daily": capital_daily,
        "monthly_cycles": monthly_cycles,
        "monthly_returns_w_div": final_monthly_returns_w_div,
        "open_options": open_options_df,
        "live_prices": live_prices,
        "live_option_prices": live_option_prices,
        "inv_df": inv_df,
        "total_unreal": total_unreal,
        "advanced_unreal": advanced_unreal,
        "yearly": yearly,
        "yearly_with_unreal": yearly_with_unreal,
        "per_ticker": per_ticker,
        "div_df": div_df,
        "as_of": as_of_ts,
        "issues": issues,
        "price_errors": price_errors,
        "unrealized_blocked": unrealized_blocked,
        "price_summary": price_summary,
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

    # cache_bust is a static knob to force rerun after logic changes
    state = build_pipeline(as_of_input, include_unrealized, cache_bust=3)
    yearly = state["yearly_with_unreal"] if include_unrealized else state["yearly"]
    monthly_cycles = state["monthly_cycles"]

    latest_year = yearly["year"].max()
    realized_total = yearly["combined_realized"].sum()
    grand_total = realized_total + state["total_unreal"]
    peak_cap = yearly["peak_capital"].max()
    issues = state.get("issues", [])
    price_errors = state.get("price_errors", [])
    unrealized_blocked = state.get("unrealized_blocked", False)
    price_summary = state.get("price_summary", {})

    if issues:
        for msg in issues:
            st.warning(msg)
    if price_errors or (
        price_summary
        and (
            price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
            or price_summary.get("options_fetched", 0) < price_summary.get("options_requested", 0)
        )
    ):
        st.info("Price fetch issues detected. See Logs tab for details.")

    with col_main:
        st.markdown("#### Portfolio Snapshot")
        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            metric_card("Grand Total P&L", f"${grand_total:,.0f}", delta=None)
        with mc2:
            metric_card("Realized P&L (w/ div)", f"${realized_total:,.0f}")
        with mc3:
            metric_card("Unrealized P&L", f"${state['total_unreal']:,.0f}")
        with mc4:
            metric_card("Return on Peak (total)", f"{(grand_total/peak_cap):.1%}" if peak_cap else "n/a")

    tab_yearly, tab_monthly, tab_ticker, tab_positions, tab_logs = st.tabs(["Yearly", "Monthly cycles", "Per ticker", "Positions", "Logs / data issues"])

    with tab_yearly:
        st.markdown("##### Yearly performance")
        display_cols = [
            "year",
            "options_pnl",
            "stock_realized_pnl",
            "avg_capital",
            "peak_capital",
            "combined_realized",
            "combined_incl_unreal" if include_unrealized else "combined_realized",
            "return_on_avg",
            "annualized_return_on_avg",
            "annualized_return_twr",
        ]
        display_cols = [c for c in display_cols if c in yearly.columns]
        st.dataframe(
            _format_df(
                yearly[display_cols],
                currency_cols=["options_pnl", "stock_realized_pnl", "avg_capital", "peak_capital", "combined_realized", "combined_incl_unreal"],
                pct_cols=["return_on_avg", "annualized_return_on_avg", "annualized_return_twr"],
                int_cols=["year"],
            ),
            use_container_width=True,
        )
        st.markdown("##### Key performance snapshot")
        snapshot_rows = [
            ("Cumulative realized P&L (incl. dividends)", realized_total, "currency"),
            ("Current unrealized P&L (stocks + options)", state["total_unreal"], "currency"),
            ("Grand total P&L (inception-to-date)", grand_total, "currency"),
            ("Peak capital deployed", peak_cap, "currency"),
            ("Return on peak capital (total P&L)", grand_total / peak_cap if peak_cap else np.nan, "percent"),
        ]
        snapshot = pd.DataFrame(
            {
                "Metric": [r[0] for r in snapshot_rows],
                "Value": [r[1] for r in snapshot_rows],
                "Display": [
                    f"${r[1]:,.0f}" if r[2] == "currency" and pd.notna(r[1]) else (f"{r[1]:.1%}" if pd.notna(r[1]) else "n/a")
                    for r in snapshot_rows
                ],
            }
        )
        snap_view = snapshot[["Metric", "Display"]].rename(columns={"Display": "Value"})
        st.dataframe(
            snap_view.style.set_properties(subset=["Value"], **{"text-align": "right"}).hide(axis="index"),
            use_container_width=True,
        )

    with tab_monthly:
        st.markdown("##### Monthly option cycles")
        show_cols = ["options_pnl_m", "stock_realized_pnl_m", "dividends_m", "combined_realized_m_w_div", "avg_capital_m", "return_m_w_div"]
        show_cols = [c for c in show_cols if c in monthly_cycles.columns]
        st.dataframe(
            _format_df(
                monthly_cycles[show_cols],
                currency_cols=["options_pnl_m", "stock_realized_pnl_m", "dividends_m", "combined_realized_m_w_div", "avg_capital_m"],
                pct_cols=["return_m_w_div"],
            ),
            use_container_width=True,
        )
        if not state["monthly_returns_w_div"].empty:
            equity_curve = (1 + state["monthly_returns_w_div"]).cumprod()
            st.line_chart(equity_curve, height=260)

    with tab_ticker:
        st.markdown("##### Per-ticker P&L (realized)")
        st.dataframe(
            _format_df(
                state["per_ticker"],
                currency_cols=["options_pnl", "stock_realized_pnl", "combined_realized"],
                int_cols=["year"],
            ),
            use_container_width=True,
        )

    with tab_positions:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### Assigned holdings (inventory)")
            st.dataframe(
                _format_df(
                    state["inv_df"],
                    currency_cols=["cost_per_share", "current_price", "unrealized_pnl"],
                    int_cols=["shares"],
                ),
                use_container_width=True,
            )
        with c2:
            st.markdown("##### Open option shorts")
            if state["open_options"].empty:
                st.info("No open short options.")
            else:
                st.dataframe(
                    _format_df(
                        state["open_options"][["ticker", "type", "strike", "qty", "expiration", "trans_date"]],
                        currency_cols=["strike"],
                        int_cols=["qty"],
                    ),
                    use_container_width=True,
                )

    with tab_logs:
        st.markdown("##### Data / connectivity issues")
        if not price_errors and not issues:
            st.success("No issues detected.")
        st.write(f"Build version: {APP_BUILD_VERSION}")
        if issues:
            st.write("General issues:")
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
                        {
                            "asset": "options",
                            "requested": price_summary.get("options_requested", 0),
                            "fetched": price_summary.get("options_fetched", 0),
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
        st.markdown("---")
        st.markdown("##### Debug / raw data")
        st.write("Options raw", state["df_opts"].head())
        st.write("Capital daily tail", state["capital_daily"].tail())
        st.write("Dividends", state["div_df"].head())


if __name__ == "__main__":
    main()
