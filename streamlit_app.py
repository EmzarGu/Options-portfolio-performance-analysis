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


def build_monthly_cycles(
    df_opts: pd.DataFrame,
    realized_sales: List[RealizedSale],
    capital_daily: pd.DataFrame,
    dividends_df: pd.DataFrame,
    as_of: pd.Timestamp,
):
    def cyc(d):
        return _cycle_end(d)

    opts = df_opts[df_opts["trans_date"] <= as_of]
    opts_cycle = opts.groupby(opts["trans_date"].apply(cyc))["total_pnl"].sum().rename("options_pnl_m")
    if realized_sales:
        rs_df = pd.DataFrame(
            {
                "date": [r.date for r in realized_sales if pd.Timestamp(r.date) <= as_of],
                "pnl": [r.pnl for r in realized_sales if pd.Timestamp(r.date) <= as_of],
            }
        )
        stock_cycle = rs_df.groupby(rs_df["date"].apply(cyc))["pnl"].sum().rename("stock_realized_pnl_m")
    else:
        stock_cycle = pd.Series(dtype=float, name="stock_realized_pnl_m")
    div_cycle = pd.Series(dtype=float, name="dividends_m")
    if not dividends_df.empty:
        div_filtered = dividends_df[dividends_df["ex_date"] <= as_of].copy()
        if not div_filtered.empty:
            div_cycle = div_filtered.groupby(div_filtered["ex_date"].apply(cyc))["cash"].sum().rename("dividends_m")
    combined = pd.concat([opts_cycle, stock_cycle, div_cycle], axis=1).fillna(0.0)
    combined["combined_realized_m"] = combined["options_pnl_m"] + combined["stock_realized_pnl_m"]
    combined["combined_realized_m_w_div"] = combined["combined_realized_m"] + combined["dividends_m"]
    cap = capital_daily.copy()
    cap["cycle"] = cap.index.to_series().apply(cyc)
    avg_cap = cap.groupby("cycle")["total"].mean().rename("avg_capital_m")
    combined = combined.join(avg_cap, how="left").fillna(0.0)
    combined["return_m"] = np.where(combined["avg_capital_m"] > 0, combined["combined_realized_m"] / combined["avg_capital_m"], np.nan)
    combined["return_m_w_div"] = np.where(combined["avg_capital_m"] > 0, combined["combined_realized_m_w_div"] / combined["avg_capital_m"], np.nan)
    return combined[combined.index <= as_of].sort_index()


def twr_annualized_by_year(ret_series):
    if ret_series.empty or not hasattr(ret_series.index, "year"):
        return pd.Series(dtype=float)
    grouped = ret_series.groupby(ret_series.index.year)
    return grouped.apply(lambda r: (1 + r).prod() ** (12 / len(r)) - 1)


def expectancies(df_opts: pd.DataFrame, stock_txns: List[StockTxn], monthly_cycles: pd.DataFrame):
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

    add_row("Options Trades", df_opts["total_pnl"].tolist())
    if stock_txns:
        stock_pnls = [r.pnl for r in compute_stock_realized_and_inventory(stock_txns)[0]]
        add_row("Stock Trades", stock_pnls)
    if not monthly_cycles.empty and "combined_realized_m_w_div" in monthly_cycles:
        add_row("Monthly Cycles", monthly_cycles["combined_realized_m_w_div"].tolist())

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


def per_ticker_yearly(df_opts: pd.DataFrame, realized_sales: List[RealizedSale], as_of: pd.Timestamp) -> pd.DataFrame:
    o = df_opts[df_opts["trans_date"] <= as_of].copy()
    o["year"] = o["trans_date"].dt.year
    o_t = o.groupby(["year", "ticker"])["total_pnl"].sum().rename("options_pnl").reset_index()
    if realized_sales:
        s = pd.DataFrame(
            [
                {"date": r.date, "ticker": r.ticker, "pnl": r.pnl}
                for r in realized_sales
                if pd.Timestamp(r.date) <= as_of
            ]
        )
        s["year"] = s["date"].dt.year
        s_t = s.groupby(["year", "ticker"])["pnl"].sum().rename("stock_realized_pnl").reset_index()
    else:
        s_t = pd.DataFrame(columns=["year", "ticker", "stock_realized_pnl"])
    out = o_t.merge(s_t, on=["year", "ticker"], how="outer").fillna(0.0)
    out["combined_realized"] = out["options_pnl"] + out["stock_realized_pnl"]
    return out.sort_values(["year", "combined_realized"], ascending=[True, False])


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
    as_of_ts = pd.Timestamp(as_of)
    issues: List[str] = []
    price_errors: List[str] = []

    df_opts = df_opts[df_opts["trans_date"] <= as_of_ts].copy()

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

    monthly_cycles = build_monthly_cycles(df_opts, realized_sales, capital_daily, div_df, as_of_ts)
    final_monthly_returns_w_div = monthly_cycles.loc[monthly_cycles.index <= as_of_ts, "return_m_w_div"].dropna()
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

    # Unrealized: use live prices for stocks and options; if missing, still compute with what we have and log coverage gaps.
    inv_df, stock_unreal = unrealized_as_of(ending_inventory, live_prices)
    advanced_unreal = calculate_advanced_unrealized_pnl(ending_inventory, open_options_df, live_prices, live_option_prices)
    total_unreal = float(advanced_unreal.sum()) if not advanced_unreal.empty else 0.0

    coverage_gaps = []
    if price_summary["stocks_fetched"] < price_summary["stocks_requested"]:
        coverage_gaps.append(f"Stocks priced: {price_summary['stocks_fetched']}/{price_summary['stocks_requested']}")
    if price_summary["options_fetched"] < price_summary["options_requested"]:
        coverage_gaps.append(f"Options priced: {price_summary['options_fetched']}/{price_summary['options_requested']}")
    if coverage_gaps:
        issues.append("Price coverage incomplete: " + "; ".join(coverage_gaps))
    if price_errors:
        issues.extend([f"Price error: {e}" for e in price_errors])

    opts_year = options_pnl_by_year(df_opts)
    stock_year = realized_stock_pnl_by_year(realized_sales)
    cap_year = capital_stats_by_year(capital_daily)
    yearly = combine_yearly(opts_year, stock_year, cap_year, as_of_ts, capital_daily)
    twr_annualized = twr_annualized_by_year(final_monthly_returns_w_div.dropna())
    yearly = yearly.merge(twr_annualized.rename("annualized_return_twr"), left_on="year", right_index=True, how="left")
    year_ret_raw = final_monthly_returns_w_div.groupby(final_monthly_returns_w_div.index.year).apply(lambda r: (1 + r).prod() - 1)
    yearly = yearly.merge(year_ret_raw.rename("year_return_raw"), left_on="year", right_index=True, how="left")

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

    per_ticker = per_ticker_yearly(df_opts, realized_sales, as_of_ts)
    per_ticker_totals = (
        per_ticker.groupby("ticker")[["options_pnl", "stock_realized_pnl", "combined_realized"]]
        .sum()
        .reset_index()
    )
    unreal_series = advanced_unreal.reindex(per_ticker_totals["ticker"]).fillna(0.0) if not advanced_unreal.empty else 0.0
    per_ticker_totals["unrealized_pnl"] = unreal_series.values if hasattr(unreal_series, "values") else unreal_series
    per_ticker_totals["total_pnl"] = per_ticker_totals["combined_realized"] + per_ticker_totals["unrealized_pnl"]

    # Benchmarks using monthly returns alignment (clip to as_of)
    benchmark_tickers = {"Cboe BXM": "^BXM", "PUTW ETF": "PUTW", "SCHD ETF": "SCHD"}
    strat_rets = final_monthly_returns_w_div.copy()
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
        "unrealized_blocked": False,
        "price_summary": price_summary,
        "stock_prices": live_prices,
        "option_prices": live_option_prices,
        "advanced_unreal": advanced_unreal,
        "benchmark_metrics": benchmark_metrics_df,
        "aligned_bench_returns": aligned_bench_returns,
        "per_ticker_totals": per_ticker_totals,
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
            "combined_realized": 0.0,
            "combined_incl_unreal": 0.0,
            "annualized_return_twr": pd.NA,
        }
    )
    realized_total = float(ytd_row.get("combined_realized", 0.0) or 0.0)
    ytd_total = float(
        ytd_row.get("combined_incl_unreal" if include_unrealized else "combined_realized", realized_total) or 0.0
    )
    ytd_twr = ytd_row.get("annualized_return_twr", pd.NA)
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
        st.error("Price fetch issues detected. See Logs tab for details.")

    with col_main:
        st.markdown("#### Portfolio Snapshot")
        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            metric_card("YTD Total P&L", f"${ytd_total:,.0f}", delta=None)
        with mc2:
            metric_card("YTD Realized P&L (w/ div)", f"${realized_total:,.0f}")
        with mc3:
            metric_card("Unrealized P&L", f"${state['total_unreal']:,.0f}")
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
            "options_pnl",
            "stock_realized_pnl",
            "avg_capital",
            "peak_capital",
            "combined_realized",
            "days_elapsed",
            "return_on_avg",
            "return_on_peak",
            "annualized_return_on_avg",
            "annualized_return_on_peak",
            "annualized_return_twr",
        ]
        realized_map = {
            "year": "Year",
            "options_pnl": "Options P&L",
            "stock_realized_pnl": "Stock P&L",
            "avg_capital": "Avg capital",
            "peak_capital": "Peak capital",
            "combined_realized": "Realized P&L",
            "days_elapsed": "Days",
            "return_on_avg": "RoAC",
            "return_on_peak": "RoPC",
            "annualized_return_on_avg": "Ann. RoAC",
            "annualized_return_on_peak": "Ann. RoPC",
            "annualized_return_twr": "Ann. TWR",
        }
        realized_display = yearly[[c for c in realized_cols if c in yearly.columns]].rename(columns=realized_map)
        st.dataframe(
            _format_df(
                realized_display.reset_index(drop=True),
                currency_cols=["Options P&L", "Stock P&L", "Avg capital", "Peak capital", "Realized P&L"],
                pct_cols=["RoAC", "RoPC", "Ann. RoAC", "Ann. RoPC", "Ann. TWR"],
                int_cols=["Year", "Days"],
                hide_index=True,
            ),
            use_container_width=True,
        )

        # Comprehensive Yearly Performance (MTM view)
        st.markdown("##### Comprehensive Yearly Performance (MTM view)")
        mtm_cols = [
            "year",
            "combined_realized",
            "combined_incl_unreal",
            "return_on_avg",
            "return_on_peak",
            "annualized_return_on_avg",
            "annualized_return_on_peak",
            "annualized_return_twr",
            "year_return_raw",
        ]
        mtm_map = {
            "year": "Year",
            "combined_realized": "Realized P&L",
            "combined_incl_unreal": "Total P&L (incl unreal)",
            "return_on_avg": "Return on avg",
            "return_on_peak": "Return on peak",
            "annualized_return_on_avg": "Ann. return on avg",
            "annualized_return_on_peak": "Ann. return on peak",
            "annualized_return_twr": "Ann. TWR",
            "year_return_raw": "Year return (raw)",
        }
        mtm_source = state["yearly_with_unreal"] if include_unrealized else state["yearly"]
        mtm_display = mtm_source[[c for c in mtm_cols if c in mtm_source.columns]].rename(columns=mtm_map)
        st.dataframe(
            _format_df(
                mtm_display.reset_index(drop=True),
                currency_cols=["Realized P&L", "Total P&L (incl unreal)"],
                pct_cols=["Return on avg", "Return on peak", "Ann. return on avg", "Ann. return on peak", "Ann. TWR", "Year return (raw)"],
                int_cols=["Year"],
                hide_index=True,
            ),
            use_container_width=True,
        )

        # Expectancy Analysis
        st.markdown("##### Expectancy Analysis")
        exp_df = expectancies(state["df_opts"], state["stock_txns"], state["monthly_cycles"])
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
                .properties(height=220, title="Monthly Returns (w/ Div)")
            )
            st.altair_chart(ret_chart, use_container_width=True)

    with tab_monthly:
        st.markdown("##### Monthly option cycles")
        col_map = {
            "index": "Cycle",
            "cycle": "Cycle",
            "options_pnl_m": "Options P&L",
            "stock_realized_pnl_m": "Stock P&L",
            "dividends_m": "Dividends",
            "combined_realized_m_w_div": "Total P&L (w/ div)",
            "avg_capital_m": "Avg capital",
            "return_m_w_div": "Return (w/ div)",
        }
        show_cols = ["Cycle", "Options P&L", "Stock P&L", "Dividends", "Total P&L (w/ div)", "Avg capital", "Return (w/ div)"]
        monthly_table = monthly_cycles.reset_index().rename(columns=col_map)
        if "Cycle" in monthly_table.columns:
            monthly_table["Cycle"] = pd.to_datetime(monthly_table["Cycle"]).dt.strftime("%Y-%m-%d")
        monthly_table = monthly_table[[c for c in show_cols if c in monthly_table.columns]]
        st.dataframe(
            _format_df(
                monthly_table[show_cols],
                currency_cols=["Options P&L", "Stock P&L", "Dividends", "Total P&L (w/ div)", "Avg capital"],
                pct_cols=["Return (w/ div)"],
                hide_index=True,
            ),
            use_container_width=True,
        )
        if not state["monthly_returns_w_div"].empty:
            equity_curve = (1 + state["monthly_returns_w_div"]).cumprod()
            curve_df = pd.DataFrame(
                {
                    "Cycle": state["monthly_returns_w_div"].index,
                    "Growth": equity_curve.values,
                }
            )
            y_min = float(curve_df["Growth"].min() * 0.98)
            y_max = float(curve_df["Growth"].max() * 1.02)
            curve_chart = (
                alt.Chart(curve_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Cycle:T", title="Option cycle"),
                    y=alt.Y("Growth:Q", title="Cumulative growth of $1", scale=alt.Scale(domain=[y_min, y_max], nice=True)),
                    tooltip=["Cycle:T", alt.Tooltip("Growth:Q", format=".3f")],
                )
                .properties(height=260, title="Cumulative growth by option cycle")
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
        realized_df = state["per_ticker"].rename(columns=realized_map)
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
                    "unrealized_pnl": "Unrealized P&L",
                }
            )
            st.dataframe(
                _format_df(
                    inv_df,
                    currency_cols=["Cost/share", "Current price", "Unrealized P&L"],
                    int_cols=["Shares"],
                ),
                use_container_width=True,
            )
        with c2:
            st.markdown("##### Open option shorts")
            if state["open_options"].empty:
                st.info("No open short options.")
            else:
                oo = state["open_options"][["ticker", "type", "strike", "qty", "expiration", "trans_date"]].copy()
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
                    }
                )
                st.dataframe(
                    _format_df(
                        oo,
                        currency_cols=["Strike"],
                        int_cols=["Qty"],
                    ),
                    use_container_width=True,
                )

    with tab_logs:
        st.markdown("##### Data / connectivity issues")
        st.write(f"Build version: {APP_BUILD_VERSION}")
        coverage_problem = price_summary and (
            price_summary.get("stocks_fetched", 0) < price_summary.get("stocks_requested", 0)
            or price_summary.get("options_fetched", 0) < price_summary.get("options_requested", 0)
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
