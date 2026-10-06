"""UI-independent source configuration and provider adapters shared by all clients."""
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path

import google.auth
import yfinance as yf
from google.oauth2 import service_account

import data_sources
from portfolio_backend.calculations import build_holding_segments
from portfolio_backend.gcp import _parse_service_account_info

logger = logging.getLogger(__name__)
SHEET_ID = os.getenv("OPTIONS_SHEET_ID", "19LhrZai3cbJ1GbPE1iTquYHUeXfpIxXFX1amF5eWi_g")
SHEETS = ["Options 2024", "Options 2025", "Options 2026"]
_DEFAULT_PROVIDER = object()
READ_SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/spreadsheets.readonly"]


def load_credentials(raw=None):
    """Use explicit legacy Sheets credentials or the runtime service identity."""
    raw = raw or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw is None and os.getenv("LOCAL_SECRETS_PATH"):
        path = Path(os.environ["LOCAL_SECRETS_PATH"]).expanduser()
        if path.suffix.lower() == ".toml":
            import tomllib
            values = tomllib.loads(path.read_text())
            raw = values.get("GOOGLE_SERVICE_ACCOUNT_JSON") or values.get("google_service_account_json") or values.get("service_account")
        else:
            raw = path.read_text()
    if raw is not None:
        return service_account.Credentials.from_service_account_info(_parse_service_account_info(raw), scopes=READ_SCOPES)
    credentials, _ = google.auth.default(scopes=READ_SCOPES)
    return credentials


@lru_cache(maxsize=4)
def _download_excel(sheet_id):
    """Cache the source workbook; explicit refresh clears this local cache."""
    override = os.getenv("LOCAL_EXCEL_PATH")
    if override:
        return data_sources.download_excel_workbook(sheet_id, local_excel_path=override)
    credentials = None
    credential_error = None
    try:
        credentials = load_credentials()
    except (google.auth.exceptions.GoogleAuthError, ValueError, OSError) as exc:
        credential_error = exc
    try:
        return data_sources.download_excel_workbook(sheet_id, credentials=credentials)
    except Exception as exc:
        if credential_error:
            raise RuntimeError("Source workbook unavailable; credentials could not be loaded.") from exc
        raise


def list_option_sheets(sheet_id):
    """Expose source failures instead of disguising them as an empty workbook."""
    return data_sources.option_sheet_names_from_excel_bytes(_download_excel(sheet_id).content)


def load_options(sheet_id, sheets):
    return data_sources.load_options_from_excel_bytes(_download_excel(sheet_id).content, sheets)


def load_prefs():
    """Read local saved defaults; browser URL preferences belong to the UI."""
    prefs = {}
    for path in (Path(".streamlit_user_prefs.json"), Path.home() / ".options_roi_prefs.json"):
        try:
            value = json.loads(path.read_text())
            if isinstance(value, dict):
                prefs.update(value)
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as exc:
            logger.warning("preferences_read_failed path=%s error_type=%s", path.name, type(exc).__name__)
    return prefs


def fetch_current_prices_yf(tickers, *, yf_module=_DEFAULT_PROVIDER):
    return data_sources.fetch_current_prices_yf(tickers, yf if yf_module is _DEFAULT_PROVIDER else yf_module)


def fetch_price_history_yf(tickers, start, end, *, yf_module=_DEFAULT_PROVIDER):
    return data_sources.fetch_price_history_yf(tickers, start, end, yf if yf_module is _DEFAULT_PROVIDER else yf_module)


def align_benchmarks_monthly(tickers, idx, *, yf_module=_DEFAULT_PROVIDER):
    return data_sources.align_benchmarks_monthly(tickers, idx, yf if yf_module is _DEFAULT_PROVIDER else yf_module)


def collect_dividend_cashflows(stock_txns, as_of, *, yf_module=_DEFAULT_PROVIDER):
    module = yf if yf_module is _DEFAULT_PROVIDER else yf_module
    provider = data_sources.YFinanceDividendProvider(module) if module is not None else None
    return data_sources.collect_dividend_cashflows(stock_txns, as_of, build_holding_segments, provider)
