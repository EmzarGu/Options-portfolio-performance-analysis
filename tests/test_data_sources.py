import json
from dataclasses import dataclass

import pandas as pd
import pytest

import data_sources
from data_sources import (
    DividendFetchResult,
    YFinanceDividendProvider,
    YFinancePriceHistoryProvider,
    clear_dividend_history_cache,
    collect_dividend_cashflows,
    fetch_current_prices_yf,
    fetch_price_history_yf,
)
from portfolio_backend.dividend_history_store import MemoryDividendHistoryStore
from portfolio_backend.price_history_store import MemoryPriceHistoryStore


@dataclass
class FakeHoldSeg:
    ticker: str
    start: pd.Timestamp
    end: pd.Timestamp
    shares: int


class FakeTicker:
    def __init__(self, dividends):
        self._dividends = dividends

    @property
    def dividends(self):
        if isinstance(self._dividends, Exception):
            raise self._dividends
        return self._dividends


class FakeYF:
    def __init__(self, mapping):
        self._mapping = mapping
        self.calls = []

    def Ticker(self, ticker):
        self.calls.append(ticker)
        return FakeTicker(self._mapping[ticker])


@pytest.fixture(autouse=True)
def clear_shared_dividend_cache():
    clear_dividend_history_cache()
    yield
    clear_dividend_history_cache()


def _segments_builder(segments):
    return lambda stock_txns, as_of: segments


def test_collect_dividend_cashflows_successful_fetch():
    segments = [
        FakeHoldSeg("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100)
    ]
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"]))
        }
    )

    result = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), yf_module)

    assert isinstance(result, DividendFetchResult)
    assert result.coverage_complete is True
    assert result.attempted_tickers == ["AAA"]
    assert result.failed_tickers == []
    assert result.errors == []
    assert result.cashflows["ticker"].tolist() == ["AAA"]
    assert result.cashflows["cash"].tolist() == [50.0]


def test_collect_dividend_cashflows_no_dividends_is_valid_zero():
    segments = [
        FakeHoldSeg("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100)
    ]
    yf_module = FakeYF(
        {
            "AAA": pd.Series(dtype=float)
        }
    )

    result = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), yf_module)

    assert result.coverage_complete is True
    assert result.attempted_tickers == ["AAA"]
    assert result.failed_tickers == []
    assert result.errors == []
    assert result.cashflows.empty


def test_collect_dividend_cashflows_per_ticker_failure_is_incomplete():
    segments = [
        FakeHoldSeg("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100),
        FakeHoldSeg("BBB", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 50),
    ]
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"])),
            "BBB": RuntimeError("boom"),
        }
    )

    result = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), yf_module)

    assert result.coverage_complete is False
    assert result.attempted_tickers == ["AAA", "BBB"]
    assert result.failed_tickers == ["BBB"]
    assert any("BBB: boom" in err for err in result.errors)
    assert result.cashflows["ticker"].tolist() == ["AAA"]
    assert result.cashflows["cash"].tolist() == [50.0]


def test_collect_dividend_cashflows_global_fetch_failure_is_incomplete():
    segments = [
        FakeHoldSeg("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100)
    ]

    result = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), None)

    assert result.coverage_complete is False
    assert result.attempted_tickers == ["AAA"]
    assert result.failed_tickers == ["AAA"]
    assert result.cashflows.empty
    assert result.errors == ["yfinance not installed; cannot fetch dividend history."]


def test_collect_dividend_cashflows_mixed_portfolio_keeps_valid_zero_and_flags_failures():
    segments = [
        FakeHoldSeg("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100),
        FakeHoldSeg("BBB", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 80),
        FakeHoldSeg("CCC", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 60),
    ]
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"])),
            "BBB": pd.Series(dtype=float),
            "CCC": RuntimeError("fetch failed"),
        }
    )

    result = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), yf_module)

    assert result.coverage_complete is False
    assert result.attempted_tickers == ["AAA", "BBB", "CCC"]
    assert result.failed_tickers == ["CCC"]
    assert any("CCC: fetch failed" in err for err in result.errors)
    assert result.cashflows["ticker"].tolist() == ["AAA"]
    assert result.cashflows["cash"].tolist() == [50.0]


def test_yfinance_dividend_provider_caches_repeated_ticker_fetches():
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"]))
        }
    )
    provider = YFinanceDividendProvider(yf_module)

    first = provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    second = provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert yf_module.calls == ["AAA"]
    assert first.tolist() == [0.5]
    assert second.tolist() == [0.5]


def test_yfinance_dividend_provider_cache_persists_across_provider_instances():
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"]))
        }
    )

    first_provider = YFinanceDividendProvider(yf_module)
    second_provider = YFinanceDividendProvider(yf_module)

    first = first_provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    second = second_provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert yf_module.calls == ["AAA"]
    assert first.tolist() == [0.5]
    assert second.tolist() == [0.5]


def test_dividend_history_cache_clear_invalidates_reload_cache_bust():
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"]))
        }
    )

    provider = YFinanceDividendProvider(yf_module)
    provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    clear_dividend_history_cache()
    provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert yf_module.calls == ["AAA", "AAA"]


def test_yfinance_dividend_provider_uses_persistent_store_after_memory_cache_clear(monkeypatch):
    yf_module = FakeYF(
        {
            "AAA": pd.Series([0.5], index=pd.to_datetime(["2024-01-15"]))
        }
    )
    store = MemoryDividendHistoryStore()
    monkeypatch.setattr(data_sources, "get_default_dividend_history_store", lambda: store)

    first_provider = YFinanceDividendProvider(yf_module)
    first = first_provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    clear_dividend_history_cache()
    second_provider = YFinanceDividendProvider(yf_module)
    second = second_provider.get_dividend_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert yf_module.calls == ["AAA"]
    assert first.tolist() == [0.5]
    assert second.tolist() == [0.5]


def test_dividend_cache_preserves_zero_dividend_and_failure_semantics():
    segments = [
        FakeHoldSeg("ZERO", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 100),
        FakeHoldSeg("FAIL", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"), 50),
    ]
    yf_module = FakeYF(
        {
            "ZERO": pd.Series(dtype=float),
            "FAIL": RuntimeError("temporary fetch failure"),
        }
    )
    provider = YFinanceDividendProvider(yf_module)

    first = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), provider)

    assert first.coverage_complete is False
    assert first.attempted_tickers == ["FAIL", "ZERO"]
    assert first.failed_tickers == ["FAIL"]
    assert first.cashflows.empty
    assert any("FAIL: temporary fetch failure" in err for err in first.errors)

    yf_module._mapping["FAIL"] = pd.Series([0.25], index=pd.to_datetime(["2024-01-15"]))
    second = collect_dividend_cashflows([], pd.Timestamp("2024-02-01"), _segments_builder(segments), provider)

    assert yf_module.calls == ["ZERO", "FAIL", "FAIL"]
    assert second.coverage_complete is True
    assert second.failed_tickers == []
    assert second.errors == []
    assert second.cashflows["ticker"].tolist() == ["FAIL"]
    assert second.cashflows["cash"].tolist() == [12.5]


class FakePriceYF:
    def __init__(self, data):
        self._data = data
        self.calls = []

    def download(self, **kwargs):
        self.calls.append(kwargs)
        return self._data


class FakeCurrentTicker:
    def __init__(self, fast_info=None, history=None):
        self.fast_info = fast_info if fast_info is not None else {}
        self._history = history if history is not None else pd.DataFrame()
        self.history_calls = []

    def history(self, **kwargs):
        self.history_calls.append(kwargs)
        return self._history


class FakeCurrentPriceYF:
    def __init__(self, download_data, ticker_mapping=None):
        self._download_data = download_data
        self._ticker_mapping = ticker_mapping or {}
        self.download_calls = []
        self.ticker_calls = []

    def download(self, **kwargs):
        self.download_calls.append(kwargs)
        return self._download_data

    def Ticker(self, ticker):
        self.ticker_calls.append(ticker)
        return self._ticker_mapping[ticker]


class FailingPriceHistoryStore:
    def get_many_history(self, tickers, start, end):
        raise RuntimeError("store unavailable")

    def upsert_history(self, ticker, series, start, end):
        raise AssertionError("fallback history should not be written when store is unavailable")


def test_fetch_current_prices_prefers_intraday_bars():
    columns = pd.MultiIndex.from_tuples([("AAA", "Close"), ("BBB", "Close")])
    data = pd.DataFrame(
        [[10.0, 20.0], [11.5, 21.5]],
        columns=columns,
        index=pd.to_datetime(["2026-05-12 13:20", "2026-05-12 13:21"]),
    )
    yf_module = FakeCurrentPriceYF(data)

    prices, errors, summary = fetch_current_prices_yf(["BBB", "AAA"], yf_module)

    assert prices == {"AAA": 11.5, "BBB": 21.5}
    assert errors == []
    assert yf_module.download_calls[0]["period"] == "1d"
    assert yf_module.download_calls[0]["interval"] == "1m"
    assert yf_module.download_calls[0]["prepost"] is True
    assert yf_module.ticker_calls == []
    assert summary == {"requested": 2, "fetched": 2, "intraday": 2, "fast_info": 0, "daily_fallback": 0}


def test_fetch_current_prices_uses_fast_info_when_intraday_missing():
    ticker = FakeCurrentTicker(fast_info={"last_price": 123.45})
    yf_module = FakeCurrentPriceYF(pd.DataFrame(), {"AAA": ticker})

    prices, errors, summary = fetch_current_prices_yf(["AAA"], yf_module)

    assert prices == {"AAA": 123.45}
    assert errors == []
    assert ticker.history_calls == []
    assert summary == {"requested": 1, "fetched": 1, "intraday": 0, "fast_info": 1, "daily_fallback": 0}


def test_fetch_current_prices_uses_daily_close_only_as_final_fallback():
    ticker = FakeCurrentTicker(
        fast_info={},
        history=pd.DataFrame({"Close": [98.0, 99.0]}, index=pd.to_datetime(["2026-05-11", "2026-05-12"])),
    )
    yf_module = FakeCurrentPriceYF(pd.DataFrame(), {"AAA": ticker})

    prices, errors, summary = fetch_current_prices_yf(["AAA"], yf_module)

    assert prices == {"AAA": 99.0}
    assert errors == []
    assert ticker.history_calls == [{"period": "5d", "interval": "1d"}]
    assert summary == {"requested": 1, "fetched": 1, "intraday": 0, "fast_info": 0, "daily_fallback": 1}


def test_yfinance_price_history_provider_caches_repeated_ticker_range_fetches():
    price_data = pd.DataFrame(
        {"Adj Close": [101.0, 102.0]},
        index=pd.to_datetime(["2024-05-20", "2024-05-21"]),
    )
    yf_module = FakePriceYF(price_data)
    provider = YFinancePriceHistoryProvider(yf_module)

    first = provider.get_price_history("AAA", pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-21"))
    second = provider.get_price_history("AAA", pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-21"))

    assert len(yf_module.calls) == 1
    assert yf_module.calls[0]["tickers"] == ["AAA"]
    assert first.tolist() == [101.0, 102.0]
    assert second.tolist() == [101.0, 102.0]


def test_fetch_price_history_uses_persistent_store_for_repeated_ranges(monkeypatch):
    price_data = pd.DataFrame(
        {"Adj Close": [101.0, 102.0]},
        index=pd.to_datetime(["2024-05-20", "2024-05-21"]),
    )
    yf_module = FakePriceYF(price_data)
    store = MemoryPriceHistoryStore()
    monkeypatch.setattr(data_sources, "get_default_price_history_store", lambda: store)

    first, first_errors, first_summary = fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )
    second, second_errors, second_summary = fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )

    assert len(yf_module.calls) == 1
    assert first_errors == []
    assert second_errors == []
    assert first_summary == {"requested": 1, "fetched": 1}
    assert second_summary == {"requested": 1, "fetched": 1}
    assert first["AAA"].tolist() == [101.0, 102.0]
    assert second["AAA"].tolist() == [101.0, 102.0]


def test_fetch_price_history_refetches_when_store_range_is_incomplete(monkeypatch):
    price_data = pd.DataFrame(
        {"Adj Close": [100.0, 101.0, 102.0]},
        index=pd.to_datetime(["2024-05-19", "2024-05-20", "2024-05-21"]),
    )
    yf_module = FakePriceYF(price_data)
    store = MemoryPriceHistoryStore()
    monkeypatch.setattr(data_sources, "get_default_price_history_store", lambda: store)

    fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )
    history, errors, summary = fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-19"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )

    assert len(yf_module.calls) == 2
    assert errors == []
    assert summary == {"requested": 1, "fetched": 1}
    assert history["AAA"].tolist() == [100.0, 101.0, 102.0]


def test_fetch_price_history_uses_bundled_fallback_before_yfinance(monkeypatch, tmp_path):
    fallback_path = tmp_path / "fallback.json"
    fallback_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "documents": {
                    "AAA:2024": {
                        "schema_version": 1,
                        "ticker": "AAA",
                        "year": 2024,
                        "coverage_start": "2024-05-20",
                        "coverage_end": "2024-05-21",
                        "prices": [
                            {"date": "2024-05-20", "close": 101.0},
                            {"date": "2024-05-21", "close": 102.0},
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    yf_module = FakePriceYF(pd.DataFrame())
    monkeypatch.setattr(data_sources, "BUNDLED_PRICE_HISTORY_FALLBACK_PATH", fallback_path)
    monkeypatch.setattr(data_sources, "_BUNDLED_PRICE_HISTORY_DOCS", None)
    monkeypatch.setattr(data_sources, "get_default_price_history_store", MemoryPriceHistoryStore)

    history, errors, summary = fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )

    assert yf_module.calls == []
    assert errors == []
    assert summary == {"requested": 1, "fetched": 1}
    assert history["AAA"].tolist() == [101.0, 102.0]


def test_fetch_price_history_does_not_write_bundled_fallback_when_store_unavailable(monkeypatch, tmp_path):
    fallback_path = tmp_path / "fallback.json"
    fallback_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "documents": {
                    "AAA:2024": {
                        "schema_version": 1,
                        "ticker": "AAA",
                        "year": 2024,
                        "coverage_start": "2024-05-20",
                        "coverage_end": "2024-05-21",
                        "prices": [
                            {"date": "2024-05-20", "close": 101.0},
                            {"date": "2024-05-21", "close": 102.0},
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    yf_module = FakePriceYF(pd.DataFrame())
    monkeypatch.setattr(data_sources, "BUNDLED_PRICE_HISTORY_FALLBACK_PATH", fallback_path)
    monkeypatch.setattr(data_sources, "_BUNDLED_PRICE_HISTORY_DOCS", None)
    monkeypatch.setattr(data_sources, "get_default_price_history_store", FailingPriceHistoryStore)

    history, errors, summary = fetch_price_history_yf(
        {"AAA"},
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-21"),
        yf_module,
    )

    assert yf_module.calls == []
    assert errors == []
    assert summary == {"requested": 1, "fetched": 1}
    assert history["AAA"].tolist() == [101.0, 102.0]
