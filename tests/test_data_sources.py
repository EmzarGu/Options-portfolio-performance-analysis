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
