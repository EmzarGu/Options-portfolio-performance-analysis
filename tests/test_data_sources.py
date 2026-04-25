from dataclasses import dataclass

import pandas as pd

from data_sources import (
    DividendFetchResult,
    YFinanceDividendProvider,
    YFinancePriceHistoryProvider,
    collect_dividend_cashflows,
)


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
