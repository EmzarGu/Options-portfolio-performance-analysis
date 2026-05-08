import pandas as pd

from portfolio_backend.dividend_history_store import MemoryDividendHistoryStore


def test_memory_dividend_history_store_serves_covered_range():
    store = MemoryDividendHistoryStore()
    series = pd.Series(
        [0.5],
        index=pd.to_datetime(["2024-01-15"]),
        name="AAA",
    )

    store.upsert_history("AAA", series, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    lookup = store.get_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert lookup.fully_covered is True
    assert lookup.series.tolist() == [0.5]


def test_memory_dividend_history_store_caches_empty_covered_range():
    store = MemoryDividendHistoryStore()

    store.upsert_history(
        "AAA",
        pd.Series(dtype=float),
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-02-01"),
    )
    lookup = store.get_history("AAA", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))

    assert lookup.fully_covered is True
    assert lookup.series.empty


def test_memory_dividend_history_store_marks_wider_range_incomplete():
    store = MemoryDividendHistoryStore()
    series = pd.Series(
        [0.5],
        index=pd.to_datetime(["2024-01-15"]),
        name="AAA",
    )

    store.upsert_history("AAA", series, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
    lookup = store.get_history("AAA", pd.Timestamp("2023-12-01"), pd.Timestamp("2024-02-01"))

    assert lookup.fully_covered is False
    assert lookup.series.tolist() == [0.5]
