import pandas as pd

from portfolio_backend.price_history_store import MemoryPriceHistoryStore


def test_memory_price_history_store_serves_covered_range():
    store = MemoryPriceHistoryStore()
    series = pd.Series(
        [101.0, 102.0],
        index=pd.to_datetime(["2024-05-20", "2024-05-21"]),
        name="AAA",
    )

    store.upsert_history("AAA", series, pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-21"))
    lookup = store.get_history("AAA", pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-21"))

    assert lookup.fully_covered is True
    assert lookup.series.tolist() == [101.0, 102.0]


def test_memory_price_history_store_marks_wider_range_incomplete():
    store = MemoryPriceHistoryStore()
    series = pd.Series(
        [101.0, 102.0],
        index=pd.to_datetime(["2024-05-20", "2024-05-21"]),
        name="AAA",
    )

    store.upsert_history("AAA", series, pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-21"))
    lookup = store.get_history("AAA", pd.Timestamp("2024-05-19"), pd.Timestamp("2024-05-21"))

    assert lookup.fully_covered is False
    assert lookup.series.tolist() == [101.0, 102.0]


def test_memory_price_history_store_splits_and_reads_year_chunks():
    store = MemoryPriceHistoryStore()
    series = pd.Series(
        [99.0, 100.0],
        index=pd.to_datetime(["2024-12-31", "2025-01-02"]),
        name="AAA",
    )

    store.upsert_history("AAA", series, pd.Timestamp("2024-12-31"), pd.Timestamp("2025-01-02"))
    lookup = store.get_history("AAA", pd.Timestamp("2024-12-31"), pd.Timestamp("2025-01-02"))

    assert lookup.fully_covered is True
    assert lookup.series.tolist() == [99.0, 100.0]


def test_memory_price_history_store_reads_many_tickers():
    store = MemoryPriceHistoryStore()
    store.upsert_history(
        "AAA",
        pd.Series([101.0], index=pd.to_datetime(["2024-05-20"]), name="AAA"),
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-20"),
    )
    store.upsert_history(
        "BBB",
        pd.Series([202.0], index=pd.to_datetime(["2024-05-20"]), name="BBB"),
        pd.Timestamp("2024-05-20"),
        pd.Timestamp("2024-05-20"),
    )

    lookups = store.get_many_history(["AAA", "BBB"], pd.Timestamp("2024-05-20"), pd.Timestamp("2024-05-20"))

    assert set(lookups) == {"AAA", "BBB"}
    assert lookups["AAA"].fully_covered is True
    assert lookups["AAA"].series.tolist() == [101.0]
    assert lookups["BBB"].fully_covered is True
    assert lookups["BBB"].series.tolist() == [202.0]
