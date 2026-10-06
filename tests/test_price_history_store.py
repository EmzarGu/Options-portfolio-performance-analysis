import pandas as pd

from portfolio_backend.price_history_store import MemoryPriceHistoryStore
from portfolio_backend.price_history_store import _series_from_doc


def test_history_parser_preserves_mixed_dates_invalid_rows_and_duplicates():
    rows = [
        {"date": "2024-05-22", "close": "103.5"},
        {"date": "May 20, 2024", "close": 100},
        {"date": "2024-05-21T15:30:00", "close": "102"},
        {"date": "2024-05-20", "close": 101.0},
        {"date": "invalid", "close": 900},
        {"date": "2024-05-23", "close": "invalid"},
        {"date": None, "close": 999},
        {"date": "2024-05-24"},
    ]
    expected = pd.Series([101.0, 102.0, 103.5],
                         index=pd.to_datetime(["2024-05-20", "2024-05-21", "2024-05-22"]), name="AAA")
    pd.testing.assert_series_equal(_series_from_doc({"prices": rows}, "AAA"), expected)


def test_history_parser_retains_empty_series_contract():
    for doc in ({}, {"prices": []}, {"prices": [{"date": "bad", "close": 3}, {}]}):
        pd.testing.assert_series_equal(_series_from_doc(doc, "AAA"), pd.Series(dtype=float, name="AAA"))


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
