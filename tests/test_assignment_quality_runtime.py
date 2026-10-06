"""Verify reuse, invalidation and price parity without depending on wall-clock speed."""
from copy import deepcopy
from types import SimpleNamespace
import json

import pandas as pd
import pytest

from portfolio_backend import assignment_quality_runtime as runtime
from portfolio_backend.ibkr.assignment_quality import build_assignment_quality_analysis, prepare_assignment_quality
from portfolio_backend.derived_payload_cache import _json_safe
from portfolio_backend.price_history_store import FirestorePriceHistoryStore, MemoryPriceHistoryStore, _document_id
from tests.test_assigned_holdings_review import _assigned_holding_report


@pytest.fixture
def setup_runtime(monkeypatch):
    runtime._CACHE.clear()
    persisted = {}
    monkeypatch.setattr(runtime, "load_derived_payload", lambda k: deepcopy(persisted.get(k)))
    monkeypatch.setattr(runtime, "save_derived_payload", lambda k, v, **kw: persisted.update({k: json.loads(json.dumps(v))}))
    report = _assigned_holding_report()
    calls = []
    def load():
        calls.append(1)
        return report
    kw = dict(state=SimpleNamespace(as_of=pd.Timestamp('2026-09-19'), stock_prices={'EMN': 65.}),
              source_metadata={'source_snapshot_id':'report-1'}, report_loader=load,
              history_store_factory=MemoryPriceHistoryStore, price_fetcher=lambda _: ({}, [], {}))
    yield kw, calls, persisted
    runtime._CACHE.clear()


def test_price_refresh_reuses_accounting_but_revalues_all_outputs(setup_runtime):
    kw, calls, _ = setup_runtime
    old = runtime.build_assignment_payload(**kw)
    saved = deepcopy(runtime._CACHE)
    kw['state'].stock_prices = {'EMN': 80.}
    new = runtime.build_assignment_payload(**kw)
    expected = build_assignment_quality_analysis(_assigned_holding_report(), as_of=kw['state'].as_of, prices={'EMN':80.})
    assert _json_safe(new) == _json_safe(expected)
    assert old['summary'] != new['summary']
    assert len(calls) == 1
    assert runtime._CACHE == saved  # Valuation did not mutate cached lots or allocations.


def test_prepared_accounting_survives_process_restart(setup_runtime):
    kw, calls, _ = setup_runtime
    first = runtime.build_assignment_payload(**kw)
    runtime._CACHE.clear()
    second = runtime.build_assignment_payload(**kw)
    assert first == second
    assert len(calls) == 1


@pytest.mark.parametrize('change', ['report', 'date'])
def test_source_and_date_changes_invalidate_preparation(setup_runtime, change):
    kw, calls, _ = setup_runtime
    runtime.build_assignment_payload(**kw)
    if change == 'report': kw['source_metadata'] = {'source_snapshot_id':'report-2'}
    else: kw['state'].as_of = pd.Timestamp('2026-09-20')
    runtime.build_assignment_payload(**kw)
    assert len(calls) == 2


def test_expired_preparation_is_rebuilt(setup_runtime, monkeypatch):
    kw, calls, persisted = setup_runtime
    runtime.build_assignment_payload(**kw)
    runtime._CACHE.clear()
    for value in persisted.values(): value['built_at'] = 0
    runtime.build_assignment_payload(**kw)
    assert len(calls) == 2


def test_prepared_snapshot_roundtrip_preserves_caps_and_allocations():
    report = _assigned_holding_report()
    date = pd.Timestamp('2026-02-01')
    prepared = json.loads(json.dumps(prepare_assignment_quality(report, as_of=date)))
    for price in [50., 70., 100.]:
        direct = build_assignment_quality_analysis(report, as_of=date, prices={'EMN':price})
        cached = build_assignment_quality_analysis(None, prepared=prepared, as_of=date, prices={'EMN':price})
        assert direct == cached


class HistoryClient:
    def __init__(self, rows):
        self.rows, self.reads = rows, []
    def collection(self, _): return self
    def document(self, key): return key
    def get_all(self, refs):
        self.reads.extend(refs)
        for key in refs:
            doc = self.rows.get(key)
            yield SimpleNamespace(id=key, exists=doc is not None, to_dict=lambda doc=doc: doc)


def test_horizon_history_reads_only_requested_years():
    key = _document_id('AAA', 2026)
    client = HistoryClient({key: {'prices':[{'date':'2026-06-17','close':90}, {'date':'2026-06-19','close':100}]}})
    result = FirestorePriceHistoryStore(client=client).get_history_points({'AAA':[pd.Timestamp('2026-06-18')]})
    assert result['AAA'].iloc[0] == 90
    assert client.reads == [key]


def test_history_missing_year_preserves_previous_year_fallback():
    client = HistoryClient({_document_id('AAA', 2024): {'prices':[{'date':'2024-12-31','close':90}]},
                            _document_id('AAA', 2026): {'prices':[{'date':'2026-06-19','close':100}]}})
    result = FirestorePriceHistoryStore(client=client).get_history_points({'AAA':[pd.Timestamp('2026-01-01')]})
    assert result['AAA'].iloc[0] == 90
    assert len(client.reads) == 27


def test_history_empty_ticker_produces_no_invented_price():
    result = FirestorePriceHistoryStore(client=HistoryClient({})).get_history_points({'AAA':[pd.Timestamp('2026-01-01')]})
    assert result['AAA'].empty
