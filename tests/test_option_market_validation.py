from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow
from portfolio_backend.option_market.models import (
    OptionChainRequest,
    OptionMarketContract,
    OptionMarketFetchResult,
    OptionTradeCandidate,
)
from portfolio_backend.option_market.cutemarkets import normalize_cutemarkets_contract
from portfolio_backend.option_market.decision_data import (
    build_decision_option_universe,
    load_or_fetch_decision_option_data,
)
from portfolio_backend.option_market.history import (
    historical_enrichment_to_probability_match,
    run_historical_option_enrichment,
)
from portfolio_backend.option_market.store import LocalJsonOptionMarketStore, MemoryOptionMarketStore
from portfolio_backend.option_market.validation import (
    attach_sheet_probabilities,
    build_probability_history_import,
    build_validation_report,
    candidates_from_ibkr_report,
    dedupe_chain_requests,
    extract_sheet_probability_rows,
    match_trade_to_contract,
    risk_bucket_summary,
)


def _trade_row(**overrides):
    attrs = {
        "assetCategory": "OPT",
        "buySell": "SELL",
        "putCall": "P",
        "openCloseIndicator": "O",
        "tradeDate": "20240315",
        "expiry": "20240419",
        "strike": "95",
        "quantity": "-1",
        "multiplier": "100",
        "netCash": "214.00",
        "proceeds": "215.00",
        "ibCommission": "-1.00",
        "underlyingSymbol": "AAA",
        "tradeID": "trade-1",
    }
    attrs.update(overrides)
    return IbkrRawRow(section="Trade", attrs=attrs)


def _report(rows):
    return IbkrFlexReport(
        root_tag="FlexQueryResponse",
        metadata={},
        rows_by_section={"Trade": rows},
        section_counts={"Trade": len(rows)},
    )


def _contract(request: OptionChainRequest, **overrides) -> OptionMarketContract:
    values = {
        "provider": request.provider,
        "request_id": request.request_id,
        "ticker": request.ticker,
        "trade_date": request.trade_date,
        "expiry": request.expiry,
        "put_call": request.put_call,
        "strike": 95.0,
    }
    values.update(overrides)
    return OptionMarketContract(**values)


def test_option_market_contract_serializes_provider_neutral_greeks():
    request = OptionChainRequest(
        provider="test-provider",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
    )
    contract = _contract(
        request,
        contract_symbol="AAA240419P00095000",
        bid=2.05,
        ask=2.2,
        mark=2.12,
        underlying_price=101.5,
        delta=-0.22,
        gamma=0.03,
        theta=-0.04,
        vega=0.12,
        volatility=0.31,
        open_interest=77,
        volume=8,
    )
    doc = contract.as_dict()

    assert doc["provider"] == "test-provider"
    assert doc["contract_symbol"] == "AAA240419P00095000"
    assert doc["delta"] == -0.22
    assert doc["open_interest"] == 77


def test_cutemarkets_normalization_maps_greeks_and_price_fallbacks():
    request = OptionChainRequest(
        provider="cutemarkets",
        ticker="AAA",
        trade_date=date(2026, 5, 25),
        expiry=date(2026, 6, 18),
        put_call="CALL",
    )
    row = {
        "details": {
            "ticker": "O:AAA260618C00100000",
            "contract_type": "call",
            "expiration_date": "2026-06-18",
            "strike_price": 100,
        },
        "greeks": {"delta": 0.24, "gamma": 0.03, "theta": -0.04, "vega": 0.12},
        "implied_volatility": 0.41,
        "last_quote": {"bid": 1.2, "ask": 1.4, "midpoint": 1.3},
        "open_interest": 120,
        "day": {"volume": 7},
        "underlying_asset": {"price": 98.5},
    }

    contract = normalize_cutemarkets_contract(row, request)

    assert contract is not None
    assert contract.provider == "cutemarkets"
    assert contract.put_call == "CALL"
    assert contract.mark == pytest.approx(1.3)
    assert contract.delta == pytest.approx(0.24)
    assert contract.volatility == pytest.approx(0.41)
    assert contract.open_interest == 120
    assert contract.raw["price_source"] == "quote_midpoint"


def test_cutemarkets_normalization_uses_fmv_or_day_price_without_quote():
    request = OptionChainRequest(
        provider="cutemarkets",
        ticker="AAA",
        trade_date=date(2026, 5, 25),
        expiry=date(2026, 6, 18),
        put_call="PUT",
    )
    contract = normalize_cutemarkets_contract(
        {
            "details": {"contract_type": "put", "expiration_date": "2026-06-18", "strike_price": 95},
            "fmv": 2.15,
            "day": {"close": 2.1},
        },
        request,
    )

    assert contract is not None
    assert contract.bid is None
    assert contract.ask is None
    assert contract.mark == pytest.approx(2.15)
    assert contract.raw["price_source"] == "fmv"


def test_local_json_store_persists_snapshot_contracts_and_matches(tmp_path):
    request = OptionChainRequest(
        provider="test-provider",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
    )
    contract = _contract(request, mark=2.12, delta=-0.22)
    result = OptionMarketFetchResult(
        request=request,
        contracts=[contract],
        raw_pages=[{"data": []}],
        fetched_at="2026-05-24T00:00:00Z",
        latency_ms=123,
        status_code=200,
    )
    store = LocalJsonOptionMarketStore(tmp_path)

    store.save_chain_snapshot(result)

    assert store.load_chain_snapshot(request)["contract_count"] == 1
    assert store.load_contracts(request)[0]["strike"] == 95


def test_decision_option_data_reuses_latest_successful_fetch_without_provider_call():
    store = MemoryOptionMarketStore()
    candidate_groups = [
        {
            "ticker": "AAA",
            "category": "Recover with covered call",
            "candidates": [
                {"action": "Sell covered call", "strike": 100, "expiry": "2026-06-18"},
            ],
        }
    ]
    universe = build_decision_option_universe(candidate_groups, as_of=date(2026, 5, 25))
    request = universe.requests[0]
    result = OptionMarketFetchResult(
        request=request,
        contracts=[_contract(request, put_call="CALL", strike=100, mark=1.25, delta=0.2)],
        raw_pages=[],
        fetched_at="2026-05-25T00:00:00+00:00",
        latency_ms=10,
        status_code=200,
    )
    store.begin_fetch_run(
        {
            "run_id": "run-1",
            "provider": "cutemarkets",
            "universe_key": universe.universe_key,
            "status": "running",
            "request_ids": [request.request_id],
            "request_count": 1,
        }
    )
    store.save_chain_snapshot(result)
    store.finish_fetch_run("run-1", {"status": "succeeded", "finished_at": "2026-05-25T00:00:00+00:00"})

    class Provider:
        configured = True

        def fetch_chain(self, _request):
            raise AssertionError("provider should not be called")

    data = load_or_fetch_decision_option_data(store=store, universe=universe, provider_client=Provider())

    assert data.status["source"] == "stored"
    assert data.status["contract_count"] == 1
    assert data.contracts[0].mark == pytest.approx(1.25)


def test_decision_option_data_reuses_latest_chain_when_request_date_changes_without_provider_call():
    store = MemoryOptionMarketStore()
    candidate_groups = [
        {
            "ticker": "AAA",
            "category": "Recover with covered call",
            "contract_requests": [{"expiry": "2026-06-18", "put_call": "CALL"}],
            "candidates": [],
        }
    ]
    old_universe = build_decision_option_universe(candidate_groups, as_of=date(2026, 5, 25))
    old_request = old_universe.requests[0]
    store.save_chain_snapshot(
        OptionMarketFetchResult(
            request=old_request,
            contracts=[_contract(old_request, put_call="CALL", strike=100, mark=1.25, delta=0.2)],
            raw_pages=[],
            fetched_at="2026-05-25T00:00:00+00:00",
            latency_ms=10,
            status_code=200,
        )
    )
    new_universe = build_decision_option_universe(candidate_groups, as_of=date(2026, 5, 26))

    class Provider:
        configured = True

        def fetch_chain(self, _request):
            raise AssertionError("stored latest chain should be reused until manual refresh")

    data = load_or_fetch_decision_option_data(
        store=store,
        universe=new_universe,
        provider_client=Provider(),
    )

    assert data.status["source"] == "stored_partial"
    assert data.status["contract_count"] == 1
    assert data.contracts[0].request_id == old_request.request_id
    assert data.contracts[0].mark == pytest.approx(1.25)


def test_decision_option_refresh_fetches_even_when_contracts_are_already_stored():
    store = MemoryOptionMarketStore()
    candidate_groups = [
        {"ticker": "AAA", "category": "Recover with covered call", "candidates": [{"action": "Sell covered call", "strike": 100, "expiry": "2026-06-18"}]}
    ]
    universe = build_decision_option_universe(candidate_groups, as_of=date(2026, 5, 25))
    request = universe.requests[0]
    store.begin_fetch_run(
        {
            "run_id": "run-1",
            "provider": "cutemarkets",
            "universe_key": universe.universe_key,
            "status": "succeeded",
            "finished_at": "2026-05-25T00:00:00+00:00",
            "request_ids": [request.request_id],
            "request_count": 1,
        }
    )
    store.save_chain_snapshot(
        OptionMarketFetchResult(
            request=request,
            contracts=[_contract(request, put_call="CALL", strike=100, mark=1.25)],
            raw_pages=[],
            fetched_at="2026-05-25T00:00:00+00:00",
            latency_ms=10,
            status_code=200,
        )
    )

    class Provider:
        configured = True
        called = False

        def fetch_chain(self, request):
            self.called = True
            return OptionMarketFetchResult(
                request=request,
                contracts=[_contract(request, put_call="CALL", strike=100, mark=1.75)],
                raw_pages=[],
                fetched_at="2026-05-26T00:00:00+00:00",
                latency_ms=10,
                status_code=200,
            )

    provider = Provider()

    data = load_or_fetch_decision_option_data(
        store=store,
        universe=universe,
        provider_client=provider,
        force_refresh=True,
    )

    assert provider.called is True
    assert data.status["status"] == "succeeded"
    assert data.status["source"] == "provider_refresh"
    assert data.status["contract_count"] == 1
    assert data.contracts[0].mark == pytest.approx(1.75)


def test_local_json_store_persists_probability_history(tmp_path):
    candidate = candidates_from_ibkr_report(_report([_trade_row()]), year=2024)[0]
    probability_rows = extract_sheet_probability_rows(
        pd.DataFrame(
            [
                {
                    "trans_date": pd.Timestamp("2024-03-15"),
                    "ticker": "AAA",
                    "type": "Put",
                    "action": "Sell",
                    "expiration": pd.Timestamp("2024-04-19"),
                    "strike": 95,
                    "Profit probability": "78%",
                }
            ]
        ),
        year=2024,
    )
    imported = build_probability_history_import([candidate], probability_rows)
    store = LocalJsonOptionMarketStore(tmp_path)

    store.begin_probability_import_run({"run_id": "run-1", "status": "running"})
    store.save_probability_history(imported.probability_rows, imported.trade_matches)
    store.finish_probability_import_run("run-1", {"status": "succeeded"})

    assert (tmp_path / "option_probability_import_runs" / "run-1.json").exists()
    assert len(list((tmp_path / "option_probability_rows").glob("*.json"))) == 1
    assert len(list((tmp_path / "option_probability_trade_matches").glob("*.json"))) == 1


def test_historical_option_enrichment_persists_provider_facts_and_reuses_existing(tmp_path):
    trade = OptionTradeCandidate(
        trade_id="trade-1",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
        strike=95.0,
        qty=1.0,
        trade_price=2.14,
        net_cash=214.0,
        source="ibkr",
        profit_probability=0.78,
    )
    store = LocalJsonOptionMarketStore(tmp_path)

    class Provider:
        provider = "cutemarkets"

        def __init__(self):
            self.contract_calls = 0
            self.aggregate_calls = 0

        def fetch_historical_contracts(self, **_kwargs):
            self.contract_calls += 1
            return [{"ticker": "O:AAA240419P00095000", "strike_price": 95.0}]

        def fetch_option_daily_aggregate(self, _contract_symbol, _trade_date):
            self.aggregate_calls += 1
            return {"c": 2.1, "vw": 2.08, "v": 17, "n": 5}

    provider = Provider()
    result = run_historical_option_enrichment([trade], store=store, provider=provider)

    assert result.run_doc["enriched_count"] == 1
    assert result.run_doc["provider_contract_matched_count"] == 1
    assert result.run_doc["option_price_observed_count"] == 1
    assert provider.contract_calls == 1
    assert provider.aggregate_calls == 1
    stored = store.load_historical_enrichment("cutemarkets", "trade-1")
    assert stored is not None
    assert stored["contract_symbol"] == "O:AAA240419P00095000"
    assert stored["option_close"] == pytest.approx(2.1)
    assert stored["premium_per_capital"] == pytest.approx(214.0 / 9500.0)

    second = run_historical_option_enrichment([trade], store=store, provider=provider)

    assert second.run_doc["enriched_count"] == 0
    assert second.run_doc["skipped_existing_count"] == 1
    assert provider.contract_calls == 1
    assert provider.aggregate_calls == 1


def test_historical_enrichment_to_strike_match_keeps_provider_and_sheet_risk():
    trade = OptionTradeCandidate(
        trade_id="trade-1",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
        strike=95.0,
        qty=1.0,
        trade_price=2.14,
        net_cash=214.0,
        source="ibkr",
        profit_probability=0.78,
    )
    store = MemoryOptionMarketStore()

    class Provider:
        provider = "cutemarkets"

        def fetch_historical_contracts(self, **_kwargs):
            return [{"ticker": "O:AAA240419P00095000", "strike_price": 95.0}]

        def fetch_option_daily_aggregate(self, _contract_symbol, _trade_date):
            return {"c": 2.1, "vw": 2.08, "v": 17, "n": 5}

    result = run_historical_option_enrichment([trade], store=store, provider=Provider())
    match = historical_enrichment_to_probability_match(result.enrichments[0].as_dict())

    assert match["matched"] is True
    assert match["provider"] == "cutemarkets"
    assert match["historical_provider_contract_matched"] is True
    assert match["assignment_risk_proxy"] == pytest.approx(0.22)
    assert match["option_close"] == pytest.approx(2.1)


def test_candidate_extraction_and_probability_column_variants_match():
    candidates = candidates_from_ibkr_report(_report([_trade_row()]), year=2024)
    sheet = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-03-15"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-04-19"),
                "strike": 95,
                "Profit probability \n(>70%)": "78%",
            }
        ]
    )
    probabilities = extract_sheet_probability_rows(sheet, year=2024)
    resolved, unmatched = attach_sheet_probabilities(candidates, probabilities)

    assert unmatched == []
    assert len(resolved) == 1
    assert resolved[0].profit_probability == 0.78
    assert round(resolved[0].assignment_risk_proxy, 6) == 0.22


def test_probability_history_import_matches_and_tracks_unmatched_rows():
    candidates = candidates_from_ibkr_report(
        _report(
            [
                _trade_row(tradeID="trade-1", strike="95"),
                _trade_row(tradeID="trade-2", strike="90"),
            ]
        ),
        year=2024,
    )
    sheet = pd.DataFrame(
        [
            {
                "trans_date": pd.Timestamp("2024-03-15"),
                "ticker": "AAA",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-04-19"),
                "strike": 95,
                "Profit probability": "78%",
                "source_sheet": "Options 2024",
            },
            {
                "trans_date": pd.Timestamp("2024-03-15"),
                "ticker": "BBB",
                "type": "Put",
                "action": "Sell",
                "expiration": pd.Timestamp("2024-04-19"),
                "strike": 50,
                "Profit probability": "80%",
                "source_sheet": "Options 2024",
            },
        ]
    )
    probability_rows = extract_sheet_probability_rows(sheet, year=2024)

    imported = build_probability_history_import(candidates, probability_rows)

    assert imported.summary["trade_count"] == 2
    assert imported.summary["probability_row_count"] == 2
    assert imported.summary["matched_trade_count"] == 1
    assert imported.summary["unmatched_trade_count"] == 1
    assert imported.summary["unmatched_probability_row_count"] == 1
    matched = [match for match in imported.trade_matches if match.matched][0]
    assert matched.trade.profit_probability == 0.78
    assert round(matched.trade.assignment_risk_proxy or 0, 6) == 0.22
    assert imported.unmatched_probability_rows[0].ticker == "BBB"


def test_dedupe_prevents_duplicate_provider_calls():
    candidates = candidates_from_ibkr_report(
        _report(
            [
                _trade_row(tradeID="trade-1", strike="95"),
                _trade_row(tradeID="trade-2", strike="90"),
            ]
        ),
        year=2024,
    )

    requests = dedupe_chain_requests(candidates, provider="test-provider")

    assert len(requests) == 1
    assert requests[0].ticker == "AAA"


def test_matching_contract_reports_delta_and_fill_spread_status():
    candidate = candidates_from_ibkr_report(_report([_trade_row()]), year=2024)[0]
    request = dedupe_chain_requests([candidate], provider="test-provider")[0]
    contract = _contract(request, bid=2.0, ask=2.3, mark=2.15, delta=-0.21)

    match = match_trade_to_contract(candidate, request.request_id, [contract])

    assert match.matched is True
    assert match.delta_risk == 0.21
    assert match.bid_ask_contains_fill is True
    assert round(match.mark_minus_fill or 0, 6) == 0.01


def test_validation_report_bucket_summary():
    candidates = candidates_from_ibkr_report(_report([_trade_row()]), year=2024)
    resolved = [
        type(candidates[0])(
            trade_id=candidates[0].trade_id,
            ticker=candidates[0].ticker,
            trade_date=candidates[0].trade_date,
            expiry=candidates[0].expiry,
            put_call=candidates[0].put_call,
            strike=candidates[0].strike,
            qty=candidates[0].qty,
            trade_price=candidates[0].trade_price,
            net_cash=candidates[0].net_cash,
            source=candidates[0].source,
            profit_probability=0.78,
        )
    ]
    request = dedupe_chain_requests(resolved, provider="test-provider")[0]
    contract = _contract(request, delta=-0.22)
    match = match_trade_to_contract(resolved[0], request.request_id, [contract])

    report = build_validation_report([match], [])
    buckets = risk_bucket_summary(report.matches)

    assert report.summary["matched_contract_rate"] == 1.0
    assert report.summary["matched_delta_rate"] == 1.0
    assert [row for row in buckets if row["bucket"] == "20-25%"][0]["trades"] == 1
