from __future__ import annotations

from datetime import date

import pandas as pd

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport, IbkrRawRow
from portfolio_backend.option_market.models import OptionChainRequest, OptionMarketFetchResult
from portfolio_backend.option_market.optionchainiq import _normalize_contracts
from portfolio_backend.option_market.store import LocalJsonOptionMarketStore
from portfolio_backend.option_market.validation import (
    attach_sheet_probabilities,
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


def test_optionchainiq_response_normalization_filters_put_call_and_keeps_greeks():
    request = OptionChainRequest(
        provider="optionchainiq",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
    )
    rows = [
        {
            "symbol": "AAA240419P00095000",
            "put_call": "PUT",
            "strike": 95,
            "bid": 2.05,
            "ask": 2.2,
            "mark": 2.12,
            "underlying_price": 101.5,
            "delta": -0.22,
            "gamma": 0.03,
            "theta": -0.04,
            "vega": 0.12,
            "volatility": 0.31,
            "open_interest": 77,
            "volume": 8,
        },
        {"symbol": "AAA240419C00095000", "put_call": "CALL", "strike": 95},
    ]

    contracts = _normalize_contracts(request, rows)

    assert len(contracts) == 1
    assert contracts[0].contract_symbol == "AAA240419P00095000"
    assert contracts[0].delta == -0.22
    assert contracts[0].open_interest == 77


def test_local_json_store_persists_snapshot_contracts_and_matches(tmp_path):
    request = OptionChainRequest(
        provider="optionchainiq",
        ticker="AAA",
        trade_date=date(2024, 3, 15),
        expiry=date(2024, 4, 19),
        put_call="PUT",
    )
    contract = _normalize_contracts(
        request,
        [{"put_call": "PUT", "strike": 95, "mark": 2.12, "delta": -0.22}],
    )[0]
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

    requests = dedupe_chain_requests(candidates, provider="optionchainiq")

    assert len(requests) == 1
    assert requests[0].ticker == "AAA"


def test_matching_contract_reports_delta_and_fill_spread_status():
    candidate = candidates_from_ibkr_report(_report([_trade_row()]), year=2024)[0]
    request = dedupe_chain_requests([candidate], provider="optionchainiq")[0]
    contract = _normalize_contracts(
        request,
        [{"put_call": "PUT", "strike": 95, "bid": 2.0, "ask": 2.3, "mark": 2.15, "delta": -0.21}],
    )[0]

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
    request = dedupe_chain_requests(resolved, provider="optionchainiq")[0]
    contract = _normalize_contracts(request, [{"put_call": "PUT", "strike": 95, "delta": -0.22}])[0]
    match = match_trade_to_contract(resolved[0], request.request_id, [contract])

    report = build_validation_report([match], [])
    buckets = risk_bucket_summary(report.matches)

    assert report.summary["matched_contract_rate"] == 1.0
    assert report.summary["matched_delta_rate"] == 1.0
    assert [row for row in buckets if row["bucket"] == "20-25%"][0]["trades"] == 1
