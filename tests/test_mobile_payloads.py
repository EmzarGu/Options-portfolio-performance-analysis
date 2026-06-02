import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from portfolio_backend.mobile_payloads import (
    MONEYNESS_BANDS,
    build_data_freshness,
    build_mobile_request,
    build_mobile_dashboard,
    build_mobile_config,
    build_mobile_issue_rows,
    build_mobile_issues,
    build_mobile_monthly_performance,
    build_mobile_open_option_shorts,
    build_mobile_positions,
    build_mobile_refresh,
    build_mobile_tickers,
    build_mobile_yearly_performance,
    build_future_monthly_performance_rows,
    build_monthly_performance_rows,
    build_open_option_short_rows,
    build_inventory_rows,
    build_ticker_summary_rows,
    build_yearly_performance_rows,
    moneyness_band,
)


def _mobile_state():
    return SimpleNamespace(
        as_of=pd.Timestamp("2026-05-03"),
        open_options=pd.DataFrame(
            [
                {
                    "ticker": "PUTT",
                    "type": "Put",
                    "strike": 100.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-05-17"),
                    "trans_date": pd.Timestamp("2026-04-01"),
                    "open_price": 2.0,
                },
                {
                    "ticker": "CALL",
                    "type": "Call",
                    "strike": 50.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-05-24"),
                    "trans_date": pd.Timestamp("2026-04-02"),
                    "open_price": 1.5,
                },
                {
                    "ticker": "MISS",
                    "type": "Put",
                    "strike": 25.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-05-10"),
                    "trans_date": pd.Timestamp("2026-04-03"),
                    "open_price": 0.75,
                },
                {
                    "ticker": "CLEAR",
                    "type": "Put",
                    "strike": 100.0,
                    "qty": 2,
                    "expiration": pd.Timestamp("2026-06-21"),
                    "trans_date": pd.Timestamp("2026-04-04"),
                    "open_price": 1.0,
                },
                {
                    "ticker": "PUTT",
                    "type": "Put",
                    "strike": 100.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-05-17"),
                    "trans_date": pd.Timestamp("2026-04-01"),
                    "open_price": 2.0,
                },
            ]
        ),
        stock_prices={"PUTT": 80.0, "CALL": 60.0, "CLEAR": 120.0},
        inv_df=pd.DataFrame(
            [
                {
                    "ticker": "CALL",
                    "buy_date": pd.Timestamp("2026-04-02"),
                    "shares": 100,
                    "cost_per_share": 45.0,
                    "current_price": 60.0,
                    "covered_shares": 100,
                    "covered_strike": 50.0,
                    "unrealized_pnl": 1500.0,
                    "source": "stock_lot",
                },
                {
                    "ticker": "MISS",
                    "buy_date": pd.Timestamp("2026-04-03"),
                    "shares": 100,
                    "cost_per_share": 25.0,
                    "current_price": pd.NA,
                    "covered_shares": 0,
                    "covered_strike": pd.NA,
                    "unrealized_pnl": pd.NA,
                    "source": "put_gap",
                },
                {
                    "ticker": "OTHER",
                    "buy_date": pd.Timestamp("2026-04-04"),
                    "shares": 100,
                    "cost_per_share": 10.0,
                    "current_price": 11.0,
                    "covered_shares": 0,
                    "covered_strike": pd.NA,
                    "unrealized_pnl": 100.0,
                    "source": "manual_note",
                },
            ]
        ),
        sheet_counts=pd.DataFrame({"source_sheet": ["Options 2025"], "rows": [2]}),
        price_summary={"stocks_requested": 4, "stocks_fetched": 3},
        missing_required_price_tickers=["MISS"],
        per_ticker=pd.DataFrame(
            [
                {
                    "year": 2025,
                    "ticker": "PUTT",
                    "options_pnl": 50.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 50.0,
                },
                {
                    "year": 2026,
                    "ticker": "PUTT",
                    "options_pnl": 150.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 150.0,
                },
                {
                    "year": 2026,
                    "ticker": "CALL",
                    "options_pnl": 10.0,
                    "stock_realized_pnl": 20.0,
                    "combined_realized": 30.0,
                },
                {
                    "year": 2026,
                    "ticker": "MISS",
                    "options_pnl": 0.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 0.0,
                },
            ]
        ),
        per_ticker_totals=pd.DataFrame(
            [
                {
                    "ticker": "CALL",
                    "options_pnl": 10.0,
                    "stock_realized_pnl": 20.0,
                    "combined_realized": 30.0,
                    "unrealized_pnl": 1500.0,
                    "total_pnl": 1530.0,
                },
                {
                    "ticker": "PUTT",
                    "options_pnl": 200.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 200.0,
                    "unrealized_pnl": -25.0,
                    "total_pnl": 175.0,
                },
                {
                    "ticker": "MISS",
                    "options_pnl": 0.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 0.0,
                    "unrealized_pnl": 0.0,
                    "total_pnl": 0.0,
                },
                {
                    "ticker": "CLEAR",
                    "options_pnl": 0.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 0.0,
                    "unrealized_pnl": -500.0,
                    "total_pnl": -500.0,
                },
            ]
        ),
        div_df=pd.DataFrame(
            [
                {
                    "ticker": "CALL",
                    "ex_date": pd.Timestamp("2026-04-10"),
                    "pay_date": pd.Timestamp("2026-04-15"),
                    "cash": 12.5,
                },
                {
                    "ticker": "PUTT",
                    "ex_date": pd.Timestamp("2025-08-10"),
                    "pay_date": pd.Timestamp("2025-08-15"),
                    "cash": 7.0,
                },
                {
                    "ticker": "PUTT",
                    "ex_date": pd.Timestamp("2026-04-20"),
                    "pay_date": pd.Timestamp("2026-04-25"),
                    "cash": 11.0,
                },
                {
                    "ticker": "FUTR",
                    "ex_date": pd.Timestamp("2026-05-10"),
                    "pay_date": pd.Timestamp("2026-05-10"),
                    "cash": 99.0,
                },
            ]
        ),
        monthly_cycles=pd.DataFrame(
            {
                "realized_options_pnl": [250.0, 100.0],
                "realized_stock_pnl": [50.0, 0.0],
                "dividends": [0.0, 0.0],
                "total_realized_pnl": [300.0, 100.0],
                "avg_capital": [10000.0, 10000.0],
                "peak_capital": [11000.0, 12000.0],
                "roac": [0.03, 0.01],
                "ropc": [0.02727272727272727, 0.008333333333333333],
            },
            index=pd.to_datetime(["2026-04-30", "2026-05-31"]),
        ),
        yearly=pd.DataFrame(
            {
                "year": [2025, 2026],
                "realized_options_pnl": [50.0, 100.0],
                "realized_stock_pnl": [0.0, 0.0],
                "dividends": [0.0, 0.0],
                "total_realized_pnl": [50.0, 100.0],
                "avg_capital": [8000.0, 10000.0],
                "peak_capital": [9000.0, 12000.0],
                "roac_year": [0.05, 0.01],
                "ropc_year": [0.044444444444444446, 0.008333333333333333],
                "ann_roac": [0.05, 0.07],
                "ann_ropc": [0.044444444444444446, 0.06],
                "annualized_return_twr": [0.04, 0.12],
                "annualized_return_twr_active": [0.045, 0.13],
            }
        ),
        yearly_with_unreal=pd.DataFrame(
            {
                "year": [2025, 2026],
                "realized_options_pnl": [50.0, 100.0],
                "realized_stock_pnl": [0.0, 0.0],
                "dividends": [0.0, 0.0],
                "total_realized_pnl": [50.0, 100.0],
                "total_pnl_incl_unreal": [50.0, 350.0],
                "avg_capital": [8000.0, 10000.0],
                "peak_capital": [9000.0, 12000.0],
                "roac_year": [0.05, 0.01],
                "ropc_year": [0.044444444444444446, 0.008333333333333333],
                "ann_roac": [0.05, 0.07],
                "ann_ropc": [0.044444444444444446, 0.06],
                "annualized_return_twr": [0.04, 0.12],
                "annualized_return_twr_active": [0.045, 0.13],
                "annualized_return_twr_unrealized_adjusted": [0.04, 0.18],
            }
        ),
        total_unreal=250.0,
        option_unreal=50.0,
        stock_unreal=200.0,
        put_assignment_unreal=0.0,
        itm_put_cash_required=0.0,
        itm_put_market_value=0.0,
        itm_put_contracts=0,
        itm_put_shares=0,
        unrealized_blocked=False,
        capital_history_affected_years=[],
        capital_history_incomplete=True,
        capital_history_coverage_issues=[
            {"ticker": "CAPGAP", "reason": "missing historical price before first trade"}
        ],
        issues=["Mixed-leg option row needs review"],
        price_errors=["MISS: no price returned"],
        historical_price_errors=["CAPGAP: missing historical price series"],
        historical_price_summary={"requested": 4, "fetched": 3},
        dividend_coverage_complete=False,
        dividend_summary={"attempted": 2, "failed": 1},
        dividend_errors=["DIVMISS: dividend history returned no usable data"],
    )


def _mobile_state_with_future_september():
    state = _mobile_state()
    state.open_options = pd.concat(
        [
            state.open_options,
            pd.DataFrame(
                [
                    {
                        "ticker": "SEPT",
                        "type": "Put",
                        "strike": 100.0,
                        "qty": 1,
                        "expiration": pd.Timestamp("2026-09-18"),
                        "trans_date": pd.Timestamp("2026-05-01"),
                        "open_price": 3.0,
                        "roll_adjusted_open_price": 3.5,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    return state


def test_mobile_request_echoes_normalized_inputs():
    assert build_mobile_request(
        pd.Timestamp("2026-05-03"),
        True,
        ["Options 2024", "Options 2025"],
    ) == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2024", "Options 2025"],
    }


@pytest.mark.parametrize(
    "value,expected",
    [
        (0.01, "in_the_money"),
        (0.0, "at_strike"),
        (-0.005, "at_strike"),
        (-0.02, "near"),
        (-0.07, "ok"),
        (-0.11, "clear"),
        (None, None),
    ],
)
def test_moneyness_band_contract(value, expected):
    assert moneyness_band(value) == expected
    assert [row["band"] for row in MONEYNESS_BANDS] == ["in_the_money", "at_strike", "near", "ok", "clear"]


def test_open_option_short_rows_emit_mobile_contract_shape():
    rows = build_open_option_short_rows(_mobile_state())

    assert rows[0]["ticker"] in {"PUTT", "CALL"}
    put_rows = [row for row in rows if row["ticker"] == "PUTT"]
    assert [row["id"] for row in put_rows] == [
        "optlot:PUTT:Put:100.0:2026-05-17:2026-04-01:0",
        "optlot:PUTT:Put:100.0:2026-05-17:2026-04-01:1",
    ]
    assert put_rows[0]["moneyness"] == pytest.approx(0.20)
    assert put_rows[0]["moneyness_band"] == "in_the_money"
    assert put_rows[0]["quantity"] == 1
    assert put_rows[0]["days_to_expiration"] == 14
    assert put_rows[0]["notional_at_strike"] == pytest.approx(10000.0)
    assert put_rows[0]["premium_collected"] == pytest.approx(200.0)
    assert put_rows[0]["roll_adjusted_premium_collected"] == pytest.approx(200.0)
    assert put_rows[0]["display_premium_collected"] == pytest.approx(200.0)
    assert put_rows[0]["covered_status"] == "cash_secured"
    assert put_rows[0]["missing_price"] is False

    call = next(row for row in rows if row["ticker"] == "CALL")
    assert call["option_type"] == "Call"
    assert call["moneyness"] == pytest.approx(0.20)
    assert call["covered_status"] == "covered"

    missing = next(row for row in rows if row["ticker"] == "MISS")
    assert missing["current_price"] is None
    assert missing["moneyness"] is None
    assert missing["moneyness_band"] is None
    assert missing["missing_price"] is True
    assert missing["risk_label"] == "Missing price"

    clear = next(row for row in rows if row["ticker"] == "CLEAR")
    assert clear["moneyness"] == pytest.approx(-0.20)
    assert clear["moneyness_band"] == "clear"
    assert clear["quantity"] == 2
    assert clear["notional_at_strike"] == pytest.approx(20000.0)


def test_open_option_short_rows_support_sort_and_limit():
    rows = build_open_option_short_rows(_mobile_state(), sort="expiration", limit=2)
    assert [row["ticker"] for row in rows] == ["MISS", "PUTT"]

    rows = build_open_option_short_rows(_mobile_state(), sort="ticker", limit=2)
    assert [row["ticker"] for row in rows] == ["CALL", "CLEAR"]

    with pytest.raises(ValueError):
        build_open_option_short_rows(_mobile_state(), sort="unsupported")


def test_mobile_open_option_shorts_composes_contract_payload():
    payload = build_mobile_open_option_shorts(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        sort="moneyness_risk",
        limit=3,
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(payload) == {"request", "data_freshness", "moneyness_legend", "items"}
    assert payload["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert [row["band"] for row in payload["moneyness_legend"]] == [
        "in_the_money",
        "at_strike",
        "near",
        "ok",
        "clear",
    ]
    assert [row["ticker"] for row in payload["items"]] == ["PUTT", "PUTT", "CALL"]
    assert "notional_at_strike" in payload["items"][0]
    assert "premium_collected" in payload["items"][0]
    assert "roll_adjusted_premium_collected" in payload["items"][0]
    assert "display_premium_collected" in payload["items"][0]
    assert "missing_price" in payload["items"][0]


def test_mobile_open_option_shorts_matches_contract_fixture():
    payload = build_mobile_open_option_shorts(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        sort="moneyness_risk",
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(payload)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_open_option_shorts_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert payload == expected


def test_inventory_rows_emit_mobile_contract_shape():
    rows = build_inventory_rows(_mobile_state())

    assert rows == [
        {
            "id": "inventory:CALL:2026-04-02:stock_lot:0",
            "ticker": "CALL",
            "buy_date": "2026-04-02",
            "shares": 100,
            "cost_per_share": 45.0,
            "current_price": 60.0,
            "covered_shares": 100,
            "covered_strike": 50.0,
            "covered_strike_mixed": False,
            "unrealized_pnl": 1500.0,
            "lot_count": 1,
            "first_buy_date": "2026-04-02",
            "latest_buy_date": "2026-04-02",
            "source": "stock_lot",
            "missing_price": False,
        },
    ]


def test_inventory_rows_group_multiple_lots_by_ticker():
    state = _mobile_state()
    state.inv_df = pd.DataFrame(
        [
            {
                "ticker": "STZ",
                "buy_date": pd.Timestamp("2026-05-13"),
                "shares": 100,
                "cost_per_share": 150.0,
                "current_price": 140.55,
                "covered_shares": 0,
                "covered_strike": pd.NA,
                "unrealized_pnl": -945.0,
                "source": "stock_lot",
            },
            {
                "ticker": "STZ",
                "buy_date": pd.Timestamp("2026-05-14"),
                "shares": 100,
                "cost_per_share": 150.0,
                "current_price": 140.55,
                "covered_shares": 0,
                "covered_strike": pd.NA,
                "unrealized_pnl": -945.0,
                "source": "stock_lot",
            },
        ]
    )

    rows = build_inventory_rows(state)

    assert rows == [
        {
            "id": "inventory:STZ:2026-05-14:stock_group:0",
            "ticker": "STZ",
            "buy_date": "2026-05-14",
            "shares": 200,
            "cost_per_share": 150.0,
            "current_price": 140.55,
            "covered_shares": 0,
            "covered_strike": None,
            "covered_strike_mixed": False,
            "unrealized_pnl": -1890.0,
            "lot_count": 2,
            "first_buy_date": "2026-05-13",
            "latest_buy_date": "2026-05-14",
            "source": "stock_group",
            "missing_price": False,
        }
    ]


def test_data_freshness_marks_loaded_and_missing_selected_sheets():
    freshness = build_data_freshness(
        _mobile_state(),
        ["Options 2025", "Options 2026"],
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
            "source_modified_at": None,
        },
    )

    assert freshness == {
        "pipeline_built_at": "2026-05-04T19:00:00+02:00",
        "prices_updated_at": "2026-05-04T19:01:00+02:00",
        "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        "source_modified_at": None,
        "price_coverage": {
            "stocks_requested": 4,
            "stocks_fetched": 3,
            "missing_tickers": ["MISS"],
        },
        "source_sheets": [
            {"name": "Options 2025", "status": "loaded", "rows": 2},
            {"name": "Options 2026", "status": "missing", "rows": 0},
        ],
    }


def test_mobile_refresh_composes_contract_payload():
    payload = build_mobile_refresh(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        cache_bust=42,
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(payload) == {"request", "data_freshness", "refresh"}
    assert payload["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert payload["data_freshness"]["prices_updated_at"] == "2026-05-04T19:01:00+02:00"
    assert payload["refresh"] == {
        "status": "partial",
        "pipeline_refreshed": True,
        "prices_refreshed": True,
        "cache_bust": 42,
        "missing_price_count": 1,
        "missing_sheet_count": 1,
        "reload_endpoints": [
            "/v1/mobile/dashboard",
            "/v1/mobile/positions",
            "/v1/mobile/open-option-shorts",
            "/v1/mobile/tickers",
            "/v1/mobile/performance/monthly",
            "/v1/mobile/performance/yearly",
            "/v1/mobile/issues",
        ],
    }


def test_mobile_refresh_matches_contract_fixture():
    payload = build_mobile_refresh(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        cache_bust=42,
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(payload)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_refresh_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert payload == expected


def test_mobile_dashboard_composes_launch_payload_contract():
    dashboard = build_mobile_dashboard(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
        open_option_preview_limit=2,
    )

    assert set(dashboard) == {
        "request",
        "data_freshness",
        "snapshot",
        "monthly_target",
        "open_option_short_preview",
        "issue_summary",
    }
    assert dashboard["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert dashboard["data_freshness"]["source_sheets"] == [
        {"name": "Options 2025", "status": "loaded", "rows": 2},
        {"name": "Options 2026", "status": "missing", "rows": 0},
    ]
    assert dashboard["snapshot"] == {
        "currency": "USD",
        "year": 2026,
        "ytd_total_pnl": 350.0,
        "ytd_realized_pnl": 100.0,
        "current_unrealized_pnl": 250.0,
        "current_option_unrealized_pnl": 50.0,
        "current_option_premium_unrealized_pnl": 50.0,
        "current_stock_unrealized_pnl": 200.0,
        "current_put_assignment_unrealized_pnl": 0.0,
        "itm_put_cash_required": 2500.0,
        "itm_put_market_value": 0.0,
        "itm_put_contracts": 1,
        "itm_put_shares": 100,
        "available_cash": None,
        "ytd_annualized_twr": 0.18,
        "unrealized_adjusted": True,
        "unrealized_blocked": False,
    }
    assert dashboard["monthly_target"] == {
        "month": "2026-05-31",
        "target_basis": "avg_capital",
        "target_return": 0.015,
        "current_return": 0.01,
        "current_return_metric": "return_roac",
        "current_pnl": 100.0,
        "target_pnl": 150.0,
        "remaining_pnl": 50.0,
        "status": "below_target",
        "realized_month_pnl": 100.0,
        "realized_options_pnl": 100.0,
        "realized_stock_pnl": 0.0,
        "open_expiring_incremental_premium": 625.0,
        "projected_month_pnl": 925.0,
        "projected_return_roac": 0.0925,
        "projected_return_ropc": 0.07708333333333334,
        "projected_remaining_pnl": 0.0,
        "current_unrealized_pnl": 200.0,
        "includes_current_unrealized": True,
        "monthly_target_status": "beat",
        "includes_open_premium": True,
        "projection_basis": "canonical_cycle_projection",
        "days_remaining": 28,
    }
    assert len(dashboard["open_option_short_preview"]) == 2
    assert "notional_at_strike" not in dashboard["open_option_short_preview"][0]
    assert "premium_collected" not in dashboard["open_option_short_preview"][0]
    assert dashboard["open_option_short_preview"][0]["id"].startswith("optlot:")
    assert dashboard["issue_summary"] == {
        "severity": "warning",
        "total_count": 2,
        "price_issue_count": 1,
        "parse_issue_count": 1,
        "audit_issue_count": 0,
        "import_issue_count": 0,
        "top_messages": ["MISS: no price returned", "Mixed-leg option row needs review"],
    }


def test_mobile_dashboard_counts_import_health_warnings():
    dashboard = build_mobile_dashboard(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["IBKR Flex"],
        },
        source_metadata={
            "import_issues": [
                {
                    "category": "import",
                    "severity": "warning",
                    "message": "IBKR import deferred for 2026-05-15: statement was not available yet.",
                    "action": "retry_import",
                }
            ]
        },
    )

    assert dashboard["issue_summary"]["severity"] == "warning"
    assert dashboard["issue_summary"]["import_issue_count"] == 1
    assert "IBKR import deferred for 2026-05-15" in dashboard["issue_summary"]["top_messages"][-1]


def test_mobile_dashboard_matches_contract_fixture():
    dashboard = build_mobile_dashboard(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
        open_option_preview_limit=2,
    )
    json.dumps(dashboard)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_dashboard_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert dashboard == expected


def test_mobile_dashboard_exposes_option_premium_before_itm_put_gap():
    state = _mobile_state()
    state.option_unreal = 700.0
    state.stock_unreal = 200.0
    state.total_unreal = 900.0
    state.inv_df = pd.DataFrame(
        [
            {
                "ticker": "RISK",
                "buy_date": pd.Timestamp("2026-05-01"),
                "shares": 100,
                "cost_per_share": 100.0,
                "current_price": 97.0,
                "covered_shares": 0,
                "covered_strike": pd.NA,
                "unrealized_pnl": -300.0,
                "source": "put_gap",
            }
        ]
    )

    dashboard = build_mobile_dashboard(
        state,
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025"],
        },
    )

    snapshot = dashboard["snapshot"]
    assert snapshot["current_unrealized_pnl"] == 900.0
    assert snapshot["current_option_unrealized_pnl"] == 700.0
    assert snapshot["current_option_premium_unrealized_pnl"] == 1000.0
    assert snapshot["current_put_assignment_unrealized_pnl"] == -300.0
    assert snapshot["current_stock_unrealized_pnl"] == 200.0


def test_mobile_dashboard_blocks_unrealized_snapshot_when_required_prices_missing():
    state = _mobile_state()
    state.unrealized_blocked = True
    dashboard = build_mobile_dashboard(
        state,
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025"],
        },
    )

    assert dashboard["snapshot"]["ytd_total_pnl"] is None
    assert dashboard["snapshot"]["current_unrealized_pnl"] is None
    assert dashboard["snapshot"]["current_option_unrealized_pnl"] is None
    assert dashboard["snapshot"]["current_option_premium_unrealized_pnl"] is None
    assert dashboard["snapshot"]["current_stock_unrealized_pnl"] is None
    assert dashboard["snapshot"]["current_put_assignment_unrealized_pnl"] is None
    assert dashboard["snapshot"]["ytd_annualized_twr"] is None
    assert dashboard["snapshot"]["unrealized_blocked"] is True


def test_mobile_positions_composes_contract_payload():
    positions = build_mobile_positions(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(positions) == {"request", "data_freshness", "inventory", "open_option_shorts"}
    assert positions["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert positions["data_freshness"]["source_sheets"] == [
        {"name": "Options 2025", "status": "loaded", "rows": 2},
        {"name": "Options 2026", "status": "missing", "rows": 0},
    ]
    assert [row["id"] for row in positions["inventory"]] == [
        "inventory:CALL:2026-04-02:stock_lot:0",
    ]
    assert len(positions["open_option_shorts"]) == 5
    assert positions["open_option_shorts"][0]["id"].startswith("optlot:")


def test_mobile_positions_matches_contract_fixture():
    positions = build_mobile_positions(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(positions)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_positions_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert positions == expected


def test_ticker_summary_rows_emit_mobile_contract_shape():
    rows = build_ticker_summary_rows(_mobile_state(), include_history=True)

    assert [row["ticker"] for row in rows] == ["CALL", "PUTT", "MISS", "CLEAR"]
    assert rows[0] == {
        "id": "ticker:CALL",
        "ticker": "CALL",
        "current_price": 60.0,
        "realized_options_pnl": 10.0,
        "realized_stock_pnl": 20.0,
        "dividends": 12.5,
        "combined_realized_pnl": 42.5,
        "unrealized_pnl": 1500.0,
        "current_option_premium_unrealized_pnl": 150.0,
        "current_put_assignment_unrealized_pnl": 0.0,
        "current_option_unrealized_pnl": 150.0,
        "current_stock_unrealized_pnl": 1500.0,
        "total_pnl": 1542.5,
        "open_option_count": 1,
        "inventory_share_count": 100,
        "risk_labels": ["In the money"],
        "history": [
            {
                "id": "year:2026:ticker:CALL",
                "year": 2026,
                "realized_options_pnl": 10.0,
                "realized_stock_pnl": 20.0,
                "dividends": 12.5,
                "combined_realized_pnl": 42.5,
            }
        ],
    }

    putt = rows[1]
    assert putt["id"] == "ticker:PUTT"
    assert putt["dividends"] == 18.0
    assert putt["open_option_count"] == 2
    assert putt["inventory_share_count"] == 0
    assert putt["current_option_premium_unrealized_pnl"] == 400.0
    assert putt["current_put_assignment_unrealized_pnl"] == 0.0
    assert putt["current_option_unrealized_pnl"] == 400.0
    assert putt["current_stock_unrealized_pnl"] == 0.0
    assert putt["risk_labels"] == ["In the money"]
    assert [row["id"] for row in putt["history"]] == ["year:2025:ticker:PUTT", "year:2026:ticker:PUTT"]

    missing = rows[2]
    assert missing["current_price"] is None
    assert missing["risk_labels"] == ["Missing price"]

    clear = rows[3]
    assert clear["risk_labels"] == ["Largest unrealized loser"]


def test_ticker_summary_rows_support_year_filter_and_blocked_unrealized():
    state = _mobile_state()
    state.unrealized_blocked = True
    rows = build_ticker_summary_rows(state, year=2026, include_history=True)

    putt = next(row for row in rows if row["ticker"] == "PUTT")
    assert putt["realized_options_pnl"] == 150.0
    assert putt["dividends"] == 11.0
    assert putt["combined_realized_pnl"] == 161.0
    assert putt["unrealized_pnl"] is None
    assert putt["current_option_premium_unrealized_pnl"] is None
    assert putt["current_put_assignment_unrealized_pnl"] is None
    assert putt["current_option_unrealized_pnl"] is None
    assert putt["current_stock_unrealized_pnl"] is None
    assert putt["total_pnl"] is None
    assert [row["id"] for row in putt["history"]] == ["year:2026:ticker:PUTT"]


def test_ticker_summary_rows_split_open_itm_put_unrealized_components():
    state = SimpleNamespace(
        as_of=pd.Timestamp("2026-05-03"),
        open_options=pd.DataFrame(
            [
                {
                    "ticker": "FUTU",
                    "type": "Put",
                    "strike": 140.0,
                    "qty": 1,
                    "expiration": pd.Timestamp("2026-05-17"),
                    "trans_date": pd.Timestamp("2026-04-01"),
                    "open_price": 1.37,
                }
            ]
        ),
        stock_prices={"FUTU": 137.12},
        inv_df=pd.DataFrame(
            [
                {
                    "ticker": "FUTU",
                    "buy_date": pd.NaT,
                    "shares": 100,
                    "cost_per_share": 140.0,
                    "current_price": 137.12,
                    "covered_shares": 0,
                    "covered_strike": 140.0,
                    "unrealized_pnl": -288.0,
                    "source": "put_gap",
                }
            ]
        ),
        per_ticker=pd.DataFrame(columns=["year", "ticker", "options_pnl", "stock_realized_pnl", "combined_realized"]),
        per_ticker_totals=pd.DataFrame(
            [
                {
                    "ticker": "FUTU",
                    "options_pnl": 0.0,
                    "stock_realized_pnl": 0.0,
                    "combined_realized": 0.0,
                    "unrealized_pnl": -151.0,
                    "total_pnl": -151.0,
                }
            ]
        ),
        div_df=pd.DataFrame(columns=["ticker", "pay_date", "cash"]),
        missing_required_price_tickers=[],
        unrealized_blocked=False,
    )

    futu = build_ticker_summary_rows(state)[0]

    assert futu["realized_options_pnl"] == 0.0
    assert futu["unrealized_pnl"] == -151.0
    assert futu["current_option_premium_unrealized_pnl"] == 137.0
    assert futu["current_put_assignment_unrealized_pnl"] == -288.0
    assert futu["current_option_unrealized_pnl"] == -151.0
    assert futu["current_stock_unrealized_pnl"] == 0.0
    assert futu["total_pnl"] == -151.0


def test_mobile_tickers_composes_contract_payload():
    tickers = build_mobile_tickers(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        year=2026,
        include_history=True,
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(tickers) == {"request", "data_freshness", "items"}
    assert tickers["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert tickers["data_freshness"]["source_sheets"] == [
        {"name": "Options 2025", "status": "loaded", "rows": 2},
        {"name": "Options 2026", "status": "missing", "rows": 0},
    ]
    assert [row["id"] for row in tickers["items"]] == [
        "ticker:CALL",
        "ticker:PUTT",
        "ticker:MISS",
        "ticker:CLEAR",
    ]
    assert tickers["items"][1]["history"] == [
        {
            "id": "year:2026:ticker:PUTT",
            "year": 2026,
            "realized_options_pnl": 150.0,
            "realized_stock_pnl": 0.0,
            "dividends": 11.0,
            "combined_realized_pnl": 161.0,
        }
    ]


def test_mobile_tickers_matches_contract_fixture():
    tickers = build_mobile_tickers(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        year=2026,
        include_history=True,
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(tickers)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_tickers_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert tickers == expected


def test_monthly_performance_rows_emit_mobile_contract_shape():
    rows = build_monthly_performance_rows(_mobile_state(), target_return=0.015, monthly_range="ytd")

    assert rows == [
        {
            "id": "month:2026-04-30",
            "month": "2026-04-30",
            "realized_options_pnl": 250.0,
            "realized_stock_pnl": 50.0,
            "dividends": 0.0,
            "total_realized_pnl": 300.0,
            "avg_capital": 10000.0,
            "peak_capital": 11000.0,
            "return_roac": 0.03,
            "return_ropc": 0.02727272727272727,
            "target_return": 0.015,
            "status": "beat",
            "realized_month_pnl": 300.0,
            "open_expiring_incremental_premium": 0.0,
            "includes_open_premium": False,
            "projection_basis": "realized_only",
            "projected_month_pnl": 300.0,
            "projected_return_roac": 0.03,
            "projected_return_ropc": 0.02727272727272727,
            "target_pnl": 150.0,
            "projected_remaining_pnl": 0.0,
            "current_unrealized_pnl": None,
            "includes_current_unrealized": False,
            "monthly_target_status": "beat",
        },
        {
            "id": "month:2026-05-31",
            "month": "2026-05-31",
            "realized_options_pnl": 100.0,
            "realized_stock_pnl": 0.0,
            "dividends": 0.0,
            "total_realized_pnl": 100.0,
            "avg_capital": 10000.0,
            "peak_capital": 12000.0,
            "return_roac": 0.01,
            "return_ropc": 0.008333333333333333,
            "target_return": 0.015,
            "status": "below_target",
            "realized_month_pnl": 100.0,
            "open_expiring_incremental_premium": 625.0,
            "includes_open_premium": True,
            "projection_basis": "canonical_cycle_projection",
            "projected_month_pnl": 925.0,
            "projected_return_roac": 0.0925,
            "projected_return_ropc": 0.07708333333333334,
            "target_pnl": 150.0,
            "projected_remaining_pnl": 0.0,
            "current_unrealized_pnl": 200.0,
            "includes_current_unrealized": True,
            "monthly_target_status": "beat",
        },
    ]


def test_monthly_performance_rows_support_ranges_and_missing_capital():
    state = _mobile_state()
    state.monthly_cycles.loc[pd.Timestamp("2026-05-31"), ["avg_capital", "roac"]] = pd.NA

    rows = build_monthly_performance_rows(state, target_return=0.015, monthly_range="3m")

    assert [row["id"] for row in rows] == ["month:2026-04-30", "month:2026-05-31"]
    assert rows[1]["return_roac"] is None
    assert rows[1]["status"] == "unavailable"
    assert rows[1]["projected_return_roac"] == pytest.approx(0.07708333333333334)
    assert rows[1]["projected_return_ropc"] == pytest.approx(0.07708333333333334)
    assert rows[1]["monthly_target_status"] == "beat"

    with pytest.raises(ValueError):
        build_monthly_performance_rows(state, monthly_range="unsupported")


def test_future_monthly_performance_rows_emit_open_expiry_months():
    rows = build_future_monthly_performance_rows(_mobile_state_with_future_september(), target_return=0.015)

    assert [row["id"] for row in rows] == ["month:2026-06-30", "month:2026-09-30"]
    assert rows[0]["open_ticker_count"] == 1
    assert rows[0]["open_option_count"] == 2
    assert rows[0]["open_expiring_incremental_premium"] == pytest.approx(200.0)
    assert rows[0]["projected_month_pnl"] == pytest.approx(200.0)
    assert rows[0]["projected_return_roac"] == pytest.approx(0.02)
    assert rows[0]["target_pnl"] == pytest.approx(150.0)
    assert rows[0]["projection_basis"] == "canonical_cycle_projection"
    assert rows[0]["cycle_projection"]["projected_cycle_pnl"] == pytest.approx(200.0)
    assert rows[1]["open_expiring_incremental_premium"] == pytest.approx(300.0)
    assert rows[1]["projected_month_pnl"] == pytest.approx(300.0)
    assert rows[1]["projected_return_roac"] == pytest.approx(0.03)


def test_current_month_performance_uses_active_cycle_when_month_row_missing():
    state = _mobile_state()
    state.as_of = pd.Timestamp("2026-06-01")

    current = build_mobile_monthly_performance(
        state,
        {"as_of": state.as_of, "include_unrealized": True},
        target_return=0.02,
        monthly_range="ytd",
    )["current_month"]

    assert current["month"] == "2026-06-30"
    assert current["avg_capital"] == 10000.0
    assert current["target_pnl"] == 200.0
    assert current["open_expiring_incremental_premium"] == 200.0
    assert current["projected_month_pnl"] == 400.0
    assert current["projected_return_roac"] == 0.04
    assert current["projected_remaining_pnl"] == 0.0
    assert current["monthly_target_status"] == "beat"
    assert current["return_roac"] == 0.04


def test_mobile_monthly_performance_composes_contract_payload():
    monthly = build_mobile_monthly_performance(
        _mobile_state_with_future_september(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        target_return=0.015,
        monthly_range="ytd",
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(monthly) == {
        "request",
        "data_freshness",
        "target_return",
        "target_basis",
        "return_metric",
        "current_month",
        "months",
        "future_months",
    }
    assert monthly["current_month"] == {
        "id": "month:2026-05-31",
        "month": "2026-05-31",
        "return_roac": 0.01,
        "return_ropc": 0.008333333333333333,
        "total_realized_pnl": 100.0,
        "realized_month_pnl": 100.0,
        "realized_options_pnl": 100.0,
        "realized_stock_pnl": 0.0,
        "open_expiring_incremental_premium": 625.0,
        "includes_open_premium": True,
        "projection_basis": "canonical_cycle_projection",
        "projected_month_pnl": 925.0,
        "projected_return_roac": 0.0925,
        "projected_return_ropc": 0.07708333333333334,
        "target_pnl": 150.0,
        "remaining_pnl": 50.0,
        "projected_remaining_pnl": 0.0,
        "current_unrealized_pnl": 200.0,
        "includes_current_unrealized": True,
        "avg_capital": 10000.0,
        "peak_capital": 12000.0,
        "status": "below_target",
        "monthly_target_status": "beat",
        "days_remaining": 28,
    }
    assert [row["id"] for row in monthly["months"]] == ["month:2026-04-30", "month:2026-05-31"]
    assert [row["id"] for row in monthly["future_months"]] == ["month:2026-06-30", "month:2026-09-30"]


def test_mobile_monthly_performance_matches_contract_fixture():
    monthly = build_mobile_monthly_performance(
        _mobile_state_with_future_september(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        target_return=0.015,
        monthly_range="ytd",
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(monthly)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_monthly_performance_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert monthly == expected


def test_yearly_performance_rows_emit_mobile_contract_shape():
    rows = build_yearly_performance_rows(_mobile_state(), include_unrealized=True)

    assert rows == [
        {
            "id": "year:2025",
            "year": 2025,
            "realized_options_pnl": 50.0,
            "realized_stock_pnl": 0.0,
            "dividends": 0.0,
            "total_realized_pnl": 50.0,
            "total_pnl_including_unrealized": None,
            "avg_capital": 8000.0,
            "peak_capital": 9000.0,
            "roac_year": 0.05,
            "ropc_year": 0.044444444444444446,
            "annualized_roac": 0.05,
            "annualized_ropc": 0.044444444444444446,
            "annualized_twr": 0.04,
            "annualized_twr_active": 0.045,
            "annualized_twr_unrealized_adjusted": None,
            "metrics_available": True,
            "suppression_reason": None,
        },
        {
            "id": "year:2026",
            "year": 2026,
            "realized_options_pnl": 100.0,
            "realized_stock_pnl": 0.0,
            "dividends": 0.0,
            "total_realized_pnl": 100.0,
            "total_pnl_including_unrealized": 350.0,
            "avg_capital": 10000.0,
            "peak_capital": 12000.0,
            "roac_year": 0.01,
            "ropc_year": 0.008333333333333333,
            "annualized_roac": 0.07,
            "annualized_ropc": 0.06,
            "annualized_twr": 0.12,
            "annualized_twr_active": 0.13,
            "annualized_twr_unrealized_adjusted": 0.18,
            "metrics_available": True,
            "suppression_reason": None,
        },
    ]


def test_yearly_performance_rows_suppress_affected_year_metrics():
    state = _mobile_state()
    state.capital_history_affected_years = [2025]

    rows = build_yearly_performance_rows(state, include_unrealized=True)
    affected = rows[0]

    assert affected["id"] == "year:2025"
    assert affected["roac_year"] is None
    assert affected["ropc_year"] is None
    assert affected["annualized_roac"] is None
    assert affected["annualized_ropc"] is None
    assert affected["annualized_twr"] is None
    assert affected["annualized_twr_active"] is None
    assert affected["annualized_twr_unrealized_adjusted"] is None
    assert affected["metrics_available"] is False
    assert affected["suppression_reason"] == "capital_history_incomplete"


def test_mobile_yearly_performance_composes_contract_payload():
    yearly = build_mobile_yearly_performance(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(yearly) == {"request", "data_freshness", "years"}
    assert yearly["request"] == {
        "as_of": "2026-05-03",
        "include_unrealized": True,
        "selected_sheets": ["Options 2025", "Options 2026"],
    }
    assert yearly["data_freshness"]["source_sheets"] == [
        {"name": "Options 2025", "status": "loaded", "rows": 2},
        {"name": "Options 2026", "status": "missing", "rows": 0},
    ]
    assert [row["id"] for row in yearly["years"]] == ["year:2025", "year:2026"]
    assert yearly["years"][0]["total_pnl_including_unrealized"] is None
    assert yearly["years"][1]["total_pnl_including_unrealized"] == 350.0


def test_mobile_yearly_performance_matches_contract_fixture():
    yearly = build_mobile_yearly_performance(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(yearly)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_yearly_performance_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert yearly == expected


def test_mobile_issue_rows_classify_raw_backend_messages():
    rows = build_mobile_issue_rows(_mobile_state())

    assert rows == [
        {
            "id": "price-coverage-stocks",
            "category": "price",
            "severity": "warning",
            "message": "Price coverage incomplete: Stocks priced: 3/4",
            "tickers": ["MISS"],
            "action": "refresh_prices",
        },
        {
            "id": "current-price-1",
            "category": "price",
            "severity": "warning",
            "message": "MISS: no price returned",
            "tickers": ["MISS"],
            "action": "refresh_prices",
        },
        {
            "id": "parse-1",
            "category": "parse",
            "severity": "warning",
            "message": "Mixed-leg option row needs review",
            "tickers": [],
            "action": "fix_workbook_row",
        },
        {
            "id": "historical-price-1",
            "category": "historical_price",
            "severity": "warning",
            "message": "CAPGAP: missing historical price series",
            "tickers": ["CAPGAP"],
            "action": "refresh_data",
        },
        {
            "id": "dividend-1",
            "category": "dividend",
            "severity": "warning",
            "message": "DIVMISS: dividend history returned no usable data",
            "tickers": ["DIVMISS"],
            "action": "refresh_data",
        },
        {
            "id": "capital-history-1",
            "category": "capital_history",
            "severity": "warning",
            "message": "Capital history coverage incomplete for CAPGAP: missing historical price before first trade",
            "tickers": ["CAPGAP"],
            "action": "refresh_data",
        },
    ]


def test_mobile_issue_rows_classify_expected_ibkr_wheel_exclusions_as_info():
    state = SimpleNamespace(
        issues=[
            "Excluded ABC call execution on 2026-01-20 because no prior put-assignment stock inventory was held.",
            "Excluded ABC call roll replacement on 2026-01-20 because the closed call lot was non-wheel.",
            "Ignored 100 assigned-call sold shares of XYZ on 2026-02-20 because no assignment-derived stock inventory was available.",
            "Excluded 10 SPY put spread contracts on 2022-12-28 because the short put was opened with a protective long put.",
            "Excluded 10 SPY put spread close contracts on 2023-01-10 because they close a non-wheel spread put lot.",
            "Excluded 4 ASAN call spread contracts on 2024-12-02 because the short call was opened with a protective long call.",
        ],
        price_errors=[],
        price_summary={},
        historical_price_errors=[],
        historical_price_summary={},
        dividend_errors=[],
        dividend_summary={},
        capital_history_coverage_issues=[],
    )

    rows = build_mobile_issue_rows(state)

    assert [row["category"] for row in rows] == ["wheel_audit"] * 6
    assert [row["severity"] for row in rows] == ["info"] * 6
    assert [row["action"] for row in rows] == [None] * 6
    assert build_mobile_issues(state, {"selected_sheets": ["IBKR Flex"], "include_unrealized": True})["summary"] == {
        "severity": "ok",
        "total_count": 0,
        "info_count": 6,
        "unrealized_blocked": False,
        "capital_history_incomplete": False,
        "dividend_coverage_complete": True,
    }
    payload = build_mobile_issues(state, {"selected_sheets": ["IBKR Flex"], "include_unrealized": True})
    assert payload["issues"] == []
    assert len(payload["audit_notes"]) == 6
    assert payload["audit_summary"] == {
        "total_count": 6,
        "by_category": {"wheel_audit": 6},
        "by_severity": {"info": 6},
    }


def test_mobile_issue_rows_classify_actionable_ibkr_accounting_warnings():
    state = SimpleNamespace(
        issues=[
            "Prorated GOOGL call execution on 2022-10-05 to 100 wheel-held shares out of 200 required shares.",
            "Buy GOOGL Put 125.0 on 2026-01-20 had no open short to close.",
            "Unmatched buy quantity for GOOGL Put 125.0 on 2026-01-20: 1 remaining.",
        ],
        price_errors=[],
        price_summary={},
        historical_price_errors=[],
        historical_price_summary={},
        dividend_errors=[],
        dividend_summary={},
        capital_history_coverage_issues=[],
    )

    rows = build_mobile_issue_rows(state)

    assert [row["id"] for row in rows] == ["wheel-warning-1", "missing-basis-2", "missing-basis-3"]
    assert [row["category"] for row in rows] == ["wheel_warning", "missing_basis", "missing_basis"]
    assert [row["severity"] for row in rows] == ["warning", "warning", "warning"]
    assert [row["action"] for row in rows] == ["review_source_data", "review_source_data", "review_source_data"]


def test_mobile_issue_rows_classify_known_historical_ibkr_warnings_as_audit_notes():
    state = SimpleNamespace(
        issues=[
            "Prorated ASAN call execution on 2022-11-23 to 100 wheel-held shares out of 400 required shares.",
            "Prorated ASAN call execution on 2023-08-21 to 100 wheel-held shares out of 400 required shares.",
            "Buy ASAN Put 25.0 on 2022-05-20 had no open short to close.",
            "Unmatched buy quantity for ASAN Put 25.0 on 2022-05-20: 1 remaining.",
            "Buy CROX Put 60.0 on 2022-05-20 had no open short to close.",
            "Unmatched buy quantity for CROX Put 60.0 on 2022-05-20: 1 remaining.",
        ],
        price_errors=[],
        price_summary={},
        historical_price_errors=[],
        historical_price_summary={},
        dividend_errors=[],
        dividend_summary={},
        capital_history_coverage_issues=[],
    )

    rows = build_mobile_issue_rows(state)
    payload = build_mobile_issues(state, {"selected_sheets": ["IBKR Flex"], "include_unrealized": True})

    assert [row["category"] for row in rows] == ["wheel_audit"] * 6
    assert [row["severity"] for row in rows] == ["info"] * 6
    assert payload["issues"] == []
    assert len(payload["audit_notes"]) == 6
    assert payload["summary"]["total_count"] == 0


def test_mobile_issues_composes_contract_payload():
    issues = build_mobile_issues(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )

    assert set(issues) == {
        "request",
        "data_freshness",
        "summary",
        "issues",
        "audit_summary",
        "audit_notes",
        "coverage",
    }
    assert issues["summary"] == {
        "severity": "warning",
        "total_count": 6,
        "info_count": 0,
        "unrealized_blocked": False,
        "capital_history_incomplete": True,
        "dividend_coverage_complete": False,
    }
    assert issues["coverage"] == {
        "current_prices": {
            "requested": 4,
            "fetched": 3,
            "missing_tickers": ["MISS"],
            "errors": ["MISS: no price returned"],
        },
        "historical_prices": {
            "requested": 4,
            "fetched": 3,
            "errors": ["CAPGAP: missing historical price series"],
        },
        "dividends": {
            "attempted_tickers": 2,
            "failed_tickers": 1,
            "errors": ["DIVMISS: dividend history returned no usable data"],
        },
    }
    assert issues["audit_summary"] == {"total_count": 0, "by_category": {}, "by_severity": {}}
    assert issues["audit_notes"] == []


def test_mobile_issues_exposes_import_health_warning():
    payload = build_mobile_issues(
        SimpleNamespace(
            as_of=pd.Timestamp("2026-05-16"),
            sheet_counts=pd.DataFrame({"source_sheet": ["IBKR Flex"], "rows": [637]}),
            price_errors=[],
            price_summary={},
            historical_price_errors=[],
            historical_price_summary={},
            dividend_errors=[],
            dividend_summary={},
            capital_history_coverage_issues=[],
            issues=[],
        ),
        {"selected_sheets": ["IBKR Flex"], "include_unrealized": True},
        source_metadata={
            "import_issues": [
                {
                    "category": "import",
                    "severity": "warning",
                    "message": "IBKR import deferred for 2026-05-15: statement was not available yet.",
                    "action": "retry_import",
                }
            ]
        },
    )

    assert payload["summary"]["severity"] == "warning"
    assert payload["summary"]["total_count"] == 1
    assert payload["issues"] == [
        {
            "id": "import-1",
            "category": "import",
            "severity": "warning",
            "message": "IBKR import deferred for 2026-05-15: statement was not available yet.",
            "tickers": [],
            "action": "retry_import",
        }
    ]


def test_mobile_issues_matches_contract_fixture():
    issues = build_mobile_issues(
        _mobile_state(),
        {
            "as_of": pd.Timestamp("2026-05-03"),
            "include_unrealized": True,
            "selected_sheets": ["Options 2025", "Options 2026"],
        },
        available_sheets=["Options 2025"],
        source_metadata={
            "pipeline_built_at": "2026-05-04T19:00:00+02:00",
            "prices_updated_at": "2026-05-04T19:01:00+02:00",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
        },
    )
    json.dumps(issues)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_issues_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert issues == expected


def test_mobile_config_composes_contract_payload():
    config = build_mobile_config(
        ["Options 2024", "Options 2025"],
        {"selected_sheets": ["Options 2024", "Options 2025", "Options 2026"], "include_unrealized": True},
        default_sheets=["Options 2024", "Options 2025", "Options 2026"],
        source_metadata={
            "kind": "local_excel",
            "name": "latest_download.xlsx",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
            "source_modified_at": None,
        },
        as_of_default=pd.Timestamp("2026-05-03"),
    )

    assert config == {
        "available_sheets": ["Options 2024", "Options 2025"],
        "default_selected_sheets": ["Options 2024", "Options 2025", "Options 2026"],
        "missing_default_sheets": ["Options 2026"],
        "include_unrealized_default": True,
        "as_of_default": "2026-05-03",
        "source": {
            "kind": "local_excel",
            "name": "latest_download.xlsx",
            "downloaded_at": "2026-05-04T18:59:00+02:00",
            "modified_at": None,
        },
        "capabilities": {
            "supports_price_refresh": True,
            "supports_data_rebuild": True,
            "supports_selected_sheets": True,
            "supports_as_of": True,
        },
    }


def test_mobile_config_matches_contract_fixture():
    config = build_mobile_config(
        ["Options 2024", "Options 2025"],
        {"selected_sheets": ["Options 2024", "Options 2025", "Options 2026"], "include_unrealized": True},
        default_sheets=["Options 2024", "Options 2025", "Options 2026"],
        source_metadata={
            "kind": "local_excel",
            "name": "latest_download.xlsx",
            "source_downloaded_at": "2026-05-04T18:59:00+02:00",
            "source_modified_at": None,
        },
        as_of_default=pd.Timestamp("2026-05-03"),
    )
    json.dumps(config)

    fixture_path = Path(__file__).parent / "fixtures" / "mobile_config_v1.json"
    expected = json.loads(fixture_path.read_text())

    assert config == expected
