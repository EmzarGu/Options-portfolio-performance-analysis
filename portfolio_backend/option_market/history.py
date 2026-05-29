from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Protocol

from portfolio_backend.option_market.models import (
    OptionHistoricalEnrichment,
    OptionTradeCandidate,
    float_or_none,
    int_or_none,
    normalize_put_call,
    now_iso,
)
from portfolio_backend.option_market.store import OptionMarketStore


@dataclass(frozen=True)
class HistoricalEnrichmentResult:
    run_doc: dict[str, Any]
    enrichments: list[OptionHistoricalEnrichment]


class HistoricalOptionDataProvider(Protocol):
    provider: str

    def fetch_historical_contracts(
        self,
        *,
        ticker: str,
        trade_date: Any,
        expiry: Any,
        put_call: str,
    ) -> list[dict[str, Any]]:
        ...

    def fetch_option_daily_aggregate(self, option_symbol: str, trade_date: Any) -> dict[str, Any]:
        ...


def enrich_historical_option_trade(
    trade: OptionTradeCandidate,
    *,
    provider: HistoricalOptionDataProvider,
    contract_rows: Optional[list[dict[str, Any]]] = None,
    aggregate: Optional[dict[str, Any]] = None,
) -> OptionHistoricalEnrichment:
    warnings: list[str] = []
    aggregate_provided = aggregate is not None
    if contract_rows is None:
        contract_rows = provider.fetch_historical_contracts(
            ticker=trade.ticker,
            trade_date=trade.trade_date,
            expiry=trade.expiry,
            put_call=trade.put_call,
        )
    contract = _match_contract_row(contract_rows, trade.strike)
    if contract is None:
        warnings.append("missing_provider_contract")
    contract_symbol = contract.get("ticker") if contract else None
    aggregate = dict(aggregate or {})
    if contract_symbol:
        if not aggregate and not aggregate_provided:
            try:
                aggregate = provider.fetch_option_daily_aggregate(str(contract_symbol), trade.trade_date)
            except Exception as exc:
                warnings.append(f"missing_option_daily_aggregate:{exc}")
        elif not aggregate:
            warnings.append("missing_option_daily_aggregate")
    else:
        warnings.append("missing_contract_symbol")

    premium = abs(float(trade.net_cash))
    capital = abs(float(trade.strike) * float(trade.qty) * 100)
    return OptionHistoricalEnrichment(
        trade=trade,
        provider=provider.provider,
        contract_symbol=str(contract_symbol) if contract_symbol else None,
        provider_contract_matched=contract is not None,
        option_close=float_or_none(aggregate.get("c")),
        option_vwap=float_or_none(aggregate.get("vw")),
        option_volume=int_or_none(aggregate.get("v")),
        option_trade_count=int_or_none(aggregate.get("n")),
        underlying_price=None,
        entry_moneyness=None,
        dte=max((trade.expiry - trade.trade_date).days, 0),
        premium_per_capital=premium / capital if capital else None,
        profit_probability_source="google_sheet" if trade.profit_probability is not None else "unavailable",
        risk_proxy_source="sheet_probability" if trade.profit_probability is not None else "unavailable",
        warnings=tuple(warnings),
    )


def run_historical_option_enrichment(
    trades: Iterable[OptionTradeCandidate],
    *,
    store: OptionMarketStore,
    provider: HistoricalOptionDataProvider,
    missing_only: bool = True,
    max_provider_calls: Optional[int] = None,
) -> HistoricalEnrichmentResult:
    run_id = f"option-history-{provider.provider}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    trade_list = list(trades)
    run_doc = {
        "run_id": run_id,
        "provider": provider.provider,
        "status": "running",
        "started_at": now_iso(),
        "trade_count": len(trade_list),
        "purpose": "historical_option_enrichment",
        "missing_only": missing_only,
    }
    store.begin_historical_enrichment_run(run_doc)
    enrichments: list[OptionHistoricalEnrichment] = []
    provider_calls = 0
    skipped_existing = 0
    skipped_budget = 0
    errors: list[str] = []
    contract_cache: dict[tuple[str, Any, Any, str], list[dict[str, Any]]] = {}
    aggregate_cache: dict[tuple[str, Any], dict[str, Any]] = {}
    for trade in trade_list:
        existing = store.load_historical_enrichment(provider.provider, trade.trade_id) if missing_only else None
        if existing:
            skipped_existing += 1
            continue
        if max_provider_calls is not None and provider_calls >= max_provider_calls:
            skipped_budget += 1
            continue
        try:
            request_key = trade.request_key
            if request_key not in contract_cache:
                if max_provider_calls is not None and provider_calls >= max_provider_calls:
                    skipped_budget += 1
                    continue
                contract_cache[request_key] = provider.fetch_historical_contracts(
                    ticker=trade.ticker,
                    trade_date=trade.trade_date,
                    expiry=trade.expiry,
                    put_call=trade.put_call,
                )
                provider_calls += 1
            contract_rows = contract_cache[request_key]
            contract = _match_contract_row(contract_rows, trade.strike)
            aggregate: dict[str, Any] = {}
            contract_symbol = contract.get("ticker") if contract else None
            if contract_symbol:
                aggregate_key = (str(contract_symbol), trade.trade_date)
                if aggregate_key not in aggregate_cache:
                    if max_provider_calls is None or provider_calls < max_provider_calls:
                        aggregate_cache[aggregate_key] = provider.fetch_option_daily_aggregate(
                            str(contract_symbol),
                            trade.trade_date,
                        )
                        provider_calls += 1
                    else:
                        aggregate_cache[aggregate_key] = {}
                aggregate = aggregate_cache[aggregate_key]
            enrichments.append(
                enrich_historical_option_trade(
                    trade,
                    provider=provider,
                    contract_rows=contract_rows,
                    aggregate=aggregate,
                )
            )
        except Exception as exc:
            errors.append(f"{trade.ticker} {trade.trade_date} {trade.expiry} {trade.put_call} {trade.strike}: {exc}")
    if enrichments:
        store.save_historical_enrichments(enrichments)
    warning_counts = Counter(warning for enrichment in enrichments for warning in enrichment.warnings)
    finish_doc = {
        "status": "succeeded",
        "finished_at": now_iso(),
        "trade_count": len(trade_list),
        "enriched_count": len(enrichments),
        "provider_contract_matched_count": sum(1 for row in enrichments if row.provider_contract_matched),
        "option_price_observed_count": sum(1 for row in enrichments if row.option_close is not None or row.option_vwap is not None),
        "skipped_existing_count": skipped_existing,
        "skipped_budget_count": skipped_budget,
        "provider_call_estimate": provider_calls,
        "unique_chain_request_count": len(contract_cache),
        "unique_option_aggregate_request_count": len(aggregate_cache),
        "warning_counts": dict(warning_counts),
        "errors": errors[:20],
        "enrichment_ids": [row.enrichment_id for row in enrichments],
    }
    store.finish_historical_enrichment_run(run_id, finish_doc)
    return HistoricalEnrichmentResult(run_doc={**run_doc, **finish_doc}, enrichments=enrichments)


def historical_enrichment_to_probability_match(doc: dict[str, Any]) -> dict[str, Any]:
    trade = doc.get("trade") if isinstance(doc.get("trade"), dict) else {}
    matched = bool(doc.get("provider_contract_matched") or trade.get("profit_probability") is not None)
    return {
        "match_id": doc.get("enrichment_id"),
        "trade": trade,
        "matched": matched,
        "provider": doc.get("provider"),
        "provider_contract_matched": bool(doc.get("provider_contract_matched")),
        "historical_provider_contract_matched": bool(doc.get("provider_contract_matched")),
        "profit_probability": trade.get("profit_probability"),
        "assignment_risk_proxy": trade.get("assignment_risk_proxy"),
        "risk_proxy_source": doc.get("risk_proxy_source") or "unavailable",
        "profit_probability_source": doc.get("profit_probability_source") or "unavailable",
        "option_close": doc.get("option_close"),
        "option_vwap": doc.get("option_vwap"),
        "option_volume": doc.get("option_volume"),
        "option_trade_count": doc.get("option_trade_count"),
        "dte": doc.get("dte"),
        "premium_per_capital": doc.get("premium_per_capital"),
        "warnings": doc.get("warnings") or [],
    }


def _match_contract_row(rows: list[dict[str, Any]], strike: float) -> Optional[dict[str, Any]]:
    compatible = []
    for row in rows:
        row_strike = float_or_none(row.get("strike_price") or row.get("strike"))
        if row_strike is not None and abs(row_strike - float(strike)) <= 0.001:
            compatible.append(row)
    return compatible[0] if compatible else None
