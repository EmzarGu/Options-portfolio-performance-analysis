from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from portfolio_backend.ibkr.flex_parser import IbkrFlexReport
from portfolio_backend.ibkr.pipeline import build_ibkr_pipeline
from portfolio_backend.mobile_api_service import (
    MobilePayloadContext,
    MobilePayloadRequest,
    MobileServiceDependencies,
)


def build_ibkr_mobile_payload_context(
    request: MobilePayloadRequest,
    dependencies: MobileServiceDependencies,
    report: IbkrFlexReport,
    *,
    available_sheets: Optional[Iterable[str]] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
) -> MobilePayloadContext:
    metadata = dict(source_metadata or {})
    metadata.setdefault("source", "ibkr_flex")
    metadata.setdefault("from_date", report.metadata.get("fromDate"))
    metadata.setdefault("to_date", report.metadata.get("toDate"))
    metadata.setdefault("source_files", report.metadata.get("sourceFiles"))
    metadata.setdefault("pipeline_built_at", datetime.now().astimezone().isoformat(timespec="seconds"))
    if dependencies.fetch_current_prices is not None:
        metadata.setdefault("prices_updated_at", datetime.now().astimezone().isoformat(timespec="seconds"))

    state = build_ibkr_pipeline(
        report,
        as_of=request.as_of,
        include_unrealized_current_year=request.include_unrealized,
        fetch_price_history_fn=dependencies.fetch_price_history,
        align_benchmarks_monthly_fn=dependencies.align_benchmarks_monthly,
        fetch_current_prices_fn=dependencies.fetch_current_prices,
        selected_sheets=request.selected_sheets,
        cache_bust=request.cache_bust,
        price_updated_at=metadata.get("prices_updated_at"),
    )
    return MobilePayloadContext(
        state=state,
        request={
            "as_of": request.as_of,
            "include_unrealized": request.include_unrealized,
            "selected_sheets": request.selected_sheets,
        },
        available_sheets=[str(sheet) for sheet in available_sheets] if available_sheets is not None else None,
        source_metadata=metadata,
    )
