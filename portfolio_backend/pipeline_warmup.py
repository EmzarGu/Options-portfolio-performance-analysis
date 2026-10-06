"""Prepare and verify the shared dashboard snapshot after an IBKR import."""
from __future__ import annotations

from time import perf_counter
import os
import logging


def warm_dashboard_snapshot() -> dict:
    # Keep the import worker lightweight until importing has finished.
    from portfolio_backend import context_runtime as runtime
    from portfolio_backend.pipeline_snapshot_store import pipeline_snapshot_id

    if runtime._data_source() != runtime.DATA_SOURCE_IBKR:
        raise RuntimeError("Dashboard preparation requires OPTIONS_DATA_SOURCE=ibkr")
    started = perf_counter()
    timings = {}
    context = runtime.get_context(
        as_of=None,
        include_unrealized=False,
        selected_sheets=None,
        cache_bust=None,
        timing_recorder=lambda name, value: timings.__setitem__(name, round(value, 2)),
    )
    metadata = context.source_metadata or {}
    source_id = metadata.get("source_snapshot_id")
    if not source_id:
        raise RuntimeError("Dashboard preparation has no successful IBKR import marker")
    snapshot_id = pipeline_snapshot_id(
        source_snapshot_id=source_id,
        as_of=context.request["as_of"],
        selected_sheets=context.request["selected_sheets"],
    )
    # Runtime writes are best-effort for interactive requests. The job must
    # confirm persistence before claiming the next browser request is prepared.
    snapshot = runtime.get_default_pipeline_snapshot_store().load(snapshot_id)
    if snapshot is None:
        raise RuntimeError("Prepared dashboard snapshot was not persisted")
    result = {
        "status": "succeeded",
        "snapshot_id": snapshot_id,
        "as_of": str(context.request["as_of"]),
        "elapsed_ms": round((perf_counter() - started) * 1000, 2),
        "timings": timings,
    }
    if os.getenv("DECISION_LAB_PROVIDER") == "marketdata":
        try:
            from portfolio_backend.option_market.marketdata_production import prepare_from_context
            result["option_data"] = prepare_from_context(context)
        except Exception:
            logging.getLogger(__name__).exception("Scheduled option-data preparation failed")
            result["option_data"] = {"status": "failed", "message": "Option data preparation failed; portfolio import remains valid"}
    return result
