import json
from datetime import date
from types import SimpleNamespace

import pytest

from portfolio_backend import context_runtime as runtime
from portfolio_backend.ibkr import import_job
from portfolio_backend.mobile_api_service import MobilePayloadContext
from portfolio_backend.pipeline_snapshot_store import MemoryPipelineSnapshotStore
from portfolio_backend.pipeline_warmup import warm_dashboard_snapshot


def test_import_preparation_is_reused_after_process_cache_clear_and_new_import_invalidates(monkeypatch):
    store = MemoryPipelineSnapshotStore()
    marker = {"source_snapshot_id": "import-1", "import_run_id": "run-1"}
    builds = []
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setattr(runtime, "get_default_pipeline_snapshot_store", lambda: store)
    monkeypatch.setattr(runtime, "_refresh_source_marker", lambda **kw: dict(marker))
    monkeypatch.setattr(runtime, "_default_as_of_date", lambda: date(2026, 9, 18))
    monkeypatch.setattr(runtime, "_resolve_dependencies", lambda *a: None)
    monkeypatch.setattr(runtime, "load_flex_report_from_env", lambda: None)

    def build(request, dependencies, report, *, available_sheets, source_metadata, **kw):
        builds.append(source_metadata["source_snapshot_id"])
        state = SimpleNamespace(value=len(builds))
        return MobilePayloadContext(state=state, base_state=state,
            request={"as_of": request.as_of, "selected_sheets": request.selected_sheets,
                     "include_unrealized": request.include_unrealized},
            available_sheets=available_sheets, source_metadata=source_metadata)

    monkeypatch.setattr(runtime, "build_ibkr_mobile_payload_context", build)
    monkeypatch.setattr(runtime, "_refresh_prices_from_cached_base", lambda context, **kw: context)
    runtime._clear_context_cache()
    try:
        prepared = warm_dashboard_snapshot()
        assert prepared["status"] == "succeeded"
        assert store.load(prepared["snapshot_id"]) is not None
        runtime._clear_context_cache()  # Browser is a separate process from the import job.
        timing = {}
        context = runtime.get_context(as_of=None, include_unrealized=True, selected_sheets=None,
            cache_bust=None, timing_recorder=lambda k, v: timing.__setitem__(k, v))
        assert context.base_state.value == 1
        assert timing["pipeline_snapshot_hit"] == 1
        assert builds == ["import-1"]
        marker.update(source_snapshot_id="import-2", import_run_id="run-2")
        again = warm_dashboard_snapshot()
        assert again["snapshot_id"] != prepared["snapshot_id"]
        assert builds == ["import-1", "import-2"]
    finally:
        runtime._clear_context_cache()


def test_preparation_does_not_report_success_when_snapshot_write_failed(monkeypatch):
    monkeypatch.setenv("OPTIONS_DATA_SOURCE", "ibkr")
    monkeypatch.setattr(runtime, "get_context", lambda **kw: SimpleNamespace(
        source_metadata={"source_snapshot_id": "import-1"},
        request={"as_of": date(2026, 9, 18), "selected_sheets": ["IBKR Flex"]}))
    monkeypatch.setattr(runtime, "get_default_pipeline_snapshot_store", MemoryPipelineSnapshotStore)
    with pytest.raises(RuntimeError, match="not persisted"):
        warm_dashboard_snapshot()


@pytest.mark.parametrize("status", ["succeeded", "succeeded_with_deferred"])
def test_import_prepares_only_after_import_completes(monkeypatch, status):
    order = []
    monkeypatch.setenv("IBKR_IMPORT_WARM_DASHBOARD", "1")
    monkeypatch.setattr("sys.argv", ["import_job"])
    monkeypatch.setattr(import_job, "run_import", lambda args: order.append("import") or {"status": status})
    monkeypatch.setattr(import_job, "warm_dashboard_snapshot", lambda: order.append("warm") or {"status": "succeeded"})
    assert import_job.main() == 0
    assert order == ["import", "warm"]


def test_import_failure_does_not_prepare_partial_import(monkeypatch):
    monkeypatch.setattr("sys.argv", ["import_job", "--warm-dashboard"])
    def failed(args):
        raise RuntimeError("import failed")
    monkeypatch.setattr(import_job, "run_import", failed)
    monkeypatch.setattr(import_job, "warm_dashboard_snapshot", lambda: pytest.fail("must not prepare"))
    with pytest.raises(RuntimeError, match="import failed"):
        import_job.main()


def test_warm_only_retries_without_contacting_ibkr(monkeypatch):
    monkeypatch.setattr("sys.argv", ["import_job", "--warm-only"])
    monkeypatch.setattr(import_job, "run_import", lambda args: pytest.fail("must not import"))
    monkeypatch.setattr(import_job, "warm_dashboard_snapshot", lambda: {"status": "succeeded"})
    assert import_job.main() == 0


def test_preparation_failure_keeps_import_status_and_signals_job_failure(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["import_job", "--warm-dashboard"])
    monkeypatch.setattr(import_job, "run_import", lambda args: {"status": "succeeded"})
    def failed():
        raise RuntimeError("snapshot storage unavailable")
    monkeypatch.setattr(import_job, "warm_dashboard_snapshot", failed)
    assert import_job.main() == 1
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["severity"] == "ERROR"
    assert lines[-1]["status"] == "succeeded"
    assert lines[-1]["dashboard_warmup"]["status"] == "failed"


def test_preparation_is_opt_in_outside_scheduled_job(monkeypatch):
    monkeypatch.delenv("IBKR_IMPORT_WARM_DASHBOARD", raising=False)
    monkeypatch.setattr("sys.argv", ["import_job"])
    monkeypatch.setattr(import_job, "run_import", lambda args: {"status": "succeeded"})
    monkeypatch.setattr(import_job, "warm_dashboard_snapshot", lambda: pytest.fail("must not prepare"))
    assert import_job.main() == 0
