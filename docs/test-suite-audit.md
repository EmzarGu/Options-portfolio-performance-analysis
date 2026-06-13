# Test Suite Audit

Last reviewed: 2026-06-13

## Test Runner

- Command: `make test`
- Underlying command: `.venv/bin/python -m pytest -q`
- Current suite size: 415 collected tests
- Line coverage tooling is not currently configured. Coverage is assessed by
  component-level tests, fixture contract tests, route tests, and explicit
  accounting scenario tests.

## Component Coverage Map

| Component | Primary tests |
| --- | --- |
| Core accounting, capital, unrealized, charts, serializers | `tests/test_pnl.py` |
| IBKR option/stock/dividend accounting cases | `tests/test_ibkr_accounting_cases.py` |
| IBKR import, Flex parsing, dedupe, backfill planning | `tests/test_ibkr_flex_import.py` |
| Mobile DTO contracts and fixtures | `tests/test_mobile_payloads.py` |
| Mobile route, refresh, caching, persisted context behavior | `tests/test_mobile_api_routes.py` |
| Mobile service context builders | `tests/test_mobile_api_service.py` |
| Web dashboard routes, auth, lazy loads, settings, monthly UI shape | `tests/test_web_dashboard.py` |
| Decision Lab analytics and candidate behavior | `tests/test_decision_lab.py` |
| Option-market provider/store/validation/history path | `tests/test_option_market_validation.py` |
| Data source caching and price/dividend providers | `tests/test_data_sources.py`, `tests/test_price_history_store.py`, `tests/test_dividend_history_store.py` |
| Firestore snapshot stores | `tests/test_pipeline_snapshot_store.py` |
| App settings / monthly target band | `tests/test_app_settings.py` |
| Issue classification | `tests/test_issue_classification.py` |
| Assignment quality | `tests/test_assignment_quality.py` |
| Cloud/GCP helpers and Cloud Run jobs | `tests/test_gcp.py`, `tests/test_cloud_run_jobs.py` |
| Streamlit source-mode compatibility | `tests/test_streamlit_source_mode.py` |

## Audit Findings

- Duplicate structural test bodies: none found by AST body hash.
- Deprecated mobile monthly fields are covered by negative assertions in
  `tests/test_mobile_payloads.py`; these are intentional regression tests, not
  stale legacy-contract tests.
- `portfolio_backend/decision_lab_candidates.py` is covered through
  `tests/test_decision_lab.py`, which exercises provider-backed candidate
  generation and rejection cases.
- `portfolio_backend/decision_lab_templates.py` and
  `portfolio_backend/web_dashboard_payloads.py` are covered through web route
  and rendered-template tests rather than direct unit tests.
- The slowest test is currently
  `tests/test_pnl.py::test_benchmark_table_formatting_renders_unavailable_sortino_as_na`.
  It is not a duplicate, but it should be monitored if suite runtime grows.

## Recent Cleanup

- Renamed the misleading pipeline-state test from a legacy-key name to a
  mapping-compatible contract name.
- Added direct issue-classification tests so classification rules are not
  protected only through mobile payload formatting.
- Added direct IBKR mobile context-builder coverage for metadata, cache-bust,
  price-overlay, and timing-recorder wiring.
