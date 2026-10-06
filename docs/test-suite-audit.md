# Test Suite Audit

Refactor and gaps reviewed: 2026-09-19. Earlier structural audit: 2026-06-13.

## Test Runner

- Command: `make test`
- Underlying command: `.venv/bin/python -m pytest -q`
- Pre-refactor baseline: 448 tests passed in 3.55 seconds on 2026-09-19,
  using Python 3.12.13 and `.venv/bin/python -m pytest -q --durations=12`.
  This includes existing uncommitted tests in the working checkout.
- After refactoring: 457 tests pass, including nine new regression cases for
  independent web startup, concurrent web/mobile context reuse, financial payload
  parity, table serialization/immutability, and the existing visual prototype's
  data-service integration. These tests run offline with controlled data access.
- `make test` was attempted first but the macOS wrapper required Xcode licence
  acceptance. The equivalent virtual-environment command above ran successfully;
  no licence or system setting was changed.
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
| Shared web/mobile runtime, financial payload parity, prototype integration and table records | `tests/test_shared_dashboard_runtime.py` |
| Decision Lab analytics and candidate behavior | `tests/test_decision_lab.py` |
| Option-market provider/store/validation/history path | `tests/test_option_market_validation.py` |
| Data source caching and price/dividend providers | `tests/test_data_sources.py`, `tests/test_price_history_store.py`, `tests/test_dividend_history_store.py` |
| Firestore snapshot stores | `tests/test_pipeline_snapshot_store.py` |
| App settings / monthly target band | `tests/test_app_settings.py` |
| Issue classification | `tests/test_issue_classification.py` |
| Assignment quality | `tests/test_assignment_quality.py` |
| Cloud/GCP helpers and Cloud Run jobs | `tests/test_gcp.py`, `tests/test_cloud_run_jobs.py` |
| Streamlit source-mode compatibility | `tests/test_streamlit_source_mode.py` |
| Assigned-holdings review payload | `tests/test_assigned_holdings_review.py` |
| Local visual prototype | `tests/test_visual_prototype.py` |

## September verification limits

Both complete OpenAPI documents match the saved pre-refactor versions. An AST
comparison of all 152 original web/mobile top-level functions found no logic
differences after normalizing module qualification, two runtime function renames
and docstrings. Existing route tests retain their assertions while patching the
new module owners. These checks protect the refactor; they do not validate live
provider access, deployment or every possible concurrent cache invalidation.

The suite passes but did not prevent three errors reproduced separately: partial
provider refreshes overwriting fallback contracts, derived-cache readers seeing new
metadata with old data, and zero targets sharing the default cache key. See
[E1–E3 and proposed regression cases](refactoring-review-2026-09-19.md).
Their fixes have not been approved or implemented.

The [subsequent authorized deployment](production-deployment-2026-09-19.md) ran
all 457 tests successfully in the production Python 3.11.15 container in Cloud Build
(17.71 seconds, one warning). Docker is not installed locally. The release retained
the previous web image's dependency layer. Dependency declarations remain unpinned.
The permanent `cloudbuild.yaml` has no test step, and both `.gcloudignore` and
`.dockerignore` exclude tests. Adding a test gate also requires test-source
packaging; simply running `pytest` in the existing image cannot exercise the
excluded suite. The one-off release supplied tests and the required backfill helper
to a network-disabled test container before deployment; that gate has not been
added to the permanent build configuration.

The slowest test in the pre-refactor baseline run was
`tests/test_mobile_api_routes.py::test_ibkr_import_health_collapses_duplicate_trailing_incomplete_statement`
at 0.36 seconds. Timings vary and are not production benchmarks.

## June structural audit findings

- Duplicate structural test bodies: none found by AST body hash.
- Deprecated mobile monthly fields are covered by negative assertions in
  `tests/test_mobile_payloads.py`; these are intentional regression tests, not
  stale legacy-contract tests.
- `portfolio_backend/decision_lab_candidates.py` is covered through
  `tests/test_decision_lab.py`, which exercises provider-backed candidate
  generation and rejection cases.
- `portfolio_backend/decision_lab_templates.py` and
  `portfolio_backend/web_dashboard_payloads.py` are covered through web route
  and rendered-template tests. September adds direct payload parity and frame
  serialization tests for `web_dashboard_payloads.py`.
- The slowest test in the June audit was
  `tests/test_pnl.py::test_benchmark_table_formatting_renders_unavailable_sortino_as_na`.
  It is not a duplicate, but it should be monitored if suite runtime grows.

## June cleanup history

- Renamed the misleading pipeline-state test from a legacy-key name to a
  mapping-compatible contract name.
- Added direct issue-classification tests so classification rules are not
  protected only through mobile payload formatting.
- Added direct IBKR mobile context-builder coverage for metadata, cache-bust,
  price-overlay, and timing-recorder wiring.
