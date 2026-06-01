# System Architecture

Last reviewed: 2026-05-30

This repository owns the Python backend, Streamlit fallback dashboard, FastAPI
mobile API, FastAPI web dashboard, Cloud Run deployment assets, Firestore-backed
stores, and operational scripts. The iOS app is a separate canonical checkout:

```text
/Users/emzar/Documents/Codex Projects/Codex Investment Workflows/Option Analysis App/ios/OptionsMonitor-iOS
```

## Runtime Surfaces

| Surface | Entry point | Purpose |
| --- | --- | --- |
| Mobile API | `mobile_api.py` | FastAPI JSON API consumed by the iOS app. |
| Web dashboard | `web_dashboard.py` | Browser dashboard backed by the same mobile DTO builders. |
| Streamlit fallback | `streamlit_app.py` | Local/legacy dashboard and Google Sheets workflow. |
| IBKR import job | `portfolio_backend/ibkr/import_job.py` | Pulls IBKR Flex data and persists normalized raw/transaction records. |
| Cloud Run job trigger | `portfolio_backend/cloud_run_jobs.py` | Starts the production IBKR import Cloud Run Job from API/web actions. |

The production data path is IBKR-first:

1. IBKR Flex import job fetches statements.
2. Importer normalizes and persists raw rows, transactions, and import markers.
3. Mobile/web requests build a `PipelineState` through `portfolio_backend.ibkr`.
4. Live prices are overlaid on the base state.
5. `portfolio_backend.mobile_payloads` builds stable DTOs for mobile and web.

The Google Sheets path remains for local and legacy workflows, but new product
surfaces should prefer the IBKR path unless there is a deliberate compatibility
requirement.

## Shared Ownership Boundaries

- `portfolio_backend/calculations.py`, `performance.py`, and `ibkr/performance.py`
  own accounting and return calculations.
- `portfolio_backend/pipeline.py` and `ibkr/pipeline.py` own state construction.
- `portfolio_backend/mobile_payloads.py` owns the mobile DTO contract. It should
  not import FastAPI, Streamlit, or iOS-specific code.
- `portfolio_backend/mobile_api_service.py` bridges pipeline state to DTO builders.
- `mobile_api.py` owns HTTP routing, request validation, cache/snapshot lookup,
  refresh orchestration, and audit recording.
- `web_dashboard.py` owns auth, web routes, and HTML responses.
- `portfolio_backend/web_dashboard_payloads.py` composes web payloads from the
  same mobile DTO builders to avoid duplicate business logic.

## Persistence

Firestore collections used by the current cloud path:

| Store | Module | Collections |
| --- | --- | --- |
| Pipeline snapshots | `pipeline_snapshot_store.py` | `pipeline_snapshots`, `app_metadata` |
| Audit/source snapshots | `audit_store.py` | `refresh_runs`, `source_snapshots` |
| Price history | `price_history_store.py` | `price_history_chunks` |
| Dividend history | `dividend_history_store.py` | `dividend_history` |
| Option market data | `option_market/store.py` | provider-specific option market documents |

Store factories default to Firestore when the runtime indicates Cloud Run or a
Firestore project is configured. Local tests can force memory/disabled stores
through the corresponding `*_STORE` environment variable.

## Performance Notes

- Read endpoints reuse request contexts where possible. IBKR mode also uses
  persisted pipeline snapshots so price-only refreshes avoid rebuilding the full
  accounting pipeline.
- Mobile and web payloads share DTO builders. Adding endpoint fields should
  happen in `portfolio_backend/mobile_payloads.py` first, then be surfaced by
  web/iOS as needed.
- Active-cycle projection metrics are centralized in
  `portfolio_backend/cycle_projection.py`; web, Decision Lab, and mobile payloads
  should consume that canonical output instead of recalculating cycle P&L.
- Dashboard routes should avoid importing Streamlit-only helpers unless they are
  already isolated behind service/dependency functions.

## Verification

Backend verification:

```bash
make test
```

iOS verification:

```bash
cd "/Users/emzar/Documents/Codex Projects/Codex Investment Workflows/Option Analysis App/ios/OptionsMonitor-iOS"
xcodebuild test \
  -project OptionsMonitor.xcodeproj \
  -scheme OptionsMonitor \
  -destination 'platform=iOS Simulator,name=iPhone 17' \
  CODE_SIGNING_ALLOWED=NO
```

Mobile API local smoke:

```bash
.venv/bin/uvicorn mobile_api:app --host 127.0.0.1 --port 8700
.venv/bin/python scripts/mobile_api_smoke.py --base-url http://127.0.0.1:8700
```

## Maintenance Rules

- Keep accounting logic out of route files and UI files.
- Keep DTO construction deterministic; SwiftUI list rows require stable IDs.
- Add cloud configuration to docs when introducing a new environment variable,
  secret, collection, job, or service.
- Preserve the IBKR and Google Sheets data-source split until Google Sheets is
  intentionally retired from the Streamlit/local workflow.
