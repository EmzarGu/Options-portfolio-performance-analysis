# Mobile API Local Runbook

This runbook is for backend and iOS development against the local FastAPI mobile
API.

Refresh semantics checked against local source on 2026-09-19.

## Install

```bash
.venv/bin/python -m pip install -r requirements.txt
```

## Run FastAPI

```bash
.venv/bin/uvicorn mobile_api:app --host 127.0.0.1 --port 8700
```

Local base URL:

```text
http://127.0.0.1:8700
```

For an iOS simulator running on the same Mac, use the same host and port. For a
physical device, expose the Mac on the local network and use the Mac LAN IP.

## Common Query Parameters

For IBKR/Firestore, configure `OPTIONS_DATA_SOURCE=ibkr`,
`IBKR_REPORT_SOURCE=firestore`, the project/query IDs, and authorized Firestore
credentials as described in the [web runbook](cloud-run-web-dashboard.md).
The sheet-selection examples below apply to the legacy Sheets path. When
`MOBILE_API_KEY` is configured, protected endpoints require an `x-api-key` header
or a bearer token; health remains public.

Read endpoints and refresh accept the same common query parameters:

- `as_of=YYYY-MM-DD`
- `include_unrealized=1` or `include_unrealized=0`
- repeated `selected_sheets`, for example:

```text
selected_sheets=Options%202024&selected_sheets=Options%202025&selected_sheets=Options%202026
```

## Example Requests

```bash
BASE='http://127.0.0.1:8700'
QS='include_unrealized=1&selected_sheets=Options%202024&selected_sheets=Options%202025&selected_sheets=Options%202026'

curl "$BASE/v1/mobile/health"
curl "$BASE/v1/mobile/config"
curl "$BASE/v1/mobile/dashboard?$QS"
curl "$BASE/v1/mobile/positions?$QS"
curl "$BASE/v1/mobile/open-option-shorts?$QS"
curl -X POST "$BASE/v1/mobile/refresh?$QS"
```

Use `GET /v1/mobile/health` for a cheap server-reachable check. It does not read
prefs, Sheets, prices, or pipeline data. Use `GET /v1/mobile/config` as the
first functional backend/config check.

After refresh succeeds, the iOS client should reload the read endpoints listed
in `refresh.reload_endpoints`. In IBKR/Firestore mode the server restores a matching
persisted **base accounting snapshot** and refreshes its price overlay. If no valid
base exists, it rebuilds and stores that base. It does not persist the full
price-refreshed context for reuse across instances. The current process remembers
the refreshed context and cache-bust value; normal clients do not need to append
`cache_bust`. Read calls check source markers before reusing in-memory contexts.

## Smoke Test

With uvicorn running:

```bash
.venv/bin/python scripts/mobile_api_smoke.py --base-url http://127.0.0.1:8700
```

The smoke script checks health, all mobile read endpoints,
`POST /v1/mobile/refresh`, and the expected validation error envelopes.
