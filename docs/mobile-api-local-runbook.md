# Mobile API Local Runbook

This runbook is for backend and iOS development against the local FastAPI mobile
API.

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
in `refresh.reload_endpoints`. The server makes the refreshed context active for
matching read calls, so the client does not need to append `cache_bust` during
normal app use.

Read endpoints reuse an in-memory request context for matching `as_of`,
`include_unrealized`, selected sheets, and active refresh token. This keeps a
multi-screen mobile load from rebuilding the same portfolio repeatedly.

## Smoke Test

With uvicorn running:

```bash
.venv/bin/python scripts/mobile_api_smoke.py --base-url http://127.0.0.1:8700
```

The smoke script checks health, all mobile read endpoints,
`POST /v1/mobile/refresh`, and the expected validation error envelopes.
