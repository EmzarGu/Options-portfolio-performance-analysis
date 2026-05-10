# Cloud Run Web Dashboard

The production web dashboard is a separate FastAPI entrypoint from the Streamlit backup app.

## Entrypoint

```bash
uvicorn web_dashboard:app --host 0.0.0.0 --port ${PORT:-8080}
```

The service reuses the same IBKR backend context and mobile DTO builders as the iOS API. Streamlit Cloud remains the Google Sheets backup/control dashboard.

## Required Runtime Config

Environment variables:

```text
OPTIONS_DATA_SOURCE=ibkr
IBKR_REPORT_SOURCE=firestore
FIRESTORE_PROJECT_ID=options-performance-dashboard
IBKR_FLEX_QUERY_ID=1503002
WEB_DASHBOARD_AUTH=1
```

Secrets:

```text
MOBILE_API_KEY=mobile-api-key:latest
```

`WEB_DASHBOARD_PASSWORD` may be added later if the browser dashboard should use a password distinct from the mobile API key.

## Smoke Test

```bash
python scripts/web_dashboard_smoke.py \
  --base-url https://<cloud-run-url> \
  --password-file /Users/emzar/.options_roi_mobile_api_key \
  --timeout 120
```

Expected result:

```text
web_dashboard_smoke ok source=ibkr_flex rows=<n> actionable_issues=0
```

## Rollback

The web dashboard is deployed as its own Cloud Run service. Rolling it back does not affect:

- Streamlit Cloud sheet backup
- `options-roi-mobile-api`
- iOS app configuration
