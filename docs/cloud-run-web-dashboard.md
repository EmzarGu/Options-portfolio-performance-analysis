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

`MOBILE_API_KEY` is also used as the fallback browser password and cookie-signing secret. `WEB_DASHBOARD_PASSWORD` may be added if the browser dashboard should use a password distinct from the mobile API key.

## Browser Authentication

The dashboard supports Google Sign-In plus an API-key fallback. Sessions are signed with an HTTP-only cookie and default to 90 days.

Google Sign-In environment variables:

```text
WEB_GOOGLE_CLIENT_ID=<web-oauth-client-id>.apps.googleusercontent.com
WEB_AUTH_ALLOWED_EMAILS=<allowed-google-email-1>,<allowed-google-email-2>
WEB_SESSION_DAYS=90
```

The Google OAuth client should be configured with these authorized JavaScript origins:

```text
https://options-roi-web-595990983720.europe-west6.run.app
https://options-roi-web-htdlrf6zjq-oa.a.run.app
```

If the OAuth setup asks for redirect/login URIs, add:

```text
https://options-roi-web-595990983720.europe-west6.run.app/auth/google
https://options-roi-web-htdlrf6zjq-oa.a.run.app/auth/google
```

Do not remove `MOBILE_API_KEY` unless `WEB_DASHBOARD_COOKIE_SECRET` is configured; Google client IDs are public and must not be used as cookie secrets.

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
