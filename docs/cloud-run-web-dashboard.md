# Cloud Run Web Dashboard

The production web dashboard is a separate FastAPI entrypoint from the Streamlit
backup app.

## Entrypoint

```bash
uvicorn web_dashboard:app --host 0.0.0.0 --port ${PORT:-8080}
```

The service reuses the same IBKR backend context and mobile DTO builders as the
iOS API. Streamlit remains a backup/testing surface while the browser dashboard
is the production web UI.

## Required Runtime Config

Environment variables:

```text
OPTIONS_DATA_SOURCE=ibkr
IBKR_REPORT_SOURCE=firestore
FIRESTORE_PROJECT_ID=options-performance-dashboard
IBKR_FLEX_QUERY_ID=1504277
WEB_DASHBOARD_AUTH=1
```

Secrets:

```text
WEB_DASHBOARD_PASSWORD=web-dashboard-password:latest
```

The browser dashboard uses a dedicated dashboard password. The mobile API key is not shown to users and is not accepted as the browser login password.

## Browser Authentication

The dashboard supports Google Sign-In plus a dashboard-password fallback. Sessions are signed with an HTTP-only cookie and default to 90 days.

Google Sign-In environment variables:

```text
WEB_GOOGLE_CLIENT_ID=<web-oauth-client-id>.apps.googleusercontent.com
WEB_AUTH_ALLOWED_EMAILS=<allowed-google-email-1>,<allowed-google-email-2>
WEB_SESSION_DAYS=90
```

When Google Sign-In is configured, the password fallback is hidden by default.
Set `WEB_PASSWORD_FALLBACK_VISIBLE=1` only for emergency troubleshooting.

The login page uses a first-party redirect link to `/auth/google/start`, which
then redirects to Google OAuth. This avoids embedded-browser iframe issues with
the Google Identity Services button while preserving the same OAuth client and
backend allowlist checks.

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

Do not remove `WEB_DASHBOARD_PASSWORD` unless `WEB_DASHBOARD_COOKIE_SECRET` is configured; Google client IDs are public and must not be used as cookie secrets.

## Smoke Test

```bash
python scripts/web_dashboard_smoke.py \
  --base-url https://<cloud-run-url> \
  --password-file /Users/emzar/.options_roi_web_password \
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
