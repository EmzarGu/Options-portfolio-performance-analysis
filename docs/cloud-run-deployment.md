# Cloud Run Deployment

This project exposes the mobile backend through `mobile_api:app`, a FastAPI app.
Cloud Run is the simplest Google Cloud target for the iOS app API because it
runs containers, scales to zero, and gives a stable HTTPS endpoint.

## Service

- Service name: `options-roi-mobile-api`
- Region: `europe-west6` for Zurich, or `europe-west1` if you prefer the default
  Western Europe region.
- Container port: `8080`
- Startup command: provided by `Dockerfile`
- Health endpoint: `/v1/mobile/health`

## Required Runtime Secret

Set these as environment variables or Secret Manager secrets in Cloud Run:

- `GOOGLE_SERVICE_ACCOUNT_JSON`: raw service account JSON with read access to
  the Google Sheet used by `streamlit_app.SHEET_ID` when sheet/local mode is
  used.
- `MOBILE_API_KEY`: shared development API key required by all mobile endpoints
  except `/v1/mobile/health`.
- `IBKR_FLEX_TOKEN`: Secret Manager secret `ibkr-flex-token:latest`.
- `IBKR_FLEX_QUERY_ID`: Secret Manager secret `ibkr-flex-query-id:latest`.
- `IBKR_RAW_BUCKET`: `options-portfolio-ibkr-raw-595990983720`.

Production currently runs the mobile API in IBKR-backed mode:

```text
OPTIONS_DATA_SOURCE=ibkr
IBKR_REPORT_SOURCE=firestore
```

The service also uses Firestore Native mode in project
`options-performance-dashboard` for persistent yfinance historical price and
dividend history caches. Cloud Run should run with a service account that has
`roles/datastore.user`; no Firestore client credentials are sent to iOS.

Recommended setup:

1. Create a Secret Manager secret named `google-service-account-json`.
2. Store the raw service account JSON as the secret value.
3. Create a Secret Manager secret named `mobile-api-key`.
4. Store a long random value as the secret value.
5. In the Cloud Run service, mount the secrets as environment variables:
   `GOOGLE_SERVICE_ACCOUNT_JSON`.
   `MOBILE_API_KEY`.

The container intentionally excludes local files like `.streamlit/secrets.toml`,
`.streamlit_user_prefs.json`, and downloaded portfolio workbooks.

## Deploy From Google Cloud Console

1. Open Cloud Run in the Google Cloud Console.
2. Select project `options-performance-dashboard`.
3. Click **Create service** or **Deploy container**.
4. Choose **Continuously deploy from a repository** and connect
   `EmzarGu/Options-portfolio-performance-analysis`.
5. Select build type **Dockerfile** and keep the Dockerfile path as
   `Dockerfile`.
6. Set service name `options-roi-mobile-api`.
7. Select a region.
8. Under authentication, allow unauthenticated invocations only if
   `MOBILE_API_KEY` is set. Otherwise the iOS app will not be able to call the
   service without Google IAM credentials.
9. Add the `GOOGLE_SERVICE_ACCOUNT_JSON` and `MOBILE_API_KEY` environment
   variables from Secret Manager.
10. Deploy.

## Market Data Caches

The backend checks Firestore before fetching historical stock prices from
yfinance. Cached records are stored in `price_history_chunks` as ticker/year
documents such as `AAPL:2026`.

Dividend history uses the same pattern. Cached records are stored in
`dividend_history` as ticker documents such as `AAPL`.

Mobile refresh audit records are also written to Firestore on a best-effort
basis. Refresh runs are stored in `refresh_runs`; source metadata snapshots are
stored in `source_snapshots`.

Production portfolio transactions are currently sourced from IBKR Flex imports
persisted in Firestore. Google Sheets/local Excel mode remains available as a
fallback/development path, but it is not the production source for the Cloud Run
mobile API.

Runtime selection:

- Cloud Run: Firestore is selected automatically when Google Cloud project
  metadata is available.
- Local development: the cache is disabled by default unless
  `PRICE_HISTORY_STORE=memory` or `PRICE_HISTORY_STORE=firestore` is set.
- Tests: use the in-memory store.

Useful environment variables:

- `PRICE_HISTORY_STORE`: `auto`, `firestore`, `memory`, or `disabled`.
- `DIVIDEND_HISTORY_STORE`: `auto`, `firestore`, `memory`, or `disabled`.
- `AUDIT_STORE`: `auto`, `firestore`, `memory`, or `disabled`.
- `FIRESTORE_PROJECT_ID`: optional explicit project override.
- `FIRESTORE_DATABASE`: optional database override; default is `(default)`.

## Smoke Test

After deployment, replace `SERVICE_URL` with the Cloud Run URL:

```bash
curl "$SERVICE_URL/v1/mobile/health"
curl -H "X-API-Key: $MOBILE_API_KEY" "$SERVICE_URL/v1/mobile/config"
python scripts/mobile_api_smoke.py --base-url "$SERVICE_URL" --api-key "$MOBILE_API_KEY"
```

When `--api-key` is supplied, the smoke script also verifies that protected
endpoints reject missing and invalid keys with `401 unauthorized`.

The iOS app should use the Cloud Run URL as its API base URL.
