# Options ROI Dashboard

IBKR portfolio accounting and options research with a shared Python backend, a
web dashboard and a mobile API. Streamlit is a separately installed backup UI.
Financial rules live in the backend; completed option chains are recognized only
when the corresponding strategy quantity ends.

## Reproducible local setup

Python 3.11 is the supported runtime; the production base pins Python 3.11.15 and
its image digest. The complete dependency sets are pinned to the verified
production versions, with test and backup-interface dependencies separated.

```sh
python3.11 -m venv .venv311
.venv311/bin/python -m pip install --no-deps -r requirements-dev.txt
.venv311/bin/python -m pip check
make test
```

Production installs only `requirements.txt`; the backup UI adds
`requirements-streamlit.txt`. To update dependencies, resolve in an isolated
Python 3.11 environment, pin every resulting transitive dependency, run `pip
check`, and pass the container test and runtime smoke stages. Do not regenerate
production locks from an unrelated personal virtual environment.

| Interface | Command |
| --- | --- |
| Web | `.venv311/bin/python -m uvicorn web_dashboard:app --host 127.0.0.1 --port 8800` |
| Mobile API | `.venv311/bin/python -m uvicorn mobile_api:app --host 127.0.0.1 --port 8700` |
| Streamlit backup | `.venv311/bin/python -m streamlit run streamlit_app.py --server.address 127.0.0.1` |

Starting a server does not configure its sources. See the [web runbook](docs/cloud-run-web-dashboard.md)
and [mobile runbook](docs/mobile-api-local-runbook.md). Keep credentials outside Git.

## Authentication

- Mobile requires `MOBILE_API_KEY`. An explicitly local test server may set
  `ALLOW_INSECURE_LOCAL_AUTH=1`; this has no effect on Cloud Run.
- Production web requires Google sign-in with a configured client ID and email
  allowlist. Password login and existing password sessions are rejected on Cloud
  Run, even if a password or the fallback-visibility flag remains configured.
  Password login is available only in local development.
- `WEB_DASHBOARD_COOKIE_SECRET` must contain at least 32 characters after trimming.
  Generate it from at least 32 random bytes; length validation alone does not
  establish randomness. It must be independent of any local dashboard password.
- `WEB_DASHBOARD_AUTH=0` is for local development only. Cloud startup rejects it.
- Sessions and OAuth state use separate expiring signatures. Legacy cookies are
  rejected, so the security upgrade requires a one-time login.
- `/health` and `/v1/mobile/health` are public reachability probes; they do not
  fetch portfolio data or call market-data providers.
- Swagger, ReDoc and OpenAPI HTTP endpoints are disabled for both services.

## Deployment and rollback

The GitHub main-branch trigger runs `cloudbuild.yaml`. It builds a test image
from the locked runtime layer, runs the full regression suite and focused lint,
then independently checks the production image. Production contains neither
Streamlit nor pytest and runs as UID 10001. A failing stage prevents deployment.

`scripts/deploy_verified.py` resolves the tested image digest, stages both web
and mobile without moving traffic, checks health/login/access denial, verifies
that production password login and documentation are unavailable, promotes
verified revisions, and updates the IBKR and historical import jobs to that
same digest. Commands and HTTP probes are explicit. Existing source/provider,
scaling and schedule settings are preserved. No job is executed by deployment.
Partial promotion failures trigger restoration of previous service traffic and
job images. Failed rollback is reported as a build failure, never success.
Full rollback configuration is written to a unique `tmp/release-*/release-rollback.json`
with directory mode 0700 and file mode 0600. It is excluded from Git and image
builds. Copy it only to private recovery storage if it must survive a build worker.

The web secret must exist as Secret Manager `options-roi-web-cookie-secret`
version 1, accessible to the web runtime identity. Rotation requires adding a
version, updating the deployment reference, testing and redeploying. Mobile-key
rotation must be coordinated with the iOS client before retiring the old key.
Record the services and client versions affected, schedule the change, update the
Secret Manager binding and iOS configuration together, then verify an authenticated
read and rejection of the retired key. Keep the previous version available for
rollback until verification passes; never put key values in Git or build logs.

After release, check authenticated web/mobile payloads, both Decision Lab views,
target image digests, and import-job health. Use saved revisions and immutable
image digests for manual rollback; preserve the last known-good images in
Artifact Registry. Build logs contain the source commit and final image digest.

## Maintenance

- [Architecture](docs/architecture.md)
- [Accounting rules](docs/ibkr-accounting-rules.md) and [scenario matrix](docs/ibkr-accounting-test-matrix.md)
- [Mobile API contract](docs/mobile-api-contract.md)
- [Hardening design and validation](docs/production-hardening-2026-10-06.md)
- [Operations history](docs/cloud-operations-current-state.md)
- [Option data and provider rules](docs/option-market-validation.md)

`portfolio_backend/data_runtime.py` owns UI-independent source/provider adapters;
`context_runtime.py` coordinates shared snapshots and refresh. Store clients may
remain process-local; Firestore source identities and build leases coordinate
instances. Derived payloads publish complete content-addressed generations, with
7-day TTL on `payload_chunks.expires_at` to reclaim old chunks. A missing or corrupt
cache entry rebuilds from source. Provider failures retain the existing snapshot
behavior, and daily-price fallbacks are explicitly reported as unavailable live
quotes.
Missing benchmarks appear in the shared issues list. Invalid stored option
contracts produce a count in Decision Lab coverage warnings; snapshot-store
initialization and lease-release failures emit sanitized operational warnings.

Raw exports, personal research and full private reconciliation evidence remain
local and are excluded from Git and build uploads. Public regression fixtures
use synthetic identifiers. The historical notebook export is in `archive/` and
is excluded from deployment.
