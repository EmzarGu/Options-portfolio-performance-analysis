# Options ROI Dashboard

Python application for IBKR portfolio accounting, options-cycle analysis, and
Decision Lab research. Web and mobile share backend calculations; Streamlit remains
a backup interface. This repository does not contain the iOS app.

## Entry points

Run from the repository root with the existing virtual environment:

| Interface | Local launch |
| --- | --- |
| Web dashboard | `.venv/bin/python -m uvicorn web_dashboard:app --host 127.0.0.1 --port 8800` |
| Mobile API | `.venv/bin/python -m uvicorn mobile_api:app --host 127.0.0.1 --port 8700` |
| Streamlit backup | `.venv/bin/python -m streamlit run streamlit_app.py --server.address 127.0.0.1` |

Dependencies are declared in `requirements.txt` and currently unpinned. Local
`.python-version` selects Python 3.12; the Dockerfile uses Python 3.11. Runtime
alignment and dependency pinning are pending work, not a verified reproducible setup.

Starting a server does not configure its data access. The web backend defaults to
IBKR/Firestore. Follow the source and credential configuration in the
[web runbook](docs/cloud-run-web-dashboard.md) and
[mobile runbook](docs/mobile-api-local-runbook.md). Browser authentication needs the
documented password or Google configuration. Keep secrets outside version control.

Web `/health` and mobile `/v1/mobile/health` are inexpensive reachability checks.
Portfolio reads can access external services and populate caches. Refresh/import
endpoints do additional work; the mobile smoke script includes a refresh.

## Tests

Run `make test`, which uses `.venv/bin/python -m pytest -q`. On 2026-09-19,
457 tests passed under Python 3.12.13 after the refactor. If Apple's `make` wrapper refuses to start
because the Xcode licence is unaccepted, the equivalent command is:

```bash
.venv/bin/python -m pytest -q
```

See the [test audit](docs/test-suite-audit.md) for scope and known gaps. A passing
local suite does not verify live provider access or the production image.

The [23 September Decision Lab release](docs/decision-lab-marketdata-production-2026-09-23.md)
extends verification to 536 tests and adds the production Market Data integration.

## Design and maintenance

- [Architecture and calculation ownership](docs/architecture.md)
- [Accounting rules](docs/ibkr-accounting-rules.md) and [scenario matrix](docs/ibkr-accounting-test-matrix.md)
- [Mobile API contract](docs/mobile-api-contract.md)
- [Web setup and deployment](docs/cloud-run-web-dashboard.md)
- [Cloud operations and dated live verification](docs/cloud-operations-current-state.md)
- [Option data storage, validation, and provider status](docs/option-market-validation.md)
- [Completed refactoring and verification](docs/refactoring-results-2026-09-19.md)
- [Original review, deferred repairs and provider alternatives](docs/refactoring-review-2026-09-19.md)
- [Decision Lab data requirements and delayed-data alternatives](docs/decision-lab-data-options-2026-09-19.md)
- [Market Data free-account live pilot and remaining validation](docs/decision-lab-marketdata-free-pilot-2026-09-23.md)
- [Market Data production setup, limits and release verification](docs/decision-lab-marketdata-production-2026-09-23.md)
- [Local Market Data integration and broader portfolio testing](docs/decision-lab-marketdata-integration-2026-09-23.md)
- [Decision Lab timeout repair and production verification](docs/decision-lab-timeout-repair-2026-09-19.md)
- [Assignment Quality Lab performance redesign](docs/assignment-quality-performance-2026-09-19.md)
- [Morning refresh optimization and background preparation](docs/morning-refresh-performance-2026-09-20.md)

Financial calculations belong in the backend. Web and mobile now share context
loading and refresh through `portfolio_backend/context_runtime.py`; web auth and
payload caching have separate modules. The local refactor preserves API contracts,
accounting rules, UI and provider behavior. It was
[deployed on 2026-09-19](docs/production-deployment-2026-09-19.md), after all 457
tests also passed in the retained production Python 3.11.15 image.

The September review identified three reproducible
data/cache errors and a historical-import deployment failure. Their fixes and
a replacement provider still require agreement.

`cloudbuild.yaml` builds one image and updates the web, mobile, and IBKR import
targets sequentially. It has no test gate and omits the separate historical options
job. A successful build or one healthy service does not prove all targets run the
same revision. Verify each target after an authorized deployment.
