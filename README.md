# Options Portfolio Performance Analysis

Backend and dashboard system for options wheel portfolio performance, IBKR Flex
imports, Cloud Run services, Firestore-backed snapshots, and the JSON mobile API
used by the iOS Options Monitor app.

Start with:

- [System architecture](docs/system-architecture.md)
- [Mobile API contract](docs/mobile-api-contract.md)
- [Cloud Run deployment](docs/cloud-run-deployment.md)
- [Cloud Run web dashboard](docs/cloud-run-web-dashboard.md)
- [IBKR Cloud Run job](docs/ibkr-cloud-run-job.md)

Run backend tests:

```bash
make test
```

Run the local mobile API:

```bash
.venv/bin/uvicorn mobile_api:app --host 127.0.0.1 --port 8700
```
