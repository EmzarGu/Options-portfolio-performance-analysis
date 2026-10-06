# Production hardening — 6 October 2026

## Scope

Record the previously deployed but uncommitted app changes, then harden access,
release reproducibility and data availability without changing accounting rules,
provider selection, quotas, scheduling or the iOS API schema.

## Security

Mobile startup requires a key or an explicit local-only opt-out. Missing/blank
keys return 503 on protected requests; wrong keys return 401. Documentation and
OpenAPI endpoints are protected too. Public health remains lightweight.

Web startup on Cloud Run rejects disabled or incomplete authentication. Request
guards enforce this independently of startup, so a signed cookie cannot bypass
missing configuration. The cookie secret is dedicated; password/public fallbacks
are removed. ItsDangerous timed serializers use different salts for sessions and
OAuth state. Legacy sessions are invalidated. Expired and future-issued tokens,
tampering, removed Google allowlist entries and rotated secrets are rejected.

Secrets are provisioned before deploying guarded revisions. No secret values
are logged or committed. The existing Google service identity remains the
Firestore credential source. Legacy Sheets credentials remain only where still
configured; no credential is removed without checking its consumer.

## Source and image provenance

The supported environment is Python 3.11. Production pins Python 3.11.15 by image
digest and all dependencies by exact version, recovered from the prior working
image rather than guessed from the local Python 3.12 environment. Tests and
Streamlit dependencies are separate; the production smoke test verifies their
absence and checks all production entry-point imports as a non-root user.

Container tests have no network and an intentionally unavailable ADC credential
path, preventing accidental calls to production storage or market providers.

Cloud Build gates deployment on regression tests, focused lint and the exact
production-image smoke check. GitHub Actions repeats those checks for pull
requests. Both build contexts include tests but exclude credentials and private
research. Application source is copied explicitly into the production image.

Release first stages no-traffic revisions, then checks them, promotes them and
updates both existing jobs. Deployment records immutable digests and the full
source commit. Rollback retains prior service traffic and job images. Automatic
rollback is covered by failure-injection tests. A runtime probe never requests
market data or starts an import.

## Data and caches

Shared source/provider adapters no longer import Streamlit. The backup app uses
those adapters while retaining UI caches/preferences. Local workbook cache is
cleared by an explicit non-IBKR refresh.

The existing price-fetch API already returns error and coverage information.
Daily-close fallbacks now add a dated warning instead of resembling a current
quote. No field type or accounting total is changed by this warning. Provider
boundary catches remain where they protect availability; known frame-shape
errors use narrower catches. Existing tests cover missing coverage and provider
outages; new tests cover the visible fallback warning.

Derived payloads use schema 2. Content-addressed chunks are written first, then
one ready pointer is published. Readers verify the content digest and never
combine writers' chunks. Interrupted writes leave the prior pointer intact.
Schema-1 entries are rebuilt on read. Chunk TTL is seven days, using Firestore's
`payload_chunks.expires_at` field policy; expired cache data is rebuildable.

Existing source markers, persistent base snapshots and cross-process build
leases are retained. No Redis, sweeping singleton replacement, `Decimal`
conversion or financial-formula migration is included.

## Verification and release record

The suite adds explicit security misconfiguration, legacy cookie, expiry,
future timestamp, secret rotation, OAuth/session separation, cache interleaving,
interrupted publication and deployment rollback cases. Existing accounting,
web/mobile parity, provider outage and import tests remain required.

The final Cloud Build log is the authoritative container test/deployment record.
Local `tmp/hardening-release/` stores rollback configurations, the dependency
inventory and authenticated production verification without publishing portfolio
values. Check the release commit and digest against both services and both jobs.
