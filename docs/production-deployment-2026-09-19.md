# Production deployment — 2026-09-19

Deployment was authorized after the local refactor and completed at 19:00:40 UTC
(21:00 Zurich). The user will perform authenticated portfolio testing in production.

## Released version

- Source snapshot: `refactor-20260919-185738-31cc83b8`.
- [Cloud Build record](https://console.cloud.google.com/cloud-build/builds/fed75f0d-40e4-484a-b5e6-538f96245de2?project=595990983720): **SUCCESS**.
- Image digest: `sha256:0be641aa3438dd40ad584fd4eaf17c6bfeeeb3670375083d73f33038244343cd`.
- Web revision: `options-roi-web-00119-wnr`, Ready and receiving 100% of traffic.
- Mobile revision: `options-roi-mobile-api-00177-kqd`, Ready and receiving 100% of traffic.
- Both service revisions resolve to the same tested image digest.
- `ibkr-flex-import` was updated to the same image tag. No import execution was
  triggered as part of deployment. The historical option-market job was not changed.

The release contains the tested working-copy application, including pre-existing
assigned-holdings code. It is identified by a source-snapshot label, not falsely
attributed to the unchanged Git HEAD. No Git commit or push was made. Local
credentials, research reports, review files and unrelated analysis were excluded
from the staged upload.

## Build and verification

The release used the established web/mobile/import-job sequence with a temporary
test gate before image publication and deployment. Tests and their backfill helper
were supplied to the test container as read-only mounts and excluded from the
application image by the existing Docker ignore rules.

To preserve the production runtime and dependency set, the final release layered
the application onto the previous web image at immutable digest
`sha256:544bfb25891c3a2e844ddfa94eaa2255572a550fe74a9e6d7f82ccf02de0a827`.
The repository Dockerfile, dependency declarations and permanent Cloud Build
configuration were not changed. This was a release-specific build configuration;
future normal builds still need the separately planned dependency/test-gate work.

- Python in the released container: **3.11.15**.
- Container suite: **457 passed, 1 warning in 17.71 seconds**, with network disabled.
- Web `/health`: HTTP 200, status `ok`.
- Web `/login`: HTTP 200 with login content.
- Unauthenticated web `/`: HTTP 303 to `/login`.
- Unauthenticated web `/api/dashboard`: HTTP 401.
- Mobile `/v1/mobile/health`: HTTP 200, status `ok`.
- Unauthenticated mobile `/v1/mobile/dashboard`: HTTP 401.

These are deployment, health and authentication-boundary checks. They do not claim
an authenticated portfolio walkthrough, successful provider access or a fresh IBKR
import. Decision Lab's existing provider and known behavior issues remain deferred.

The initial build stopped at test collection because the test-only backfill script
was absent from the normal upload. A subsequent attempt was cancelled to correct
the runtime-inventory command's Python 3.11 syntax. Neither attempt reached a deploy
step. The successful build supplied the helper and used the retained production
runtime; application code and test assertions were not changed to make it pass.

## Rollback references

Previous web revision: `options-roi-web-00118-hnn`.
Previous mobile revision: `options-roi-mobile-api-00176-w8q`.
Previous IBKR job image:
`europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/options-roi-mobile-api:9cdc5a31c68a3880244eb245c4c3e830c0f103bf`.

Service rollbacks are independent: route each service back to its recorded previous
revision and restore the job image if a full release rollback is needed. Do not
assume rolling back web also rolls back mobile or the import job. Verify retained
revision/image availability before using these references.

[Open the production dashboard](https://options-roi-web-htdlrf6zjq-oa.a.run.app).
