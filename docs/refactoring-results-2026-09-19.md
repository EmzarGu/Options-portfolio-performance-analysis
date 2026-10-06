# Refactoring results — 2026-09-19

The authorized internal refactor is complete in the local checkout. Screens,
financial formulas, HTTP contracts, authentication rules, cache policies and
Decision Lab provider behavior are preserved. After separate user authorization,
the refactor was [deployed to production](production-deployment-2026-09-19.md).

## Changes

| Responsibility | Owner after refactoring |
| --- | --- |
| Web HTTP routes and request validation | `web_dashboard.py` |
| Web login, configuration and signed sessions | `portfolio_backend/web_auth.py` |
| Web payload caches, history loading and existing Decision Lab orchestration | `portfolio_backend/web_data_service.py` |
| Mobile HTTP routes, authentication, timing and audit | `mobile_api.py` |
| Shared portfolio context, source markers, refresh, snapshots and build coordination | `portfolio_backend/context_runtime.py` |
| IBKR import-health classification | `portfolio_backend/ibkr/import_health.py` |

Web and mobile consumers use the same `get_context` / `refresh_context` runtime.
Web startup no longer imports and creates the mobile HTTP application. The existing
local visual prototype now imports its data service directly; its payload logic
is unchanged. Tests patch the owning modules instead of private HTTP helpers.

The web entry point shrank from 1,079 to 392 lines and the mobile entry point from
1,845 to 593 lines. Code moved into focused modules; this is separation of
responsibilities, not a claim that the total implementation shrank.

Table serialization now resets the index once instead of twice when adding an
index column. Output, row limits and input immutability are preserved. This removes
one redundant allocation per indexed table conversion; no production latency or
memory improvement has been benchmarked.

README, architecture, web/mobile runbooks, test audit, operational status and
option-market documentation now distinguish current implementation, verified
observations and deferred repairs.

## Verification

- **457 tests passed in 3.32 seconds** using the existing Python 3.12.13 virtual
  environment: `.venv/bin/python -m pytest -q`. The pre-refactor suite had 448 tests.
- `make test` was attempted but Apple's tool wrapper stopped at the unaccepted
  Xcode licence. The equivalent virtual-environment command ran successfully;
  no system configuration or licence acceptance was changed.
- Both complete OpenAPI documents match the saved pre-refactor versions.
- All 152 original web/mobile top-level functions retain identical syntax trees
  after normalizing module qualification, two shared-runtime function renames and
  docstrings. The prototype's function logic also matches its baseline.
- The 310 assertions across the existing mobile and web route tests are preserved,
  with module references updated. No tests or assertions were removed.
- Nine new cases cover independent web startup, concurrent web/mobile context reuse,
  financial payload parity for both display modes and zero/default/custom targets,
  table serialization and source immutability, and prototype service integration.
- Accounting, projection, DTO, provider, persistent-store and template modules
  match the pre-refactor working files. Pre-existing assigned-holdings changes and
  review/prototype work were preserved; the prototype entry point only needed its
  imports/calls updated. The checkout was already dirty before this task.

The baseline comparison used a saved copy of this working checkout, including its
pre-existing edits, rather than assuming Git HEAD represented the user's current
app. These are local checks, not live provider or production-container validation.
Docker is unavailable on this host. The subsequent authorized release ran all 457
tests successfully in Cloud Build using the retained production Python 3.11.15 image.
Dependency declarations remain unpinned; the release reused the previous web image's
dependency layer. See the deployment record for the release-specific test gate.

## Deferred work

Per the user's instruction, Decision Lab provider selection comes after refactoring.
Its current service and failure behavior remain unchanged. The
[original review](refactoring-review-2026-09-19.md) records alternatives and the
separate decisions still required:

- E1: partial provider refresh can overwrite previous successful contracts.
- E2: derived-cache metadata can become visible before matching chunks.
- E3: explicit zero targets can share the default Decision Lab cache key.
- E4: the historical option-import job references an unavailable image.
- Runtime/dependency alignment, a production-image test gate, and cache eviction /
  provider timeout-policy changes.

The shared runtime still imports existing provider/settings dependencies from
`streamlit_app` and uses FastAPI exceptions for validation. Further extraction is
possible but was not bundled into this refactor. Existing cache invalidation and
cross-process lease behavior are retained; the new concurrency test establishes
ordinary shared cold-read reuse, not correctness for every invalidation race.

## Review and rollback

Review the new modules together with changes to both HTTP entry points, web payload
assembly, the prototype's import wiring, route test references and the new regression
file. Application launch commands and environment configuration remain the same.

No commit was created. The authorized production release and previous service
revisions are recorded in the deployment document. For a local rollback, restore
these changes as a group against the
pre-refactor working copy, preserving the user's earlier uncommitted work. A blanket
reset to HEAD would also discard unrelated work and is not an appropriate rollback.
