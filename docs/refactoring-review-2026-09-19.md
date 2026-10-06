# Dashboard review, refactoring and deferred decisions

Reviewed: 2026-09-19. Status: the user authorized refactoring first; batch A is
complete and was subsequently [deployed with user authorization](production-deployment-2026-09-19.md).
Behavior corrections, historical-job repair and Decision Lab provider selection
remain deferred. See [completed changes and verification](refactoring-results-2026-09-19.md).

## Recommendation

Refactor the existing Python application incrementally. Keep its screens, routes,
payload fields, financial formulas, accounting rules, and stored history intact.
Do not introduce a frontend rewrite or new infrastructure as part of cleanup.
Approve the three reproduced code-error fixes and the verified deployment repair
separately from internal restructuring.
Select the Decision Lab data source independently, after a small data-quality pilot.

## Evidence and limits

- Local HEAD: `9cdc5a31c68a3880244eb245c4c3e830c0f103bf` (2026-07-12).
- Existing uncommitted work predates this review in `mobile_api.py`,
  `portfolio_backend/mobile_api_service.py`, `portfolio_backend/ibkr/assignment_quality.py`,
  mobile route tests/contract documentation, and untracked review/prototype files.
  Preserve it; do not reset the checkout or include it in a cleanup commit accidentally.
- Baseline: **448 tests passed in 3.55 seconds** under the existing Python 3.12.13
  virtual environment, using `.venv/bin/python -m pytest -q --durations=12`.
  `make test` was attempted first but Apple's tool wrapper stopped at the unaccepted
  Xcode licence. No system settings or licence acceptance were changed.
- Cloud Run web service was Ready, with 100% traffic on
  `options-roi-web-00118-hnn`. Its image tag matched local HEAD. This does **not**
  establish that the local uncommitted changes are deployed.
- The deployed `/health` endpoint returned HTTP 200. This was a health/configuration
  check, not an authenticated browser walkthrough or proof of provider availability.
- Live configuration confirmed IBKR/Firestore source mode and a Secret Manager
  reference for `CUTEMARKETS_API_KEY`. Secret contents were not retrieved.
- Unauthenticated connection probes: CuteMarkets website raised a connection
  error; its chain API timed out after a 12-second timeout. The web research fetch
  of its documentation also failed. These observations support an availability
  problem from the tested locations; they do not prove permanent closure or identify
  an entitlement, DNS, account, or server-side root cause.
- No production refresh, import, data write, subscription, trade, or deployment
  was performed. No replacement provider has been authenticated or tested yet.

## Findings

### Errors reproduced locally — approval requested before fixes

| ID | Trigger and observed result | Proposed correction | Verification required |
| --- | --- | --- | --- |
| E1 | A provider returns some chain rows, then a later page fails. `decision_data.py` saves the partial result before checking its error. In-memory reproduction: previous bid `1.0` became `9.0`, while status said `failed_refresh_kept_previous`. All three store implementations upsert contract IDs, so the same failure path can overwrite previous successful rows. | Stage a complete successful chain before publishing it. Preserve the prior successful generation when any page fails. Define partial success across different chains separately. | Pagination failure, transport failure, successful smaller replacement chain, and fallback must retain the correct contracts and timestamps. |
| E2 | `save_derived_payload` publishes ready metadata before writing its chunks. A read inserted between these writes returned the old payload with the new refresh metadata. | Write immutable generation chunks first and publish the generation pointer last; check content integrity on read. Keep old-format reads during transition. Merely writing metadata last is insufficient when concurrent writers share chunk names. | Interleaved readers/writers, interrupted upload, corrupt/missing chunk, old-format read, and successful replacement. |
| E3 | `_decision_lab_cache_key` uses `value or default` for targets. A payload with both target values explicitly zero produced exactly the same key as default values. | Use the default only when the value is missing/None, and version the changed cache key. | Zero and default must differ; explicit default and implicit default must match. |

Relevant implementation: `portfolio_backend/option_market/decision_data.py`,
`portfolio_backend/option_market/store.py`,
`portfolio_backend/derived_payload_cache.py`, and
`portfolio_backend/web_data_service.py::_decision_lab_cache_key` (moved unchanged
from `web_dashboard.py` during batch A).
These are demonstrated local code errors; the review does not claim each has
occurred in production.

### E4 — verified historical-import deployment failure

The enabled weekday scheduler targets `option-market-history-import`. Its configured
image is `cycle-signal-20260530T102728Z`; the specific September 18 execution
`option-market-history-import-x6zmc` reports `Completed=False` because the image was
not found. This prevents startup before the provider is contacted. The job summary's
`EXECUTION_PENDING` is not evidence of an active import.

`cloudbuild.yaml` updates the web, mobile and IBKR import targets but omits this
historical job. Proposed repair, subject to agreement: decide whether historical
enrichment remains needed, then update and verify the retained job and include it
in release/retention checks. Do not simply restart the broken execution. No job or
scheduler configuration has been changed.

### Architecture and performance

These observations describe the pre-refactor baseline. Batch A addressed module
ownership and the redundant index reset; cache-policy and provider-limit changes
remain deferred.

- `web_dashboard.py` is 1,079 lines and mixes authentication, HTML composition,
  routes, history loading, provider construction, and three caches.
- `mobile_api.py` is 1,845 lines and combines HTTP transport with source markers,
  import-health classification, snapshot coordination, and refresh management.
  `web_dashboard_payloads.py` imports this HTTP entry point for shared context.
  Shared context belongs in a backend service used by both entry points.
- Accounting already has a useful separation: preserve canonical calculations,
  cycle projections, transport DTOs, and the regression fixtures protecting them.
- Cache lookup/build/eviction logic is repeated. Clearing or evicting per-key locks
  while builds are active can permit duplicate builds. The dashboard's zero-TTL
  default also retains a key lock without the normal payload eviction path.
  These need dedicated concurrency tests before changing cache ownership.
- `_frame_records` resets the same frame index twice to discover its column name.
  This is a small, straightforward allocation reduction with identical output.
  Broader latency gains need measured representative workloads; the fast test
  suite does not establish production performance.
- CuteMarkets pagination has no total page/deadline bound. Its retry delay accepts
  provider `Retry-After` values without a maximum. A nominal provider-call budget
  counts chains, not HTTP pages/retries. Any new limits would change failure behavior
  and should be agreed as part of reliability work.

### Build and documentation

- `requirements.txt` leaves all dependencies unpinned. Docker uses Python 3.11;
  `.python-version` and the local environment use 3.12. A local pass alone does not
  prove the Docker runtime has the same dependency behavior.
- `cloudbuild.yaml` builds and deploys but contains no test step. Both build ignore
  files also exclude tests; a gate must deliberately supply them. Deployment should
  be gated on tests in the same image/runtime. No deployment configuration was changed.
- The test audit formerly reported 415 tests; it now records the measured 448-test
  baseline and its limits. Docker is absent locally, so the production container
  was not tested on this host.
- Cloud operations documentation now records verified web/mobile/job/scheduler
  observations separately from unverified May storage/retention details.
- Option-market documentation now distinguishes configured CuteMarkets from verified
  availability and records E1's failed-refresh exception.
- Added a root README linking entry points, configuration, tests, design and operations;
  updated the architecture ownership map and corrected the mobile runbook's stale
  claim that full refreshed contexts are persisted across instances.

## Implementation batches

| Batch | Concrete changes | Preserved behavior / acceptance |
| --- | --- | --- |
| A — internal refactoring: complete locally | Extracted web authentication and cache/history services; moved shared context orchestration and IBKR import-health rules out of `mobile_api.py`; updated web, mobile, prototype and test consumers; removed a redundant frame index reset; updated README, architecture, test and runbook documents. | 457 passing tests and identical OpenAPI documents. Same routes, auth/session semantics, DTOs, calculations, UI, cache TTLs and explicit-refresh behavior. No new provider, dependency or deployment. |
| B — agreed error corrections | Fix E1–E3 with regression tests. Agree cache invalidation and provider timeout/page limits before adding those further behavior corrections. | Changes limited to the specified failure cases and zero-target collision. No accounting formula changes. |
| C — reproducible build | Test the existing production Python runtime, select a common supported Python version, record tested dependency versions, supply test sources and add a gate before deploy steps. Agree E4's historical-job repair and release coverage. | Present runtime/version and job changes before applying them; test the exact selected build. No deploy as part of this review. |
| D — agreed Decision Lab provider | Pilot the selected service with a few ticker/expiry/type groups, compare normalized fields and freshness, measure quota cost, then implement its adapter behind the existing interface. | Preserve historical source provenance, sheet probability fallback, user-triggered refresh, candidate logic, and prior successful data. No silent fallback to a different quote quality. |

Implement and test each batch before the next. Review the final diff and document
the actual measured improvement, remaining risks, rollback, and any deferred item.
Avoid broad formatting changes that obscure financial-code review.

## Decision Lab alternatives

The subsequent [module-specific investigation](decision-lab-data-options-2026-09-19.md)
incorporates the user's acceptance of delayed data and supersedes the preliminary
shortlist below. It distinguishes required delta from optional IV, documents the
existing mark-only path, adds EODHD, and examines Market Data's historical-Greek
limitations and paid cached mode. No provider switch has been approved or made.

Prices below are USD, checked against official pages on 2026-09-19. Eligibility,
entitlements, tax, and any added hosting cost remain to be verified for this account.
Current recommendations need chain identity, bid/ask or explicitly indicative marks,
underlying price, delta/IV, and preferably volume/open interest. Existing historical
enrichment is a different requirement from fetching today's chains.

| Option | Published cost | Fit and limitations |
| --- | --- | --- |
| IBKR market-data API | OPRA non-professional subscription is [private reconciliation amount]/month, with a commission-based waiver; additional underlying subscriptions may be needed. | Worth evaluating because the user already has IBKR. This is a separate market-data integration, not Flex. Requires a supported authenticated gateway/session and verified account entitlements; greater operational work for Cloud Run. Do not assume the total cost is [private reconciliation amount] [Pricing](https://www.interactivebrokers.com/en/pricing/market-data-pricing.php), [API access](https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/), [gateway](https://www.interactivebrokers.com/docs/web-api/v1/endpoints/introduction), [Greeks subscriptions](https://interactivebrokers.github.io/tws-api/option_computations.html). |
| Market Data Starter | [private reconciliation amount] month-to-month, or [private reconciliation amount]/month with annual commitment ([private reconciliation amount]/year). | 15-minute delayed options, 10,000 daily credits; chains expose bid/ask, IV and Greeks. Promising for on-demand analysis if delayed data is acceptable. The free tier is 24-hour delayed with only 100 daily credits. [Pricing](https://www.marketdata.app/pricing/), [chain fields](https://www.marketdata.app/docs/api/options/chain/). |
| Tradier Brokerage API | No API fee for brokerage account holders. | Real-time US stock/options data; Greeks update hourly. Sandbox quotes are delayed 15 minutes and have **no Greeks**, so sandbox alone does not meet current candidate scoring needs. Requires a qualifying brokerage account; residence eligibility and account fees were not established. [API fee](https://docs.tradier.com/docs/faq), [freshness/Greeks](https://docs.tradier.com/docs/market-data). |
| Alpaca indicative feed | Free indicative feed. | Chain endpoint provides quotes/trades/Greeks, but quotes are modified derivatives of OPRA and trades are delayed. Suitable only if indicative analytics are explicitly accepted; must not present these as executable bid/ask. Volume/OI and underlying coverage require a pilot and possibly additional endpoints. [Feed quality](https://docs.alpaca.markets/us/docs/historical-option-data), [chain endpoint](https://docs.alpaca.markets/us/reference/optionchain). |
| Massive Options Starter | [private reconciliation amount]/month. | Snapshot, Greeks/IV, daily OI and 15-minute delayed data. Quotes are listed under the [private reconciliation amount] Advanced tier, so Starter must not be assumed to provide executable bid/ask. Its free tier does not list Greeks/snapshots. A close schema match to the existing adapter, but data quality/entitlements still need checking. [Plan comparison](https://massive.com/pricing?product=options). |

Market Data counts each returned option symbol against credits, so filter expiry,
type and strikes. It also restricts simultaneous access to one IP address: Cloud
Run egress and concurrency must be checked before choosing it; additional network
infrastructure could outweigh the subscription saving. [Limits](https://www.marketdata.app/docs/account/plan-limits/).

My recommendation: evaluate existing IBKR entitlements first for the lowest possible
recurring cost. If an independently hosted HTTP service is preferred and 15-minute
delay is acceptable, pilot Market Data on a trial before considering a paid plan;
resolve its IP restriction first. Use Alpaca only after explicitly accepting
indicative data. No verified unconditional free replacement currently meets every
requirement without an account, operational, quota, or quote-quality tradeoff.

Before replacement, agree: acceptable delay/quote quality, monthly budget, willingness
to operate an IBKR gateway or open another brokerage account, and whether historical
enrichment must migrate too. No account signup, purchase, provider switch, or new
Greeks estimation should happen implicitly.
