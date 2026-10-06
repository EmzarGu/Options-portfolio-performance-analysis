# Decision Lab Market Data production release — 23 September 2026

The user authorized production adoption after the free-account pilot and broader
local integration tests. This release connects the existing Decision Lab to
Market Data for dated planning comparisons. It preserves the existing candidate
calculations, accounting rules, position classification and portfolio data.

## Data and normal use

- Normal page loads read saved option selections from Firestore and make no
  Market Data requests. The visible status identifies the quote date, credit
  usage and any selections still awaiting preparation.
- Comparisons are labelled **dated estimates**. These are not executable prices.
  Delta and IV have no separate provider timestamps; the field-timing discrepancy
  described in the [local integration report](decision-lab-marketdata-integration-2026-09-23.md)
  remains unresolved and is disclosed in the interface.
- The existing **Fetch option data** button can request missing selections with
  a shared 20-second budget. Successful partial progress is saved. Repeated reads
  and already-prepared selections do not consume credits.
- The existing IBKR job prepares selections after dashboard preparation, with a
  90-second option-data budget. The existing 07:15 and 19:45 Europe/Zurich
  schedules are reused. An option-provider failure does not invalidate a
  successful portfolio import.
- Under the verified free-account availability rule, 24 September morning can
  still show quotes dated **22 September**. The 23 September observation becomes
  eligible after the US market opens on 24 September; the evening preparation
  can then retrieve it. The application does not relabel an older observation
  as today's price.

## Credit and storage controls

Production uses a transactional Firestore credit ledger and refresh lease shared
by the web service and import job. The application stops at 90 credits within the
account's 100-credit allowance; it reconciles provider response headers so use
outside the application reduces the available budget. The quota window resets
at 09:30 America/New_York, including daylight-saving offsets. Uncertain calls
retain their reserved credit. No paid subscription was enabled.

A five-minute gap after the most recent provider request prevents immediate
successive refreshes from different server instances. Requests are serialized;
there are no automatic provider retries or full-chain downloads. This is an
operational precaution for the free plan, not a static-egress-IP guarantee.

The `decision_lab_marketdata` collection holds the shared `control` document and
one dated snapshot document per observation date. Snapshot publication verifies
the refresh lease transactionally. The token is stored in Secret Manager as
`marketdata-api-token:1`, available to the existing runtime service account.
Both runtimes select this path with `DECISION_LAB_PROVIDER=marketdata`.

For initial rollout, 74 previously validated selections were copied into shared
storage without additional provider calls. Readback against the original 11
situations returned 60 unique contracts with zero missing selections. The ledger
was seeded with the pilot's actual 89 credits used, leaving one application
credit before its conservative ceiling. Live portfolio prices may identify
additional situations and therefore additional pending selections.

## Release verification

The production image layers eight integration files onto the immutable image
already running in production, preserving its installed dependencies. Tests are
mounted only during build verification; they are not added to the deployed image.
All 536 local tests passed. All **536 tests also passed in the final production
container** (18.71 seconds; one existing dependency deprecation warning).

| Evidence | Result |
| --- | --- |
| Final Cloud Build | `8fcb67d2-54a3-45a2-8e3d-dc94203562ab`, SUCCESS |
| Web revision | `options-roi-web-00124-c6x`, Ready, 100% traffic |
| Web and import-job image | `marketdata-20260923-202818` |
| Image digest | `sha256:265e34f25827a0634ffada78eb0c6e9e009a7de994abedcd23b2106d6b255ea4` |
| Health | HTTP 200, `status=ok` |
| Schedules | Both existing schedules ENABLED; unchanged |
| Production Decision Lab timing | First integration request: 1,026 ms server-side |
| Browser | Main dashboard renders 12 situations, 21 dated-estimate rows, quote-date columns and provider status |
| Latest stored data | 62 contracts; quotes dated 22 September; 90 credits used; three selections pending |

Warm-only execution `ibkr-flex-import-485m9` completed successfully against the
initial integration image, before the final dashboard-label correction. It read
back the portfolio snapshot, fetched one additional AU selection from the real
provider, then stopped at the 90-credit application ceiling. Its `option_data`
status is correctly `partial`, with three selections missing. Current prices made
AU a near-strike put situation, increasing the live universe from 11 to 12.
The core preparation code is identical in the final image. The job's normal
arguments were verified unchanged after this execution override.

Final browser reads did not spend additional API credits; the control document
remained at 90 used/10 account credits remaining, with its lease released. AU's
three pending selections cannot be completed within today's application ceiling.
They can be retried after the next quota reset; the existing evening preparation
will attempt the then-current selection plan. The free plan and bounded budget
do not guarantee exhaustive chain coverage.

Both embedded and standalone Decision Lab views rendered the date and credit
status, nine quote-date column headers and 21 dated-estimate comparison rows.
A standalone manual-refresh click during the five-minute cooldown completed with
the explicit wait notice, retained all 21 comparison rows, re-enabled the button
and consumed no additional credits. The final eight source-file hashes match the
release manifest.

The first build stopped because the verification environment omitted the
`scripts.ibkr_backfill` helper imported by an existing test. The helper was added
to the test-only mount and the build restarted. No traffic was changed by that
failed build.

## Rollback

Route `options-roi-web` traffic back to `options-roi-web-00122-mr7`. Restore the
existing import job's image to `morning-refresh-20260920-082304` and remove its
`DECISION_LAB_PROVIDER` override. That restores the previous code/provider path;
it does not restore the unavailable CuteMarkets service. Retain the dated Market
Data snapshots for inspection. There is no accounting-data migration to undo.

The historical CuteMarkets enrichment job is a separate subsystem and is not
repaired or reconfigured by this release.
