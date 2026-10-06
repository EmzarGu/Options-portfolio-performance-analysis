# Decision Lab calculation corrections — 27 September 2026

The user authorized correcting the errors identified in the
[Decision Lab review](../analysis/decision-lab-review-2026-09-27/review.md), testing,
and production deployment.

## Corrections

Covered-call rolls now include the **signed change in strike proceeds** when
calculating exit P&L and the change versus retaining the current call. A lower
strike reduces the conditional stock-sale proceeds; receiving a roll credit
does not by itself improve the exercise outcome. The separate added-upside-room
field remains nonnegative because it describes additional upside only.

Covered-call exercise scenarios now include the existing call's unbooked
accounting premium. Amounts already booked in realized P&L are not added again.
The non-exercise branch already includes that premium in current unrealized P&L,
so it is not added a second time there. New calls on uncovered holdings do not
inherit premium from unrelated open positions.

Price-source labels now describe the inputs actually used: ask to close, bid to
sell, or the existing provider-mark fallback. Both dashboard views label the
weighted result as a scenario estimate and delta as an exercise proxy. A visible
note explains baseline-first ordering, different expiry horizons, fixed current
stock-price inputs and exclusion of future trading fees.

The existing selection policy and delta-weighted two-case model are unchanged.
These estimates are not forecasts or executable quotes. The legacy API field
`recommended` still refers to the first displayed row, which is the current
position where a baseline exists; it must not be interpreted as an independent
best-trade ranking. API numeric field names remain compatible.

## Verification

- 11 new regression cases cover actual GLW, CCJ and FLEX roll-down examples,
  same-strike and higher-strike rolls, one/two contracts, partially booked
  premium, zero and negative accounting premium, no existing call, and the
  SHOP baseline/roll through the dashboard payload builder.
- All **547 tests passed locally** (4.26 seconds).
- All **547 tests passed in the production container** (13.67 seconds), with one
  existing dependency deprecation warning.
- Replaying the saved 24 September quotes gives the results below. No fresh
  provider requests were needed for this replay.

| Comparison | Net roll credit | Correct change in exit proceeds |
| --- | ---: | ---: |
| GLW 190 → 180, one contract | [private reconciliation amount] | −[private reconciliation amount] |
| CCJ 105 → 100, two contracts | [private reconciliation amount] | −[private reconciliation amount] |
| FLEX 130 → 120, two contracts | [private reconciliation amount] | −[private reconciliation amount] |

SHOP's exercise outcome is **[private reconciliation amount]**, including its [private reconciliation amount] unbooked premium,
versus the previous [private reconciliation amount] These figures describe the saved inputs before
future trading fees; they are not predictions of actual realized results.

## Release

Cloud Build `33423c4c-3597-43a2-9079-6975b1efb3e3` succeeded. The release overlays
only the candidate-calculation module and the two Decision Lab UI templates onto
the immutable prior production image. Tests and their helper are mounted for
verification only. No dependency, provider, accounting-store, credential,
schedule or database migration is included.

- Image tag: `lab-calculation-fix-20260927-175940`.
- Digest: `sha256:24302bc1a9e33b637bae83efdd0d08404091a07d9886149321302fa1d3e39546`.
- Previous web revision: `options-roi-web-00124-c6x`.
- Previous web/import image: `marketdata-20260923-202818`.
- Build, source hashes and replay evidence: `tmp/decision-lab-fix-release/`.

### Production verification

- Web revision `options-roi-web-00125-fts` is Ready and receives **100%** of traffic.
  There is one active production dashboard. The import job uses the same image.
- Health returned HTTP 200 with `status=ok`.
- Main and standalone browser views each display the corrected GLW −[private reconciliation amount]
  CCJ −[private reconciliation amount] and FLEX −[private reconciliation amount] exit changes, and SHOP's [private reconciliation amount] rounded exercise
  result. The main view retains all 21 comparison rows across 12 situations.
- Both views show the scenario labels, quote date and actual bid/ask sources.
  Browser console checks returned no errors. The scoped new-revision server
  check returned no errors; Lab requests took 975.50 and 1,541.23 milliseconds.
- Warm-only execution `ibkr-flex-import-fdlvd` completed successfully in 15.9
  seconds. Dashboard preparation itself took 3.01 seconds and restored a shared
  pipeline snapshot. This did not initiate an IBKR statement import.
- Option preparation succeeded with 67 contracts, 73 prepared selection requests,
  zero missing selections and quote date 24 September. The provider control
  document was identical before/after deployment verification: no API credits
  were spent.
- Normal job arguments remain `-m portfolio_backend.ibkr.import_job`. The two
  existing schedules remain enabled at 07:15 and 19:45 Europe/Zurich.
- The separate mobile API and historical enrichment job were not redeployed.

## Rollback

Route web traffic to `options-roi-web-00124-c6x` and restore the import job image
`marketdata-20260923-202818`. Keep the existing Market Data configuration and
snapshots. No data restoration or accounting migration is necessary.
