# Realized option P&L correction — 1 October 2026

> Historical record, superseded by [completed roll-chain accounting](completed-roll-chain-accounting-2026-10-01.md).
> Current dashboard strategy P&L defers the full linked chain balance until completion.


The dashboard previously added replacement opening credits to the old lot's
realized close event whenever both legs belonged to a roll order. This booked
still-open option proceeds as realized income and assigned zero unrecognized
premium to the replacement. The user authorized correcting open-premium recognition, testing and deployment.
Contract-level splitting was an implementation choice, subsequently superseded
by the agreed completed-chain rule.

A close now recognizes only its own opening credit less closing debit, net of
commissions/rebates. Every replacement retains its actual opening premium until
its own close, expiration, or assignment. This applies to puts and covered calls,
same-expiration and later-expiration rolls, partial fills, and repeated rolls.
Wheel eligibility, assignment stock attribution, dividends and market-data
provider behavior are unchanged. This is the app's existing wheel attribution
model, not a switch to IBKR's assignment stock-basis presentation or tax reporting.

Monthly/yearly and ticker totals are recalculated consistently from saved raw
transactions. API fields and Decision Lab see actual unbooked premium on open
contracts, with zero `realized_premium_already_booked` in new IBKR builds. Existing
Lab exercise calculations already add unbooked premium once; no changes to their
outcome formulas are needed. Expiry projections add open premium separately from
historical realized results.

Snapshot schema is raised from 6 to 7. The version changes the persistent snapshot
ID and old snapshots are rejected, including price-only refresh paths. Derived
Lab payloads include that pipeline ID in their cache key, preventing stale
accounting results from being reused. Raw IBKR rows and provider snapshots are
preserved.

## Verification before deployment

- All 559 tests passed locally.
- Updated old-policy tests verify losses stay in the old-contract closing year
  and replacement premium is recognized in its own closing/expiry year.
- Added partial/repeated roll cases for puts and calls, different fill prices,
  larger replacement quantities, cross-month recognition and lifetime cashflow
  conservation. API/Decision Lab checks verify replacement premium is unbooked
  and included once in projected outcomes.
- Cache regression verifies unchanged source imports receive a new snapshot ID
  and schema-6 stored states cannot be loaded by the corrected reader.
- Replayed actual stored IBKR rows through 30 September 2026. September options
  P&L is [private reconciliation amount]; stock -[private reconciliation amount]; net dividends [private reconciliation amount]; total [private reconciliation amount]
  November open premium is [private reconciliation amount] Nineteen historical months change;
  lifetime option receipts less payments are conserved across realized and open
  premiums. Assignment stock transactions and dividend cashflows are unchanged.
- The local accounting replay omits price history; its capital/return fields are
  not production validations. Production checks use the normal persisted price
  history and current-price overlay.

Release evidence is saved in `tmp/realized-pnl-fix-release/`. The production
container overlays the two changed runtime files onto the previous immutable
web/import image, preserving existing dependencies and unrelated local work.

## Production release and verification

Cloud Build `155cc33e-749a-4653-95d7-054a30730874` built and tested the release:
all 559 tests passed in the production container. The immutable image digest is
`sha256:32cdd783a06aca6eab5471c17c4ca200da8dfc90d43cf521d9e587443305f01b`
in `europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/options-roi-mobile-api`.

The existing web service now sends 100% of traffic to
`options-roi-web-00127-4tl`; the existing mobile service sends 100% to
`options-roi-mobile-api-00178-pcj`. Both revisions are Ready and their public
health endpoints returned HTTP 200 (`/health` and `/v1/mobile/health`). The
existing `ibkr-flex-import` job uses the same image. No duplicate dashboard
service was created, and scheduler configuration was unchanged.

Warm-only execution `ibkr-flex-import-qvtld` succeeded in 30.52 seconds. It used
the saved IBKR report, without fetching a new statement or consuming provider
credits. It prepared schema-7 snapshot
`ibkr_flex:2026-10-01:381a6e0c1afe5b4bd571454ffd6469d2`. Normal production price
history is complete. September average capital is [private reconciliation amount] and peak
capital [private reconciliation amount] unchanged from the prior accounting snapshot.
Corrected September RoAC is 1.3663717166% and RoPC 1.1235416306%.

The live Monthly screen displays September options [private reconciliation amount] stock -[private reconciliation amount]
dividends [private reconciliation amount] total realized [private reconciliation amount] and November open premium [private reconciliation amount] (rounded
UI values). Browser proof is saved as `production-monthly.png` in the release
evidence folder. Embedded and standalone Decision Lab views load alternatives
using 79 stored contracts dated September 30, with 85/85 selections ready and
no browser console errors. November baseline premiums appear as unbooked open
premium: SHOP [private reconciliation amount] ATI [private reconciliation amount] GLW [private reconciliation amount] and CCJ [private reconciliation amount] (rounded). Provider
control was identical before and after verification: 80 credits used, 20
remaining. These remain dated scenario estimates; liquidity filters still
exclude some tickers' unsuitable quotes.

## Rollback

If a release defect requires rollback, route web traffic back to
`options-roi-web-00126-tgx` and mobile traffic back to
`options-roi-mobile-api-00177-kqd`. Restore the import job image to
`europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/options-roi-mobile-api:lab-snapshot-fix-20260930-174752`.
The prior schema-6 snapshots and raw reports were preserved, so there is no
destructive database migration to reverse. Rolling back also restores the
incorrect early recognition behavior; it is an emergency operational fallback,
not an accounting alternative.
