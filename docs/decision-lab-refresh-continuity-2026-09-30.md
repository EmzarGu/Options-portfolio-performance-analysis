# Decision Lab refresh continuity — 30 September 2026

A new free-plan observation date previously replaced the displayed date before
its selections were prepared. The 30 September 19:37 manual refresh ran for
20.45 seconds and saved 20 of 78 selections (19 unique contracts), leaving 58
pending. Most covered-call tickers consequently lost their comparisons despite
a complete 28 September snapshot remaining in storage.

## Behavior

When the latest date is incomplete, the Lab displays the newest valid complete
saved dataset covering the **current selection plan**, within seven calendar
days of the current New York date. The interface retains its actual quote date
and identifies the newer date being prepared. Once every new selection has a
valid response, the next read switches to that date. Quotes from different dates
are never combined in one comparison dataset.

The fallback search uses one bounded Firestore batch read and no provider calls.
A complete dataset means every requested selection has a validated response;
valid `no_data` responses count as prepared, but do not invent option contracts.
A changed portfolio requiring unavailable selections, an invalid older dataset,
or data beyond the age limit cannot be labelled complete. Without a suitable
complete dataset, the available latest partial data remains visible with an
explicit notice. Invalid latest responses are treated as missing and can be
repaired on the next authorized fetch.

Both UI views show download progress separately: `Download for DATE: X / Y
selections ready`. Field-presence percentages explicitly refer only to displayed
contracts and are not labelled as overall download completeness.

No new scheduler, background worker, automatic provider retry or subscription
was added. Existing manual/scheduled preparation, the 20/90-second time budgets,
90-credit application ceiling, five-minute cooldown and accounting calculations
are unchanged.

## Status fields

- `quote_date`, contract count, field coverage and fetch timestamp describe the
  dataset actually displayed.
- `latest_quote_date`, `prepared_request_count`, `request_count` and
  `missing_request_count` describe progress on the latest eligible date.
- `using_previous_complete` and `source=stored_fallback` identify fallback.
- `display_missing_request_count=0` describes a complete displayed dataset even
  when newer selections are still pending. Overall `status=partial` continues
  to signal unfinished preparation to operational logs.

## Verification

All **556 tests passed locally** (3.81 seconds), including nine added regression
cases for partial/failed updates, promotion, mixed-date prevention, corrupt
snapshots, changed selection plans, age bounds and valid no-data responses.

A replay using actual saved September 28/29 data recreated today's first 20
responses entirely in memory. It kept all **73 older contracts** visible while
reporting **20/78 prepared and 58 pending**. Replacing the in-memory partial batch
with the completed snapshot switched to **73 contracts dated September 29,
78/78 ready, zero pending**. Production snapshots were only read. The Firestore
batch lookup was also exercised directly against the existing dated documents.

## Production release and verification

All **556 tests also passed in the production container** (19.22 seconds; one
existing dependency deprecation warning). Cloud Build
`03be9c52-712c-4cb2-af4c-ff2ffa826db1` succeeded. Only the provider snapshot reader,
coverage-note builder and two UI templates were overlaid onto the immutable
previous production image; dependencies and unrelated work were preserved.

- Web revision `options-roi-web-00126-tgx`, Ready, **100%** of dashboard traffic.
- Web and import job image: `lab-snapshot-fix-20260930-174752`.
- Digest: `sha256:87d14d23ec4a66620dce44fb65467cd5d73935c480ed5416fda11e4567f48816`.
- Health: HTTP 200, `status=ok`.
- Main and standalone Lab: quotes dated September 29, **78/78 selections ready,
  73 contracts, 24 comparison rows**, including roll alternatives for SHOP,
  FLEX, GLW, CCJ, CSCO, ATI, NVT and NVDA. STZ, NLR and FUTU show their actual
  eligibility rejection reasons rather than missing-data messages.
- Both browser error checks were empty. Scoped new-revision logs showed no
  errors; Lab requests took 939.03 and 532.36 milliseconds.
- Warm-only execution `ibkr-flex-import-7v54t` succeeded; dashboard preparation
  took 2.98 seconds, with 78 selections ready and zero missing. No IBKR statement
  import was initiated; the job's normal arguments remain unchanged.
- The provider control document was identical before/after deployment checks:
  no additional API credits were consumed.

Today's scheduled job had completed the new dataset at 19:46 Zurich before the
fix was deployed. Live checks therefore verified complete-data display; the
partial-update fallback was verified by the actual-data in-memory replay and
regression tests, without deliberately damaging production data.

Build logs, file hashes, actual-data replay and verification records are saved
under `tmp/decision-lab-refresh-release/`.

## Rollback

Route dashboard traffic back to `options-roi-web-00125-fts` and restore the
`ibkr-flex-import` image tag `lab-calculation-fix-20260927-175940`. Preserve existing
provider configuration and all snapshots. No database migration is required.
