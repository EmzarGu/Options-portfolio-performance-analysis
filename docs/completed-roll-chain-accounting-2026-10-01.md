# Completed roll-chain accounting — 1 October 2026

The user authorized implementing the agreed rule as the next version on the
current refactored app, testing, updating documentation and deploying. This is
a forward implementation, not a rollback or an Excel reconciliation.

## Recognition rule

An ordinary short option realizes on final close, expiration or assignment.
A proven broker-linked roll continues the same strategy quantity. Its balance
is all opening/replacement receipts minus all buybacks and actual fees, including
rebates. Carry this signed balance through every replacement; do not realize
roll fragments or still-open credits. Recognize the full net result once when
that quantity ends. Allocate partial closes and multiple fills FIFO; carry only
linked quantities and open extra replacement quantities independently. Unrelated
same-day trades remain separate. Negative carried balances must stay negative.
Stock P&L, dividends, assignment-derived wheel eligibility and excluded-chain
inheritance retain their existing rules. These are dashboard strategy results,
not broker/tax contract realization; original raw executions remain auditable.

## Historical documentation and the AAPL correction

The pre-October documents explicitly warned that separate contract legs distort
cross-year roll reporting. The May 10 April note also recorded carried strategy
summaries, but its AAPL -[private reconciliation amount] example sums only the last three adjustments.
The complete linked broker chain begins June 26, 2025; earlier accumulated
credits add [private reconciliation amount] The whole option result is **+[private reconciliation amount]**, recognized
April 17, 2026. The Google-chain example was not a complete broker-chain oracle.
GOOGL's continuous June 2025–February 2026 chain totals **+[private reconciliation amount]**, recognized
February 20, 2026. Assignment stock P&L remains separate. Tests use an exported
AAPL trade fixture containing only noncredential trade fields.

## Implementation

`portfolio_backend/ibkr/pipeline.py` links quantities by broker execution prefix,
date, ticker, type and multiplier. It transfers each FIFO slice independently.
Contract snapshots retain roll close dates and parent execution comments for
audit; realized events contain only terminal quantities. In IBKR mode both
`open_price` and `roll_adjusted_open_price` are per-share deferred net balances.
Existing downstream formulas add these balances once. Transport premium fields
keep compatible names but document signed net balance semantics. Web and both
Decision Lab views display Open strategy balance. Snapshot schema is 8; old
accounting states and their derived responses are not reused.

## Local and independent verification

All 565 tests pass locally via `make test`. An independent raw-broker audit uses execution timestamps and broker group IDs,
without calling production roll-planning functions or using Excel. It verifies
808 included executions, 81 roll transfers, 350 terminal events and all 47
historical months. Maximum monthly discrepancy is below [private reconciliation amount]
Realized option P&L plus all open net balances exactly conserves option cashflow.
Stock realized events and dividend data equal the preceding production state.

Replayed results through September 30, 2026:

| Metric | Completed-chain result |
| --- | ---: |
| 2025 realized options | [private reconciliation amount] |
| 2025 total realized | [private reconciliation amount] |
| 2026 realized options | [private reconciliation amount] |
| 2026 total realized | [private reconciliation amount] |
| August 2026 total realized | [private reconciliation amount] |
| September 2026 total realized | [private reconciliation amount] |
| November open strategy balance | [private reconciliation amount] |
| All open strategy balances | [private reconciliation amount] |

Nineteen monthly option results change versus contract-split reporting. These
replay inputs omit price history, so capital/return fields are not validated by
this audit; production checks use persisted production prices and capital.

Evidence: `tmp/completed-roll-chain-release/` contains raw-derived chain events,
roll transfers, monthly comparisons, replay summary, build/deployment evidence
and verification. Regression coverage includes cross-year calls/puts, repeated
rolls, different fill prices, extra replacement quantities, partial terminal
closures, negative balances, API/Decision Lab outcomes, unrelated trades, orphan
closes, the actual AAPL chain, expiration-session visibility and schema rejection.

## Production deployment

All 565 tests passed in the production container. Cloud Build
`44aca68a-12c6-4c8a-a2eb-6d6d6e3952b5` published immutable image
`europe-west6-docker.pkg.dev/options-performance-dashboard/cloud-run-source-deploy/options-portfolio-performance-analysis/options-roi-mobile-api@sha256:5baddbbbb67256e1a0e009fdd514473c8bb1cc84a7a786f19a21caecbd6a581e`.

The existing web service serves 100% of traffic through
`options-roi-web-00128-5hd`; the existing mobile API serves 100%
through `options-roi-mobile-api-00179-w9g`. The existing import
job uses the identical image. Both public health endpoints returned HTTP 200.
No additional production service was created; schedules were preserved.

Warm-only execution `ibkr-flex-import-j42t9` succeeded in 28.58 seconds using the
saved successful IBKR import. It prepared schema-8 snapshot
`ibkr_flex:2026-10-01:307b58244cceabf7fc13c449a8394282`. Its full historical monthly series matches the independent
raw-execution replay. Production capital coverage is complete, and historical
average/peak capital, stock realized events and dividend cashflows match the
preceding production state. September RoAC is 1.4558447595% and RoPC 1.1971136222%.

Authenticated live mobile requests verify yearly/monthly totals, all open
balances, zero booked-premium fields and November's [private reconciliation amount] projected net
balance. Live web Performance and Monthly screens display the same rounded
figures. The browser remains on Performance for user review; screenshot and
accessibility evidence are in the release folder.

Both embedded and standalone Decision Lab views display comparison candidates,
74 stored contracts dated September 30 and 80/80 ready selections. Existing
liquidity filters still exclude unsuitable quotes. Browser error logs are empty.
Provider control remained at 80 credits used and 20 remaining across warm-up,
deployment and live checks. No provider download was requested.

Production verification completed at `2026-10-01T20:58:11.318335+00:00` (UTC). The
corresponding JSON evidence is `tmp/completed-roll-chain-release/production-verification.json`.
