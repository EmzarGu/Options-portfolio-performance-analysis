# April 2026 Option P&L Reconciliation

This note reconciles the April 2026 `realized_options_pnl` difference between
the Google Sheet pipeline and the IBKR/mobile pipeline as of 2026-05-10.

Monthly option P&L should remain lifecycle-date based:

- realized on close, expiration, assignment, or valid roll close date
- not on the original option opening date
- not double-counting roll replacement opening premium when already included in
  realized roll close economics

## Summary

| Source | April 2026 realized options P&L |
| --- | ---: |
| Google Sheet pipeline | 3716.60 |
| IBKR/mobile pipeline | 5542.66 |
| Difference, IBKR minus sheet | 1826.06 |

## Main Ticker Differences

| Ticker | Sheet April option P&L | IBKR April option P&L | Difference | Classification | Explanation |
| --- | ---: | ---: | ---: | --- | --- |
| CCJ | 0.00 | 1284.38 | 1284.38 | Sheet periodization/manual roll row | IBKR has 200 assignment-derived shares from 2026-03-20 CCJ 105P assignment. The 2026-03-23 110C covered calls are valid wheel calls. April close/roll events on 2026-04-08 and 2026-04-24 are valid lifecycle realizations. The sheet carries the same economics in one 2026-03-23 row with comment `Moved from April 110, moved from May 115`, so the sheet is not April lifecycle-date based for this chain. |
| NVDA | 0.00 | 214.11 | 214.11 | Sheet periodization/manual roll row | IBKR has assignment-derived NVDA shares from the 2025-03-21 put assignment. The 2026-03-25 192.5C covered call and April 2026 roll closes are valid wheel calls. The sheet carries this chain in one 2026-03-25 row for the final 210C with comment `Moved from April, $192.5, moved from May 200`, so sheet April is missing the lifecycle-date close/roll recognition. The small difference versus the sheet row is commission/rounding. |
| AAPL | -320.20 | 0.00 | 320.20 | Sheet defers prior roll economics to assignment month | The sheet has one 2025-08-25 AAPL 230C row expiring 2026-04-17 with `total_pnl=-320.20` and assignment comment. IBKR does not drop the loss: the same chain sums to -320.20107 across roll close dates 2025-08-25 (-117.40569), 2025-11-17 (-367.39459), and 2026-02-03 (+164.59921). The 2026-04-17 assignment option event is 0.00 because the roll-adjusted replacement had no remaining unrecognized option premium; the related stock sale is stock-side P&L, not option P&L. |

## Conclusion

No backend accounting logic change is indicated by these three differences.
IBKR/mobile is applying the intended lifecycle-date rules. The sheet rows are
economically useful manual roll-chain summaries, but they periodize some roll
chains by the manual row/opening date or final assignment date rather than by
the actual close/roll lifecycle dates.
