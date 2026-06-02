# April 2026 Option P&L Reconciliation

This note reconciles the April 2026 `realized_options_pnl` difference between
the Google Sheet pipeline and the IBKR/mobile pipeline as of 2026-05-10.

Monthly option P&L should remain lifecycle-date based:

- realized on close, expiration, assignment, or valid roll close date
- not on the original option opening date
- not moving replacement opening premium into an earlier close; replacement
  premium remains open until the replacement option closes, expires, or is
  assigned

## Summary

Historical note: this reconciliation was produced before the June 2026 roll
accounting cleanup. Use `docs/ibkr-accounting-rules.md` as the current source of
truth for roll replacement premium treatment.

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
| AAPL | -320.20 | 0.00 | 320.20 | Sheet defers prior roll economics to assignment month | The sheet has one 2025-08-25 AAPL 230C row expiring 2026-04-17 with `total_pnl=-320.20` and assignment comment. This row-level reconciliation was generated under the prior roll-netting model. Under the current canonical model, each close realizes only the closed lot economics and each replacement opening premium remains open until that replacement closes, expires, or is assigned. Rerun this historical reconciliation before using the row-level AAPL values for current accounting decisions. |

## Conclusion

This is a historical reconciliation note. Current backend accounting follows
the canonical lifecycle-date rules in `docs/ibkr-accounting-rules.md`: close
economics and replacement opening premium are separate facts, and replacement
premium is not realized early merely because the trade was part of a roll.
