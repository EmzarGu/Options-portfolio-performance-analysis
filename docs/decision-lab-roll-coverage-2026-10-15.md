# Decision Lab roll coverage — 15 October 2026

Most Decision Lab rows showed only the current position, or nothing at all.
Three causes were in the Lab itself rather than in market conditions.

## Changes

- **Expiry window.** Requests used the next two standard monthlies counted from
  today. A position already in the second month had no later month, so no
  roll-out could ever be priced. Each open leg now also requests the first
  monthly after its own expiry (up to the 75-day actionable limit). Months
  before the open leg's expiry are no longer requested, because the roll
  engine never uses them; harvest situations request only the open expiry.
- **Same-strike roll-outs.** The open strike is now priced in every requested
  month from its expiry onward, not only in its own month. Covered-call
  "Roll out same strike" already existed but never had data. Short puts can
  now be rolled out at the same strike when the net credit is at least $25
  and the scenario estimate beats keeping the current put.
- **Near-miss note.** When no alternative passes the filters, the candidate
  status carries `near_miss` and `near_miss_message`: the rejected contract
  nearest 0.25 delta and the rule it failed. Both Lab views show the note.

- **Calls for underwater holdings.** An assigned holding without a covered call
  first looks for calls at or above the cost basis (0.12–0.45 delta, at most 15%
  above basis). When the stock is too far below basis for any such call, the Lab
  now offers calls at or above the current price in the same delta band as
  "Sell covered call below basis". Their exit economics show the stock loss
  realized if the shares are called away. The cost basis is the assignment
  basis from the accounting rules; option premium is not netted into it.

- **Every action-queue row gets options.** "Pause new entries" rows previously
  requested no contracts. They now get the comparison that fits the position
  they hold: put rolls for an open put, call rolls for an open call, or covered
  calls for uncovered shares. The row keeps its category and objective.

## Credit budget

Replaying the selection plan for a saved 16-situation portfolio gave roughly the
same number of Market Data selections as before (97 versus 96 for 6 October;
107 versus 109 for 15 October), because unusable earlier months are dropped.
The 90-credit ceiling, cooldown and time budgets are unchanged.

## Not changed

Accounting, scoring, the delta-weighted scenario model and the recovery
covered-call rules (strike at or above basis, 0.12–0.45 delta) are unchanged.
