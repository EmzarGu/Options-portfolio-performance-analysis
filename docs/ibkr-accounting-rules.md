# IBKR Accounting Rules

This document defines how IBKR activity should be interpreted before it is fed
into the portfolio performance pipeline.

The current Google Sheet source is intentionally simplified. It records mostly
short option activity, then derives stock transactions from manual assignment
flags. IBKR is more complete: it contains option trades, option
exercise/assignment/expiration events, explicit stock trades, cash movements,
dividends, and positions. The IBKR importer should preserve the raw facts and
then produce a calculation model with explicit option lots, stock lots, realized
events, and cashflows.

## Current App Model

The active pipeline currently does this:

- A `Sell` option row opens a short option lot.
- A matching `Buy` row closes that short option lot FIFO by ticker, type,
  strike, and expiration.
- A remaining short option lot is realized at expiration if `as_of` is at or
  after expiration.
- If the remaining lot is marked assigned:
  - assigned short put creates `StockTxn(..., side="BUY")` at strike.
  - assigned short call creates `StockTxn(..., side="SELL")` at strike.
- Stock realized P&L is computed FIFO from `StockTxn` rows.
- Dividends are currently fetched from yfinance based on stock holding
  segments.
- Capital denominator uses short put reserve and stock holding value.

The IBKR model should keep these calculation concepts, but source them from
IBKR events rather than manual comments.

## Core Principle

Use IBKR as the accounting source of truth, but scope the dashboard to the
wheel strategy rather than the whole IBKR portfolio:

```text
Short puts can start wheel exposure.
Short calls count only while prior put-assignment stock inventory is held.
Stock P&L comes only from assignment-derived stock that is later sold.
Dividends/cashflows count only during assignment-derived holding periods.
Unrelated stock trades and covered calls on non-wheel shares are excluded.
```

Do not force all IBKR activity into the old Google Sheet row shape if doing so
would lose information.

For dashboard option performance, IBKR mode needs two explicit option views:

```text
option_cashflow_pnl = short option opening credits - close debits - commissions, by execution date
option_lifecycle_pnl = close/expiration/assignment P&L by option lot lifecycle
```

`option_cashflow_pnl` is the appropriate first comparison to the manually
maintained sheet because the sheet often records rolled options as net strategy
cash movements. `option_lifecycle_pnl` is still useful for lot audits, tax-style
realization, and open-lot reconciliation, but it is misleading on its own for
yearly dashboard P&L when an in-the-money covered call is rolled into a future
expiry. In that case IBKR records a large buy-to-close debit and a large
sell-to-open credit; the net roll cashflow, not only the close debit, reflects
the strategy movement for the period.

Short calls require one extra wheel eligibility check. A call execution is part
of wheel option P&L only if assignment-derived shares from an earlier short put
are held on the call execution date. A covered call written against unrelated
shares is excluded, even if IBKR later records a call assignment.

## Case Table

| Case | IBKR evidence | Option P&L handling | Stock P&L / inventory handling | Dividend handling | Current app behavior | Proposed IBKR behavior |
|---|---|---|---|---|---|---|
| Short put sold, expires worthless | `Trade` `assetCategory=OPT`, `buySell=SELL`, `openCloseIndicator=O`; later `OptionEAE` `transactionType=Expiration` or no close by expiry | Realize premium net of commission when expired. If `OptionEAE.realizedPnl` is available, use it as check/source; otherwise use opening net cash. | No stock transaction. | None. | Realizes `open_price * qty * 100` at expiry. | Realize at IBKR expiration date; reconcile calculated premium to `OptionEAE.realizedPnl`. |
| Short call sold, expires worthless | Same as short put with `putCall=C`; plus assignment-derived stock inventory exists before the call | Realize premium net of commission at expiration only if the call is wheel-eligible. | Stock inventory remains unchanged. | Dividends continue only for assignment-derived shares. | Same as put expiration. | Exclude calls with no prior put-assignment inventory. |
| Short put partially/fully bought to close | Opening `SELL/O`; closing `BUY/C` rows same conid or same underlying/type/strike/expiry | FIFO match close quantity to open lots. Realized P&L = open net premium minus buy-to-close cost, including commissions. | No stock transaction. | None. | FIFO closes matching short lots. | Preserve IBKR trade IDs and use `Trade.fifoPnlRealized` as reconciliation where available. |
| Short call partially/fully bought to close | Opening `SELL/O`; closing `BUY/C`; plus assignment-derived stock inventory exists before the call | Same as short put close only if the call is wheel-eligible. | No stock transaction. | Dividends continue only for assignment-derived shares. | FIFO closes matching short lots. | Exclude calls with no prior put-assignment inventory. |
| Short put assigned | Opening short put; `OptionEAE` `transactionType=Assignment`; stock-side `OptionEAE` or `Trade`/position evidence shows stock buy | Option premium is realized. Option close price is zero for option leg; realized option P&L is premium net of fees. | Create/integrate stock buy lot: shares = contracts * multiplier, cost basis = strike adjusted by IBKR stock-side basis if available. | Dividends accrue from holding segment after assignment until stock sale/as_of. | Manual assigned flag creates stock buy at strike. | Use `OptionEAE` as assignment evidence and stock-side row as authoritative stock lot. Keep option P&L and stock cost basis separate, but reconcile to IBKR basis. |
| Short call assigned against assignment-derived held shares | Opening short call; `OptionEAE` `Assignment`; stock-side sell; prior put-assignment inventory exists | Option premium is realized. | Sell assignment-derived shares FIFO at strike or IBKR stock-side proceeds. Realized stock P&L = proceeds minus basis of matched assignment-derived lots. | Holding segment ends at assignment date. | Manual assigned flag creates stock sell at strike; uncovered assigned calls assume zero P&L for missing inventory. | Use explicit stock-side sell. If insufficient assignment-derived inventory exists, exclude/flag the unmatched portion as non-wheel. |
| Short put quantity 4 assigned, later covered calls quantity 2 + 2 at different strikes | Assignment creates one stock buy of 400 shares or equivalent stock-side rows; later short call openings at different strikes/expiries | Each call lot tracks its own premium and lifecycle. | One stock inventory pool per ticker, FIFO lots. Covered-call exposure consumes available shares for unrealized cap/risk display but does not split stock basis unless shares are sold. | Dividends apply to full held shares while held. | Stock lot created from put assignment; open calls cap unrealized stock P&L. | Same concept, but stock inventory comes from IBKR assignment stock-side row. Covered-call matching should be exposure matching, not realized P&L matching until assignment/sale. |
| Covered call expires worthless | `SELL/O` call; `OptionEAE Expiration` or no close by expiry | Realize call premium. | Stock inventory remains unchanged. | Dividends continue if stock still held. | Realizes option premium; stock lot remains. | Same. |
| Covered call bought to close | `SELL/O` call; `BUY/C` call | Realize option P&L from premium minus close cost. | Stock inventory unchanged. | Dividends continue. | Same for option lot. | Same; use IBKR close execution and realized P&L reconciliation. |
| Covered call assigned partially | One or more call lots assigned; stock-side sell less than total holdings | Realize assigned call premium. | Sell matched shares FIFO. Remaining stock lots stay open. | Dividends stop only for sold shares after assignment date. | Handles assigned call as stock sell, FIFO. | Same, but stock-side sell from IBKR is authoritative for quantity/proceeds. |
| Roll short call up/out for credit | `BUY/C` old option plus `SELL/O` new option, possibly same order/combo | Old lot closes with realized gain/loss. New lot opens independently. Net credit/debit is not one blended P&L item for accounting; display can group as roll later. | Stock inventory unchanged. | Dividends continue. | If rows are separate, close/open works. Mixed rows rely on manual comment parsing. | Treat as two option events. Use `relatedTradeID`, order IDs, or same timestamp only for optional strategy grouping, not accounting. |
| Roll short call up/out for debit | Same as above | Old lot closes with realized loss; new lot opens with new premium. | Stock inventory unchanged. | Dividends continue. | Same if represented as separate rows. | Same. |
| Partial roll | Close part of old lot and open smaller/larger new lot | FIFO close only the closed quantity. Preserve remaining old lot. New lot opens independently. | Stock inventory unchanged. | Dividends continue. | Partial closes preserve remaining lot. | Same; IBKR quantities should make this explicit. |
| Roll short put down/out | `BUY/C` old put plus `SELL/O` new put | Old put realized; new put opens. | No stock transaction unless later assigned. | None until stock exists. | Same if rows separate. | Same. |
| Short put assigned then stock later sold manually | Put assignment stock buy plus later `Trade` `assetCategory=STK`, `buySell=SELL` | Put premium already realized at assignment. | Stock sale realizes stock P&L FIFO. | Dividends included for holding period. | Can handle only if sale comes from assigned call or manually represented stock flow; sheet source does not generally load stock sells. | Use explicit IBKR stock sell. This is required for reliable accounting. |
| Manual stock buy followed by covered call | `Trade` stock buy; later short call `SELL/O` | Exclude from wheel option P&L. | Exclude from wheel stock P&L. | Exclude dividends. | Current source usually lacks independent stock buys unless derived. | Preserve raw activity, but do not include in wheel performance unless a separate covered-call strategy view is added. |
| Manual stock sell not linked to option | `Trade` stock sell | No option P&L. | Realize stock P&L FIFO. If insufficient known basis, use prior-period positions or flag. | Holding segment ends for sold shares. | Limited/unsupported unless represented indirectly. | Use explicit stock sell; flag missing basis. |
| Protective long put/call bought | `BUY/O` option | Not part of short premium strategy. It should not be fed into current short-lot option pipeline as a close. | No stock transaction. | None. | Current code ignores standalone long buys if no matching short sell exists. | Preserve in raw/normalized transactions. Exclude from short-option performance unless we add long-option strategy accounting. |
| Long option sold to close | `SELL/C` option | Realize long-option P&L only if long-option strategy support is enabled. | No stock transaction. | None. | Not modeled. | Preserve raw. Exclude from initial short-option strategy metrics, but track as unsupported/ignored amount for reconciliation. |
| Multi-leg spread or collar | Multiple `Trade` rows, often same order/timestamp; sheet sometimes used `Put/Call` manual comments | Accounting should be per leg. Short legs enter short-option strategy. Long legs are either hedges or separate strategy legs. | Stock handling independent. | Dividends from stock holdings only. | Current parser tries to infer the short leg from comments and ignores standalone long legs. | Use IBKR per-leg rows. Initially include only short legs in existing strategy metrics; preserve long legs for future spread/collar analytics and reconciliation. |
| Corporate action affecting held stock or option contract | `CorporateActions`, `SecurityInfo`, changed conid/multiplier/strike | Adjust contract identifiers, multiplier, strike, or basis using IBKR data. | Adjust stock lots/basis according to IBKR action rows. | Cash in lieu/dividends from cash rows. | Mostly unsupported/manual. | Preserve and flag first; implement specific actions only when encountered. |
| Dividend paid on held stock | `CashTransaction` type `Dividends`, `Payment in Lieu`, withholding rows | No option P&L. | No stock realized P&L. | Add cash dividend net/gross based on chosen convention. | yfinance estimates dividends from holding periods, may miss withholding/timing. | Prefer IBKR `CashTransaction` rows for actual dividends and withholding. |
| Withholding tax on dividends | `CashTransaction` type `Withholding Tax` or `871(m) Withholding` | No option P&L. | No stock P&L. | Either reduce dividend cash or report separately. | Not handled directly by yfinance. | Recommended: net dividends in total realized P&L, with gross/withholding fields available for reporting. |
| Interest, fees, taxes | `CashTransaction`, `TransactionFees`, `Interest*` | Do not mix into option premium P&L unless directly tied to trade commission/tax. | Not stock P&L. | Separate cashflow category. | Mostly ignored except option commissions. | Store and classify separately. Decide later whether total portfolio P&L includes these. |
| Deposit/withdrawal/transfer cash | `Transfers`, `CashTransaction`, `CashReport` | No trading P&L. | No trading P&L. | Cash movement only. | Not modeled. | Store for NAV/capital reconciliation, exclude from trading P&L. |
| Prior positions before import window | `PriorPeriodPosition`, `OpenPosition`, first available stock trade sells more than known lots | No option P&L unless open options exist. | Need opening stock lots and basis for correct future realized stock P&L. | Dividends from known holding periods if shares held. | Current app starts from sheet history; pre-owned shares on assigned calls assume zero P&L for uncovered portion. | Use prior/open positions to seed lots where possible; otherwise flag missing basis. |

## Recommended Calculation Layers

The IBKR implementation should not stop at a sheet-shaped option DataFrame.
Instead, build a transaction model with these outputs:

```text
OptionTrade rows -> option lot lifecycle
OptionEAE rows -> expiration/assignment/exercise confirmation
Stock Trade rows -> stock buys/sells
Stock-side OptionEAE rows -> assignment/exercise stock movement
CashTransaction rows -> dividends, withholding, interest, fees
OpenPosition/PriorPeriodPosition rows -> opening state and reconciliation
```

Then the existing performance pipeline can be extended in a controlled way:

```text
build_base_pipeline(..., stock_txns_override_fn=None, dividend_cashflows=None)
```

Default Google Sheet behavior remains unchanged. IBKR mode now passes explicit
wheel stock transactions derived from stock-side `OptionEAE` rows and actual
IBKR dividend cashflows.

## Matching Rules

Recommended defaults:

1. Option lots are matched FIFO by account, underlying, put/call, strike,
   expiration, and multiplier.
2. Prefer IBKR `conid` for exact option identity when available.
3. Rolls are not accounting events by themselves; they are close old lot plus
   open new lot. Grouping rolls is a reporting layer.
4. Stock lots are matched FIFO by account and ticker/conid, but wheel dashboard
   stock lots are seeded only from short-put assignment stock-side rows.
5. Assignment/exercise stock movement should be sourced from stock-side
   `OptionEAE` rows when available; otherwise derive from option EAE quantity,
   strike, and multiplier.
6. Commissions/taxes directly attached to a trade reduce that trade's realized
   P&L.
7. Dividends should use actual IBKR cash transactions rather than yfinance once
   stock holding segments are sourced from IBKR.
8. Cash deposits/withdrawals are capital flows, not trading P&L.
9. Dashboard option P&L in IBKR mode should use execution-date option cashflow
   unless the UI explicitly labels a value as lot-lifecycle realized P&L.
10. Dashboard call option P&L is included only while assignment-derived shares
    from a prior short put are held. Covered calls without that prior put are
    preserved in raw storage and excluded from wheel performance.

## Open Decisions

These should be agreed before Firestore import becomes the source of truth:

| Decision | Recommended default |
|---|---|
| Include long options in strategy P&L? | No for first cutover; preserve raw and report ignored unsupported rows. |
| Use gross or net dividends? | Net dividend cash in realized P&L; keep gross/withholding fields separately if IBKR provides them. |
| Include interest and account fees in dashboard P&L? | Store separately; exclude from option strategy P&L at first. |
| How to seed positions before import window? | Use IBKR four-year backfill as the authoritative baseline. For positions opened before `2022-05-09`, seed from IBKR prior/open positions where possible and flag missing basis. |
| Roll grouping in UI? | Later reporting feature; accounting remains per close/open leg. |
| What if IBKR realized P&L disagrees with calculated FIFO? | Store both; use calculated model for continuity initially, then decide whether IBKR realized P&L becomes authoritative. |
