# Decision Lab new cash-secured puts — 15 October 2026

The Lab previously managed open positions only. It now also suggests new
cash-secured puts for a short watchlist, following the wheel entry rules:
0.15–0.25 delta, 20–45 DTE and about 1–2% a month of premium on collateral.

## Configuration

Set `DECISION_LAB_PUT_WATCHLIST` to a comma- or space-separated list of tickers
on **both** the web service and the import job. The two must match, because the
job prepares the same option selections the page reads. The list stays in
runtime configuration, not in this public repository. With no list, the Lab
behaves as before.

At most 10 tickers are used. Tickers that already have an open short option,
assigned shares or a Lab situation are skipped, so a name drops out of this
section once a put is sold.

## Selection and scoring

- One standard monthly expiry per ticker: the one in the 20–45 DTE window
  closest to 35 DTE.
- Three Market Data selections per ticker, at 0.15, 0.20 and 0.25 delta. Ten
  tickers add about 30 selections per quote date to the existing plan.
- A contract must pass the existing quote, spread, liquidity and minimum-premium
  checks. It must also have 20–45 DTE and a returned delta between 0.12 and 0.28
  (delta selection is nearest-neighbour).
- **Return per month** = (bid × 100 − estimated $1 commission) ÷ (strike × 100)
  × 30 ÷ DTE. This is a premium yield on collateral for comparing strikes, not
  cycle-accounting P&L or RoAC.
- The score favours returns inside the 1–2% band and deltas near 0.20, with the
  usual liquidity adjustment. Returns above 2% are labelled for a closer look
  rather than ranked higher.

Ticker selection itself (which names belong on the watchlist) is outside the
Lab; the Stock Rover screener work is the intended source.
