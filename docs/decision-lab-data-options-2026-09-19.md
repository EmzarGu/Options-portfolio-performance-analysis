# Decision Lab: functionality and alternative data sources

Reviewed 2026-09-19 after the production refactor. The user accepts delayed data;
real-time delivery is not required. This is an investigation and proposed direction,
not authorization to change providers, scoring, refresh behavior or subscriptions.
Application code and production configuration were not changed during this review.

Update, 23 September: an authenticated [free-account pilot](decision-lab-marketdata-free-pilot-2026-09-23.md)
returned prior-close quotes with delta on default chain requests. This supersedes
the untested free-tier uncertainty below; field timing and full coverage remain
open before adoption.

## What the module actually does

| Function | Current inputs | Need for a replacement options feed |
| --- | --- | --- |
| Portfolio situations | Existing stock inventory, open options, cost basis, current underlying prices and realized/unrealized results | No additional option chain needed to classify the situation |
| Active-cycle context | Canonical dashboard/monthly projection | No new feed; reuse existing accounting |
| Candidate comparison | Existing positions plus available option strikes, prices and delta | Yes; this is the main provider-dependent function |
| Historical strike quality | Imported historical probability matches and attributed portfolio outcomes | Preserve existing records; a current chain does not supply entry-date probabilities |
| Historical enrichment | Contract identity and option close/VWAP/volume on the original trade date | Separate optional backfill; not a prerequisite for restoring current candidate comparison |

The situations include covered-call recovery, call roll/exit comparison, short-put
assignment-risk reduction and rolling up a far-out-of-the-money put. Candidate
construction requests the current option expirations plus the next two standard
monthly expirations, for the relevant put/call side. Proposed contracts are
restricted to 7–75 days to expiry. This is a portfolio-specific search, not a scan
of the entire US options market or a trading execution system.

Sell proceeds prefer bid; closing cost prefers ask; a provider mark is used when
quotes are unavailable. Rolls are assessed as packages including the current leg.
The score uses lifecycle outcome comparisons, liquidity and time to expiry. Delta
is used as a bounded exercise-probability proxy; the feed is not supplying a
calibrated probability of profit. The current result list retains the existing
position first when a baseline exists, followed by up to two proposals; without
a baseline it retains up to three proposals.

## Required fields and their actual role

| Data | Current use and minimum requirement |
| --- | --- |
| Underlying symbol, option symbol, put/call, expiration, strike | Join contracts to current positions and candidate requests; both current and proposed legs must be present |
| Bid and ask | Preferred price path; checks positivity, ordering and spread width; used for sell/open versus buy/close economics |
| Mark or dated close/VWAP | Existing alternative price path; can support indicative candidates if liquidity and other gates pass |
| Delta | Required for every proposed actionable contract; also used for risk limits and outcome weighting |
| Underlying stock price | Already available from the dashboard; provider value is a fallback. Its timestamp must be considered alongside delayed option prices |
| Open interest and volume | Affect liquidity ranking; at least OI 20 or volume 5 is required on the mark-only path. A latest trade size is not daily volume |
| Implied volatility | Displayed when present; missing IV does not block current candidate construction |
| Gamma, theta, vega | Representable in stored contracts but not consumed by the inspected Decision Lab candidate ranking |
| Quote/mark date, Greek date if available, retrieval time, provider and feed type | Needed to distinguish a dated market observation from a newly downloaded old record |
| Contract size/deliverable and currency | The calculations assume standard 100-share contracts. Adapters must exclude unsupported adjusted contracts rather than silently treating them as standard |

Local controlled checks confirmed: a complete fixture produces a candidate;
removing delta removes it; removing IV does not; a mark with adequate OI/volume
can produce an indicative candidate; removing both OI and volume blocks that path.
The 26 existing Decision Lab tests passed. No provider account was authenticated
or new vendor response tested during this investigation.

Code evidence:

- [Assembly and situation analysis](../portfolio_backend/decision_lab.py)
- [Requests, eligibility, roll economics and scores](../portfolio_backend/decision_lab_candidates.py)
- [Provider interface, stored-data reuse and fetch budget](../portfolio_backend/option_market/decision_data.py)
- [Normalized contract fields](../portfolio_backend/option_market/models.py)
- [Historical enrichment](../portfolio_backend/option_market/history.py)
- [Web orchestration and provider construction](../portfolio_backend/web_data_service.py)

## Provider shortlist

Published prices are USD, before any taxes or additional infrastructure/account
costs. Field availability is based on official documentation, not an authenticated
coverage or reliability pilot.

### Market Data Starter: strongest straightforward delayed-quote fit

Published price is [private reconciliation amount] month-to-month, or [private reconciliation amount]/month with annual commitment
([private reconciliation amount]/year). It includes 10,000 daily credits and 15-minute delayed options.
[Pricing](https://www.marketdata.app/pricing/).

Use its option-chain endpoint filtered by ticker, expiration, side and a suitable
strike range; map its symbols, bid/ask, delta, IV, volume, OI and timestamps into
the existing contract model. Explicit historical `date=` chains return null Greeks
and IV. Therefore a historical lookup is not interchangeable with a delayed current
chain for this app.
[Chain documentation](https://www.marketdata.app/docs/api/options/chain/).

Free/trial access is historical-only, and the options session rollover can lag
through weekends or until the next session opens. Do not assume the free tier has
usable delayed delta merely from its pricing-table wording. Verify the exact
free-plan response before treating it as a viable candidate source.
[Freshness rules](https://www.marketdata.app/docs/account/data-freshness/).

Ordinary chain access charges by returned contract, not just HTTP request. The
single simultaneous IP policy needs checking against Cloud Run's concurrent
instances and any local access. A static IP solution may add cost and should not
be assumed necessary without checking the actual permitted usage pattern.
[Plan limits](https://www.marketdata.app/docs/account/plan-limits/).

Paid cached mode can return a chain for one credit per request, with a maximum-age
filter and an explicit empty-cache response. It has no guaranteed freshness and is
not available on Free/Trial. This is a useful cost optimization after validating
coverage, not a replacement for a freshness policy.
[Data modes](https://www.marketdata.app/docs/api/universal-parameters/mode/).

### Alpaca Indicative: best free standalone API pilot

Its free feed contains modified indicative quotes and delayed derivative trades;
it is not simply an older copy of the actual OPRA bid/ask. Historical coverage starts
in February 2024.
[Feed definitions](https://docs.alpaca.markets/us/docs/historical-option-data).

Fetch filtered option-chain snapshots with `feed=indicative`; the documented
endpoint returns trade, quote and Greeks and supports expiration/type/strike
filters and pagination. Actual delta completeness needs a pilot.
[Chain endpoint](https://docs.alpaca.markets/us/reference/optionchain).

Add contract metadata for size, OI and OI date. Daily volume would need an
appropriate aggregate rather than snapshot trade size; a coherent underlying price
also needs to be supplied.
[Contract metadata](https://docs.alpaca.markets/us/docs/options-trading).

This is suitable to evaluate for indicative planning, subject to account/API
eligibility. It requires explicit agreement on quote quality: the current engine
labels any bid/ask pair as quote-backed and uses the spread in its ranking. Simply
inserting modified quotes would give misleading provenance. Preserve the feed type
and label estimated roll economics clearly; approve any scoring treatment separately.

### EODHD / UnicornBay: end-of-day completeness at a higher price

The product lists EOD bid/ask, volume, OI, IV, Greeks, contract identifiers and
historical coverage of roughly two years. Its prose advertises [private reconciliation amount]/month for the
first three months instead of [private reconciliation amount] while other price labels on the page are
inconsistent. Budget for [private reconciliation amount] ongoing and confirm the actual checkout terms.
[Product, fields and pricing](https://eodhd.com/marketplace/unicornbay/options).

Use its filtered EOD dataset to select one completed session for current and
replacement contracts, then obtain the underlying close for the same session.
This is a good functional fit if daily snapshots and historical Greek research
matter, but it does not beat the cheaper delayed-quote subscription. Publication
timing, target-ticker coverage and quote completeness remain pilot questions.

### Tradier: no API fee, conditional on a brokerage account

API access has no separate fee for brokerage account holders.
[API FAQ](https://docs.tradier.com/docs/faq).
Its brokerage feed provides US options and hourly Greeks; the 15-minute delayed
sandbox explicitly has no Greeks.
[Data availability](https://docs.tradier.com/docs/market-data).

An adapter could fetch expiration chains with Greeks enabled and normalize them
directly. This is attractive if the user already has, or wants, a qualifying
Tradier account. Residency eligibility, funding and account fees have not been
verified; opening another brokerage relationship only for data is a separate choice.
The sandbox alone does not preserve current candidate functionality.

### IBKR: potentially cheap data, more operational work

TWS supports free delayed/delayed-frozen requests; frozen live data requires the
corresponding live subscriptions.
[Market-data types](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-delayed/introduction).
Current Greek documentation also flags the underlying market-data subscription
requirement. Do not promise a complete free Greek feed without an account-specific
test.
[Options Greeks](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/option-greeks/request-options-greeks).

This would be a new TWS/IB Gateway collector, not an extension of Flex reports.
It could upload occasional snapshots to Firestore, but would need a running host
and authenticated session. IBKR documents manual weekly reauthentication.
[Session operation](https://www.interactivebrokers.com/docs/tws-api/doc/tws-settings/daily-weekly-reauthentication).
Use a read-only integration. Exact account entitlements and total operating cost
remain unverified; for this Cloud Run app it is less convenient than a REST feed.

### Lower-priority alternatives

- **Massive Starter, [private reconciliation amount]/month:** snapshot/Greeks/IV/OI are listed; bid/ask quotes
  are listed only in Advanced. A close/aggregate-based adapter might fit the
  existing indicative path after validating mark age, matching Greek timing and
  plan entitlements. Free Basic does not list the snapshot/Greeks needed here.
  [Options plans](https://massive.com/pricing?product=options).
- **Yahoo via yfinance:** the chain exposes prices, volume, OI and IV, but no delta.
  Computing delta ourselves would add model assumptions and validation work, which
  is a functional change. The project is unofficial and documents personal-use
  limitations. I would not make this the primary replacement for a reliability
  improvement. [Returned fields](https://github.com/ranaroussi/yfinance/blob/main/yfinance/ticker.py),
  [project status](https://github.com/ranaroussi/yfinance/blob/main/README.md).

## A proportionate data plan

Recommendation for agreement: use one coherent daily session snapshot, or refresh
15-minute delayed data when the user explicitly requests it. No streaming service
is needed. Retain valid data across page loads and display its market observation
time. Do not refresh old timestamps to the download time.

Fetch current open contracts plus relevant strikes for the two candidate monthly
expirations. Do not filter away the current leg by applying candidate delta limits
to the whole request. Do not restrict strikes solely around the stock price:
recovery calls may be anchored around the holding's cost basis. Exclude nonstandard
deliverables until the calculations support them explicitly.

Illustrative ordinary-chain usage, not a measured current portfolio count:
10 tickers × 2 expirations × 15 strikes on one side = 300 contract observations per
refresh, plus current-leg and underlying lookups. That exceeds a 100-credit daily
free allowance but is modest within 10,000 credits. Paid cached requests may cost
much less; their missing/stale-chain coverage must be measured. A final cost estimate
should use the actual candidate universe and observed responses.

A daily archive of newly collected Greek-bearing snapshots could support future
historical analysis. It does not recreate Greeks at old trade dates. Preserve the
existing imported historical probabilities and successful enrichments independently
of any new current-chain provider. Fixing the historical job is a separate decision.

## Changes to agree before implementation

1. Select feed quality: delayed exchange bid/ask versus modified indicative prices.
   The user has accepted delay; that does not automatically accept modified prices.
2. Select budget and collection pattern: daily snapshot versus on-demand delayed
   refresh. Agree maximum acceptable age in trading sessions, including weekends.
3. Add a provider adapter and explicit provider configuration, preserving stored
   source provenance. Historical loading currently hardcodes CuteMarkets separately
   from current-chain construction and should retain access to the old records.
4. Add generic quote/Greek/underlying timestamps and delayed/indicative labels.
   The current seven-day age check applies only to specific close/VWAP marks;
   ordinary quoted contracts are not uniformly age-checked. Stored successful chains
   can be reused without an age-based refresh. A new daily policy changes behavior
   and must be agreed rather than implied by changing the vendor.
5. Keep comparisons coherent. The engine currently prefers the dashboard stock
   price over a contract's underlying price, so EOD options can be mixed with a
   newer stock price. Agree how the candidate view uses a dated underlying snapshot
   without altering the canonical portfolio accounting.
6. Address the previously reproduced partial-refresh overwrite and derived-cache
   publication errors before relying on fallback data. Treat pagination as one
   staged retrieval, publish only validated data, bound calls and retries, and retain
   the prior successful generation on failure. The zero-target cache collision also
   remains deferred from the initial review.

## Recommended next decision

If avoiding a subscription is the priority, approve a small Alpaca indicative pilot
first, with explicit recognition that this is a change in quote quality. If preserving
exchange-quote economics with a simple hosted API is the priority, investigate
Market Data Starter's deployment/IP suitability and run a small delayed-data pilot
before any annual commitment. EODHD is the alternative if genuine historical Greeks
become a requirement worth its higher price.

The pilot should cover a recovery call, an existing covered-call roll and a put roll;
verify both legs, delta availability, timestamps, volume/OI, contract size, pagination,
empty chains, failed refresh and estimated credits. No signup, purchase, provider
switch, production refresh or scoring change has been made in this review.

## Follow-up: observed production failure and actual workload

The user's open Chrome dashboard was inspected on 2026-09-19. Decision Lab showed
`Unexpected token 'u', "upstream r"... is not valid JSON` instead of its content.
Cloud Run recorded the corresponding GET `/api/decision-lab` at 19:38:53 UTC,
returning HTTP 504 after exactly 300 seconds on revision `options-roi-web-00119-wnr`.
The application logged completion at 19:45:19 with total elapsed time 386.35 seconds.

The matching Firestore fetch run `decision_cutemarkets_20260919193855_235b1643`
ran from 19:38:55 to 19:45:01 UTC: 18 requested chains, 18 connection timeouts,
zero contracts. Each connection used the provider's 20-second timeout. These are
sequential requests in `load_or_fetch_decision_option_data`, without a shared
deadline or stopping after repeated connection failures. The route waits for the
whole fetch. The browser then calls `response.json()` before checking the status,
so the gateway's text error masks the actual timeout.

The same universe failed with 18 connection timeouts at 17:50 UTC, before today's
production deployment. Failed runs also exist on September 13 and August 12.
The provider failure therefore predates the refactor. Deployment validation did
not exercise this authenticated, uncached failure path; the successful test suite
and unauthenticated health checks did not establish Decision Lab availability.

After the original calculation completed, clicking the page's **Retry** displayed
the action queue and historical tables, with `cutemarkets failed · 0 contracts`.
Every recovery planner still reported no stored call contracts. This is temporary
access to the completed result, not a provider or timeout repair. No provider-refresh
button was pressed, and application code and production configuration remain unchanged.

The actual current universe is 9 tickers × 2 expiries = 18 call chains:
STZ, FLEX, NLR, FUTU, CCJ, SHOP, CSCO, ATI, GLW; expiries October 16 and November 20,
2026. GLW includes an existing October 16 [private reconciliation amount] covered call, which must be priced
alongside its replacement legs. The other eight situations seek recovery calls.
Future put positions still require put-chain support even though this snapshot
only requests calls. A request for a chain must distinguish no listed contracts
from a missing or failed provider response.

For ordinary Market Data chain responses, an illustrative 15 returned strikes
per chain would consume 270 credits per complete refresh. The observed 18-chain
count is real; 15 strikes is an assumption, not measured provider coverage. This
is comfortably within Starter's 10,000 daily credits but exceeds the free plan's
100 credits for completing all groups in a day. Paid cached-mode pricing differs.
The last permitted request may overdraw the quota; this does not make subsequent
chain requests available. [Plan limits](https://www.marketdata.app/docs/account/plan-limits/).

The page also reports historical risk-proxy coverage of only 89/518 short-option
opening trades (about 17%). Restoring current quotes will not repair that historical
gap. The history tables should not be interpreted as a complete portfolio study.

### Proposed reliability repair, independent of provider selection

1. Show stored portfolio situations and historical analysis even when the chain
   provider is unavailable. Keep candidate availability and observation dates explicit.
2. Bound provider work with an overall deadline and stop repeated connection
   failures promptly; move longer refresh work outside the page-loading request.
3. Retain the last successful option snapshot when refresh fails, with its actual
   observation date. Do not mark stale prices as fresh or invent missing candidates.
4. Handle non-JSON HTTP failures and offer a useful retry message.
5. Verify the authenticated first-load path with no option cache and a timed-out
   provider; verify the queue survives, candidates remain unavailable, and a later
   successful refresh restores them. Confirm both integrated and standalone Lab views.

These are a proposed scope for agreement, not deployed changes. Raising the web
timeout alone would still leave users waiting several minutes for zero contracts.

### Revised assessment of provider fit

Market Data Starter remains the strongest documented match for preserving current
automated comparisons at a relatively low subscription cost. Use current delayed
chains, not historical `date=` requests, which omit delta. Alpaca free remains an
indicative research option, not a proven equivalent quote feed: modified quotes
affect premium comparisons and spread-based liquidity checks. Merely changing the
provider name would misclassify their quality in the current app.

No alternative-provider credential names were found in the current process,
project `.env`, or `.streamlit/secrets.toml`; no authenticated alternative-provider
response was obtained. Exact contract availability, usable delta coverage,
timestamps and account entitlements for the 18 groups remain unverified. The
existing fixture checks establish which fields the app needs, not vendor quality.
The purchase/switch decision should depend on a read-only coverage pilot using
those actual groups, plus a representative put roll, and deployment/IP suitability.

### Other ways to meet the original objective

The distinctive objective is portfolio-specific comparison of holding, accepting
assignment/exit, selling a recovery call and rolling an existing option, with
cost basis and lifecycle results visible. Full automatic strike discovery is one
way to support that decision, but is not the only useful implementation.

| Approach | What it preserves | What changes or remains missing |
| --- | --- | --- |
| Daily delayed-chain snapshot | Automated strike discovery and current candidate rules | Requires a suitable provider, coherent observation dates, and reliable background refresh |
| Manual contract comparison in our app | Portfolio context, entered bid/ask or premium assumptions, roll debit/credit, breakeven, capital and expiration scenarios | New input workflow; no automatic chain search. Without entered delta, replace probability-weighted ranking with explicitly conditional scenarios, subject to agreement |
| Our action queue plus IBKR analysis | Portfolio triage here, contract and scenario evaluation in the user's existing broker | Separate workflow; no automatic import into our recovery planner. Broker tools do not reproduce this app's lifecycle accounting |
| Our action queue plus OptionStrat | Visual comparison of covered calls, puts and multi-leg alternatives | Manual strategy setup; free access is limited, and it does not automatically know our portfolio history |

[IBKR Performance Profile](https://www.interactivebrokers.com/campus/trading-lessons/performance-profile-for-options-2/)
supports stock/option profit-and-loss scenarios across underlying prices and dates,
including strategies assembled in Strategy Builder. Using the existing tool for
analysis avoids building a new API collector; available market data still depends
on the user's broker entitlements. No order action is needed for the proposed workflow.

[OptionStrat](https://optionstrat.com/?trk=public_post_share-update_update-text)
offers strategy construction and an optimizer. Its current page advertises
15-minute-delayed OPRA data and limited features for free users; full Live Tools
costs [private reconciliation amount]/month and includes probability, net Greeks and other enhancements.
The free interface is a possible manual companion, not an established free API
replacement or feature-complete substitute for this application.

If full automatic comparison inside the dashboard matters most, pilot Market Data
Starter before committing annually. If zero recurring data cost matters most,
the manual scenario approach is a more transparent proposal than treating an
indicative feed as equivalent to delayed market quotes. Both require agreement
before implementing a provider or workflow change.

## Approved timeout repair

The user approved fixing the timeout after the review above. The repair makes
ordinary Decision Lab page loads read stored option data only, including on a
cache miss; a missing option feed therefore does not block portfolio analysis.
Explicit **Fetch option data** requests share a 30-second provider budget, with
connection attempts capped at five seconds, and stop on a connection failure or
service-wide HTTP failure. Pagination and retry delays use the same budget.
Failed or partial chains are rejected before storing contract data. The existing
candidate calculations and provider selection are unchanged. Both web views show
provider status messages and handle non-JSON gateway errors explicitly.

Verification: 464 Python tests passed, including seven new outage/deadline tests;
both web views' JavaScript syntax checks and four response-handling checks passed.
Production verification is recorded separately in the timeout repair release note.
This repair uses bounded synchronous manual refresh; it does not add the separate
background ingestion service considered in the earlier proposal.
