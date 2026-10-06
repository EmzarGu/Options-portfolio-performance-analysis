# Market Data free-account pilot

Verified 23 September 2026. This is a read-only integration investigation, not a
production provider change or a trading recommendation.

Follow-up: the [local integration and broader tests](decision-lab-marketdata-integration-2026-09-23.md)
are now complete. That report contains the expanded coverage and final credit use;
the figures below describe only the original 20-credit pilot.

## Conclusion

The existing Free Forever account can return the fields needed for dated Decision
Lab candidate comparisons, including delta. No payment was required. It is a
promising replacement for a small portfolio-specific search, but field timestamp
consistency and coverage across the full portfolio still need validation before
production use. Historical entry-date probability reconstruction is not solved.

## Account and observed requests

- Chrome confirmed an active lifetime Free Forever subscription. Payment history
  showed the Starter trial beginning 24 May 2026 and no confirmed payments.
- The user authorized generating a token. It was delivered by email and stored in
  a local, Git-ignored file with owner-only access. No production secret changed.
- Nine requests at approximately 19:39–19:44 UTC returned HTTP 203 and `s=ok`.
- The API headers and account page both confirmed **20/100 credits consumed**;
  **80 remained**. No further requests were made after the local pilot cap.
- Individual responses took about 0.42–1.86 seconds. This is a small sample, not
  a reliability or production latency benchmark.
- Eight default requests (without `date`) returned 19 rows / 18 unique contracts.
  All had bid, ask, delta, IV, volume, OI, underlying price and an observation time.
- All returned quote timestamps were **22 September 2026 at 20:00 UTC**, the prior
  session close. Retrieval time was kept separate from observation time.
- One explicit historical request for that same date returned null delta and IV.

| Request | Returned rows | Credits |
| --- | ---: | ---: |
| GLW October 190 call | 1 | 1 |
| GLW November 180/190/200 calls | 3 | 3 |
| SHOP October 135/140/145 calls | 3 | 3 |
| SHOP November 135/140/145 calls | 3 | 3 |
| SHOP October 120/125/130 puts | 3 | 3 |
| SHOP November 115/120/125 puts | 3 | 3 |
| GLW October 190 call with explicit historical date | 1 | 1 |
| SHOP October calls: delta .2,.3 plus strikeLimit 3 | 2, duplicate symbol | 2 |
| SHOP October call: delta .2, without strikeLimit | 1 | 1 |

Default requests cost one credit per returned row in this pilot. A duplicate also
consumed a credit. Do not budget by HTTP request count or assume the historical
bulk discount applies to default requests merely because the quotes are old.

## Compatibility with the existing calculations

An offline replay mapped and deduplicated the recorded fields into the existing
contract schema, then called `build_decision_lab_data` without modifying app code.
The portfolio inputs were explicitly synthetic; they were not a current-account
refresh or personalized recommendations.

- A GLW uncovered-stock fixture generated three covered-call candidates.
- A GLW covered-call fixture produced the current-position baseline and two roll
  proposals. Closing cost used the recorded ask; opening proceeds used the bid.
- A SHOP October 130 put fixture produced no situation or proposals: at the
  supplied underlying price it was neither near strike nor sufficiently far OTM
  for the existing situation rules. This does not validate the put-roll branch.
- These checks establish schema compatibility for the exercised paths. They do
  not establish quote executability, Greek accuracy, or full portfolio coverage.

## Findings that constrain adoption

**Filter interaction.** Combining `.2,.3` delta targets with `strikeLimit=3`
returned the same 152.5 call twice, with delta .4408. Removing the strike limit
and requesting the single .2 target returned a 170 call with delta .1827. This
supports a filter-interaction explanation, not a general failure of delta lookup.
Future fetching should use bounded exact-strike requests or single delta targets,
deduplicate symbols and validate the returned delta locally.

**Field timing is not yet reconciled.** The same GLW October 190 call had identical
bid/ask and quote timestamp in default and explicit-date responses, but:

| Field | Default | Explicit 22 September |
| --- | ---: | ---: |
| Bid / ask | 1.83 / 2.12 | 1.83 / 2.12 |
| Underlying price | 159.62 | 159.69 |
| Open interest | 1,114 | 978 |
| Volume | 96 | 68 |
| Delta | .1586 | null |

The provider documents separate settlement timing for historical OI, so differing
OI alone does not prove an error. The underlying/volume differences and absence
of a separate Greek timestamp remain unresolved. Do not silently merge these two
response modes or describe all fields as a verified synchronous snapshot.

**Data age.** Historical-only options roll to the prior session after the next
session opens. A European morning refresh can therefore be two trading sessions
behind. Weekends and holidays require session-aware handling.
[Official freshness rules](https://www.marketdata.app/docs/account/data-freshness/).

**Historical probabilities.** Explicit dated requests omit Greeks on every plan.
Upgrading alone would not restore entry-date delta for historical strike-quality
analysis. Preserve existing historical matches; this pilot addresses current
planning with dated observations.
[Official chain documentation](https://www.marketdata.app/docs/api/options/chain/).

## Proposed next step, requiring provider-change agreement

Build a local Market Data adapter with a daily credit ceiling, saved snapshots,
explicit quote dates, duplicate protection, and existing eligibility/scoring rules.
Fetch only relevant sides, current legs, and a bounded candidate set. For example,
10 tickers × 2 expirations × 3 contracts consumes about 60 credits at the observed
rate, before additional current legs, discovery and retries. This is an illustrative
budget, not measured full-portfolio coverage.

Before deployment, reconcile default-field timing with a repeat observation or
provider clarification, test all actual portfolio symbols and the put-roll path,
and agree the maximum acceptable observation age and user-facing dated-data labels.
Production should read saved results, keeping external calls out of page loads.
No paid subscription is justified by the evidence so far.

Local evidence: `tmp/marketdata-pilot/summaries.json`, individual response records,
`validate_offline.py`, and `offline-validation.json`. These are ignored local
artifacts. The credential is separate and is not included in this report.
