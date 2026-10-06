# Decision Lab: local Market Data integration and broader testing

Subsequent authorized deployment: see the [production release record](decision-lab-marketdata-production-2026-09-23.md).
The local-only statements below describe the earlier evaluation stage.

Completed locally on 23 September 2026. No deployment, production configuration,
Firestore write, subscription change, or scoring change was made.

## Result

The free account is technically usable for a bounded portfolio-specific planning
workflow. The integration now feeds normalized, saved Market Data contracts into
the existing `build_decision_lab_data` function. It is an opt-in local evaluation
runner; the production web and mobile provider factories still use CuteMarkets.

The remaining adoption issue is field timing, not API access. Repeated default
GLW responses matched exactly, but the discrepancy between default and explicit
historical fields from the original pilot remains unexplained. Greek timestamps
are unavailable. All local outputs explicitly retain `production_ready=false`
and `field_timing_verified=false`.

## Live coverage and calculation results

The input came from the persisted dashboard snapshot prepared on 23 September at
17:45 UTC, based on the IBKR import through 22 September. Reading the snapshot did
not rebuild or modify production. Its base state contains no stock-price overlay;
the local evaluation used underlying prices from the dated Market Data responses,
recorded separately from the portfolio source date. These are evaluation inputs,
not a claim that the normal dashboard now uses this provider.

- **17/17 active portfolio tickers** returned an option contract and non-null delta
  in the initial coverage pass: AEM, ANET, ATI, AU, CCJ, CSCO, FLEX, FUTU, GLW, HPE,
  NLR, NVDA, NVT, OKTA, SHOP, STZ and TECK. This sampled the relevant side and a
  current leg, or one call where no current leg existed; it was not a full-chain scan.
- The existing classification rules identified **11 Decision Lab situations**.
  The other six put tickers did not trigger a current situation under those rules.
- The complete bounded plan covered **68 selections**, returning **60 unique
  contracts** for those 11 situations. Four selections returned no data; others
  overlapped and were deduplicated. Every retained contract had bid, ask and delta.
- All quote observation dates were **22 September 2026**. Non-null quotes and delta
  do not automatically qualify a contract: existing spread, premium, strike and
  delta rules still apply.
- The original pilot plus this work consumed **89 of 100 credits** according to
  the final provider allowance: **11 remained**. The local conservative reserve
  can report less headroom after zero-charge responses. No paid service was used.

| Ticker | Unique contracts | Comparison rows | New proposals |
| --- | ---: | ---: | ---: |
| STZ | 6 | 0 | 0 |
| NLR | 3 | 0 | 0 |
| FUTU | 5 | 0 | 0 |
| SHOP | 6 | 2 | 1 |
| FLEX | 6 | 2 | 1 |
| GLW | 4 | 3 | 2 |
| CCJ | 6 | 3 | 2 |
| CSCO | 5 | 3 | 2 |
| ATI | 6 | 3 | 2 |
| NVT | 7 | 1 | 0 |
| NVDA | 6 | 3 | 2 |

Comparison rows include the existing-position baseline. NVT retained only that
baseline. STZ, NLR and FUTU produced no proposals within the sampled universe;
the existing rules rejected quotes/strikes for reasons including zero bids,
wide spreads or a strike below basis. This does not establish that no suitable
option exists elsewhere in their chains, and these results are not trade advice.

Re-reading the saved selections and recalculating took **5.94 ms locally**, returned
identical candidate rows, and made zero HTTP calls. Re-running the explicit
refresh also reused the current saved selections and left credit use unchanged.
This is a local calculation/cache measurement, not production page-load timing.

## Implementation and safety checks

Validation: **42 new adapter/integration tests** and **525 tests across the full
repository** passed in the local Python environment. The full suite took about
four seconds. These checks do not prove production deployment or long-term feed
accuracy.

- [Provider adapter](../portfolio_backend/option_market/marketdata.py): fixed host,
  bearer authentication, bounded single-strike or single-delta queries, no redirect
  following or automatic retry, response identity/column checks, credit reservation
  before sending, and account allowance reconciliation.
- [Local integration](../portfolio_backend/option_market/marketdata_local.py): exact
  current legs, two delta targets per relevant expiry, basis-strike probes, symbol
  deduplication, separate observation/retrieval times, and saved responses. Unlisted
  strikes return a normal empty result. Failed/stale refreshes retain prior good
  responses. Corrupt cache files are reported and repaired only during refresh.
- [Runner](../scripts/evaluate_marketdata_decision_lab.py): exclusive local run lock,
  private output files, cache-only default, explicit refresh, and a persistent daily
  credit ledger. It does not import or instantiate a Firestore store.
- [Tests](../tests/test_marketdata.py): provider failures and missing credentials or
  accounting, quota exhaustion, malformed/mismatched data, stale quotes, cache reuse,
  holidays/weekend/session-open boundaries, and call/put roll economics. Controlled
  put fixtures are separate from actual portfolio positions. Removing proposal
  delta blocks the roll without changing existing scoring.

The local age gate accepts only the most recent session available to a free account.
It accounts for the next-session-open rollover, including weekends and holidays.
This is a conservative evaluation policy; production labels and age policy still
need agreement. Historical entry-date Greek reconstruction remains outside scope.

## Reproduce locally

Use the same root directory for a shared credit ledger. The token must stay in a
private ignored file or `MARKETDATA_API_TOKEN`, never in a command-line value.
The existing ignored portfolio input and evidence are under
`tmp/marketdata-integration/`. They are not bundled as public fixtures.

```bash
# Offline: saved observations only; no credential needed.
PYTHONPATH=. .venv/bin/python scripts/evaluate_marketdata_decision_lab.py \
  --payload tmp/marketdata-integration/portfolio-input.json

# Explicit bounded refresh: existing up-to-date selections are reused.
PYTHONPATH=. .venv/bin/python scripts/evaluate_marketdata_decision_lab.py \
  --payload tmp/marketdata-integration/portfolio-input.json \
  --refresh --token-file tmp/marketdata-pilot/.env --daily-limit 90
```

On a new ledger day, use `--initial-used` to account for known credits consumed
outside this runner. A local file is not a distributed quota controller; any later
Cloud Run integration must coordinate fetching and budgeting centrally. The free
allowance fits this bounded once-per-session workflow, with little room for repeated
broad scans. Current results do not establish long-term provider reliability.

Evidence: `broader-smoke.json`, `live-evaluation.json`, `verification.json`,
`cache-reuse-summary.json`, `credits.json`, and `responses/` in the local evidence
directory. Application behavior remains unchanged until a production switch is
agreed. Before that switch, resolve the field-timing uncertainty and agree dated
planning labels and background refresh behavior.

Sources: [free-plan freshness](https://www.marketdata.app/docs/account/data-freshness/),
[chain fields and historical Greek limits](https://www.marketdata.app/docs/api/options/chain/),
[404/no-data behavior](https://www.marketdata.app/docs/sdk/js/client/).
