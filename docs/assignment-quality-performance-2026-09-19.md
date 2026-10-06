# Assignment Quality Lab performance redesign

The user authorized a performance redesign while preserving calculations and
functionality. Production previously took 19.72 seconds to answer the Lab request
on revision `options-roi-web-00120-94n`; an earlier request took 19.25 seconds.

## Evidence and changes

A read-only profile used the actual persisted report: 2,033 Trade rows, 1,351
CashTransaction rows and 418 OptionEAE rows. Local measurements were 0.60 seconds
for the report, 1.83 seconds for full history and 0.85 seconds for profiled analysis.
These are local measurements, not an attribution of the production 19.72 seconds.
A separate local quote fetch returned all 15 sampled extra tickers in 0.47 seconds.

The old path reparsed option executions four times within the analysis, rebuilt
positions twice, and separately rebuilt positions to discover the ticker list.
It also requested every history year from 2000 for every assigned ticker. Every
new dashboard price timestamp invalidated the complete calculated result.

The redesigned path separates:

1. **Historical accounting:** reconstruct assignment lots, allocate stock sales
   and option cashflows, and identify open calls once per source report/date.
   Cache the resulting compact JSON in memory and Firestore. Its identity includes
   report identity, query/source version and as-of date, not a price timestamp.
2. **Horizon observations:** read only the years containing mature 6-, 12- and
   18-month evaluation dates. If a requested year lacks an earlier observation,
   fetch prior years back to 2000 to preserve the existing last-known-close rule.
   Persist only the closes needed by those evaluation dates in a separate cache.
3. **Current valuation:** copy the immutable historical accounting, apply current
   prices/open-call caps and produce the same payload. Extra current-price requests
   overlap independent history loading. All existing calculations remain in the
   original analysis module.

Prepared accounting and horizon caches expire after six hours and are scoped to
the as-of date and imported source. Normal price refreshes retain these caches
and revalue the current view. Final response caching still includes the dashboard
price timestamp. New timing logs separate report loading, accounting preparation,
historical observations, extra quotes and valuation. No infrastructure size,
subscription, data provider, formula, UI flow or dependency was changed.

## Validation

- With the actual report and frozen valuation inputs, every field of the new
  payload equals the pre-change payload, including current, cohort and horizon
  detail, summary, covered-call caps, allocations and coverage.
- History reads fell from 729 requested year documents to 28, in one batch.
- Local prepared valuation: 0.03 seconds. First preparation plus targeted database
  history loading: 0.74 seconds, excluding report/current-price fetching already
  captured separately in the profile. These figures are not production promises.
- 473 Python tests passed locally. Nine new regression cases cover price-only
  refresh reuse, process restart/persistent reuse, report/date/age invalidation,
  immutable prepared snapshots, and missing-year/empty-history behavior.
- The existing make wrapper remains blocked by the previously documented Xcode
  license issue; the equivalent project-venv pytest invocation was used.

## Production verification

Deployed web revision `options-roi-web-00121-wqq`, ready with 100% traffic.
Build `ec876c6e-40fd-4891-8845-6287fd1f09d3` succeeded on September 19 at
20:45:43 UTC; 473 tests passed inside the production container. The mobile API
and import jobs were not redeployed. Previous web revision: `options-roi-web-00120-94n`.

| Authenticated production request | Before | After |
| --- | --- | --- |
| First load with no prepared cache | 19.72 seconds | 8.80 seconds |
| Rebuild after dashboard price refresh | 19.72 seconds in the observed prior refresh case | 1.53 seconds |

The cold request spent 3.39 seconds loading the report and 0.92 seconds preparing
accounting. It then overlapped 3.48 seconds of horizon loading with 3.60 seconds
fetching prices for 16 extra tickers. Valuation took 0.22 seconds. The subsequent
price refresh reused accounting; horizon loading took 0.003 seconds, extra prices
0.84 seconds and valuation 0.20 seconds. Route/cache overhead explains the remainder.
These are measured requests, not a guarantee for every external-data response.

Browser verification confirmed the current figures were unchanged, switching to
6M displayed the historical comparison, and returning to To current restored the
default figures. New source reports, a new date or expired prepared caches can
still incur the cold preparation cost; ordinary price refreshes avoid it.

Release records and measured timings are saved under `tmp/assignment-performance/`;
private profiling inputs stayed local and were excluded from the image and upload.
