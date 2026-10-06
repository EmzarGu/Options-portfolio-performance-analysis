# Options ROI Architecture

Implementation map checked: 2026-09-19, after the functionality-preserving refactor.

## Current module ownership

| Layer | Current owner |
| --- | --- |
| Web HTTP routes, middleware and request validation | `web_dashboard.py` |
| Web authentication, signed sessions and login rendering | `portfolio_backend/web_auth.py` |
| Web payload caches, history loading and existing Decision Lab orchestration | `portfolio_backend/web_data_service.py` |
| Mobile HTTP routes, authentication, request timing and audit | `mobile_api.py` |
| Shared context, source markers, refresh and snapshot coordination | `portfolio_backend/context_runtime.py` |
| Post-import dashboard snapshot preparation and persistence verification | `portfolio_backend/pipeline_warmup.py`, called by `portfolio_backend/ibkr/import_job.py` |
| IBKR import-health classification | `portfolio_backend/ibkr/import_health.py` |
| Web payload assembly | `portfolio_backend/web_dashboard_payloads.py` |
| Mobile transport payload assembly | `portfolio_backend/mobile_api_service.py`, `portfolio_backend/mobile_payloads.py` |
| Canonical accounting/projections | `portfolio_backend/ibkr/`, `portfolio_backend/calculations.py`, `portfolio_backend/performance.py`, `portfolio_backend/cycle_projection.py` |
| Decision Lab analytics and candidate construction | `portfolio_backend/decision_lab.py`, `portfolio_backend/decision_lab_candidates.py` |
| Provider adapter, normalization, storage and option-data loading | `portfolio_backend/option_market/` |
| Shared base snapshots / derived JSON payload storage | `portfolio_backend/pipeline_snapshot_store.py`, `portfolio_backend/derived_payload_cache.py` |
| Web rendering | `portfolio_backend/web_dashboard_templates.py`, `portfolio_backend/decision_lab_templates.py` |

Web payload assembly and mobile routes call `context_runtime.get_context` and
`context_runtime.refresh_context`. Importing the web application no longer loads
the mobile HTTP application. Both share one context cache within a process;
separate processes continue to coordinate through persisted snapshots and leases.
The local visual prototype uses `web_data_service` directly.

Since the September 20 morning-refresh change, the scheduled IBKR job also
prepares the default base snapshot after import. The browser restores it and
refreshes current prices through the existing runtime. The preparation job uses
the same import/date key and verifies persistence; interactive rebuild remains
the fallback if preparation fails. See the
[performance record](morning-refresh-performance-2026-09-20.md).

The shared runtime still obtains existing settings/provider dependencies from
`streamlit_app` and uses FastAPI's exception type for request validation. Removing
those remaining dependencies would need a separate, tested extraction. Cache
policies and financial algorithms were preserved; see the
[refactoring results](refactoring-results-2026-09-19.md).

Derived JSON caches are distinct from accounting snapshots. Their present write
sequence and Decision Lab partial-fetch fallback have reproduced reliability
defects; the linked review records proposed fixes rather than guarantees already
provided by the code.

## Design rule

Financial values are calculated once on the backend. Web and iOS clients may
format, filter, sort, and choose a visual treatment, but they must not derive
P&L, returns, targets, moneyness, or option projections from raw fields.

## Data flow

```text
IBKR Flex / stored market data
        |
        v
raw and normalized Firestore records
        |
        v
canonical accounting pipeline state
        |
        +--> persisted pipeline_snapshots
        |
        v
domain projections and table builders
        |
        +--> web payload
        +--> mobile API payload
        +--> Decision Lab payload
```

### Source and persistence

- IBKR Flex imports write normalized source records and an import marker.
- `pipeline_snapshots` persist the computed base accounting state by source
  marker, as-of date, selected sheets, and schema version.
- Current prices are a separate overlay. Refreshing prices must not rebuild or
  reinterpret historical accounting.
- Read endpoints load the latest valid persisted pipeline state. They do not
  fetch IBKR data or rebuild history unless the persisted state is missing or
  invalid.

### Accounting ownership

- Core option, stock, dividend, capital, and realized/unrealized accounting
  remains in the existing accounting/performance modules.
- `portfolio_backend/cycle_projection.py` is the only owner of active-cycle,
  future-cycle, target, and projected-return calculations.
- `portfolio_backend/mobile_payloads.py` assembles transport DTOs from canonical
  accounting and projection values. It must not define parallel formulas.
- `portfolio_backend/web_dashboard_templates.py` renders backend values. It
  must not reconstruct missing financial values in JavaScript.
- iOS DTO display helpers expose canonical backend fields only. A missing
  canonical value is displayed as unavailable rather than replaced by a
  different metric.

## Canonical cycle projection

Every active or future expiry month uses the same projection builder and the
same field names. The canonical additive premium field is
`open_premium_collected`. The legacy aliases
`open_expiring_option_premium` and `open_expiring_incremental_premium` are not
part of the contract.

The projection follows the documented accounting rules:

```text
projected cycle P&L
  = realized cycle P&L
  + accounting open premium for that expiry month
  + ITM put assignment P&L
  + ITM covered-call stock P&L
```

Linked assigned-stock unrealized P&L remains available as a separate exposure
field. It enters projected cycle P&L only when an ITM covered call would dispose
of the shares in that cycle, in which case stock P&L is capped at the call
strike. OTM calls do not assign an old inventory gain or loss to the current
cycle.

## Client contract invariants

The following must hold for a single backend state and request configuration:

1. Web and mobile identify the same active cycle.
2. Dashboard monthly target and monthly performance current month use the same
   cycle projection values.
3. Current unrealized equals its canonical option and stock components.
4. Realized totals equal options P&L plus stock P&L plus dividends.
5. Target return and target floor come from persisted shared settings, never a
   hardcoded client value.
6. Missing values remain unavailable; clients do not calculate fallbacks.
7. One business meaning has one API field name.

## Refresh behavior

- Scheduled imports update source records and invalidate the affected persisted
  pipeline state.
- Normal web and mobile reads reuse the persisted state.
- Explicit data refresh updates the source or price layer, then rebuilds and
  persists the affected canonical state once.
- Concurrent cold requests share the same build rather than duplicating the
  historical pipeline work.

## Change gate

Accounting changes require an update to
`docs/ibkr-accounting-rules.md` and
`docs/ibkr-accounting-test-matrix.md` before implementation. Refactors that do
not change accounting must preserve those tests and add contract tests proving
that web and mobile consume the same canonical fields.

## Assignment Quality Lab performance

`assignment_quality_runtime.py` caches price-independent accounting and dated
horizon observations separately from the price-sensitive response. Cached lots
are copied before valuation. See [performance design and verification](assignment-quality-performance-2026-09-19.md).

## Completed option strategy accounting — 1 October 2026

Broker-confirmed rolls close old contract lots for inventory/audit, but carry
all net credits/debits and fees onto replacement lots without a realized event
for the continuing quantity. Only terminal quantities realize their entire net
balance on final close, expiration or assignment. Multiple source lots preserve
separate FIFO balances internally; identical contracts still aggregate for UI.
Stock/assignment and dividend attribution remain unchanged. Monthly/yearly,
unrealized, cycle projections and Decision Lab read the same signed balances.
Schema 8 rejects both schema-6 early-booking and schema-7 contract-split snapshots
and produces new derived payload keys. Actual broker rows/provider data survive.
See [rules](ibkr-accounting-rules.md) and
[release evidence](completed-roll-chain-accounting-2026-10-01.md).
