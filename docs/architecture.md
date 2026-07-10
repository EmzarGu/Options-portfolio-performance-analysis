# Options ROI Architecture

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
