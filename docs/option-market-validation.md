# Option market validation

## Decision Lab production status — 2026-09-23

Decision Lab uses the existing Market Data free account for dated planning
comparisons. Normal reads use shared Firestore snapshots. The existing import job
prepares missing selections, and manual refresh has a bounded time budget and
shared credit ceiling. See the [production setup and verification](decision-lab-marketdata-production-2026-09-23.md).

Since September 30, incomplete new quote dates retain the newest valid complete
snapshot covering the current selection plan, bounded to seven calendar days.
The displayed quote date stays explicit; download completeness is separate from
quote/Greek field presence. See [refresh continuity](decision-lab-refresh-continuity-2026-09-30.md).
The following historical validation subsystem and CuteMarkets records remain
separate; this release does not repair the historical import job.

## Earlier operational status — checked 2026-09-19

CuteMarkets remains configured, but current availability is unverified: its
unauthenticated chain endpoint timed out during a connection probe and its website
failed to connect. The web service has an API-key Secret Manager reference; key
contents and account entitlements were not inspected. No replacement is enabled.

The separate `option-market-history-import` Cloud Run job has an additional
deployment failure: execution `option-market-history-import-x6zmc` reports its
configured May image is not found. Its weekday scheduler remains enabled. This
failure precedes provider access and must not be attributed to the API outage.
See [live operational evidence](cloud-operations-current-state.md).

The September 19 timeout repair rejects failed/partial chains before writing
contracts, correcting the previously documented partial-page overwrite defect.
The broader derived-payload cache publication issue remains separate. See
[the review and provider comparison](refactoring-review-2026-09-19.md).

This subsystem validates whether an option-data provider is reliable enough for strike-selection analytics. It is intentionally separate from accounting, the mobile API, and the web dashboard.

## Scope

The historical validation scope is short option opening trades that can be compared to the legacy Google Sheet `Profit probability` columns. The framework is provider-neutral. Decision Lab's production provider is Market Data; the earlier CuteMarkets path remains available in code when no provider override is configured.

OptionChainIQ was tested and removed as an active provider after live validation returned insufficient historical coverage for the required strategy period. Keep this note only as historical context; do not use OptionChainIQ env vars or command examples.

The validation fetches only chains required by actual trades. It does not fetch all historical chains.

The legacy CuteMarkets path stores current-chain fetches in the shared option-market collections and reuses them until the user presses **Fetch option data**. Page loads only read stored contracts, including when the cache is empty. Its manual refresh shares a 30-second provider budget across chains, pages and retry delays and stops on the first connection timeout or connection failure. Failed or incomplete chains do not overwrite stored contracts. A failed refresh reuses the previous successful universe, or existing chains when no successful universe run exists. The Market Data path uses separate dated snapshots and 20-second interactive/90-second background budgets, as documented in the production release. Google Sheet probability data remains a historical fallback only.

## Collections

- `decision_lab_marketdata`: dated Market Data snapshots plus a transactional quota/refresh control document for production Decision Lab.
- `option_market_fetch_runs`: one document per validation run.
- `option_market_chain_snapshots`: one document per provider/ticker/trade-date/expiry/put-call request. Firestore snapshots keep compact raw page metadata to avoid document-size limits; full raw pages are retained by the local JSON store during validation runs.
- `option_market_contracts`: normalized contract rows from each chain snapshot.
- `option_market_trade_matches`: local matches between IBKR trades, sheet probability rows, and provider contracts.
- `option_probability_import_runs`: one document per historical Google Sheet probability import.
- `option_probability_rows`: normalized historical sheet probability rows.
- `option_probability_trade_matches`: matches between IBKR short-option opening trades and historical probability rows.
- `option_historical_enrichment_runs`: one document per persistent historical CuteMarkets enrichment run.
- `option_historical_trade_enrichments`: one document per IBKR short-option opening trade enriched with provider contract existence and historical option daily price facts.

## Provider contract

A provider adapter must fetch a historical option chain by:

- `ticker`
- `trade_date`
- `expiry`
- `put_call`

Normalized contracts expose:

- `bid`
- `ask`
- `mark`
- `underlying_price`
- `delta`
- `gamma`
- `theta`
- `vega`
- `volatility`
- `open_interest`
- `volume`

## Provider state

CuteMarkets remains the historical option daily-price enrichment adapter and the legacy Decision Lab fallback; configured does not mean operational. Its key is `CUTEMARKETS_API_KEY`. Production Decision Lab selects Market Data with `DECISION_LAB_PROVIDER=marketdata` and `MARKETDATA_API_TOKEN` from Secret Manager. Do not commit provider keys.

Historical CuteMarkets coverage available on the current plan is contract existence and option daily aggregate data. Historical Greeks/delta are not available from the tested endpoint, so historical risk buckets still use the legacy sheet probability where present. The system must not invent historical delta.

The validation CLI can still perform dry-run candidate discovery and sheet-probability matching. Historical provider backfills remain separate from the Decision Lab current-chain refresh path.

Example dry run:

```bash
python scripts/option_market_validation_backfill.py --year 2024 --dry-run
```

## Historical probability import

Use the historical import script to load Google Sheet `Profit probability` values into Firestore or a local JSON simulation. This does not call any option-market provider and does not change accounting, mobile payloads, or dashboard output.

Default scope is 2022 through the current year:

```bash
python scripts/import_option_probability_history.py --store local-json
```

Firestore import:

```bash
python scripts/import_option_probability_history.py --store firestore
```

The import writes normalized probability rows, IBKR trade match rows including missing-probability coverage, unmatched sheet rows, and a run document containing the exact persisted row and match IDs. Re-running the import creates a new run and upserts stable row/match documents, so consumers should read from the latest successful `option_probability_import_runs` document when reload semantics matter. Use `--matched-only` only for ad hoc local artifacts that should exclude missing-probability trade rows.

## Historical provider enrichment

Use the historical provider import to persist provider facts for actual IBKR short-option opening trades. It is missing-only by default: existing trade enrichments are reused and not requested again. This is the routine intended for a daily job after the IBKR import has completed.

Dry run:

```bash
python scripts/import_option_market_history.py --dry-run --start-year 2022 --end-year 2026
```

Local JSON simulation with a small provider-call budget:

```bash
python scripts/import_option_market_history.py --store local-json --max-provider-calls 50
```

Firestore import:

```bash
python scripts/import_option_market_history.py --store firestore --start-year 2022 --end-year 2026
```

The import writes:

- one run document in `option_historical_enrichment_runs`,
- one stable enrichment document per IBKR opening trade in `option_historical_trade_enrichments`,
- optional local artifacts under `tmp/option_market_history/<run_id>/`.

Recommended operating pattern:

- Run missing-only daily after the IBKR statement import.
- Keep `--refresh-existing` for explicit repair/reload runs only.
- Use `--max-provider-calls` when first loading the full history if the provider quota should be spread across several runs.
- Keep Google Sheet probability as historical risk-proxy fallback only; provider facts are preferred where available.

## Validation metrics

Current Decision Lab option data is acceptable when:

- the actionable ticker/expiry/type universe is persisted and reused without repeat API calls,
- three-candidate recommendation rows use stored provider contracts where available,
- current chains have usable Greeks for candidate risk scoring,
- missing quote-grade bid/ask is surfaced as indicative data instead of being hidden,
- failed refreshes preserve the latest successful stored data.

Historical enrichment is acceptable when:

- actual IBKR short-option opening trades are enriched once and reused,
- provider contract and option daily-price coverage are visible in Coverage,
- missing historical Greeks are not inferred,
- Google Sheet `Profit probability` is used only as a legacy risk-proxy fallback where present.

The sheet `Profit probability` value is not treated as exact delta. For short puts, `1 - profit_probability` is only an assignment-risk proxy used for comparison.

## Outputs

Each run writes local artifacts under `tmp/option_market_validation/<run_id>/`:

- `trade_candidates.csv`
- `trade_matches.csv`
- `risk_bucket_summary.csv`
- `summary.json`
- `report.md`

These files are generated artifacts and should not be committed unless a specific validation report is requested.
