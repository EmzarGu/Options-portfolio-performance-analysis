# Option market validation

This subsystem validates whether an option-data provider is reliable enough for strike-selection analytics. It is intentionally separate from accounting, the mobile API, and the web dashboard.

## Scope

The historical validation scope is short option opening trades that can be compared to the legacy Google Sheet `Profit probability` columns. The framework is provider-neutral. Decision Lab now also has a current-chain provider path using CuteMarkets for live prototype recommendations.

OptionChainIQ was tested and removed as an active provider after live validation returned insufficient historical coverage for the required strategy period. Keep this note only as historical context; do not use OptionChainIQ env vars or command examples.

The validation fetches only chains required by actual trades. It does not fetch all historical chains.

Decision Lab option-chain data is persistent. Current-chain fetches are stored in the shared option-market collections and reused until the user presses **Fetch option data**. Failed refreshes do not replace the latest successful stored data. Google Sheet probability data is a historical fallback only; newly managed option decisions should prefer stored provider snapshots once available.

## Collections

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

CuteMarkets is the active prototype adapter for current Decision Lab option-chain data and historical option daily-price enrichment. Configure the key with `CUTEMARKETS_API_KEY` in the runtime environment or Secret Manager. Do not commit provider keys.

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
