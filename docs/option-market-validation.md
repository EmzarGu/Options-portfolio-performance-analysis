# Option market validation

This subsystem validates whether an option-data provider is reliable enough for strike-selection analytics. It is intentionally separate from accounting, the mobile API, and the web dashboard.

## Scope

The first validation scope is short option opening trades that can be compared to the legacy Google Sheet `Profit probability` columns. The framework is provider-neutral; no live provider is currently active in this repo.

OptionChainIQ was tested and removed as an active provider after live validation returned insufficient historical coverage for the required strategy period. Keep this note only as historical context; do not use OptionChainIQ env vars or command examples.

The validation fetches only chains required by actual trades. It does not fetch all historical chains.

## Collections

- `option_market_fetch_runs`: one document per validation run.
- `option_market_chain_snapshots`: one document per provider/ticker/trade-date/expiry/put-call request. Firestore snapshots keep compact raw page metadata to avoid document-size limits; full raw pages are retained by the local JSON store during validation runs.
- `option_market_contracts`: normalized contract rows from each chain snapshot.
- `option_market_trade_matches`: local matches between IBKR trades, sheet probability rows, and provider contracts.
- `option_probability_import_runs`: one document per historical Google Sheet probability import.
- `option_probability_rows`: normalized historical sheet probability rows.
- `option_probability_trade_matches`: matches between IBKR short-option opening trades and historical probability rows.

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

There is no active live provider adapter at the moment. The validation CLI can still perform dry-run candidate discovery and sheet-probability matching, but live fetches fail clearly until a replacement provider is added.

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

## Validation metrics

Provider acceptance requires:

- at least 90% of 2024 sheet-probability trades match a provider contract row,
- at least 90% of matched rows have non-null delta,
- provider mark is reasonably close to IBKR fill after bid/ask spread tolerance,
- provider underlying price is not systematically wrong,
- fetch latency and failure rate are acceptable under provider limits.

The sheet `Profit probability` value is not treated as exact delta. For short puts, `1 - profit_probability` is only an assignment-risk proxy used for comparison.

## Outputs

Each run writes local artifacts under `tmp/option_market_validation/<run_id>/`:

- `trade_candidates.csv`
- `trade_matches.csv`
- `risk_bucket_summary.csv`
- `summary.json`
- `report.md`

These files are generated artifacts and should not be committed unless a specific validation report is requested.
