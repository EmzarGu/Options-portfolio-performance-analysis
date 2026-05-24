# Option market validation

This subsystem validates whether a historical option-data provider is reliable enough for future strike-selection analytics. It is intentionally separate from accounting, the mobile API, and the web dashboard.

## Scope

The first provider is OptionChainIQ. The first validation scope is 2024 short option opening trades that can be compared to the legacy Google Sheet `Profit probability` columns.

The validation fetches only chains required by actual trades. It does not fetch all historical chains.

## Collections

- `option_market_fetch_runs`: one document per validation run.
- `option_market_chain_snapshots`: one document per provider/ticker/trade-date/expiry/put-call request. Firestore snapshots keep compact raw page metadata to avoid document-size limits; full raw pages are retained by the local JSON store during validation runs.
- `option_market_contracts`: normalized contract rows from each chain snapshot.
- `option_market_trade_matches`: local matches between IBKR trades, sheet probability rows, and provider contracts.

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

## OptionChainIQ

Use `OPTIONCHAINIQ_API_KEY` locally. Do not commit keys. If this provider is accepted, move the key to Secret Manager before any Cloud Run use.

Example dry run:

```bash
python scripts/option_market_validation_backfill.py --year 2024 --dry-run
```

Example local JSON validation:

```bash
OPTIONCHAINIQ_API_KEY=... python scripts/option_market_validation_backfill.py \
  --year 2024 \
  --store local-json \
  --max-requests 25
```

Example Firestore validation:

```bash
OPTIONCHAINIQ_API_KEY=... python scripts/option_market_validation_backfill.py \
  --year 2024 \
  --store firestore
```

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
