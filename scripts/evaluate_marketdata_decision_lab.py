"""Local-only integration runner. Default is cache-only and makes no API calls.

Run: PYTHONPATH=. .venv/bin/python scripts/evaluate_marketdata_decision_lab.py --help
The input is an exported dashboard payload. This script never reads/writes Firestore.
"""
import argparse
import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from portfolio_backend.decision_lab import build_decision_lab_data
from portfolio_backend.option_market.marketdata import CreditLedger, MarketDataClient, NY
from portfolio_backend.option_market.marketdata_local import load_local_market_data, write_private_json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--payload', type=Path, required=True)
    p.add_argument('--root', type=Path, default=Path('tmp/marketdata-integration'))
    p.add_argument('--refresh', action='store_true', help='Explicitly allow bounded provider requests')
    p.add_argument('--token-file', type=Path, help='Private KEY=value file; alternatively use MARKETDATA_API_TOKEN')
    p.add_argument('--daily-limit', type=int, default=80)
    p.add_argument('--initial-used', type=int, default=0, help='Known credits already consumed today; used only for a new ledger day')
    args = p.parse_args()
    args.root.mkdir(parents=True, exist_ok=True, mode=0o700)
    args.root.chmod(0o700)
    with (args.root / 'run.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        now = datetime.now(timezone.utc)
        payload = json.loads(args.payload.read_text())
        source_date = (payload.get('dashboard') or {}).get('request', {}).get('as_of')
        # Preserve the input file and record its date separately. Candidate DTE
        # and default provider requests use the actual evaluation date.
        as_of = now.astimezone(NY).date()
        payload.setdefault('dashboard', {}).setdefault('request', {})['as_of'] = as_of.isoformat()
        groups = build_decision_lab_data(payload)['recommendation_candidates']
        client = None
        ledger = None
        if args.refresh:
            token = os.getenv('MARKETDATA_API_TOKEN', '')
            if args.token_file:
                lines = args.token_file.read_text().splitlines()
                token = next((s.split('=', 1)[1].strip() for s in lines if s.startswith('MARKETDATA_API_TOKEN=')), '')
            ledger = CreditLedger(args.root / 'credits.json', daily_limit=args.daily_limit, initial_used=args.initial_used)
            client = MarketDataClient(token, ledger=ledger)
        market = load_local_market_data(groups, as_of=as_of, root=args.root, client=client, refresh=args.refresh, now=now)
        result = build_decision_lab_data(payload, option_market_data=market)
        output = {'evaluation_only': True, 'input_as_of': source_date, 'evaluated_at': now.isoformat(),
                  'credit_ledger': ledger.state if ledger else None, 'market_data': market, 'decision_lab': result}
        write_private_json(args.root / 'evaluation.json', output)
        print(json.dumps({'output': str(args.root / 'evaluation.json'), 'status': market['status'],
            'credits': ledger.state if ledger else 'cache-only; no API calls',
            'tickers': [{'ticker': g['ticker'], 'category': g['category'], 'candidates': len(g['candidates']),
                        'coverage': g.get('candidate_status')} for g in result['recommendation_candidates']]}, indent=2))


if __name__ == '__main__':
    main()
