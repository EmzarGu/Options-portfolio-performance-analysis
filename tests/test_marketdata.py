from copy import deepcopy
from datetime import date, datetime, timezone
import json

import pytest
import requests

from portfolio_backend.option_market.marketdata import (
    CreditLedger, MarketDataClient, MarketDataError, Selection, latest_free_session, normalize_chain,
)
from portfolio_backend.option_market.marketdata_local import load_local_market_data, plan_requests
from portfolio_backend.option_market.models import OptionChainRequest

NOW = datetime(2026, 9, 23, 19, 0, tzinfo=timezone.utc)
REQUEST = OptionChainRequest('marketdata', 'GLW', date(2026, 9, 23), date(2026, 10, 16), 'CALL')


def body():
    return {'s':'ok', 'optionSymbol':['GLW261016C00190000'], 'underlying':['GLW'],
            'expiration':[1792180800], 'side':['call'], 'strike':[190], 'updated':[1790107200],
            'bid':[1.83], 'ask':[2.12], 'delta':[.1586], 'iv':[.6215],
            'underlyingPrice':[159.62], 'openInterest':[1114], 'volume':[96]}


class Session:
    def __init__(self, data=None, status=203, failure=None, headers=None):
        self.data = body() if data is None else data
        self.status_code = status
        self.failure = failure
        self.headers = headers if headers is not None else {'X-Api-Ratelimit-Consumed':'1','X-Api-Ratelimit-Remaining':'79'}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.failure:
            raise self.failure
        return self

    def json(self):
        return self.data


def client(tmp_path, session=None, **kwargs):
    return MarketDataClient('test-private-token', ledger=CreditLedger(tmp_path/'credits.json', now=lambda:NOW, **kwargs),
                            session=session or Session(), now=lambda:NOW)


@pytest.mark.parametrize('stamp,expected', [
    ('2026-09-23T12:00:00+00:00','2026-09-21'),
    ('2026-09-23T13:30:00+00:00','2026-09-21'),
    ('2026-09-23T13:30:01+00:00','2026-09-22'),
    ('2026-09-26T15:00:00+00:00','2026-09-24'),
    ('2026-09-28T12:00:00+00:00','2026-09-24'),
    ('2026-09-28T14:00:00+00:00','2026-09-25'),
    ('2026-09-07T15:00:00+00:00','2026-09-03'),
])
def test_free_session_rollover(stamp, expected):
    assert latest_free_session(datetime.fromisoformat(stamp)).isoformat() == expected


def test_default_request_keeps_observation_time_and_credentials_private(tmp_path):
    s=Session(); c=client(tmp_path,s)
    result=c.fetch_selected(REQUEST,Selection(strike=190))
    assert result.contracts[0].trade_date == date(2026,9,22)
    assert result.contracts[0].raw['greek_observed_at'] is None
    url,kwargs=s.calls[0]
    assert url == 'https://api.marketdata.app/v1/options/chain/GLW/'
    assert 'date' not in kwargs['params'] and 'strikeLimit' not in kwargs['params']
    assert kwargs['allow_redirects'] is False
    assert 'test-private-token' not in json.dumps(result.as_snapshot_doc())


@pytest.mark.parametrize('field,value', [
    ('delta',[float('nan')]), ('delta',[-.2]), ('updated',[None]),
    ('optionSymbol',['GLW1261016C00190000']), ('underlying',['SHOP']),
    ('expiration',[1790107200]), ('strike',[-1]), ('ask',[]),
    ('optionSymbol',['GLW261016C00190000']*2),
])
def test_rejects_malformed_or_wrong_contract(field,value):
    data=body(); data[field]=value
    with pytest.raises(MarketDataError):
        normalize_chain(data, REQUEST, fetched_at=NOW)


def test_null_delta_preserved_not_invented():
    data=body(); data['delta']=[None]
    assert normalize_chain(data,REQUEST,fetched_at=NOW)[0].delta is None


@pytest.mark.parametrize('selection',[Selection(),Selection(strike=190,delta=.2),Selection(delta=1),Selection(strike=float('inf'))])
def test_unbounded_selection_rejected_before_credit_use(tmp_path,selection):
    s=Session();c=client(tmp_path,s)
    with pytest.raises(ValueError): c.fetch_selected(REQUEST,selection)
    assert not s.calls and c.ledger.state['used']==0


def test_credit_reservation_survives_timeout_and_restart(tmp_path):
    c=client(tmp_path,Session(failure=requests.Timeout('secret request details')),daily_limit=1)
    with pytest.raises(MarketDataError,match='reserved credit retained'):
        c.fetch_selected(REQUEST,Selection(delta=.2))
    again=client(tmp_path,daily_limit=1)
    with pytest.raises(MarketDataError,match='ceiling'):
        again.fetch_selected(REQUEST,Selection(delta=.2))
    assert not again.session.calls


@pytest.mark.parametrize('status',[301,401,403,429,500])
def test_no_retry_on_provider_errors(tmp_path,status):
    s=Session(status=status);c=client(tmp_path,s)
    with pytest.raises(MarketDataError,match=f'HTTP {status}'):
        c.fetch_selected(REQUEST,Selection(delta=.2))
    assert len(s.calls)==1


def test_unlisted_strike_is_empty_and_does_not_abort_later_tickers(tmp_path):
    c=client(tmp_path,Session(status=404,data={'s':'no_data'}))
    result=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert result['status']['status']=='succeeded'
    assert len(c.session.calls)==3 and result['contracts']==[]


def groups():
    return [{'ticker':'GLW','current_state':{'cost_basis':190,'open_options':[
        {'type':'Call','strike':190,'expiry':'2026-10-16'}]},
        'contract_requests':[{'expiry':'2026-10-16','put_call':'CALL'}]}]


def test_cache_read_never_fetches_and_failed_refresh_keeps_good_data(tmp_path):
    c=client(tmp_path)
    first=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert first['status']['contract_count']==1 # same contract from three selectors deduplicated
    assert len(c.session.calls)==3
    cached=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,now=NOW)
    assert len(c.session.calls)==3
    assert cached['contracts']==first['contracts']
    before={p:p.read_bytes() for p in (tmp_path/'responses').glob('*.json')}
    c.session=Session(failure=requests.ConnectionError())
    failed=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,force=True,now=NOW)
    assert failed['status']['status']=='partial' and failed['status']['contract_count']==1
    assert len(c.session.calls)==1
    assert all(p.read_bytes()==data for p,data in before.items())


def test_new_download_of_stale_quote_is_excluded(tmp_path):
    data=body(); data['updated']=[1789761600] # previous Friday
    c=client(tmp_path,Session(data=data))
    result=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert result['contracts']==[]
    assert result['status']['status']=='partial'


def test_all_current_legs_included_without_duplicate_basis_query():
    g=groups();g[0]['current_state']['open_options'].append({'type':'Call','strike':200,'expiry':'2026-10-16'})
    _,plan=plan_requests(g,as_of=REQUEST.trade_date)
    assert [s.strike for _,s in plan if s.strike] == [190,200]


def test_refresh_reuses_current_saved_selection_without_spending(tmp_path):
    c=client(tmp_path)
    first=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    used=c.ledger.state['used']
    again=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert c.ledger.state['used']==used
    assert again['contracts']==first['contracts']


def test_stale_refresh_preserves_previous_good_response(tmp_path):
    c=client(tmp_path)
    load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    before={p:p.read_bytes() for p in (tmp_path/'responses').glob('*.json')}
    data=body();data['updated']=[1789761600]
    c.session=Session(data=data)
    result=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,force=True,now=NOW)
    assert result['status']['status']=='partial'
    assert all(p.read_bytes()==data for p,data in before.items())


def test_corrupt_cache_is_reported_then_repaired_only_on_explicit_refresh(tmp_path):
    c=client(tmp_path)
    load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    path=next((tmp_path/'responses').glob('*.json'));path.write_text('{broken')
    calls=len(c.session.calls)
    offline=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,now=NOW)
    assert offline['status']['status']=='partial' and len(c.session.calls)==calls
    repaired=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert repaired['status']['status']=='succeeded' and len(c.session.calls)==calls+1


def test_account_allowance_stops_requests_even_with_local_budget(tmp_path):
    c=client(tmp_path,Session(headers={'x-api-ratelimit-consumed':'1','x-api-ratelimit-remaining':'0'}))
    c.fetch_selected(REQUEST,Selection(delta=.2))
    with pytest.raises(MarketDataError,match='ceiling'): c.fetch_selected(REQUEST,Selection(delta=.3))
    assert len(c.session.calls)==1


def test_missing_accounting_stops_following_portfolio_calls(tmp_path):
    c=client(tmp_path,Session(headers={}))
    result=load_local_market_data(groups(),as_of=REQUEST.trade_date,root=tmp_path,client=c,refresh=True,now=NOW)
    assert result['status']['status']=='partial'
    assert len(c.session.calls)==1 and c.ledger.state['used']==1


def test_missing_token_never_sends_or_spends(tmp_path):
    c=MarketDataClient('',ledger=CreditLedger(tmp_path/'credits.json',now=lambda:NOW),session=Session(),now=lambda:NOW)
    with pytest.raises(MarketDataError,match='not configured'):
        c.fetch_selected(REQUEST,Selection(delta=.2))
    assert not c.session.calls and c.ledger.state['used']==0


def test_historical_request_cannot_receive_default_current_greeks(tmp_path):
    c=client(tmp_path)
    historical=OptionChainRequest('marketdata','GLW',date(2026,9,22),REQUEST.expiry,'CALL')
    with pytest.raises(ValueError,match="today's analysis date"):
        c.fetch_selected(historical,Selection(delta=.2))
    assert not c.session.calls and c.ledger.state['used']==0


def test_invalid_json_keeps_reserved_credit_and_hides_provider_details(tmp_path):
    class InvalidJSON(Session):
        def json(self): raise ValueError('private provider content')
    c=client(tmp_path,InvalidJSON())
    with pytest.raises(MarketDataError,match='invalid JSON') as exc:
        c.fetch_selected(REQUEST,Selection(delta=.2))
    assert 'private provider' not in str(exc.value)
    assert c.ledger.state['used']==1


@pytest.mark.parametrize('kind',['call','put'])
def test_normalized_quotes_drive_existing_roll_engine_without_score_changes(kind):
    from portfolio_backend.decision_lab import build_decision_lab_data
    from tests.test_decision_lab import _base_payload
    payload=_base_payload();payload['dashboard']['request']['as_of']='2026-09-23'
    is_call=kind=='call'
    spot=159.62 if is_call else 134
    strike=190 if is_call else 130
    if is_call:
        payload['positions']['inventory']=[{'ticker':'GLW','shares':100,'covered_shares':100,
            'cost_per_share':190,'current_price':spot,'unrealized_pnl':-3038}]
    payload['positions']['open_option_shorts']=[{'ticker':'GLW','option_type':kind.title(),
        'strike':strike,'expiration':'2026-10-16','days_to_expiration':23,'quantity':-1,
        'current_price':spot,'moneyness':(spot-strike)/strike,
        'accounting_open_premium':200,'strategy_premium_collected':200}]
    payload['tickers']['items']=[{'ticker':'GLW','total_pnl':-3038 if is_call else 200,
        'unrealized_pnl':-3038 if is_call else 200,'realized_options_pnl':0}]
    rows=[]
    for expiry,k,bid,ask,delta in [('2026-10-16',strike,1.83 if is_call else 3,2.12 if is_call else 3.2,.1586 if is_call else -.4),
                                  ('2026-11-20',strike if is_call else 125,6.6 if is_call else 4,7.2 if is_call else 4.2,.2998 if is_call else -.25)]:
        req=OptionChainRequest('marketdata','GLW',REQUEST.trade_date,date.fromisoformat(expiry),kind.upper())
        data=body()
        data.update(optionSymbol=[f'GLW{req.expiry:%y%m%d}{kind[0].upper()}{k*1000:08d}'],
            side=[kind],strike=[k],bid=[bid],ask=[ask],delta=[delta],underlyingPrice=[spot],
            expiration=[int(datetime.fromisoformat(expiry+'T20:00:00+00:00').timestamp())])
        rows.extend(c.as_dict() for c in normalize_chain(data,req,fetched_at=NOW))
    market={'status':{'provider':'marketdata'},'contracts':rows}
    result=build_decision_lab_data(payload,option_market_data=market)
    candidates=result['recommendation_candidates'][0]['candidates']
    roll=next(c for c in candidates if c['action']==('Roll out same strike' if is_call else 'Roll put down/out'))
    assert roll['roll_close_cost']==pytest.approx(212 if is_call else 320)
    assert roll['roll_new_credit']==pytest.approx(660 if is_call else 400)
    assert roll['roll_net_credit']==pytest.approx(448 if is_call else 80)
    if not is_call:
        assert roll['assignment_risk_reduction']==500
    without_delta=deepcopy(market)
    without_delta['contracts'][1]['delta']=None
    rejected=build_decision_lab_data(payload,option_market_data=without_delta)
    assert not any(c['action']==roll['action'] for c in rejected['recommendation_candidates'][0]['candidates'])
