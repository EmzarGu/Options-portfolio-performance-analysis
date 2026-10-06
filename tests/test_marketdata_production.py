from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from portfolio_backend.option_market import marketdata_production as prod
from portfolio_backend.option_market.marketdata import MarketDataError, latest_free_session
from portfolio_backend.option_market.models import OptionMarketFetchResult
from tests.test_marketdata import body, groups, NOW, REQUEST


class Store:
    def __init__(self):
        self.control = {}
        self.docs = {}
        self.writes = 0

    def state(self): return deepcopy(self.control)
    def mutate(self, change):
        self.control = change(deepcopy(self.control))
        self.writes += 1
        return deepcopy(self.control)
    def load(self, session): return deepcopy(self.docs.get(session, {}))
    def load_previous(self, session, *, earliest):
        return [(day, deepcopy(self.docs[day])) for day in sorted(self.docs, reverse=True)
                if earliest <= day < session]
    def save(self, session, responses, *, owner=None):
        if owner: assert self.control['owner']==owner
        self.docs[session] = {'responses':deepcopy(responses)}
        self.writes += 1


def seed(store):
    _,plan=prod.plan_requests(groups(),as_of=REQUEST.trade_date)
    store.save(latest_free_session(NOW),{prod.selection_key(r,s):{'body':body(),'fetched_at':NOW.isoformat()} for r,s in plan})


def test_stored_reads_do_not_write_or_construct_provider():
    store=Store();seed(store);before=store.writes
    out=prod.load_market_data(groups(),store=store,now=NOW,client_factory=lambda _:pytest.fail('no provider'))
    assert out['status']['contract_count']==1 and out['status']['missing_request_count']==0
    assert store.writes==before and out['status']['quote_date']=='2026-09-22'


def test_following_european_morning_reuses_same_observation_across_analysis_dates():
    store=Store();seed(store)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(hours=12),refresh=True,
                             client_factory=lambda _:pytest.fail('no new request'))
    assert out['status']['contract_count']==1 and out['status']['missing_request_count']==0


def test_session_rollover_does_not_present_previous_session_as_fresh():
    store=Store();seed(store)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1))
    assert len(out['contracts'])==1 and out['status']['missing_request_count']==3
    assert out['status']['quote_date']=='2026-09-22'
    assert out['status']['latest_quote_date']=='2026-09-23'
    assert out['status']['using_previous_complete'] is True
    assert out['status']['prepared_request_count']==0
    assert out['status']['display_missing_request_count']==0
    assert latest_free_session(NOW) in store.docs


def test_shared_reservation_prevents_competing_worker_and_limits_credits():
    store=Store()
    prod._acquire(store,'one',NOW,30)
    with pytest.raises(MarketDataError,match='already'):
        prod._acquire(store,'two',NOW,30)
    first=prod.SharedCredits(store,'one',now=lambda:NOW,limit=1)
    first.reserve()
    with pytest.raises(MarketDataError,match='allowance'): first.reserve()
    with pytest.raises(MarketDataError,match='owns'):
        prod.SharedCredits(store,'two',now=lambda:NOW).reserve()
    assert store.control['used']==1


def test_quota_resets_at_eastern_open_not_utc_midnight():
    assert prod.quota_window(NOW)==prod.quota_window(NOW+timedelta(hours=12))
    assert prod.quota_window(NOW)!=prod.quota_window(NOW+timedelta(days=1))
    assert prod.quota_window(datetime(2026,11,3,13,tzinfo=timezone.utc)).startswith('2026-11-02T09:30:00-05:00')


def test_failed_refresh_keeps_saved_rows_and_releases_lock():
    store=Store();seed(store)
    session=latest_free_session(NOW)
    firstkey=next(iter(store.docs[session]['responses']))
    store.docs[session]['responses'].pop(firstkey)
    saved=deepcopy(store.docs)
    class Failed:
        def fetch_selected(self,*args,**kwargs): raise MarketDataError('provider unavailable')
    out=prod.load_market_data(groups(),store=store,now=NOW,refresh=True,client_factory=lambda _:Failed())
    assert out['status']['status']=='partial' and out['status']['contract_count']==1
    assert store.docs==saved and store.control['owner'] is None


def test_partial_progress_saved_and_repeat_only_requests_missing():
    store=Store();calls=[]
    class Partial:
        def fetch_selected(self,r,s,**kwargs):
            calls.append(s)
            if len(calls)==2: raise MarketDataError('timeout')
            return OptionMarketFetchResult(r,[],[{'s':'no_data'}],NOW.isoformat(),1,404)
    one=prod.load_market_data(groups(),store=store,now=NOW,refresh=True,client_factory=lambda _:Partial())
    assert one['status']['missing_request_count']==2
    class Next:
        def fetch_selected(self,r,s,**kwargs):
            calls.append(s)
            return OptionMarketFetchResult(r,[],[{'s':'no_data'}],NOW.isoformat(),1,404)
    two=prod.load_market_data(groups(),store=store,now=NOW,refresh=True,client_factory=lambda _:Next())
    assert len(calls)==4 and two['status']['missing_request_count']==0


def test_credit_reconciliation_accounts_for_other_usage_and_unknown_attempts():
    store=Store();prod._acquire(store,'one',NOW,30)
    ledger=prod.SharedCredits(store,'one',now=lambda:NOW)
    ledger.reserve();ledger.reconcile({'x-api-ratelimit-consumed':'1','x-api-ratelimit-remaining':'11'})
    assert store.control['used']==89
    ledger.reserve()
    with pytest.raises(MarketDataError,match='allowance'):ledger.reserve()
    assert store.control['used']==90


def test_recent_provider_call_blocks_new_egress_worker():
    store=Store();prod._acquire(store,'one',NOW,30)
    prod.SharedCredits(store,'one',now=lambda:NOW).reserve()
    prod._release(store,'one')
    with pytest.raises(MarketDataError,match='five minutes'):prod._acquire(store,'two',NOW+timedelta(seconds=100),30)
    prod._acquire(store,'two',NOW+timedelta(seconds=301),30)


def test_marketdata_bypasses_legacy_derived_cache(monkeypatch):
    from portfolio_backend import web_data_service as web
    monkeypatch.setenv('DECISION_LAB_PROVIDER','marketdata')
    monkeypatch.setattr(web,'load_derived_payload',lambda _:pytest.fail('legacy cache'))
    monkeypatch.setattr(web,'_build_decision_lab_payload',lambda payload,force_refresh:{'new':force_refresh})
    assert web._get_cached_decision_lab_payload({})=={'new':False}


def test_schedule_uses_existing_context_and_background_time_budget(monkeypatch):
    from types import SimpleNamespace
    from portfolio_backend import mobile_api_service as mobile
    monkeypatch.setattr(mobile,'build_mobile_positions_payload',lambda c:{'inventory':[]})
    monkeypatch.setattr(mobile,'build_mobile_tickers_payload',lambda c,**kw:{'items':[]})
    calls=[]
    monkeypatch.setattr(prod,'load_market_data',lambda groups,**kw:calls.append(kw) or {'status':{'status':'succeeded'}})
    assert prod.prepare_from_context(SimpleNamespace(request={}))['status']=='succeeded'
    assert calls==[{'refresh':True,'time_limit':90}]


def seed_next_session(store, *, complete=False):
    session=latest_free_session(NOW+timedelta(days=1))
    previous=deepcopy(store.docs[latest_free_session(NOW)]['responses'])
    rows={}
    for key, record in previous.items():
        record['body']['updated']=[stamp+86400 for stamp in record['body']['updated']]
        record['body']['bid']=[3.0]
        record['body']['ask']=[3.2]
        record['fetched_at']=(NOW+timedelta(days=1)).isoformat()
        rows[key]=record
        if not complete: break
    store.save(session,rows)
    return session


def test_partial_new_day_keeps_complete_old_day_without_mixing_or_provider_calls():
    store=Store();seed(store);seed_next_session(store)
    before=deepcopy(store.docs);writes=store.writes
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1),
                             client_factory=lambda _:pytest.fail('read called provider'))
    assert out['status']['prepared_request_count']==1
    assert out['status']['missing_request_count']==2
    assert out['status']['source']=='stored_fallback'
    assert {c['trade_date'] for c in out['contracts']}=={'2026-09-22'}
    assert out['contracts'][0]['bid']==1.83
    assert store.docs==before and store.writes==writes


def test_completed_new_day_replaces_previous_day_on_next_read():
    store=Store();seed(store);seed_next_session(store,complete=True)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1))
    assert out['status']['status']=='succeeded'
    assert out['status']['using_previous_complete'] is False
    assert out['status']['quote_date']=='2026-09-23'
    assert out['status']['prepared_request_count']==3
    assert out['status']['missing_request_count']==0
    assert {c['trade_date'] for c in out['contracts']}=={'2026-09-23'}
    assert out['contracts'][0]['bid']==3.0


def test_failed_new_day_refresh_preserves_complete_display_and_partial_progress():
    store=Store();seed(store);session=seed_next_session(store)
    before=deepcopy(store.docs);calls=[]
    class Failed:
        def fetch_selected(self,r,s,**kw):
            calls.append(s)
            raise MarketDataError('Daily option-data allowance reached')
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1),
                             refresh=True,client_factory=lambda _:Failed())
    assert out['status']['using_previous_complete'] is True
    assert 'allowance' in out['status']['message']
    assert out['status']['prepared_request_count']==1
    assert len(calls)==1 and store.docs==before
    assert store.control['owner'] is None


def test_refresh_completes_new_day_then_switches_display():
    store=Store();seed(store);session=seed_next_session(store)
    calls=[]
    class Complete:
        def fetch_selected(self,r,s,**kw):
            calls.append(s)
            data=body();data['updated']=[data['updated'][0]+86400]
            return OptionMarketFetchResult(r,[],[data],(NOW+timedelta(days=1)).isoformat(),1,203)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1),
                             refresh=True,client_factory=lambda _:Complete())
    assert len(calls)==2
    assert out['status']['missing_request_count']==0
    assert out['status']['using_previous_complete'] is False
    assert out['status']['quote_date']==session.isoformat()
    assert len(store.docs[session]['responses'])==3


def test_incomplete_or_corrupt_older_snapshot_cannot_be_called_complete():
    for corrupt in (False,True):
        store=Store();seed(store);seed_next_session(store)
        records=store.docs[latest_free_session(NOW)]['responses']
        key=next(iter(records))
        if corrupt: records[key]['body']={'s':'ok'}
        else: records.pop(key)
        out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1))
        assert out['status']['using_previous_complete'] is False
        assert out['status']['display_missing_request_count']>0


def test_old_snapshot_does_not_cover_new_portfolio_selections():
    store=Store();seed(store)
    changed=deepcopy(groups());changed[0]['current_state']['cost_basis']=195
    out=prod.load_market_data(changed,store=store,now=NOW+timedelta(days=1))
    assert out['status']['using_previous_complete'] is False
    assert out['contracts']==[]
    assert 'no recent complete dataset covers' in out['status']['message']


def test_fallback_age_is_bounded_and_uses_newest_valid_complete_day():
    store=Store();seed(store);seed_next_session(store,complete=True)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=5))
    assert out['status']['quote_date']=='2026-09-23'
    assert out['status']['using_previous_complete'] is True
    expired=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=8))
    assert expired['contracts']==[]
    assert expired['status']['using_previous_complete'] is False


def test_no_data_response_counts_as_prepared_not_missing():
    store=Store();seed(store)
    for record in store.docs[latest_free_session(NOW)]['responses'].values():
        record['body']={'s':'no_data'}
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1))
    assert out['status']['using_previous_complete'] is True
    assert out['status']['display_missing_request_count']==0
    assert out['contracts']==[]


def test_invalid_latest_record_is_refetched_before_display_switch():
    store=Store();seed(store);session=seed_next_session(store,complete=True)
    key=next(iter(store.docs[session]['responses']))
    store.docs[session]['responses'][key]['body']={'s':'ok'}
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1))
    assert out['status']['using_previous_complete'] is True
    assert out['status']['prepared_request_count']==2
    calls=[]
    class Repair:
        def fetch_selected(self,r,s,**kw):
            calls.append(s)
            data=body();data['updated']=[data['updated'][0]+86400]
            return OptionMarketFetchResult(r,[],[data],(NOW+timedelta(days=1)).isoformat(),1,203)
    out=prod.load_market_data(groups(),store=store,now=NOW+timedelta(days=1),
                             refresh=True,client_factory=lambda _:Repair())
    assert len(calls)==1
    assert out['status']['using_previous_complete'] is False
    assert out['status']['missing_request_count']==0
