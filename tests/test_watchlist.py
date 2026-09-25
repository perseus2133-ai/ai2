import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import pytest

from watchlist import WatchStore, make_entry, opinion, summary


def entry():
    return make_entry({'종목코드': '005930', '종목명': '테스트', 'PER': float('nan'),
                       '영업이익_2026': 100, '영업이익_2028': 200},
                      {'rows': [{'date': '2026-09-23', 'close': 100}]}, '2026-09-24',
                      dt.datetime(2026, 9, 24, tzinfo=ZoneInfo('Asia/Seoul')))


def test_persistence_duplicate_and_remove(tmp_path):
    path = tmp_path / 'watch.sqlite3'
    store = WatchStore(path)
    initial = entry()
    store.add(initial)
    store.add(dict(initial, price=200))
    assert WatchStore(path).entries()['005930']['price'] == 100
    assert initial['snapshot']['PER'] is None
    store.remove('005930')
    assert not store.entries()


def test_backup_merge_and_atomic_invalid_restore(tmp_path):
    store = WatchStore(tmp_path / 'watch.sqlite3')
    store.add(entry())
    other = WatchStore(tmp_path / 'other.sqlite3')
    other.restore(store.backup())
    assert other.entries() == store.entries()
    with pytest.raises((ValueError, KeyError)):
        other.restore(json.dumps({'version': 1, 'entries': [dict(entry(), code='000001'), {'code': 'bad'}]}))
    assert list(other.entries()) == ['005930']


def test_concurrent_inserts_do_not_lose_records(tmp_path):
    store = WatchStore(tmp_path / 'watch.sqlite3')
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda i: store.add(dict(entry(), code=f'{i:06d}')), range(12)))
    assert len(store.entries()) == 12


def test_missing_chart_does_not_invent_price_date():
    with pytest.raises(ValueError, match='일봉'):
        make_entry({'종목코드': '005930', '현재가': 100}, {}, '')


def test_today_candle_not_used():
    with pytest.raises(ValueError, match='일봉'):
        make_entry({'종목코드': '005930'}, {'rows': [{'date': '2026-09-24', 'close': 100}]}, '',
                   dt.datetime(2026, 9, 24, tzinfo=ZoneInfo('Asia/Seoul')))


def test_opinion_is_transparent_and_handles_missing():
    text = opinion({}, {})
    assert '보류' in text and '생성형 AI 분석이 아닌' in text
    rows = [{'date': '2026-09-23', 'close': 100, 'volume': 100, 'ma5': 100, 'ma20': 90, 'ma60': 80}] * 21
    text = opinion({'영업이익_2026': 100, '영업이익_2028': 200}, {'rows': rows})
    assert '정배열' in text and '1.00배' in text and '200억원' in text


def test_summary_uses_recent_intraday_low_and_cached_price():
    selected = entry()
    selected['price'] = 100
    chart = {'end': '2026-09-24', 'rows': [
        {'date': '2026-07-25', 'low': 40, 'close': 90, 'volume': 100},
        {'date': '2026-08-01', 'low': 80, 'close': 95, 'volume': 100},
        {'date': '2026-09-23', 'low': 90, 'close': 110, 'volume': 200},
    ]}
    row = {'현재가': 120, '시가총액': 12_000, 'Recent_Volume': 300,
           '평균거래량_20d': 100, '매출액_2026': 1000, '매출액_2028': 2000,
           '영업이익_2026': 100, '영업이익_2028': 200}
    result = summary(selected, row, chart, '2026-09-24T05:00:00+09:00')
    assert result['price_source'] == '캐시 현재가'
    assert result['since_selected_pct'] == pytest.approx(20)
    assert result['low_60d'] == 80
    assert result['low_60d_date'] == '2026-08-01'
    assert result['since_low_pct'] == pytest.approx(50)
    assert result['volume_multiple'] == pytest.approx(3)
    assert result['turnover_eok'] == pytest.approx(120 * 300 / 1e8)
    assert result['market_cap'] == 12_000


def test_summary_falls_back_to_completed_close_without_current_data():
    selected = entry()
    chart = {'end': '2026-09-24', 'rows': [
        {'date': '2026-09-23', 'low': 80, 'close': 110, 'volume': 200},
    ]}
    result = summary(selected, None, chart, '2026-09-24T05:00:00+09:00')
    assert result['price'] == 110
    assert result['price_source'] == '최근 완료 종가'
    assert result['financial_source'] == '지정 당시 저장값'
    assert result['low_60d'] == 80
    assert result['volume'] == 200


def test_summary_prefers_newer_completed_close_over_stale_cache():
    selected = entry()
    chart = {'end': '2026-09-25', 'rows': [
        {'date': '2026-09-24', 'low': 90, 'close': 120, 'volume': 250},
    ]}
    result = summary(selected, {'현재가': 110, 'Recent_Volume': 100}, chart,
                     '2026-09-23T05:00:00+09:00')
    assert result['price'] == 120
    assert result['price_source'] == '최근 완료 종가'
    assert result['volume'] == 250


def test_summary_does_not_compare_pre_registration_price():
    selected = entry()
    result = summary(selected, {'현재가': 120}, {'end': '2026-09-24', 'rows': []},
                     '2026-09-22T05:00:00+09:00')
    assert result['price'] is None
    assert result['since_selected_pct'] is None


def test_summary_list_shows_every_entry_and_escapes_names():
    import watchlist_ui as ui
    entries = [dict(entry(), code=f'{i:06d}', name='<script>alert(1)</script>' if i == 0 else f'종목{i}')
               for i in range(12)]
    blank = {'price': None, 'price_source': '시세 없음', 'price_date': '',
             'since_selected_pct': None, 'low_60d': None, 'low_60d_date': '',
             'since_low_pct': None, 'market_cap': None, 'volume': None,
             'volume_multiple': None, 'turnover_eok': None,
             'revenue_2026': None, 'revenue_2028': None,
             'op_2026': None, 'op_2028': None, 'financial_source': '지정 당시 저장값'}
    markup = ui._summary_html(entries, {e['code']: blank for e in entries})
    assert markup.count('class="watch-sum-row"') == 12
    assert '&lt;script&gt;' in markup
    assert '<script>' not in markup


def ui_app():
    import streamlit as st
    import pandas as pd
    import watchlist_ui as ui
    import app
    st.session_state['_watch_render_count'] = st.session_state.get('_watch_render_count', 0) + 1
    ui.initialize('.')
    ui.st.session_state['_watch_data_as_of'] = '2026-09-24'
    frame = pd.read_csv('data/consensus_data.csv', dtype={'종목코드': str}).head(1)
    row = app.compute_card_fields(frame).iloc[0]
    chart = {'rows': [{'date': '2026-09-23', 'open': 100, 'high': 110, 'low': 90,
                       'close': 100, 'volume': 100, 'ma5': 100, 'ma20': 99, 'ma60': 98}],
             'error': '', 'start': '2026-03-24', 'end': '2026-09-23', 'invalid': 0}
    ui.prefetch_candles = lambda codes: {c: chart for c in codes}
    app.render_stock_card(row, 1, chart)
    app.render_stock_card(row, 2, chart)
    if st.checkbox('현재 데이터에서 제외', key='missing'):
        frame = frame.iloc[:0]
    ui.render_watchlist(frame, app.render_stock_card, app.compute_card_fields, '2026-09-24')


def test_ui_add_rerun_missing_record_and_remove(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv('AI2_WATCHLIST_DB', str(tmp_path / 'watch.sqlite3'))
    # ASCII launcher avoids AppTest's locale-dependent temporary source encoding on Windows.
    launcher = "import sys\nsys.path.insert(0, 'tests')\nfrom test_watchlist import ui_app\nui_app()"
    at = AppTest.from_string(launcher, default_timeout=30).run()
    assert not at.exception
    before = at.session_state['_watch_render_count']
    next(b for b in at.button if b.label == '☆ 관심종목 등록').click().run()
    assert not at.exception
    assert at.session_state['_watch_render_count'] == before + 1
    assert len(WatchStore(tmp_path / 'watch.sqlite3').entries()) == 1
    assert any('AI 의견' in m.value for m in at.markdown)
    assert any('watch-sum-list' in m.value and '60일 저점' in m.value for m in at.markdown)
    at.checkbox(key='missing').check().run()
    assert not at.exception
    next(c for c in at.checkbox if '삭제 확인' in c.label).check().run()
    next(b for b in at.button if b.label == '관심목록에서 해제').click().run()
    assert not at.exception
    assert not WatchStore(tmp_path / 'watch.sqlite3').entries()
