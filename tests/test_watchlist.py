import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import pytest

from watchlist import WatchStore, make_entry, opinion


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
    at.checkbox(key='missing').check().run()
    assert not at.exception
    next(c for c in at.checkbox if '삭제 확인' in c.label).check().run()
    next(b for b in at.button if b.label == '관심목록에서 해제').click().run()
    assert not at.exception
    assert not WatchStore(tmp_path / 'watch.sqlite3').entries()
