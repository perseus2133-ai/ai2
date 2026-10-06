import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest


def fixture_report():
    from market_strength import compare_market
    dates = pd.bdate_range('2026-08-10', periods=31).strftime('%Y-%m-%d').tolist()
    index = dict.fromkeys(dates, 100.)
    index[dates[-1]] = 95.
    stocks = {'000001': dict.fromkeys(dates, 100.), '000002': dict.fromkeys(dates, 100.)}
    stocks['000001'][dates[-1]] = 120.
    stocks['000002'][dates[-1]] = 98.
    del stocks['000002'][dates[-31]]
    return compare_market(pd.DataFrame({'종목코드': ['000001', '000002'],
                                       '종목명': ['상승기업', '하락기업'], '시장': ['KOSDAQ', 'KOSPI']}),
                          index, stocks, dates[-1])


def test_period_switch_positive_toggle_and_missing_data_display():
    app = AppTest.from_string('''
import streamlit as st
from market_strength_ui import render_strength_report
render_strength_report(st.session_state.frame, st.session_state.periods, '2026-09-22T06:00:00+09:00')
''')
    app.session_state['frame'], app.session_state['periods'] = fixture_report()
    app.run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 2
    assert '코스피 수익률 (%)' in app.dataframe[0].value.columns
    app.checkbox(key='market_positive').check().run()
    assert not app.exception
    assert app.dataframe[0].value['종목명'].tolist() == ['상승기업']
    app.checkbox(key='market_positive').uncheck().run()
    app.radio(key='market_horizon').set_value('세 기간 모두').run()
    assert not app.exception
    assert app.dataframe[0].value['종목명'].tolist() == ['상승기업']
    assert '30일 초과 (%p)' in app.dataframe[0].value.columns
    assert '시작일 가격 없음' in app.dataframe[1].value['30일 상태'].tolist()


def test_index_outage_is_not_presented_as_zero_matches(monkeypatch):
    import market_strength_ui as ui
    monkeypatch.setattr(ui, 'load_quotes', lambda *args: {'prices': {}, 'error': '시세 조회 실패'})
    with pytest.raises(ValueError, match='코스피'):
        ui.collect_comparison(pd.DataFrame({'종목코드': ['000001']}), '2026-09-22T06:00:00+09:00')


def test_partial_network_failure_preserves_valid_results(monkeypatch):
    import market_strength_ui as ui
    frame, periods = fixture_report()
    dates = pd.bdate_range('2026-08-10', periods=31).strftime('%Y-%m-%d').tolist()
    def load(kind, code, start, end):
        prices = dict.fromkeys(dates, 100.)
        if kind == 'item' and code == '000002':
            return {'prices': {}, 'error': '시세 조회 실패'}
        prices[dates[-1]] = 120. if kind == 'item' else 110.
        return {'prices': prices, 'error': ''}
    monkeypatch.setattr(ui, 'load_quotes', load)
    result, _ = ui.collect_comparison(frame, '2026-09-22T06:00:00+09:00')
    assert result.loc[0, 'excess_5'] == pytest.approx(10)
    assert result.loc[1, 'status_5'] == '시세 조회 실패'


def test_conflicting_market_calendars_stop_comparison(monkeypatch):
    import market_strength_ui as ui
    monkeypatch.setattr(ui, 'load_quotes', lambda kind, code, *args: {
        'prices': {'2026-09-18' if code == 'KOSPI' else '2026-09-21': 100}, 'error': ''})
    with pytest.raises(ValueError, match='거래일'):
        ui.collect_comparison(pd.DataFrame({'종목코드': ['000001']}), '2026-09-22T06:00:00+09:00')


def test_main_tab_receives_sidebar_filters_and_reacts_to_changes(monkeypatch, valid_frame):
    import app as dashboard
    import market_strength_ui as ui
    data = valid_frame.iloc[:3].copy()
    data['매출액_성장률_2026'] = [120, 10, 120]
    data['영업이익_성장률_2026'] = [120, 10, 120]
    data['영업이익_성장률_2025'] = 0
    data['평균거래량_20d'] = [200000, 200000, 1]
    monkeypatch.setattr(dashboard, 'check_password', lambda: True)
    monkeypatch.setattr(dashboard.watchlist_ui, 'initialize', lambda *args: None)
    monkeypatch.setattr(dashboard, 'get_sector_per_map', lambda: {})
    monkeypatch.setattr(dashboard, '_load_snapshot_n_days_ago', lambda **kwargs: ({}, None))
    monkeypatch.setattr(dashboard, 'load_cache', lambda: {
        'data': data.copy(), 'timestamp': pd.Timestamp('2026-09-22T06:00:00+09:00').to_pydatetime(),
        'meta': {'data_count': 3}})
    dates = pd.bdate_range('2026-08-10', periods=31).strftime('%Y-%m-%d').tolist()
    def load(kind, code, start, end):
        prices = dict.fromkeys(dates, 100.)
        prices[dates[-1]] = 120. if kind == 'item' else 110.
        return {'prices': prices, 'error': ''}
    monkeypatch.setattr(ui, 'load_quotes', load)
    at = AppTest.from_string('import app\napp.main()', default_timeout=30)
    at.session_state['main_tabs'] = '📈 시장을 이기는 종목'
    at.run()
    assert not at.exception
    assert at.dataframe[0].value['종목명'].tolist() == ['Company0']
    next(s for s in at.slider if s.label == '매출액 성장률 (% 이상)').set_value(0).run()
    assert not at.exception
    assert set(at.dataframe[0].value['종목명']) == {'Company0', 'Company1'}
