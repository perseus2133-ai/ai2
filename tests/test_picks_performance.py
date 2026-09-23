import datetime as dt
import json

import pytest

import picks_performance as perf


@pytest.fixture
def history():
    return {'2026-07-02': [{'code': '000001', 'name': 'First', 'market': 'KOSPI', 'price': 999}],
            '2026-07-06': [{'code': '000001', 'name': 'Again', 'market': 'KOSPI', 'price': 9999}]}


@pytest.fixture
def calendar():
    import pandas as pd
    return [d.date().isoformat() for d in pd.bdate_range('2026-07-01', periods=65)]


def make_report(history, calendar, prices=None, cutoff=None):
    index = {d: 1000 + i * 10 for i, d in enumerate(calendar)}
    stocks = prices if prices is not None else {'000001': {d: 100 + i * 2 for i, d in enumerate(calendar)}}
    return perf.build_report(history, {'KOSPI': index, 'KOSDAQ': index}, stocks,
                             dt.date.fromisoformat(cutoff or calendar[-1]))


def test_first_selection_and_equal_dates(history, calendar):
    report = make_report(history, calendar)
    assert len(report['rows']) == 1
    row = report['rows'][0]
    assert row['selected'] == '2026-07-02'
    assert row['base_date'] == '2026-07-01'
    for horizon in (5, 20, 60):
        result = row['periods'][str(horizon)]
        assert result['end_date'] == calendar[horizon]
        assert result['stock_return'] == pytest.approx(horizon * 2)
        assert result['market_return'] == pytest.approx(horizon)
        assert result['excess_pp'] == pytest.approx(horizon)
    assert history['2026-07-02'][0]['price'] == 999  # 저장된 선정가와 차트 가격을 섞지 않는다.


def test_immature_period_not_zero_or_future(history, calendar):
    report = make_report(history, calendar, cutoff=calendar[6])
    periods = report['rows'][0]['periods']
    assert periods['latest']['end_date'] == calendar[6]
    assert periods['5']['status'] == '완료'
    assert periods['20']['status'] == '기간 미도래'
    assert periods['20']['stock_return'] is None


def test_holiday_uses_market_calendar_not_weekdays(history, calendar):
    holiday_removed = [d for d in calendar if d != '2026-07-03']
    report = make_report(history, holiday_removed)
    assert report['rows'][0]['periods']['5']['end_date'] == calendar[6]


def test_weekend_selection_starts_before_weekend(calendar):
    history = {'2026-07-04': [{'code': '000001', 'name': 'A', 'market': 'KOSPI'}]}
    report = make_report(history, calendar)
    assert report['rows'][0]['base_date'] == '2026-07-03'
    assert report['rows'][0]['periods']['5']['end_date'] == '2026-07-10'


@pytest.mark.parametrize('missing', ['base', 'exit'])
def test_missing_or_delisted_prices_not_forward_filled(history, calendar, missing):
    prices = {d: 100 for d in calendar}
    del prices[calendar[0 if missing == 'base' else 5]]
    result = make_report(history, calendar, {'000001': prices})['rows'][0]['periods']['5']
    assert result['status'] == '종목 가격 누락'
    assert result['stock_return'] is None


def test_unknown_market_and_missing_base_are_explicit(calendar):
    history = {'2026-07-01': [{'code': '000001', 'name': 'A', 'market': 'KOSPI'},
                              {'code': '000002', 'name': 'B'}]}
    rows = make_report(history, calendar)['rows']
    assert rows[0]['periods']['5']['status'] == '기준일 없음'
    assert rows[1]['periods']['5']['status'] == '시장 정보 없음'


def test_future_selection_not_evaluated(calendar):
    history = {calendar[10]: [{'code': '000001', 'name': 'A', 'market': 'KOSPI'}]}
    report = make_report(history, calendar, cutoff=calendar[6])
    assert report['rows'][0]['periods']['latest']['status'] == '기간 미도래'


def test_fetch_filters_bounds_bad_prices_and_sorts(monkeypatch):
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return [{'localDate': '20260703', 'closePrice': 20},
                    {'localDate': '20260701', 'closePrice': 10},
                    {'localDate': '20260702', 'closePrice': float('inf')},
                    {'localDate': '20260704', 'closePrice': 30}]
    monkeypatch.setattr(perf.requests, 'get', lambda *args, **kwargs: Response())
    result = perf.fetch_closes('item', '000001', dt.date(2026, 7, 1), dt.date(2026, 7, 3))
    assert list(result) == ['2026-07-01', '2026-07-03']


@pytest.fixture
def collection(tmp_path, monkeypatch, history, calendar):
    (tmp_path / 'daily_picks.json').write_text(json.dumps(history), encoding='utf-8')
    (tmp_path / 'meta.json').write_text(json.dumps({'timestamp': '2026-07-15T05:00:00+09:00'}))
    def fetch(kind, symbol, start, end):
        return {d: 100 + i for i, d in enumerate(calendar) if d <= end.isoformat()}
    monkeypatch.setattr(perf, 'fetch_closes', fetch)
    return tmp_path


def test_collection_excludes_current_day_and_does_not_edit_picks(collection):
    original = (collection / 'daily_picks.json').read_bytes()
    report = perf.collect_report(collection, now=dt.datetime(2026, 7, 15, 10, tzinfo=perf.KST))
    assert report['cutoff'] == '2026-07-14'
    assert report['rows'][0]['periods']['latest']['end_date'] == '2026-07-14'
    assert (collection / 'daily_picks.json').read_bytes() == original
    assert json.loads((collection / 'daily_picks_performance.json').read_text(encoding='utf-8'))['schema_version'] == 1


def test_benchmark_failure_preserves_previous_report(collection, monkeypatch):
    target = collection / 'daily_picks_performance.json'
    target.write_text('previous', encoding='utf-8')
    def failed(*args): raise RuntimeError('test outage')
    monkeypatch.setattr(perf, 'fetch_closes', failed)
    with pytest.raises(RuntimeError):
        perf.collect_report(collection)
    assert target.read_text(encoding='utf-8') == 'previous'


def test_different_market_calendars_are_rejected(collection, monkeypatch):
    monkeypatch.setattr(perf, 'fetch_closes', lambda kind, symbol, *a: {'2026-07-01' if symbol == 'KOSPI' else '2026-07-02': 100})
    with pytest.raises(ValueError, match='거래일 불일치'):
        perf.collect_report(collection)


def test_partial_stock_failure_is_visible(collection, monkeypatch):
    h = json.loads((collection / 'daily_picks.json').read_text(encoding='utf-8'))
    h['2026-07-02'].append({'code': '000002', 'name': 'Missing', 'market': 'KOSDAQ'})
    (collection / 'daily_picks.json').write_text(json.dumps(h), encoding='utf-8')
    original_fetch = perf.fetch_closes
    def fetch(kind, symbol, *args):
        if symbol == '000002': raise RuntimeError('missing stock')
        return original_fetch(kind, symbol, *args)
    monkeypatch.setattr(perf, 'fetch_closes', fetch)
    report = perf.collect_report(collection, now=dt.datetime(2026, 7, 15, 10, tzinfo=perf.KST))
    assert report['errors'] == {'000002': 'RuntimeError'}
    assert len(report['rows']) == 2
    assert report['rows'][1]['periods']['5']['status'] == '종목 가격 누락'


def test_view_switching_and_empty_period(history, calendar):
    from streamlit.testing.v1 import AppTest
    app = AppTest.from_string('from performance_view import render_report\nimport streamlit as st\nrender_report(st.session_state.report)')
    app.session_state['report'] = make_report(history, calendar, cutoff=calendar[6])
    app.run()
    assert not app.exception
    assert app.metric[0].value == '1 / 1'
    app.selectbox[0].set_value('20').run()
    assert not app.exception
    assert app.metric[0].value == '0 / 1'
    assert app.metric[1].value == '—'
    app.radio[0].set_value('KOSDAQ').run()
    assert not app.exception
    assert '기록이 없습니다' in app.info[0].value


def test_stale_history_hides_report(tmp_path, history, calendar):
    from streamlit.testing.v1 import AppTest
    report = make_report(history, calendar)
    report['history_digest'] = 'stale'
    (tmp_path / 'daily_picks_performance.json').write_text(json.dumps(report), encoding='utf-8')
    app = AppTest.from_string('from performance_view import render_performance\nimport streamlit as st\nrender_performance(st.session_state.root, st.session_state.history)')
    app.session_state['root'] = str(tmp_path)
    app.session_state['history'] = history
    app.run()
    assert not app.exception
    assert len(app.warning) == 1
    assert len(app.dataframe) == 0


def test_kosdaq_uses_its_own_benchmark(history, calendar):
    history['2026-07-02'][0]['market'] = 'KOSDAQ'
    benchmarks = {'KOSPI': {d: 100 for d in calendar},
                  'KOSDAQ': {d: 100 + i for i, d in enumerate(calendar)}}
    report = perf.build_report(history, benchmarks, {'000001': {d: 100 for d in calendar}},
                               dt.date.fromisoformat(calendar[-1]))
    result = report['rows'][0]['periods']['5']
    assert result['market_return'] == pytest.approx(5)
    assert result['excess_pp'] == pytest.approx(-5)


def test_all_stock_failure_does_not_publish_empty_success(collection, monkeypatch):
    fetch = perf.fetch_closes
    def failed_items(kind, symbol, *args):
        if kind == 'item': raise RuntimeError('down')
        return fetch(kind, symbol, *args)
    monkeypatch.setattr(perf, 'fetch_closes', failed_items)
    with pytest.raises(RuntimeError, match='전 종목'):
        perf.collect_report(collection)
    assert not (collection / 'daily_picks_performance.json').exists()


@pytest.mark.parametrize('same_timestamp', [True, False])
def test_view_checks_data_timestamp(collection, same_timestamp):
    from streamlit.testing.v1 import AppTest
    perf.collect_report(collection, now=dt.datetime(2026, 7, 15, 10, tzinfo=perf.KST))
    if not same_timestamp:
        (collection / 'meta.json').write_text(json.dumps({'timestamp': '2026-07-16T05:00:00+09:00'}))
    app = AppTest.from_string('from performance_view import render_performance\nimport streamlit as st\nrender_performance(st.session_state.root, st.session_state.history)')
    app.session_state['root'] = str(collection)
    app.session_state['history'] = json.loads((collection / 'daily_picks.json').read_text(encoding='utf-8'))
    app.run()
    assert not app.exception
    assert len(app.dataframe) == (1 if same_timestamp else 0)
    assert len(app.warning) == (0 if same_timestamp else 1)
