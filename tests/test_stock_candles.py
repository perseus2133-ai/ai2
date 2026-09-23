import datetime as dt
import xml.etree.ElementTree as ET

import pytest
import requests

import stock_candles as charts


@pytest.fixture(autouse=True)
def clear_cache():
    charts.load_candles.clear()
    yield
    charts.load_candles.clear()


def raw(day='20260922', **changes):
    row = {'localDate': day, 'openPrice': 100, 'highPrice': 120, 'lowPrice': 90,
           'closePrice': 110, 'accumulatedTradingVolume': 1000}
    return dict(row, **changes)


def test_exact_six_calendar_months_and_completed_days():
    assert charts.six_month_window(dt.date(2026, 9, 23)) == (dt.date(2026, 3, 23), dt.date(2026, 9, 22))
    assert charts.six_month_window(dt.date(2024, 8, 31))[0] == dt.date(2024, 2, 29)
    assert charts.six_month_window(dt.date(2026, 1, 31))[0] == dt.date(2025, 7, 31)


def test_filter_sort_deduplicate_and_exclude_today():
    payload = [raw('20260922'), raw('20260323'), raw('20260923'), raw('20260322'), raw('20260922')]
    rows, skipped = charts.normalize_candles(payload, *charts.six_month_window(dt.date(2026, 9, 23)))
    assert [r['date'] for r in rows] == ['2026-03-23', '2026-09-22']
    assert skipped == 0


@pytest.mark.parametrize('change', [{'openPrice': None}, {'closePrice': float('inf')},
                                   {'highPrice': 95}, {'lowPrice': 111}, {'openPrice': 0}])
def test_invalid_ohlc_is_not_drawn(change):
    rows, skipped = charts.normalize_candles([raw(**change)], dt.date(2026, 9, 1), dt.date(2026, 9, 22))
    assert not rows
    assert skipped == 1


def test_duplicate_conflict_rejected():
    with pytest.raises(ValueError, match='중복'):
        charts.normalize_candles([raw(), raw(closePrice=105)], dt.date(2026, 9, 1), dt.date(2026, 9, 22))


def test_missing_volume_does_not_remove_price():
    rows, _ = charts.normalize_candles([raw(accumulatedTradingVolume=None)], dt.date(2026, 9, 1), dt.date(2026, 9, 22))
    assert len(rows) == 1 and rows[0]['volume'] is None
    assert '거래량 없음' in charts.candle_svg(rows)


def test_up_down_doji_tooltips_and_constant_price_svg():
    payload = [raw('20260918'), raw('20260921', closePrice=95),
               raw('20260922', openPrice=100, highPrice=100, lowPrice=100, closePrice=100, accumulatedTradingVolume=0)]
    rows, _ = charts.normalize_candles(payload, dt.date(2026, 9, 1), dt.date(2026, 9, 22))
    svg = charts.candle_svg(rows)
    root = ET.fromstring(svg)
    assert len(root.findall('.//g')) == 3
    assert all(color in svg for color in ('#EF4444', '#3B82F6', '#94A3B8'))
    assert '시가 100' in svg and '종가 110' in svg
    assert 'nan' not in charts.candle_svg(rows[-1:]).lower()
    assert 'inf' not in charts.candle_svg(rows[-1:]).lower()
    assert charts.candle_svg([]) == ''


def test_loading_is_cached_and_only_requested_symbols(monkeypatch):
    calls = []
    class Response:
        def raise_for_status(self): pass
        def json(self): return [raw()]
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response()
    monkeypatch.setattr(charts.requests, 'get', get)
    results = charts.prefetch_candles(['005930', '005930', '000660'], dt.date(2026, 9, 23))
    charts.prefetch_candles(['005930'], dt.date(2026, 9, 23))
    assert len(calls) == 2
    assert len(results) == 2
    assert all(call[1]['params']['endDateTime'] == '202609222359' for call in calls)


def test_error_is_cached_and_safe_for_card(monkeypatch):
    calls = []
    def failed(*args, **kwargs):
        calls.append(1)
        raise requests.Timeout('private upstream details')
    monkeypatch.setattr(charts.requests, 'get', failed)
    data = charts.load_candles('005930', '2026-09-23')
    charts.load_candles('005930', '2026-09-23')
    assert len(calls) == 1
    panel = charts.candle_panel(data)
    assert '불러오지 못했습니다' in panel
    assert 'private upstream' not in panel
    assert '<svg' not in panel


def test_invalid_symbol_never_reaches_network(monkeypatch):
    def forbidden(*args, **kwargs): pytest.fail('Invalid symbol caused request')
    monkeypatch.setattr(charts.requests, 'get', forbidden)
    assert charts.load_candles('../../evil', '2026-09-23')['error']


def test_panel_empty_and_short_listing_are_honest():
    data = {'rows': [], 'error': '', 'start': '2026-03-23', 'end': '2026-09-22', 'invalid': 0}
    assert '표시할 일봉 데이터가 없습니다' in charts.candle_panel(data)
    data['rows'], _ = charts.normalize_candles([raw()], dt.date(2026, 3, 23), dt.date(2026, 9, 22))
    assert '1개 일봉' in charts.candle_panel(data)


def test_card_integration_has_separate_candles_above_growth(monkeypatch):
    from pathlib import Path
    from streamlit.testing.v1 import AppTest

    payload = [raw()]
    rows, _ = charts.normalize_candles(payload, dt.date(2026, 3, 23), dt.date(2026, 9, 22))
    data = {'rows': rows, 'error': '', 'start': '2026-03-23', 'end': '2026-09-22', 'invalid': 0}
    monkeypatch.setattr(charts, 'load_candles', lambda *args: data)
    monkeypatch.setenv('APP_PASSWORD', 'test-only-password')
    def offline(*args, **kwargs): raise requests.RequestException('offline test')
    monkeypatch.setattr(requests.Session, 'request', offline)
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'app.py'))
    app.session_state['password_correct'] = True
    app.run(timeout=30)
    assert not app.exception
    cards = [m.value for m in app.markdown if '<div class="quant-card-dark">' in m.value]
    assert cards
    for card in cards:
        assert 'qcd-chart-stack' in card
        assert card.index('qcd-candle-box') < card.index('매출 성장률')
        assert '영업이익 성장률' in card
        assert '최근 6개월 · 일봉' in card
