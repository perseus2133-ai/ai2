import datetime as dt

import pandas as pd
import pytest


@pytest.fixture
def quotes():
    # A holiday is deliberately absent: offsets must follow actual market dates.
    dates = pd.bdate_range('2026-08-10', periods=33).strftime('%Y-%m-%d').tolist()
    dates.remove('2026-08-17')
    dates = dates[:31]
    index = dict.fromkeys(dates, 100.)
    index[dates[-1]] = 110.
    stocks = {'000001': dict.fromkeys(dates, 100.),
              '000002': dict.fromkeys(dates, 100.)}
    stocks['000001'][dates[-1]] = 130.
    stocks['000002'][dates[-1]] = 110.
    return dates, index, stocks


def test_same_market_dates_n_intervals_and_strict_outperformance(quotes):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    candidates = pd.DataFrame({'종목코드': ['000001', '000002'], '시장': ['KOSDAQ', 'KOSPI']})
    report, periods = compare_market(candidates, index, stocks, dates[-1])
    for n in (5, 15, 30):
        assert periods[n]['start'] == dates[-1-n]
        assert periods[n]['end'] == dates[-1]
        assert report.loc[0, f'return_{n}'] == pytest.approx(30)
        assert report.loc[0, f'excess_{n}'] == pytest.approx(20)
        assert report.loc[0, f'market_{n}'] == pytest.approx(10)
        assert select_winners(report, (n,))['종목코드'].tolist() == ['000001']
    assert select_winners(report, (5, 15, 30))['종목코드'].tolist() == ['000001']


def test_stock_missing_endpoints_is_not_shifted_or_filled(quotes):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    del stocks['000001'][dates[-16]]
    stocks['000001']['2026-12-31'] = 10000  # Never use future data.
    frame, _ = compare_market(pd.DataFrame({'종목코드': ['000001']}), index, stocks, dates[-1])
    assert frame.loc[0, 'status_15'] == '시작일 가격 없음'
    assert pd.isna(frame.loc[0, 'return_15'])
    assert len(select_winners(frame, (5,))) == 1
    assert select_winners(frame, (5, 15, 30)).empty
    del stocks['000001'][dates[-1]]
    frame, _ = compare_market(pd.DataFrame({'종목코드': ['000001']}), index, stocks, dates[-1])
    assert frame.loc[0, 'status_5'] == '종료일 가격 없음'
    assert select_winners(frame, (5,)).empty


def test_short_index_and_listing_history_remain_missing(quotes):
    from market_strength import compare_market
    dates, index, stocks = quotes
    recent_index = {d: index[d] for d in dates[-16:]}
    frame, periods = compare_market(pd.DataFrame({'종목코드': ['000001']}), recent_index, stocks, dates[-1])
    assert periods[30]['start'] is None
    assert frame.loc[0, 'status_30'] == '지수 이력 부족'
    assert pd.isna(frame.loc[0, 'return_30'])
    assert frame.loc[0, 'status_15'] == '완료'


def test_negative_return_can_beat_market_and_positive_option_excludes_it(quotes):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    index[dates[-1]] = 95
    stocks['000001'][dates[-1]] = 98
    frame, _ = compare_market(pd.DataFrame({'종목코드': ['000001']}), index, stocks, dates[-1])
    assert frame.loc[0, 'excess_5'] == pytest.approx(3)
    assert len(select_winners(frame, (5,))) == 1
    assert select_winners(frame, (5,), positive_only=True).empty


def test_all_periods_require_each_win_and_sort_by_weakest_excess(quotes):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    stocks['000002'][dates[-1]] = 120
    stocks['000001'][dates[-6]] = 120  # 5d +8.33%, below KOSPI +10%.
    frame, _ = compare_market(pd.DataFrame({'종목코드': list(stocks)}), index, stocks, dates[-1])
    assert select_winners(frame, (5, 15, 30))['종목코드'].tolist() == ['000002']
    stocks['000001'][dates[-6]] = 110  # All win, weakest excess +8.18pp vs +10pp.
    frame, _ = compare_market(pd.DataFrame({'종목코드': list(stocks)}), index, stocks, dates[-1])
    assert select_winners(frame, (5, 15, 30))['종목코드'].tolist() == ['000002', '000001']


@pytest.mark.parametrize('invalid', [0, -1, float('inf'), float('nan'), None])
def test_invalid_stock_prices_never_rank(quotes, invalid):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    stocks['000001'][dates[-1]] = invalid
    frame, _ = compare_market(pd.DataFrame({'종목코드': ['000001']}), index, stocks, dates[-1])
    assert select_winners(frame, (5,)).empty


def test_cutoff_excludes_intraday_and_does_not_exceed_filter_data():
    from market_strength import comparison_cutoff
    now = dt.datetime.fromisoformat('2026-10-07T12:00:00+09:00')
    assert comparison_cutoff('2026-10-07T06:00:00+09:00', now).isoformat() == '2026-10-06'
    assert comparison_cutoff('2026-10-04T06:00:00+09:00', now).isoformat() == '2026-10-03'
    assert comparison_cutoff('2026-10-08T06:00:00+09:00', now).isoformat() == '2026-10-06'


def test_empty_input_and_network_failure_are_explicit(quotes):
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    empty, _ = compare_market(pd.DataFrame(columns=['종목코드']), index, {}, dates[-1])
    assert select_winners(empty, (5, 15, 30)).empty
    frame, _ = compare_market(pd.DataFrame({'종목코드': ['000001']}), index, {}, dates[-1],
                              errors={'000001': '시세 조회 실패'})
    assert frame.loc[0, 'status_5'] == '시세 조회 실패'
    with pytest.raises(ValueError, match='코스피'):
        compare_market(pd.DataFrame({'종목코드': ['000001']}), {}, stocks, dates[-1])


def test_only_existing_filter_survivors_can_enter_ranking(valid_frame, quotes):
    import app
    from market_strength import compare_market, select_winners
    dates, index, stocks = quotes
    data = valid_frame.iloc[:3].copy()
    data['매출액_성장률_2026'] = [120, 10, 120]
    data['영업이익_성장률_2026'] = [120, 10, 120]
    data['영업이익_성장률_2025'] = 0
    data['평균거래량_20d'] = [200000, 200000, 1]  # Last row fails liquidity.
    for code in data['종목코드']:
        stocks[code] = dict(stocks['000001'])
    before = data.copy(deep=True)
    filtered = app.apply_filters(data, 100, 100, 0, ['KOSPI', 'KOSDAQ'],
                                 min_amt=10, op_size_label='500억 이상')
    frame, _ = compare_market(filtered, index, stocks, dates[-1])
    assert select_winners(frame, (5,))['종목코드'].tolist() == ['000001']
    pd.testing.assert_frame_equal(data, before)
