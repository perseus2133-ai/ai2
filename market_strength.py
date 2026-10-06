"""현재 스크리너 통과 종목의 5·15·30거래일 코스피 대비 가격수익률."""
import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

from picks_performance import positive

HORIZONS = (5, 15, 30)
KST = ZoneInfo('Asia/Seoul')


def comparison_cutoff(data_timestamp, now=None):
    """장중 값 제외. 필터 데이터 수집일보다 최신인 가격을 섞지 않는다."""
    now = now or dt.datetime.now(KST)
    timestamp = pd.Timestamp(data_timestamp)
    timestamp = timestamp.tz_localize(KST) if timestamp.tzinfo is None else timestamp.tz_convert(KST)
    return min(timestamp.date(), now.astimezone(KST).date()) - dt.timedelta(days=1)


def compare_market(candidates, benchmark, prices, cutoff, errors=None):
    """N거래일 = 같은 코스피 거래일 T와 T-N의 종가 비교(총 N+1개 관측일).

    이미 필터링된 후보만 보존한다. 종목별 최근 N행으로 날짜를 바꾸거나
    없는 가격을 보간하지 않는다. 코스닥 종목도 사용자 요청대로 코스피와 비교한다.
    """
    cutoff = str(cutoff)
    index = {d: positive(v) for d, v in benchmark.items() if d <= cutoff}
    if not index or any(v is None for v in index.values()):
        raise ValueError('코스피 종가 데이터가 없거나 유효하지 않습니다.')
    calendar = sorted(index)
    end = calendar[-1]
    periods = {}
    frame = candidates.copy().reset_index(drop=True)
    frame['종목코드'] = frame['종목코드'].astype(str).str.zfill(6)
    frame['평가종가'] = frame['종목코드'].map(lambda c: positive(prices.get(c, {}).get(end)))
    errors = errors or {}
    for n in HORIZONS:
        start = calendar[-1-n] if len(calendar) > n else None
        market = (index[end] / index[start] - 1) * 100 if start else None
        periods[n] = {'start': start, 'end': end, 'market_return': market}
        returns, statuses = [], []
        for code in frame['종목코드']:
            stock = prices.get(code, {})
            a, b = positive(stock.get(start)), positive(stock.get(end))
            status = ('지수 이력 부족' if start is None else
                      errors[code] if code in errors else
                      '종료일 가격 없음' if b is None else
                      '시작일 가격 없음' if a is None else '완료')
            returns.append((b / a - 1) * 100 if status == '완료' else float('nan'))
            statuses.append(status)
        frame[f'return_{n}'] = pd.Series(returns, dtype=float)
        frame[f'market_{n}'] = market if market is not None else float('nan')
        frame[f'excess_{n}'] = frame[f'return_{n}'] - frame[f'market_{n}']
        frame[f'status_{n}'] = pd.Series(statuses, dtype=str)
    return frame, periods


def select_winners(frame, horizons, positive_only=False):
    """선택한 모든 기간에서 초과수익 > 0. 모두 보기의 정렬은 최소 초과수익순."""
    horizons = tuple(horizons)
    if not horizons or any(n not in HORIZONS for n in horizons):
        raise ValueError('비교 기간은 5·15·30거래일 중 선택해야 합니다.')
    mask = pd.Series(True, index=frame.index)
    for n in horizons:
        mask &= frame[f'status_{n}'].eq('완료') & frame[f'excess_{n}'].gt(1e-9)
        if positive_only:
            mask &= frame[f'return_{n}'].gt(1e-9)
    result = frame.loc[mask].copy()
    result['최소초과수익'] = result[[f'excess_{n}' for n in horizons]].min(axis=1, skipna=False)
    return result.sort_values(['최소초과수익', '종목코드'], ascending=[False, True]).reset_index(drop=True)
