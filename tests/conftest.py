import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def valid_frame():
    rows = []
    for i in range(10):
        r = {
            '종목코드': f'{i + 1:06d}', '종목명': f'Company{i}',
            '시장': 'KOSPI' if i < 5 else 'KOSDAQ', '업종': f'Sector{i % 3}',
            '현재가': 10000., '시가총액': 3000., 'Recent_Volume': 1000000.,
            '평균거래량_20d': 100000., 'RSI': 55., 'PER': 10., 'ROE': 15.,
            '부채비율': 100., '순차입금': 0., '지배비율': 1.,
            '외인_5d': 100., '기관_5d': 100., '외인_20d': 100., '기관_20d': 100.,
            'MA_align': 'up', 'OBV_trend': 'flat', 'MACD_signal': '',
        }
        for y in range(2024, 2029):
            r[f'매출액_{y}'] = 1000. + (y - 2024) * 100
            r[f'영업이익_{y}'] = 100. + (y - 2024) * 100
        rows.append(r)
    return pd.DataFrame(rows)
