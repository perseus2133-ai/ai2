import numpy as np
import pytest

import app


@pytest.mark.parametrize(
    ('label', 'expected'),
    [
        ('300억 이상', {'000002', '000003', '000004', '000005', '000006'}),
        ('500억 이상', {'000004', '000005', '000006'}),
        ('1000억 이상', {'000006'}),
    ],
)
def test_operating_profit_thresholds_are_cumulative(valid_frame, label, expected):
    values = [299, 300, 499, 500, 999, 1000, np.nan]
    df = valid_frame.iloc[:len(values)].copy()
    for year in (2026, 2027, 2028):
        df[f'영업이익_{year}'] = values
    df['매출액_성장률_2026'] = 20
    df['영업이익_성장률_2025'] = 20
    df['영업이익_성장률_2026'] = 20

    result = app.apply_filters(
        df, rev_thresh=10, op_thresh=10, min_vol=0,
        markets=['KOSPI', 'KOSDAQ'], op_size_label=label,
    )
    assert set(result['종목코드']) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        (np.nan, ''),
        (299, '300억 미만'),
        (300, '300~500억'),
        (499, '300~500억'),
        (500, '500~1000억'),
        (999, '500~1000억'),
        (1000, '1000억 이상'),
    ],
)
def test_operating_profit_band_matches_filter_boundaries(value, expected):
    assert app.operating_profit_band(value) == expected
