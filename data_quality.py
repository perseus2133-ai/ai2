"""수집 결과 검증. 실패한 데이터는 게시·모의매매에 사용하지 않는다."""
import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

KST = ZoneInfo('Asia/Seoul')
# 종목별 결측은 허용하되 시장/지표 전체 장애는 중단한다.
MIN_COVERAGE = 0.50
MIN_ROW_RETENTION = 0.80
INDICATORS = ('평균거래량_20d', 'RSI', '외인_5d', '기관_5d', '외인_20d', '기관_20d')


def validate_frame(df, previous=None):
    errors = []
    required = ('종목코드', '시장', '업종', '현재가', '시가총액', 'Recent_Volume')
    missing = [c for c in required if c not in df]
    if df.empty or missing:
        raise ValueError(f'빈 데이터 또는 필수 컬럼 누락: {missing}')
    codes = df['종목코드'].astype(str).str.zfill(6)
    if codes.duplicated().any() or not codes.str.fullmatch(r'[0-9A-Z]{6}').all() or codes.eq('000000').any():
        errors.append('종목코드 중복/형식 오류')
    for col in ('현재가', '시가총액'):
        v = pd.to_numeric(df[col], errors='coerce')
        if not (np.isfinite(v) & (v > 0)).all():
            errors.append(f'{col} 결측/비정상 값')
    volume = pd.to_numeric(df['Recent_Volume'], errors='coerce')
    if not (np.isfinite(volume) & (volume >= 0)).all():
        errors.append('거래량 결측/비정상 값')
    for market in ('KOSPI', 'KOSDAQ'):
        subset = df[df['시장'] == market]
        if subset.empty:
            errors.append(f'{market} 수집 없음')
            continue
        if previous is not None and '시장' in previous:
            old_count = int(previous['시장'].eq(market).sum())
            if len(subset) < old_count * MIN_ROW_RETENTION:
                errors.append(f'{market} 종목 수 급감: {old_count} → {len(subset)}')
        for col in INDICATORS:
            v = pd.to_numeric(subset.get(col, pd.Series(np.nan, index=subset.index)), errors='coerce')
            valid = np.isfinite(v)
            if col == '평균거래량_20d':
                valid &= v > 0
            if col == 'RSI':
                valid &= v.between(0, 100)
            if valid.mean() < MIN_COVERAGE:
                errors.append(f'{market} {col} 유효 수집률 {valid.mean():.0%}')
        sector_ok = subset['업종'].notna() & ~subset['업종'].isin(['', '기타'])
        if sector_ok.mean() < MIN_COVERAGE:
            errors.append(f'{market} 업종 매핑 부족')
    if errors:
        raise ValueError('데이터 검증 실패: ' + '; '.join(errors))


def validate_saved_data(data_dir, today=None):
    """당일 수집 완료 여부와 가격/지표를 확인한 뒤 DataFrame 반환."""
    root = Path(data_dir)
    today = today or datetime.datetime.now(KST).date()
    meta = json.loads((root / 'meta.json').read_text(encoding='utf-8'))
    ts = datetime.datetime.fromisoformat(meta['timestamp'])
    ts = ts.replace(tzinfo=KST) if ts.tzinfo is None else ts.astimezone(KST)
    if ts.date() != today:
        raise ValueError(f'수집 데이터가 오늘({today}) 것이 아닙니다: {ts.date()}')
    df = pd.read_csv(root / 'consensus_data.csv', dtype={'종목코드': str})
    if len(df) != meta.get('data_count'):
        raise ValueError('CSV와 meta.json의 종목 수 불일치')
    validate_frame(df)
    return df


if __name__ == '__main__':
    frame = validate_saved_data(Path(__file__).parent / 'data')
    print(f'데이터 검증 통과: {len(frame)}종목')
