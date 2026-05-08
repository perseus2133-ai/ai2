#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업종 상대 멀티플 기반 적정시총 밴드 계산.

- 멀티플: POP = 시가총액 / 영업이익 (peer-relative price-to-OP)
- 피어 정의: 동일 업종 + 시총·영업이익 둘 다 양수 (대상 종목 제외)
- 3가지 통계: median / aggregate(합계기반) / trimmed mean(10% 양측 절단)
- 자동 폴백: 28E → 27E → 26E → 25E (양수 영업이익 기준)
- '기타' 업종 또는 피어 0개 시: 빈 결과 + 사유 명시

대상 외부 함수
- _resolve_op_year(target_row) -> (label, year, op_value)
- select_peers(target_row, universe_df, year) -> peer DataFrame
- compute_for_target(target_row, universe_df) -> dict (모든 결과)
"""

import numpy as np
import pandas as pd

# ────────────────────────────────────────────────────────────
# 상수
# ────────────────────────────────────────────────────────────
PEER_YEARS_PRIORITY = (2028, 2027, 2026, 2025)
TRIM_PCT = 0.10                # 양측 10% 절단
MIN_PEERS_FOR_TRIM = 5         # 절단 평균은 5개 이상일 때만 의미


# ────────────────────────────────────────────────────────────
# 1. 영업이익 연도 자동 폴백
# ────────────────────────────────────────────────────────────
def _resolve_op_year(target_row):
    """대상 종목에서 양수 영업이익이 있는 가장 미래 연도를 선택.
    Returns (label, year, value):
        label  : "'28E", "'27E", "'26E", "'25E" 중 하나 또는 ''
        year   : 정수 연도 또는 None
        value  : float 영업이익 (억) 또는 None
    """
    for y in PEER_YEARS_PRIORITY:
        v = target_row.get(f'영업이익_{y}')
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if pd.notna(v) and v > 0:
            return f"'{str(y)[2:]}E", y, v
    return '', None, None


# ────────────────────────────────────────────────────────────
# 2. 피어 셀렉션
# ────────────────────────────────────────────────────────────
def select_peers(target_row, universe_df, year):
    """동일 업종 피어 데이터프레임을 반환.
    - 대상 종목 제외
    - 해당 연도의 영업이익·시총이 둘 다 양수인 종목만
    - '기타' 업종 또는 universe에 '업종' 컬럼 없으면 빈 DF
    """
    if universe_df is None or len(universe_df) == 0:
        return pd.DataFrame()

    sector = target_row.get('업종')
    if not sector or pd.isna(sector) or sector == '기타':
        return pd.DataFrame()

    if '업종' not in universe_df.columns:
        return pd.DataFrame()

    op_col = f'영업이익_{year}'
    if op_col not in universe_df.columns or '시가총액' not in universe_df.columns:
        return pd.DataFrame()

    target_code = str(target_row.get('종목코드', '')).zfill(6)
    udf = universe_df.copy()
    udf['__code'] = udf['종목코드'].astype(str).str.zfill(6)

    op_vals = pd.to_numeric(udf[op_col], errors='coerce')
    mcap    = pd.to_numeric(udf['시가총액'], errors='coerce')

    mask = (
        (udf['업종'] == sector) &
        (udf['__code'] != target_code) &
        op_vals.notna() & (op_vals > 0) &
        mcap.notna()    & (mcap > 0)
    )
    return udf[mask].drop(columns='__code', errors='ignore')


# ────────────────────────────────────────────────────────────
# 3. 메인 계산
# ────────────────────────────────────────────────────────────
def _empty_result():
    return {
        'peer_pop_median':    np.nan,
        'peer_pop_aggregate': np.nan,
        'peer_pop_trimmed':   np.nan,
        'fair_min':           np.nan,
        'fair_median':        np.nan,
        'fair_max':           np.nan,
        'upside_pct':         np.nan,
        'peer_year_used':     '',
        'n_peers':            0,
        'is_fallback_year':   False,
        'peer_status':        'no_year',   # ok / no_year / no_sector / no_peers
    }


def compute_for_target(target_row, universe_df):
    """대상 종목의 업종 상대 멀티플 + 적정시총 밴드 + Upside 계산.

    Returns dict (always — never raises). Keys:
        peer_pop_median / peer_pop_aggregate / peer_pop_trimmed
        fair_min / fair_median / fair_max
        upside_pct
        peer_year_used (e.g. "'28E", "'27E"; '' if no data)
        n_peers
        is_fallback_year (True면 28E 외 다른 해 사용)
        peer_status ('ok' / 'no_year' / 'no_sector' / 'no_peers')
    """
    out = _empty_result()

    # 1) 영업이익 연도 결정
    label, year, target_op = _resolve_op_year(target_row)
    if year is None:
        return out
    out['peer_year_used'] = label
    out['is_fallback_year'] = (year != 2028)

    # 2) 업종 체크
    sector = target_row.get('업종')
    if not sector or pd.isna(sector) or sector == '기타':
        out['peer_status'] = 'no_sector'
        return out

    # 3) 피어 추출
    peers = select_peers(target_row, universe_df, year)
    if peers.empty:
        out['peer_status'] = 'no_peers'
        return out

    op_col = f'영업이익_{year}'
    op_vals = pd.to_numeric(peers[op_col], errors='coerce')
    mcap    = pd.to_numeric(peers['시가총액'], errors='coerce')
    pop     = (mcap / op_vals).replace([np.inf, -np.inf], np.nan).dropna()

    if pop.empty:
        out['peer_status'] = 'no_peers'
        return out

    # 4) 3종 멀티플
    out['n_peers'] = int(len(pop))
    out['peer_pop_median'] = float(pop.median())

    op_sum = float(op_vals.sum())
    if op_sum > 0:
        out['peer_pop_aggregate'] = float(mcap.sum() / op_sum)

    if len(pop) >= MIN_PEERS_FOR_TRIM:
        sorted_pop = pop.sort_values()
        k = max(1, int(len(sorted_pop) * TRIM_PCT))
        trimmed = sorted_pop.iloc[k:-k] if k > 0 else sorted_pop
        out['peer_pop_trimmed'] = float(trimmed.mean()) if not trimmed.empty else float(pop.mean())
    else:
        out['peer_pop_trimmed'] = float(pop.mean())

    # 5) 적정시총 밴드 + Upside
    multiples = [out['peer_pop_median'], out['peer_pop_aggregate'], out['peer_pop_trimmed']]
    multiples = [m for m in multiples if pd.notna(m) and m > 0]

    if multiples and target_op > 0:
        fairs = [m * target_op for m in multiples]
        out['fair_min']    = float(min(fairs))
        out['fair_median'] = float(np.median(fairs))
        out['fair_max']    = float(max(fairs))

        try:
            cur_mcap = float(target_row.get('시가총액', np.nan))
        except (TypeError, ValueError):
            cur_mcap = np.nan
        if pd.notna(cur_mcap) and cur_mcap > 0:
            out['upside_pct'] = (out['fair_median'] - cur_mcap) / cur_mcap * 100.0

    out['peer_status'] = 'ok'
    return out
