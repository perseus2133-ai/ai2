#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 데일리 3선 — 매일 크롤 후 종합 점수로 코스피·코스닥 각 3종목(총 6)을 선정해 누적 기록.

'종합 판단'을 점수식으로 코드화한 것 (페이블 설계, 2026-07-22):

  [자격 요건 — 하나라도 미달이면 후보 제외]
    시총 ≥ 1,000억 · 거래대금 ≥ 10억 · 우선주/스팩 제외
    영업이익 2024~2028 존재값 전부 흑자 · 27E 또는 28E 컨센 보유
    부채비율 < 400%

  [팩터 점수 — 자격 통과 풀 내 백분위 0~100]
    ① 컨센 모멘텀 30% : 30일 전 대비 영업이익 컨센 가중 변화율
                        (26E×0.5 + 27E×0.3 + 28E×0.2)
    ② 성장성    20% : 영업이익 25→28E CAGR (폴백 26→28E)
    ③ 밸류      20% : EV·지배지분 보정 업종 상대 Upside (숏리스트만 계산)
    ④ 수급      15% : (외인+기관 5일 순매수액) / 시가총액
    ⑤ 퀄리티    15% : ROE 백분위 60% + 부채비율 역백분위 40%
    + 기술 가점 (최대 +10): OBV 매집 +3 · 정배열 +3 · MACD 상승 +2 · RSI 40~65 +2

  [선정] 코스피·코스닥 각각 상위 3종목 (동일 업종 최대 2종목).

기록: data/daily_picks.json = { "YYYY-MM-DD": [ {code,name,price,...,reasons}, ... ] }
선정가(price)는 선정 시점 현재가(전일 종가) — 앱에서 수익률 추적에 사용.
"""
import os
import json
import glob
import re
import datetime

import numpy as np
import pandas as pd

REV_WEIGHTS = {2026: 0.5, 2027: 0.3, 2028: 0.2}
FACTOR_W = {'rev': 30, 'growth': 20, 'value': 20, 'flow': 15, 'quality': 15}
SHORTLIST_N = 30
TOP_N = 3
MAX_PER_SECTOR = 2


def _is_preferred(name):
    s = str(name or '').strip()
    return len(s) >= 2 and (s.endswith('우') or s.endswith('우B'))


def _load_snapshot_before(snapshot_dir, days_ago=30, today=None):
    """today 기준 days_ago일 이전(가장 가까운) 스냅샷 dict."""
    if not os.path.isdir(snapshot_dir):
        return {}
    today = today or datetime.date.today()
    cutoff = (today - datetime.timedelta(days=days_ago)).isoformat()
    chosen = None
    for p in sorted(glob.glob(os.path.join(snapshot_dir, '*.json'))):
        d = os.path.basename(p)[:-5]
        if re.match(r'\d{4}-\d{2}-\d{2}$', d) and d <= cutoff:
            chosen = p
    if chosen is None:
        return {}
    try:
        return json.load(open(chosen, encoding='utf-8'))
    except Exception:
        return {}


def _revision_score(df, snap):
    codes = df['종목코드'].astype(str).str.zfill(6)
    ws = pd.Series(0.0, index=df.index)
    wu = pd.Series(0.0, index=df.index)
    for y, w in REV_WEIGHTS.items():
        new_v = pd.to_numeric(df.get(f'영업이익_{y}'), errors='coerce')
        old_v = pd.to_numeric(
            codes.map(lambda c, _y=y: (snap.get(c, {}) or {}).get(f'영업이익_{_y}')),
            errors='coerce')
        m = new_v.notna() & old_v.notna() & (old_v > 0)
        r = pd.Series(np.nan, index=df.index)
        r.loc[m] = (new_v[m] - old_v[m]) / old_v[m] * 100.0
        ws.loc[m] += r[m] * w
        wu.loc[m] += w
    return pd.Series(np.where(wu > 0, ws / wu, np.nan), index=df.index)


def _op_cagr(row):
    """영업이익 25→28E CAGR(%). 폴백 26→28E, 그마저 없으면 NaN."""
    def f(y):
        try:
            v = float(row.get(f'영업이익_{y}'))
            return v if pd.notna(v) and v > 0 else None
        except (TypeError, ValueError):
            return None
    a, b = f(2025), f(2028)
    if a and b:
        return ((b / a) ** (1 / 3) - 1) * 100
    a, b = f(2026), f(2028)
    if a and b:
        return ((b / a) ** 0.5 - 1) * 100
    return np.nan


def generate_daily_picks(df, snapshot_dir, history_path, today=None, top_n=TOP_N):
    """선정 + 기록. 반환: 오늘의 픽 리스트 (실패/후보부족 시 빈 리스트)."""
    today = today or datetime.date.today()
    df = df.copy()
    df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)

    num = lambda col: pd.to_numeric(df.get(col), errors='coerce')
    mcap = num('시가총액')
    price = num('현재가')
    vol = num('Recent_Volume')
    turnover_won = vol * price                       # 원
    debt = num('부채비율')

    # ── 자격 요건 ──
    ok_deficit = pd.Series(True, index=df.index)
    for y in (2024, 2025, 2026, 2027, 2028):
        v = num(f'영업이익_{y}')
        ok_deficit &= v.isna() | (v >= 0)
    has_far = num('영업이익_2027').notna() | num('영업이익_2028').notna()

    eligible = (
        mcap.notna() & (mcap >= 1000) &
        price.notna() & (price > 0) &
        (turnover_won >= 1_000_000_000) &
        ~df['종목명'].apply(_is_preferred) &
        ~df['종목명'].astype(str).str.contains('스팩') &
        ok_deficit & has_far &
        (debt.isna() | (debt < 400))
    )
    pool = df[eligible].copy()
    if len(pool) < top_n:
        return []

    # ── 팩터 원값 ──
    snap = _load_snapshot_before(snapshot_dir, 30, today)
    pool['_rev'] = _revision_score(pool, snap)
    pool['_growth'] = pool.apply(_op_cagr, axis=1)
    p_price = pd.to_numeric(pool['현재가'], errors='coerce')
    p_mcap = pd.to_numeric(pool['시가총액'], errors='coerce')
    flow_sh = (pd.to_numeric(pool.get('외인_5d'), errors='coerce').fillna(0)
               + pd.to_numeric(pool.get('기관_5d'), errors='coerce').fillna(0))
    pool['_flow'] = flow_sh * p_price / (p_mcap * 1e8) * 100     # % of mcap
    roe = pd.to_numeric(pool.get('ROE'), errors='coerce')
    p_debt = pd.to_numeric(pool.get('부채비율'), errors='coerce')

    pct = lambda s: s.rank(pct=True) * 100
    pool['s_rev'] = pct(pool['_rev'])
    pool['s_growth'] = pct(pool['_growth'])
    pool['s_flow'] = pct(pool['_flow'])
    pool['s_quality'] = pct(roe).fillna(50) * 0.6 + (100 - pct(p_debt).fillna(50)) * 0.4

    # 기술 가점
    bonus = pd.Series(0.0, index=pool.index)
    bonus += np.where(pool.get('OBV_trend', '').astype(str) == 'up', 3, 0)
    bonus += np.where(pool.get('MA_align', '').astype(str) == 'bull', 3, 0)
    bonus += np.where(pool.get('MACD_signal', '').astype(str).isin(['bull', 'bull_cross']), 2, 0)
    rsi = pd.to_numeric(pool.get('RSI'), errors='coerce')
    bonus += np.where(rsi.notna() & (rsi >= 40) & (rsi <= 65), 2, 0)
    pool['s_bonus'] = bonus

    # ── 1차 점수 (밸류 제외 가중 재정규화) ──
    w_no_val = FACTOR_W['rev'] + FACTOR_W['growth'] + FACTOR_W['flow'] + FACTOR_W['quality']
    pool['_prelim'] = (
        pool['s_rev'].fillna(30) * FACTOR_W['rev'] +
        pool['s_growth'].fillna(30) * FACTOR_W['growth'] +
        pool['s_flow'].fillna(50) * FACTOR_W['flow'] +
        pool['s_quality'] * FACTOR_W['quality']
    ) / w_no_val + pool['s_bonus']

    def _select_from(subpool, n):
        """숏리스트 → EV보정 Upside → 최종점수 → 업종 다양성 지키며 n개."""
        short = subpool.nlargest(SHORTLIST_N, '_prelim').copy()
        if short.empty:
            return []
        try:
            from industry_multiple import compute_for_target
            ups = []
            for _, r in short.iterrows():
                try:
                    res = compute_for_target(r, df)   # 피어 universe = 전체
                    ups.append(res.get('upside_pct', np.nan))
                except Exception:
                    ups.append(np.nan)
            short['_upside'] = ups
        except Exception:
            short['_upside'] = np.nan
        # 절대 앵커 매핑: -50% → 0점, +100% → 100점
        short['s_value'] = (short['_upside'].clip(-50, 100) + 50) / 150 * 100
        short['score'] = (
            short['s_rev'].fillna(30) * FACTOR_W['rev'] +
            short['s_growth'].fillna(30) * FACTOR_W['growth'] +
            short['s_value'].fillna(40) * FACTOR_W['value'] +
            short['s_flow'].fillna(50) * FACTOR_W['flow'] +
            short['s_quality'] * FACTOR_W['quality']
        ) / 100 + short['s_bonus']
        sel, sec_cnt = [], {}
        for _, r in short.sort_values('score', ascending=False).iterrows():
            sec = str(r.get('업종') or '기타')
            if sec_cnt.get(sec, 0) >= MAX_PER_SECTOR:
                continue
            sec_cnt[sec] = sec_cnt.get(sec, 0) + 1
            sel.append(r)
            if len(sel) >= n:
                break
        return sel

    # ── 코스피/코스닥 각각 top_n 선정 ──
    picks = []
    for mkt in ('KOSPI', 'KOSDAQ'):
        picks.extend(_select_from(pool[pool['시장'] == mkt], top_n))

    # ── 사유 생성 ──
    def reasons_for(r):
        out = []
        if pd.notna(r['_rev']) and r['_rev'] > 0:
            out.append(f"영업이익 컨센 30일 {r['_rev']:+.1f}% 상향")
        if pd.notna(r['_upside']):
            tag = '저평가' if r['_upside'] > 0 else '고평가'
            out.append(f"업종 대비 Upside {r['_upside']:+.0f}% (EV·지배 보정, {tag})")
        if pd.notna(r['_growth']):
            out.append(f"영업이익 CAGR(→28E) +{r['_growth']:.0f}%/년")
        if pd.notna(r['_flow']) and r['_flow'] > 0.02:
            out.append(f"외인·기관 5일 순매수 시총의 {r['_flow']:.2f}%")
        tech = []
        if str(r.get('OBV_trend')) == 'up': tech.append('OBV 매집')
        if str(r.get('MA_align')) == 'bull': tech.append('정배열')
        if str(r.get('MACD_signal')) in ('bull', 'bull_cross'): tech.append('MACD 상승')
        if tech:
            out.append(' · '.join(tech))
        roe_v = pd.to_numeric(pd.Series([r.get('ROE')]), errors='coerce').iloc[0]
        if pd.notna(roe_v) and roe_v >= 15:
            out.append(f"ROE {roe_v:.0f}%")
        return out[:5]

    records = []
    for r in picks:
        records.append({
            'code': r['종목코드'],
            'name': str(r['종목명']),
            'market': str(r.get('시장') or ''),
            'sector': str(r.get('업종') or ''),
            'price': float(pd.to_numeric(pd.Series([r['현재가']]), errors='coerce').iloc[0]),
            'mcap': float(pd.to_numeric(pd.Series([r['시가총액']]), errors='coerce').iloc[0]),
            'score': round(float(r['score']), 1),
            'rev_score': None if pd.isna(r['_rev']) else round(float(r['_rev']), 1),
            'upside': None if pd.isna(r['_upside']) else round(float(r['_upside']), 1),
            'reasons': reasons_for(r),
        })

    # ── 누적 기록 저장 (같은 날짜 재실행 시 덮어씀) ──
    hist = {}
    if os.path.exists(history_path):
        try:
            hist = json.load(open(history_path, encoding='utf-8'))
        except Exception:
            hist = {}
    hist[today.isoformat()] = records
    try:
        os.makedirs(os.path.dirname(history_path), exist_ok=True)
        json.dump(hist, open(history_path, 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
    except Exception:
        pass
    return records
