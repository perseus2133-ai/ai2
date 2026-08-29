#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주도업종 랭킹 + 업종 내 저평가 종목 발굴.

목적
----
① "지금 주가 되는 업종" Top N 을 데이터로 뽑고,
② 그 업종의 **전 종목**을 2년 뒤(28E 우선) 적정시총 대비 괴리율로 줄세워
   저평가 후보를 찾는다.

핵심 설계
--------
- 업종 강도 = 5요소 복합 점수 (모두 **중앙값/비율** 기반 → 대형주·급등주 왜곡 차단)
    가격 모멘텀 25 + 기술 강세 25 + 수급 20 + 컨센 상향 20 + 거래 활발도 10
  ⚠️ 결측 요소는 가중치에서 빼고 남은 요소로 재정규화한다.
     (현재 외인·기관 수급은 소스 결측, 과거 스냅샷엔 현재가가 없어 모멘텀이
      짧다 → 기술 강세가 '지금 강한 업종'을 대변. 데이터 복구 시 자동 반영)
- 적정시총은 industry_multiple.compute_for_target() 를 그대로 재사용
  (EV = 시총+순차입금, OP_adj = 영업이익×지배비율 보정이 이미 들어있음)
- ⚠️ 성장 보정: 단순 괴리율만 쓰면 "쌀 이유가 있는" 저성장주가 1등이 된다.
    정당멀티플 = 피어중앙값 × (1 + GROWTH_K × (자기OP성장 − 피어OP성장중앙값)/100)
                 → [0.6, 1.6] 클램프
    적정시총(보정) = 정당멀티플 × OP_adj − 순차입금
  단순 괴리율도 함께 반환해 비교 가능하게 한다.
"""

import numpy as np
import pandas as pd

import snapshot_io
from industry_multiple import (compute_for_target, _resolve_op_year,
                               _net_debt, _ctrl_ratio)

# ────────────────────────────────────────────────────────────
# 상수
# ────────────────────────────────────────────────────────────
MOMENTUM_DAYS = 20          # 모멘텀 기준 (거래일 아님, 달력일 근사)
MIN_STOCKS_PER_SECTOR = 3   # 이보다 적은 업종은 순위에서 제외
MAX_ABS_RET = 30.0          # ±30% 초과 = 감자/재상장 등 → 이상치 제외
MIN_PEERS_RELIABLE = 5      # 피어 5개 미만이면 신뢰도 경고

W_MOMENTUM, W_TECH, W_FLOW, W_REVISION, W_ACTIVITY = 25.0, 25.0, 20.0, 20.0, 10.0

GROWTH_K = 0.5              # 성장 프리미엄 강도
GROWTH_CLAMP = (0.6, 1.6)   # 정당멀티플 배수 상·하한
MIN_SALES_GROWTH = -10.0    # 매출 역성장 하한 (게이트)


def _num(v):
    v = pd.to_numeric(v, errors='coerce')
    return float(v) if pd.notna(v) else np.nan


def _pct_rank(s: pd.Series) -> pd.Series:
    """0~100 백분위. 전부 NaN이면 50(중립)."""
    valid = s.dropna()
    if valid.empty:
        return pd.Series(50.0, index=s.index)
    if valid.nunique() == 1:
        return pd.Series(np.where(s.notna(), 50.0, 50.0), index=s.index)
    r = s.rank(pct=True) * 100.0
    return r.fillna(50.0)


# ════════════════════════════════════════════════════════════
# ① 업종 모멘텀 (스냅샷 기반)
# ════════════════════════════════════════════════════════════
def sector_momentum(all_df: pd.DataFrame, snapshot_dir: str,
                    days: int = MOMENTUM_DAYS, today=None) -> pd.DataFrame:
    """days일 전 스냅샷 대비 업종별 수익률 중앙값.

    Returns: DataFrame[업종, momentum_pct, n_momentum]
             (스냅샷이 없으면 빈 DF — 호출부에서 모멘텀 없이 진행)
    """
    import datetime
    if today is None:
        today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=days)

    def _has_price(d):
        return bool(d) and any(v.get('현재가') not in (None, '') for v in d.values())

    past, used_date = None, None
    try:
        snaps = snapshot_io.list_snapshots(snapshot_dir)   # 오름차순 [(date, path)]
    except Exception:
        snaps = []

    # 1) days일 전 이하에서, cutoff에 가장 가까운 '현재가 있는' 스냅샷
    for d, p in reversed([(d, p) for d, p in snaps if d <= cutoff][-10:]):
        try:
            data = snapshot_io.read_snapshot(p)
        except Exception:
            continue
        if _has_price(data):
            past, used_date = data, d
            break
    # 2) 없으면 '현재가 있는' 가장 오래된 스냅샷으로 폴백
    #    (과거 json 스냅샷엔 현재가가 없어 기간이 짧아질 수 있음 —
    #     csv 스냅샷이 쌓이면 자동으로 원하는 기간으로 확장된다)
    if past is None:
        for d, p in snaps:
            if d >= today:
                break
            try:
                data = snapshot_io.read_snapshot(p)
            except Exception:
                continue
            if _has_price(data):
                past, used_date = data, d
                break

    if not past:
        empty = pd.DataFrame(columns=['업종', 'momentum_pct', 'n_momentum'])
        empty.attrs['momentum_days'] = 0
        return empty

    cur = all_df.copy()
    cur['__code'] = cur['종목코드'].astype(str).str.zfill(6)
    rows = []
    for _, r in cur.iterrows():
        sec = r.get('업종')
        if not isinstance(sec, str) or not sec or sec == '기타':
            continue
        p1 = _num(r.get('현재가'))
        entry = past.get(r['__code'])
        if not entry:
            continue
        p0 = _num(entry.get('현재가'))
        if not (p0 and p1) or p0 <= 0:
            continue
        ret = (p1 / p0 - 1) * 100.0
        # 감자/재상장 등 자본 이벤트 제외. ±30%는 상·하한가 경계이므로
        # 부동소수점 오차(30.000000000000004)로 배제되지 않게 여유를 둔다.
        if abs(ret) > MAX_ABS_RET + 1e-6:
            continue
        rows.append({'업종': sec, 'ret': ret})
    if not rows:
        empty = pd.DataFrame(columns=['업종', 'momentum_pct', 'n_momentum'])
        empty.attrs['momentum_days'] = 0
        return empty
    g = (pd.DataFrame(rows).groupby('업종')['ret']
         .agg(momentum_pct='median', n_momentum='size').reset_index())
    g['momentum_pct'] = g['momentum_pct'].round(2)
    # 실제로 몇 일 구간을 썼는지 (요청 days와 다를 수 있음 — 위 폴백 참조)
    g.attrs['momentum_days'] = (today - used_date).days if used_date else 0
    g.attrs['momentum_from'] = str(used_date) if used_date else ''
    return g


# ════════════════════════════════════════════════════════════
# ② 업종 랭킹 (4요소 복합)
# ════════════════════════════════════════════════════════════
def rank_sectors(all_df: pd.DataFrame, snapshot_dir: str = None,
                 top_n: int = 10, days: int = MOMENTUM_DAYS,
                 today=None) -> pd.DataFrame:
    """업종별 강도 점수 → 내림차순 DataFrame."""
    df = all_df.copy()
    if '업종' not in df.columns:
        return pd.DataFrame()
    df = df[df['업종'].notna() & (df['업종'] != '기타') & (df['업종'] != '')]
    if df.empty:
        return pd.DataFrame()

    for c in ('외인_20d', '기관_20d', '거래량배수', 'Revision_Score', '시가총액'):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    grp = df.groupby('업종')
    base = pd.DataFrame({
        'n_stocks': grp.size(),
        'mcap_sum': grp['시가총액'].sum() if '시가총액' in df.columns else np.nan,
    }).reset_index()

    # 수급: 외인·기관 20일 순매수 양수 종목 비율(%)
    def _flow_ratio(g):
        f = g['외인_20d'] > 0 if '외인_20d' in g else pd.Series(dtype=bool)
        i = g['기관_20d'] > 0 if '기관_20d' in g else pd.Series(dtype=bool)
        both = pd.concat([f, i]) if len(f) or len(i) else pd.Series(dtype=bool)
        return float(both.mean() * 100) if len(both) else np.nan
    base['flow_pct'] = grp.apply(_flow_ratio, include_groups=False).values

    # 컨센 상향 (있으면 사용)
    if 'Revision_Score' in df.columns:
        base['revision'] = grp['Revision_Score'].median().values
    else:
        base['revision'] = np.nan

    # 거래 활발도
    if '거래량배수' in df.columns:
        base['activity'] = grp['거래량배수'].median().values
    else:
        base['activity'] = np.nan

    # 기술적 강세 — 스냅샷 모멘텀이 없거나 짧을 때 '지금 강한 업종'을 대변한다.
    #   정배열·MACD 강세·OBV 매집 종목 비율 평균(0~100)에 RSI 중앙값을 가볍게 가산
    def _tech(g):
        parts = []
        if 'MA_align' in g:
            parts.append((g['MA_align'] == 'up').mean() * 100)
        if 'MACD_signal' in g:
            parts.append(g['MACD_signal'].isin(['bull', 'bull_cross']).mean() * 100)
        if 'OBV_trend' in g:
            parts.append((g['OBV_trend'] == 'up').mean() * 100)
        if not parts:
            return np.nan
        score = float(np.mean(parts))
        if 'RSI' in g:
            rsi = pd.to_numeric(g['RSI'], errors='coerce').median()
            if pd.notna(rsi):
                score = score * 0.8 + float(rsi) * 0.2   # RSI를 보조로 20%
        return score
    base['tech'] = grp.apply(_tech, include_groups=False).values

    # 가격 모멘텀
    mom_days = 0
    if snapshot_dir:
        mom = sector_momentum(all_df, snapshot_dir, days=days, today=today)
        mom_days = mom.attrs.get('momentum_days', 0)
        if not mom.empty:
            base = base.merge(mom, on='업종', how='left')
    if 'momentum_pct' not in base.columns:
        base['momentum_pct'] = np.nan
        base['n_momentum'] = 0

    # 종목 수 미달 업종 제외 (통계 신뢰도)
    base = base[base['n_stocks'] >= MIN_STOCKS_PER_SECTOR].copy()
    if base.empty:
        return base

    # ⚠️ 가용 요소만으로 가중치를 재정규화한다.
    #    (현재 외인·기관 수급은 소스 결측으로 전량 NaN, 과거 스냅샷에 현재가가
    #     없으면 모멘텀도 불가 — 죽은 요소가 가중치를 차지하면 남은 요소의
    #     변별력이 희석된다. 데이터가 복구되면 자동으로 다시 반영된다.)
    candidates = [
        ('가격모멘텀', W_MOMENTUM, base['momentum_pct']),
        ('기술강세',   W_TECH,     base['tech']),
        ('수급',       W_FLOW,     base['flow_pct']),
        ('컨센상향',   W_REVISION, base['revision']),
        ('거래활발도', W_ACTIVITY, base['activity']),
    ]
    used = [(n, w, s) for n, w, s in candidates
            if s.notna().any() and s.dropna().nunique() > 1]
    if not used:                       # 전부 결측 → 종목수 기준 폴백
        base['score'] = _pct_rank(base['n_stocks']).round(1)
        used_names = []
    else:
        tot_w = sum(w for _, w, _ in used)
        score = sum(_pct_rank(s) * (w / tot_w) for _, w, s in used)
        base['score'] = score.round(1)
        used_names = [n for n, _, _ in used]

    base = base.sort_values('score', ascending=False).reset_index(drop=True)
    base.insert(0, 'rank', base.index + 1)
    for c in ('momentum_pct', 'tech', 'flow_pct', 'revision', 'activity'):
        base[c] = pd.to_numeric(base[c], errors='coerce').round(2)
    out = base.head(top_n) if top_n else base
    out.attrs['components_used'] = used_names
    out.attrs['momentum_days'] = mom_days
    return out


# ════════════════════════════════════════════════════════════
# ③ 업종 상세 — 전 종목 괴리율 (성장 보정 포함)
# ════════════════════════════════════════════════════════════
def _growth_adjusted(res: dict, row: pd.Series, peer_op_growth_med: float):
    """정당멀티플(성장 프리미엄) 기반 적정시총·괴리율.

    Returns (fair_adj, upside_adj, premium) — 계산 불가 시 (nan, nan, nan)
    """
    label, year, op = _resolve_op_year(row)
    if year is None:
        return np.nan, np.nan, np.nan

    fair_med = res.get('fair_median')
    if fair_med is None or not pd.notna(fair_med):
        return np.nan, np.nan, np.nan

    op_adj = op * _ctrl_ratio(row)
    nd = _net_debt(row)
    if op_adj <= 0:
        return np.nan, np.nan, np.nan

    # ⚠️ 기준 멀티플은 단순 적정시총(fair_median)에서 역산한다.
    #    peer_pop_median 만 쓰면 단순 괴리율(3종 멀티플의 median 기반)과
    #    기준이 달라져 '보정 vs 단순' 비교가 부당해진다.
    #    이렇게 하면 premium=1.0 일 때 보정 == 단순 이 되어,
    #    두 값의 차이가 순수하게 성장 프리미엄 효과만 나타낸다.
    base_mult = (fair_med + nd) / op_adj
    if not pd.notna(base_mult) or base_mult <= 0:
        return np.nan, np.nan, np.nan

    own_g = _num(row.get(f'영업이익_성장률_{year}'))
    if np.isnan(own_g) or np.isnan(peer_op_growth_med):
        premium = 1.0
    else:
        premium = 1.0 + GROWTH_K * (own_g - peer_op_growth_med) / 100.0
        premium = float(np.clip(premium, *GROWTH_CLAMP))

    fair_adj = base_mult * premium * op_adj - nd
    cur = _num(row.get('시가총액'))
    upside_adj = ((fair_adj - cur) / cur * 100.0) if (cur and cur > 0) else np.nan
    return float(fair_adj), float(upside_adj), round(float(premium), 3)


def _quality_flags(row: pd.Series, res: dict, year):
    """품질 게이트 — 통과 못한 사유 목록 반환 (빈 리스트면 정상)."""
    flags = []
    if year is not None:
        op = _num(row.get(f'영업이익_{year}'))
        if not (pd.notna(op) and op > 0):
            flags.append('영업이익 적자')
        sg = _num(row.get(f'매출액_성장률_{year}'))
        if pd.notna(sg) and sg < MIN_SALES_GROWTH:
            flags.append(f'매출 역성장 {sg:.0f}%')
    else:
        flags.append('컨센 없음')
    if res.get('n_peers', 0) < MIN_PEERS_RELIABLE:
        flags.append(f"피어 {res.get('n_peers', 0)}개(표본 부족)")
    return flags


def sector_detail(sector: str, all_df: pd.DataFrame) -> pd.DataFrame:
    """업종 내 **전 종목**의 현재시총 / 적정시총 / 괴리율 테이블.

    반환 컬럼:
      종목코드 종목명 현재가 시가총액 기준연도 피어멀티플 n_peers
      적정시총 괴리율            (단순 — industry_multiple 그대로)
      적정시총_보정 괴리율_보정 성장프리미엄   (성장 보정)
      영업이익성장 매출성장 경고
    정렬: 괴리율_보정 내림차순 (저평가 우선)
    """
    if all_df is None or all_df.empty or '업종' not in all_df.columns:
        return pd.DataFrame()
    df = all_df.copy()
    sub = df[df['업종'] == sector]
    if sub.empty:
        return pd.DataFrame()

    # 피어 영업이익 성장률 중앙값 (성장 프리미엄 기준선) — 연도별로 산출
    peer_growth_by_year = {}
    for y in (2028, 2027, 2026, 2025):
        col = f'영업이익_성장률_{y}'
        if col in sub.columns:
            v = pd.to_numeric(sub[col], errors='coerce').dropna()
            peer_growth_by_year[y] = float(v.median()) if len(v) else np.nan
        else:
            peer_growth_by_year[y] = np.nan

    rows = []
    for _, row in sub.iterrows():
        try:
            res = compute_for_target(row, df)
        except Exception:
            continue
        label, year, _op = _resolve_op_year(row)
        peer_g = peer_growth_by_year.get(year, np.nan)
        fair_adj, up_adj, premium = _growth_adjusted(res, row, peer_g)
        flags = _quality_flags(row, res, year)
        rows.append({
            '종목코드': str(row.get('종목코드', '')).zfill(6),
            '종목명': row.get('종목명', ''),
            '시장': row.get('시장', ''),
            '현재가': _num(row.get('현재가')),
            '시가총액': _num(row.get('시가총액')),
            '기준연도': label or '-',
            '피어멀티플': (round(res['peer_pop_median'], 1)
                       if pd.notna(res.get('peer_pop_median')) else np.nan),
            'n_peers': res.get('n_peers', 0),
            '적정시총': res.get('fair_median', np.nan),
            '괴리율': (round(res['upside_pct'], 1)
                     if pd.notna(res.get('upside_pct')) else np.nan),
            '적정시총_보정': fair_adj,
            '괴리율_보정': round(up_adj, 1) if pd.notna(up_adj) else np.nan,
            '성장프리미엄': premium,
            '영업이익성장': _num(row.get(f'영업이익_성장률_{year}')) if year else np.nan,
            '매출성장': _num(row.get(f'매출액_성장률_{year}')) if year else np.nan,
            '경고': ' · '.join(flags),
            '상태': res.get('peer_status', ''),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values('괴리율_보정', ascending=False,
                           na_position='last').reset_index(drop=True)
