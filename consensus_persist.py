#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
컨센서스 데이터 지속성(carry-forward) 레이어.

근본 문제:
    FnGuide(comp.fnguide.com SVD_Main.asp)는 봇/캐시 정책 때문에 날마다
    간헐적으로 차단되어(모든 요청에 정적 삼성전자 페이지 반환) 27E·28E를
    통째로 못 받는 날이 생긴다. 매 크롤이 CSV를 통째로 덮어쓰므로,
    차단된 날엔 어제 잘 받은 27E·28E가 NaN으로 날아간다. → "자꾸 사라짐"

해결:
    컨센서스 추정치는 천천히(월 단위) 바뀌므로, 이번 크롤에서 NaN이면
    최근 스냅샷(data/consensus_snapshots/*.json)에 남아 있는
    '마지막으로 받은 좋은 값'을 가져와 메운다(carry-forward).
    기본 보존 한도 45일 — 그보다 오래된 값은 신선하지 않다고 보고 버린다.

    이렇게 하면 FnGuide가 막힌 날에도 27E·28E가 유지되고, 막히지 않은
    날엔 새 값으로 자연스럽게 갱신된다(best-effort fresh + last-known-good).
"""

import os
import json
import glob
import datetime

import numpy as np
import pandas as pd

# carry-forward 대상 (스냅샷에 저장되는 8개 키)
CARRY_FIELDS = [f'{m}_{y}'
                for m in ('매출액', '영업이익')
                for y in (2025, 2026, 2027, 2028)]

DEFAULT_MAX_AGE_DAYS = 45


def _parse_snapshot_date(filename):
    base = os.path.basename(filename)[:-5]  # strip .json
    try:
        return datetime.datetime.strptime(base, '%Y-%m-%d').date()
    except ValueError:
        return None


def build_last_known_good(snapshot_dir, today=None, max_age_days=DEFAULT_MAX_AGE_DAYS):
    """스냅샷들을 신선한 순서로 훑어 종목별·필드별 '마지막 좋은 값'을 만든다.

    Returns: dict[code(str6)][field] = (value: float, asof: 'YYYY-MM-DD')
    """
    if today is None:
        today = datetime.date.today()

    files = []
    for fp in glob.glob(os.path.join(snapshot_dir, '*.json')):
        d = _parse_snapshot_date(fp)
        if d is None:
            continue
        age = (today - d).days
        if age < 0 or age > max_age_days:
            continue
        files.append((d, fp))
    # 최신 → 과거 순으로 정렬, 최신 값을 우선 채택
    files.sort(key=lambda x: x[0], reverse=True)

    lkg = {}
    for d, fp in files:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        d_str = d.strftime('%Y-%m-%d')
        for code, entry in data.items():
            code = str(code).zfill(6)
            slot = lkg.setdefault(code, {})
            for field in CARRY_FIELDS:
                if field in slot:
                    continue  # 이미 더 최신 값으로 채워짐
                v = entry.get(field)
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if pd.notna(fv):
                    slot[field] = (fv, d_str)
    return lkg


def _recompute_growth(row):
    """매출액/영업이익 절대값으로부터 성장률·최대성장률·가용연도를 재계산."""
    ty = [2025, 2026, 2027, 2028]
    by = [2024, 2025, 2026, 2027]
    for m in ('매출액', '영업이익'):
        mg = []
        for i, t in enumerate(ty):
            tv = row.get(f'{m}_{t}', np.nan)
            bv = row.get(f'{m}_{by[i]}', np.nan)
            if pd.notna(tv) and pd.notna(bv) and bv != 0:
                g = ((tv - bv) / abs(bv)) * 100.0
                row[f'{m}_성장률_{t}'] = round(g, 2)
                mg.append(g)
            else:
                row[f'{m}_성장률_{t}'] = np.nan
        row[f'{m}_최대성장률'] = round(max(mg), 2) if mg else np.nan

    av = sum(1 for y in ty
             if any(pd.notna(row.get(f'{m}_{y}', np.nan)) for m in ('매출액', '영업이익')))
    row['데이터_가용성'] = f'{av}년치 존재'
    row['가용_연도수'] = av
    return row


def merge_carry_forward(df, snapshot_dir, today=None,
                        max_age_days=DEFAULT_MAX_AGE_DAYS, verbose=True):
    """이번 크롤 결과(df)의 NaN 컨센 칸을 최근 스냅샷 값으로 보강한다.

    - 채우는 대상: 매출액/영업이익 × 2025~2028 (NaN인 칸만)
    - 보강 후 성장률·최대성장률·가용연도수·데이터_가용성을 재계산
    - 보강 추적용 컬럼 추가:
        컨센_보강     : 27E 또는 28E가 carry-forward로 채워졌으면 True
        컨센_보강일   : 사용한 스냅샷 날짜 중 가장 오래된 것 (staleness 표시용)
    """
    if df is None or df.empty:
        return df
    if not os.path.isdir(snapshot_dir):
        # 스냅샷이 없으면 추적 컬럼만 추가하고 그대로 반환
        if '컨센_보강' not in df.columns:
            df['컨센_보강'] = False
        if '컨센_보강일' not in df.columns:
            df['컨센_보강일'] = ''
        return df

    lkg = build_last_known_good(snapshot_dir, today=today, max_age_days=max_age_days)
    if not lkg:
        if '컨센_보강' not in df.columns:
            df['컨센_보강'] = False
        if '컨센_보강일' not in df.columns:
            df['컨센_보강일'] = ''
        return df

    df = df.copy()
    # 누락된 컬럼 보강 (옛 캐시 호환)
    for field in CARRY_FIELDS:
        if field not in df.columns:
            df[field] = np.nan

    filled_rows = 0
    filled_cells = 0
    boost_flags = []
    boost_dates = []

    for idx, row in df.iterrows():
        code = str(row.get('종목코드', '')).zfill(6)
        slot = lkg.get(code)
        boosted = False
        oldest_date = ''
        if slot:
            row_changed = False
            for field in CARRY_FIELDS:
                cur = row.get(field, np.nan)
                if pd.isna(cur) and field in slot:
                    val, asof = slot[field]
                    df.at[idx, field] = val
                    row[field] = val
                    filled_cells += 1
                    row_changed = True
                    # 27E/28E를 채운 경우만 'boost'로 표시 (핵심 결손)
                    if field.endswith('_2027') or field.endswith('_2028'):
                        boosted = True
                        if (not oldest_date) or asof < oldest_date:
                            oldest_date = asof
            if row_changed:
                filled_rows += 1
                # 성장률 등 재계산
                merged = _recompute_growth(dict(row))
                for k in ['매출액_성장률_2025', '매출액_성장률_2026', '매출액_성장률_2027',
                          '매출액_성장률_2028', '매출액_최대성장률',
                          '영업이익_성장률_2025', '영업이익_성장률_2026', '영업이익_성장률_2027',
                          '영업이익_성장률_2028', '영업이익_최대성장률',
                          '데이터_가용성', '가용_연도수']:
                    if k in df.columns:
                        df.at[idx, k] = merged.get(k)
        boost_flags.append(boosted)
        boost_dates.append(oldest_date)

    df['컨센_보강'] = boost_flags
    df['컨센_보강일'] = boost_dates

    if verbose:
        n27 = int(df['매출액_2027'].notna().sum()) if '매출액_2027' in df.columns else 0
        n28 = int(df['매출액_2028'].notna().sum()) if '매출액_2028' in df.columns else 0
        print(f"[carry-forward] {filled_rows}개 종목 / {filled_cells}개 셀 보강 "
              f"→ 27E있음={n27}, 28E있음={n28}")
    return df
