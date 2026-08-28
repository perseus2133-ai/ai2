#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
컨센서스 스냅샷 입출력 — CSV 신규 저장 / 기존 JSON 하위호환 읽기.

배경 (2026-08-28):
    스냅샷은 매일 GitHub Actions가 자동 커밋한다. 기존 JSON 포맷은 종목마다
    키 이름을 2,600번 반복해, 필드 4개만으로 279KB/일을 썼다(100일 = 26MB).
    귀속 분석(수익률을 '이익 배수 × 멀티플 배수'로 분해)을 하려면 시총·
    순차입금·지배비율·업종이 시점별로 필요한데, JSON 그대로 필드를 늘리면
    저장소가 연 150MB씩 불어난다.

    → CSV로 바꾸면 키 반복이 사라지고 git이 델타 압축을 걸 수 있다.
      필드 4배(15개)에 용량 약 1.7배로 끝난다.
      gzip은 쓰지 않는다 — 압축된 파일은 git 델타가 먹지 않아 오히려 손해.

읽기는 .csv와 기존 .json을 모두 지원한다(같은 날짜면 CSV 우선).
read_snapshot()의 반환 구조는 기존 json.load()와 동일한
dict[code(str6)][field] 이며, 결측은 키 자체를 생략한다 → 하위 코드 무변경.

주의:
    저장 필드를 줄이지 말 것. 역채움(백필)을 하지 않기로 했으므로, 지금
    저장하는 것이 훗날 귀속 분석에 쓸 수 있는 재료의 전부다.
"""

import os
import re
import json
import datetime

import numpy as np
import pandas as pd

# 컨센서스 추정치 — carry-forward 및 Revision Score의 재료
CONSENSUS_FIELDS = [f'{m}_{y}'
                    for m in ('매출액', '영업이익')
                    for y in (2025, 2026, 2027, 2028)]

# 멀티플 재현용 — 시점별 EV/OP_adj 및 피어 멀티플을 사후 복원하려면 필수.
#   현재가와 시가총액을 함께 저장하는 이유: 둘의 비율로 주식수 변화
#   (유상증자·자사주 소각·액면분할)를 감지할 수 있다. 하나만 저장하면
#   훗날 수익률 계산이 조용히 틀린다.
VALUATION_FIELDS = ['지배비율', '순차입금', '시가총액', '현재가', 'PER']

# 피어 그룹은 그 시점 기준으로 잡아야 한다. sector_map.json은 현재값만
# 갖고 있어 시점 정합성이 깨지므로 스냅샷에 함께 박아둔다.
LABEL_FIELDS = ['업종', '시장']

NUM_FIELDS = CONSENSUS_FIELDS + VALUATION_FIELDS
ALL_FIELDS = NUM_FIELDS + LABEL_FIELDS

_NAME_RE = re.compile(r'^(\d{4}-\d{2}-\d{2})\.(csv|json)$')


# ────────────────────────────────────────────────────────────
# 저장
# ────────────────────────────────────────────────────────────
def save_snapshot(df, snapshot_dir, date_str):
    """오늘자 스냅샷을 CSV로 저장. 실패해도 예외를 올리지 않는다(기존 동작 유지).

    Returns: True(저장됨) / False(건너뜀·실패)
    """
    if df is None or getattr(df, 'empty', True):
        return False
    if '종목코드' not in df.columns:
        return False
    try:
        os.makedirs(snapshot_dir, exist_ok=True)

        out = pd.DataFrame({'종목코드': df['종목코드'].astype(str).str.zfill(6)})
        for c in ALL_FIELDS:
            out[c] = df[c].values if c in df.columns else np.nan

        out = out[out['종목코드'].str.len() == 6]
        out = out[out['종목코드'] != '000000']
        # 컨센 8칸이 전부 비면 기록 가치가 없다 (기존 JSON도 빈 entry는 skip)
        out = out[out[CONSENSUS_FIELDS].notna().any(axis=1)]
        if out.empty:
            return False

        out.to_csv(os.path.join(snapshot_dir, f'{date_str}.csv'),
                   index=False, encoding='utf-8-sig')
        return True
    except Exception:
        return False


def save_snapshot_dict(snap, snapshot_dir, date_str):
    """dict[code][field] → CSV 저장 (read-modify-write 경로).

    refresh_27_28.py 처럼 오늘자 스냅샷을 읽어 일부 필드만 갱신한 뒤 되쓰는
    경우에 쓴다. 아침 크롤이 남긴 밸류·라벨 필드는 entry에 그대로 실려
    있으므로 왕복해도 보존된다.
    """
    if not snap:
        return False
    try:
        os.makedirs(snapshot_dir, exist_ok=True)
        rows = []
        for code, entry in snap.items():
            if not entry:
                continue
            rec = {'종목코드': str(code).zfill(6)}
            for c in ALL_FIELDS:
                rec[c] = entry.get(c)
            rows.append(rec)
        if not rows:
            return False
        out = pd.DataFrame(rows, columns=['종목코드'] + ALL_FIELDS)
        out.to_csv(os.path.join(snapshot_dir, f'{date_str}.csv'),
                   index=False, encoding='utf-8-sig')
        return True
    except Exception:
        return False


# ────────────────────────────────────────────────────────────
# 조회
# ────────────────────────────────────────────────────────────
def list_snapshots(snapshot_dir):
    """[(date, path)] 오름차순. 같은 날짜에 csv·json이 함께 있으면 CSV 우선."""
    if not os.path.isdir(snapshot_dir):
        return []
    best = {}
    for fn in os.listdir(snapshot_dir):
        m = _NAME_RE.match(fn)
        if not m:
            continue
        try:
            d = datetime.datetime.strptime(m.group(1), '%Y-%m-%d').date()
        except ValueError:
            continue
        if d not in best or m.group(2) == 'csv':
            best[d] = os.path.join(snapshot_dir, fn)
    return sorted(best.items())


def read_snapshot(path):
    """스냅샷 파일 → dict[code(str6)][field].

    결측은 키를 생략한다 — 기존 json.load() 결과와 동일하게 .get()이 None을
    돌려주도록 하기 위함. 실패 시 빈 dict.
    """
    try:
        if path.endswith('.json'):
            with open(path, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            return {str(k).zfill(6): v for k, v in raw.items()}

        sdf = pd.read_csv(path, dtype={'종목코드': str}, encoding='utf-8-sig')
        if '종목코드' not in sdf.columns:
            return {}
        sdf['종목코드'] = sdf['종목코드'].astype(str).str.zfill(6)

        num_cols = [c for c in NUM_FIELDS if c in sdf.columns]
        str_cols = [c for c in LABEL_FIELDS if c in sdf.columns]
        for c in num_cols:
            sdf[c] = pd.to_numeric(sdf[c], errors='coerce')

        out = {}
        for rec in sdf.to_dict('records'):
            entry = {}
            for c in num_cols:
                v = rec.get(c)
                if v is not None and pd.notna(v):
                    entry[c] = float(v)
            for c in str_cols:
                v = rec.get(c)
                if isinstance(v, str) and v.strip():
                    entry[c] = v.strip()
            if entry:
                out[rec['종목코드']] = entry
        return out
    except Exception:
        return {}


def find_snapshot_on_or_before(snapshot_dir, cutoff_date):
    """cutoff_date 이하 중 가장 최근 스냅샷 (date, path). 없으면 (None, None)."""
    snaps = [(d, p) for d, p in list_snapshots(snapshot_dir) if d <= cutoff_date]
    return snaps[-1] if snaps else (None, None)
