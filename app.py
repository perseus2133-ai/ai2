#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FnGuide 컨센서스 기반 초고성장 종목 발굴 시스템
Streamlit 대시보드 (캐시 지원)
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import time
import datetime
import threading
import os
import json
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
import warnings
from zoneinfo import ZoneInfo

warnings.filterwarnings('ignore')

KST = ZoneInfo('Asia/Seoul')

KST = ZoneInfo("Asia/Seoul")

# ============================================================
# 페이지 설정
# ============================================================
st.set_page_config(
    page_title="초고성장 종목 발굴 시스템",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 캐시 설정
# ============================================================

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
CSV_FILE  = os.path.join(DATA_DIR, "consensus_data.csv")
META_FILE = os.path.join(DATA_DIR, "meta.json")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
SNAPSHOT_DIR = os.path.join(DATA_DIR, "consensus_snapshots")

def now_kst():
    return datetime.datetime.now(KST)

def now_kst():
    return datetime.datetime.now(KST)

def save_cache(data_df, meta):
    os.makedirs(DATA_DIR, exist_ok=True)
    data_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
    meta['timestamp'] = now_kst().isoformat()
    with open(META_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def load_cache():
    if not os.path.exists(CSV_FILE):
        return None
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        meta = {}
        if os.path.exists(META_FILE):
            with open(META_FILE, 'r', encoding='utf-8') as f:
                meta = json.load(f)
        ts_str = meta.get('timestamp', '')
        if ts_str:
            ts = datetime.datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
        else:
            ts = now_kst()
        return {'data': df, 'meta': meta, 'timestamp': ts}
    except Exception:
        return None

def get_cache_info():
    cache = load_cache()
    if cache is None:
        return None
    return {
        'timestamp':    cache['timestamp'],
        'total_stocks': len(cache['data']),
        'meta':         cache.get('meta', {}),
    }

# ============================================================
# 누적 기록 저장/로드
# ============================================================
def save_history(df, min_vol=1000000):
    """크롤링 결과에서 거래량 100만 이상 종목을 날짜별로 누적 저장"""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    today_str = now_kst().strftime('%Y-%m-%d')
    history_file = os.path.join(HISTORY_DIR, "accumulation.json")

    # 기존 누적 데이터 로드
    history = {}
    if os.path.exists(history_file):
        with open(history_file, 'r', encoding='utf-8') as f:
            history = json.load(f)

    # 거래량 100만 이상 필터
    vol_df = df[df['Recent_Volume'] >= min_vol].copy()
    if vol_df.empty:
        return

    # 재무 필터 (고정): 매출 500억↑, 영업이익 흑자, 매출 초과 적자 제외
    def strict_financial_check(row):
        for y in [2023, 2024, 2025, 2026, 2027, 2028]:
            rv = row.get(f'매출액_{y}', np.nan)
            ov = row.get(f'영업이익_{y}', np.nan)
            if pd.notna(rv) and rv < 500: return False
            if pd.notna(ov) and ov < 0: return False
            if pd.notna(ov) and pd.notna(rv) and ov < 0 and abs(ov) > rv: return False
        return True

    vol_df = vol_df[vol_df.apply(strict_financial_check, axis=1)].copy()
    if vol_df.empty:
        return

    # apply_filters와 동일한 점수 계산을 위해 간단히 수행
    def calc_scores(row):
        s = 0
        rm = row.get('매출액_최대성장률', np.nan)
        if pd.notna(rm): s += min(rm, 2000)
        om = row.get('영업이익_최대성장률', np.nan)
        if pd.notna(om): s += min(om, 2000)
        con = sum(1 for y in [2025,2026,2027,2028]
                  if (pd.notna(row.get(f'매출액_성장률_{y}')) and row.get(f'매출액_성장률_{y}') > 30)
                  or (pd.notna(row.get(f'영업이익_성장률_{y}')) and row.get(f'영업이익_성장률_{y}') > 30))
        s += con * 50
        return s

    def calc_visibility(row):
        rv24 = row.get('매출액_2024', np.nan)
        rv25 = row.get('매출액_2025', np.nan)
        rv26 = row.get('매출액_2026', np.nan)
        rv27 = row.get('매출액_2027', np.nan)
        rv28 = row.get('매출액_2028', np.nan)
        pr = 5
        mt = 0
        if pd.notna(rv28) and pd.notna(rv25) and rv25 > 0:
            pr = 1; mt = ((rv28 / rv25) ** (1/3) - 1) * 100
        elif pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 2; mt = ((rv27 / rv25) ** (1/2) - 1) * 100
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 3; mt = ((rv26 / rv25) - 1) * 100
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 4; mt = ((rv25 / rv24) - 1) * 100
        return pr, mt

    # 4개 카테고리별 종목 추출
    categories = {
        '미래가시성핵심성장': [],
        '매출+영업이익환산점수': [],
        '매출1년최대성장률': [],
        '영업이익1년최대성장률': [],
    }

    for _, row in vol_df.iterrows():
        name = row.get('종목명', '')
        if not name:
            continue

        pr, mt = calc_visibility(row)
        score = calc_scores(row)
        rev_max = row.get('매출액_최대성장률', np.nan)
        op_max = row.get('영업이익_최대성장률', np.nan)

        # 미래가시성핵심성장: P1~P4 등급 (28E or 27E or 26E or 25E 데이터 있는 종목)
        if pr <= 4:
            categories['미래가시성핵심성장'].append(name)

        # 매출+영업이익환산점수: 점수 > 0
        if score > 0:
            categories['매출+영업이익환산점수'].append(name)

        # 매출1년최대성장률
        if pd.notna(rev_max) and rev_max > 0:
            categories['매출1년최대성장률'].append(name)

        # 영업이익1년최대성장률
        if pd.notna(op_max) and op_max > 0:
            categories['영업이익1년최대성장률'].append(name)

    # 날짜별 누적
    for cat_name, stocks in categories.items():
        if cat_name not in history:
            history[cat_name] = {}
        history[cat_name][today_str] = stocks

    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_history():
    history_file = os.path.join(HISTORY_DIR, "accumulation.json")
    if not os.path.exists(history_file):
        return {}
    with open(history_file, 'r', encoding='utf-8') as f:
        return json.load(f)


# ============================================================
# 컨센서스 스냅샷 (Estimates Revision 분석용)
# ============================================================
_REV_YEARS = [2025, 2026, 2027, 2028]
_REV_COLS  = [f'{m}_{y}' for m in ('매출액', '영업이익') for y in _REV_YEARS]


def save_consensus_snapshot(df):
    """오늘자 컨센서스 추정치를 날짜별 JSON으로 저장 (1개월 후 비교용)."""
    if df is None or df.empty:
        return
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    today = now_kst().strftime('%Y-%m-%d')
    path  = os.path.join(SNAPSHOT_DIR, f'{today}.json')
    snap  = {}
    for _, row in df.iterrows():
        code = str(row.get('종목코드', '')).zfill(6)
        if not code or code == '000000':
            continue
        entry = {}
        for c in _REV_COLS:
            v = row.get(c, np.nan)
            if pd.notna(v):
                entry[c] = float(v)
        if entry:
            snap[code] = entry
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False)
    except:
        pass


@st.cache_data(ttl=21600, show_spinner=False)
def load_old_consensus_snapshot(target_days_ago=30):
    """target_days_ago 일 근처의 가장 가까운 스냅샷을 로드 (7~60일 범위)."""
    if not os.path.exists(SNAPSHOT_DIR):
        return {}
    today  = now_kst().date()
    target = today - datetime.timedelta(days=target_days_ago)
    best, best_diff = None, None
    for fn in os.listdir(SNAPSHOT_DIR):
        if not fn.endswith('.json'):
            continue
        try:
            d = datetime.datetime.strptime(fn[:-5], '%Y-%m-%d').date()
        except:
            continue
        age = (today - d).days
        if not (7 <= age <= 60):
            continue
        diff = abs((d - target).days)
        if best_diff is None or diff < best_diff:
            best, best_diff = fn, diff
    if not best:
        return {}
    try:
        with open(os.path.join(SNAPSHOT_DIR, best), 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {'snapshot_date': best[:-5], 'data': data}
    except:
        return {}


def calc_consensus_revision(stock_code, current_row):
    """1개월 전 스냅샷 대비 26E 매출/영업이익 컨센 변화율(%) 계산."""
    snap = load_old_consensus_snapshot(30)
    if not snap or 'data' not in snap:
        return None
    code = str(stock_code).zfill(6)
    old = snap['data'].get(code)
    if not old:
        return None

    def _pct(curr, prev):
        if pd.isna(curr) or prev is None or prev == 0:
            return np.nan
        return ((float(curr) - float(prev)) / abs(float(prev))) * 100.0

    rev_changes = {}
    for y in (2026, 2025):  # 26E 우선, 없으면 25E
        r = _pct(current_row.get(f'매출액_{y}'),  old.get(f'매출액_{y}'))
        o = _pct(current_row.get(f'영업이익_{y}'), old.get(f'영업이익_{y}'))
        if pd.notna(r) or pd.notna(o):
            rev_changes['year']  = y
            rev_changes['rev']   = r
            rev_changes['op']    = o
            rev_changes['date']  = snap.get('snapshot_date', '')
            return rev_changes
    return None


def build_history_excel():
    """누적 기록을 엑셀 BytesIO로 변환 (4개 시트)"""
    history = load_history()
    if not history:
        return None

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        for sheet_name in ['미래가시성핵심성장', '매출+영업이익환산점수', '매출1년최대성장률', '영업이익1년최대성장률']:
            cat_data = history.get(sheet_name, {})
            if not cat_data:
                pd.DataFrame({'날짜 없음': []}).to_excel(writer, sheet_name=sheet_name, index=False)
                continue

            # 날짜를 컬럼으로, 각 컬럼에 종목명 나열
            dates = sorted(cat_data.keys())
            max_len = max(len(cat_data[d]) for d in dates) if dates else 0
            data = {}
            for d in dates:
                stocks = cat_data[d]
                padded = stocks + [''] * (max_len - len(stocks))
                data[d] = padded
            sheet_df = pd.DataFrame(data)
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

    return buf.getvalue()


# ============================================================
# 커스텀 CSS
# ============================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;900&family=JetBrains+Mono:wght@400;700&display=swap');
html, body, [data-testid="stAppViewContainer"] {
    font-family: 'Inter', 'Pretendard', sans-serif;
    background-color: #3E4A59 !important;
    color: #FFFFFF !important;
}

[data-testid="stSidebar"] > div:first-child {
    background-color: #1A1C24 !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
    color: #E2E8F0 !important;
}

div[data-baseweb="select"] > div {
    background-color: #1A1C24 !important;
    color: #FFFFFF !important;
}
div[data-baseweb="select"] span {
    color: #FFFFFF !important;
}
div[data-baseweb="popover"] ul {
    background-color: #1A1C24 !important;
}
div[data-baseweb="popover"] li {
    color: #FFFFFF !important;
}

div[data-testid="stVerticalBlock"] > div:has(div.element-container) {
    transition: all 0.2s ease-in-out;
}

.hero-header { border-bottom: 1px solid #4C566A; padding-bottom: 16px; margin-bottom: 24px; text-align: left; background: transparent !important; border-radius: 0; padding: 0 0 16px 0; }
.hero-header p { color: #A0AEC0; font-size: 1.0rem; margin: 0; font-family: 'JetBrains Mono', monospace; }

.quant-card-light {
    background-color: #FFFFFF;
    color: #212529;
    border: 1px solid #E9ECEF;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 12px;
    transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1);
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

.quant-card-light:hover {
    transform: translateY(-5px) scale(1.01);
    background-color: #FFF4A3 !important;
    box-shadow: 0 15px 25px rgba(0,0,0,0.15);
    border: 1px solid transparent;
    border-image: linear-gradient(to right, #ff2400, #e81d1d, #e8b71d, #1de840, #1ddde8, #2b1de8, #dd00f3, #dd00f3);
    border-image-slice: 1;
}

/* === Dark Quant Card (screenshot-style, slightly brighter) === */
.quant-card-dark {
    background: linear-gradient(135deg, #3F4C60 0%, #313B4D 100%);
    color: #E2E8F0;
    border: 1px solid #4A5568;
    border-radius: 14px;
    padding: 22px;
    margin-bottom: 16px;
    box-shadow: 0 10px 24px rgba(0,0,0,0.30), 0 2px 6px rgba(0,0,0,0.20);
    transition: all 0.3s ease;
}
.quant-card-dark:hover {
    transform: translateY(-3px);
    box-shadow: 0 14px 32px rgba(0,0,0,0.40), 0 3px 8px rgba(0,0,0,0.25);
    border-color: #1D3557;
    background: linear-gradient(135deg, #FFFFFF 0%, #F8F9FA 100%);
    color: #212529;
}
/* 호버 시 카드 내부 텍스트 라이트 테마 반전 */
.quant-card-dark:hover .qcd-name { color: #111827; }
.quant-card-dark:hover .qcd-code { color: #475569; }
.quant-card-dark:hover .qcd-stat-label,
.quant-card-dark:hover .qcd-tech-label,
.quant-card-dark:hover .qcd-tech-item .k,
.quant-card-dark:hover .qcd-pill .lbl,
.quant-card-dark:hover .qcd-evidence .head,
.quant-card-dark:hover .qcd-verdict-reason,
.quant-card-dark:hover .qcd-level-line .lk,
.quant-card-dark:hover .qcd-level-pos { color: #475569; }
.quant-card-dark:hover .qcd-tech-mid,
.quant-card-dark:hover .qcd-tech-right { border-left-color: #E2E8F0; }
.quant-card-dark:hover .qcd-level-bar { background-color: #E5E7EB; }
.quant-card-dark:hover .qcd-level-marker { background: #1D3557; box-shadow: 0 0 5px rgba(29,53,87,0.45); }
.quant-card-dark:hover .qcd-stat-val,
.quant-card-dark:hover .qcd-pill .val { color: #111827; }
.quant-card-dark:hover .qcd-rank {
    background: rgba(29, 53, 87, 0.08);
    color: #1D3557;
    border-color: rgba(29, 53, 87, 0.30);
}
.quant-card-dark:hover .qcd-pill {
    background: #F8F9FA;
    border-color: #E2E8F0;
}
.quant-card-dark:hover .qcd-pill.hi {
    border-color: #1D3557;
    box-shadow: 0 0 8px rgba(29,53,87,0.10) inset;
}
.quant-card-dark:hover .qcd-pill.hi .lbl,
.quant-card-dark:hover .qcd-pill.hi .val { color: #1D3557; }
.quant-card-dark:hover .qcd-chart-box,
.quant-card-dark:hover .qcd-tech-box,
.quant-card-dark:hover .qcd-evidence {
    background: #F8F9FA;
    border-color: #E2E8F0;
}
.quant-card-dark:hover .qcd-chart-legend { color: #475569; }
.quant-card-dark:hover .qcd-naver-link {
    background: rgba(29,53,87,0.06);
    color: #1D3557 !important;
    border-color: rgba(29,53,87,0.35);
}
.qcd-rank {
    background: rgba(98, 239, 255, 0.12);
    color: #62EFFF;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
    font-weight: 700;
    padding: 3px 9px;
    border-radius: 5px;
    border: 1px solid rgba(98, 239, 255, 0.25);
}
.qcd-badge-kospi {
    background: linear-gradient(135deg, #FBBF24, #F59E0B);
    color: #1A1C24;
    font-weight: 800;
    font-size: 0.7rem;
    padding: 3px 9px;
    border-radius: 5px;
    letter-spacing: 0.4px;
}
.qcd-badge-kosdaq {
    background: linear-gradient(135deg, #FB923C, #F97316);
    color: #1A1C24;
    font-weight: 800;
    font-size: 0.7rem;
    padding: 3px 9px;
    border-radius: 5px;
    letter-spacing: 0.4px;
}
.qcd-name { color: #FFFFFF; font-size: 1.18rem; font-weight: 700; }
.qcd-code { color: #94A3B8; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; margin-left: 6px; }
.qcd-stat-label { color: #94A3B8; font-size: 0.72rem; font-weight: 600; }
.qcd-stat-val { color: #FFFFFF; font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 1.0rem; }

.qcd-chart-box {
    background: rgba(17, 24, 39, 0.45);
    border: 1px solid #4A5568;
    border-radius: 10px;
    padding: 8px 10px 4px 10px;
    margin: 10px 0;
    max-width: 440px;
}
.qcd-chart-legend { display:flex; gap:18px; align-items:center; font-size:0.75rem; color:#CBD5E0; margin-bottom:6px; }
.qcd-chart-legend .dot { display:inline-block; width:10px; height:3px; border-radius:1px; margin-right:6px; vertical-align:middle; }

.qcd-pill {
    background: rgba(17, 24, 39, 0.55);
    border: 1px solid #4A5568;
    border-radius: 9px;
    padding: 9px 14px;
    min-width: 78px;
    text-align: center;
    transition: all 0.2s ease;
}
.qcd-pill .lbl { color: #94A3B8; font-size: 0.7rem; font-weight: 600; }
.qcd-pill .val { color: #FFFFFF; font-family: 'JetBrains Mono', monospace; font-weight: 800; font-size: 1.05rem; margin-top: 2px; }
.qcd-pill.hi { border-color: #62EFFF; box-shadow: 0 0 10px rgba(98,239,255,0.18) inset, 0 0 8px rgba(98,239,255,0.12); }
.qcd-pill.hi .lbl { color: #62EFFF; }
.qcd-pill.hi .val { color: #62EFFF; }

.qcd-tech-box {
    background: rgba(17, 24, 39, 0.45);
    border: 1px solid #4A5568;
    border-radius: 10px;
    padding: 12px 16px;
    display: flex;
    flex-direction: row;
    align-items: stretch;
    gap: 16px;
    flex: 1;
    min-width: 200px;
}
.qcd-tech-left {
    flex: 1 1 auto;
    display: flex;
    flex-direction: column;
    gap: 8px;
    justify-content: center;
}
.qcd-tech-mid {
    flex: 0 0 auto;
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 4px;
    padding: 0 14px;
    border-left: 1px solid rgba(74, 85, 104, 0.6);
    min-width: 150px;
}
.qcd-tech-right {
    flex: 0 0 auto;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    justify-content: center;
    gap: 4px;
    padding-left: 14px;
    border-left: 1px solid rgba(74, 85, 104, 0.6);
    min-width: 140px;
    text-align: right;
}
.qcd-level-line {
    display: flex; justify-content: space-between; align-items: baseline;
    font-family: 'JetBrains Mono', monospace; font-size: 0.84rem;
}
.qcd-level-line .lk { color: #94A3B8; font-size: 0.7rem; font-weight: 600; }
.qcd-level-line .lv { font-weight: 700; }
.qcd-level-bar {
    height: 5px; background: rgba(74, 85, 104, 0.4);
    border-radius: 3px; position: relative; margin: 5px 0 2px 0;
    background-image: linear-gradient(to right, rgba(52,211,153,0.45) 0%, rgba(251,191,36,0.45) 50%, rgba(248,113,113,0.45) 100%);
}
.qcd-level-marker {
    position: absolute; top: -3px; width: 3px; height: 11px;
    background: #FFFFFF; border-radius: 1px;
    box-shadow: 0 0 5px rgba(0,0,0,0.55);
}
.qcd-level-pos {
    font-family: 'JetBrains Mono', monospace; font-size: 0.7rem;
    color: #CBD5E0; text-align: center; margin-top: 1px;
}
.qcd-tech-row {
    display:flex; align-items:center; gap:14px; flex-wrap:wrap;
}
.qcd-verdict-big {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 800;
    font-size: 1.45rem;
    line-height: 1.1;
}
.qcd-verdict-reason {
    color: #94A3B8;
    font-size: 0.72rem;
    line-height: 1.4;
    text-align: right;
    margin-top: 2px;
}
.qcd-tech-label { color:#94A3B8; font-size:0.72rem; font-weight:600; letter-spacing:0.3px; }
.qcd-tech-item { display:flex; flex-direction:column; gap:2px; }
.qcd-tech-item .k { color:#94A3B8; font-size:0.7rem; }
.qcd-tech-item .v { font-family:'JetBrains Mono', monospace; font-weight:700; font-size:0.95rem; }

.qcd-evidence {
    background: rgba(17, 24, 39, 0.40);
    border: 1px solid #4A5568;
    border-radius: 10px;
    padding: 10px 14px;
    margin: 0;
}
.qcd-evidence .head { color:#94A3B8; font-size:0.7rem; font-weight:600; margin-bottom:6px; }
.qcd-naver-link {
    display:inline-flex; align-items:center; gap:5px;
    background: rgba(98,239,255,0.08);
    color: #62EFFF !important;
    border: 1px solid rgba(98,239,255,0.35);
    padding: 6px 12px; border-radius: 6px;
    font-size: 0.78rem; font-weight: 700; text-decoration: none !important;
    transition: all 0.2s ease;
}
.qcd-naver-link:hover { background: rgba(98,239,255,0.15); transform: translateY(-1px); }

.stock-name { font-size: 1.15rem; font-weight: 700; color: #212529 !important; }
.stock-code { color: #1D3557; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; margin-left: 4px; }

.badge-kospi { display: inline-block; background-color: #F8F9FA; border: 1px solid #1D3557; color: #1D3557; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 6px; }
.badge-kosdaq { display: inline-block; background-color: #F8F9FA; border: 1px solid #1D3557; color: #1D3557; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 6px; }

@keyframes rainbow-text {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

.rainbow-title {
    font-weight: 900;
    font-size: 2.4rem;
    background: linear-gradient(to right, #62efff, #ffb3fd, #ffeead, #62efff);
    background-size: 400% 400%;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    animation: rainbow-text 5s ease infinite;
    display: flex; align-items: center; gap: 8px;
    margin: 0 0 6px 0;
}

.rainbow-score {
    background: linear-gradient(to right, #62efff, #ffb3fd, #ffeead, #62efff);
    background-size: 400% 400%;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    animation: rainbow-text 5s ease infinite;
}

.metric-card { text-align: center; background: rgba(255, 255, 255, 0.05); border-radius: 8px; padding: 15px; border: 1px solid #4C566A; }
.metric-label { color: #A0AEC0; font-size: 0.85rem; margin-bottom: 4px; }
.metric-value { font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 1.5rem; color: #FFFFFF !important; }
.growth-positive { color: #FF6B6B; font-weight: 600; font-family: 'JetBrains Mono', monospace; }
.growth-negative { color: #4A90E2; font-weight: 600; font-family: 'JetBrains Mono', monospace; }
.growth-mega { color: #2EAA7B; font-weight: 800; font-family: 'JetBrains Mono', monospace; }

a.naver-link { display: inline-flex; align-items: center; justify-content: center; gap: 4px; background: transparent; color: #1D3557 !important; text-decoration: none !important; padding: 6px 14px; border: 1px solid #1D3557; border-radius: 6px; font-size: 0.8rem; font-weight: 600; transition: all 0.2s ease; }
a.naver-link:hover { background: rgba(29,53,87,0.05); box-shadow: 0 2px 8px rgba(29,53,87,0.15); transform: translateY(-1px); }

div.stButton > button, div.stDownloadButton > button, button[kind="secondary"] {
    background: linear-gradient(135deg, #ffffff, #f8f9fa) !important;
    border: 1px solid #E9ECEF !important;
    border-radius: 8px !important;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -2px rgba(0,0,0,0.1) !important;
    width: 100% !important;
}

div.stButton > button *, div.stDownloadButton > button *, button[kind="secondary"] * {
    color: #111827 !important;
    font-weight: 800 !important;
    font-size: 1.0rem !important;
    font-family: 'Inter', 'Pretendard', sans-serif !important;
}

div.stButton > button:hover, div.stDownloadButton > button:hover, button[kind="secondary"]:hover {
    border-color: #111827 !important;
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1) !important;
    transform: translateY(-2px);
}

div[data-baseweb="tab-list"] {
    gap: 12px;
}
button[data-baseweb="tab"] {
    background: linear-gradient(135deg, #2D3139, #3E4A59) !important;
    border: 1px solid #4C566A !important;
    border-radius: 10px !important;
    padding: 12px 24px !important;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.3), 0 2px 4px -2px rgba(0,0,0,0.3) !important;
    transition: all 0.3s ease !important;
    margin-right: 5px;
}
button[data-baseweb="tab"] > div[data-testid="stMarkdownContainer"] > p {
    color: #A0AEC0 !important;
    font-weight: 700 !important;
    font-size: 1.2rem !important;
}
button[data-baseweb="tab"]:hover {
    transform: translateY(-3px);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.4) !important;
    border-color: #A0AEC0 !important;
}
button[data-baseweb="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, #1D3557, #457B9D) !important;
    border-color: #62efff !important;
    box-shadow: inset 0 2px 4px rgba(0,0,0,0.1), 0 6px 12px rgba(98, 239, 255, 0.2) !important;
}
button[data-baseweb="tab"][aria-selected="true"] > div[data-testid="stMarkdownContainer"] > p {
    color: #FFFFFF !important;
    font-weight: 900 !important;
    font-size: 1.3rem !important;
}

.stProgress > div > div { background: #1D3557 !important; }

.cache-info { background: #FFFFFF; border-left: 3px solid #1D3557; border-radius: 4px; padding: 10px 14px; margin: 8px 0; color: #6C757D; font-size: 0.8rem; text-align: left; font-family: 'JetBrains Mono', monospace; box-shadow: 0 1px 3px rgba(0,0,0,0.02); }
.cache-none { background: #FFFFFF; border-left: 3px solid #E74C3C; border-radius: 4px; padding: 10px 14px; margin: 8px 0; color: #E74C3C; font-size: 0.8rem; text-align: left; font-family: 'JetBrains Mono', monospace; box-shadow: 0 1px 3px rgba(0,0,0,0.02); }
.divider { border: none; border-top: 1px solid #E9ECEF; margin: 24px 0; }

.stMarkdown { margin-bottom: 0px !important; }

#MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}

@media (max-width: 768px) {
    .quant-card-light { padding: 12px 10px; }
    .hero-header { padding-bottom: 12px; }
    .rainbow-title { font-size: 1.6rem; }
    .hero-header p { font-size: 0.8rem; }
    div.evidence-scroll { overflow-x: auto; white-space: nowrap; padding-bottom: 5px; }
    div.evidence-scroll > div { min-width: 480px; }
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# 상수 & 크롤링 함수
# ============================================================
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,'
               'image/avif,image/webp,*/*;q=0.8'),
}

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session

@st.cache_data(ttl=86400)
def get_all_naver_sectors():
    sector_map = {}
    try:
        url = 'https://finance.naver.com/sise/sise_group.naver?type=upjong'
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'lxml')
        links = soup.select('table.type_1 td a')

        def fetch_sector(a_tag):
            s_name = a_tag.text.strip()
            link = 'https://finance.naver.com' + a_tag['href']
            sub_res = requests.get(link, headers=HEADERS, timeout=5)
            sub_res.encoding = 'euc-kr'
            sub_soup = BeautifulSoup(sub_res.text, 'lxml')
            codes = []
            for sub_a in sub_soup.select('table.type_5 td.name a'):
                c = sub_a['href'].split('code=')[-1]
                codes.append((c, s_name))
            return codes

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch_sector, a) for a in links]
            for f in as_completed(futures):
                try:
                    for c, s in f.result():
                        sector_map[c] = s
                except: pass
    except: pass
    return sector_map

@st.cache_data(ttl=3600)
def _load_sector_map():
    """sector_map.json 우선 로드, 없으면 런타임 크롤링 폴백"""
    json_path = os.path.join(DATA_DIR, 'sector_map.json')
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return get_all_naver_sectors()


@st.cache_data(ttl=86400)
def get_sector_per_map():
    """업종별 평균 PER을 가져온다."""
    sector_per = {}
    try:
        url = 'https://finance.naver.com/sise/sise_group.naver?type=upjong'
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find('table', class_='type_1')
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 5:
                    a_tag = cells[0].find('a')
                    if a_tag:
                        name = a_tag.text.strip()
                        per_text = cells[4].get_text(strip=True).replace(',', '')
                        try:
                            sector_per[name] = float(per_text)
                        except:
                            pass
    except: pass
    return sector_per


def get_stock_list_naver(market="0"):
    market_name = "KOSPI" if market == "0" else "KOSDAQ"
    all_stocks, page, last_page = [], 1, 1
    while True:
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={market}&page={page}"
        try:
            session = get_session()
            resp = session.get(url, timeout=10)
            resp.encoding = 'euc-kr'
            soup = BeautifulSoup(resp.text, 'lxml')
            if page == 1:
                pg = soup.find('td', class_='pgRR')
                if pg and pg.find('a'):
                    last_page = int(re.search(r'page=(\d+)', pg.find('a')['href']).group(1))
            table = soup.find('table', class_='type_2')
            if not table: break
            found = False
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 10: continue
                nl = cells[1].find('a')
                if not nl: continue
                nm = nl.get_text(strip=True)
                if not nm: continue
                cm = re.search(r'code=(\d{6})', nl.get('href', ''))
                if not cm: continue
                v = cells[9].get_text(strip=True).replace(',', '')
                m = cells[6].get_text(strip=True).replace(',', '')
                p = cells[2].get_text(strip=True).replace(',', '')
                all_stocks.append({'종목코드': cm.group(1), '종목명': nm, '시장': market_name,
                    '현재가': int(p) if p.isdigit() else 0, '시가총액': int(m) if m.isdigit() else 0,
                    'Recent_Volume': int(v) if v.isdigit() else 0})
                found = True
            if not found or page >= last_page: break
            page += 1; time.sleep(0.05)
        except: break
    return pd.DataFrame(all_stocks)

def parse_numeric(text):
    if not text or text.strip() in ['', '-', 'N/A', 'nan', '\xa0']: return np.nan
    text = text.strip().replace(',', '').replace(' ', '').replace('\xa0', '')
    if text.startswith('(') and text.endswith(')'): text = '-' + text[1:-1]
    try: return float(text)
    except: return np.nan

def scrape_fnguide_supplement(stock_code, stock_name=''):
    """FnGuide에서 2026E, 2027E, 2028E 컨센서스 데이터를 보조로 가져온다.
    FnGuide는 응답이 무거워서 timeout 넉넉히 + 1회 재시도.
    """
    session = get_session()
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}'
    resp = None
    for attempt in (1, 2):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                break
        except Exception:
            resp = None
            if attempt == 2:
                return {}
            time.sleep(0.4)
    if resp is None or resp.status_code != 200:
        return {}
    try:
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'lxml')

        page_name = ''
        for tag in [soup.find('h1', class_='giName'), soup.find('title')]:
            if tag:
                page_name = tag.get_text(strip=True)
                break
        if stock_name and page_name and stock_name not in page_name and page_name not in stock_name:
            return {}

        tables = soup.find_all('table')
        # FnGuide는 종종 IFRS(연결) Annual / IFRS(별도) Annual 두 테이블이 있고
        # 종목에 따라 연결만, 별도만, 또는 둘 다 채워져 있음.
        # 모든 Annual 테이블을 순회하며 빈칸을 메운다 (먼저 매칭된 값 유지).
        annual_tables = []
        for tbl in tables:
            rows = tbl.find_all('tr')
            if len(rows) < 3: continue
            r0 = [c.get_text(strip=True) for c in rows[0].find_all(['th','td'])]
            combined = ' '.join(r0)
            if 'Annual' in combined and 'Quarter' not in combined:
                annual_tables.append(tbl)
        if not annual_tables:
            return {}

        dm = {}
        for tbl in annual_tables:
            rows = tbl.find_all('tr')
            hcells = rows[1].find_all(['th','td'])
            col_years = []
            for c in hcells:
                txt = c.get_text(strip=True)
                m = re.search(r'(\d{4})[./]', txt)
                col_years.append(int(m.group(1)) if m else None)
            for ri in range(2, min(10, len(rows))):
                cells = rows[ri].find_all(['th','td'])
                if not cells: continue
                lb = cells[0].get_text(strip=True)
                for mn in ['매출액', '영업이익']:
                    if mn in lb and '률' not in lb:
                        if mn not in dm: dm[mn] = {}
                        for ci, cell in enumerate(cells[1:]):
                            if ci >= len(col_years) or col_years[ci] is None: continue
                            yr = col_years[ci]
                            val = parse_numeric(cell.get_text(strip=True))
                            if pd.notna(val) and (yr not in dm[mn] or pd.isna(dm[mn][yr])):
                                dm[mn][yr] = val
        return dm
    except:
        return {}


def scrape_naver_per_pbr_roe(stock_code):
    """네이버 증권에서 PER, PBR, ROE를 크롤링한다."""
    result = {}
    try:
        session = get_session()
        # 종목 메인 페이지에서 PER, PBR 가져오기
        resp = session.get(f"https://finance.naver.com/item/main.naver?code={stock_code}", timeout=7)
        resp.encoding = 'utf-8'
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, 'lxml')

        # PER, PBR - 종목 상단 aside 테이블
        aside = soup.find('div', class_='aside_invest_info')
        if aside:
            for em_tag in aside.find_all('em'):
                txt = em_tag.get_text(strip=True)
                parent_text = em_tag.parent.get_text(strip=True) if em_tag.parent else ''
                # PER
                if 'PER' in parent_text and 'PER' not in result:
                    val = parse_numeric(txt)
                    if pd.notna(val):
                        result['PER'] = round(val, 2)
                # PBR
                if 'PBR' in parent_text and 'PBR' not in result:
                    val = parse_numeric(txt)
                    if pd.notna(val):
                        result['PBR'] = round(val, 2)

        # 테이블에서 PER/PBR 찾기 (대안)
        if 'PER' not in result or 'PBR' not in result:
            for table in soup.find_all('table', class_='per_table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if 'PER' in cell_text and 'PER' not in result and i + 1 < len(cells):
                            val = parse_numeric(cells[i+1].get_text(strip=True))
                            if pd.notna(val): result['PER'] = round(val, 2)
                        if 'PBR' in cell_text and 'PBR' not in result and i + 1 < len(cells):
                            val = parse_numeric(cells[i+1].get_text(strip=True))
                            if pd.notna(val): result['PBR'] = round(val, 2)

        # per_table 없을 때 body 전체에서 검색
        if 'PER' not in result:
            body_text = soup.get_text()
            per_match = re.search(r'PER\s*[\(배\)]*\s*([\d,.]+)', body_text)
            if per_match:
                val = parse_numeric(per_match.group(1))
                if pd.notna(val): result['PER'] = round(val, 2)
        if 'PBR' not in result:
            body_text = soup.get_text()
            pbr_match = re.search(r'PBR\s*[\(배\)]*\s*([\d,.]+)', body_text)
            if pbr_match:
                val = parse_numeric(pbr_match.group(1))
                if pd.notna(val): result['PBR'] = round(val, 2)

        # ROE - cop_analysis 테이블에서 가져오기
        cop = soup.find('div', class_='section cop_analysis')
        if cop:
            table = cop.find('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if cells:
                        label = cells[0].get_text(strip=True)
                        if 'ROE' in label:
                            # 최신 연도 값 가져오기
                            for cell in reversed(cells[1:]):
                                val = parse_numeric(cell.get_text(strip=True))
                                if pd.notna(val):
                                    result['ROE'] = round(val, 2)
                                    break
    except:
        pass
    return result


def get_daily_pv(stock_code, n_pages=2):
    """네이버 일별 시세에서 종가/거래량 시계열을 가져온다 (최신순)."""
    session = get_session()
    prices, volumes = [], []
    for page in range(1, n_pages + 1):
        try:
            url = f'https://finance.naver.com/item/sise_day.naver?code={stock_code}&page={page}'
            resp = session.get(url, timeout=6)
            resp.encoding = 'euc-kr'
            soup = BeautifulSoup(resp.text, 'lxml')
            table = soup.find('table', class_='type2')
            if not table:
                break
            page_added = 0
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 7:
                    continue
                close_text = cells[1].get_text(strip=True).replace(',', '')
                vol_text = cells[6].get_text(strip=True).replace(',', '')
                if close_text.isdigit() and vol_text.isdigit() and int(vol_text) > 0:
                    prices.append(int(close_text))
                    volumes.append(int(vol_text))
                    page_added += 1
            if page_added == 0:
                break
        except:
            break
    return prices, volumes


def calc_obv_rsi(prices, volumes, period=14):
    """OBV 추세와 RSI(14)를 계산한다 (Wilder's smoothing 방식, 네이버와 동일)."""
    if len(prices) < period + 1:
        return {}
    p = list(reversed(prices))   # 시계열 순(과거→현재)
    v = list(reversed(volumes))

    # ── OBV (단순 누적) ──────────────────────────────────────
    obv = [0]
    for i in range(1, len(p)):
        if p[i] > p[i - 1]:
            obv.append(obv[-1] + v[i])
        elif p[i] < p[i - 1]:
            obv.append(obv[-1] - v[i])
        else:
            obv.append(obv[-1])
    n_obv = min(10, len(obv))
    obv_change = obv[-1] - obv[-n_obv]
    if obv_change > 0:
        obv_trend = 'up'
    elif obv_change < 0:
        obv_trend = 'down'
    else:
        obv_trend = 'flat'

    # ── RSI (Wilder's smoothing) ─────────────────────────────
    gains, losses = [], []
    for i in range(1, len(p)):
        d = p[i] - p[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    if len(gains) < period:
        return {'OBV_trend': obv_trend, 'RSI': np.nan}

    # 1) 첫 period 동안의 단순평균으로 초기화
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    # 2) 이후 EMA-like 가중평균 (Wilder)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        rsi = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    return {'OBV_trend': obv_trend, 'RSI': round(rsi, 1)}


def calc_support_resistance(prices, lookback=60):
    """최근 N일 종가에서 단순 지지선/저항선 추정."""
    if not prices:
        return {}
    recent = prices[:lookback]
    return {
        '저항선': max(recent),
        '지지선': min(recent),
    }


def calc_ma_alignment(prices, periods=(5, 20, 60)):
    """이평선 정배열/역배열 판정. prices는 최신순 입력."""
    if not prices or len(prices) < max(periods):
        return ''
    p = list(reversed(prices))
    mas = [sum(p[-n:]) / n for n in periods]
    # 정배열: 단기 > 중기 > 장기
    if mas[0] > mas[1] > mas[2]:
        return 'up'
    if mas[0] < mas[1] < mas[2]:
        return 'down'
    return 'mixed'


def _ema(values, n):
    if not values:
        return []
    k = 2.0 / (n + 1.0)
    e = values[0]
    out = [e]
    for v in values[1:]:
        e = v * k + e * (1.0 - k)
        out.append(e)
    return out


def calc_macd_signal(prices, fast=12, slow=26, sig=9):
    """MACD 상태: 'bull_cross' / 'bear_cross' / 'bull' / 'bear' / ''."""
    if not prices or len(prices) < slow + sig:
        return ''
    p = list(reversed(prices))
    ema_f = _ema(p, fast)
    ema_s = _ema(p, slow)
    macd  = [f - s for f, s in zip(ema_f, ema_s)]
    sigl  = _ema(macd, sig)
    if len(macd) < 2:
        return ''
    m_prev, m_now = macd[-2], macd[-1]
    s_prev, s_now = sigl[-2], sigl[-1]
    if m_prev <= s_prev and m_now > s_now:
        return 'bull_cross'
    if m_prev >= s_prev and m_now < s_now:
        return 'bear_cross'
    if m_now > s_now:
        return 'bull'
    if m_now < s_now:
        return 'bear'
    return ''


def scrape_foreign_inst(stock_code):
    """네이버 외국인·기관 일별 순매매 (단위: 주). 5일/20일 누적 순매수."""
    out = {'외인_5d': np.nan, '외인_20d': np.nan,
           '기관_5d': np.nan, '기관_20d': np.nan}
    foreign_buys, inst_buys = [], []
    try:
        session = get_session()
        for page in (1, 2):
            url = f'https://finance.naver.com/item/frgn.naver?code={stock_code}&page={page}'
            resp = session.get(url, timeout=6)
            resp.encoding = 'euc-kr'
            soup = BeautifulSoup(resp.text, 'lxml')
            table = soup.find('table', class_='type2')
            if not table:
                break
            page_added = 0
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 9:
                    continue
                date_text = cells[0].get_text(strip=True)
                if not re.match(r'\d{4}\.\d{2}\.\d{2}', date_text):
                    continue
                try:
                    f_text = cells[5].get_text(strip=True).replace(',', '').replace('+', '')
                    i_text = cells[8].get_text(strip=True).replace(',', '').replace('+', '')
                    f_val = int(f_text) if f_text not in ('', '-') else 0
                    i_val = int(i_text) if i_text not in ('', '-') else 0
                except (ValueError, IndexError):
                    continue
                foreign_buys.append(f_val)
                inst_buys.append(i_val)
                page_added += 1
            if page_added == 0:
                break
        if foreign_buys:
            out['외인_5d']  = sum(foreign_buys[:5])
            out['외인_20d'] = sum(foreign_buys[:20])
            out['기관_5d']  = sum(inst_buys[:5])
            out['기관_20d'] = sum(inst_buys[:20])
    except:
        pass
    return out


def fetch_supplement_indicators(stock_code):
    """20일 평균 거래량 + OBV + RSI + 지지/저항선 + MA/MACD + 외인·기관 수급."""
    out = {'평균거래량_20d': np.nan, 'OBV_trend': '', 'RSI': np.nan,
           '저항선': np.nan, '지지선': np.nan,
           'MA_align': '', 'MACD_signal': '',
           '외인_5d': np.nan, '외인_20d': np.nan,
           '기관_5d': np.nan, '기관_20d': np.nan}
    try:
        prices, volumes = get_daily_pv(stock_code, n_pages=6)
        if volumes:
            recent_vols = volumes[:20]
            if len(recent_vols) >= 3:
                out['평균거래량_20d'] = round(sum(recent_vols) / len(recent_vols))
        ind = calc_obv_rsi(prices, volumes)
        if ind:
            out['OBV_trend'] = ind.get('OBV_trend', '')
            out['RSI'] = ind.get('RSI', np.nan)
        sr = calc_support_resistance(prices)
        out['저항선'] = sr.get('저항선', np.nan)
        out['지지선'] = sr.get('지지선', np.nan)
        out['MA_align']    = calc_ma_alignment(prices)
        out['MACD_signal'] = calc_macd_signal(prices)
    except:
        pass
    try:
        fi = scrape_foreign_inst(stock_code)
        out.update(fi)
    except:
        pass
    return out


def get_avg_volume_20d(stock_code):
    """레거시 호환 - 20일 평균 거래량만 반환."""
    return fetch_supplement_indicators(stock_code).get('평균거래량_20d', np.nan)


@st.cache_data(ttl=3600, show_spinner=False)
def compute_obv_rsi_cached(stock_code):
    """렌더 시점 폴백 (가격 기반 지표): OBV/RSI/지지/저항/MA/MACD."""
    out = {'OBV_trend': '', 'RSI': np.nan, '저항선': np.nan, '지지선': np.nan,
           'MA_align': '', 'MACD_signal': ''}
    try:
        prices, volumes = get_daily_pv(str(stock_code).zfill(6), n_pages=6)
        ind = calc_obv_rsi(prices, volumes)
        out['OBV_trend'] = ind.get('OBV_trend', '')
        out['RSI'] = ind.get('RSI', np.nan)
        sr = calc_support_resistance(prices)
        out['저항선'] = sr.get('저항선', np.nan)
        out['지지선'] = sr.get('지지선', np.nan)
        out['MA_align']    = calc_ma_alignment(prices)
        out['MACD_signal'] = calc_macd_signal(prices)
    except:
        pass
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_foreign_inst_cached(stock_code):
    """렌더 시점 폴백 (수급): 외인·기관 5일/20일 누적 순매수."""
    try:
        return scrape_foreign_inst(str(stock_code).zfill(6))
    except:
        return {'외인_5d': np.nan, '외인_20d': np.nan,
                '기관_5d': np.nan, '기관_20d': np.nan}


def scrape_naver_consensus(stock_code, stock_name):
    result = {'종목코드': stock_code, '종목명': stock_name}
    try:
        session = get_session()
        resp = session.get(f"https://finance.naver.com/item/main.naver?code={stock_code}", timeout=7)
        resp.encoding = 'utf-8'
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.text, 'lxml')
        cop = soup.find('div', class_='section cop_analysis')
        if not cop: return None
        table = cop.find('table')
        if not table: return None
        rows = table.find_all('tr')
        if len(rows) < 5: return None

        hcells = rows[1].find_all(['th', 'td'])
        yi = []
        for c in hcells:
            m = re.search(r'(\d{4})[./]\d{2}', c.get_text(strip=True))
            yi.append((int(m.group(1)), '(E)' in c.get_text(strip=True)) if m else None)
        if not yi: return None

        ac = 4
        for c in rows[0].find_all(['th', 'td']):
            cs = c.get('colspan')
            if cs and ('연간' in c.get_text(strip=True) or '주요' in c.get_text(strip=True)):
                try: ac = int(cs)
                except: pass
                break

        dm = {}
        for mn in ['매출액', '영업이익']:
            candidate_rows = []
            for ai in range(2, min(15, len(rows))):
                ac2 = rows[ai].find_all(['th', 'td'])
                if not ac2: continue
                lb = ac2[0].get_text(strip=True)
                if mn == '매출액' and '매출' in lb:
                    candidate_rows.append(ac2)
                elif mn == '영업이익' and '영업이익' in lb and '률' not in lb:
                    candidate_rows.append(ac2)

            best_data = {}
            best_valid_count = -1
            for cs in candidate_rows:
                temp_data = {}
                valid_count = 0
                for i, cell in enumerate(cs[1:], 0):
                    if i >= len(yi) or yi[i] is None or i >= ac: break
                    val = parse_numeric(cell.get_text(strip=True))
                    temp_data[yi[i][0]] = val
                    if pd.notna(val):
                        valid_count += 1
                if valid_count > best_valid_count:
                    best_valid_count = valid_count
                    best_data = temp_data
            dm[mn] = best_data

        # FnGuide에서 2026E, 2027E, 2028E 보충 데이터 가져오기
        try:
            fg = scrape_fnguide_supplement(stock_code, stock_name)
            for mn in ['매출액', '영업이익']:
                if mn in fg:
                    for yr in [2025, 2026, 2027, 2028]:
                        if (yr not in dm.get(mn, {}) or pd.isna(dm.get(mn, {}).get(yr))) and yr in fg[mn] and pd.notna(fg[mn][yr]):
                            if mn not in dm: dm[mn] = {}
                            dm[mn][yr] = fg[mn][yr]
        except:
            pass

        if not dm: return None

        ty = [2025, 2026, 2027, 2028]; by = [2024, 2025, 2026, 2027]
        for m in ['매출액', '영업이익']:
            if m not in dm: continue
            for y in [2023, 2024] + ty: result[f'{m}_{y}'] = dm[m].get(y, np.nan)
        for m in ['매출액', '영업이익']:
            if m not in dm: continue
            mg = []
            for i, t in enumerate(ty):
                tv, bv = dm[m].get(t, np.nan), dm[m].get(by[i], np.nan)
                if pd.notna(tv) and pd.notna(bv) and bv != 0:
                    g = ((tv - bv) / abs(bv)) * 100
                    result[f'{m}_성장률_{t}'] = round(g, 2)
                    mg.append(g)
                else:
                    result[f'{m}_성장률_{t}'] = np.nan
            result[f'{m}_최대성장률'] = round(max(mg), 2) if mg else np.nan

        av = sum(1 for y in ty if any(pd.notna(result.get(f'{m}_{y}')) for m in ['매출액', '영업이익']))
        result['데이터_가용성'] = f'{av}년치 존재'
        result['가용_연도수'] = av

        # PER, PBR, ROE 크롤링
        try:
            indicators = scrape_naver_per_pbr_roe(stock_code)
            result['PER'] = indicators.get('PER', np.nan)
            result['PBR'] = indicators.get('PBR', np.nan)
            result['ROE'] = indicators.get('ROE', np.nan)
        except:
            result['PER'] = np.nan
            result['PBR'] = np.nan
            result['ROE'] = np.nan

        # 20일 평균 거래량 + OBV/RSI + 지지/저항선 + MA/MACD + 외인·기관 수급
        try:
            sup = fetch_supplement_indicators(stock_code)
            result['평균거래량_20d'] = sup.get('평균거래량_20d', np.nan)
            result['OBV_trend']     = sup.get('OBV_trend', '')
            result['RSI']           = sup.get('RSI', np.nan)
            result['저항선']         = sup.get('저항선', np.nan)
            result['지지선']         = sup.get('지지선', np.nan)
            result['MA_align']      = sup.get('MA_align', '')
            result['MACD_signal']   = sup.get('MACD_signal', '')
            result['외인_5d']        = sup.get('외인_5d', np.nan)
            result['외인_20d']       = sup.get('외인_20d', np.nan)
            result['기관_5d']        = sup.get('기관_5d', np.nan)
            result['기관_20d']       = sup.get('기관_20d', np.nan)
        except:
            for k in ['평균거래량_20d', 'RSI', '저항선', '지지선',
                      '외인_5d', '외인_20d', '기관_5d', '기관_20d']:
                result[k] = np.nan
            for k in ['OBV_trend', 'MA_align', 'MACD_signal']:
                result[k] = ''

        return result
    except Exception as e:
        return None


# ============================================================
# 크롤링 (데이터 수집만 - 캐시 저장)
# ============================================================
def crawl_all_data(progress_bar, status_text, markets, max_workers, resume=True):
    """전종목 컨센서스 데이터를 크롤링하고 캐시에 저장한다.

    Args:
        resume: True면 기존 캐시에 27E/28E가 채워진 종목은 건너뛰고
                빠진 것만 새로 수집. 중간에 멈춰도 이어서 가능.
    """
    status_text.markdown("⏳ **1단계:** 종목 리스트 수집 중...")
    progress_bar.progress(0.02)
    dfs = []
    if 'KOSPI' in markets: dfs.append(get_stock_list_naver("0"))
    if 'KOSDAQ' in markets: dfs.append(get_stock_list_naver("1"))
    if not dfs: return pd.DataFrame()
    stock_df = pd.concat(dfs, ignore_index=True)
    for p in ['스팩', 'SPAC', 'ETF', 'ETN', '리츠', 'REIT', '인버스', '레버리지', '선물', '채권']:
        stock_df = stock_df[~stock_df['종목명'].str.contains(p, na=False)]
    stock_df = stock_df.reset_index(drop=True)
    progress_bar.progress(0.10)
    status_text.markdown(f"✅ **1단계 완료:** {len(stock_df)}개 종목 수집")

    # ── Resume: 기존 캐시에서 27E·28E·OBV 모두 있는 종목은 건너뛰기 ─
    existing_results = []
    skip_codes = set()
    if resume and os.path.exists(CSV_FILE):
        try:
            old_df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
            old_df['종목코드'] = old_df['종목코드'].astype(str).str.zfill(6)
            # "완전한" 행 = 27E 매출액 + OBV_trend 둘 다 채워진 행
            valid_mask = old_df['매출액_2027'].notna()
            if 'OBV_trend' in old_df.columns:
                valid_mask &= old_df['OBV_trend'].fillna('').astype(str).ne('')
            valid = old_df[valid_mask]
            existing_results = valid.to_dict('records')
            skip_codes = set(valid['종목코드'])
        except Exception:
            pass

    rows = [r.to_dict() for _, r in stock_df.iterrows()]
    rows_to_crawl = [r for r in rows
                     if str(r['종목코드']).zfill(6) not in skip_codes]
    total_remaining = len(rows_to_crawl)
    total_overall = len(rows)

    if skip_codes:
        status_text.markdown(
            f"♻️ **이어서 수집:** 기존 캐시 {len(skip_codes):,}개 유지, "
            f"빠진 {total_remaining:,}개만 새로 수집 ({max_workers}워커)"
        )
    else:
        status_text.markdown(
            f"⏳ **2단계:** {total_remaining}개 종목 컨센서스 분석 중... ({max_workers}워커)"
        )

    if total_remaining == 0:
        df = pd.DataFrame(existing_results)
        save_cache(df, {'markets': markets, 'data_count': len(df), 'resumed_only': True})
        save_history(df); save_consensus_snapshot(df)
        progress_bar.progress(1.0)
        status_text.markdown(f"✅ 이미 모두 수집됨 ({len(df)}개)")
        return df

    results = list(existing_results)
    counter, lock = [0], threading.Lock()
    save_lock = threading.Lock()

    def process(rd):
        c = scrape_naver_consensus(rd['종목코드'], rd['종목명'])
        if c:
            c['시장'] = rd['시장']; c['현재가'] = rd['현재가']
            c['시가총액'] = rd['시가총액']; c['Recent_Volume'] = rd['Recent_Volume']
            avg_vol = c.get('평균거래량_20d', np.nan)
            today_vol = rd['Recent_Volume']
            if pd.notna(avg_vol) and avg_vol > 0 and today_vol > 0:
                c['거래량배수'] = round(today_vol / avg_vol, 1)
            else:
                c['거래량배수'] = np.nan
            return ('ok', c)
        return ('no', None)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(process, r): i for i, r in enumerate(rows_to_crawl)}
        for f in as_completed(futs):
            with lock:
                counter[0] += 1; cnt = counter[0]
            try:
                s, d = f.result()
                if s == 'ok': results.append(d)
            except:
                pass

            # 진행률 / 상태 (100 단위)
            if cnt % 100 == 0 or cnt == total_remaining:
                progress_bar.progress(min(0.10 + (cnt/total_remaining)*0.85, 0.95))
                status_text.markdown(
                    f"⏳ **2단계:** {cnt}/{total_remaining} ({cnt/total_remaining*100:.0f}%) | "
                    f"누적: {len(results):,}/{total_overall:,}개"
                )

            # 부분 저장 (500 단위) — 중간에 멈춰도 이어서 수집 가능
            if cnt % 500 == 0 and cnt > 0:
                with save_lock:
                    try:
                        partial_df = pd.DataFrame(results)
                        save_cache(partial_df, {
                            'markets': markets, 'partial': True,
                            'progress': f'{cnt}/{total_remaining}',
                            'data_count': len(results),
                        })
                    except:
                        pass

    progress_bar.progress(1.0)
    if not results: return pd.DataFrame()

    df = pd.DataFrame(results)
    meta = {'markets': markets, 'total_analyzed': total_overall, 'data_count': len(results)}
    save_cache(df, meta)

    # 누적 기록 + 컨센서스 스냅샷 저장 (Estimates Revision 분석용)
    save_history(df)
    save_consensus_snapshot(df)

    status_text.markdown(f"✅ **완료!** 총 {len(results):,}개 종목 → 캐시 저장됨")
    return df


# ============================================================
# 필터링 (캐시 데이터에서 즉시 필터링)
# ============================================================
def apply_filters(df, rev_thresh, op_thresh, min_vol, markets, req_min_rev_500=True, req_op_profit=True, drop_huge_loss=True, op_size_label="1000억 이상"):
    if df.empty: return df
    if markets: df = df[df['시장'].isin(markets)]
    if min_vol > 0: df = df[df['Recent_Volume'] >= min_vol]

    # 2026년 이후(2026/2027/2028) 영업이익 최대값 컬럼 생성 → 규모 필터 & 정렬에 사용
    def _op_max_26(row):
        vals = [row.get(f'영업이익_{y}', np.nan) for y in [2026, 2027, 2028]]
        vals = [v for v in vals if pd.notna(v)]
        return max(vals) if vals else np.nan
    df = df.copy()
    df['영업이익_26이후_최대'] = df.apply(_op_max_26, axis=1)

    # 영업이익 규모 필터 (단위: 억)
    if op_size_label == "300억 이하":
        df = df[df['영업이익_26이후_최대'].notna() & (df['영업이익_26이후_최대'] <= 300)]
    elif op_size_label == "500억~1000억":
        df = df[df['영업이익_26이후_최대'].notna() & (df['영업이익_26이후_최대'] >= 500) & (df['영업이익_26이후_최대'] <= 1000)]
    elif op_size_label == "1000억 이상":
        df = df[df['영업이익_26이후_최대'].notna() & (df['영업이익_26이후_최대'] >= 1000)]

    def strict_financial_check(row):
        yrs = [2023, 2024, 2025, 2026, 2027, 2028]
        for y in yrs:
            rv = row.get(f'매출액_{y}', np.nan)
            ov = row.get(f'영업이익_{y}', np.nan)
            if req_min_rev_500 and pd.notna(rv) and rv < 500: return False
            if req_op_profit and pd.notna(ov) and ov < 0: return False
            if drop_huge_loss and pd.notna(ov) and pd.notna(rv) and ov < 0 and abs(ov) > rv: return False
        return True

    df = df[df.apply(strict_financial_check, axis=1)].copy()
    def meets(row):
        rv = [row.get(f'매출액_성장률_{y}', np.nan) for y in [2025,2026,2027,2028]]
        ov = [row.get(f'영업이익_성장률_{y}', np.nan) for y in [2025,2026,2027,2028]]
        rv = [x for x in rv if pd.notna(x)]; ov = [x for x in ov if pd.notna(x)]
        return (any(x >= rev_thresh for x in rv)) or (any(x >= op_thresh for x in ov))
    df = df[df.apply(meets, axis=1)].copy()
    df = compute_card_fields(df)
    return df.sort_values('가시성기준_정렬점수', ascending=False).reset_index(drop=True)


def compute_card_fields(df):
    """카드 렌더에 필요한 파생 컬럼들을 추가한다.
    종합성장점수 / 미래가시성_순위·성장률·정렬점수 / Forward_PER / PEG /
    영업이익_26이후_최대 — 필터링 없이 전 종목에 적용 가능.
    """
    if df.empty:
        return df
    df = df.copy()

    # 영업이익 26~28 최대값
    def _op_max_26(row):
        vals = [row.get(f'영업이익_{y}', np.nan) for y in [2026, 2027, 2028]]
        vals = [v for v in vals if pd.notna(v)]
        return max(vals) if vals else np.nan
    if '영업이익_26이후_최대' not in df.columns:
        df['영업이익_26이후_최대'] = df.apply(_op_max_26, axis=1)

    # 점수 + 가시성 등급
    scores, priority_ranks, priority_scores, metric_pcts = [], [], [], []
    for _, row in df.iterrows():
        s = 0
        rm = row.get('매출액_최대성장률', np.nan)
        if pd.notna(rm): s += min(rm, 2000)
        om = row.get('영업이익_최대성장률', np.nan)
        if pd.notna(om): s += min(om, 2000)
        con = sum(1 for y in [2025, 2026, 2027, 2028]
                  if (pd.notna(row.get(f'매출액_성장률_{y}')) and row.get(f'매출액_성장률_{y}') > 30)
                  or (pd.notna(row.get(f'영업이익_성장률_{y}')) and row.get(f'영업이익_성장률_{y}') > 30))
        s += con * 50
        scores.append(round(s, 2))

        rv24 = row.get('매출액_2024', np.nan)
        rv25 = row.get('매출액_2025', np.nan)
        rv26 = row.get('매출액_2026', np.nan)
        rv27 = row.get('매출액_2027', np.nan)
        rv28 = row.get('매출액_2028', np.nan)

        pr, mt = 5, 0
        if pd.notna(rv28) and pd.notna(rv25) and rv25 > 0:
            pr = 1; mt = ((rv28 / rv25) ** (1/3) - 1) * 100
        elif pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 2; mt = ((rv27 / rv25) ** (1/2) - 1) * 100
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 3; mt = ((rv26 / rv25) - 1) * 100
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 4; mt = ((rv25 / rv24) - 1) * 100
        priority_ranks.append(pr)
        metric_pcts.append(mt)
        priority_scores.append((5 - pr) * 100_000_000 + mt)

    df['종합성장점수']        = scores
    df['미래가시성_순위']      = priority_ranks
    df['미래가시성_성장률']    = metric_pcts
    df['가시성기준_정렬점수'] = priority_scores

    # Forward PER & PEG (벡터화)
    fwd_g = df['영업이익_성장률_2026'].where(
        df['영업이익_성장률_2026'].notna(), df['영업이익_성장률_2025']
    )
    valid_fwd = df['PER'].notna() & (df['PER'] > 0) & fwd_g.notna() & (fwd_g > 0)
    df['Forward_PER'] = np.where(
        valid_fwd,
        (df['PER'] / (1 + fwd_g / 100)).round(1),
        np.nan,
    )
    df['PEG'] = np.where(
        valid_fwd & df['Forward_PER'].notna(),
        (df['Forward_PER'] / fwd_g).round(2),
        np.nan,
    )

    # ── 업종 상대 멀티플 (peer-relative multiples) — 추가 기능 ──────
    # 기존 PEG/Forward PER 계산은 그대로 두고, 별도 컬럼만 부여한다.
    # 실패해도 silent fail (기존 기능 영향 없도록).
    try:
        df = _apply_peer_multiples(df)
    except Exception:
        pass

    return df


def _apply_peer_multiples(df):
    """compute_card_fields의 마지막 단계에서 호출되는 피어 멀티플 부여.
    - universe_df는 같은 df (apply_filters 이후 검색 탭의 hits 등 부분집합)에서
      뽑되, 업종이 비어 있으면 sector_map.json으로 즉석 보강.
    - 결과 컬럼은 모두 'peer_*' 또는 fair_*/upside_pct/n_peers 등 새 이름이라
      기존 컬럼과 충돌하지 않는다.
    """
    from industry_multiple import compute_for_target

    peer_cols_init = {
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
        'peer_status':        '',
    }
    for c, default in peer_cols_init.items():
        if c not in df.columns:
            df[c] = default

    # 업종 보강 (universe로 사용할 df 기준)
    univ = df
    if '업종' not in univ.columns or univ['업종'].isna().all():
        try:
            sector_map = _load_sector_map()
            univ = univ.copy()
            univ['업종'] = univ['종목코드'].astype(str).str.zfill(6).map(sector_map).fillna('기타')
            df['업종']   = univ['업종']
        except Exception:
            return df

    # 피어 모집단이 너무 작으면 (e.g. 검색 탭의 hits 1~3개) — universe로 부족
    # 이 경우는 호출자(개별종목확인 탭)가 직접 all_df를 universe로 넣도록 별도
    # 헬퍼 apply_peer_multiples_with_universe()가 있다.
    for idx, row in df.iterrows():
        try:
            res = compute_for_target(row, univ)
            for k, v in res.items():
                df.at[idx, k] = v
        except Exception:
            continue
    return df


def apply_peer_multiples_with_universe(df, universe_df):
    """피어 모집단(universe_df)을 외부에서 명시적으로 지정해 부여.
    검색 탭처럼 df가 부분집합이고 universe는 전체 캐시일 때 사용.
    universe_df에 '업종' 컬럼이 없으면 자동 보강.
    """
    from industry_multiple import compute_for_target

    df = df.copy()
    universe_df = universe_df.copy() if universe_df is not None else df

    # universe에 업종 보강
    if '업종' not in universe_df.columns or universe_df['업종'].isna().all():
        try:
            sector_map = _load_sector_map()
            universe_df['업종'] = universe_df['종목코드'].astype(str).str.zfill(6).map(sector_map).fillna('기타')
        except Exception:
            return df
    # df 자체에도 업종 보강
    if '업종' not in df.columns or df['업종'].isna().all():
        try:
            sector_map = _load_sector_map()
            df['업종'] = df['종목코드'].astype(str).str.zfill(6).map(sector_map).fillna('기타')
        except Exception:
            return df

    peer_cols_init = {
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
        'peer_status':        '',
    }
    for c, default in peer_cols_init.items():
        df[c] = default

    for idx, row in df.iterrows():
        try:
            res = compute_for_target(row, universe_df)
            for k, v in res.items():
                df.at[idx, k] = v
        except Exception:
            continue
    return df


# ============================================================
# 포맷 함수
# ============================================================
def format_number(v):
    if pd.isna(v): return "-"
    if abs(v) >= 1_000_000: return f"{v/10000:.0f}조"
    return f"{v:,.0f}" if abs(v) >= 10000 else f"{v:,.1f}"

def format_mcap(v):
    """시가총액·적정시총 표시용: 'X조Y억' 형태 (단위: 억원 입력 가정)."""
    if pd.isna(v) or v is None: return "-"
    val = float(v)
    sign = '-' if val < 0 else ''
    a = abs(val)
    if a >= 10000:
        jo = int(a // 10000)
        eok = int(round(a - jo * 10000))
        if eok >= 10000:  # 반올림 캐리 처리
            jo += 1; eok = 0
        return f"{sign}{jo:,}조" if eok == 0 else f"{sign}{jo:,}조 {eok:,}억"
    return f"{sign}{a:,.0f}억"

def format_growth(v):
    if pd.isna(v): return '<span style="color:#CED4DA;">-</span>'
    if v >= 100: return f'<span class="growth-mega">🔥 {v:,.1f}%</span>'
    if v > 0: return f'<span class="growth-positive">▲ {v:,.1f}%</span>'
    if v < 0: return f'<span class="growth-negative">▼ {v:,.1f}%</span>'
    return f'<span style="color:#6C757D; font-family:\'JetBrains Mono\', monospace;">{v:,.1f}%</span>'

def format_price(v):
    return f"{int(v):,}원" if (not pd.isna(v) and v > 0) else "-"

def format_volume(v):
    if pd.isna(v) or v == 0: return "-"
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000: return f"{v/1_000:.0f}K"
    return f"{int(v)}"

def format_turnover(volume, price):
    if pd.isna(volume) or pd.isna(price) or volume == 0 or price == 0: return "-"
    won = volume * price
    if won >= 1_000_000_000_000: return f"{won/1_000_000_000_000:.1f}조"
    if won >= 100_000_000: return f"{won/100_000_000:,.0f}억"
    if won >= 10_000: return f"{won/10_000:,.0f}만"
    return f"{int(won):,}원"


# ============================================================
# 성장률 라인차트 (SVG, 카드 내부에 인라인 삽입)
# ============================================================
def build_growth_svg(rev_vals, op_vals, year_labels, width=420, height=130):
    """매출/영업이익 성장률을 카드 내부에 그리는 SVG 라인차트."""
    pad_l, pad_r, pad_t, pad_b = 34, 14, 26, 22
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    rev_clean = [v for v in rev_vals if pd.notna(v)]
    op_clean  = [v for v in op_vals  if pd.notna(v)]
    all_vals  = rev_clean + op_clean

    legend = (
        f'<g transform="translate({pad_l},10)" font-family="Inter, sans-serif" font-size="11" fill="#CBD5E0">'
        f'<rect x="0" y="3" width="14" height="3" fill="#34D399" rx="1"/>'
        f'<text x="20" y="11">매출 성장률</text>'
        f'<rect x="110" y="3" width="14" height="3" fill="#A78BFA" rx="1"/>'
        f'<text x="130" y="11">영업이익 성장률</text>'
        f'</g>'
    )

    if not all_vals:
        empty = (
            f'<text x="{width/2}" y="{height/2 + 4}" fill="#94A3B8" '
            f'text-anchor="middle" font-size="13" font-family="Inter, sans-serif">'
            f'성장률 데이터 없음</text>'
        )
        return (
            f'<svg width="100%" viewBox="0 0 {width} {height}" '
            f'preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" '
            f'style="display:block;">{legend}{empty}</svg>'
        )

    vmin, vmax = min(all_vals), max(all_vals)
    if vmin > 0: vmin = 0
    if vmax < 0: vmax = 0
    if vmin == vmax:
        vmin -= 10; vmax += 10
    span = vmax - vmin
    pad = span * 0.12 if span > 0 else 10
    vmin -= pad; vmax += pad
    span = vmax - vmin if vmax != vmin else 1

    n = len(year_labels)
    def x_at(i):
        return pad_l + (inner_w * i / max(1, n - 1))
    def y_at(v):
        return pad_t + inner_h - ((v - vmin) / span) * inner_h

    # 가로 그리드 + 0% 기준선
    grid = ''
    for frac in (0.0, 0.5, 1.0):
        gy = pad_t + inner_h * frac
        grid += (
            f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{width - pad_r}" y2="{gy:.1f}" '
            f'stroke="#4A5568" stroke-width="0.5" stroke-dasharray="2,3" opacity="0.6"/>'
        )
    if vmin <= 0 <= vmax:
        zy = y_at(0)
        grid += (
            f'<line x1="{pad_l}" y1="{zy:.1f}" x2="{width - pad_r}" y2="{zy:.1f}" '
            f'stroke="#94A3B8" stroke-width="0.8" stroke-dasharray="3,3" opacity="0.55"/>'
        )

    # X축 라벨
    x_labels = ''
    for i, lab in enumerate(year_labels):
        x_labels += (
            f'<text x="{x_at(i):.1f}" y="{height - 8}" fill="#94A3B8" '
            f'font-size="11" text-anchor="middle" '
            f'font-family="JetBrains Mono, monospace">{lab}</text>'
        )

    # Y축 라벨 (vmin / vmax)
    y_labels = (
        f'<text x="{pad_l - 6}" y="{pad_t + 4:.1f}" fill="#94A3B8" font-size="9" '
        f'text-anchor="end" font-family="JetBrains Mono, monospace">{vmax:.0f}%</text>'
        f'<text x="{pad_l - 6}" y="{pad_t + inner_h + 3:.1f}" fill="#94A3B8" font-size="9" '
        f'text-anchor="end" font-family="JetBrains Mono, monospace">{vmin:.0f}%</text>'
    )

    def build_line(vals, color, label_color, label_above=True):
        pts = [(x_at(i), y_at(v), v) for i, v in enumerate(vals) if pd.notna(v)]
        if not pts:
            return ''
        dy = -7 if label_above else 13
        if len(pts) == 1:
            x, y, v = pts[0]
            return (
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"/>'
                f'<text x="{x:.1f}" y="{y + dy:.1f}" fill="{label_color}" font-size="9" '
                f'text-anchor="middle" font-family="JetBrains Mono, monospace" font-weight="700" '
                f'paint-order="stroke" stroke="rgba(17,24,39,0.85)" stroke-width="2">'
                f'{v:.0f}%</text>'
            )
        d = ' '.join(f'{"M" if i == 0 else "L"} {x:.1f} {y:.1f}' for i, (x, y, _) in enumerate(pts))
        circles = ''.join(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.2" fill="{color}" stroke="#1F2937" stroke-width="1"/>'
            for x, y, _ in pts
        )
        labels = ''.join(
            f'<text x="{x:.1f}" y="{y + dy:.1f}" fill="{label_color}" font-size="9" '
            f'text-anchor="middle" font-family="JetBrains Mono, monospace" font-weight="700" '
            f'paint-order="stroke" stroke="rgba(17,24,39,0.85)" stroke-width="2">'
            f'{v:.0f}%</text>'
            for x, y, v in pts
        )
        return (
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2.2" '
            f'stroke-linecap="round" stroke-linejoin="round"/>'
            + circles + labels
        )

    # 매출은 라벨을 점 위에, 영업이익은 점 아래에 배치해 겹침 방지
    rev_path = build_line(rev_vals, '#34D399', '#6EE7B7', label_above=True)
    op_path  = build_line(op_vals,  '#A78BFA', '#C4B5FD', label_above=False)

    return (
        f'<svg width="100%" viewBox="0 0 {width} {height}" '
        f'preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" '
        f'style="display:block;">'
        f'{grid}{legend}{rev_path}{op_path}{x_labels}{y_labels}'
        f'</svg>'
    )


def obv_rsi_verdict(obv_trend, rsi, macd_signal=''):
    """OBV + RSI + MACD 3종 동시 판정.
    우선순위: ① 트리플 동행 → ② 추세 전환 동행 → ③ 다이버전스
            → ④ RSI 극단 → ⑤ 일반 추세 → ⑥ 점수 기반 fallback.
    """
    obv_known  = isinstance(obv_trend, str) and obv_trend != ''
    rsi_known  = pd.notna(rsi)
    macd_known = isinstance(macd_signal, str) and macd_signal in (
        'bull_cross', 'bear_cross', 'bull', 'bear'
    )
    if not (obv_known or rsi_known or macd_known):
        return {'verdict': '데이터 없음', 'color': '#94A3B8', 'icon': '·',
                'reason': '기술 지표 미수집'}

    obv_up = obv_trend == 'up'
    obv_dn = obv_trend == 'down'

    rsi_over  = rsi_known and rsi >= 70   # 과매수
    rsi_under = rsi_known and rsi <= 30   # 과매도
    rsi_bull  = rsi_known and 50 <= rsi < 70
    rsi_bear  = rsi_known and 30 < rsi < 50

    macd_gc   = macd_signal == 'bull_cross'   # 골든크로스
    macd_dc   = macd_signal == 'bear_cross'   # 데드크로스
    macd_bull = macd_signal == 'bull'         # 상승 진행
    macd_bear = macd_signal == 'bear'         # 하락 진행

    # ── ① 트리플 동행 (최강 신호) ───────────────────────────
    if macd_gc and rsi_under and obv_up:
        return {'verdict': '바닥 매수', 'color': '#10B981', 'icon': '🚀',
                'reason': '골든크로스 + 과매도 + OBV 매집 (트리플 매수)'}
    if macd_dc and rsi_over and obv_dn:
        return {'verdict': '고점 매도', 'color': '#EF4444', 'icon': '💀',
                'reason': '데드크로스 + 과매수 + OBV 분산 (트리플 매도)'}

    # ── ② 추세 전환 (크로스 + RSI/OBV 동행) ─────────────────
    if macd_gc and (rsi_bull or rsi_under) and obv_up:
        return {'verdict': '추세 확정', 'color': '#10B981', 'icon': '💎',
                'reason': '골든크로스 + RSI 강세 + OBV 매집 (강한 상승 추세 진입)'}
    if macd_dc and (rsi_bear or rsi_over) and obv_dn:
        return {'verdict': '추세 붕괴', 'color': '#EF4444', 'icon': '🔻',
                'reason': '데드크로스 + RSI 약세 + OBV 분산 (하락 추세 진입)'}
    if macd_gc and obv_up:
        return {'verdict': '매수 진입', 'color': '#34D399', 'icon': '✦',
                'reason': '골든크로스 + OBV 매집 (단기 진입 신호)'}
    if macd_dc and obv_dn:
        return {'verdict': '매도 진입', 'color': '#F87171', 'icon': '✦',
                'reason': '데드크로스 + OBV 분산 (단기 이탈 신호)'}

    # ── ③ 다이버전스 (수급 vs 추세 불일치) ───────────────────
    if (macd_bear or macd_dc) and obv_up:
        return {'verdict': '강세 다이버전스', 'color': '#34D399', 'icon': '🔄',
                'reason': 'MACD 약세이지만 OBV 매집 (반등 가능성)'}
    if (macd_bull or macd_gc) and obv_dn:
        return {'verdict': '약세 다이버전스', 'color': '#F59E0B', 'icon': '⚠',
                'reason': 'MACD 강세이지만 OBV 분산 (가짜 신호 주의)'}

    # ── ④ RSI 극단 + MACD 보강 ──────────────────────────────
    if rsi_over and (macd_bull or macd_gc) and obv_up:
        return {'verdict': '과열 주의', 'color': '#F59E0B', 'icon': '🔥',
                'reason': 'RSI 과매수 + MACD 강세 + 매집 (차익실현 검토)'}
    if rsi_over and (macd_bear or macd_dc):
        return {'verdict': '고점 신호', 'color': '#EF4444', 'icon': '▼',
                'reason': 'RSI 과매수 + MACD 약세 (단기 조정 위험)'}
    if rsi_under and (macd_bull or macd_gc):
        return {'verdict': '저평가 매수', 'color': '#10B981', 'icon': '★',
                'reason': 'RSI 과매도 + MACD 강세 (반등 진입 시점)'}
    if rsi_under and (macd_bear or macd_dc):
        return {'verdict': '약세 관망', 'color': '#94A3B8', 'icon': '⏳',
                'reason': 'RSI 과매도 + MACD 약세 (반등 신호 대기)'}

    # ── ④' RSI 극단 단독 (MACD 신호 없을 때) ─────────────────
    if rsi_over and not (macd_bull or macd_gc or macd_bear or macd_dc):
        return {'verdict': '과열 경계', 'color': '#F59E0B', 'icon': '⚠',
                'reason': 'RSI 과매수 (단기 차익실현 검토)'}
    if rsi_under and not (macd_bull or macd_gc or macd_bear or macd_dc):
        return {'verdict': '저점 경계', 'color': '#34D399', 'icon': '★',
                'reason': 'RSI 과매도 (반등 시점 모색)'}

    # ── ⑤ 일반 추세 (3지표 일관 동행) ───────────────────────
    if (macd_bull or macd_gc) and rsi_bull and obv_up:
        return {'verdict': '상승 추세', 'color': '#10B981', 'icon': '📈',
                'reason': 'MACD·RSI·OBV 모두 강세 (정석 보유)'}
    if (macd_bear or macd_dc) and rsi_bear and obv_dn:
        return {'verdict': '하락 추세', 'color': '#EF4444', 'icon': '📉',
                'reason': 'MACD·RSI·OBV 모두 약세 (손절선 점검)'}

    # ── ⑥ 점수 기반 fallback ────────────────────────────────
    obv_s  = 1 if obv_up  else (-1 if obv_dn  else 0)
    rsi_s  = 1 if (rsi_bull or rsi_over)  else (-1 if (rsi_bear or rsi_under) else 0)
    macd_s = 2 if macd_gc else (1 if macd_bull else
             (-2 if macd_dc else (-1 if macd_bear else 0)))
    total  = obv_s + rsi_s + macd_s   # range: -4 ~ +4

    if total >= 3:
        return {'verdict': '강세 우위', 'color': '#34D399', 'icon': '↗',
                'reason': f'3지표 종합 +{total} (강세 우위)'}
    if total >= 1:
        return {'verdict': '매집 진행', 'color': '#34D399', 'icon': '▲',
                'reason': f'3지표 종합 +{total} (수급 양호)'}
    if total <= -3:
        return {'verdict': '약세 우위', 'color': '#F87171', 'icon': '↘',
                'reason': f'3지표 종합 {total} (약세 우위)'}
    if total <= -1:
        return {'verdict': '분산 진행', 'color': '#EF4444', 'icon': '▽',
                'reason': f'3지표 종합 {total} (수급 약화)'}
    return {'verdict': '중립', 'color': '#62EFFF', 'icon': '·',
            'reason': '특별한 시그널 없음'}

# ============================================================
# 접근 제어 (비밀번호)
# ============================================================
def check_password():
    def password_entered():
        if st.session_state["password"] == "9084":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown('<div class="hero-header"><div class="rainbow-title"><svg viewBox="0 0 24 24" width="28" height="28" stroke="currentColor" stroke-width="2" fill="none"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path></svg> SYSTEM LOCKED</div><p>퀀트 스크리닝 터미널 · 인증 필요</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("비밀번호 입력", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown('<div class="hero-header"><div class="rainbow-title"><svg viewBox="0 0 24 24" width="28" height="28" stroke="currentColor" stroke-width="2" fill="none"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path></svg> SYSTEM LOCKED</div><p>퀀트 스크리닝 터미널 · 인증 필요</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("비밀번호 입력", type="password", on_change=password_entered, key="password")
            st.error("인증 실패.")
        return False
    return True

# ============================================================
# 메인 UI
# ============================================================
def render_stock_card(row, rank):
    code = row.get('종목코드','')
    name = row.get('종목명','')
    market = row.get('시장','')
    price = row.get('현재가',0)
    volume = row.get('Recent_Volume',0)
    mcap = row.get('시가총액',0)
    score = row.get('종합성장점수',0)
    avail = row.get('데이터_가용성','-')

    rg25 = row.get('매출액_성장률_2025', np.nan)
    rg26 = row.get('매출액_성장률_2026', np.nan)
    rg27 = row.get('매출액_성장률_2027', np.nan)
    rg28 = row.get('매출액_성장률_2028', np.nan)
    og25 = row.get('영업이익_성장률_2025', np.nan)
    og26 = row.get('영업이익_성장률_2026', np.nan)
    og27 = row.get('영업이익_성장률_2027', np.nan)
    og28 = row.get('영업이익_성장률_2028', np.nan)

    per_val     = row.get('PER', np.nan)
    pbr_val     = row.get('PBR', np.nan)
    roe_val     = row.get('ROE', np.nan)
    sector_per  = row.get('업종평균PER', np.nan)
    fwd_per_val = row.get('Forward_PER', np.nan)
    peg_val     = row.get('PEG', np.nan)
    vol_ratio   = row.get('거래량배수', np.nan)
    fair_mc_28   = row.get('적정시총_2028E', np.nan)
    fair_px_28   = row.get('적정주가_2028E', np.nan)
    gap_28       = row.get('괴리율_2028E', np.nan)
    peer_mult_28 = row.get('업종_2028E_멀티플_중앙값', np.nan)
    ref_name_28  = str(row.get('멀티플기준_종목명_2028E', '') or '')
    is_fallback  = bool(row.get('멀티플_시장폴백_2028E', False))

    code_str = str(code).zfill(6)
    nurl = f"https://finance.naver.com/item/main.naver?code={code_str}"

    badge_cls = 'qcd-badge-kospi' if market == 'KOSPI' else 'qcd-badge-kosdaq'
    badge = f'<span class="{badge_cls}">{market}</span>'
    si2 = "⭐" if score >= 500 else "▪"

    # ── OBV/RSI/지지/저항/MA/MACD: 캐시에 없으면 즉석 계산 ─────
    obv_trend   = row.get('OBV_trend', '')
    rsi_val     = row.get('RSI', np.nan)
    resistance  = row.get('저항선', np.nan)
    support     = row.get('지지선', np.nan)
    ma_align    = row.get('MA_align', '')
    macd_signal = row.get('MACD_signal', '')
    need_fetch = (
        (not isinstance(obv_trend, str) or obv_trend == '')
        or pd.isna(rsi_val) or pd.isna(resistance) or pd.isna(support)
        or (not isinstance(ma_align, str) or ma_align == '')
        or (not isinstance(macd_signal, str) or macd_signal == '')
    )
    if need_fetch:
        try:
            ind = compute_obv_rsi_cached(code_str)
            if not isinstance(obv_trend, str) or obv_trend == '':
                obv_trend = ind.get('OBV_trend', '')
            if pd.isna(rsi_val):
                rsi_val = ind.get('RSI', np.nan)
            if pd.isna(resistance):
                resistance = ind.get('저항선', np.nan)
            if pd.isna(support):
                support = ind.get('지지선', np.nan)
            if not isinstance(ma_align, str) or ma_align == '':
                ma_align = ind.get('MA_align', '')
            if not isinstance(macd_signal, str) or macd_signal == '':
                macd_signal = ind.get('MACD_signal', '')
        except:
            pass

    # ── 외인·기관 5d/20d 순매수: 캐시에 없으면 즉석 계산 ────────
    foreign_5d  = row.get('외인_5d', np.nan)
    foreign_20d = row.get('외인_20d', np.nan)
    inst_5d     = row.get('기관_5d', np.nan)
    inst_20d    = row.get('기관_20d', np.nan)
    if pd.isna(foreign_5d) and pd.isna(inst_5d):
        try:
            fi = fetch_foreign_inst_cached(code_str)
            foreign_5d  = fi.get('외인_5d', np.nan)
            foreign_20d = fi.get('외인_20d', np.nan)
            inst_5d     = fi.get('기관_5d', np.nan)
            inst_20d    = fi.get('기관_20d', np.nan)
        except:
            pass

    # ── 컨센서스 변경 추세 (1M) ─────────────────────────────────
    revision = calc_consensus_revision(code_str, row)

    verdict = obv_rsi_verdict(obv_trend, rsi_val, macd_signal)

    # ── 거래량 폭증 배수 표시 ─────────────────────────────────
    if pd.notna(vol_ratio):
        if vol_ratio >= 5:
            vol_ratio_html = f'<span style="color:#FCA5A5;font-weight:800;font-family:\'JetBrains Mono\',monospace;">🔥 {vol_ratio:.1f}x</span>'
        elif vol_ratio >= 2:
            vol_ratio_html = f'<span style="color:#FBBF24;font-weight:700;font-family:\'JetBrains Mono\',monospace;">▲ {vol_ratio:.1f}x</span>'
        else:
            vol_ratio_html = f'<span style="color:#94A3B8;font-family:\'JetBrains Mono\',monospace;">{vol_ratio:.1f}x</span>'
    else:
        vol_ratio_html = '<span style="color:#64748B;">-</span>'

    # ── 지표 포맷 ──────────────────────────────────────────────
    per_str     = f'{per_val:.1f}' if pd.notna(per_val) else '-'
    pbr_str     = f'{pbr_val:.2f}' if pd.notna(pbr_val) else '-'
    roe_str     = f'{roe_val:.1f}%' if pd.notna(roe_val) else '-'
    fwd_per_str = f'{fwd_per_val:.1f}' if pd.notna(fwd_per_val) else '-'
    peg_str     = f'{peg_val:.2f}' if pd.notna(peg_val) else '-'

    # ── 재무 소스 값 먼저 로드 (차트에서 '24 성장률 계산에 필요) ──
    rv23 = row.get('매출액_2023', np.nan); rv24 = row.get('매출액_2024', np.nan)
    rv25 = row.get('매출액_2025', np.nan); rv26 = row.get('매출액_2026', np.nan)
    rv27 = row.get('매출액_2027', np.nan); rv28 = row.get('매출액_2028', np.nan)
    ov23 = row.get('영업이익_2023', np.nan); ov24 = row.get('영업이익_2024', np.nan)
    ov25 = row.get('영업이익_2025', np.nan); ov26 = row.get('영업이익_2026', np.nan)
    ov27 = row.get('영업이익_2027', np.nan); ov28 = row.get('영업이익_2028', np.nan)

    def _yoy(curr, base):
        if pd.notna(curr) and pd.notna(base) and base != 0:
            return ((curr - base) / abs(base)) * 100
        return np.nan
    rg24 = _yoy(rv24, rv23)
    og24 = _yoy(ov24, ov23)

    # ── 성장률 라인차트 (SVG) — '23 데이터 기준 '24부터 표시 ────
    chart_svg = build_growth_svg(
        [rg24, rg25, rg26, rg27, rg28],
        [og24, og25, og26, og27, og28],
        ["'24", "'25E", "'26E", "'27E", "'28E"],
    )

    def fv(v):
        return '-' if pd.isna(v) else f'{v:,.0f}'
    def fv_color(v):
        if pd.isna(v): return '#64748B'
        return '#FCA5A5' if v > 0 else '#93C5FD'

    hdr = 'display:flex;gap:0;font-size:0.7rem;color:#94A3B8;margin-bottom:4px;border-bottom:1px solid #4A5568;padding-bottom:3px;'
    rw = "display:flex;gap:0;font-size:0.8rem;margin-bottom:2px;font-family:'JetBrains Mono', monospace;"
    lb = 'width:80px;padding:2px 6px;color:#94A3B8;font-size:0.75rem;flex-shrink:0;'
    cc = 'flex:1;text-align:right;padding:2px 6px;'
    ce = 'flex:1;text-align:right;padding:2px 6px;font-weight:700;'

    # ── 컨센 변경(1M) 한 줄: 스냅샷 있을 때만 표시 ──────────────
    revision_line = ''
    if revision:
        def _rev_chip(v, label):
            if pd.isna(v):
                return ''
            color = '#34D399' if v > 0 else ('#F87171' if v < 0 else '#94A3B8')
            arrow = '▲' if v > 0 else ('▼' if v < 0 else '·')
            return (
                f'<span style="color:#94A3B8;font-size:0.72rem;margin-right:3px;">{label}</span>'
                f'<span style="color:{color};font-family:\'JetBrains Mono\',monospace;'
                f'font-weight:700;font-size:0.78rem;margin-right:10px;">{arrow} {v:+.1f}%</span>'
            )
        chips = _rev_chip(revision.get('rev'), '매출') + _rev_chip(revision.get('op'), '영업이익')
        if chips:
            yr = revision.get('year')
            revision_line = (
                f'<div style="margin-top:6px;padding-top:6px;border-top:1px dashed rgba(74,85,104,0.5);'
                f'display:flex;align-items:center;flex-wrap:wrap;gap:4px;">'
                f'<span style="color:#94A3B8;font-size:0.7rem;font-weight:600;margin-right:6px;">'
                f'📈 컨센 변경 (1M, {yr}E)</span>{chips}'
                f'</div>'
            )

    evidence_html = (
        f'<div class="qcd-evidence">'
        f'<div class="head">재무 소스 (단위: 억원)</div>'
        f'<div class="evidence-scroll">'
        f'<div style="{hdr}"><div style="{lb}"></div>'
        f'<div style="{cc}">\'23</div><div style="{cc}">\'24</div>'
        f'<div style="{cc}color:#62EFFF;">\'25E</div>'
        f'<div style="{cc}color:#62EFFF;">\'26E</div>'
        f'<div style="{cc}color:#62EFFF;">\'27E</div>'
        f'<div style="{cc}color:#62EFFF;">\'28E</div></div>'
        f'<div style="{rw}"><div style="{lb}">매출액</div>'
        f'<div style="{cc}color:{fv_color(rv23)};">{fv(rv23)}</div>'
        f'<div style="{cc}color:{fv_color(rv24)};">{fv(rv24)}</div>'
        f'<div style="{ce}color:{fv_color(rv25)};">{fv(rv25)}</div>'
        f'<div style="{ce}color:{fv_color(rv26)};">{fv(rv26)}</div>'
        f'<div style="{ce}color:{fv_color(rv27)};">{fv(rv27)}</div>'
        f'<div style="{ce}color:{fv_color(rv28)};">{fv(rv28)}</div></div>'
        f'<div style="{rw}"><div style="{lb}">영업이익</div>'
        f'<div style="{cc}color:{fv_color(ov23)};">{fv(ov23)}</div>'
        f'<div style="{cc}color:{fv_color(ov24)};">{fv(ov24)}</div>'
        f'<div style="{ce}color:{fv_color(ov25)};">{fv(ov25)}</div>'
        f'<div style="{ce}color:{fv_color(ov26)};">{fv(ov26)}</div>'
        f'<div style="{ce}color:{fv_color(ov27)};">{fv(ov27)}</div>'
        f'<div style="{ce}color:{fv_color(ov28)};">{fv(ov28)}</div></div>'
        f'</div>'
        f'{revision_line}'
        f'</div>'
    )

    # ── OBV / RSI 박스 ─────────────────────────────────────────
    obv_label_map = {'up': '매집 ↗', 'down': '분산 ↘', 'flat': '횡보 →', '': '데이터 없음'}
    obv_color_map = {'up': '#34D399', 'down': '#F87171', 'flat': '#94A3B8', '': '#64748B'}
    obv_label = obv_label_map.get(obv_trend, '데이터 없음')
    obv_color = obv_color_map.get(obv_trend, '#64748B')

    if pd.isna(rsi_val):
        rsi_str = '-'
        rsi_zone = '데이터 없음'
        rsi_color = '#64748B'
    else:
        rsi_str = f'{rsi_val:.1f}'
        if rsi_val >= 70:
            rsi_zone = '과매수'; rsi_color = '#F87171'
        elif rsi_val <= 30:
            rsi_zone = '과매도'; rsi_color = '#34D399'
        else:
            rsi_zone = '중립'; rsi_color = '#62EFFF'

    # ── 가격 레벨 (60일 지지/저항선, 현재가 위치) ────────────
    if pd.notna(resistance) and pd.notna(support) and resistance > support:
        cur_for_pos = price if (price and price > 0) else resistance
        pos_pct = max(0.0, min(100.0, (cur_for_pos - support) / (resistance - support) * 100.0))
        if pos_pct >= 80:
            pos_color = '#F87171'; pos_zone = '저항 근접'
        elif pos_pct <= 20:
            pos_color = '#34D399'; pos_zone = '지지 근접'
        else:
            pos_color = '#FBBF24'; pos_zone = '구간 내'
        res_str = f'{int(resistance):,}'
        sup_str = f'{int(support):,}'
        cur_str = f'{int(cur_for_pos):,}'
        level_html = (
            f'<div class="qcd-tech-mid">'
            f'<div class="qcd-tech-label">📊 60일 가격 레벨</div>'
            f'<div class="qcd-level-line"><span class="lk">저항</span>'
            f'<span class="lv" style="color:#F87171;">{res_str}</span></div>'
            f'<div class="qcd-level-line"><span class="lk">지지</span>'
            f'<span class="lv" style="color:#34D399;">{sup_str}</span></div>'
            f'<div class="qcd-level-bar">'
            f'<div class="qcd-level-marker" style="left:calc({pos_pct:.1f}% - 1.5px);"></div>'
            f'</div>'
            f'<div class="qcd-level-pos">현재 <b style="color:{pos_color};">{cur_str}</b> '
            f'· <b style="color:{pos_color};">{pos_pct:.0f}%</b> ({pos_zone})</div>'
            f'</div>'
        )
    else:
        level_html = (
            f'<div class="qcd-tech-mid">'
            f'<div class="qcd-tech-label">📊 60일 가격 레벨</div>'
            f'<div class="qcd-level-pos" style="text-align:left;">데이터 없음</div>'
            f'</div>'
        )

    # ── 이평선 정배열 / MACD 라벨 ──────────────────────────────
    ma_map = {
        'up':    ('정배열 ▲',  '#34D399'),
        'down':  ('역배열 ▽',  '#F87171'),
        'mixed': ('혼조 →',     '#94A3B8'),
        '':      ('-',         '#64748B'),
    }
    macd_map = {
        'bull_cross': ('골든크로스 ✦', '#34D399'),
        'bear_cross': ('데드크로스 ✦', '#F87171'),
        'bull':       ('상승 ↗',       '#34D399'),
        'bear':       ('하락 ↘',       '#F87171'),
        '':           ('-',            '#64748B'),
    }
    ma_label,   ma_color   = ma_map.get(ma_align, ma_map[''])
    macd_label, macd_color = macd_map.get(macd_signal, macd_map[''])

    tech_html = (
        f'<div class="qcd-tech-box">'
        f'<div class="qcd-tech-left">'
        f'<div class="qcd-tech-label">📡 보조지표 분석</div>'
        f'<div class="qcd-tech-row">'
        f'<div class="qcd-tech-item"><span class="k">OBV 추세</span>'
        f'<span class="v" style="color:{obv_color};">{obv_label}</span></div>'
        f'<div class="qcd-tech-item"><span class="k">RSI(14)</span>'
        f'<span class="v" style="color:{rsi_color};">{rsi_str} '
        f'<span style="font-size:0.74rem;color:#94A3B8;font-weight:600;">({rsi_zone})</span></span></div>'
        f'</div>'
        f'<div class="qcd-tech-row">'
        f'<div class="qcd-tech-item"><span class="k">이평 (5/20/60)</span>'
        f'<span class="v" style="color:{ma_color};">{ma_label}</span></div>'
        f'<div class="qcd-tech-item"><span class="k">MACD (12/26/9)</span>'
        f'<span class="v" style="color:{macd_color};">{macd_label}</span></div>'
        f'</div>'
        f'</div>'
        f'{level_html}'
        f'<div class="qcd-tech-right">'
        f'<span class="k" style="color:#94A3B8;font-size:0.72rem;font-weight:600;">종합 판정</span>'
        f'<span class="qcd-verdict-big" style="color:{verdict["color"]};">'
        f'{verdict["icon"]} {verdict["verdict"]}</span>'
        f'<span class="qcd-verdict-reason">↳ {verdict["reason"]}</span>'
        f'</div>'
        f'</div>'
    )

    # ── 6개 지표 pill (Fwd PER, ROE는 highlight) ───────────────
    def pill(label, val, hi=False, tip=''):
        cls = 'qcd-pill hi' if hi else 'qcd-pill'
        title_attr = f' title="{tip}"' if tip else ''
        return f'<div class="{cls}"{title_attr}><div class="lbl">{label}</div><div class="val">{val}</div></div>'

    sec_per_str = f'{sector_per:.1f}' if pd.notna(sector_per) else '-'

    # ── 적정시총 / 적정주가 / 괴리율 (2028E 영업이익 × 업종 멀티플 중앙값) ──
    fair_mc_str = format_mcap(fair_mc_28)
    fair_px_str = f'{int(round(fair_px_28)):,}원' if pd.notna(fair_px_28) and fair_px_28 > 0 else '-'
    # 적정시총 표시에 기준 멀티플 + 기준 종목명 작게 부기. 시장 폴백시 색상 구분
    if pd.notna(peer_mult_28) and pd.notna(fair_mc_28):
        _src_label = '*시장' if is_fallback else ''
        _src_color = '#F59E0B' if is_fallback else '#94A3B8'
        _ref_disp  = f' {ref_name_28}' if ref_name_28 else ''
        fair_mc_html = (
            f'{fair_mc_str}'
            f'<span style="font-size:0.62rem;color:{_src_color};font-weight:500;'
            f'margin-left:4px;letter-spacing:0;">@{peer_mult_28:.1f}x{_src_label}{_ref_disp}</span>'
        )
    else:
        fair_mc_html = fair_mc_str
    if pd.notna(gap_28):
        if gap_28 >= 30:    gap_color = '#34D399'; gap_str = f'▲ {gap_28:+.1f}%'
        elif gap_28 >= 0:   gap_color = '#34D399'; gap_str = f'{gap_28:+.1f}%'
        elif gap_28 > -30:  gap_color = '#F87171'; gap_str = f'{gap_28:+.1f}%'
        else:               gap_color = '#EF4444'; gap_str = f'▼ {gap_28:+.1f}%'
        gap_html = f'<span style="color:{gap_color};">{gap_str}</span>'
    else:
        gap_html = '-'
    peer_mult_str = f'{peer_mult_28:.1f}x' if pd.notna(peer_mult_28) else '-'
    _src_desc = (
        f"시장 전체 시총 1위({ref_name_28})의 멀티플로 폴백 - 업종 내 비교군 없음"
        if is_fallback else
        f"업종 내 시총 1위({ref_name_28})의 멀티플 적용 (본인 제외)"
    )
    fair_tip = f"기준 멀티플({peer_mult_str}) × 본 종목 2028E 영업이익. {_src_desc}."
    fair_px_tip = "적정주가 = 현재가 × (적정시총 / 현재시총). 발행주식수 변동 없다고 가정."
    gap_tip  = "(적정시총 / 현재시총 − 1) × 100. 양수=저평가, 음수=고평가."

    pills_html = (
        f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:4px;align-items:flex-start;">'
        + pill('PER (TTM)', per_str)
        + pill('Forward PER', fwd_per_str, hi=True)
        + pill('업종 PER', sec_per_str)
        + pill('PBR', pbr_str)
        + pill('PEG', peg_str)
        + pill('ROE', roe_str, hi=True)
        + pill("적정시총'28E", fair_mc_html, hi=True, tip=fair_tip)
        + pill("적정주가'28E", fair_px_str, hi=True, tip=fair_px_tip)
        + pill("괴리율'28E", gap_html, hi=True, tip=gap_tip)
        + f'</div>'
    )

    # ── 업종 상대 멀티플 (피어 비교) ─────────────────────────
    peer_status   = row.get('peer_status', '') or ''
    peer_n        = int(row.get('n_peers', 0) or 0)
    peer_med      = row.get('peer_pop_median', np.nan)
    peer_year     = row.get('peer_year_used', '') or ''
    fair_min      = row.get('fair_min', np.nan)
    fair_med      = row.get('fair_median', np.nan)
    fair_max      = row.get('fair_max', np.nan)
    upside        = row.get('upside_pct', np.nan)
    is_fb         = bool(row.get('is_fallback_year', False))

    if peer_status == 'no_sector':
        peer_html = (
            '<div class="qcd-peer-box" style="background:rgba(17,24,39,0.40);'
            'border:1px dashed #4A5568;border-radius:10px;padding:8px 12px;'
            'margin:8px 0;color:#94A3B8;font-size:0.78rem;">'
            '⚪ 업종 미분류로 피어 비교 불가'
            '</div>'
        )
    elif peer_status == 'no_peers':
        peer_html = (
            '<div class="qcd-peer-box" style="background:rgba(17,24,39,0.40);'
            'border:1px dashed #4A5568;border-radius:10px;padding:8px 12px;'
            'margin:8px 0;color:#94A3B8;font-size:0.78rem;">'
            '⚪ 동일 업종 내 비교 가능한 피어가 없습니다'
            '</div>'
        )
    elif peer_status == 'ok' and pd.notna(peer_med):
        # Upside 색상
        if pd.notna(upside) and upside >= 30:
            up_emoji, up_color = '🟢', '#10B981'
        elif pd.notna(upside) and upside <= -30:
            up_emoji, up_color = '🔴', '#EF4444'
        else:
            up_emoji, up_color = '⚪', '#94A3B8'

        # 적정시총 밴드 (format_number 재사용 — 억원 단위 → 조/억 환산)
        fmin_str = format_number(fair_min) if pd.notna(fair_min) else '-'
        fmed_str = format_number(fair_med) if pd.notna(fair_med) else '-'
        fmax_str = format_number(fair_max) if pd.notna(fair_max) else '-'
        up_str   = f'{upside:+.1f}%' if pd.notna(upside) else '-'

        # 폴백 연도 라벨
        fb_label = (
            f'<span style="color:#FBBF24;font-size:0.7rem;font-weight:600;margin-left:6px;'
            f'padding:1px 6px;border:1px solid rgba(251,191,36,0.4);border-radius:4px;">'
            f'⚠️ {peer_year} 기준 (28E 결측)</span>'
        ) if is_fb else (
            f'<span style="color:#94A3B8;font-size:0.7rem;margin-left:6px;">{peer_year} 기준</span>'
            if peer_year else ''
        )

        peer_html = (
            f'<div class="qcd-peer-box" style="background:rgba(17,24,39,0.45);'
            f'border:1px solid #4A5568;border-radius:10px;padding:10px 14px;'
            f'margin:8px 0;display:flex;flex-wrap:wrap;align-items:center;gap:18px;">'

            f'<div class="qcd-tech-label" style="display:flex;align-items:center;">'
            f'🎯 업종 상대 멀티플{fb_label}</div>'

            f'<div class="qcd-tech-item"><span class="k">피어 멀티플 (중앙값)</span>'
            f'<span class="v" style="color:#62EFFF;">{peer_med:.1f}'
            f' <span style="color:#94A3B8;font-size:0.74rem;font-weight:600;">(n={peer_n})</span>'
            f'</span></div>'

            f'<div class="qcd-tech-item"><span class="k">적정시총 밴드</span>'
            f'<span class="v" style="font-size:0.92rem;">'
            f'<span style="color:#94A3B8;">{fmin_str}</span>'
            f' <span style="color:#FFFFFF;">∼ {fmed_str} ∼</span>'
            f' <span style="color:#94A3B8;">{fmax_str}</span></span></div>'

            f'<div style="flex:1;"></div>'

            f'<div class="qcd-tech-item" style="text-align:right;">'
            f'<span class="k">Upside (vs 현재 시총)</span>'
            f'<span class="v" style="color:{up_color};font-size:1.1rem;">'
            f'{up_emoji} {up_str}</span></div>'

            f'</div>'
        )
    else:
        # peer_status가 비어있거나 다른 케이스 — 새 캐시 컬럼이 없는 옛 데이터일 수 있음
        peer_html = ''

    # ── 헤더 (rank, badge, name, code) ─────────────────────────
    header_html = (
        f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;'
        f'border-bottom:1px solid #4A5568;padding-bottom:10px;margin-bottom:12px;">'
        f'<span class="qcd-rank">#{rank}</span>'
        f'{badge}'
        f'<span class="qcd-name">{name}</span>'
        f'<span class="qcd-code">{code_str}</span>'
        f'<div style="flex:1;"></div>'
        f'<a href="{nurl}" target="_blank" class="qcd-naver-link">'
        f'<svg viewBox="0 0 24 24" width="13" height="13" stroke="currentColor" stroke-width="2" '
        f'fill="none" stroke-linecap="round" stroke-linejoin="round">'
        f'<path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>'
        f'<polyline points="15 3 21 3 21 9"></polyline>'
        f'<line x1="10" y1="14" x2="21" y2="3"></line></svg>상세보기</a>'
        f'</div>'
    )

    # ── 외인·기관 순매수 포맷 (주 → 억원) ────────────────────
    def _flow_fmt(shares_5d, shares_20d):
        if pd.isna(shares_5d) and pd.isna(shares_20d):
            return '<span style="color:#64748B;">-</span>', '#94A3B8'
        def _to_won_str(s):
            if pd.isna(s) or s == 0: return '0'
            won = s * (price if (price and price > 0) else 0)
            sign = '+' if s > 0 else '−'
            absw = abs(won)
            if absw >= 100_000_000:
                return f'{sign}{absw/100_000_000:.1f}억'
            if absw >= 10_000_000:
                return f'{sign}{absw/10_000_000:.0f}천만'
            return f'{sign}{abs(int(s)):,}주'
        s5 = _to_won_str(shares_5d) if pd.notna(shares_5d) else '-'
        s20 = _to_won_str(shares_20d) if pd.notna(shares_20d) else '-'
        # 5일 기준 색상
        if pd.notna(shares_5d):
            color = '#FCA5A5' if shares_5d > 0 else ('#93C5FD' if shares_5d < 0 else '#94A3B8')
        else:
            color = '#94A3B8'
        return (
            f'<span style="color:{color};font-family:\'JetBrains Mono\',monospace;'
            f'font-weight:700;font-size:0.95rem;">{s5}</span>'
            f'<span style="color:#94A3B8;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:0.78rem;margin-left:4px;">/ 20d {s20}</span>'
        ), color

    foreign_html, _ = _flow_fmt(foreign_5d, foreign_20d)
    inst_html, _    = _flow_fmt(inst_5d, inst_20d)

    # ── 통계 라인 ──────────────────────────────────────────────
    stats_html = (
        f'<div style="display:flex;gap:22px;flex-wrap:wrap;margin-bottom:6px;align-items:flex-end;">'
        f'<div><div class="qcd-stat-label">현재가</div>'
        f'<div class="qcd-stat-val">{format_price(price)}</div></div>'
        f'<div><div class="qcd-stat-label">거래량 / 거래대금</div>'
        f'<div><span class="qcd-stat-val" style="font-size:0.95rem;">{format_volume(volume)}</span>'
        f'<span style="color:#94A3B8;font-family:\'JetBrains Mono\',monospace;font-size:0.85rem;margin-left:4px;">'
        f'/ {format_turnover(volume, price)}</span>'
        f'<span style="font-size:0.82rem;margin-left:6px;">{vol_ratio_html}</span></div></div>'
        f'<div><div class="qcd-stat-label">시가총액</div>'
        f'<div class="qcd-stat-val" style="font-size:0.95rem;">{format_mcap(mcap)}</div></div>'
        f'<div><div class="qcd-stat-label">외인 순매수 (5d)</div>'
        f'<div>{foreign_html}</div></div>'
        f'<div><div class="qcd-stat-label">기관 순매수 (5d)</div>'
        f'<div>{inst_html}</div></div>'
        f'<div><div class="qcd-stat-label">데이터</div>'
        f'<div style="color:#62EFFF;font-family:\'JetBrains Mono\',monospace;font-size:0.9rem;">{avail}</div></div>'
        f'<div style="flex:1;"></div>'
        f'<div style="text-align:center;padding:6px 12px;background:rgba(17,24,39,0.5);'
        f'border:1px solid #4A5568;border-radius:8px;">'
        f'<div class="qcd-stat-label">가시성 P{row.get("미래가시성_순위", 5)}</div>'
        f'<div class="qcd-stat-val" style="font-size:1.05rem;">{row.get("미래가시성_성장률", 0):,.1f}%</div></div>'
        f'<div style="text-align:center;padding:6px 12px;background:rgba(17,24,39,0.5);'
        f'border:1px solid #4A5568;border-radius:8px;">'
        f'<div class="qcd-stat-label">종합점수</div>'
        f'<div class="rainbow-score" style="font-family:\'JetBrains Mono\',monospace;'
        f'font-weight:900;font-size:1.1rem;">{si2} {score:,.0f}</div></div>'
        f'</div>'
    )

    # ── 차트 박스 (성장률 라인차트) ────────────────────────────
    chart_html = (
        f'<div class="qcd-chart-box" style="margin:0;height:100%;display:flex;flex-direction:column;">'
        f'<div class="qcd-chart-legend" style="font-size:0.68rem;gap:12px;margin-bottom:2px;">'
        f'<span><span class="dot" style="background:#34D399;"></span>매출 성장률</span>'
        f'<span><span class="dot" style="background:#A78BFA;"></span>영업이익 성장률</span>'
        f'</div>'
        f'<div style="flex:1;display:flex;align-items:center;">{chart_svg}</div>'
        f'</div>'
    )

    # ── 좌(재무소스+보조지표) / 우(그래프) 2열 그리드 ──────────
    main_grid = (
        f'<div style="display:flex;gap:12px;margin:10px 0;flex-wrap:wrap;align-items:stretch;">'
        f'<div style="flex:1 1 320px;min-width:280px;display:flex;flex-direction:column;gap:10px;">'
        f'{evidence_html}'
        f'{tech_html}'
        f'</div>'
        f'<div style="flex:0 1 440px;min-width:300px;max-width:460px;">'
        f'{chart_html}'
        f'</div>'
        f'</div>'
    )

    st.markdown(
        f'<div class="quant-card-dark">'
        f'{header_html}'
        f'{stats_html}'
        f'{pills_html}'
        f'{peer_html}'
        f'{main_grid}'
        f'</div>',
        unsafe_allow_html=True,
    )

def main():
    if not check_password():
        return

    st.markdown("""
    <div class="hero-header">
        <div class="rainbow-title">
            <svg viewBox="0 0 24 24" width="28" height="28" stroke="currentColor" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline></svg>
            퀀트 터미널
        </div>
        <p>초고성장 종목 스크리너 · 컨센서스 알고리즘</p>
    </div>
    """, unsafe_allow_html=True)

    cache_info = get_cache_info()

    with st.sidebar:
        st.markdown("## ⚙️ 스크리닝 설정")
        st.markdown("---")

        st.markdown("### 💾 데이터 캐시")
        if cache_info:
            ts = cache_info['timestamp']
            ts_kst = ts.astimezone(KST) if ts.tzinfo else ts.replace(tzinfo=KST)
            ts_str = ts_kst.strftime('%Y-%m-%d %H:%M KST')
            cnt = cache_info['total_stocks']
            st.markdown(f'<div class="cache-info">✅ 캐시 존재<br>{ts_str} 수집<br>{cnt}개 종목</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="cache-none">⚠️ 캐시 없음 — 데이터 수집 필요</div>', unsafe_allow_html=True)

        st.markdown("### 📊 시장 선택")
        markets = st.multiselect("분석 대상 시장", ["KOSPI", "KOSDAQ"], default=["KOSPI", "KOSDAQ"])

        st.markdown("### 📈 성장률 기준")
        rev_thresh = st.slider("매출액 성장률 (% 이상)", 0, 500, 100, 10)
        op_thresh = st.slider("영업이익 성장률 (% 이상)", 0, 500, 100, 10)

        st.markdown("### 💎 영업이익 규모 (2026년 이후)")
        op_size_label = st.radio(
            "영업이익 규모",
            ["300억 이하", "500억~1000억", "1000억 이상"],
            index=2,
            horizontal=True,
            label_visibility="collapsed",
            help="2026·2027·2028년 예상 영업이익 중 최대값(단위: 억) 기준으로 필터링합니다.",
        )

        st.markdown("### 📊 거래량 필터")
        vol_opts = {"제한 없음": 0, "1만 이상": 10000, "5만 이상": 50000, "10만 이상": 100000,
                    "50만 이상": 500000, "100만 이상": 1000000}
        vol_sel = st.selectbox("최소 거래량", list(vol_opts.keys()), index=5)
        min_vol = vol_opts[vol_sel]

        st.markdown("### 🛡️ 엄격한 재무 필터")
        req_min_rev_500 = st.checkbox("매출액 500억 이상 (매년)", value=True, help="어느 한 연도라도 매출액이 500억 미만이면 제외합니다.")
        req_op_profit = st.checkbox("영업이익 흑자 필수", value=True, help="최근 3개년 및 컨센서스 중 한 번이라도 영업손실(적자)이면 제외합니다.")
        drop_huge_loss = st.checkbox("매출 초과 적자기업 제외", value=True, help="영업손실 규모가 매출액보다 큰 경우(바이오/성장주 특화) 무조건 제외합니다.")

        st.markdown("### ⚡ 성능 설정")
        max_workers = st.slider("병렬 워커 수", 5, 100, 30, 5,
                                help="높을수록 빠르지만 timeout/IP차단 위험 증가. 30~40 권장")

        st.markdown("---")

        force_fresh = st.checkbox(
            "처음부터 전체 재수집",
            value=False,
            help="체크 해제 시: 기존 캐시 유지하고 빠진 종목만 추가 수집(이어서). "
                 "체크 시: 캐시 무시하고 전체 새로 수집."
        )
        crawl_btn = st.button(
            "🔄 데이터 수집 (이어서)" if not force_fresh else "🔄 전체 재수집",
            width="stretch",
            help="중간에 멈춰도 500종목마다 자동 저장되니 다시 누르면 이어집니다.",
        )

        st.markdown("---")
        st.markdown("""
        <div style="color:#64748b; font-size:0.8rem; text-align:center; line-height:1.6;">
            <b>💡 사용법</b><br>
            1) <b>데이터 수집</b> 버튼으로 캐시 생성<br>
            2) 거래량/성장률 <b>즉시 필터링</b><br>
            필터 변경 시 재크롤링 불필요!
        </div>
        """, unsafe_allow_html=True)

    # ---- 메인 영역 ----

    if crawl_btn:
        progress_bar = st.progress(0)
        status_text = st.empty()
        start = time.time()
        crawl_all_data(progress_bar, status_text, markets, max_workers,
                       resume=(not force_fresh))
        elapsed = time.time() - start
        st.session_state['elapsed'] = elapsed
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
        st.rerun()

    cache = load_cache()
    if cache is not None:
        all_df = cache['data']
        cache_ts = cache['timestamp']
        elapsed = st.session_state.get('elapsed', 0)

        # ── 업종 매핑은 필터 적용 전 all_df에 먼저 적용 ───────────
        # (멀티플 계산이 전체 모집단을 기준으로 이루어져야 사용자 필터에
        #  영향받지 않고 안정적으로 산출됨)
        if '업종' not in all_df.columns or all_df['업종'].isna().all():
            sector_map = _load_sector_map()
            all_df['업종'] = all_df['종목코드'].astype(str).str.zfill(6).map(sector_map).fillna('기타')
        else:
            all_df['업종'] = all_df['업종'].fillna('기타')

        # ── 업종별 2028E 영업이익 멀티플(시총/2028E OP) - 전체 모집단 기준 ──
        # 규칙: 업종 내 본인 제외, 시총 최대 종목(2028E 영업이익 보유)의 멀티플을 적용
        #       업종 내 본인 외 valid 종목이 없으면 시장 전체 시총 최대 종목으로 폴백
        if '시가총액' in all_df.columns and '영업이익_2028' in all_df.columns:
            op28_all = pd.to_numeric(all_df['영업이익_2028'], errors='coerce')
            mc_all   = pd.to_numeric(all_df['시가총액'],   errors='coerce')
            all_df['멀티플_2028E'] = np.where((op28_all > 0) & (mc_all > 0), mc_all / op28_all, np.nan)

            # 업종별 상위 2개 (멀티플 valid + 시총 내림차순) — 본인이 1위인 경우 2위 사용
            sector_top2 = {}
            for sec, grp in all_df.groupby('업종'):
                valid = grp[grp['멀티플_2028E'].notna()].sort_values('시가총액', ascending=False)
                sector_top2[sec] = valid.head(2)

            # 시장 전체 상위 2개 (폴백용)
            mkt_valid = all_df[all_df['멀티플_2028E'].notna()].sort_values('시가총액', ascending=False).head(2)

            ref_mult = []
            ref_name = []
            is_mkt_fb = []
            for idx, sec in all_df['업종'].items():
                # 1단계: 업종 내 (본인 제외) 시총 최대
                top2 = sector_top2.get(sec)
                picked = None
                if top2 is not None and not top2.empty:
                    if top2.iloc[0].name == idx:  # 본인이 1위
                        if len(top2) >= 2:
                            picked = top2.iloc[1]
                    else:
                        picked = top2.iloc[0]
                fb = False
                # 2단계: 업종에서 못 찾으면 시장 전체 시총 최대 (본인 제외)
                if picked is None and not mkt_valid.empty:
                    if mkt_valid.iloc[0].name == idx:
                        if len(mkt_valid) >= 2:
                            picked = mkt_valid.iloc[1]; fb = True
                    else:
                        picked = mkt_valid.iloc[0]; fb = True
                if picked is not None:
                    ref_mult.append(picked['멀티플_2028E'])
                    ref_name.append(str(picked.get('종목명', '')))
                    is_mkt_fb.append(fb)
                else:
                    ref_mult.append(np.nan); ref_name.append(''); is_mkt_fb.append(False)

            all_df['업종_2028E_멀티플_중앙값']  = ref_mult   # 컬럼명은 유지(다운스트림 호환)
            all_df['멀티플기준_종목명_2028E']  = ref_name
            all_df['멀티플_시장폴백_2028E']    = is_mkt_fb

            all_df['적정시총_2028E'] = all_df['업종_2028E_멀티플_중앙값'] * op28_all
            all_df['괴리율_2028E'] = np.where(
                pd.notna(all_df['적정시총_2028E']) & (mc_all > 0),
                (all_df['적정시총_2028E'] / mc_all - 1) * 100,
                np.nan
            )
            # 적정주가 = 현재가 × (적정시총 / 현재시총)
            price_all = pd.to_numeric(all_df.get('현재가', np.nan), errors='coerce')
            all_df['적정주가_2028E'] = np.where(
                pd.notna(all_df['적정시총_2028E']) & (mc_all > 0) & (price_all > 0),
                price_all * (all_df['적정시총_2028E'] / mc_all),
                np.nan
            )
            # 본 종목의 영업이익_2028 자체가 없으면 적정시총 산출 불가
            no_op28 = ~(op28_all > 0)
            all_df.loc[no_op28, ['적정시총_2028E', '괴리율_2028E', '적정주가_2028E']] = np.nan
        else:
            for _c in ['멀티플_2028E', '업종_2028E_멀티플_중앙값',
                       '적정시총_2028E', '괴리율_2028E', '적정주가_2028E']:
                all_df[_c] = np.nan
            all_df['멀티플기준_종목명_2028E'] = ''
            all_df['멀티플_시장폴백_2028E'] = False

        df = apply_filters(all_df.copy(), rev_thresh, op_thresh, min_vol, markets, req_min_rev_500, req_op_profit, drop_huge_loss, op_size_label)

        # 업종평균 PER 매핑 (df 업종은 all_df에서 이미 채워진 상태로 전파됨)
        sector_per_map = get_sector_per_map()
        df['업종평균PER'] = df['업종'].map(sector_per_map)

        # 업종 상대 멀티플: universe = 전체 캐시(all_df) 기준으로 재계산
        # (apply_filters 내부 호출은 filtered df를 universe로 쓰므로 피어가 좁다)
        try:
            df = apply_peer_multiples_with_universe(df, all_df)
        except Exception:
            pass

        # 메트릭 카드
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-label">발굴 종목</div><div class="metric-value">{len(df)}</div><div class="metric-label">/ {len(all_df)}개 중</div></div>', unsafe_allow_html=True)
        with col2:
            kp = len(df[df['시장']=='KOSPI']) if '시장' in df.columns else 0
            kd = len(df[df['시장']=='KOSDAQ']) if '시장' in df.columns else 0
            st.markdown(f'<div class="metric-card"><div class="metric-label">KOSPI / KOSDAQ</div><div class="metric-value">{kp} / {kd}</div><div class="metric-label">종목</div></div>', unsafe_allow_html=True)
        with col3:
            ao = df['영업이익_최대성장률'].mean() if '영업이익_최대성장률' in df.columns and len(df) > 0 else 0
            st.markdown(f'<div class="metric-card"><div class="metric-label">평균 영업이익 성장률</div><div class="metric-value">{ao:,.0f}%</div><div class="metric-label">최대 기준</div></div>', unsafe_allow_html=True)
        with col4:
            ts_kst = cache_ts.astimezone(KST) if cache_ts.tzinfo else cache_ts.replace(tzinfo=KST)
            ts_str = ts_kst.strftime('%m/%d %H:%M KST')
            st.markdown(f'<div class="metric-card"><div class="metric-label">캐시 데이터</div><div class="metric-value" style="font-size:1.3rem;">{ts_str}</div><div class="metric-label">수집 시점</div></div>', unsafe_allow_html=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        if df.empty:
            st.warning("⚠️ 조건에 부합하는 종목이 없습니다. 사이드바에서 기준을 완화하거나, '개별종목확인' 탭에서 종목명으로 직접 검색하세요.")

        # 탭 (5개)
        tab_cards, tab_search, tab_sector, tab_table, tab_hist = st.tabs([
            "📋 종목 카드 뷰", "🔍 개별종목확인",
            "🏢 업종별 테마순위", "📊 데이터 테이블", "📅 누적 기록",
        ])

        with tab_cards:
            if df.empty:
                st.info("필터 조건을 완화하거나 옆 탭의 '개별종목확인'을 사용하세요.")
            else:
                st.markdown(f'<div style="color:#FFFFFF; font-size:0.9rem; font-family:\'JetBrains Mono\', monospace; margin-bottom:10px;">> 스크리너 결과: {len(df)}개 발굴</div>', unsafe_allow_html=True)
            scol1, scol2 = st.columns([2, 1])
            with scol1:
                sort_options = {
                    "🌟 미래 가시성 핵심성장 (1~3순위)": "가시성기준_정렬점수",
                    "💎 영업이익 규모 (2026+)": "영업이익_26이후_최대",
                    "📊 매출+영업이익 합산점수": "종합성장점수",
                    "🎯 2028E 괴리율 (저평가 우선)": "괴리율_2028E",
                    "💰 매출 1년최대성장률 (단기)": "매출액_최대성장률",
                    "📈 영업이익 1년최대성장률 (단기)": "영업이익_최대성장률",
                    "🔥 거래량배수 (20일평균 대비)": "거래량배수",
                    "🔥 거래량순": "Recent_Volume",
                    "💹 Forward PER (낮을수록)": "Forward_PER",
                    "⭐ PEG (낮을수록)": "PEG",
                    "🏢 시가총액순": "시가총액",
                    "💵 현재가순": "현재가",
                }
                sort_label = st.selectbox("정렬 기준", list(sort_options.keys()), index=0, label_visibility="collapsed")
                sort_col = sort_options[sort_label]
            with scol2:
                # Forward PER, PEG는 낮을수록 좋으므로 기본 오름차순
                asc_default = sort_col in ('Forward_PER', 'PEG')
                sort_order = st.selectbox("순서", ["오름차순", "내림차순"] if asc_default else ["내림차순", "오름차순"], index=0, label_visibility="collapsed")
            df_s = df.sort_values(sort_col, ascending=(sort_order=="오름차순"), na_position='last').reset_index(drop=True)

            page_size = 20
            total_pages = max(1, (len(df_s)-1)//page_size+1)
            if 'page' not in st.session_state: st.session_state['page'] = 1
            if st.session_state['page'] > total_pages: st.session_state['page'] = 1

            pc1, pc2, pc3 = st.columns([1,2,1])
            with pc1:
                if st.button("◀ 이전", disabled=st.session_state['page']<=1): st.session_state['page']-=1; st.rerun()
            with pc2:
                st.markdown(f"<div style='text-align:center;color:#FFFFFF;padding:8px;'>페이지 {st.session_state['page']} / {total_pages}</div>", unsafe_allow_html=True)
            with pc3:
                if st.button("다음 ▶", disabled=st.session_state['page']>=total_pages): st.session_state['page']+=1; st.rerun()

            si = (st.session_state['page']-1)*page_size
            for rank, (_, row) in enumerate(df_s.iloc[si:si+page_size].iterrows(), start=si+1):
                render_stock_card(row, rank)

        # ────────────────────────────────────────────────────────
        # 개별종목확인 탭 — 필터 무시하고 종목명/코드로 직접 검색
        # ────────────────────────────────────────────────────────
        with tab_search:
            st.markdown(
                "<h3 style='color:#FFFFFF;'>🔍 개별종목 확인</h3>"
                "<div style='color:#A0AEC0;font-size:0.85rem;margin-bottom:12px;'>"
                "필터에 걸리지 않은 종목도 종목명(한글)이나 종목코드로 검색해 카드 형태로 확인할 수 있습니다."
                "</div>",
                unsafe_allow_html=True,
            )

            search_q = st.text_input(
                "종목명 또는 종목코드",
                value=st.session_state.get('search_q', ''),
                placeholder="예: 삼성전자, 005930, HBM, 2차전지",
                key="search_q",
            )

            q = (search_q or '').strip()
            if not q:
                st.markdown(
                    "<div style='color:#A0AEC0;font-size:0.9rem;padding:30px 0;'>"
                    "↑ 종목명 또는 6자리 코드를 입력하세요. 부분 일치(예: '삼성' → 삼성전자/삼성SDI/...) 가능."
                    "</div>",
                    unsafe_allow_html=True,
                )
            else:
                base = all_df.copy()
                # 코드 정규화
                base['__code'] = base['종목코드'].astype(str).str.zfill(6)
                # 검색 조건: 종목명 contains (대소문자 무시) OR 코드 contains
                name_mask = base['종목명'].astype(str).str.contains(q, case=False, na=False)
                if q.isdigit():
                    code_mask = base['__code'].str.contains(q.zfill(min(6, len(q))) if len(q) >= 3 else q, na=False)
                else:
                    code_mask = pd.Series(False, index=base.index)
                hits = base[name_mask | code_mask].drop(columns='__code', errors='ignore')

                if hits.empty:
                    st.warning(f"⚠️ '{q}'에 해당하는 종목이 없습니다. (캐시: {len(all_df):,}개 종목)")
                else:
                    # 카드 렌더에 필요한 파생 컬럼 + 업종/업종평균PER 매핑
                    hits = compute_card_fields(hits)
                    if '업종' not in hits.columns or hits['업종'].isna().all():
                        sector_map_local = _load_sector_map()
                        hits['업종'] = hits['종목코드'].astype(str).str.zfill(6).map(sector_map_local).fillna('기타')
                    else:
                        hits['업종'] = hits['업종'].fillna('기타')
                    hits['업종평균PER'] = hits['업종'].map(get_sector_per_map())

                    # 거래량배수 폴백 (기존 캐시에 없을 수 있음)
                    if '거래량배수' not in hits.columns:
                        hits['거래량배수'] = np.nan

                    # 업종 상대 멀티플: universe = 전체 캐시(all_df)
                    try:
                        hits = apply_peer_multiples_with_universe(hits, all_df)
                    except Exception:
                        pass

                    hits = hits.sort_values('가시성기준_정렬점수', ascending=False).reset_index(drop=True)

                    st.markdown(
                        f"<div style='color:#62EFFF; font-size:0.9rem; "
                        f"font-family:\"JetBrains Mono\", monospace; margin: 8px 0 14px 0;'>"
                        f"&gt; \"{q}\" 검색 결과: <b>{len(hits)}</b>개 종목</div>",
                        unsafe_allow_html=True,
                    )

                    # 너무 많으면 상위 N개만 (안전장치)
                    MAX_CARDS = 30
                    show_df = hits.head(MAX_CARDS)
                    if len(hits) > MAX_CARDS:
                        st.info(f"검색 결과가 {len(hits)}개로 많아 상위 {MAX_CARDS}개만 카드로 표시합니다. 더 정확한 검색어를 입력하세요.")
                    for rank, (_, row) in enumerate(show_df.iterrows(), start=1):
                        render_stock_card(row, rank)

        with tab_sector:
            st.markdown("<h3 style='color:#FFFFFF;'>🏢 업종별 수익 테마 순위</h3>", unsafe_allow_html=True)
            if df.empty:
                st.info("필터에 걸린 종목이 없어 업종 순위를 계산할 수 없습니다.")
            elif '업종' in df.columns:
                ind_df = df.groupby('업종')['영업이익_최대성장률'].mean().reset_index()
                ind_df.rename(columns={'영업이익_최대성장률': '평균_성장률'}, inplace=True)
                ind_df = ind_df.sort_values('평균_성장률', ascending=False).reset_index(drop=True)

                for i, irow in ind_df.iterrows():
                    ind_name = irow['업종']
                    avg_score = irow['평균_성장률']
                    comp_df = df[df['업종'] == ind_name].sort_values('영업이익_최대성장률', ascending=False)
                    with st.expander(f"🏅 {i+1}위: {ind_name} (평균 영업이익 성장률: {avg_score:,.1f}% / {len(comp_df)}종목)"):
                        st.markdown("<div style='margin-bottom:8px;font-size:0.9rem;color:#A0AEC0;'>상위 10개 종목만 표시됩니다.</div>", unsafe_allow_html=True)
                        for rank, (_, row) in enumerate(comp_df.head(10).iterrows(), start=1):
                            render_stock_card(row, rank)
            else:
                st.warning("데이터에 '업종' 정보가 포함되어 있지 않습니다.")

        with tab_table:
            st.markdown("### 📊 전체 데이터 테이블")
            dc1, dc2, dc3 = st.columns(3)
            with dc1:
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📥 CSV 다운로드", data=csv, file_name=f"high_growth_{now_kst().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)
            with dc2:
                try:
                    buf = io.BytesIO()
                    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
                    df_clean = df.copy()
                    for col in df_clean.columns:
                        if df_clean[col].dtype == 'object':
                            df_clean[col] = df_clean[col].apply(lambda x: ILLEGAL_CHARACTERS_RE.sub('', str(x)) if x is not None else x)

                    with pd.ExcelWriter(buf, engine='openpyxl') as w:
                        df_clean.to_excel(w, index=False, sheet_name='초고성장종목')

                    st.download_button(
                        label="📥 Excel 다운로드",
                        data=buf.getvalue(),
                        file_name=f"high_growth_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"엑셀 오류: {str(e)}", icon="🚨")
            with dc3:
                # 누적 기록 엑셀 다운로드
                history_data = build_history_excel()
                if history_data:
                    st.download_button(
                        label="📥 누적기록 Excel",
                        data=history_data,
                        file_name=f"accumulation_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.button("📥 누적기록 없음", disabled=True, use_container_width=True)

            show_cols = ['종목명','종목코드','시장','현재가','Recent_Volume','거래량배수','시가총액','업종',
                'PER','Forward_PER','PEG','PBR','ROE','업종평균PER',
                '업종_2028E_멀티플_중앙값','멀티플기준_종목명_2028E','적정시총_2028E','적정주가_2028E','괴리율_2028E','데이터_가용성',
                '매출액_성장률_2025','매출액_성장률_2026','매출액_성장률_2027','매출액_성장률_2028','매출액_최대성장률',
                '영업이익_성장률_2025','영업이익_성장률_2026','영업이익_성장률_2027','영업이익_성장률_2028','영업이익_최대성장률','종합성장점수']
            ac = [c for c in show_cols if c in df.columns]
            st.dataframe(df[ac], use_container_width=True, height=600, column_config={
                "종목명": st.column_config.TextColumn("종목명", width="medium"),
                "종목코드": st.column_config.TextColumn("코드", width="small"),
                "현재가": st.column_config.NumberColumn("현재가", format="%d원"),
                "Recent_Volume": st.column_config.NumberColumn("거래량", format="%d"),
                "거래량배수": st.column_config.NumberColumn("거래량배수(20d)", format="%.1fx"),
                "시가총액": st.column_config.NumberColumn("시총(억)", format="%d"),
                "PER": st.column_config.NumberColumn("PER(TTM)", format="%.1f"),
                "Forward_PER": st.column_config.NumberColumn("Fwd PER", format="%.1f"),
                "PEG": st.column_config.NumberColumn("PEG", format="%.2f"),
                "PBR": st.column_config.NumberColumn("PBR", format="%.2f"),
                "ROE": st.column_config.NumberColumn("ROE", format="%.1f%%"),
                "업종평균PER": st.column_config.NumberColumn("업종PER", format="%.1f"),
                "업종_2028E_멀티플_중앙값": st.column_config.NumberColumn("기준멀티플'28E", format="%.1fx", help="업종 내 시총 1위 종목(본인 제외)의 시총/2028E 영업이익. 업종에 비교군 없으면 시장 시총 1위로 폴백"),
                "멀티플기준_종목명_2028E": st.column_config.TextColumn("기준종목", help="멀티플을 가져온 기준 종목명"),
                "적정시총_2028E": st.column_config.NumberColumn("적정시총'28E(억)", format="%.0f", help="업종 멀티플 중앙값 × 본 종목 2028E 영업이익"),
                "적정주가_2028E": st.column_config.NumberColumn("적정주가'28E(원)", format="%.0f", help="현재가 × (적정시총/현재시총), 발행주식수 동일 가정"),
                "괴리율_2028E": st.column_config.NumberColumn("괴리율'28E", format="%+.1f%%", help="(적정시총/현재시총-1)×100, 양수=저평가"),
                "매출액_최대성장률": st.column_config.NumberColumn("매출MAX%", format="%.1f%%"),
                "영업이익_최대성장률": st.column_config.NumberColumn("영업이익MAX%", format="%.1f%%"),
                "종합성장점수": st.column_config.NumberColumn("종합점수", format="%.0f"),
            })

        with tab_hist:
            st.markdown("### 📅 누적 기록 (거래량 100만 이상)")
            history = load_history()
            if not history:
                st.info("아직 누적 기록이 없습니다. 데이터 수집을 먼저 실행해주세요.")
            else:
                for cat_name in ['미래가시성핵심성장', '매출+영업이익환산점수', '매출1년최대성장률', '영업이익1년최대성장률']:
                    cat_data = history.get(cat_name, {})
                    dates = sorted(cat_data.keys())
                    total_unique = len(set(s for d in dates for s in cat_data[d]))
                    with st.expander(f"📌 {cat_name} ({len(dates)}일 기록 / 누적 {total_unique}종목)"):
                        if not dates:
                            st.write("기록 없음")
                        else:
                            max_len = max(len(cat_data[d]) for d in dates)
                            data = {}
                            for d in dates:
                                stocks = cat_data[d]
                                padded = stocks + [''] * (max_len - len(stocks))
                                data[d] = padded
                            st.dataframe(pd.DataFrame(data), use_container_width=True, height=400)

                # 누적기록 엑셀 다운로드
                history_data = build_history_excel()
                if history_data:
                    st.download_button(
                        label="📥 누적기록 전체 Excel 다운로드",
                        data=history_data,
                        file_name=f"accumulation_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
    else:
        st.markdown("""
        <div style="text-align:center; padding:60px 20px;">
            <div style="font-size:4rem; margin-bottom:16px;">🔍</div>
            <h2 style="color:#818cf8; font-weight:700; margin-bottom:12px;">종목 발굴을 시작하세요</h2>
            <p style="color:#94a3b8; font-size:1.05rem; max-width:500px; margin:0 auto; line-height:1.8;">
                왼쪽 사이드바에서 <b style="color:#a5b4fc;">🔄 데이터 수집</b> 버튼을 눌러<br>
                전종목 컨센서스 데이터를 수집하세요. (~5분)<br><br>
                수집 후에는 거래량/성장률 필터를 <b style="color:#34d399;">즉시</b> 변경할 수 있습니다!
            </p>
            <div style="margin-top:32px; display:flex; justify-content:center; gap:20px; flex-wrap:wrap;">
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">1️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.9rem; margin-top:4px;">데이터 수집</div>
                    <div style="color:#64748b; font-size:0.8rem;">~5분 (1회만)</div>
                </div>
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">2️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.9rem; margin-top:4px;">즉시 필터링</div>
                    <div style="color:#64748b; font-size:0.8rem;">거래량/성장률 자유 변경</div>
                </div>
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">3️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.9rem; margin-top:4px;">결과 확인</div>
                    <div style="color:#64748b; font-size:0.8rem;">카드 뷰 + Excel</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


if __name__ == '__main__':
    main()
