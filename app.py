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

    # apply_filters와 동일한 점수 계산을 위해 간단히 수행
    def calc_scores(row):
        s = 0
        rm = row.get('매출액_최대성장률', np.nan)
        if pd.notna(rm): s += min(rm, 2000)
        om = row.get('영업이익_최대성장률', np.nan)
        if pd.notna(om): s += min(om, 2000)
        con = sum(1 for y in [2025,2026,2027]
                  if (pd.notna(row.get(f'매출액_성장률_{y}')) and row.get(f'매출액_성장률_{y}') > 30)
                  or (pd.notna(row.get(f'영업이익_성장률_{y}')) and row.get(f'영업이익_성장률_{y}') > 30))
        s += con * 50
        return s

    def calc_visibility(row):
        rv25 = row.get('매출액_2025', np.nan)
        rv26 = row.get('매출액_2026', np.nan)
        rv27 = row.get('매출액_2027', np.nan)
        rv24 = row.get('매출액_2024', np.nan)
        pr = 4
        mt = 0
        if pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 1; mt = ((rv27 / rv25) ** (1/2) - 1) * 100
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 2; mt = ((rv26 / rv25) - 1) * 100
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 3; mt = ((rv25 / rv24) - 1) * 100
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

        # 미래가시성핵심성장: P1~P3 등급 (27E or 26E or 25E 데이터 있는 종목)
        if pr <= 3:
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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'ko-KR,ko;q=0.9',
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
    """FnGuide에서 2026E, 2027E 컨센서스 데이터를 보조로 가져온다."""
    try:
        session = get_session()
        url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}'
        resp = session.get(url, timeout=7)
        resp.encoding = 'utf-8'
        if resp.status_code != 200: return {}
        soup = BeautifulSoup(resp.text, 'lxml')

        page_name = ''
        for tag in [soup.find('h1', class_='giName'), soup.find('title')]:
            if tag:
                page_name = tag.get_text(strip=True)
                break
        if stock_name and page_name and stock_name not in page_name and page_name not in stock_name:
            return {}

        tables = soup.find_all('table')
        target_tbl = None
        for tbl in tables:
            rows = tbl.find_all('tr')
            if len(rows) < 3: continue
            r0 = [c.get_text(strip=True) for c in rows[0].find_all(['th','td'])]
            combined = ' '.join(r0)
            if 'Annual' in combined and 'Quarter' not in combined:
                target_tbl = tbl
                break
        if not target_tbl: return {}
        rows = target_tbl.find_all('tr')
        hcells = rows[1].find_all(['th','td'])
        col_years = []
        for c in hcells:
            txt = c.get_text(strip=True)
            m = re.search(r'(\d{4})[./]', txt)
            col_years.append(int(m.group(1)) if m else None)
        dm = {}
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

        # FnGuide에서 2026E, 2027E 보충 데이터 가져오기
        try:
            fg = scrape_fnguide_supplement(stock_code, stock_name)
            for mn in ['매출액', '영업이익']:
                if mn in fg:
                    for yr in [2025, 2026, 2027]:
                        if (yr not in dm.get(mn, {}) or pd.isna(dm.get(mn, {}).get(yr))) and yr in fg[mn] and pd.notna(fg[mn][yr]):
                            if mn not in dm: dm[mn] = {}
                            dm[mn][yr] = fg[mn][yr]
        except:
            pass

        if not dm: return None

        ty = [2025, 2026, 2027]; by = [2024, 2025, 2026]
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

        return result
    except Exception as e:
        return None


# ============================================================
# 크롤링 (데이터 수집만 - 캐시 저장)
# ============================================================
def crawl_all_data(progress_bar, status_text, markets, max_workers):
    """전종목 컨센서스 데이터를 크롤링하고 캐시에 저장한다."""
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

    total = len(stock_df)
    status_text.markdown(f"⏳ **2단계:** {total}개 종목 컨센서스 분석 중... ({max_workers}워커)")
    results, counter, lock = [], [0], threading.Lock()

    def process(rd):
        c = scrape_naver_consensus(rd['종목코드'], rd['종목명'])
        if c:
            c['시장'] = rd['시장']; c['현재가'] = rd['현재가']
            c['시가총액'] = rd['시가총액']; c['Recent_Volume'] = rd['Recent_Volume']
            return ('ok', c)
        return ('no', None)

    rows = [r.to_dict() for _, r in stock_df.iterrows()]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(process, r): i for i, r in enumerate(rows)}
        for f in as_completed(futs):
            with lock: counter[0] += 1; cnt = counter[0]
            try:
                s, d = f.result()
                if s == 'ok': results.append(d)
            except: pass
            if cnt % 100 == 0 or cnt == total:
                progress_bar.progress(min(0.10 + (cnt/total)*0.85, 0.95))
                status_text.markdown(f"⏳ **2단계:** {cnt}/{total} ({cnt/total*100:.0f}%) | 수집: {len(results)}개")

    progress_bar.progress(1.0)
    if not results: return pd.DataFrame()

    df = pd.DataFrame(results)
    meta = {'markets': markets, 'total_analyzed': total, 'data_count': len(results)}
    save_cache(df, meta)

    # 누적 기록 저장
    save_history(df)

    status_text.markdown(f"✅ **완료!** {len(results)}개 종목 데이터 수집 → 캐시 저장됨")
    return df


# ============================================================
# 필터링 (캐시 데이터에서 즉시 필터링)
# ============================================================
def apply_filters(df, rev_thresh, op_thresh, min_vol, markets, req_min_rev_500=True, req_op_profit=True, drop_huge_loss=True):
    if df.empty: return df
    if markets: df = df[df['시장'].isin(markets)]
    if min_vol > 0: df = df[df['Recent_Volume'] >= min_vol]

    def strict_financial_check(row):
        yrs = [2023, 2024, 2025, 2026, 2027]
        for y in yrs:
            rv = row.get(f'매출액_{y}', np.nan)
            ov = row.get(f'영업이익_{y}', np.nan)
            if req_min_rev_500 and pd.notna(rv) and rv < 500: return False
            if req_op_profit and pd.notna(ov) and ov < 0: return False
            if drop_huge_loss and pd.notna(ov) and pd.notna(rv) and ov < 0 and abs(ov) > rv: return False
        return True

    df = df[df.apply(strict_financial_check, axis=1)].copy()
    def meets(row):
        rv = [row.get(f'매출액_성장률_{y}', np.nan) for y in [2025,2026,2027]]
        ov = [row.get(f'영업이익_성장률_{y}', np.nan) for y in [2025,2026,2027]]
        rv = [x for x in rv if pd.notna(x)]; ov = [x for x in ov if pd.notna(x)]
        return (any(x >= rev_thresh for x in rv)) or (any(x >= op_thresh for x in ov))
    df = df[df.apply(meets, axis=1)].copy()
    scores, priority_ranks, priority_scores, metric_pcts = [], [], [], []
    for _, row in df.iterrows():
        s = 0
        rm = row.get('매출액_최대성장률', np.nan)
        if pd.notna(rm): s += min(rm, 2000)
        om = row.get('영업이익_최대성장률', np.nan)
        if pd.notna(om): s += min(om, 2000)
        con = sum(1 for y in [2025,2026,2027]
                  if (pd.notna(row.get(f'매출액_성장률_{y}')) and row.get(f'매출액_성장률_{y}') > 30)
                  or (pd.notna(row.get(f'영업이익_성장률_{y}')) and row.get(f'영업이익_성장률_{y}') > 30))
        s += con * 50; scores.append(round(s, 2))

        rv24 = row.get('매출액_2024', np.nan)
        rv25 = row.get('매출액_2025', np.nan)
        rv26 = row.get('매출액_2026', np.nan)
        rv27 = row.get('매출액_2027', np.nan)

        pr = 4
        mt = 0
        if pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 1; mt = ((rv27 / rv25) ** (1/2) - 1) * 100
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 2; mt = ((rv26 / rv25) - 1) * 100
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 3; mt = ((rv25 / rv24) - 1) * 100

        priority_ranks.append(pr)
        metric_pcts.append(mt)
        priority_scores.append((4 - pr) * 100_000_000 + mt)

    df['종합성장점수'] = scores
    df['미래가시성_순위'] = priority_ranks
    df['미래가시성_성장률'] = metric_pcts
    df['가시성기준_정렬점수'] = priority_scores

    return df.sort_values('가시성기준_정렬점수', ascending=False).reset_index(drop=True)


# ============================================================
# 포맷 함수
# ============================================================
def format_number(v):
    if pd.isna(v): return "-"
    if abs(v) >= 1_000_000: return f"{v/10000:.0f}조"
    return f"{v:,.0f}" if abs(v) >= 10000 else f"{v:,.1f}"

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
    code = row.get('종목코드',''); name = row.get('종목명',''); market = row.get('시장','')
    price = row.get('현재가',0); volume = row.get('Recent_Volume',0)
    mcap = row.get('시가총액',0); score = row.get('종합성장점수',0); avail = row.get('데이터_가용성','-')
    rg25,rg26,rg27 = row.get('매출액_성장률_2025',np.nan),row.get('매출액_성장률_2026',np.nan),row.get('매출액_성장률_2027',np.nan)
    og25,og26,og27 = row.get('영업이익_성장률_2025',np.nan),row.get('영업이익_성장률_2026',np.nan),row.get('영업이익_성장률_2027',np.nan)

    per_val = row.get('PER', np.nan)
    pbr_val = row.get('PBR', np.nan)
    roe_val = row.get('ROE', np.nan)
    sector_per = row.get('업종평균PER', np.nan)

    # 네이버 증권 링크: 종목코드 6자리 확인 후 올바른 URL 생성
    code_str = str(code).zfill(6)
    nurl = f"https://finance.naver.com/item/main.naver?code={code_str}"

    badge = f'<span class="badge-kospi">KOSPI</span>' if market=='KOSPI' else f'<span class="badge-kosdaq">KOSDAQ</span>'
    sc = "#2EAA7B" if score>=1000 else "#4A90E2" if score>=500 else "#8B949E"
    si2 = "⭐" if score>=500 else "▪"

    def fv(v):
        if pd.isna(v): return '-'
        return f'{v:,.0f}'
    def fv_color(v):
        if pd.isna(v): return '#CED4DA'
        return '#FF6B6B' if v > 0 else '#4A90E2'

    rv23,rv24,rv25,rv26,rv27 = row.get('매출액_2023',np.nan),row.get('매출액_2024',np.nan),row.get('매출액_2025',np.nan),row.get('매출액_2026',np.nan),row.get('매출액_2027',np.nan)
    ov23,ov24,ov25,ov26,ov27 = row.get('영업이익_2023',np.nan),row.get('영업이익_2024',np.nan),row.get('영업이익_2025',np.nan),row.get('영업이익_2026',np.nan),row.get('영업이익_2027',np.nan)

    hdr = 'display:flex;gap:0;font-size:0.72rem;color:#6C757D;margin-bottom:4px;border-bottom:1px solid #E9ECEF;padding-bottom:2px;'
    rw = 'display:flex;gap:0;font-size:0.8rem;margin-bottom:2px;font-family:\\\'JetBrains Mono\\\', monospace;'
    lb = 'width:80px;padding:2px 6px;color:#6C757D;font-size:0.75rem;flex-shrink:0;'
    c = 'flex:1;text-align:right;padding:2px 6px;'
    ce = 'flex:1;text-align:right;padding:2px 6px;font-weight:700;'

    # PER/PBR/ROE 표시
    per_str = f'{per_val:.1f}' if pd.notna(per_val) else '-'
    pbr_str = f'{pbr_val:.2f}' if pd.notna(pbr_val) else '-'
    roe_str = f'{roe_val:.1f}%' if pd.notna(roe_val) else '-'
    sec_per_str = f'{sector_per:.1f}' if pd.notna(sector_per) else '-'

    # PER 비교 색상
    per_color = '#6C757D'
    if pd.notna(per_val) and pd.notna(sector_per):
        if per_val < sector_per:
            per_color = '#2EAA7B'  # 저평가 (녹색)
        else:
            per_color = '#FF6B6B'  # 고평가 (빨간)

    indicators_html = f"""<div style="display:flex;gap:16px;margin-top:4px;flex-wrap:wrap;">
        <div><span style="color:#6C757D;font-size:0.72rem;">PER</span><br><span style="color:{per_color};font-family:'JetBrains Mono',monospace;font-weight:700;font-size:0.95rem;">{per_str}</span></div>
        <div><span style="color:#6C757D;font-size:0.72rem;">업종PER</span><br><span style="color:#6C757D;font-family:'JetBrains Mono',monospace;font-size:0.9rem;">{sec_per_str}</span></div>
        <div><span style="color:#6C757D;font-size:0.72rem;">PBR</span><br><span style="color:#6C757D;font-family:'JetBrains Mono',monospace;font-weight:700;font-size:0.95rem;">{pbr_str}</span></div>
        <div><span style="color:#6C757D;font-size:0.72rem;">ROE</span><br><span style="color:#1D3557;font-family:'JetBrains Mono',monospace;font-weight:700;font-size:0.95rem;">{roe_str}</span></div>
    </div>"""

    evidence_html = f'<div style="margin-top:12px;padding:10px;background-color:#F8F9FA;border-radius:4px;border:1px solid #E9ECEF;"><div style="color:#6C757D;font-size:0.7rem;font-weight:600;margin-bottom:6px;">데이터 소스 (단위: 억원)</div><div class="evidence-scroll"><div style="{hdr}"><div style="{lb}"></div><div style="{c}">\'23</div><div style="{c}">\'24</div><div style="{c}color:#1D3557;">\'25E</div><div style="{c}color:#1D3557;">\'26E</div><div style="{c}color:#1D3557;">\'27E</div></div><div style="{rw}"><div style="{lb}">매출액</div><div style="{c}color:{fv_color(rv23)};">{fv(rv23)}</div><div style="{c}color:{fv_color(rv24)};">{fv(rv24)}</div><div style="{ce}color:{fv_color(rv25)};">{fv(rv25)}</div><div style="{ce}color:{fv_color(rv26)};">{fv(rv26)}</div><div style="{ce}color:{fv_color(rv27)};">{fv(rv27)}</div></div><div style="{rw}"><div style="{lb}">영업이익</div><div style="{c}color:{fv_color(ov23)};">{fv(ov23)}</div><div style="{c}color:{fv_color(ov24)};">{fv(ov24)}</div><div style="{ce}color:{fv_color(ov25)};">{fv(ov25)}</div><div style="{ce}color:{fv_color(ov26)};">{fv(ov26)}</div><div style="{ce}color:{fv_color(ov27)};">{fv(ov27)}</div></div></div></div>'

    st.markdown(f"""
    <div class="quant-card-light">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;">
            <div style="flex:1;min-width:260px;">
                <div style="display:flex;align-items:baseline;gap:8px;margin-bottom:8px;border-bottom:1px solid #E9ECEF;padding-bottom:6px;">
                    <span style="color:#1D3557;font-family:'JetBrains Mono',monospace;font-size:0.95rem;">#{rank}</span>
                    <span class="stock-name">{name}</span> {badge}
                    <span class="stock-code">{code_str}</span>
                </div>
                <div style="display:flex;gap:24px;flex-wrap:wrap;margin-top:4px;">
                    <div><span style="color:#6C757D;font-size:0.75rem;">현재가</span><br><span style="color:#212529;font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1.05rem;">{format_price(price)}</span></div>
                    <div><span style="color:#6C757D;font-size:0.75rem;">거래량</span><br><span style="color:#212529;font-family:'JetBrains Mono',monospace;font-size:0.95rem;">{format_volume(volume)}</span></div>
                    <div><span style="color:#6C757D;font-size:0.75rem;">시가총액</span><br><span style="color:#212529;font-family:'JetBrains Mono',monospace;font-size:0.95rem;">{format_number(mcap)}</span></div>
                    <div><span style="color:#6C757D;font-size:0.75rem;">데이터</span><br><span style="color:#1D3557;font-family:'JetBrains Mono',monospace;font-size:0.9rem;">{avail}</span></div>
                </div>
                {indicators_html}
            </div>
            <div style="display:flex;gap:20px;flex-wrap:wrap;align-items:center;">
                <div style="text-align:center;"><span style="color:#6C757D;font-size:0.72rem;">매출성장률</span>
                    <div style="display:flex;gap:8px;margin-top:2px;">
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">25E</span><br>{format_growth(rg25)}</div>
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">26E</span><br>{format_growth(rg26)}</div>
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">27E</span><br>{format_growth(rg27)}</div>
                    </div></div>
                <div style="text-align:center;"><span style="color:#6C757D;font-size:0.72rem;">영업이익성장률</span>
                    <div style="display:flex;gap:8px;margin-top:2px;">
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">25E</span><br>{format_growth(og25)}</div>
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">26E</span><br>{format_growth(og26)}</div>
                        <div style="text-align:center;"><span style="color:#6C757D;font-size:0.65rem;">27E</span><br>{format_growth(og27)}</div>
                    </div></div>
                <div style="display:flex;gap:12px;align-items:center;">
                    <div style="text-align:center;padding:6px 10px;background:#F8F9FA;border:1px solid #E9ECEF;border-radius:4px;">
                        <span style="color:#6C757D;font-size:0.7rem;">가시성 P{row.get('미래가시성_순위', 4)}</span><br>
                        <span style="color:#212529;font-family:'JetBrains Mono',monospace;font-weight:800;font-size:1.1rem;">{row.get('미래가시성_성장률',0):,.1f}%</span>
                    </div>
                    <div style="text-align:center;padding:6px 10px;background:#F8F9FA;border:1px solid #E9ECEF;border-radius:4px;">
                        <span style="color:#6C757D;font-size:0.7rem;">종합점수</span><br>
                        <span class="rainbow-score" style="font-family:'JetBrains Mono',monospace;font-weight:900;font-size:1.2rem;">{si2} {score:,.0f}</span>
                    </div>
                    <a href="{nurl}" target="_blank" class="naver-link" style="margin-left:4px;">
                        <svg viewBox="0 0 24 24" width="14" height="14" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
                        상세보기
                    </a>
                </div>
            </div>
        </div>
        {evidence_html}
    </div>""", unsafe_allow_html=True)

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

        st.markdown("### 📊 거래량 필터")
        vol_opts = {"제한 없음": 0, "1만 이상": 10000, "5만 이상": 50000, "10만 이상": 100000,
                    "50만 이상": 500000, "100만 이상": 1000000}
        vol_sel = st.selectbox("최소 거래량", list(vol_opts.keys()), index=2)
        min_vol = vol_opts[vol_sel]

        st.markdown("### 🛡️ 엄격한 재무 필터")
        req_min_rev_500 = st.checkbox("매출액 500억 이상 (매년)", value=True, help="어느 한 연도라도 매출액이 500억 미만이면 제외합니다.")
        req_op_profit = st.checkbox("영업이익 흑자 필수", value=True, help="최근 3개년 및 컨센서스 중 한 번이라도 영업손실(적자)이면 제외합니다.")
        drop_huge_loss = st.checkbox("매출 초과 적자기업 제외", value=True, help="영업손실 규모가 매출액보다 큰 경우(바이오/성장주 특화) 무조건 제외합니다.")

        st.markdown("### ⚡ 성능 설정")
        max_workers = st.slider("병렬 워커 수", 5, 100, 50, 5)

        st.markdown("---")

        crawl_btn = st.button("🔄 데이터 수집 (크롤링)", width="stretch",
                              help="전종목 컨센서스를 새로 크롤링합니다 (~5분)")

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
        crawl_all_data(progress_bar, status_text, markets, max_workers)
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

        df = apply_filters(all_df.copy(), rev_thresh, op_thresh, min_vol, markets, req_min_rev_500, req_op_profit, drop_huge_loss)

        # 업종 매핑 적용
        # CSV에 업종 컴럼이 이미 있으면 그대로 사용, 없으면 네이버에서 실시간 회수
        if '업종' not in df.columns or df['업종'].isna().all():
            sector_map = get_all_naver_sectors()
            df['업종'] = df['종목코드'].map(sector_map).fillna('기타')
        else:
            df['업종'] = df['업종'].fillna('기타')

        # 업종평균 PER 매핑
        sector_per_map = get_sector_per_map()
        df['업종평균PER'] = df['업종'].map(sector_per_map)

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
            st.warning("⚠️ 조건에 부합하는 종목이 없습니다. 사이드바에서 기준을 완화해보세요.")
            return

        # 탭
        tab1, tab2, tab3, tab4 = st.tabs(["📋 종목 카드 뷰", "🏢 업종별 테마순위", "📊 데이터 테이블", "📅 누적 기록"])

        with tab1:
            st.markdown(f'<div style="color:#FFFFFF; font-size:0.9rem; font-family:\'JetBrains Mono\', monospace; margin-bottom:10px;">> 스크리너 결과: {len(df)}개 발굴</div>', unsafe_allow_html=True)
            scol1, scol2 = st.columns([2, 1])
            with scol1:
                sort_options = {
                    "🌟 미래 가시성 핵심성장 (1~3순위)": "가시성기준_정렬점수",
                    "📊 매출+영업이익 합산점수": "종합성장점수",
                    "💰 매출 1년최대성장률 (단기)": "매출액_최대성장률",
                    "📈 영업이익 1년최대성장률 (단기)": "영업이익_최대성장률",
                    "🔥 거래량순": "Recent_Volume",
                    "🏢 시가총액순": "시가총액",
                    "💵 현재가순": "현재가",
                }
                sort_label = st.selectbox("정렬 기준", list(sort_options.keys()), index=0, label_visibility="collapsed")
                sort_col = sort_options[sort_label]
            with scol2:
                sort_order = st.selectbox("순서", ["내림차순", "오름차순"], index=0, label_visibility="collapsed")
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

        with tab2:
            st.markdown("<h3 style='color:#FFFFFF;'>🏢 업종별 수익 테마 순위</h3>", unsafe_allow_html=True)
            if '업종' in df.columns:
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

        with tab3:
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

            show_cols = ['종목명','종목코드','시장','현재가','Recent_Volume','시가총액','업종',
                'PER','PBR','ROE','업종평균PER','데이터_가용성',
                '매출액_성장률_2025','매출액_성장률_2026','매출액_성장률_2027','매출액_최대성장률',
                '영업이익_성장률_2025','영업이익_성장률_2026','영업이익_성장률_2027','영업이익_최대성장률','종합성장점수']
            ac = [c for c in show_cols if c in df.columns]
            st.dataframe(df[ac], use_container_width=True, height=600, column_config={
                "종목명": st.column_config.TextColumn("종목명", width="medium"),
                "종목코드": st.column_config.TextColumn("코드", width="small"),
                "현재가": st.column_config.NumberColumn("현재가", format="%d원"),
                "Recent_Volume": st.column_config.NumberColumn("거래량", format="%d"),
                "시가총액": st.column_config.NumberColumn("시총(억)", format="%d"),
                "PER": st.column_config.NumberColumn("PER", format="%.1f"),
                "PBR": st.column_config.NumberColumn("PBR", format="%.2f"),
                "ROE": st.column_config.NumberColumn("ROE", format="%.1f%%"),
                "업종평균PER": st.column_config.NumberColumn("업종PER", format="%.1f"),
                "매출액_최대성장률": st.column_config.NumberColumn("매출MAX%", format="%.1f%%"),
                "영업이익_최대성장률": st.column_config.NumberColumn("영업이익MAX%", format="%.1f%%"),
                "종합성장점수": st.column_config.NumberColumn("종합점수", format="%.0f"),
            })

        with tab4:
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
