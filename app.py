#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║   🚀 FnGuide 컨센서스 기반 초고성장 종목 발굴 시스템       ║
║   Streamlit 대시보드 (캐시 지원)                            ║
╚══════════════════════════════════════════════════════════════╝
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
import pickle
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

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
CACHE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(CACHE_DIR, "consensus_cache.pkl")

def save_cache(data_df, meta):
    """전체 컨센서스 데이터를 pickle 캐시로 저장"""
    cache = {'data': data_df, 'meta': meta, 'timestamp': datetime.datetime.now()}
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)

def load_cache():
    """캐시 파일 로드. 없으면 None 반환"""
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE, 'rb') as f:
            return pickle.load(f)
    except Exception:
        return None

def get_cache_info():
    """캐시 파일 정보 반환"""
    cache = load_cache()
    if cache is None:
        return None
    return {
        'timestamp': cache['timestamp'],
        'total_stocks': len(cache['data']),
        'meta': cache.get('meta', {}),
    }

# ============================================================
# 커스텀 CSS
# ============================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700;900&display=swap');
html, body, [class*="st-"] { font-family: 'Noto Sans KR', sans-serif; }
.hero-header { background: linear-gradient(135deg, rgba(99,102,241,0.15), rgba(168,85,247,0.1)); border: 1px solid rgba(99,102,241,0.3); border-radius: 16px; padding: 28px 36px; margin-bottom: 24px; text-align: center; backdrop-filter: blur(10px); }
.hero-header h1 { background: linear-gradient(135deg, #818cf8, #c084fc, #f472b6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.2rem; font-weight: 900; margin: 0 0 6px 0; }
.hero-header p { color: #94a3b8; font-size: 0.95rem; margin: 0; }
.metric-card { background: linear-gradient(135deg, rgba(30,30,60,0.9), rgba(40,40,80,0.8)); border: 1px solid rgba(99,102,241,0.25); border-radius: 14px; padding: 20px 22px; text-align: center; transition: all 0.3s ease; }
.metric-card:hover { border-color: rgba(129,140,248,0.5); transform: translateY(-2px); box-shadow: 0 8px 25px rgba(99,102,241,0.15); }
.metric-card .metric-value { font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #818cf8, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 4px 0; }
.metric-card .metric-label { color: #94a3b8; font-size: 0.82rem; font-weight: 500; letter-spacing: 0.5px; }
.badge-kospi { display: inline-block; background: linear-gradient(135deg, #3b82f6, #6366f1); color: white; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }
.badge-kosdaq { display: inline-block; background: linear-gradient(135deg, #10b981, #059669); color: white; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }
.stock-card { background: linear-gradient(135deg, rgba(25,25,55,0.95), rgba(35,35,70,0.9)); border: 1px solid rgba(99,102,241,0.2); border-radius: 14px; padding: 18px 20px; margin-bottom: 12px; transition: all 0.3s ease; }
.stock-card:hover { border-color: rgba(129,140,248,0.5); box-shadow: 0 6px 20px rgba(99,102,241,0.12); }
.stock-name { font-size: 1.05rem; font-weight: 700; color: #e2e8f0; }
.stock-code { color: #64748b; font-size: 0.8rem; }
.growth-positive { color: #f87171; font-weight: 700; }
.growth-negative { color: #60a5fa; font-weight: 700; }
.growth-mega { color: #fbbf24; font-weight: 800; text-shadow: 0 0 10px rgba(251,191,36,0.3); }
a.naver-link { display: inline-flex; align-items: center; gap: 4px; background: linear-gradient(135deg, #059669, #10b981); color: white !important; text-decoration: none !important; padding: 5px 14px; border-radius: 8px; font-size: 0.78rem; font-weight: 600; transition: all 0.2s ease; }
a.naver-link:hover { background: linear-gradient(135deg, #047857, #059669); box-shadow: 0 4px 12px rgba(16,185,129,0.3); }
div.stButton > button { background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; color: white !important; border: none !important; border-radius: 12px !important; padding: 12px 32px !important; font-weight: 700 !important; font-size: 1rem !important; transition: all 0.3s ease !important; width: 100% !important; }
div.stButton > button:hover { background: linear-gradient(135deg, #4f46e5, #7c3aed) !important; box-shadow: 0 8px 25px rgba(99,102,241,0.35) !important; }
.stProgress > div > div { background: linear-gradient(90deg, #6366f1, #a855f7, #ec4899) !important; }
.cache-info { background: rgba(16,185,129,0.1); border: 1px solid rgba(16,185,129,0.3); border-radius: 10px; padding: 10px 14px; margin: 8px 0; color: #34d399; font-size: 0.8rem; text-align: center; }
.cache-none { background: rgba(251,191,36,0.1); border: 1px solid rgba(251,191,36,0.3); border-radius: 10px; padding: 10px 14px; margin: 8px 0; color: #fbbf24; font-size: 0.8rem; text-align: center; }
.divider { border: none; border-top: 1px solid rgba(99,102,241,0.15); margin: 24px 0; }
#MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}

/* 모바일 반응형 전용 사이즈 (화면 너비 768px 이하) */
@media (max-width: 768px) {
    .hero-header { padding: 20px 16px; }
    .hero-header h1 { font-size: 1.6rem; }
    .hero-header p { font-size: 0.85rem; }
    .metric-card { padding: 12px 10px; }
    .metric-card .metric-value { font-size: 1.3rem; }
    .metric-card .metric-label { font-size: 0.7rem; }
    .stock-card { padding: 14px 12px; }
    .stock-name { font-size: 0.95rem; }
    div.evidence-scroll { overflow-x: auto; white-space: nowrap; padding-bottom: 5px; }
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

import threading

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session

def scrape_fnguide_supplement(stock_code, stock_name=''):
    """FnGuide에서 2026E, 2027E 컨센서스 데이터를 보조로 가져온다."""
    try:
        session = get_session()
        url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}'
        resp = session.get(url, timeout=7)
        resp.encoding = 'utf-8'
        if resp.status_code != 200: return {}
        soup = BeautifulSoup(resp.text, 'lxml')
        
        # 종목명 검증: FnGuide가 다른 종목(삼성전자 등) 기본값을 반환하는 경우 차단
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
    status_text.markdown(f"✅ **완료!** {len(results)}개 종목 데이터 수집 → 캐시 저장됨")
    return df


# ============================================================
# 필터링 (캐시 데이터에서 즉시 필터링)
# ============================================================
def apply_filters(df, rev_thresh, op_thresh, min_vol, markets, req_min_rev_500=True, req_op_profit=True, drop_huge_loss=True):
    """캐시된 전체 데이터에서 필터 조건 적용 (즉시)"""
    if df.empty: return df
    # 시장 필터
    if markets: df = df[df['시장'].isin(markets)]
    # 거래량 필터
    if min_vol > 0: df = df[df['Recent_Volume'] >= min_vol]
    
    # 📉 엄격한 재무 필터들
    def strict_financial_check(row):
        yrs = [2023, 2024, 2025, 2026, 2027]
        for y in yrs:
            rv = row.get(f'매출액_{y}', np.nan)
            ov = row.get(f'영업이익_{y}', np.nan)
            
            # 매출액 하한선 (500억 이하 제외)
            if req_min_rev_500 and pd.notna(rv) and rv < 500: return False
            
            # 영업이익 흑자 필수 (한번이라도 마이너스면 제외)
            if req_op_profit and pd.notna(ov) and ov < 0: return False
            
            # 적자폭이 매출액보다 큰 경우 제외 (바이오성장주 특화 제외)
            if drop_huge_loss and pd.notna(ov) and pd.notna(rv) and ov < 0 and abs(ov) > rv: return False
            
        return True
        
    df = df[df.apply(strict_financial_check, axis=1)].copy()
    # 성장률 필터
    def meets(row):
        rv = [row.get(f'매출액_성장률_{y}', np.nan) for y in [2025,2026,2027]]
        ov = [row.get(f'영업이익_성장률_{y}', np.nan) for y in [2025,2026,2027]]
        rv = [x for x in rv if pd.notna(x)]; ov = [x for x in ov if pd.notna(x)]
        return (any(x >= rev_thresh for x in rv)) or (any(x >= op_thresh for x in ov))
    df = df[df.apply(meets, axis=1)].copy()
    # 종합 점수 및 미래가시성 등급 계산
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
        
        # 1. 최우선 정렬 조건 (미래 전망치 가용 여부에 따른 그룹화 및 수익률 계산)
        rv24 = row.get('매출액_2024', np.nan)
        rv25 = row.get('매출액_2025', np.nan)
        rv26 = row.get('매출액_2026', np.nan)
        rv27 = row.get('매출액_2027', np.nan)
        
        pr = 4
        mt = 0
        if pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 1
            mt = ((rv27 / rv25) ** (1/2) - 1) * 100
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 2
            mt = ((rv26 / rv25) - 1) * 100
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 3
            mt = ((rv25 / rv24) - 1) * 100
            
        priority_ranks.append(pr)
        metric_pcts.append(mt)
        # Sort score: 1순위: 3억 +, 2순위: 2억 +, 3순위: 1억 +, 4순위: mt
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
    if pd.isna(v): return '<span style="color:#64748b;">-</span>'
    if v >= 500: return f'<span class="growth-mega">🔥 {v:,.1f}%</span>'
    if v > 0: return f'<span class="growth-positive">▲ {v:,.1f}%</span>'
    if v < 0: return f'<span class="growth-negative">▼ {v:,.1f}%</span>'
    return f'<span style="color:#94a3b8;">{v:,.1f}%</span>'

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
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == "9084":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown('<h3 style="text-align:center; color:#e2e8f0; margin-top:50px;">🔒 초고성장 종목 발굴 시스템</h3>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("접속 비밀번호를 입력해주세요", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown('<h3 style="text-align:center; color:#e2e8f0; margin-top:50px;">🔒 초고성장 종목 발굴 시스템</h3>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("접속 비밀번호를 입력해주세요", type="password", on_change=password_entered, key="password")
            st.error("비밀번호가 올바르지 않습니다.")
        return False
    return True

# ============================================================
# 메인 UI
# ============================================================
def main():
    if not check_password():
        return

    st.markdown("""
    <div class="hero-header">
        <h1>🚀 초고성장 종목 발굴 시스템</h1>
        <p>FnGuide 컨센서스 기반 · 매출/영업이익 비약적 상승 종목 실시간 스크리닝</p>
    </div>
    """, unsafe_allow_html=True)

    # 캐시 상태 확인
    cache_info = get_cache_info()

    # ---- 사이드바 ----
    with st.sidebar:
        st.markdown("## ⚙️ 스크리닝 설정")
        st.markdown("---")

        # 캐시 상태 표시
        st.markdown("### 💾 데이터 캐시")
        if cache_info:
            ts = cache_info['timestamp'].strftime('%Y-%m-%d %H:%M')
            cnt = cache_info['total_stocks']
            st.markdown(f'<div class="cache-info">✅ 캐시 존재<br>{ts} 수집<br>{cnt}개 종목</div>', unsafe_allow_html=True)
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
        max_workers = st.slider("병렬 워커 수", 5, 30, 20, 5)

        st.markdown("---")

        # 데이터 수집 버튼 (크롤링)
        crawl_btn = st.button("🔄 데이터 수집 (크롤링)", width="stretch",
                              help="전종목 컨센서스를 새로 크롤링합니다 (~5분)")

        st.markdown("---")
        st.markdown("""
        <div style="color:#64748b; font-size:0.75rem; text-align:center; line-height:1.6;">
            <b>💡 사용법</b><br>
            1) <b>데이터 수집</b> 버튼으로 캐시 생성<br>
            2) 거래량/성장률 <b>즉시 필터링</b><br>
            필터 변경 시 재크롤링 불필요!
        </div>
        """, unsafe_allow_html=True)

    # ---- 메인 영역 ----

    # 크롤링 실행
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

    # 캐시에서 데이터 로드 + 필터 적용
    cache = load_cache()
    if cache is not None:
        all_df = cache['data']
        cache_ts = cache['timestamp']
        elapsed = st.session_state.get('elapsed', 0)

        # 필터 적용 (즉시)
        df = apply_filters(all_df.copy(), rev_thresh, op_thresh, min_vol, markets, req_min_rev_500, req_op_profit, drop_huge_loss)

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
            ts_str = cache_ts.strftime('%m/%d %H:%M')
            st.markdown(f'<div class="metric-card"><div class="metric-label">캐시 데이터</div><div class="metric-value" style="font-size:1.3rem;">{ts_str}</div><div class="metric-label">수집 시점</div></div>', unsafe_allow_html=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        if df.empty:
            st.warning("⚠️ 조건에 부합하는 종목이 없습니다. 사이드바에서 기준을 완화해보세요.")
            return

        # 탭
        tab1, tab2 = st.tabs(["📋 종목 카드 뷰", "📊 데이터 테이블"])

        with tab1:
            st.markdown(f"### 🏆 발굴된 {len(df)}개 종목")
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
                st.markdown(f"<div style='text-align:center;color:#94a3b8;padding:8px;'>페이지 {st.session_state['page']} / {total_pages}</div>", unsafe_allow_html=True)
            with pc3:
                if st.button("다음 ▶", disabled=st.session_state['page']>=total_pages): st.session_state['page']+=1; st.rerun()

            si = (st.session_state['page']-1)*page_size
            for rank, (_, row) in enumerate(df_s.iloc[si:si+page_size].iterrows(), start=si+1):
                code = row.get('종목코드',''); name = row.get('종목명',''); market = row.get('시장','')
                price = row.get('현재가',0); volume = row.get('Recent_Volume',0)
                mcap = row.get('시가총액',0); score = row.get('종합성장점수',0); avail = row.get('데이터_가용성','-')
                rg25,rg26,rg27 = row.get('매출액_성장률_2025',np.nan),row.get('매출액_성장률_2026',np.nan),row.get('매출액_성장률_2027',np.nan)
                og25,og26,og27 = row.get('영업이익_성장률_2025',np.nan),row.get('영업이익_성장률_2026',np.nan),row.get('영업이익_성장률_2027',np.nan)
                nurl = f"https://finance.naver.com/item/main.naver?code={code}"
                badge = f'<span class="badge-kospi">KOSPI</span>' if market=='KOSPI' else f'<span class="badge-kosdaq">KOSDAQ</span>'
                sc = "#fbbf24" if score>=1000 else "#f87171" if score>=500 else "#818cf8"
                si2 = "🔥" if score>=1000 else "🚀" if score>=500 else "📈"

                # 근거 데이터: 실제 매출액/영업이익 수치
                def fv(v):
                    if pd.isna(v): return '-'
                    return f'{v:,.0f}'
                def fv_color(v):
                    if pd.isna(v): return '#475569'
                    return '#60a5fa' if v < 0 else '#e2e8f0'

                rv23,rv24,rv25,rv26,rv27 = row.get('매출액_2023',np.nan),row.get('매출액_2024',np.nan),row.get('매출액_2025',np.nan),row.get('매출액_2026',np.nan),row.get('매출액_2027',np.nan)
                ov23,ov24,ov25,ov26,ov27 = row.get('영업이익_2023',np.nan),row.get('영업이익_2024',np.nan),row.get('영업이익_2025',np.nan),row.get('영업이익_2026',np.nan),row.get('영업이익_2027',np.nan)

                hdr = 'display:flex;gap:0;font-size:0.68rem;color:#64748b;margin-bottom:2px;'
                rw = 'display:flex;gap:0;font-size:0.78rem;margin-bottom:1px;'
                lb = 'width:70px;padding:2px 6px;color:#94a3b8;font-weight:600;font-size:0.72rem;flex-shrink:0;'
                c = 'flex:1;text-align:right;padding:2px 6px;'
                ce = 'flex:1;text-align:right;padding:2px 6px;font-weight:700;'

                evidence_html = f'<div style="margin-top:10px;padding:10px 14px;background:rgba(99,102,241,0.06);border-radius:10px;border:1px solid rgba(99,102,241,0.12);"><div style="color:#818cf8;font-size:0.72rem;font-weight:700;margin-bottom:6px;">근거 데이터 (억원)</div><div class="evidence-scroll"><div style="{hdr}"><div style="{lb}"></div><div style="{c}">23</div><div style="{c}">24</div><div style="{c}color:#a5b4fc;">25E</div><div style="{c}color:#a5b4fc;">26E</div><div style="{c}color:#a5b4fc;">27E</div></div><div style="{rw}"><div style="{lb}">매출액</div><div style="{c}color:{fv_color(rv23)};">{fv(rv23)}</div><div style="{c}color:{fv_color(rv24)};">{fv(rv24)}</div><div style="{ce}color:{fv_color(rv25)};">{fv(rv25)}</div><div style="{ce}color:{fv_color(rv26)};">{fv(rv26)}</div><div style="{ce}color:{fv_color(rv27)};">{fv(rv27)}</div></div><div style="{rw}"><div style="{lb}">영업이익</div><div style="{c}color:{fv_color(ov23)};">{fv(ov23)}</div><div style="{c}color:{fv_color(ov24)};">{fv(ov24)}</div><div style="{ce}color:{fv_color(ov25)};">{fv(ov25)}</div><div style="{ce}color:{fv_color(ov26)};">{fv(ov26)}</div><div style="{ce}color:{fv_color(ov27)};">{fv(ov27)}</div></div></div></div>'

                st.markdown(f"""
                <div class="stock-card">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;">
                        <div style="flex:1;min-width:200px;">
                            <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
                                <span style="color:#4f46e5;font-weight:800;font-size:1.1rem;">#{rank}</span>
                                <span class="stock-name">{name}</span> {badge}
                                <span class="stock-code">{code}</span>
                            </div>
                            <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:8px;">
                                <div><span style="color:#64748b;font-size:0.75rem;">현재가</span><br><span style="color:#e2e8f0;font-weight:700;font-size:1.05rem;">{format_price(price)}</span></div>
                                <div><span style="color:#64748b;font-size:0.75rem;">거래량</span><br><span style="color:#e2e8f0;font-weight:600;">{format_volume(volume)}</span></div>
                                <div><span style="color:#64748b;font-size:0.75rem;">시가총액</span><br><span style="color:#e2e8f0;font-weight:600;">{format_number(mcap)}억</span></div>
                                <div><span style="color:#64748b;font-size:0.75rem;">데이터</span><br><span style="color:#a5b4fc;font-weight:600;">{avail}</span></div>
                            </div>
                        </div>
                        <div style="display:flex;gap:16px;flex-wrap:wrap;align-items:center;">
                            <div style="text-align:center;"><span style="color:#64748b;font-size:0.7rem;">매출 성장률</span>
                                <div style="display:flex;gap:6px;margin-top:3px;">
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">25E</span><br>{format_growth(rg25)}</div>
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">26E</span><br>{format_growth(rg26)}</div>
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">27E</span><br>{format_growth(rg27)}</div>
                                </div></div>
                            <div style="text-align:center;"><span style="color:#64748b;font-size:0.7rem;">영업이익 성장률</span>
                                <div style="display:flex;gap:6px;margin-top:3px;">
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">25E</span><br>{format_growth(og25)}</div>
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">26E</span><br>{format_growth(og26)}</div>
                                    <div style="text-align:center;"><span style="color:#475569;font-size:0.65rem;">27E</span><br>{format_growth(og27)}</div>
                                </div></div>
                            <!-- 가시성 순위 및 종합점수 -->
                            <div style="display:flex;gap:12px;align-items:center;"><div style="text-align:center;padding:8px 12px;background:rgba(236,72,153,0.1);border-radius:10px;border:1px solid rgba(236,72,153,0.2);margin-right:2px;"><span style="color:#64748b;font-size:0.65rem;">가시성 {row.get('미래가시성_순위', 4)}순위</span><br><span style="color:#ec4899;font-weight:800;font-size:1.1rem;">{row.get('미래가시성_성장률',0):,.1f}%</span></div><div style="text-align:center;padding:8px 12px;background:rgba(99,102,241,0.1);border-radius:10px;border:1px solid rgba(99,102,241,0.2);"><span style="color:#64748b;font-size:0.65rem;">종합점수</span><br><span style="color:{sc};font-weight:800;font-size:1.1rem;">{si2} {score:,.0f}</span></div><a href="{nurl}" target="_blank" class="naver-link" style="margin-left:4px;">📊 네이버 금융</a></div>
                        </div>
                    </div>
                    {evidence_html}
                </div>""", unsafe_allow_html=True)

        with tab2:
            st.markdown("### 📊 전체 데이터 테이블")
            dc1, dc2 = st.columns(2)
            with dc1:
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📥 CSV 다운로드", data=csv, file_name=f"high_growth_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)
            with dc2:
                try:
                    import io; buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine='openpyxl') as w: df.to_excel(w, index=False, sheet_name='초고성장종목')
                    st.download_button("📥 Excel 다운로드", data=buf.getvalue(), file_name=f"high_growth_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                except: pass
            show_cols = ['종목명','종목코드','시장','현재가','Recent_Volume','시가총액','데이터_가용성',
                '매출액_성장률_2025','매출액_성장률_2026','매출액_성장률_2027','매출액_최대성장률',
                '영업이익_성장률_2025','영업이익_성장률_2026','영업이익_성장률_2027','영업이익_최대성장률','종합성장점수']
            ac = [c for c in show_cols if c in df.columns]
            st.dataframe(df[ac], use_container_width=True, height=600, column_config={
                "종목명": st.column_config.TextColumn("종목명", width="medium"),
                "종목코드": st.column_config.TextColumn("코드", width="small"),
                "현재가": st.column_config.NumberColumn("현재가", format="%d원"),
                "Recent_Volume": st.column_config.NumberColumn("거래량", format="%d"),
                "시가총액": st.column_config.NumberColumn("시총(억)", format="%d"),
                "매출액_최대성장률": st.column_config.NumberColumn("매출MAX%", format="%.1f%%"),
                "영업이익_최대성장률": st.column_config.NumberColumn("OP MAX%", format="%.1f%%"),
                "종합성장점수": st.column_config.NumberColumn("종합점수", format="%.0f"),
            })
    else:
        # 캐시 없음 - 초기 화면
        st.markdown("""
        <div style="text-align:center; padding:60px 20px;">
            <div style="font-size:4rem; margin-bottom:16px;">🔍</div>
            <h2 style="color:#818cf8; font-weight:700; margin-bottom:12px;">종목 발굴을 시작하세요</h2>
            <p style="color:#94a3b8; font-size:1rem; max-width:500px; margin:0 auto; line-height:1.8;">
                왼쪽 사이드바에서 <b style="color:#a5b4fc;">🔄 데이터 수집</b> 버튼을 눌러<br>
                전종목 컨센서스 데이터를 수집하세요. (~5분)<br><br>
                수집 후에는 거래량/성장률 필터를 <b style="color:#34d399;">즉시</b> 변경할 수 있습니다!
            </p>
            <div style="margin-top:32px; display:flex; justify-content:center; gap:20px; flex-wrap:wrap;">
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">1️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.85rem; margin-top:4px;">데이터 수집</div>
                    <div style="color:#64748b; font-size:0.75rem;">~5분 (1회만)</div>
                </div>
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">2️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.85rem; margin-top:4px;">즉시 필터링</div>
                    <div style="color:#64748b; font-size:0.75rem;">거래량/성장률 자유 변경</div>
                </div>
                <div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:16px 24px; text-align:center;">
                    <div style="font-size:1.5rem;">3️⃣</div>
                    <div style="color:#a5b4fc; font-weight:600; font-size:0.85rem; margin-top:4px;">결과 확인</div>
                    <div style="color:#64748b; font-size:0.75rem;">카드 뷰 + Excel</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


if __name__ == '__main__':
    main()
