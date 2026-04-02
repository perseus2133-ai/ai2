#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
독립 실행형 크롤링 스크립트 (Streamlit 없이 동작)
GitHub Actions에서 스케줄 실행용
"""

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
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

# ============================================================
# 저장 경로 설정
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_FILE  = os.path.join(DATA_DIR, "consensus_data.csv")
META_FILE = os.path.join(DATA_DIR, "meta.json")

os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================
# 상수
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

# ============================================================
# 크롤링 함수 (app.py와 동일)
# ============================================================
def parse_numeric(text):
    if not text or text.strip() in ['', '-', 'N/A', 'nan', '\xa0']: return np.nan
    text = text.strip().replace(',', '').replace(' ', '').replace('\xa0', '')
    if text.startswith('(') and text.endswith(')'): text = '-' + text[1:-1]
    try: return float(text)
    except: return np.nan


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
                all_stocks.append({
                    '종목코드': cm.group(1), '종목명': nm, '시장': market_name,
                    '현재가': int(p) if p.isdigit() else 0,
                    '시가총액': int(m) if m.isdigit() else 0,
                    'Recent_Volume': int(v) if v.isdigit() else 0
                })
                found = True
            if not found or page >= last_page: break
            page += 1
            time.sleep(0.05)
        except Exception as e:
            print(f"[WARN] 종목리스트 {page}페이지 오류: {e}")
            break
    return pd.DataFrame(all_stocks)


def scrape_fnguide_supplement(stock_code, stock_name=''):
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
                    if pd.notna(val): valid_count += 1
                if valid_count > best_valid_count:
                    best_valid_count = valid_count
                    best_data = temp_data
            dm[mn] = best_data

        try:
            fg = scrape_fnguide_supplement(stock_code, stock_name)
            for mn in ['매출액', '영업이익']:
                if mn in fg:
                    for yr in [2025, 2026, 2027]:
                        if (yr not in dm.get(mn, {}) or pd.isna(dm.get(mn, {}).get(yr))) \
                                and yr in fg[mn] and pd.notna(fg[mn][yr]):
                            if mn not in dm: dm[mn] = {}
                            dm[mn][yr] = fg[mn][yr]
        except:
            pass

        if not dm: return None

        ty = [2025, 2026, 2027]; by = [2024, 2025, 2026]
        for m in ['매출액', '영업이익']:
            if m not in dm: continue
            for y in [2023, 2024] + ty:
                result[f'{m}_{y}'] = dm[m].get(y, np.nan)
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

        av = sum(1 for y in ty if any(
            pd.notna(result.get(f'{m}_{y}')) for m in ['매출액', '영업이익']))
        result['데이터_가용성'] = f'{av}년치 존재'
        result['가용_연도수'] = av
        return result
    except:
        return None


# ============================================================
# 메인 크롤링 실행
# ============================================================
def main():
    MARKETS = ['KOSPI', 'KOSDAQ']
    MAX_WORKERS = 50

    print(f"[{datetime.datetime.now()}] ▶ 크롤링 시작")

    # 1단계: 종목 리스트
    print("1단계: 종목 리스트 수집...")
    dfs = []
    for m_code, m_name in [("0", "KOSPI"), ("1", "KOSDAQ")]:
        if m_name in MARKETS:
            df_tmp = get_stock_list_naver(m_code)
            dfs.append(df_tmp)
            print(f"  {m_name}: {len(df_tmp)}개")

    stock_df = pd.concat(dfs, ignore_index=True)
    for p in ['스팩', 'SPAC', 'ETF', 'ETN', '리츠', 'REIT', '인버스', '레버리지', '선물', '채권']:
        stock_df = stock_df[~stock_df['종목명'].str.contains(p, na=False)]
    stock_df = stock_df.reset_index(drop=True)
    print(f"  필터 후 총 {len(stock_df)}개")

    # 2단계: 컨센서스 크롤링
    total = len(stock_df)
    print(f"2단계: {total}개 종목 컨센서스 수집 (워커: {MAX_WORKERS})")
    results, counter, lock = [], [0], threading.Lock()

    def process(rd):
        c = scrape_naver_consensus(rd['종목코드'], rd['종목명'])
        if c:
            c['시장'] = rd['시장']
            c['현재가'] = rd['현재가']
            c['시가총액'] = rd['시가총액']
            c['Recent_Volume'] = rd['Recent_Volume']
            return ('ok', c)
        return ('no', None)

    rows = [r.to_dict() for _, r in stock_df.iterrows()]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process, r): i for i, r in enumerate(rows)}
        for f in as_completed(futs):
            with lock:
                counter[0] += 1
                cnt = counter[0]
            try:
                s, d = f.result()
                if s == 'ok': results.append(d)
            except:
                pass
            if cnt % 200 == 0 or cnt == total:
                print(f"  {cnt}/{total} ({cnt/total*100:.0f}%) | 수집: {len(results)}개")

    if not results:
        print("[ERROR] 수집된 데이터 없음")
        return

    # 3단계: CSV 저장
    df = pd.DataFrame(results)
    df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')

    meta = {
        'timestamp': datetime.datetime.now().isoformat(),
        'markets': MARKETS,
        'total_analyzed': total,
        'data_count': len(results),
    }
    with open(META_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[{datetime.datetime.now()}] ✅ 완료! {len(results)}개 → {CSV_FILE}")


if __name__ == '__main__':
    main()
