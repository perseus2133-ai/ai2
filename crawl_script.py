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
from zoneinfo import ZoneInfo
import warnings

warnings.filterwarnings('ignore')

KST = ZoneInfo("Asia/Seoul")

# ============================================================
# 저장 경로 설정
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_FILE  = os.path.join(DATA_DIR, "consensus_data.csv")
META_FILE = os.path.join(DATA_DIR, "meta.json")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
SNAPSHOT_DIR = os.path.join(DATA_DIR, "consensus_snapshots")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

def now_kst():
    return datetime.datetime.now(KST)

# ============================================================
# 상수
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

# FnGuide 동시 요청 제한용 세마포어.
# 메인 크롤은 워커 50개로 돌지만, FnGuide(comp.fnguide.com)는 해외 IP에서
# 동시 요청이 많으면 rate limit/connect timeout으로 27·28E를 놓친다.
# 네이버 호출은 50워커 그대로 두고 FnGuide만 동시 4개로 제한해 성공률을 확보.
# (8→4: 좋은 날 rate-limit 차단 확률을 더 낮춤. 크롤은 다소 길어짐)
_FNGUIDE_SEM = threading.Semaphore(4)

# FnGuide 회로 차단기(circuit breaker).
# FnGuide는 한 IP에서 많이 때리면 '삼성전자 고정 페이지'로 차단하고 쿨다운이
# 길다. GitHub Actions(미국 IP)는 특히 쉽게 차단되어, 2600종목을 계속 5회씩
# 재시도하면 (a) 1~2시간 낭비 (b) 차단을 더 악화시킨다.
# → 삼성 고정 페이지가 연속 N회 감지되면 이번 실행에서 FnGuide 호출을 전면
#   중단한다. 27/28E는 carry-forward가 최근 스냅샷 값으로 채운다.
_FG_BLOCK_THRESHOLD = 20
_FG_lock = threading.Lock()
_FG_consec_block = [0]
_FG_tripped = threading.Event()

def _fg_note_block():
    with _FG_lock:
        _FG_consec_block[0] += 1
        if _FG_consec_block[0] >= _FG_BLOCK_THRESHOLD and not _FG_tripped.is_set():
            _FG_tripped.set()
            print(f"[FnGuide] 연속 {_FG_BLOCK_THRESHOLD}회 차단(삼성 고정 페이지) "
                  f"→ 이번 실행 FnGuide 호출 중단. 27/28E는 carry-forward로 보강.")

def _fg_note_ok():
    with _FG_lock:
        _FG_consec_block[0] = 0

def _fg_is_block_page(resp, stock_code):
    """응답이 '삼성전자 고정 페이지'(차단)인지 title로 판별."""
    if resp is None:
        return False
    try:
        t = BeautifulSoup(resp.text, 'lxml').find('title')
        name = t.get_text(strip=True) if t else ''
        return ('삼성전자' in name) and (str(stock_code).zfill(6) != '005930')
    except Exception:
        return False

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session

# ============================================================
# 크롤링 함수
# ============================================================
def parse_numeric(text):
    if not text or text.strip() in ['', '-', 'N/A', 'nan', '\xa0']: return np.nan
    text = text.strip().replace(',', '').replace(' ', '').replace('\xa0', '')
    if text.startswith('(') and text.endswith(')'): text = '-' + text[1:-1]
    try: return float(text)
    except: return np.nan



def get_naver_sector_map():
    """네이버 금융 업종별 종목 코드 -> 업종명 매핑 딕셔너리를 반환한다."""
    sector_map = {}
    try:
        session = get_session()
        url = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
        res = session.get(url, timeout=10)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "lxml")
        links = soup.select("table.type_1 td a")
        print(f"  업종 수: {len(links)}개")

        def fetch_sector(a_tag):
            s_name = a_tag.text.strip()
            link = "https://finance.naver.com" + a_tag["href"]
            try:
                sub_res = session.get(link, timeout=10)
                sub_res.encoding = "euc-kr"
                sub_soup = BeautifulSoup(sub_res.text, "lxml")
                codes = []
                for sub_a in sub_soup.select("table.type_5 td.name a"):
                    href = sub_a.get("href", "")
                    if "code=" in href:
                        c = href.split("code=")[-1][:6]
                        codes.append((c, s_name))
                return codes
            except:
                return []

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch_sector, a) for a in links]
            for f in as_completed(futures):
                try:
                    for c, s in f.result():
                        sector_map[c] = s
                except:
                    pass
    except Exception as e:
        print(f"[WARN] 업종 매핑 실패: {e}")
    return sector_map


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


def scrape_fnguide_supplement(stock_code, stock_name='', _max_retries=5):
    """FnGuide에서 27E/28E 컨센서스 보충.

    중요: GitHub Actions 등 해외 IP에서는 FnGuide(comp.fnguide.com)가
    Referer 헤더 없이 호출하면 간헐적으로 connect timeout이 발생한다.
    진단 결과 Referer 헤더 + connect/read 분리 타임아웃(5,12) + 재시도로
    성공률 100% 확인 (2026-06 검증). 아래 전략을 반드시 유지할 것.
    """
    # 회로 차단기: 이미 이번 실행에서 FnGuide가 차단으로 판정됐으면 즉시 포기
    # (네트워크 호출 안 함 → 시간 절약 + 차단 악화 방지, 27/28E는 carry-forward)
    if _FG_tripped.is_set():
        return {}

    session = get_session()
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}'
    fg_headers = {'Referer': 'https://comp.fnguide.com/'}
    best_dm = {}
    for attempt in range(1, _max_retries + 1):
        if _FG_tripped.is_set():
            return best_dm
        resp = None
        try:
            # connect 5s / read 12s 분리: 연결이 열리는 순간을 빠르게 잡고
            # 안 되면 즉시 재시도 (해외 IP 간헐 차단 우회)
            # 세마포어로 동시 FnGuide 요청을 8개로 제한 → rate limit 회피
            with _FNGUIDE_SEM:
                resp = session.get(url, headers=fg_headers, timeout=(5, 12))
        except Exception:
            resp = None
        if resp is None or resp.status_code != 200:
            if attempt < _max_retries:
                time.sleep(0.4 * attempt)
            continue
        # 삼성 고정 페이지(차단) 감지 → 회로 차단기 카운트, 재시도 무의미하므로 중단
        if _fg_is_block_page(resp, stock_code):
            _fg_note_block()
            return best_dm
        # 정상 응답 → 차단 카운터 리셋
        _fg_note_ok()
        # 정상 파싱 시도
        dm_attempt = _parse_fnguide_response(resp, stock_name)
        # 27/28 모두 채워졌으면 종료
        op = dm_attempt.get('영업이익', {}) or {}
        rv = dm_attempt.get('매출액', {}) or {}
        has_27_28 = (op.get(2027) is not None or rv.get(2027) is not None) and \
                    (op.get(2028) is not None or rv.get(2028) is not None)
        # 더 많은 데이터가 들어온 결과를 유지
        cur_keys = sum(len(v) for v in dm_attempt.values())
        best_keys = sum(len(v) for v in best_dm.values())
        if cur_keys > best_keys:
            best_dm = dm_attempt
        if has_27_28:
            return best_dm
        # 27/28 비어 있으면 한 번 더 (lite 응답 가능성)
        if attempt < _max_retries:
            time.sleep(0.4 * attempt)
    return best_dm


def _parse_fnguide_response(resp, stock_name=''):
    """FnGuide 응답을 dm={매출액:{년도:값}, 영업이익:{...}} 형태로 반환."""
    if resp is None:
        return {}
    try:
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'lxml')
        page_name = ''
        for tag in [soup.find('h1', class_='giName'), soup.find('title')]:
            if tag:
                page_name = tag.get_text(strip=True)
                break
        # 종목명 정규화 비교: 공백·non-breaking-space(U+00A0)·대소문자 무시
        # (LS ELECTRIC 같은 종목은 페이지 제목에 \xa0가 끼어있어서 단순 in 비교는 실패)
        def _norm(s):
            return ''.join(str(s or '').split()).upper()
        sn = _norm(stock_name); pn = _norm(page_name)
        if sn and pn and sn not in pn and pn not in sn:
            return {}
        tables = soup.find_all('table')
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
    except Exception:
        return {}


def scrape_naver_per_pbr_roe(stock_code):
    """네이버 증권에서 PER, PBR, ROE를 크롤링한다."""
    result = {}
    try:
        session = get_session()
        resp = session.get(f"https://finance.naver.com/item/main.naver?code={stock_code}", timeout=7)
        resp.encoding = 'utf-8'
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, 'lxml')

        aside = soup.find('div', class_='aside_invest_info')
        if aside:
            for em_tag in aside.find_all('em'):
                txt = em_tag.get_text(strip=True)
                parent_text = em_tag.parent.get_text(strip=True) if em_tag.parent else ''
                if 'PER' in parent_text and 'PER' not in result:
                    val = parse_numeric(txt)
                    if pd.notna(val): result['PER'] = round(val, 2)
                if 'PBR' in parent_text and 'PBR' not in result:
                    val = parse_numeric(txt)
                    if pd.notna(val): result['PBR'] = round(val, 2)

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

        cop = soup.find('div', class_='section cop_analysis')
        if cop:
            table = cop.find('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if cells:
                        label = cells[0].get_text(strip=True)
                        if 'ROE' in label:
                            for cell in reversed(cells[1:]):
                                val = parse_numeric(cell.get_text(strip=True))
                                if pd.notna(val):
                                    result['ROE'] = round(val, 2)
                                    break
                        elif '부채비율' in label and '부채비율' not in result:
                            for cell in reversed(cells[1:]):
                                val = parse_numeric(cell.get_text(strip=True))
                                if pd.notna(val):
                                    result['부채비율'] = round(val, 2)
                                    break
    except:
        pass
    return result


def get_daily_pv(stock_code, n_pages=2):
    """네이버 일별 시세에서 종가/거래량 시계열을 반환한다 (최신순)."""
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
    """OBV 추세 + RSI(14) Wilder's smoothing. 입력은 최신순."""
    if len(prices) < period + 1:
        return {}
    p = list(reversed(prices))
    v = list(reversed(volumes))
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
    obv_trend = 'up' if obv_change > 0 else ('down' if obv_change < 0 else 'flat')

    gains, losses = [], []
    for i in range(1, len(p)):
        d = p[i] - p[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    if len(gains) < period:
        return {'OBV_trend': obv_trend, 'RSI': np.nan}
    # Wilder 초기화 + 누적 EMA
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
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
    """최근 N일 종가에서 지지선/저항선 추정."""
    if not prices:
        return {}
    recent = prices[:lookback]
    return {'저항선': max(recent), '지지선': min(recent)}


def calc_ma_alignment(prices, periods=(5, 20, 60)):
    """이평선 정/역배열 판정."""
    if not prices or len(prices) < max(periods):
        return ''
    p = list(reversed(prices))
    mas = [sum(p[-n:]) / n for n in periods]
    if mas[0] > mas[1] > mas[2]: return 'up'
    if mas[0] < mas[1] < mas[2]: return 'down'
    return 'mixed'


def _ema(values, n):
    if not values: return []
    k = 2.0 / (n + 1.0)
    e = values[0]; out = [e]
    for v in values[1:]:
        e = v * k + e * (1.0 - k); out.append(e)
    return out


def calc_macd_signal(prices, fast=12, slow=26, sig=9):
    """MACD 상태."""
    if not prices or len(prices) < slow + sig: return ''
    p = list(reversed(prices))
    ema_f = _ema(p, fast); ema_s = _ema(p, slow)
    macd = [f - s for f, s in zip(ema_f, ema_s)]
    sigl = _ema(macd, sig)
    if len(macd) < 2: return ''
    if macd[-2] <= sigl[-2] and macd[-1] > sigl[-1]: return 'bull_cross'
    if macd[-2] >= sigl[-2] and macd[-1] < sigl[-1]: return 'bear_cross'
    if macd[-1] > sigl[-1]: return 'bull'
    if macd[-1] < sigl[-1]: return 'bear'
    return ''


def scrape_foreign_inst(stock_code):
    """네이버 외인·기관 일별 순매매 (단위: 주). 5일/20일 누적."""
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
            if not table: break
            page_added = 0
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 9: continue
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
                foreign_buys.append(f_val); inst_buys.append(i_val); page_added += 1
            if page_added == 0: break
        if foreign_buys:
            out['외인_5d']  = sum(foreign_buys[:5])
            out['외인_20d'] = sum(foreign_buys[:20])
            out['기관_5d']  = sum(inst_buys[:5])
            out['기관_20d'] = sum(inst_buys[:20])
    except:
        pass
    return out


def fetch_supplement_indicators(stock_code):
    """20일 평균 거래량 + OBV + RSI + 지지/저항선 + MA/MACD + 외인/기관 수급."""
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
            out['RSI']       = ind.get('RSI', np.nan)
        sr = calc_support_resistance(prices)
        out['저항선'] = sr.get('저항선', np.nan)
        out['지지선'] = sr.get('지지선', np.nan)
        out['MA_align']    = calc_ma_alignment(prices)
        out['MACD_signal'] = calc_macd_signal(prices)
    except:
        pass
    try:
        out.update(scrape_foreign_inst(stock_code))
    except:
        pass
    return out


def get_avg_volume_20d(stock_code):
    """레거시 호환 - 20일 평균 거래량만 반환."""
    return fetch_supplement_indicators(stock_code).get('평균거래량_20d', np.nan)


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
                    for yr in [2025, 2026, 2027, 2028]:
                        if (yr not in dm.get(mn, {}) or pd.isna(dm.get(mn, {}).get(yr))) \
                                and yr in fg[mn] and pd.notna(fg[mn][yr]):
                            if mn not in dm: dm[mn] = {}
                            dm[mn][yr] = fg[mn][yr]
        except:
            pass

        if not dm: return None

        ty = [2025, 2026, 2027, 2028]; by = [2024, 2025, 2026, 2027]
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

        # PER, PBR, ROE, 부채비율 크롤링
        try:
            indicators = scrape_naver_per_pbr_roe(stock_code)
            result['PER'] = indicators.get('PER', np.nan)
            result['PBR'] = indicators.get('PBR', np.nan)
            result['ROE'] = indicators.get('ROE', np.nan)
            result['부채비율'] = indicators.get('부채비율', np.nan)
        except:
            result['PER'] = np.nan
            result['PBR'] = np.nan
            result['ROE'] = np.nan
            result['부채비율'] = np.nan

        # 보조지표·수급 통합 수집
        try:
            sup = fetch_supplement_indicators(stock_code)
            for k in ['평균거래량_20d', 'OBV_trend', 'RSI', '저항선', '지지선',
                      'MA_align', 'MACD_signal',
                      '외인_5d', '외인_20d', '기관_5d', '기관_20d']:
                result[k] = sup.get(k, '' if k in ('OBV_trend','MA_align','MACD_signal') else np.nan)
        except:
            for k in ['평균거래량_20d', 'RSI', '저항선', '지지선',
                      '외인_5d', '외인_20d', '기관_5d', '기관_20d']:
                result[k] = np.nan
            for k in ['OBV_trend', 'MA_align', 'MACD_signal']:
                result[k] = ''

        return result
    except:
        return None


# ============================================================
# 컨센서스 스냅샷 저장 (Estimates Revision 분석용)
# ============================================================
def save_consensus_snapshot(df):
    """오늘자 컨센서스 추정치를 날짜별 JSON으로 저장."""
    if df is None or df.empty:
        return
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    today = now_kst().strftime('%Y-%m-%d')
    path = os.path.join(SNAPSHOT_DIR, f'{today}.json')
    cols = [f'{m}_{y}' for m in ('매출액', '영업이익') for y in (2025, 2026, 2027, 2028)]
    snap = {}
    for _, row in df.iterrows():
        code = str(row.get('종목코드', '')).zfill(6)
        if not code or code == '000000':
            continue
        entry = {}
        for c in cols:
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


# ============================================================
# 누적 기록 저장
# ============================================================
def save_history(df, min_vol=1000000):
    """크롤링 결과에서 거래량 100만 이상 종목을 날짜별로 누적 저장"""
    today_str = now_kst().strftime('%Y-%m-%d')
    history_file = os.path.join(HISTORY_DIR, "accumulation.json")

    history = {}
    if os.path.exists(history_file):
        with open(history_file, 'r', encoding='utf-8') as f:
            history = json.load(f)

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
        if pd.notna(rv28) and pd.notna(rv25) and rv25 > 0:
            pr = 1
        elif pd.notna(rv27) and pd.notna(rv25) and rv25 > 0:
            pr = 2
        elif pd.notna(rv26) and pd.notna(rv25) and rv25 > 0:
            pr = 3
        elif pd.notna(rv25) and pd.notna(rv24) and rv24 > 0:
            pr = 4
        return pr

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

        pr = calc_visibility(row)
        score = calc_scores(row)
        rev_max = row.get('매출액_최대성장률', np.nan)
        op_max = row.get('영업이익_최대성장률', np.nan)

        if pr <= 4:
            categories['미래가시성핵심성장'].append(name)
        if score > 0:
            categories['매출+영업이익환산점수'].append(name)
        if pd.notna(rev_max) and rev_max > 0:
            categories['매출1년최대성장률'].append(name)
        if pd.notna(op_max) and op_max > 0:
            categories['영업이익1년최대성장률'].append(name)

    for cat_name, stocks in categories.items():
        if cat_name not in history:
            history[cat_name] = {}
        history[cat_name][today_str] = stocks

    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print(f"  누적 기록 저장 완료: {today_str}")
    for cat_name, stocks in categories.items():
        print(f"    {cat_name}: {len(stocks)}개 종목")


# ============================================================
# 메인 크롤링 실행
# ============================================================
def main():
    MARKETS = ['KOSPI', 'KOSDAQ']
    MAX_WORKERS = 50

    print(f"[{now_kst()}] ▶ 크롤링 시작")

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


    # 1.5단계: 업종 매핑 수집
    print("1.5단계: 업종 데이터 수집...")
    sector_map = get_naver_sector_map()
    print(f"  업종 매핑 {len(sector_map)}개 종목")
    # sector_map.json 저장 (앱에서 즉시 사용 가능)
    sector_json_path = os.path.join(DATA_DIR, "sector_map.json")
    with open(sector_json_path, "w", encoding="utf-8") as _f:
        json.dump(sector_map, _f, ensure_ascii=False)
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
            # 거래량 폭증 배수 계산
            avg_vol = c.get('평균거래량_20d', np.nan)
            today_vol = rd['Recent_Volume']
            if pd.notna(avg_vol) and avg_vol > 0 and today_vol > 0:
                c['거래량배수'] = round(today_vol / avg_vol, 1)
            else:
                c['거래량배수'] = np.nan
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
    # 업종 데이터 매핑 (인덱스 정수 접두어 제거 후 6자리)
    df['업종'] = df['종목코드'].astype(str).str.zfill(6).map(sector_map).fillna('기타')

    # 3-A: 컨센서스 스냅샷은 '오늘 실제로 받은 값'을 정직하게 기록한다.
    #      (carry-forward 보강 전에 저장 → 보강 소스가 raw fetch로 유지되어
    #       stale 값이 무한히 전파되지 않음. staleness는 실제 fetch일 기준.)
    print("스냅샷 저장 (raw fetch)...")
    save_consensus_snapshot(df)

    # 3-B: FnGuide 간헐 차단으로 27E·28E가 NaN인 칸을 최근 스냅샷의
    #      마지막 좋은 값으로 보강한다 (consensus_persist.merge_carry_forward).
    try:
        from consensus_persist import merge_carry_forward
        df = merge_carry_forward(df, SNAPSHOT_DIR, today=now_kst().date())
    except Exception as e:
        print(f"[WARN] carry-forward 실패(무시): {e}")

    df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')

    meta = {
        'timestamp': now_kst().isoformat(),
        'markets': MARKETS,
        'total_analyzed': total,
        'data_count': len(results),
    }
    with open(META_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[{now_kst()}] ✅ 데이터 저장 완료! {len(results)}개 → {CSV_FILE}")

    # 4단계: 누적 기록 저장 (보강된 df 사용)
    print("4단계: 누적 기록 저장...")
    save_history(df)

    print(f"[{now_kst()}] ✅ 전체 완료!")


if __name__ == '__main__':
    main()
