#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기업개요(무엇을 하는 회사인지·주력 제품) 수집.

출처: 네이버 금융 종목분석(navercomp.wisereport.co.kr) 의 '기업개요' 항목
      → <li class="dot_cmp"> 문장들.

기업개요는 거의 바뀌지 않으므로 profiles.json 에 캐시하고,
새로 등장한 종목만 증분 수집한다 (매일 부하 거의 없음).
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
PROFILE_PATH = os.path.join(HERE, 'data', 'profiles.json')

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'ko-KR,ko;q=0.9',
    'Referer': 'https://finance.naver.com/',
}
_local = threading.local()


def _sess() -> requests.Session:
    if not hasattr(_local, 's'):
        _local.s = requests.Session()
        _local.s.headers.update(HEADERS)
    return _local.s


# 한 줄 요약 선택용 가중치 — '무엇을 파는 회사인가'를 말하는 문장을 우선하고,
# 설립·상장·인수 같은 연혁 문장은 뒤로 보낸다.
_POS = {'주력': 4, '주요 제품': 4, '주요제품': 4, '영위': 4, '주요 사업': 3, '주요사업': 3,
        '제조': 2, '생산': 2, '판매': 2, '공급': 2, '매출': 2, '부문': 2, '사업을': 2}
_NEG = {'설립': 3, '상장': 3, '인수': 2, '합병': 2, '분할': 2, '변경': 2, '전략': 2, '주력하고': 1}


def pick_summary(lines: list[str]) -> str:
    """문장들 중 사업 내용을 가장 잘 설명하는 한 줄을 고른다."""
    best, best_sc = '', -99
    for t in lines:
        sc = 0.0
        for k, w in _POS.items():
            if k in t:
                sc += w
        for k, w in _NEG.items():
            if k in t:
                sc -= w
        if re.match(r'^\s*\d{4}년', t):      # '2018년 …' 연혁 문장
            sc -= 2
        sc -= len(t) / 200                   # 지나치게 긴 문장은 살짝 감점
        if sc > best_sc:
            best, best_sc = t, sc
    return re.sub(r'^(동사는|동사가|당사는)\s*', '', best).strip()


def fetch_profile(code: str, retries: int = 2) -> dict | None:
    """{'summary': 한 줄 요약, 'lines': [문장들]} 또는 None."""
    url = (f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx'
           f'?cmp_cd={code}')
    for _ in range(retries):
        try:
            r = _sess().get(url, timeout=(4, 12))
            if r.status_code != 200:
                time.sleep(0.2)
                continue
            soup = BeautifulSoup(r.text, 'lxml')
            lis = soup.select('li.dot_cmp')
            lines = []
            for li in lis:
                t = re.sub(r'\s+', ' ', li.get_text(strip=True)).strip()
                if t and t not in lines:
                    lines.append(t)
            if not lines:
                return None
            return {'summary': pick_summary(lines), 'lines': lines[:4]}
        except Exception:
            time.sleep(0.2)
    return None


def load_profiles() -> dict:
    try:
        return json.load(open(PROFILE_PATH, encoding='utf-8'))
    except Exception:
        return {}


def save_profiles(p: dict) -> None:
    os.makedirs(os.path.dirname(PROFILE_PATH), exist_ok=True)
    json.dump(p, open(PROFILE_PATH, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=0, sort_keys=True)


def update_profiles(codes, workers: int = 20, verbose: bool = True) -> dict:
    """캐시에 없는 종목만 수집해 병합 후 저장."""
    prof = load_profiles()
    todo = [c for c in codes if c not in prof]
    if not todo:
        if verbose:
            print(f'  기업개요: 캐시 {len(prof)}건 (신규 없음)')
        return prof
    if verbose:
        print(f'  기업개요 신규 수집 {len(todo)}종목...')
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_profile, c): c for c in todo}
        for f in as_completed(futs):
            c = futs[f]
            try:
                r = f.result()
            except Exception:
                r = None
            prof[c] = r if r else {'summary': '', 'lines': []}
    save_profiles(prof)
    got = sum(1 for c in todo if prof.get(c, {}).get('summary'))
    if verbose:
        print(f'  기업개요: {got}/{len(todo)} 수집 ({time.time()-t0:.0f}초), 총 {len(prof)}건')
    return prof


if __name__ == '__main__':
    import sys
    codes = sys.argv[1:] or ['241710']
    for c in codes:
        print(c, fetch_profile(c))
