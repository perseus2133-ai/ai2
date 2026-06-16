#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FnGuide 접근 진단 — GitHub Actions(미국 IP)에서 FnGuide가
응답하는지/차단되는지 확인하는 일회성 스크립트."""
import sys, time
import requests
from crawl_script import scrape_fnguide_supplement, scrape_naver_consensus

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'ko-KR,ko;q=0.9',
}

TESTS = [('000660', 'SK하이닉스'), ('005930', '삼성전자'), ('240810', '원익IPS')]

print('=' * 60)
print('1) FnGuide 원시 HTTP 응답 확인')
for code, name in TESTS:
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
    try:
        t0 = time.time()
        r = requests.get(url, headers=HEADERS, timeout=20)
        dt = time.time() - t0
        body = r.text or ''
        print(f'  {name}: status={r.status_code} len={len(body)} time={dt:.1f}s '
              f'has_giName={"giName" in body} has_Annual={"Annual" in body}')
    except Exception as e:
        print(f'  {name}: EXCEPTION {type(e).__name__}: {e}')

print('=' * 60)
print('2) scrape_fnguide_supplement() 결과')
for code, name in TESTS:
    fg = scrape_fnguide_supplement(code, name)
    op = fg.get('영업이익', {})
    print(f'  {name}: op2027={op.get(2027)} op2028={op.get(2028)} keys={sorted(op.keys())}')

print('=' * 60)
print('3) scrape_naver_consensus() 최종 결과')
for code, name in TESTS:
    r = scrape_naver_consensus(code, name)
    print(f'  {name}: 가용={r.get("데이터_가용성")} '
          f'op27={r.get("영업이익_2027")} op28={r.get("영업이익_2028")}')
print('=' * 60)
print('진단 완료')
