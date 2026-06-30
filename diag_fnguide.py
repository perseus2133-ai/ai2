#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FnGuide 차단 한도 측정 — GitHub Actions IP의 상태를 진단.
A) cold: 시작 즉시 1개 (공유 IP가 이미 소진됐나)
B) slow: 6초 간격 8개 (천천히 하면 되나 = rate limit인가)
C) burst: 간격0 빠르게 15개 (몇 개째 막히나)
응답 title의 회사명이 요청 gicode와 다르면(삼성 고정) 차단."""
import sys, time
import requests
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8')

H = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
     'Accept-Language': 'ko-KR,ko;q=0.9', 'Referer': 'https://comp.fnguide.com/'}

POOL = [('000660','SK하이닉스'),('042700','한미반도체'),('012330','현대모비스'),
        ('035420','NAVER'),('000270','기아'),('207940','삼성바이오로직스'),
        ('068270','셀트리온'),('051910','LG화학'),('006400','삼성SDI'),
        ('373220','LG에너지솔루션'),('005380','현대차'),('000810','삼성화재'),
        ('105560','KB금융'),('055550','신한지주'),('015760','한국전력'),
        ('034730','SK'),('009150','삼성전기'),('011200','HMM'),
        ('032830','삼성생명'),('086790','하나금융지주')]

def check(code, expect):
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
    try:
        r = requests.get(url, headers=H, timeout=(5, 15))
        t = BeautifulSoup(r.text, 'lxml').find('title')
        name = t.get_text(strip=True)[:22] if t else ''
        blocked = ('삼성전자' in name) and (code != '005930')
        return ('정상' if not blocked else '차단'), name
    except Exception as e:
        return 'EXC', type(e).__name__

print('=== A) COLD: 시작 즉시 단일 요청 ===')
st, nm = check(*POOL[0])
print(f'  {POOL[0][1]} → {nm!r} [{st}]')
print('  → 차단이면 공유 IP가 이미 소진된 상태 (천천히도 무의미)\n')

print('=== B) SLOW: 6초 간격 8개 ===')
ok = 0
for code, exp in POOL[1:9]:
    st, nm = check(code, exp)
    ok += int(st == '정상')
    print(f'  {exp} → [{st}] {nm!r}')
    time.sleep(6)
print(f'  SLOW 정상: {ok}/8  → 대부분 정상이면 rate limit(천천히 하면 됨)\n')

print('=== C) BURST: 간격0 빠르게 ===')
ok = 0; first_block = None
for i, (code, exp) in enumerate(POOL[9:], 1):
    st, nm = check(code, exp)
    if st == '정상':
        ok += 1
    elif first_block is None:
        first_block = i
    print(f'  {i}. {exp} → [{st}]')
print(f'  BURST 정상: {ok}/{len(POOL[9:])}, 첫 차단: {first_block}번째')
print('\n=== 판정 ===')
print('COLD 차단 → IP 소진(self-hosted/로컬 필요) | COLD 정상+SLOW 정상 → 천천히 가능')
