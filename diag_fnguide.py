#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FnGuide 재시도 전략 진단 v2 — 짧은 connect timeout + 다회 재시도의
성공률을 GitHub Actions(해외 IP)에서 측정."""
import sys, time
import requests

H = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'ko-KR,ko;q=0.9',
    'Referer': 'https://comp.fnguide.com/',
}

# 다양한 시총대 20종목 (성공률을 모집단으로 측정)
CODES = ['000660','005930','240810','000270','005380','035420','000810',
         '207940','068270','051910','006400','105560','055550','015760',
         '034730','009150','011200','032830','086790','138040']

def try_fnguide(code, connect_to=5, read_to=12, retries=6, backoff=0.4):
    url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
    sess = requests.Session()
    sess.headers.update(H)
    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, timeout=(connect_to, read_to))
            if r.status_code == 200 and 'Annual' in (r.text or ''):
                return True, attempt
        except Exception:
            pass
        time.sleep(backoff)
    return False, retries

print('=== FnGuide 재시도 전략 성공률 (connect=5s, read=12s, retries=6) ===')
ok = 0
t0 = time.time()
for code in CODES:
    success, att = try_fnguide(code)
    ok += int(success)
    print(f'  A{code}: {"OK @시도"+str(att) if success else "FAIL"}')
dt = time.time() - t0
print(f'\n성공: {ok}/{len(CODES)} ({ok/len(CODES)*100:.0f}%) / 총 {dt:.0f}초 / 종목당 {dt/len(CODES):.1f}초')
