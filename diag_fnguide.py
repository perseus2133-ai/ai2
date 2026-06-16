#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""수정된 scrape_naver_consensus가 GitHub Actions에서 27/28을
실제로 채우는지 최종 검증."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from crawl_script import scrape_naver_consensus

TESTS = [('000660','SK하이닉스'),('005930','삼성전자'),('240810','원익IPS'),
         ('000270','기아'),('207940','삼성바이오로직스')]

print('=== 수정 후 최종 검증 (scrape_naver_consensus) ===')
for code, name in TESTS:
    r = scrape_naver_consensus(code, name)
    print(f'  {name}: 가용={r.get("데이터_가용성")} '
          f'op26={r.get("영업이익_2026")} op27={r.get("영업이익_2027")} op28={r.get("영업이익_2028")}')
