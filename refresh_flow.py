#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""외인·기관 수급(외인_5d/20d, 기관_5d/20d)만 재수집해 기존 CSV에 채운다.

배경:
    scrape_foreign_inst()의 테이블 선택·열 인덱스 버그로 2619종목 전부
    수급이 NaN이었다(2026-09 발견·수정). 다음 새벽 크롤이면 자동으로
    채워지지만, 그때까지 기다리지 않고 지금 CSV를 메우기 위한 일회성
    도구다. 컨센서스·가격 등 다른 컬럼은 건드리지 않는다.

사용:
    python refresh_flow.py            # 전체
    python refresh_flow.py 50         # 앞 50종목만 (테스트)
"""
import os
import sys
import shutil
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

import crawl_script as cs

CSV = os.path.join('data', 'consensus_data.csv')
FLOW_COLS = ['외인_5d', '외인_20d', '기관_5d', '기관_20d']
WORKERS = 6


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None

    df = pd.read_csv(CSV, dtype={'종목코드': str})
    df['종목코드'] = df['종목코드'].str.zfill(6)
    codes = df['종목코드'].tolist()
    if limit:
        codes = codes[:limit]
    print(f'대상 {len(codes)}종목 / 워커 {WORKERS}', flush=True)

    results, done, fail = {}, 0, 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(cs.scrape_foreign_inst, c): c for c in codes}
        for fut in as_completed(futs):
            code = futs[fut]
            try:
                results[code] = fut.result()
            except Exception:
                fail += 1
                continue
            done += 1
            if done % 200 == 0:
                print(f'  {done}/{len(codes)} 완료', flush=True)

    got = sum(1 for v in results.values() if pd.notna(v.get('외인_5d')))
    print(f'수집 완료: {done}종목 (수급 확보 {got}, 실패 {fail})', flush=True)

    if got == 0:
        print('!! 확보 0건 — CSV를 건드리지 않고 종료합니다.')
        return 1

    backup = CSV.replace('.csv', f'.bak_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv')
    shutil.copy2(CSV, backup)
    print(f'백업: {backup}', flush=True)

    for col in FLOW_COLS:
        mapped = df['종목코드'].map(lambda c: results.get(c, {}).get(col))
        df[col] = mapped.combine_first(df[col])   # 새 값 우선, 실패분은 기존 유지

    df.to_csv(CSV, index=False, encoding='utf-8-sig')
    for col in FLOW_COLS:
        print(f'  {col}: 값있음 {int(df[col].notna().sum())}개')
    print('CSV 저장 완료')
    return 0


if __name__ == '__main__':
    sys.exit(main())
