#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
집 PC(한국 IP) 전용 — 27E/28E 컨센서스만 FnGuide에서 fresh 갱신
=================================================================
GitHub Actions(미국 Azure IP)는 FnGuide(comp.fnguide.com)가 지역 차단하여
27/28E를 못 받는다 (모든 요청에 '삼성전자 고정 페이지' 반환). 진단 확정됨.
27/28E는 FnGuide에만 존재하므로, 한국 IP인 집 PC에서 이 스크립트를
주기적으로(주 1~2회) 실행해 보충한다.

동작:
  1. 기존 CSV에서 27/28 컨센이 있던 종목을 시총 큰 순으로 갱신 대상 선정
  2. FnGuide를 '천천히'(기본 0.8초 간격) 직렬 호출
  3. 차단(삼성 고정 페이지) 감지 → 연속 N회면 한도 도달로 보고 즉시 중단
  4. 받은 fresh 27/28을 오늘 스냅샷에 저장 → carry-forward로 CSV 보강
  5. git pull→commit→push (--no-push 로 생략 가능)

사용: 27_28_갱신.bat 더블클릭  (또는  python refresh_27_28.py)
"""
import sys, os, time, json, subprocess

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from crawl_script import _parse_fnguide_response, CSV_FILE, SNAPSHOT_DIR, now_kst, HEADERS

FG_REFERER = {'Referer': 'https://comp.fnguide.com/'}
NO_PUSH = '--no-push' in sys.argv

# 요청 간격(초) — 기본 1.5s. FnGuide IP 한도가 낮고 한번 차단되면 쿨다운이
# 길어서(시간~하루) 보수적으로. `--interval 2.0` 처럼 조정 가능.
REQ_INTERVAL = 1.5
for _i, _a in enumerate(sys.argv):
    if _a == '--interval' and _i + 1 < len(sys.argv):
        try:
            REQ_INTERVAL = float(sys.argv[_i + 1])
        except ValueError:
            pass
MAX_CONSEC_BLOCK = 4     # 연속 차단 N회면 한도 도달로 보고 중단 (오염 최소화)


def is_block(resp, code):
    """응답이 '삼성 고정 페이지'(차단)인지 — title 회사명으로 판별."""
    if resp is None:
        return True
    try:
        t = BeautifulSoup(resp.text, 'lxml').find('title')
        name = t.get_text(strip=True) if t else ''
        return ('삼성전자' in name) and (code != '005930')
    except Exception:
        return False


def main():
    if not os.path.exists(CSV_FILE):
        print('❌ consensus_data.csv 가 없습니다. 먼저 크롤이 한 번 돌아야 합니다.')
        return

    df = pd.read_csv(CSV_FILE, dtype={'종목코드': str})
    df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
    op27 = pd.to_numeric(df.get('영업이익_2027'), errors='coerce')
    op28 = pd.to_numeric(df.get('영업이익_2028'), errors='coerce')

    today = now_kst().strftime('%Y-%m-%d')
    snap_path = os.path.join(SNAPSHOT_DIR, f'{today}.json')
    snap = {}
    if os.path.exists(snap_path):
        try:
            snap = json.load(open(snap_path, encoding='utf-8'))
        except Exception:
            snap = {}

    # 27 또는 28 컨센이 (과거에라도) 존재한 종목 = 갱신 대상
    target = df[op27.notna() | op28.notna()].copy()
    # 정렬: 가장 stale한 것(보강일 오래된 순) 먼저 → 매일 조금씩 돌려도 롤링 갱신.
    #        보강일 없으면(원래 fresh였던 것) 맨 뒤, 그 안에서는 시총 큰 순.
    if '컨센_보강일' in target.columns:
        target['_asof'] = target['컨센_보강일'].fillna('9999-99-99').replace('', '9999-99-99')
    else:
        target['_asof'] = '9999-99-99'
    target['_mc'] = pd.to_numeric(target['시가총액'], errors='coerce').fillna(0)
    target = target.sort_values(['_asof', '_mc'], ascending=[True, False]).reset_index(drop=True)

    # resume: 오늘 이미 fresh 받은 종목(오늘 스냅샷에 28E 존재)은 스킵
    already = {c for c, e in snap.items() if e.get('영업이익_2028') is not None
               or e.get('영업이익_2027') is not None}
    target = target[~target['종목코드'].isin(already)].reset_index(drop=True)

    print(f'📋 갱신 대상: {len(target)}개 (stale 오래된 순, 오늘 이미 받은 {len(already)}개 스킵)')
    print(f'   요청 간격 {REQ_INTERVAL}s · 연속 차단 {MAX_CONSEC_BLOCK}회 시 중단\n')

    session = requests.Session()
    session.headers.update(HEADERS)

    got = 0
    consec = 0
    tried = 0
    for _, row in target.iterrows():
        code = row['종목코드']
        name = row['종목명']
        url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
        tried += 1
        try:
            r = session.get(url, headers=FG_REFERER, timeout=(5, 15))
        except Exception:
            r = None

        if is_block(r, code):
            consec += 1
            if consec >= MAX_CONSEC_BLOCK:
                print(f'⛔ 연속 {MAX_CONSEC_BLOCK}회 차단 → IP 한도 도달로 판단, 중단.')
                print(f'   (이번에 받은 fresh: {got}개. 잠시 후 다시 실행하면 이어서 받습니다.)')
                break
            time.sleep(REQ_INTERVAL * 2)
            continue
        consec = 0

        dm = _parse_fnguide_response(r, name)
        op = dm.get('영업이익', {}) or {}
        rv = dm.get('매출액', {}) or {}
        if op.get(2027) is None and op.get(2028) is None:
            time.sleep(REQ_INTERVAL)
            continue

        entry = snap.get(code, {})
        for m, src in (('매출액', rv), ('영업이익', op)):
            for y in (2025, 2026, 2027, 2028):
                if src.get(y) is not None:
                    entry[f'{m}_{y}'] = float(src[y])
        if entry:
            snap[code] = entry
            got += 1
            if got % 25 == 0:
                print(f'   ...{got}개 fresh 수집 (시도 {tried}/{len(target)})')
        time.sleep(REQ_INTERVAL)

    # ── 스냅샷 저장 ──
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    json.dump(snap, open(snap_path, 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'\n✅ 스냅샷 저장: 오늘({today}) fresh 27/28 누적 {got}개')

    if got == 0:
        print('   받은 게 없습니다(차단 또는 갱신할 게 없음). CSV/push 생략.')
        return

    # ── CSV carry-forward 보강 (오늘 fresh 우선 반영) ──
    try:
        from consensus_persist import merge_carry_forward
        df2 = merge_carry_forward(df, SNAPSHOT_DIR, today=now_kst().date())
        df2.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        n28 = pd.to_numeric(df2['영업이익_2028'], errors='coerce').notna().sum()
        print(f'✅ CSV 보강 완료 — 28E 보유 종목 {n28}개')
    except Exception as e:
        print(f'[WARN] CSV 보강 실패(무시 가능): {e}')

    # ── git push ──
    if NO_PUSH:
        print('ℹ️  --no-push: git push 생략 (테스트 모드)')
        return
    try:
        subprocess.run(['git', 'pull', '--rebase', 'origin', 'main'], cwd=HERE)
        subprocess.run(['git', 'add', 'data/'], cwd=HERE)
        msg = f'집 PC 27/28 fresh 갱신: {got}개 ({today})'
        c = subprocess.run(['git', 'commit', '-m', msg], cwd=HERE,
                           capture_output=True, text=True)
        if c.returncode != 0 and 'nothing to commit' in (c.stdout + c.stderr):
            print('ℹ️  변경 없음 (push 생략)')
            return
        subprocess.run(['git', 'push', 'origin', 'main'], cwd=HERE, check=True)
        print('✅ git push 완료 — 다음 GitHub Actions 크롤이 이 값을 이어받습니다.')
    except Exception as e:
        print(f'[WARN] git push 실패 — 수동으로 push 해주세요: {e}')


if __name__ == '__main__':
    main()
