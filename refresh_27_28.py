#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
집 PC(한국 IP) 전용 — 27E/28E 컨센서스 FnGuide 자가적응 갱신
=================================================================
[배경 — 2026-07 페이블 재진단으로 확정]
- 27E/28E 연간 컨센서스는 FnGuide(comp.fnguide.com)에만 존재.
  (네이버/wisereport = 26E까지, 퀀트킹 CSV export = 26년1Q(E)까지)
- FnGuide는 '지역 차단'이 아니라 **누적 요청 기반 소프트 봇차단**:
  한 IP에서 요청이 쌓이면 gicode를 무시하고 기본종목(삼성전자,
  defaultCompanyCode=005930) 페이지를 반환한다. requests든 실제
  브라우저(Playwright)든 동일 → 도구 문제가 아니라 IP 상태 문제.
- 한번 차단되면 쿨다운이 김(시간~하루). 따라서 **차단에 부딪히기 전에
  스스로 멈추는 것**이 핵심 전략이다.

[동작 — 자가 적응]
  1. 시작 시 git pull (GitHub Actions가 푸시한 최신 데이터/코드 동기화)
  2. 학습 파일(.fnguide_stats.json, 로컬 전용)에서 이 PC의 안전 한도 로드
  3. 갱신 대상: 27/28 보유 종목 중 최근 7일 내 fresh 못 받은 것, stale 순
  4. 랜덤 지터 간격으로 직렬 요청, **안전 한도에서 자발적 중단** (IP 청정 유지)
     - 차단 감지 시: 즉시 중단 + 한도를 70%로 하향 학습
     - 무차단 완주 시: 한도를 15% 상향 학습 (자동 튜닝)
  5. fresh → 오늘 스냅샷 저장 → carry-forward로 CSV 보강 → git push

사용: 27_28_갱신.bat 더블클릭. 매일 1~2회 돌리면 전체 종목이 순환 갱신됨.
옵션: --no-push (push 생략) / --quota N (이번 실행 요청 수 강제)
"""
import sys, os, time, json, random, subprocess, glob

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
STATS_FILE = os.path.join(HERE, '.fnguide_stats.json')   # 로컬 전용(.gitignore)

NO_PUSH = '--no-push' in sys.argv
FORCE_QUOTA = None
for _i, _a in enumerate(sys.argv):
    if _a == '--quota' and _i + 1 < len(sys.argv):
        try:
            FORCE_QUOTA = int(sys.argv[_i + 1])
        except ValueError:
            pass

INITIAL_QUOTA = 120        # 첫 실행 요청 수 (보수적 시작 → 이후 자동 학습)
QUOTA_MIN, QUOTA_MAX = 30, 800
INTERVAL_LO, INTERVAL_HI = 1.2, 2.6   # 요청 간격 랜덤 지터(초)
MAX_CONSEC_BLOCK = 3       # 연속 차단 N회 = IP 한도 도달 → 즉시 손 뗌
FRESH_WINDOW_DAYS = 7      # 최근 N일 내 fresh 받은 종목은 스킵 (순환 갱신)


def load_stats():
    try:
        return json.load(open(STATS_FILE, encoding='utf-8'))
    except Exception:
        return {}


def save_stats(st):
    try:
        json.dump(st, open(STATS_FILE, 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
    except Exception:
        pass


def is_block(resp, code):
    """차단 = gicode 무시하고 삼성전자 기본 페이지 반환 (title로 판별)."""
    if resp is None:
        return True
    try:
        t = BeautifulSoup(resp.text, 'lxml').find('title')
        name = t.get_text(strip=True) if t else ''
        return ('삼성전자' in name) and (code != '005930')
    except Exception:
        return False


def recent_fresh_codes(days=FRESH_WINDOW_DAYS):
    """최근 N일 스냅샷에서 27/28을 fresh로 받은 종목코드 집합."""
    codes = set()
    cutoff = (now_kst().date() - pd.Timedelta(days=days)).isoformat()
    for path in glob.glob(os.path.join(SNAPSHOT_DIR, '*.json')):
        d = os.path.basename(path).replace('.json', '')
        if d < cutoff:
            continue
        try:
            snap = json.load(open(path, encoding='utf-8'))
        except Exception:
            continue
        for c, e in snap.items():
            if e.get('영업이익_2027') is not None or e.get('영업이익_2028') is not None:
                codes.add(c)
    return codes


def git(*args, check=False):
    return subprocess.run(['git', *args], cwd=HERE, capture_output=True, text=True)


def main():
    today = now_kst().strftime('%Y-%m-%d')

    # 0) 최신 동기화 — Actions가 매일 푸시하므로 시작 전 pull이 충돌을 예방
    if not NO_PUSH:
        p = git('pull', '--rebase', 'origin', 'main')
        print(('🔄 git pull: ' + (p.stdout or p.stderr).strip().splitlines()[-1])
              if (p.stdout or p.stderr).strip() else '🔄 git pull 완료')

    if not os.path.exists(CSV_FILE):
        print('❌ consensus_data.csv 가 없습니다. 먼저 크롤이 한 번 돌아야 합니다.')
        return

    # 1) 이 PC의 학습된 안전 한도
    stats = load_stats()
    quota = FORCE_QUOTA or int(stats.get('safe_limit', INITIAL_QUOTA))
    quota = max(QUOTA_MIN, min(QUOTA_MAX, quota))
    hist = stats.get('history', [])

    df = pd.read_csv(CSV_FILE, dtype={'종목코드': str})
    df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
    op27 = pd.to_numeric(df.get('영업이익_2027'), errors='coerce')
    op28 = pd.to_numeric(df.get('영업이익_2028'), errors='coerce')

    snap_path = os.path.join(SNAPSHOT_DIR, f'{today}.json')
    snap = {}
    if os.path.exists(snap_path):
        try:
            snap = json.load(open(snap_path, encoding='utf-8'))
        except Exception:
            snap = {}

    # 2) 갱신 대상: 27/28 보유(과거 포함) 종목 - 최근 7일 fresh 제외, stale 순
    target = df[op27.notna() | op28.notna()].copy()
    if '컨센_보강일' in target.columns:
        target['_asof'] = target['컨센_보강일'].fillna('9999-99-99').replace('', '9999-99-99')
    else:
        target['_asof'] = '9999-99-99'
    target['_mc'] = pd.to_numeric(target['시가총액'], errors='coerce').fillna(0)
    target = target.sort_values(['_asof', '_mc'], ascending=[True, False])

    fresh_recent = recent_fresh_codes()
    target = target[~target['종목코드'].isin(fresh_recent)].reset_index(drop=True)

    print(f'📋 대상 {len(target)}개 (최근 {FRESH_WINDOW_DAYS}일 fresh {len(fresh_recent)}개 제외, stale 순)')
    print(f'🎚️  이번 실행 요청 한도: {quota}개 (학습된 안전선'
          + (f', 직전 기록 {len(hist)}회)' if hist else ', 초기값)'))
    print(f'   간격 {INTERVAL_LO}~{INTERVAL_HI}s 랜덤 · 연속차단 {MAX_CONSEC_BLOCK}회 시 즉시 중단\n')

    if target.empty:
        print('✅ 갱신할 종목이 없습니다 (전부 최근 fresh).')
        return

    session = requests.Session()
    session.headers.update(HEADERS)

    got, tried, consec = 0, 0, 0
    blocked = False
    for _, row in target.iterrows():
        if tried >= quota:
            print(f'🛑 요청 한도 {quota}개 도달 → 자발적 중단 (IP 청정 유지).')
            print('   몇 시간 뒤 다시 실행하면 이어서 받습니다.')
            break
        code, name = row['종목코드'], row['종목명']
        url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}'
        tried += 1
        try:
            r = session.get(url, headers=FG_REFERER, timeout=(5, 15))
        except Exception:
            r = None

        if is_block(r, code):
            consec += 1
            if tried <= 2 and consec == tried:
                # 시작부터 차단 = 이 IP는 아직 쿨다운 중 → 더 찌르지 않음
                print('⛔ 시작부터 차단 상태입니다. 이 IP는 쿨다운 중입니다.')
                print('   몇 시간 후(가급적 다음 날) 다시 실행해주세요.')
                blocked = True
                break
            if consec >= MAX_CONSEC_BLOCK:
                print(f'⛔ 연속 {MAX_CONSEC_BLOCK}회 차단 → 한도 도달, 즉시 중단.')
                blocked = True
                break
            time.sleep(random.uniform(INTERVAL_HI, INTERVAL_HI * 2))
            continue
        consec = 0

        dm = _parse_fnguide_response(r, name)
        op = dm.get('영업이익', {}) or {}
        rv = dm.get('매출액', {}) or {}
        if op.get(2027) is not None or op.get(2028) is not None \
                or rv.get(2027) is not None or rv.get(2028) is not None:
            entry = snap.get(code, {})
            for m, src in (('매출액', rv), ('영업이익', op)):
                for y in (2025, 2026, 2027, 2028):
                    if src.get(y) is not None:
                        entry[f'{m}_{y}'] = float(src[y])
            snap[code] = entry
            got += 1
            if got % 25 == 0:
                print(f'   ...{got}개 fresh (요청 {tried}/{quota})')
        time.sleep(random.uniform(INTERVAL_LO, INTERVAL_HI))

    # 3) 한도 학습 업데이트
    run_rec = {'date': today, 'tried': tried, 'got': got, 'blocked': blocked,
               'quota': quota}
    hist.append(run_rec)
    stats['history'] = hist[-30:]
    if blocked and tried > 2:
        stats['safe_limit'] = max(QUOTA_MIN, int(tried * 0.7))
        print(f'📉 한도 학습: 차단 지점 {tried} → 다음 안전선 {stats["safe_limit"]}')
    elif not blocked and tried >= quota:
        stats['safe_limit'] = min(QUOTA_MAX, int(quota * 1.15))
        print(f'📈 한도 학습: 무차단 완주 → 다음 안전선 {stats["safe_limit"]}')
    save_stats(stats)

    # 4) 스냅샷 저장
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    json.dump(snap, open(snap_path, 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'\n✅ fresh 27/28: {got}개 (요청 {tried}개) → 스냅샷({today}) 저장')

    if got == 0:
        print('   받은 게 없어 CSV/push 생략.')
        return

    # 5) CSV carry-forward 보강
    try:
        from consensus_persist import merge_carry_forward
        df2 = merge_carry_forward(df, SNAPSHOT_DIR, today=now_kst().date())
        df2.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        n28 = pd.to_numeric(df2['영업이익_2028'], errors='coerce').notna().sum()
        print(f'✅ CSV 보강 — 28E 보유 {n28}개')
    except Exception as e:
        print(f'[WARN] CSV 보강 실패: {e}')

    # 6) git push
    if NO_PUSH:
        print('ℹ️  --no-push: push 생략')
        return
    try:
        git('pull', '--rebase', 'origin', 'main')
        git('add', 'data/')
        c = git('commit', '-m', f'집 PC 27/28 fresh 갱신: {got}개 ({today})')
        if c.returncode != 0 and 'nothing to commit' in (c.stdout + c.stderr):
            print('ℹ️  변경 없음')
            return
        p = git('push', 'origin', 'main')
        if p.returncode == 0:
            print('✅ git push 완료')
        else:
            print(f'[WARN] push 실패 — 수동 push 필요: {p.stderr.strip()[:200]}')
    except Exception as e:
        print(f'[WARN] git 오류: {e}')


if __name__ == '__main__':
    main()
