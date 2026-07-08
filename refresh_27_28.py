#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수동 27E/28E 컨센서스 갱신 (백업용 — 평소엔 GitHub Actions가 자동 수행)
=================================================================
[2026-07-08 진실 확정 — 페이블 재진단]
'차단'은 처음부터 없었다. FnGuide가 2026-06-22 사이트를 개편하면서
구 URL(comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=)이 죽고, 어떤
gicode를 넣어도 기본종목(삼성전자) 페이지가 나왔던 것. 신규 URL은
  https://wcomp.fnguide.com/CompanyInfo/Consensus?cmp_cd={6자리}
이며 crawl_script.scrape_fnguide_supplement 가 이걸 쓰도록 교체됐다.
→ GitHub Actions 자동 크롤이 27/28E를 다시 받는다. 이 스크립트는
  자동 크롤 실패 시 수동 보충용 백업이다.

[동작]
  1. 시작 시 git pull → 대상: 27/28 보유 종목 중 최근 7일 fresh 없는 것
  2. scrape_fnguide_supplement(신규 엔드포인트)로 직렬 수집
  3. 연속 실패 시 중단(사이트 장애 대비), 한도 학습(스로틀 재등장 대비)
  4. fresh → 오늘 스냅샷 → carry-forward로 CSV 보강 → git push

사용: 27_28_갱신.bat 더블클릭
옵션: --no-push (push 생략) / --quota N (요청 수 강제)
"""
import sys, os, time, json, random, subprocess, glob

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import pandas as pd
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from crawl_script import scrape_fnguide_supplement, CSV_FILE, SNAPSHOT_DIR, now_kst

STATS_FILE = os.path.join(HERE, '.fnguide_stats.json')   # 로컬 전용(.gitignore)

NO_PUSH = '--no-push' in sys.argv
FORCE_QUOTA = None
for _i, _a in enumerate(sys.argv):
    if _a == '--quota' and _i + 1 < len(sys.argv):
        try:
            FORCE_QUOTA = int(sys.argv[_i + 1])
        except ValueError:
            pass

INITIAL_QUOTA = 700        # 신규 엔드포인트는 스로틀 미확인 → 사실상 전량, 문제 시 학습으로 하향
QUOTA_MIN, QUOTA_MAX = 30, 800
INTERVAL_LO, INTERVAL_HI = 0.4, 1.0   # 요청 간격 랜덤 지터(초)
MAX_CONSEC_BLOCK = 3       # (실패 판정 배수 기준으로 사용)
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
    if FORCE_QUOTA:
        quota = FORCE_QUOTA          # 강제값은 클램프하지 않음
    else:
        quota = int(stats.get('safe_limit', INITIAL_QUOTA))
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

    got, tried, consec = 0, 0, 0
    blocked = False
    for _, row in target.iterrows():
        if tried >= quota:
            print(f'🛑 요청 한도 {quota}개 도달 → 자발적 중단.')
            print('   다시 실행하면 이어서 받습니다.')
            break
        code, name = row['종목코드'], row['종목명']
        tried += 1
        # 신규 엔드포인트 (wcomp.fnguide.com/CompanyInfo/Consensus)
        dm = scrape_fnguide_supplement(code, name)
        op = dm.get('영업이익', {}) or {}
        rv = dm.get('매출액', {}) or {}

        if op.get(2027) is not None or op.get(2028) is not None \
                or rv.get(2027) is not None or rv.get(2028) is not None:
            consec = 0
            entry = snap.get(code, {})
            for m, src in (('매출액', rv), ('영업이익', op)):
                for y in (2025, 2026, 2027, 2028):
                    if src.get(y) is not None:
                        entry[f'{m}_{y}'] = float(src[y])
            snap[code] = entry
            got += 1
            if got % 25 == 0:
                print(f'   ...{got}개 fresh (요청 {tried}/{quota})')
        elif dm:
            # 페이지는 정상인데 27/28 컨센만 없음 = 커버리지 상실 (실패 아님)
            consec = 0
        else:
            # 완전 빈 결과 = 네트워크/404/종목명 불일치 → 연속되면 사이트 문제
            consec += 1
            if consec >= MAX_CONSEC_BLOCK * 2:
                print(f'⛔ 연속 {consec}회 완전 실패 → 사이트/네트워크 문제로 판단, 중단.')
                blocked = True
                break
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
