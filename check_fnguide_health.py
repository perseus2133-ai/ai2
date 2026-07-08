#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FnGuide 수집 헬스체크 — GitHub Actions 마지막 단계에서 실행.

crawl_script.py가 남긴 data/fnguide_health.json 을 읽어 이상이면
exit 1 로 run 을 실패 처리한다. 데이터 push '이후'에 실행되므로
그날 네이버 데이터는 이미 안전하게 커밋된 상태에서 알림만 발생한다.

run 이 실패하면 GitHub 이 저장소 소유자에게 자동으로 실패 메일을
보내므로, 별도 알림 인프라 없이 '주소 변경/사이트 개편' 같은 이상을
당일 인지할 수 있다. (2026-06-22 FnGuide 개편으로 구 URL 이 죽은 것을
6일 뒤에야 사람이 눈치챈 사고의 재발 방지책.)

이상 판정:
  - 헬스 파일 없음        → 크롤이 저장 단계까지 못 감 (실패)
  - 파일 날짜 ≠ 오늘(KST) → 크롤 비정상 종료 추정 (실패)
  - ok == false           → fresh 27/28E 0건 = 엔드포인트 이상 (실패)
"""
import os
import sys
import json
import datetime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
HEALTH_FILE = os.path.join(HERE, 'data', 'fnguide_health.json')
KST = ZoneInfo('Asia/Seoul')


def main():
    today = datetime.datetime.now(KST).strftime('%Y-%m-%d')

    if not os.path.exists(HEALTH_FILE):
        print('❌ 헬스 파일 없음 — 크롤이 저장 단계까지 도달하지 못했습니다.')
        sys.exit(1)

    with open(HEALTH_FILE, encoding='utf-8') as f:
        h = json.load(f)
    print(f'헬스 파일: {json.dumps(h, ensure_ascii=False)}')

    if h.get('date') != today:
        print(f"❌ 헬스 파일이 오늘({today}) 것이 아닙니다 — 크롤 비정상 종료 추정.")
        sys.exit(1)

    if not h.get('ok'):
        print('❌ FnGuide fresh 27/28E = 0건 — 엔드포인트 이상(주소 변경/사이트 개편) 가능성.')
        probe = h.get('probe') or {}
        if probe:
            print(f'   진단 프로브: {json.dumps(probe, ensure_ascii=False)}')
            loc = probe.get('location')
            if loc:
                print(f'   ↪ 리다이렉트 목적지: {loc}   ← 새 주소 수사는 여기부터 시작')
        err = h.get('error')
        if err:
            print(f'   오류: {err}')
        print('   (앱 데이터는 carry-forward 로 45일간 유지되므로 급하지 않지만, 확인이 필요합니다.)')
        sys.exit(1)

    print(f"✅ FnGuide 정상: fresh 27E={h.get('fresh_27')} / 28E={h.get('fresh_28')}")


if __name__ == '__main__':
    main()
