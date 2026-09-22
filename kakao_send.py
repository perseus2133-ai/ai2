#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 데일리 3선 → 카카오톡 '나에게 보내기' 전송.

GitHub Actions에서 크롤 후 실행. 필요한 환경변수(GitHub Secrets):
  KAKAO_REST_KEY      : 카카오 개발자 앱 REST API 키
  KAKAO_REFRESH_TOKEN : 1회 발급한 리프레시 토큰 (kakao_auth_setup.py 로 발급)
  APP_URL (선택)       : 메시지 버튼이 열 주소 (기본: GitHub 저장소)
  GH_PAT (선택)        : 리프레시 토큰이 갱신 발급될 때 시크릿 자동 업데이트용
                        (repo secrets 쓰기 권한 PAT. 갱신 시 없으면 실패 처리)

카카오 토큰 정책: refresh_token 유효 2개월. 만료 1개월 전 갱신 요청 시
새 refresh_token이 응답에 포함됨 → GH_PAT 있으면 시크릿 자동 갱신,
없으면 실패 처리(로컬 재인증 후 수동 교체 필요). 토큰 값은 로그에 남기지 않는다.
"""
import os
import sys
import json
import datetime
import subprocess
from zoneinfo import ZoneInfo

import requests

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
PICKS_PATH = os.path.join(HERE, 'data', 'daily_picks.json')

REST_KEY = os.environ.get('KAKAO_REST_KEY', '').strip()
REFRESH_TOKEN = os.environ.get('KAKAO_REFRESH_TOKEN', '').strip()
CLIENT_SECRET = os.environ.get('KAKAO_CLIENT_SECRET', '').strip()
APP_URL = os.environ.get('APP_URL', '').strip() or 'https://github.com/perseus2133-ai/ai2'
GH_PAT = os.environ.get('GH_PAT', '').strip()


def today_kst():
    return datetime.datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat()


def refresh_access_token():
    """리프레시 토큰 → 액세스 토큰. 새 리프레시 토큰이 오면 시크릿 갱신 시도."""
    payload = {
        'grant_type': 'refresh_token',
        'client_id': REST_KEY,
        'refresh_token': REFRESH_TOKEN,
    }
    if CLIENT_SECRET:   # [카카오 로그인]>[보안] Client Secret '사용함'이면 필수
        payload['client_secret'] = CLIENT_SECRET
    r = requests.post('https://kauth.kakao.com/oauth/token', data=payload, timeout=15)
    j = r.json()
    if 'access_token' not in j:
        print('❌ 토큰 갱신 실패 (응답에 비밀값이 포함될 수 있어 본문 생략)')
        if str(j.get('error_code')) == 'KOE322':
            print('   → 리프레시 토큰이 만료됐습니다. python kakao_auth_setup.py 로 재발급하세요.')
        return None
    new_rt = j.get('refresh_token')
    if new_rt and new_rt != REFRESH_TOKEN:
        print('ℹ️ 카카오가 새 리프레시 토큰을 발급했습니다.')
        if GH_PAT:
            try:
                env = dict(os.environ, GH_TOKEN=GH_PAT)
                subprocess.run(['gh', 'secret', 'set', 'KAKAO_REFRESH_TOKEN'],
                               input=new_rt, env=env, check=True,
                               capture_output=True, text=True, timeout=30)
                print('✅ KAKAO_REFRESH_TOKEN 시크릿 자동 갱신 완료')
            except Exception:
                raise RuntimeError('리프레시 토큰 저장 실패 — 로컬 재인증 후 Secret 교체 필요') from None
        else:
            raise RuntimeError('GH_PAT 미설정 — 새 토큰을 저장하지 못했습니다. 로컬 재인증 후 Secret 교체 필요')
    return j['access_token']


def build_message():
    """오늘 생성된 픽만 전송. 과거 추천의 재전송을 금지한다."""
    with open(PICKS_PATH, encoding='utf-8') as f:
        hist = json.load(f)
    latest = today_kst()
    if latest not in hist:
        raise ValueError('오늘의 AI 선정 기록 없음 — 오래된 추천 전송 중단')
    picks = hist[latest] or []
    if not picks:
        return None, None

    def line(mkt, label):
        ps = [p for p in picks if p.get('market') == mkt]
        if not ps:
            return ''
        names = ' / '.join(f"{p['name']} {p.get('score', '')}" for p in ps)
        return f'[{label}] {names}'

    d = latest[5:].replace('-', '/')          # 'MM/DD'
    txt = f'🤖 AI 데일리 3선 {d}\n{line("KOSPI", "코스피")}\n{line("KOSDAQ", "코스닥")}'
    if len(txt) > 197:
        txt = txt[:196] + '…'
    return latest, txt


def send(access_token, text):
    template = {
        'object_type': 'text',
        'text': text,
        'link': {'web_url': APP_URL, 'mobile_web_url': APP_URL},
        'button_title': '스크리너 열기',
    }
    r = requests.post(
        'https://kapi.kakao.com/v2/api/talk/memo/default/send',
        headers={'Authorization': f'Bearer {access_token}'},
        data={'template_object': json.dumps(template, ensure_ascii=False)},
        timeout=15)
    j = r.json()
    return j.get('result_code') == 0, j


def main():
    if not REST_KEY or not REFRESH_TOKEN:
        print('ℹ️ KAKAO_REST_KEY / KAKAO_REFRESH_TOKEN 미설정 — 카톡 전송 건너뜀')
        return 0
    try:
        date, txt = build_message()
        if not txt:
            print('ℹ️ 오늘 선정된 후보가 없습니다 — 건너뜀')
            return 0
        token = refresh_access_token()
        if not token:
            return 1
        ok, resp = send(token, txt)
    except Exception:
        print('❌ 알림 준비/인증/전송 실패 — 당일 데이터, 네트워크, Secret 설정을 확인하세요.')
        return 1
    if ok:
        print(f'✅ 카톡 전송 완료 ({date} 선정, {len(txt)}자)')
        return 0
    else:
        print('❌ 카톡 전송 실패 (응답 본문 생략)')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
