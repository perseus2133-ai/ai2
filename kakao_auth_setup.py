#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
카카오 '나에게 보내기' 1회 인증 도우미 (로컬 실행 전용).

사전 준비 (developers.kakao.com — 카톡설정법.md 참고):
  1) 앱 생성 → [앱 키]에서 REST API 키 복사
  2) [카카오 로그인] 활성화 + Redirect URI에  https://localhost  등록
  3) [동의항목] → '카카오톡 메시지 전송(talk_message)' 선택 동의 설정

실행:  python kakao_auth_setup.py
  → 브라우저가 열리면 카카오 로그인/동의
  → 주소창이 https://localhost/?code=XXXX 로 바뀌면 그 code 값을 붙여넣기
  → 리프레시 토큰 출력 + 테스트 메시지 발송
"""
import sys
import json
import webbrowser

import requests

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

REDIRECT = 'https://localhost'
TOKEN_URL = 'https://kauth.kakao.com/oauth/token'


def verify_credentials(rest_key, client_secret):
    """브라우저 왕복 없이 자격증명(키+시크릿)만 즉시 검증.

    일부러 무효한 code를 보낸다:
      - 자격증명이 맞으면 code 검증 단계까지 가서 KOE320(code 오류)
      - 자격증명이 틀리면 그 전에 KOE010(Bad client credentials)
    실제 code를 소모하지 않으므로 몇 번이든 반복 가능.
    """
    payload = {
        'grant_type': 'authorization_code',
        'client_id': rest_key,
        'redirect_uri': REDIRECT,
        'code': 'CREDENTIAL_CHECK_ONLY',
    }
    if client_secret:
        payload['client_secret'] = client_secret
    try:
        j = requests.post(TOKEN_URL, data=payload, timeout=15).json()
    except Exception as e:
        return False, {'error': str(e)}
    return str(j.get('error_code') or '') != 'KOE010', j


def main():
    rest_key = input('REST API 키: ').strip()
    if not rest_key:
        print('REST API 키가 필요합니다.')
        return
    # [카카오 로그인] > [보안] 에서 Client Secret을 '사용함'으로 켠 경우 필수.
    # 안 켰으면 그냥 Enter (KOE010 에러의 주원인)
    client_secret = input('Client Secret (안 켰으면 그냥 Enter): ').strip()

    # ── 자격증명 사전 검증 (브라우저 열기 전에 빠르게 판별) ──
    print(f'\n🔎 자격증명 확인 중... (키 {len(rest_key)}자, 시크릿 {len(client_secret)}자)')
    ok, resp = verify_credentials(rest_key, client_secret)
    if not ok:
        print('\n❌ 자격증명이 거부됐습니다 (KOE010).')
        print('   아래를 확인하세요:')
        print('   1) REST API 키: [내 애플리케이션] > [앱 키] 의 "REST API 키" 값인지')
        print('      (JavaScript 키·네이티브 앱 키·어드민 키가 아님)')
        print('   2) Client Secret: [카카오 로그인] > [보안] 의 "카카오 로그인" 행 코드인지')
        print('      (아래쪽 "비즈니스 인증" 코드가 아님)')
        print('   3) 앞뒤 공백/줄바꿈이 섞이지 않았는지')
        print(f'   서버 응답: {resp}')
        again = input('\n다시 입력할까요? (y/Enter=종료): ').strip().lower()
        if again != 'y':
            return
        rest_key = input('REST API 키: ').strip()
        client_secret = input('Client Secret (없으면 Enter): ').strip()
        ok, resp = verify_credentials(rest_key, client_secret)
        if not ok:
            print(f'\n❌ 여전히 거부됩니다: {resp}')
            print('   → Client Secret을 OFF로 끄고 (활성화 토글) 다시 시도해보세요.')
            return
    print('✅ 자격증명 통과 — 이제 로그인 동의를 진행합니다.')

    auth_url = (f'https://kauth.kakao.com/oauth/authorize?response_type=code'
                f'&client_id={rest_key}&redirect_uri={REDIRECT}&scope=talk_message')
    print('\n브라우저에서 카카오 로그인/동의를 진행하세요...')
    print(f'(자동으로 안 열리면 직접 열기: {auth_url})\n')
    webbrowser.open(auth_url)

    print("동의 후 주소창이 'https://localhost/?code=...' 로 바뀝니다 (페이지는 에러여도 정상).")
    code = input('주소창의 code= 뒤 값 붙여넣기: ').strip()
    if 'code=' in code:
        code = code.split('code=')[-1].split('&')[0]

    payload = {
        'grant_type': 'authorization_code',
        'client_id': rest_key,
        'redirect_uri': REDIRECT,
        'code': code,
    }
    if client_secret:
        payload['client_secret'] = client_secret
    r = requests.post('https://kauth.kakao.com/oauth/token', data=payload, timeout=15)
    j = r.json()
    if 'refresh_token' not in j:
        print(f'\n❌ 토큰 발급 실패: {j}')
        ec = str(j.get('error_code') or '')
        if ec == 'KOE010':
            print('   → KOE010: REST API 키가 틀렸거나, Client Secret이 켜져 있는데')
            print('     입력하지 않은 경우입니다.')
            print('     [카카오 로그인] > [보안] > Client Secret 활성화 상태 확인 후')
            print('     "사용함"이면 그 코드를 두 번째 질문에 입력하세요.')
        elif ec == 'KOE320':
            print('   → KOE320: code가 만료됐거나 이미 사용됨. 스크립트를 다시 실행하세요.')
        else:
            print('   (Redirect URI 등록·동의항목 설정을 확인하세요)')
        return

    print('\n' + '=' * 55)
    print('✅ 발급 성공! GitHub Secrets에 등록하세요:')
    print(f'  KAKAO_REST_KEY      = {rest_key}')
    print(f'  KAKAO_REFRESH_TOKEN = {j["refresh_token"]}')
    if client_secret:
        print(f'  KAKAO_CLIENT_SECRET = {client_secret}')
    print('=' * 55)

    # 테스트 발송
    tpl = {'object_type': 'text', 'text': '🤖 ai2 카톡 연동 테스트 성공!',
           'link': {'web_url': 'https://github.com/perseus2133-ai/ai2',
                    'mobile_web_url': 'https://github.com/perseus2133-ai/ai2'},
           'button_title': '스크리너 열기'}
    t = requests.post('https://kapi.kakao.com/v2/api/talk/memo/default/send',
                      headers={'Authorization': f'Bearer {j["access_token"]}'},
                      data={'template_object': json.dumps(tpl, ensure_ascii=False)},
                      timeout=15).json()
    if t.get('result_code') == 0:
        print('📨 테스트 메시지를 보냈습니다 — 카톡 "나와의 채팅"을 확인하세요!')
    else:
        print(f'⚠️ 테스트 발송 실패: {t}')


if __name__ == '__main__':
    main()
