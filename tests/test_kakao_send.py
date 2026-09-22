# -*- coding: utf-8 -*-
"""카카오 알림 단계의 성공/실패 종료 코드 테스트."""

import kakao_send


def test_missing_credentials_is_clean_skip(monkeypatch):
    monkeypatch.setattr(kakao_send, 'REST_KEY', '')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', '')

    assert kakao_send.main() == 0


def test_refresh_token_failure_returns_error(monkeypatch):
    monkeypatch.setattr(kakao_send, 'REST_KEY', 'rest-key')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', 'refresh-token')
    monkeypatch.setattr(kakao_send, 'build_message', lambda: ('2026-09-23', 'message'))
    monkeypatch.setattr(kakao_send, 'refresh_access_token', lambda: None)

    assert kakao_send.main() == 1


def test_send_failure_returns_error(monkeypatch):
    monkeypatch.setattr(kakao_send, 'REST_KEY', 'rest-key')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', 'refresh-token')
    monkeypatch.setattr(kakao_send, 'build_message', lambda: ('2026-09-23', 'message'))
    monkeypatch.setattr(kakao_send, 'refresh_access_token', lambda: 'access-token')
    monkeypatch.setattr(kakao_send, 'send', lambda token, text: (False, {'error': 'failed'}))

    assert kakao_send.main() == 1


def test_send_success_returns_zero(monkeypatch):
    monkeypatch.setattr(kakao_send, 'REST_KEY', 'rest-key')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', 'refresh-token')
    monkeypatch.setattr(kakao_send, 'build_message', lambda: ('2026-09-23', 'message'))
    monkeypatch.setattr(kakao_send, 'refresh_access_token', lambda: 'access-token')
    monkeypatch.setattr(kakao_send, 'send', lambda token, text: (True, {'result_code': 0}))

    assert kakao_send.main() == 0
