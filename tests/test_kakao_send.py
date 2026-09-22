# -*- coding: utf-8 -*-
"""카카오 알림 단계의 성공/실패 종료 코드 테스트."""

import json
import subprocess
from types import SimpleNamespace

import pytest

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


@pytest.mark.parametrize('history,expected', [({'2026-09-22': []}, 1), ({'2026-09-23': []}, 0)])
def test_stale_picks_blocked_and_empty_today_skipped(tmp_path, monkeypatch, history, expected):
    path = tmp_path / 'picks.json'
    path.write_text(json.dumps(history), encoding='utf-8')
    monkeypatch.setattr(kakao_send, 'PICKS_PATH', str(path))
    monkeypatch.setattr(kakao_send, 'today_kst', lambda: '2026-09-23')
    monkeypatch.setattr(kakao_send, 'REST_KEY', 'test-key')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', 'test-token')
    def forbidden(*args, **kwargs):
        pytest.fail('Stale or empty picks must not make HTTP calls')
    monkeypatch.setattr(kakao_send.requests, 'post', forbidden)
    assert kakao_send.main() == expected


def test_token_rotation_uses_stdin_without_logging_secret(monkeypatch, capsys):
    new_token = 'fake-rotated-token-sensitive'
    monkeypatch.setattr(kakao_send, 'GH_PAT', 'fake-pat')
    monkeypatch.setattr(kakao_send, 'REFRESH_TOKEN', 'old-fake-token')
    monkeypatch.setattr(kakao_send.requests, 'post', lambda *a, **kw: SimpleNamespace(
        json=lambda: {'access_token': 'fake-access-token', 'refresh_token': new_token}))
    calls = []
    monkeypatch.setattr(kakao_send.subprocess, 'run', lambda args, **kw: calls.append((args, kw)))
    assert kakao_send.refresh_access_token() == 'fake-access-token'
    args, kwargs = calls[0]
    assert args == ['gh', 'secret', 'set', 'KAKAO_REFRESH_TOKEN']
    assert kwargs['input'] == new_token
    assert kwargs['env']['GH_TOKEN'] == 'fake-pat'
    assert new_token not in capsys.readouterr().out


@pytest.mark.parametrize('has_pat', [True, False])
def test_failed_rotation_never_exposes_new_secret(monkeypatch, capsys, has_pat):
    new_token = 'fake-new-token-sensitive'
    monkeypatch.setattr(kakao_send, 'GH_PAT', 'fake-pat' if has_pat else '')
    monkeypatch.setattr(kakao_send.requests, 'post', lambda *a, **kw: SimpleNamespace(
        json=lambda: {'access_token': 'fake-access-token', 'refresh_token': new_token}))
    def failed(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ['gh'], stderr=new_token)
    monkeypatch.setattr(kakao_send.subprocess, 'run', failed)
    with pytest.raises(RuntimeError) as error:
        kakao_send.refresh_access_token()
    assert new_token not in str(error.value)
    assert new_token not in capsys.readouterr().out
