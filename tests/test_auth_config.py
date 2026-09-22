import pytest

from auth_config import configured_password


@pytest.mark.parametrize('value', ['', '  ', None, 123])
def test_no_default_password(monkeypatch, value):
    monkeypatch.delenv('APP_PASSWORD', raising=False)
    assert configured_password({'APP_PASSWORD': value}) == ''


def test_environment_takes_precedence(monkeypatch):
    monkeypatch.setenv('APP_PASSWORD', 'test-env-password')
    assert configured_password({'APP_PASSWORD': 'test-secret'}) == 'test-env-password'


def test_streamlit_secret(monkeypatch):
    monkeypatch.delenv('APP_PASSWORD', raising=False)
    assert configured_password({'APP_PASSWORD': 'test-secret'}) == 'test-secret'


def test_absent_secret_file_fails_closed(monkeypatch):
    class NoSecrets:
        def get(self, *args):
            raise FileNotFoundError('no secrets')
    monkeypatch.delenv('APP_PASSWORD', raising=False)
    assert configured_password(NoSecrets()) == ''


def test_app_without_password_stops_before_loading_data(monkeypatch):
    from pathlib import Path
    import requests
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv('APP_PASSWORD', '')
    def forbidden(*args, **kwargs):
        pytest.fail('Unauthenticated app must not fetch data')
    monkeypatch.setattr(requests.Session, 'request', forbidden)
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'app.py')).run(timeout=10)
    assert not app.exception
    assert len(app.error) == 1
    assert '비밀번호' in app.error[0].value
