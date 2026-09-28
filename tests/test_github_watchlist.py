import base64
import json

import pytest

from github_watchlist import GitHubWatchStore, GitHubWatchStoreError
from test_watchlist import entry


class Response:
    def __init__(self, status_code, data=None):
        self.status_code = status_code
        self.data = data or {}

    def json(self):
        return self.data


class FakeGitHub:
    def __init__(self):
        self.data = {'version': 1, 'entries': []}
        self.sha = 'sha-1'
        self.writes = 0
        self.conflict_entry = None
        self.status = None

    def request(self, method, url, *, headers, timeout, json=None):
        assert url == 'https://api.github.com/repos/perseus2133-ai/ai2-watchlist-data/contents/watchlist.json'
        assert headers['Authorization'] == 'Bearer test-token'
        assert timeout > 0
        if self.status:
            return Response(self.status)
        if method == 'GET':
            content = base64.b64encode(__import__('json').dumps(self.data).encode()).decode()
            return Response(200, {'sha': self.sha, 'encoding': 'base64', 'content': content})
        assert method == 'PUT'
        if self.conflict_entry is not None:
            self.data['entries'].append(self.conflict_entry)
            self.conflict_entry = None
            self.sha = 'sha-external'
            return Response(409)
        if json['sha'] != self.sha:
            return Response(409)
        self.data = __import__('json').loads(base64.b64decode(json['content']))
        self.writes += 1
        self.sha = f'sha-{self.writes + 1}'
        return Response(200)


def store(remote):
    return GitHubWatchStore('perseus2133-ai/ai2-watchlist-data', 'test-token', session=remote)


def test_add_and_remove_are_shared_across_store_instances():
    remote = FakeGitHub()
    home = store(remote)
    work = store(remote)
    selected = entry()

    home.add(selected)
    home.add(dict(selected, price=999))
    assert work.entries()['005930']['price'] == selected['price']
    assert remote.writes == 1

    work.remove('005930')
    assert home.entries() == {}
    assert remote.writes == 2


def test_conflicting_add_reloads_remote_and_preserves_both_stocks():
    remote = FakeGitHub()
    remote.conflict_entry = dict(entry(), code='000001')

    store(remote).add(entry())

    assert set(store(remote).entries()) == {'000001', '005930'}


def test_restore_rejects_invalid_input_without_writing_and_merges_valid_backup():
    remote = FakeGitHub()
    selected = entry()
    remote.data['entries'] = [selected]
    client = store(remote)

    with pytest.raises((ValueError, KeyError)):
        client.restore(json.dumps({'version': 1, 'entries': [dict(selected, code='bad')]}))
    assert remote.writes == 0

    client.restore(json.dumps({'version': 1, 'entries': [dict(selected, code='000001'), dict(selected, price=999)]}))
    assert set(client.entries()) == {'000001', '005930'}
    assert client.entries()['005930']['price'] == selected['price']


def test_restore_keeps_first_copy_of_duplicate_stock():
    remote = FakeGitHub()
    selected = entry()

    store(remote).restore(json.dumps({'version': 1, 'entries': [selected, dict(selected, price=999)]}))

    assert len(store(remote).entries()) == 1
    assert store(remote).entries()['005930']['price'] == selected['price']


@pytest.mark.parametrize('status', [401, 404, 500])
def test_unavailable_remote_never_looks_like_an_empty_watchlist(status):
    remote = FakeGitHub()
    remote.status = status

    with pytest.raises(GitHubWatchStoreError):
        store(remote).entries()
    assert remote.writes == 0


def test_streamlit_uses_configured_github_store(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import watchlist_ui as ui

    remote = FakeGitHub()
    monkeypatch.delenv('AI2_WATCHLIST_DB', raising=False)
    monkeypatch.setenv('AI2_WATCHLIST_GITHUB_REPO', 'perseus2133-ai/ai2-watchlist-data')
    monkeypatch.setenv('AI2_WATCHLIST_GITHUB_TOKEN', 'test-token')
    monkeypatch.setattr(ui, 'GitHubWatchStore', lambda repo, token: store(remote))
    launcher = "import sys\nsys.path.insert(0, 'tests')\nfrom test_watchlist import ui_app\nui_app()"

    at = AppTest.from_string(launcher, default_timeout=30).run()
    assert not at.exception
    next(button for button in at.button if button.label == '☆ 관심종목 등록').click().run()

    assert not at.exception
    assert set(store(remote).entries()) == {'005930'}


def test_surviving_server_database_is_migrated_only_once(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import watchlist_ui as ui
    from watchlist import WatchStore

    local = tmp_path / 'private_data' / 'watchlist.sqlite3'
    WatchStore(local).add(entry())
    remote = FakeGitHub()
    monkeypatch.delenv('AI2_WATCHLIST_DB', raising=False)
    monkeypatch.setenv('AI2_WATCHLIST_GITHUB_REPO', 'perseus2133-ai/ai2-watchlist-data')
    monkeypatch.setenv('AI2_WATCHLIST_GITHUB_TOKEN', 'test-token')
    monkeypatch.setattr(ui, 'GitHubWatchStore', lambda repo, token: store(remote))
    at = AppTest.from_string("import streamlit as st\nimport watchlist_ui as ui\nui.initialize(st.session_state['base'])")
    at.session_state['base'] = str(tmp_path)

    at.run()
    assert not at.exception
    assert set(store(remote).entries()) == {'005930'}

    remote.data['entries'] = []
    at.run()
    assert not at.exception
    assert store(remote).entries() == {}
