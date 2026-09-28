"""Durable watchlist storage in a dedicated GitHub repository.

Each write uses the current blob SHA. A conflicting write reloads the file and
reapplies the operation so separate browser sessions do not overwrite each other.
"""

import base64
import binascii
import json
import re

import requests

from watchlist import WatchStore


class GitHubWatchStoreError(RuntimeError):
    """The remote watchlist could not be read or saved."""


class GitHubWatchStore:
    def __init__(self, repository, token, session=None):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository or ''):
            raise ValueError('GitHub 저장소 이름은 owner/repo 형식이어야 합니다.')
        if not token:
            raise ValueError('GitHub 관심목록 저장 토큰이 설정되지 않았습니다.')
        self.url = f'https://api.github.com/repos/{repository}/contents/watchlist.json'
        self.session = session or requests.Session()
        self.headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {token}',
            'X-GitHub-Api-Version': '2022-11-28',
        }

    def _request(self, method, **kwargs):
        try:
            return self.session.request(method, self.url, headers=self.headers,
                                        timeout=15, **kwargs)
        except requests.RequestException as exc:
            raise GitHubWatchStoreError('GitHub 관심목록 저장소에 연결하지 못했습니다.') from exc

    @staticmethod
    def _validated_entries(data):
        if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('entries'), list) or len(data['entries']) > 10000:
            raise GitHubWatchStoreError('GitHub 관심목록 파일 형식이 올바르지 않습니다.')
        entries = data['entries']
        try:
            for entry in entries:
                WatchStore.validate(entry)
        except (ValueError, KeyError, TypeError) as exc:
            raise GitHubWatchStoreError('GitHub 관심목록에 잘못된 종목 기록이 있습니다.') from exc
        if len({entry['code'] for entry in entries}) != len(entries):
            raise GitHubWatchStoreError('GitHub 관심목록에 종목코드가 중복되어 있습니다.')
        return entries

    def _read(self):
        response = self._request('GET')
        if response.status_code != 200:
            raise GitHubWatchStoreError(f'GitHub 관심목록을 읽지 못했습니다 (HTTP {response.status_code}).')
        try:
            result = response.json()
            if result.get('encoding') != 'base64':
                raise ValueError('unsupported encoding')
            raw = base64.b64decode(result['content'], validate=False)
            entries = self._validated_entries(json.loads(raw.decode('utf-8')))
            return entries, result['sha']
        except (ValueError, KeyError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
            raise GitHubWatchStoreError('GitHub 관심목록 응답을 읽을 수 없습니다.') from exc

    def entries(self):
        items, _ = self._read()
        return {entry['code']: entry for entry in items}

    def _mutate(self, change, message):
        for _ in range(5):
            items, sha = self._read()
            if not change(items):
                return
            raw = json.dumps({'version': 1, 'entries': items}, ensure_ascii=False,
                             allow_nan=False, indent=2).encode('utf-8')
            response = self._request('PUT', json={
                'message': message,
                'content': base64.b64encode(raw).decode('ascii'),
                'sha': sha,
            })
            if response.status_code in (200, 201):
                return
            if response.status_code != 409:
                raise GitHubWatchStoreError(f'GitHub 관심목록을 저장하지 못했습니다 (HTTP {response.status_code}).')
        raise GitHubWatchStoreError('GitHub 관심목록이 동시에 수정되어 저장하지 못했습니다. 다시 시도해 주세요.')

    def add(self, entry):
        WatchStore.validate(entry)

        def change(items):
            if any(item['code'] == entry['code'] for item in items):
                return False
            items.insert(0, entry)
            return True

        self._mutate(change, f"Add watchlist stock {entry['code']}")

    def remove(self, code):
        def change(items):
            remaining = [item for item in items if item['code'] != code]
            if len(remaining) == len(items):
                return False
            items[:] = remaining
            return True

        self._mutate(change, f'Remove watchlist stock {code}')

    def restore(self, raw):
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('entries'), list) or len(data['entries']) > 10000:
            raise ValueError('지원하지 않는 백업 형식입니다.')
        incoming = data['entries']
        for entry in incoming:
            WatchStore.validate(entry)

        def change(items):
            present = {item['code'] for item in items}
            added = []
            for item in incoming:
                if item['code'] not in present:
                    added.append(item)
                    present.add(item['code'])
            if not added:
                return False
            items[:0] = added
            return True

        self._mutate(change, 'Restore watchlist backup')

    def backup(self):
        return json.dumps({'version': 1, 'entries': list(self.entries().values())},
                          ensure_ascii=False, indent=2)
