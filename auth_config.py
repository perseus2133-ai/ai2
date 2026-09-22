"""앱 접근 비밀번호. 공개 코드에 기본 비밀번호를 두지 않는다."""
import os


def configured_password(secrets):
    value = os.environ.get('APP_PASSWORD')
    if value is None:
        try:
            value = secrets.get('APP_PASSWORD', '')
        except (FileNotFoundError, KeyError):
            value = ''
    return value if isinstance(value, str) and value.strip() else ''
