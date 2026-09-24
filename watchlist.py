"""Private watchlist storage and reproducible, non-LLM indicator commentary."""
import datetime as dt
import json
import math
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from zoneinfo import ZoneInfo


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def snapshot(row):
    return {str(k): (v if isinstance(v, (str, bool)) else number(v)) for k, v in dict(row).items()}


def opinion(row, candles):
    parts = []
    rows = candles.get('rows', []) if candles else []
    if rows:
        last = rows[-1]
        averages = [number(last.get(f'ma{n}')) for n in (5, 20, 60)]
        if all(v is not None for v in averages):
            trend = '정배열' if averages[0] > averages[1] > averages[2] else '역배열' if averages[0] < averages[1] < averages[2] else '혼조'
            parts.append(f"{last['date']} 종가 기준 5·20·60일선은 {trend}입니다.")
        prior = [number(r.get('volume')) for r in rows[-21:-1]]
        vol = number(last.get('volume'))
        if len(prior) == 20 and all(v is not None for v in prior) and sum(prior) > 0 and vol is not None:
            parts.append(f'최근 거래량은 직전 20거래일 평균의 {vol / (sum(prior) / 20):.2f}배입니다. 거래량 증가만으로 매집을 단정할 수 없습니다.')
    else:
        parts.append('일봉 자료가 없어 추세·거래량 판단을 보류합니다.')
    estimates = [(y, number(row.get(f'영업이익_{y}'))) for y in (2026, 2027, 2028)]
    available = [(y, v) for y, v in estimates if v is not None]
    if len(available) >= 2:
        y0, v0 = available[0]; y1, v1 = available[-1]
        parts.append(f'영업이익 전망은 {y0}년 {v0:,.0f}억원 → {y1}년 {v1:,.0f}억원입니다. 추정치의 실현 여부와 이익률을 확인하십시오.')
    else:
        parts.append('향후 실적 추정치가 부족하여 성장성 판단을 보류합니다.')
    debt = number(row.get('부채비율'))
    if debt is not None and debt >= 200:
        parts.append(f'부채비율 {debt:,.0f}%로 재무 부담을 추가 점검하십시오.')
    parts.append('뉴스·공시를 새로 읽은 생성형 AI 분석이 아닌 규칙 기반 해설입니다. 예상 적정가는 매수 목표가가 아니며 매매 권유가 아닙니다.')
    return '\n\n'.join(parts)


def make_entry(row, candles, data_as_of, now=None):
    now = now or dt.datetime.now(ZoneInfo('Asia/Seoul'))
    code = str(row.get('종목코드', '')).zfill(6)
    if not re.fullmatch(r'[0-9A-Z]{6}', code):
        raise ValueError('종목코드가 올바르지 않습니다.')
    rows = candles.get('rows', []) if candles else []
    valid = [r for r in rows if r['date'] < now.date().isoformat() and (number(r.get('close')) or 0) > 0]
    if not valid:
        raise ValueError('기준 거래일을 확인할 일봉이 없습니다. 차트가 복구된 후 등록해 주세요.')
    last = max(valid, key=lambda r: r['date'])
    return {'code': code, 'name': str(row.get('종목명', code)), 'selected_at': now.isoformat(),
            'price_date': last['date'], 'price': float(last['close']), 'data_as_of': str(data_as_of),
            'snapshot': snapshot(row), 'initial_opinion': opinion(row, candles)}


class WatchStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.execute('CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY, payload TEXT NOT NULL)')

    @contextmanager
    def connect(self):
        con = sqlite3.connect(str(self.path), timeout=10)
        try:
            with con:
                yield con
        finally:
            con.close()

    def entries(self):
        with self.connect() as con:
            return {code: json.loads(payload) for code, payload in con.execute('SELECT code,payload FROM watchlist ORDER BY rowid DESC')}

    def add(self, entry):
        self.validate(entry)
        with self.connect() as con:
            con.execute('INSERT OR IGNORE INTO watchlist VALUES (?,?)', (entry['code'], json.dumps(entry, ensure_ascii=False, allow_nan=False)))

    @staticmethod
    def validate(entry):
        if not isinstance(entry, dict) or not re.fullmatch(r'[0-9A-Z]{6}', str(entry.get('code', ''))):
            raise ValueError('관심목록 형식 오류')
        selected = dt.datetime.fromisoformat(entry['selected_at'])
        day = dt.date.fromisoformat(entry['price_date'])
        if selected.tzinfo is None or day >= selected.date() or (number(entry['price']) or 0) <= 0:
            raise ValueError('기준일 또는 기준 가격 오류')
        if not isinstance(entry['snapshot'], dict) or not isinstance(entry['initial_opinion'], str):
            raise ValueError('스냅샷 형식 오류')
        if not isinstance(entry['name'], str) or not isinstance(entry['data_as_of'], str):
            raise ValueError('기록 형식 오류')

    def remove(self, code):
        with self.connect() as con:
            con.execute('DELETE FROM watchlist WHERE code=?', (code,))

    def restore(self, raw):
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('entries'), list) or len(data['entries']) > 10000:
            raise ValueError('지원하지 않는 백업 형식입니다.')
        for entry in data['entries']:
            self.validate(entry)
        with self.connect() as con:
            for entry in data['entries']:
                con.execute('INSERT OR IGNORE INTO watchlist VALUES (?,?)', (entry['code'], json.dumps(entry, ensure_ascii=False, allow_nan=False)))

    def backup(self):
        return json.dumps({'version': 1, 'entries': list(self.entries().values())}, ensure_ascii=False, indent=2)
