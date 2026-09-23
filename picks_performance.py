"""데일리 최초 선정 종목의 동일 기간 종가/시장 비교. 기존 선정 기록은 수정하지 않는다."""
import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

KST = ZoneInfo('Asia/Seoul')
MARKETS = ('KOSPI', 'KOSDAQ')
HORIZONS = ('latest', '5', '20', '60')
SOURCE = 'https://api.stock.naver.com/chart/domestic/'


def history_digest(history):
    payload = json.dumps(history, sort_keys=True, ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def first_picks(history):
    first = {}
    for day in sorted(history):
        dt.date.fromisoformat(day)
        for pick in history[day]:
            code = str(pick['code']).zfill(6)
            if not re.fullmatch(r'[0-9A-Z]{6}', code):
                raise ValueError('선정 종목코드 형식 오류')
            if code not in first:
                first[code] = dict(pick, code=code, selected=day)
    return list(first.values())


def positive(value):
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (ValueError, TypeError):
        return None


def fetch_closes(kind, symbol, start, end):
    """공개 차트의 날짜가 있는 종가만 사용. 누락 가격을 보간하지 않는다."""
    response = requests.get(
        f'{SOURCE}{kind}/{symbol}/day',
        params={'startDateTime': start.strftime('%Y%m%d') + '0000',
                'endDateTime': end.strftime('%Y%m%d') + '2359'},
        headers={'User-Agent': 'Mozilla/5.0'}, timeout=(5, 20))
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError('시세 응답 형식 오류')
    prices = {}
    for row in rows:
        day = dt.datetime.strptime(str(row['localDate']), '%Y%m%d').date()
        close = positive(row.get('closePrice'))
        if close is not None and start <= day <= end:
            key = day.isoformat()
            if key in prices and prices[key] != close:
                raise ValueError('동일 거래일의 종가 충돌')
            prices[key] = close
    if not prices:
        raise ValueError('유효 종가 없음')
    return dict(sorted(prices.items()))


def build_report(history, benchmarks, prices, cutoff, errors=None):
    """시장 실제 거래일로 D+N 계산. 종목 거래 누락으로 평가일을 뒤로 밀지 않는다."""
    cutoff = cutoff.isoformat()
    benchmarks = {m: {d: v for d, v in benchmarks.get(m, {}).items()
                      if d <= cutoff and positive(v) is not None} for m in MARKETS}
    rows = []
    for pick in first_picks(history):
        code, market, selected = pick['code'], pick.get('market'), pick['selected']
        index = benchmarks.get(market, {})
        calendar = sorted(index)
        base_pos = bisect.bisect_left(calendar, selected) - 1
        base = calendar[base_pos] if base_pos >= 0 else None
        record = {k: pick.get(k) for k in ('code', 'name', 'market', 'selected')}
        record.update(base_date=base, periods={})
        stock = prices.get(code, {})
        for horizon in HORIZONS:
            result = {'status': '기준일 없음', 'end_date': None,
                      'stock_return': None, 'market_return': None, 'excess_pp': None}
            if market not in MARKETS:
                result['status'] = '시장 정보 없음'
            elif not calendar:
                result['status'] = '지수 데이터 없음'
            elif selected > cutoff:
                result['status'] = '기간 미도래'
            elif base is not None:
                end_pos = len(calendar) - 1 if horizon == 'latest' else base_pos + int(horizon)
                if end_pos >= len(calendar) or end_pos <= base_pos:
                    result['status'] = '기간 미도래'
                else:
                    end = calendar[end_pos]
                    result['end_date'] = end
                    a, b = positive(stock.get(base)), positive(stock.get(end))
                    if a is None or b is None:
                        result['status'] = '종목 가격 누락'
                    else:
                        sr = (b / a - 1) * 100
                        mr = (float(index[end]) / float(index[base]) - 1) * 100
                        result.update(status='완료', stock_return=sr, market_return=mr,
                                      excess_pp=sr - mr)
            record['periods'][horizon] = result
        rows.append(record)
    return {'schema_version': 1, 'cutoff': cutoff,
            'history_digest': history_digest(history), 'rows': rows,
            'market_asof': {m: max(v) if v else None for m, v in benchmarks.items()},
            'errors': errors or {}, 'source': SOURCE,
            'method': 'first-selection / previous-session-close / price-return / no-costs',
            'quote_history': {'benchmarks': benchmarks, 'stocks': prices}}


def collect_report(data_dir, now=None):
    root = Path(data_dir)
    now = now or dt.datetime.now(KST)
    history = json.loads((root / 'daily_picks.json').read_text(encoding='utf-8'))
    meta = json.loads((root / 'meta.json').read_text(encoding='utf-8'))
    timestamp = dt.datetime.fromisoformat(meta['timestamp'])
    timestamp = timestamp.replace(tzinfo=KST) if timestamp.tzinfo is None else timestamp.astimezone(KST)
    # 장중 값을 완결 종가로 오인하지 않도록 수집일 당일은 항상 제외한다.
    cutoff = min(now.astimezone(KST).date(), timestamp.date()) - dt.timedelta(days=1)
    picks = first_picks(history)
    start = min((dt.date.fromisoformat(p['selected']) for p in picks), default=cutoff) - dt.timedelta(days=30)
    benchmarks = {m: fetch_closes('index', m, start, cutoff) for m in MARKETS}
    # 한 지수만 거래일이 빠지면 D+N 자체가 달라지므로 보고서를 게시하지 않는다.
    if set(benchmarks['KOSPI']) != set(benchmarks['KOSDAQ']):
        raise ValueError('KOSPI/KOSDAQ 거래일 불일치')
    prices, errors = {}, {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_closes, 'item', p['code'], start, cutoff): p['code']
                   for p in picks}
        for future in as_completed(futures):
            code = futures[future]
            try:
                prices[code] = future.result()
            except Exception as exc:
                errors[code] = type(exc).__name__
    if picks and not prices:
        raise RuntimeError('전 종목 시세 수집 실패')
    report = build_report(history, benchmarks, dict(sorted(prices.items())), cutoff, errors)
    report['generated_at'] = now.isoformat()
    report['selection_data_timestamp'] = meta['timestamp']
    target = root / 'daily_picks_performance.json'
    temporary = target.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=1, allow_nan=False), encoding='utf-8')
    os.replace(temporary, target)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).parent / 'data')
    args = parser.parse_args()
    report = collect_report(args.data_dir)
    print(f"Performance report: {len(report['rows'])} stocks, cutoff={report['cutoff']}, failures={len(report['errors'])}")
