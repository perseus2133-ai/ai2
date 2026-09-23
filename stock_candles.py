"""종목 카드용 최근 6개월 OHLCV와 가벼운 SVG 일봉. 운영 데이터는 수정하지 않는다."""
import calendar
import datetime as dt
import html
import math
import re
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import requests
import streamlit as st

KST = ZoneInfo('Asia/Seoul')


def six_month_window(today):
    month = today.year * 12 + today.month - 1 - 6
    year, month = divmod(month, 12)
    month += 1
    start = dt.date(year, month, min(today.day, calendar.monthrange(year, month)[1]))
    return start, today - dt.timedelta(days=1)


def number(value):
    try:
        value = float(str(value).replace(',', ''))
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def normalize_candles(payload, start, end):
    if not isinstance(payload, list):
        raise ValueError('일봉 응답 형식 오류')
    rows, invalid = {}, 0
    for item in payload:
        try:
            day = dt.datetime.strptime(str(item['localDate']), '%Y%m%d').date()
            if not start <= day <= end:
                continue
            o, h, l, c = (number(item.get(key)) for key in ('openPrice', 'highPrice', 'lowPrice', 'closePrice'))
            if any(v is None or v <= 0 for v in (o, h, l, c)) or not l <= min(o, c) <= max(o, c) <= h:
                raise ValueError('OHLC 오류')
            volume = number(item.get('accumulatedTradingVolume'))
            row = {'date': day.isoformat(), 'open': o, 'high': h, 'low': l, 'close': c,
                   'volume': volume if volume is not None and volume >= 0 else None}
        except (ValueError, TypeError, KeyError):
            invalid += 1
            continue
        if row['date'] in rows and rows[row['date']] != row:
            raise ValueError('거래일 중복 값 충돌')
        rows[row['date']] = row
    return sorted(rows.values(), key=lambda row: row['date']), invalid


@st.cache_data(ttl=300, max_entries=512, show_spinner=False)
def load_candles(code, today_iso):
    """성공/장애 응답 모두 5분 캐시하여 반복 카드와 실패 시 재요청을 제한한다."""
    today = dt.date.fromisoformat(today_iso)
    start, end = six_month_window(today)
    result = {'rows': [], 'start': start.isoformat(), 'end': end.isoformat(), 'invalid': 0, 'error': ''}
    if not re.fullmatch(r'[0-9A-Z]{6}', code):
        result['error'] = '종목코드를 확인할 수 없습니다.'
        return result
    try:
        response = requests.get(f'https://api.stock.naver.com/chart/domestic/item/{code}/day',
                                params={'startDateTime': start.strftime('%Y%m%d') + '0000',
                                        'endDateTime': end.strftime('%Y%m%d') + '2359'},
                                headers={'User-Agent': 'Mozilla/5.0'}, timeout=(3, 6))
        response.raise_for_status()
        result['rows'], result['invalid'] = normalize_candles(response.json(), start, end)
    except (requests.RequestException, ValueError, TypeError):
        result['error'] = '일봉을 불러오지 못했습니다. 잠시 후 다시 확인해 주세요.'
    return result


def prefetch_candles(codes, today=None):
    """렌더링할 카드만 최대 6개 동시 조회. 동일 종목은 중복 요청하지 않는다."""
    today = today or dt.datetime.now(KST).date()
    codes = sorted({str(code).zfill(6) for code in codes})
    if not codes:
        return {}
    with ThreadPoolExecutor(max_workers=min(6, len(codes))) as pool:
        data = list(pool.map(lambda code: load_candles(code, today.isoformat()), codes))
    return dict(zip(codes, data))


def candle_svg(rows):
    """거래일을 등간격으로 배치. 가격(원)과 거래량(주)은 서로 다른 축이다."""
    if not rows:
        return ''
    width, height = 380, 252
    left, right, top, bottom = 62, 370, 14, 171
    volume_top, volume_bottom = 191, 226
    low, high = min(r['low'] for r in rows), max(r['high'] for r in rows)
    pad = max((high - low) * 0.06, high * 0.01, 1)
    low, high = max(0, low - pad), high + pad
    scale = lambda price: bottom - (price - low) / (high - low) * (bottom - top)
    step = (right - left) / len(rows)
    body = max(0.6, min(6, step * .7))
    vmax = max((r['volume'] or 0 for r in rows), default=0)
    elements = [f'<svg class="qcd-candle-svg" viewBox="0 0 {width} {height}" '
                'style="width:100%;height:auto;display:block;" role="img" '
                'aria-label="최근 6개월 일봉 및 거래량">',
                '<title>최근 6개월 일봉 및 거래량</title>',
                '<desc>캔들은 시가 고가 저가 종가, 아래 막대는 거래량입니다. '
                '빨강은 시가보다 종가 상승, 파랑은 하락입니다.</desc>']
    for i in range(4):
        price = low + (high - low) * i / 3
        y = scale(price)
        elements.append(f'<line x1="{left}" x2="{right}" y1="{y:.2f}" y2="{y:.2f}" '
                        'stroke="currentColor" opacity="0.12"/>')
        elements.append(f'<text x="{left-5}" y="{y+4:.2f}" text-anchor="end" '
                        f'fill="currentColor" font-size="12">{price:,.0f}</text>')
    elements.append(f'<text x="5" y="12" fill="currentColor" font-size="11">원</text>')
    elements.append(f'<text x="5" y="{volume_top+13}" fill="currentColor" font-size="11">거래량</text>')
    elements.append(f'<text x="5" y="{volume_top+27}" fill="currentColor" font-size="11">(주)</text>')
    for i, row in enumerate(rows):
        x = left + step * (i + .5)
        color = '#EF4444' if row['close'] > row['open'] else '#3B82F6' if row['close'] < row['open'] else '#94A3B8'
        volume_label = f"{row['volume']:,.0f}" if row['volume'] is not None else '없음'
        tooltip = html.escape(f"{row['date']} · 시가 {row['open']:,.0f} / 고가 {row['high']:,.0f} / "
                              f"저가 {row['low']:,.0f} / 종가 {row['close']:,.0f}원 · 거래량 {volume_label}주")
        yo, yc = scale(row['open']), scale(row['close'])
        elements.append(f'<g class="qcd-candle"><title>{tooltip}</title>'
                        f'<rect x="{x-step/2:.2f}" y="{top}" width="{step:.2f}" '
                        f'height="{volume_bottom-top}" fill="transparent"/>'
                        f'<line x1="{x:.2f}" x2="{x:.2f}" y1="{scale(row["high"]):.2f}" '
                        f'y2="{scale(row["low"]):.2f}" stroke="{color}" stroke-width="0.8"/>'
                        f'<rect x="{x-body/2:.2f}" y="{min(yo,yc):.2f}" width="{body:.2f}" '
                        f'height="{max(1,abs(yo-yc)):.2f}" fill="{color}"/>')
        if vmax and row['volume'] is not None:
            vh = row['volume'] / vmax * (volume_bottom - volume_top)
            elements.append(f'<rect x="{x-body/2:.2f}" y="{volume_bottom-vh:.2f}" '
                            f'width="{body:.2f}" height="{vh:.2f}" fill="{color}" opacity="0.6"/>')
        elements.append('</g>')
    for i in sorted({round((len(rows)-1) * fraction / 3) for fraction in range(4)}):
        x = left + step * (i + .5)
        anchor = 'start' if i == 0 else 'end' if i == len(rows)-1 else 'middle'
        elements.append(f'<text x="{x:.2f}" y="246" text-anchor="{anchor}" '
                        f'fill="currentColor" font-size="12">{rows[i]["date"][5:].replace("-", "/")}</text>')
    return ''.join(elements) + '</svg>'


def candle_panel(data):
    rows = data['rows']
    head = '<div class="qcd-candle-heading">최근 6개월 · 일봉</div>'
    if not rows:
        message = html.escape(data['error'] or '해당 기간에 표시할 일봉 데이터가 없습니다.')
        body = f'<div class="qcd-candle-empty">{message}</div>'
    else:
        latest = rows[-1]
        head += (f'<div class="qcd-candle-meta">{latest["date"]} 종가 '
                 f'<b>{latest["close"]:,.0f}원</b> · {len(rows)}개 일봉</div>')
        head += '<div class="qcd-candle-meta"><span style="color:#EF4444;">■ 양봉</span> · <span style="color:#3B82F6;">■ 음봉</span> (시가 대비)</div>'
        body = candle_svg(rows)
    note = f'{data["start"]} ~ {data["end"]} · 전일까지 · 네이버 차트'
    if data['invalid']:
        note += f' · 비정상 {data["invalid"]}개 제외'
    return (f'<div class="qcd-chart-box qcd-candle-box">{head}{body}'
            f'<div class="qcd-candle-meta">{html.escape(note)}</div></div>')
