"""Price-index comparisons; never fill a missing date with a future quote."""
import datetime as dt
import requests
import pandas as pd


def fetch_index(symbol, start, end):
    start = pd.Timestamp(start).date() - dt.timedelta(days=14)
    end = pd.Timestamp(end).date() + dt.timedelta(days=2)
    params = {
        'period1': int(dt.datetime.combine(start, dt.time(), dt.timezone.utc).timestamp()),
        'period2': int(dt.datetime.combine(end, dt.time(), dt.timezone.utc).timestamp()),
        'interval': '1d',
    }
    response = requests.get(
        f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}',
        params=params, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20,
    )
    response.raise_for_status()
    result = response.json()['chart']['result'][0]
    closes = result['indicators']['quote'][0]['close']
    rows = {}
    for timestamp, close in zip(result.get('timestamp', []), closes):
        if close is None or close <= 0:
            continue
        instant = pd.Timestamp(timestamp, unit='s', tz='UTC')
        local = instant.tz_convert(result['meta']['exchangeTimezoneName'])
        closing = local.normalize() + pd.Timedelta(hours=15, minutes=30) if symbol == '^KS11' else local.normalize() + pd.Timedelta(hours=16)
        if closing.tz_convert('UTC') > pd.Timestamp.now(tz='UTC'):
            continue
        # US close becomes available the next Korean calendar day.
        date = local.date() if symbol == '^KS11' else instant.tz_convert('Asia/Seoul').date() + dt.timedelta(days=1)
        rows[pd.Timestamp(date)] = float(close)
    return pd.Series(rows, dtype=float).sort_index()


def comparison_curve(history, indices):
    values = pd.DataFrame(history).sort_values('date').drop_duplicates('date', keep='last')
    values.index = pd.to_datetime(values['date'])
    total = pd.to_numeric(values['total'], errors='coerce')
    curve = pd.DataFrame({'모의투자': total / total.iloc[0] * 100}, index=values.index)
    for label, series in indices.items():
        series = series.sort_index().dropna()
        aligned = series.reindex(series.index.union(curve.index)).ffill().reindex(curve.index)
        if len(aligned) and pd.notna(aligned.iloc[0]) and aligned.iloc[0] > 0:
            curve[label] = aligned / aligned.iloc[0] * 100
    return curve
