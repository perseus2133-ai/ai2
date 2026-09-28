import json
from pathlib import Path

from paper_performance import summarize_traded_stocks


def test_all_traded_stocks_include_closed_winners_and_losers():
    trades = [
        {'date': '2026-07-01', 'action': 'BUY', '종목코드': '000001', '종목명': '승자', 'shares': 10, 'price': 100, 'amount': 1000},
        {'date': '2026-07-01', 'action': 'BUY', '종목코드': '000002', '종목명': '패자', 'shares': 10, 'price': 100, 'amount': 1000},
        {'date': '2026-07-08', 'action': 'SELL', '종목코드': '000001', '종목명': '승자', 'shares': 10, 'price': 120, 'amount': 1200},
        {'date': '2026-07-08', 'action': 'SELL', '종목코드': '000002', '종목명': '패자', 'shares': 10, 'price': 80, 'amount': 800},
    ]

    rows = {row['code']: row for row in summarize_traded_stocks(trades, {})}

    assert set(rows) == {'000001', '000002'}
    assert (rows['000001']['profit'], rows['000001']['return_pct'], rows['000001']['status']) == (200, 20, '매도 완료')
    assert (rows['000002']['profit'], rows['000002']['return_pct'], rows['000002']['status']) == (-200, -20, '매도 완료')


def test_partial_sale_and_rebuy_use_all_cash_flows_and_latest_valuation():
    trades = [
        {'date': '2026-07-01', 'action': 'BUY', '종목코드': '000001', '종목명': 'A', 'shares': 10, 'price': 100, 'amount': 1000},
        {'date': '2026-07-08', 'action': 'SELL', '종목코드': '000001', '종목명': 'A', 'shares': 4, 'price': 150, 'amount': 600},
        {'date': '2026-07-15', 'action': 'SELL', '종목코드': '000001', '종목명': 'A', 'shares': 6, 'price': 100, 'amount': 600},
        {'date': '2026-07-22', 'action': 'BUY', '종목코드': '000001', '종목명': 'A', 'shares': 5, 'price': 200, 'amount': 1000},
    ]
    latest = {'000001': {'name': 'A', 'shares': 5, 'price': 180, 'avg': 200}}

    row = summarize_traded_stocks(trades, latest)[0]

    assert row['buy_amount'] == 2000
    assert row['sell_amount'] == 1200
    assert row['market_value'] == 900
    assert row['profit'] == 100
    assert row['return_pct'] == 5
    assert row['status'] == '보유 중'
    assert row['first_buy_date'] == '2026-07-01'
    assert row['last_trade_date'] == '2026-07-22'


def test_missing_latest_price_does_not_claim_a_return():
    trades = [{'date': '2026-07-01', 'action': 'BUY', '종목코드': '000001', '종목명': 'A', 'shares': 2, 'price': 100, 'amount': 200}]

    row = summarize_traded_stocks(trades, {'000001': {'shares': 2, 'price': None}})[0]

    assert row['status'] == '평가가 없음'
    assert row['market_value'] is None
    assert row['profit'] is None
    assert row['return_pct'] is None


def test_repository_trade_ledger_reconciles_to_latest_portfolio_value():
    root = Path(__file__).resolve().parents[1] / 'data' / 'paper_trading'
    trades = json.loads((root / 'trades.json').read_text(encoding='utf-8'))
    history = json.loads((root / 'history.json').read_text(encoding='utf-8'))
    latest = history[-1]

    rows = summarize_traded_stocks(trades, latest['holdings'])

    assert len(rows) == len({trade['종목코드'] for trade in trades})
    assert all(row['profit'] is not None for row in rows)
    assert round(sum(row['profit'] for row in rows) + 100_000_000) == latest['total']
