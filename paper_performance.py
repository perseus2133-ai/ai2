"""Aggregate paper-trading cash flows and current value by stock."""


def summarize_traded_stocks(trades, latest_holdings):
    stocks = {}
    for trade in trades:
        code = str(trade['종목코드'])
        row = stocks.setdefault(code, {
            'code': code, 'name': trade['종목명'],
            'first_buy_date': None, 'last_trade_date': None,
            'buy_amount': 0, 'sell_amount': 0,
            'shares': 0,
        })
        row['name'] = trade['종목명']
        date = trade['date']
        row['last_trade_date'] = max(row['last_trade_date'] or date, date)
        amount = trade.get('amount', trade['shares'] * trade['price'])
        if trade['action'] == 'BUY':
            row['first_buy_date'] = min(row['first_buy_date'] or date, date)
            row['buy_amount'] += amount
            row['shares'] += trade['shares']
        elif trade['action'] == 'SELL':
            row['sell_amount'] += amount
            row['shares'] -= trade['shares']

    for code, row in stocks.items():
        shares = row['shares']
        if shares == 0:
            row['status'] = '매도 완료'
            row['market_value'] = 0
        else:
            holding = latest_holdings.get(code) or {}
            price = holding.get('price')
            if shares > 0 and holding.get('shares') == shares and price is not None and price > 0:
                row['status'] = '보유 중'
                row['market_value'] = shares * price
            else:
                row['status'] = '평가가 없음'
                row['market_value'] = None

        if row['market_value'] is not None and row['buy_amount'] > 0:
            row['profit'] = row['sell_amount'] + row['market_value'] - row['buy_amount']
            row['return_pct'] = round(row['profit'] / row['buy_amount'] * 100, 2)
        else:
            row['profit'] = None
            row['return_pct'] = None

    return sorted(stocks.values(), key=lambda row: (row['last_trade_date'], row['code']), reverse=True)
