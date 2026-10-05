import json
import pandas as pd
from paper_benchmarks import comparison_curve, fetch_index


def test_no_future_fill():
    history = [{'date': '2026-07-08', 'total': 100},
               {'date': '2026-07-09', 'total': 110},
               {'date': '2026-07-10', 'total': 105}]
    series = pd.Series([200, 240], index=pd.to_datetime(['2026-07-07', '2026-07-10']))
    curve = comparison_curve(history, {'Index': series})
    assert curve['Index'].tolist() == [100, 100, 120]
    assert all(abs(a-b) < 1e-9 for a,b in zip(curve['모의투자'], [100,110,105]))
    future = series.iloc[1:]
    assert 'Index' not in comparison_curve(history, {'Index': future})


if __name__ == '__main__':
    test_no_future_fill()
    with open('data/paper_trading/history.json', encoding='utf-8') as file:
        history = json.load(file)
    indices = {}
    for label, symbol in [('KOSPI', '^KS11'), ('SP500', '^GSPC')]:
        indices[label] = fetch_index(symbol, history[0]['date'], history[-1]['date'])
        print(label, len(indices[label]))
        assert not indices[label].empty
    print(comparison_curve(history, indices).tail())
