import datetime
import json

import numpy as np
import pytest

import crawl_script as crawler
import data_quality
import paper_trading
import snapshot_io
from consensus_persist import merge_carry_forward


def test_normal_frame_passes(valid_frame):
    data_quality.validate_frame(valid_frame, previous=valid_frame)


@pytest.mark.parametrize('fault', ['empty', 'market', 'rows', 'duplicate', 'price', 'volume', 'indicator', 'sector'])
def test_bad_crawl_is_rejected(valid_frame, fault):
    df = valid_frame.copy()
    if fault == 'empty': df = df.iloc[:0]
    if fault == 'market': df = df[df['시장'] == 'KOSPI']
    if fault == 'rows': df = df.drop(index=[0, 1])
    if fault == 'duplicate': df.loc[0, '종목코드'] = df.loc[1, '종목코드']
    if fault == 'price': df.loc[0, '현재가'] = np.inf
    if fault == 'volume': df.loc[0, 'Recent_Volume'] = -1.
    if fault == 'indicator': df.loc[:4, '외인_5d'] = np.nan
    if fault == 'sector': df['업종'] = '기타'
    with pytest.raises(ValueError):
        data_quality.validate_frame(df, previous=valid_frame)


def test_stale_data_blocks_trading(tmp_path, monkeypatch, valid_frame):
    csv = tmp_path / 'consensus_data.csv'
    valid_frame.to_csv(csv, index=False)
    (tmp_path / 'meta.json').write_text(json.dumps({'timestamp': '2026-09-22T05:00:00+09:00', 'data_count': 10}))
    monkeypatch.setattr(paper_trading, 'CSV_FILE', str(csv))
    monkeypatch.setattr(paper_trading, 'PORTFOLIO_FILE', str(tmp_path / 'portfolio.json'))
    monkeypatch.setattr(paper_trading, 'now_kst', lambda: datetime.datetime(2026, 9, 23, tzinfo=data_quality.KST))
    with pytest.raises(ValueError, match='오늘'):
        paper_trading.cmd_auto()
    assert not (tmp_path / 'portfolio.json').exists()


def test_crawl_failure_preserves_existing_data(tmp_path, monkeypatch, valid_frame):
    csv = tmp_path / 'consensus_data.csv'
    valid_frame.to_csv(csv, index=False)
    original = csv.read_bytes()
    monkeypatch.setattr(crawler, 'CSV_FILE', str(csv))
    monkeypatch.setattr(crawler, 'DATA_DIR', str(tmp_path))
    monkeypatch.setattr(crawler, 'get_stock_list_naver', lambda market: valid_frame[valid_frame['시장'] == ('KOSPI' if market == '0' else 'KOSDAQ')])
    monkeypatch.setattr(crawler, 'get_naver_sector_map', lambda: dict(zip(valid_frame['종목코드'], valid_frame['업종'])))
    def broken(code, name, market_cap=None):
        row = valid_frame[valid_frame['종목코드'] == code].iloc[0].to_dict()
        row['외인_5d'] = np.nan
        return row
    monkeypatch.setattr(crawler, 'scrape_naver_consensus', broken)
    with pytest.raises(ValueError, match='외인_5d'):
        crawler.main()
    assert csv.read_bytes() == original
    assert not (tmp_path / 'sector_map.json').exists()


def test_per_receives_market_cap_and_loss_has_no_per(monkeypatch):
    fg = {'매출액': {2025: 1000}, '영업이익': {2025: 100}, '당기순이익_지배': 50}
    monkeypatch.setattr(crawler, 'scrape_fnguide_supplement', lambda *args: fg)
    monkeypatch.setattr(crawler, 'scrape_fnguide_netdebt', lambda *args: None)
    monkeypatch.setattr(crawler, 'fetch_supplement_indicators', lambda *args: {})
    assert crawler.scrape_naver_consensus('000001', 'Test', market_cap=1000)['PER'] == 20
    fg['당기순이익_지배'] = -50
    assert np.isnan(crawler.scrape_naver_consensus('000001', 'Test', market_cap=1000)['PER'])


def test_snapshot_failure_stops_crawl(monkeypatch, valid_frame):
    monkeypatch.setattr(snapshot_io, 'save_snapshot', lambda *args: False)
    with pytest.raises(RuntimeError, match='스냅샷'):
        crawler.save_consensus_snapshot(valid_frame)


def test_carry_forward_expires_and_never_carries_prices(tmp_path):
    import pandas as pd
    snapshot_io.save_snapshot_dict({'000001': {'영업이익_2028': 123., '현재가': 9000.}}, str(tmp_path), '2026-08-01')
    df = pd.DataFrame([{'종목코드': '000001', '현재가': 10000., '영업이익_2028': np.nan}])
    fresh = merge_carry_forward(df, str(tmp_path), today=datetime.date(2026, 9, 15), verbose=False)
    stale = merge_carry_forward(df, str(tmp_path), today=datetime.date(2026, 9, 16), verbose=False)
    assert fresh.iloc[0]['영업이익_2028'] == 123.
    assert fresh.iloc[0]['현재가'] == 10000.
    assert fresh.iloc[0]['컨센_보강일'] == '2026-08-01'
    assert np.isnan(stale.iloc[0]['영업이익_2028'])


def test_corrupt_portfolio_is_not_silently_reset(tmp_path):
    path = tmp_path / 'portfolio.json'
    path.write_text('{invalid')
    with pytest.raises(json.JSONDecodeError):
        paper_trading._load_json(path, {})
    assert path.read_text() == '{invalid'


@pytest.mark.parametrize('col', ['현재가', '시가총액', 'Recent_Volume'])
@pytest.mark.parametrize('value', [np.nan, np.inf])
def test_nonfinite_trading_inputs_rejected(valid_frame, col, value):
    row = valid_frame.iloc[0].copy()
    row[col] = value
    assert not paper_trading._hard_filter(row)


def test_latest_loss_does_not_use_older_profitable_year():
    from types import SimpleNamespace
    obj = {
        'header': [{'CD': 'VAL1', 'YYMM': '2024/12'}, {'CD': 'VAL2', 'YYMM': '2025/12'}],
        'data': [{'NAME': '당기순이익(지배)', 'VAL1': 100, 'VAL2': -20}],
    }
    response = SimpleNamespace(text='perforTrend: ' + json.dumps(obj), encoding=None)
    assert crawler._parse_fnguide_consensus_json(response)['당기순이익_지배'] == -20


@pytest.fixture
def mocked_crawl(tmp_path, monkeypatch, valid_frame):
    import fetch_profiles
    paths = {'DATA_DIR': tmp_path, 'CSV_FILE': tmp_path / 'consensus_data.csv',
             'META_FILE': tmp_path / 'meta.json', 'HEALTH_FILE': tmp_path / 'fnguide_health.json',
             'SNAPSHOT_DIR': tmp_path / 'snapshots', 'HISTORY_DIR': tmp_path / 'history'}
    for key, path in paths.items():
        monkeypatch.setattr(crawler, key, str(path))
    (tmp_path / 'history').mkdir()
    valid_frame.to_csv(paths['CSV_FILE'], index=False)
    monkeypatch.setattr(crawler, 'now_kst', lambda: datetime.datetime(2026, 9, 23, 5, tzinfo=data_quality.KST))
    monkeypatch.setattr(crawler, 'get_stock_list_naver', lambda market: valid_frame[valid_frame['시장'] == ('KOSPI' if market == '0' else 'KOSDAQ')])
    monkeypatch.setattr(crawler, 'get_naver_sector_map', lambda: dict(zip(valid_frame['종목코드'], valid_frame['업종'])))
    monkeypatch.setattr(crawler, 'scrape_naver_consensus', lambda code, name, market_cap=None: valid_frame[valid_frame['종목코드'] == code].iloc[0].to_dict())
    monkeypatch.setattr(fetch_profiles, 'update_profiles', lambda *args: None)
    def forbidden(*args, **kwargs):
        pytest.fail('Mocked crawl must not use the network')
    monkeypatch.setattr(crawler.requests, 'get', forbidden)
    return tmp_path


def test_mocked_collection_to_picks_and_paper_trading(mocked_crawl, monkeypatch):
    root = mocked_crawl
    crawler.main()
    df = data_quality.validate_saved_data(root, today=datetime.date(2026, 9, 23))
    assert len(df) == 10
    assert json.loads((root / 'fnguide_health.json').read_text())['ok']
    picks = json.loads((root / 'daily_picks.json').read_text(encoding='utf-8'))
    assert len(picks['2026-09-23']) == 6
    assert (root / 'history' / 'accumulation.json').exists()
    assert snapshot_io.find_snapshot_on_or_before(str(root / 'snapshots'), datetime.date(2026, 9, 23))[1]
    pt_dir = root / 'paper_trading'
    for key, path in {'CSV_FILE': root / 'consensus_data.csv', 'PT_DIR': pt_dir,
                      'PORTFOLIO_FILE': pt_dir / 'portfolio.json', 'TRADES_FILE': pt_dir / 'trades.json',
                      'HISTORY_FILE': pt_dir / 'history.json'}.items():
        monkeypatch.setattr(paper_trading, key, str(path))
    monkeypatch.setattr(paper_trading, 'now_kst', crawler.now_kst)
    paper_trading.cmd_auto()
    before = (pt_dir / 'trades.json').read_bytes()
    paper_trading.cmd_auto()
    assert (pt_dir / 'trades.json').read_bytes() == before
    pf = json.loads((pt_dir / 'portfolio.json').read_text(encoding='utf-8'))
    assert pf['cash'] >= 0
    assert len(pf['holdings']) == 5
    assert len(json.loads((pt_dir / 'history.json').read_text(encoding='utf-8'))) == 1


@pytest.mark.parametrize('stage', ['snapshot', 'health', 'carry'])
def test_critical_stage_failure_preserves_csv(mocked_crawl, monkeypatch, stage):
    import consensus_persist
    root = mocked_crawl
    original = (root / 'consensus_data.csv').read_bytes()
    def failed(*args, **kwargs):
        raise OSError('simulated failure')
    if stage == 'snapshot': monkeypatch.setattr(crawler, 'save_consensus_snapshot', failed)
    if stage == 'health': monkeypatch.setattr(crawler, 'write_fnguide_health', failed)
    if stage == 'carry': monkeypatch.setattr(consensus_persist, 'merge_carry_forward', failed)
    with pytest.raises(OSError, match='simulated failure'):
        crawler.main()
    assert (root / 'consensus_data.csv').read_bytes() == original
    assert not (root / 'daily_picks.json').exists()
