# -*- coding: utf-8 -*-
"""주도업종 랭킹 + 업종 저평가 발굴 단위 테스트 (합성 데이터)."""
import os
import sys
import datetime

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snapshot_io
from sector_leaders import (sector_momentum, rank_sectors, sector_detail,
                            MIN_STOCKS_PER_SECTOR, MAX_ABS_RET, GROWTH_CLAMP)


def stock(code, sector, price=1000.0, mcap=1000.0, op28=100.0,
          op_g28=20.0, sales_g28=10.0, net_debt=0.0, ctrl=1.0,
          foreign=1.0, inst=1.0, volx=1.5, rev=1.0):
    """합성 종목 1행 (28E 기준으로 단순화)."""
    return {
        '종목코드': code, '종목명': f'S{code}', '시장': 'KOSPI', '업종': sector,
        '현재가': price, '시가총액': mcap,
        '지배비율': ctrl, '순차입금': net_debt,
        '매출액_2028': 1000.0, '영업이익_2028': op28,
        '영업이익_성장률_2028': op_g28, '매출액_성장률_2028': sales_g28,
        '외인_20d': foreign, '기관_20d': inst,
        '거래량배수': volx, 'Revision_Score': rev,
    }


def universe(rows):
    return pd.DataFrame(rows)


# ── ① 모멘텀: 중앙값 + 이상치 제외 ──────────────────────────
def test_momentum_median_and_outlier_excluded(tmp_path):
    snap_dir = str(tmp_path / 'snap')
    past_day = datetime.date(2026, 1, 1)
    # 과거 스냅샷: 전부 100원
    snap = {f'00000{i}': {'현재가': 100.0} for i in range(1, 5)}
    snapshot_io.save_snapshot_dict(snap, snap_dir, past_day.strftime('%Y-%m-%d'))

    # 현재가: +10%, +20%, +30%(경계 통과), +500%(이상치 → 제외)
    df = universe([
        stock('000001', '전력', price=110.0),
        stock('000002', '전력', price=120.0),
        stock('000003', '전력', price=130.0),
        stock('000004', '전력', price=600.0),   # +500% → 제외
    ])
    today = past_day + datetime.timedelta(days=20)
    mom = sector_momentum(df, snap_dir, days=20, today=today)
    assert len(mom) == 1
    assert mom.iloc[0]['n_momentum'] == 3          # 이상치 1건 제외
    assert mom.iloc[0]['momentum_pct'] == pytest.approx(20.0)   # 중앙값


def test_momentum_no_snapshot_returns_empty(tmp_path):
    df = universe([stock('000001', '전력')])
    mom = sector_momentum(df, str(tmp_path / 'none'), days=20)
    assert mom.empty


# ── ② 랭킹: 종목수 미달 제외 / 강한 업종이 상위 ─────────────
def test_rank_excludes_small_sectors():
    rows = [stock(f'00000{i}', '큰업종') for i in range(1, 5)]
    rows += [stock('000010', '작은업종')]           # 1종목 → 제외
    res = rank_sectors(universe(rows), snapshot_dir=None)
    assert '작은업종' not in set(res['업종'])
    assert '큰업종' in set(res['업종'])


def test_rank_strong_sector_first():
    strong = [stock(f'0001{i:02d}', '강한업종', foreign=1, inst=1,
                    volx=5.0, rev=10.0) for i in range(3)]
    weak = [stock(f'0002{i:02d}', '약한업종', foreign=-1, inst=-1,
                  volx=0.5, rev=-5.0) for i in range(3)]
    res = rank_sectors(universe(strong + weak), snapshot_dir=None)
    assert res.iloc[0]['업종'] == '강한업종'
    assert res.iloc[0]['rank'] == 1
    assert res.iloc[0]['score'] > res.iloc[1]['score']


def test_rank_top_n_limit():
    rows = []
    for s in range(6):
        rows += [stock(f'{s}0000{i}', f'업종{s}') for i in range(3)]
    res = rank_sectors(universe(rows), snapshot_dir=None, top_n=3)
    assert len(res) == 3


# ── ③ 상세: 괴리율 + 성장 보정 ──────────────────────────────
def test_detail_undervalued_first_and_growth_premium():
    # 피어 3개는 멀티플 10배(시총1000/OP100), 성장률 20%
    rows = [stock(f'00010{i}', '반도체', mcap=1000.0, op28=100.0, op_g28=20.0)
            for i in range(3)]
    # 대상 A: 같은 이익인데 시총 절반(500) → 저평가 + 고성장(60%) → 프리미엄
    rows.append(stock('000200', '반도체', mcap=500.0, op28=100.0, op_g28=60.0))
    # 대상 B: 시총 2000 → 고평가 + 저성장(0%) → 디스카운트
    rows.append(stock('000300', '반도체', mcap=2000.0, op28=100.0, op_g28=0.0))
    d = sector_detail('반도체', universe(rows))

    assert not d.empty
    assert d.iloc[0]['종목코드'] == '000200'          # 저평가가 1위
    a = d[d['종목코드'] == '000200'].iloc[0]
    b = d[d['종목코드'] == '000300'].iloc[0]
    assert a['괴리율'] > 0 and b['괴리율'] < 0        # 단순 괴리율 부호
    assert a['성장프리미엄'] > 1.0                    # 고성장 → 프리미엄
    assert b['성장프리미엄'] < 1.0                    # 저성장 → 디스카운트
    assert a['괴리율_보정'] > a['괴리율']             # 보정이 저평가를 더 키움
    assert a['기준연도'] == "'28E"


def test_growth_premium_is_clamped():
    rows = [stock(f'00040{i}', '바이오', mcap=1000.0, op28=100.0, op_g28=0.0)
            for i in range(3)]
    rows.append(stock('000500', '바이오', mcap=1000.0, op28=100.0,
                      op_g28=100000.0))       # 극단 성장률
    d = sector_detail('바이오', universe(rows))
    prem = d[d['종목코드'] == '000500'].iloc[0]['성장프리미엄']
    assert prem <= GROWTH_CLAMP[1] + 1e-9     # 상한 클램프


def test_detail_flags_loss_and_small_peers():
    # 피어 2개(5개 미만) + 적자 종목
    rows = [stock('000601', '조선', mcap=1000.0, op28=100.0),
            stock('000602', '조선', mcap=1000.0, op28=100.0)]
    loss = stock('000603', '조선', mcap=1000.0, op28=-50.0)
    loss['영업이익_2028'] = -50.0
    rows.append(loss)
    d = sector_detail('조선', universe(rows))
    warn_all = ' '.join(d['경고'].tolist())
    assert '표본 부족' in warn_all
    row_loss = d[d['종목코드'] == '000603'].iloc[0]
    # 28E 적자 → 폴백 연도 없음 → 컨센 없음 또는 적자 플래그
    assert row_loss['경고'] != ''


def test_detail_net_debt_lowers_fair_mcap():
    """순차입금이 크면 적정시총이 그만큼 낮아져야 한다 (EV→equity 환원)."""
    peers = [stock(f'00070{i}', '기계', mcap=1000.0, op28=100.0) for i in range(3)]
    clean = stock('000800', '기계', mcap=1000.0, op28=100.0, net_debt=0.0)
    heavy = stock('000801', '기계', mcap=1000.0, op28=100.0, net_debt=300.0)
    d = sector_detail('기계', universe(peers + [clean, heavy]))
    f_clean = d[d['종목코드'] == '000800'].iloc[0]['적정시총']
    f_heavy = d[d['종목코드'] == '000801'].iloc[0]['적정시총']
    assert f_heavy < f_clean


def test_detail_empty_inputs():
    assert sector_detail('없는업종', universe([stock('000900', '전력')])).empty
    assert sector_detail('전력', pd.DataFrame()).empty
    assert rank_sectors(pd.DataFrame()).empty
