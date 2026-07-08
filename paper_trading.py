#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
가상(모의) 투자 엔진 — ai2 컨센서스 데이터 기반
=================================================
- 초기 자본 1억원(가상), 최적 5종목 선정 후 모의 매수
- 매일: 자동 크롤 직후 현재가로 평가액 기록 (equity curve)
- 매주(7일 경과): 매도 룰 점검 + 재채점 → 교체/유지 결정
- 모든 거래는 사유와 함께 trades.json 에 기록

[매도 룰 — 사용자 실전 매매 룰 반영]
  1. 손절: 매수가 대비 -15%
  2. 트레일링 스탑: 보유 중 고점 대비 -20%
  3. 익절: +50% 도달 시 1/3 매도, +100% 도달 시 추가 1/3 매도
  4. 시간 손절: 26주(6개월) 보유 & 수익률 +10% 미만 → 교체
  5. 순위 탈락: 재채점에서 상위 20위 밖 & 대체 후보가 10점 이상 우위

[선정 점수 (0~100) — Claude 판단의 코드화]
  성장성 30: 영업이익 최대성장률(cap 300%) 20 + 미래가시성 P등급 10
  밸류   20: PEG (Forward PER / 26E 성장률) 구간 점수
  수급   20: 외인/기관 5d·20d 순매수 각 +5
  기술   20: 이평 정배열 +5, MACD 강세 +5, OBV 매집 +5, RSI 40~65 +5
  유동성 10: 일 거래대금 (300억+ =10 / 100억+ =7 / 50억+ =4)
  하드필터: 흑자, 매출 500억+, 시총 2,000억+, 거래대금 30억+,
            RSI < 78(과열 진입 금지), 업종당 최대 2종목

사용:
  python paper_trading.py --init    # 최초 포트폴리오 구성
  python paper_trading.py --auto    # 매일 평가 + (7일 경과 시) 리밸런싱
  python paper_trading.py --status  # 현재 상태 출력
"""
import os
import sys
import json
import datetime

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')
HERE = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(HERE, 'data', 'consensus_data.csv')
PT_DIR = os.path.join(HERE, 'data', 'paper_trading')
PORTFOLIO_FILE = os.path.join(PT_DIR, 'portfolio.json')
HISTORY_FILE = os.path.join(PT_DIR, 'history.json')
TRADES_FILE = os.path.join(PT_DIR, 'trades.json')

INITIAL_CAPITAL = 100_000_000   # 1억원 (가상)
N_HOLDINGS = 5
TARGET_INVEST_RATIO = 0.90      # 현금 ~10% 유지 (사용자 룰: 현금 10~20%)
REBALANCE_DAYS = 7              # 주 1회
MAX_PER_SECTOR = 2

STOP_LOSS = -0.15               # 손절 -15%
TRAILING_STOP = -0.20           # 고점 대비 -20%
TP1_TRIGGER, TP2_TRIGGER = 0.50, 1.00   # 익절 +50% / +100% → 각 1/3
TIME_STOP_DAYS = 182            # 26주
TIME_STOP_MIN_RET = 0.10
RANK_CUT = 20                   # 상위 20위 밖이면 교체 후보
HYSTERESIS = 10.0               # 대체 후보가 10점 이상 우위일 때만 교체


def now_kst():
    return datetime.datetime.now(KST)


def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return default


def _save_json(path, obj):
    os.makedirs(PT_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ============================================================
# 유니버스 로드 + 채점
# ============================================================
def load_universe():
    df = pd.read_csv(CSV_FILE, dtype={'종목코드': str})
    df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
    for c in ['현재가', '시가총액', 'Recent_Volume', 'PER', 'RSI',
              '영업이익_최대성장률', '영업이익_성장률_2025', '영업이익_성장률_2026',
              '외인_5d', '외인_20d', '기관_5d', '기관_20d']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def _visibility_rank(row):
    rv = {y: row.get(f'매출액_{y}', np.nan) for y in (2024, 2025, 2026, 2027, 2028)}
    if pd.notna(rv[2028]) and pd.notna(rv[2025]) and rv[2025] > 0: return 1
    if pd.notna(rv[2027]) and pd.notna(rv[2025]) and rv[2025] > 0: return 2
    if pd.notna(rv[2026]) and pd.notna(rv[2025]) and rv[2025] > 0: return 3
    if pd.notna(rv[2025]) and pd.notna(rv[2024]) and rv[2024] > 0: return 4
    return 5


def _hard_filter(row):
    """진입 가능 종목인지 (사용자 스크리너의 엄격 필터 + 트레이딩 안전장치)."""
    price = row.get('현재가', 0) or 0
    vol = row.get('Recent_Volume', 0) or 0
    mcap = row.get('시가총액', 0) or 0
    if price <= 0 or mcap < 2000:                      # 시총 2,000억+
        return False
    if price * vol < 3_000_000_000:                    # 거래대금 30억+
        return False
    rsi = row.get('RSI', np.nan)
    if pd.notna(rsi) and rsi >= 78:                    # 과열 진입 금지
        return False
    for y in (2024, 2025, 2026, 2027, 2028):
        rv = row.get(f'매출액_{y}', np.nan)
        ov = row.get(f'영업이익_{y}', np.nan)
        if pd.notna(rv) and rv < 500:                  # 매출 500억+ (매년)
            return False
        if pd.notna(ov) and ov < 0:                    # 영업이익 흑자
            return False
    return True


def score_row(row):
    """0~100 종합 점수 + 세부 내역."""
    detail = {}
    # ── 성장성 (30) ──
    g = row.get('영업이익_최대성장률', np.nan)
    growth = min(max(g, 0), 300) / 300 * 20 if pd.notna(g) else 0.0
    pr = _visibility_rank(row)
    vis = {1: 10, 2: 8, 3: 5, 4: 2}.get(pr, 0)
    detail['성장'] = round(growth + vis, 1)
    # ── 밸류 (20) — PEG ──
    per = row.get('PER', np.nan)
    g26 = row.get('영업이익_성장률_2026', np.nan)
    if pd.isna(g26):
        g26 = row.get('영업이익_성장률_2025', np.nan)
    val = 8.0  # 데이터 없으면 중립
    if pd.notna(per) and per > 0 and pd.notna(g26) and g26 > 0:
        peg = (per / (1 + g26 / 100)) / g26
        val = 20 if peg <= 0.5 else 16 if peg <= 1 else 10 if peg <= 1.5 else 6 if peg <= 2 else 2
    detail['밸류'] = round(val, 1)
    # ── 수급 (20) ──
    flow = 0
    for c in ('외인_5d', '기관_5d', '외인_20d', '기관_20d'):
        v = row.get(c, np.nan)
        if pd.notna(v) and v > 0:
            flow += 5
    detail['수급'] = flow
    # ── 기술 (20) ──
    tech = 0
    if row.get('MA_align') == 'up': tech += 5
    if row.get('MACD_signal') in ('bull', 'bull_cross'): tech += 5
    if row.get('OBV_trend') == 'up': tech += 5
    rsi = row.get('RSI', np.nan)
    if pd.notna(rsi) and 40 <= rsi <= 65: tech += 5
    detail['기술'] = tech
    # ── 유동성 (10) ──
    turnover = (row.get('현재가', 0) or 0) * (row.get('Recent_Volume', 0) or 0)
    liq = 10 if turnover >= 3e10 else 7 if turnover >= 1e10 else 4 if turnover >= 5e9 else 0
    detail['유동성'] = liq

    total = round(sum(detail.values()), 1)
    return total, detail


def rank_universe(df):
    """하드필터 통과 종목을 채점해 점수 내림차순 DataFrame 반환."""
    rows = []
    for _, row in df.iterrows():
        if not _hard_filter(row):
            continue
        total, detail = score_row(row)
        rows.append({
            '종목코드': row['종목코드'], '종목명': row.get('종목명', ''),
            '업종': row.get('업종', '기타'), '현재가': float(row.get('현재가', 0) or 0),
            '점수': total, '세부': detail,
        })
    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return ranked
    return ranked.sort_values('점수', ascending=False).reset_index(drop=True)


def pick_top(ranked, n, exclude_codes=(), sector_count=None):
    """업종당 MAX_PER_SECTOR 제한을 지키며 상위 n개 선정."""
    if sector_count is None:
        sector_count = {}
    picks = []
    for _, r in ranked.iterrows():
        if r['종목코드'] in exclude_codes:
            continue
        sec = r['업종'] or '기타'
        if sector_count.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        picks.append(r)
        sector_count[sec] = sector_count.get(sec, 0) + 1
        if len(picks) >= n:
            break
    return picks


# ============================================================
# 포트폴리오 연산
# ============================================================
def _log_trade(trades, action, code, name, shares, price, reason):
    trades.append({
        'date': now_kst().strftime('%Y-%m-%d'),
        'action': action, '종목코드': code, '종목명': name,
        'shares': int(shares), 'price': float(price),
        'amount': int(shares * price), 'reason': reason,
    })
    print(f"  [{action}] {name}({code}) {shares:,}주 @ {price:,.0f}원 — {reason}")


def _buy(pf, trades, code, name, price, budget, reason):
    shares = int(budget // price)
    if shares <= 0:
        return False
    cost = shares * price
    pf['cash'] -= cost
    pf['holdings'][code] = {
        '종목명': name, 'shares': shares, 'avg_price': price,
        'peak_price': price, 'bought': now_kst().strftime('%Y-%m-%d'),
        'tp50_done': False, 'tp100_done': False,
    }
    _log_trade(trades, 'BUY', code, name, shares, price, reason)
    return True


def _sell(pf, trades, code, price, reason, portion=1.0):
    h = pf['holdings'].get(code)
    if not h:
        return
    shares = h['shares'] if portion >= 1.0 else max(1, int(h['shares'] * portion))
    pf['cash'] += shares * price
    _log_trade(trades, 'SELL', code, h['종목명'], shares, price, reason)
    h['shares'] -= shares
    if h['shares'] <= 0:
        del pf['holdings'][code]


def current_prices(df):
    m = {}
    for _, r in df.iterrows():
        p = r.get('현재가', 0) or 0
        if p > 0:
            m[r['종목코드']] = float(p)
    return m


def portfolio_value(pf, prices):
    total = pf['cash']
    for code, h in pf['holdings'].items():
        total += h['shares'] * prices.get(code, h['avg_price'])
    return total


# ============================================================
# 일일 평가
# ============================================================
def daily_valuation(pf, df):
    prices = current_prices(df)
    # 트레일링 고점 갱신
    for code, h in pf['holdings'].items():
        p = prices.get(code)
        if p and p > h.get('peak_price', 0):
            h['peak_price'] = p
    total = portfolio_value(pf, prices)
    history = _load_json(HISTORY_FILE, [])
    today = now_kst().strftime('%Y-%m-%d')
    entry = {
        'date': today, 'total': int(total), 'cash': int(pf['cash']),
        'ret_pct': round((total / INITIAL_CAPITAL - 1) * 100, 2),
        'holdings': {c: {'name': h['종목명'], 'shares': h['shares'],
                         'price': prices.get(c), 'avg': h['avg_price']}
                     for c, h in pf['holdings'].items()},
    }
    history = [e for e in history if e['date'] != today] + [entry]
    _save_json(HISTORY_FILE, history)
    print(f"평가액 {total:,.0f}원 ({entry['ret_pct']:+.2f}%) | 현금 {pf['cash']:,.0f}원")
    return total


# ============================================================
# 주간 리밸런싱
# ============================================================
def weekly_rebalance(pf, df):
    trades = _load_json(TRADES_FILE, [])
    prices = current_prices(df)
    ranked = rank_universe(df)
    if ranked.empty:
        print('[WARN] 채점 가능한 종목 없음 — 리밸런싱 생략')
        return
    rank_of = {r['종목코드']: i + 1 for i, r in ranked.iterrows()}
    score_of = {r['종목코드']: r['점수'] for _, r in ranked.iterrows()}
    today = now_kst()

    # ── 1) 매도 룰 점검 ─────────────────────────────────────
    for code in list(pf['holdings'].keys()):
        h = pf['holdings'][code]
        p = prices.get(code)
        if not p:
            continue
        ret = p / h['avg_price'] - 1
        dd = p / h.get('peak_price', p) - 1
        held_days = (today - datetime.datetime.strptime(h['bought'], '%Y-%m-%d').replace(tzinfo=KST)).days

        if ret <= STOP_LOSS:
            _sell(pf, trades, code, p, f'손절 룰: 수익률 {ret*100:+.1f}% ≤ -15%')
            continue
        if dd <= TRAILING_STOP:
            _sell(pf, trades, code, p, f'트레일링 스탑: 고점 대비 {dd*100:+.1f}% ≤ -20%')
            continue
        if ret >= TP2_TRIGGER and not h['tp100_done']:
            _sell(pf, trades, code, p, f'익절 2차: 수익률 {ret*100:+.1f}% ≥ +100% → 1/3 매도', portion=1/3)
            if code in pf['holdings']:
                pf['holdings'][code]['tp100_done'] = True
            continue
        if ret >= TP1_TRIGGER and not h['tp50_done']:
            _sell(pf, trades, code, p, f'익절 1차: 수익률 {ret*100:+.1f}% ≥ +50% → 1/3 매도', portion=1/3)
            if code in pf['holdings']:
                pf['holdings'][code]['tp50_done'] = True
            continue
        if held_days >= TIME_STOP_DAYS and ret < TIME_STOP_MIN_RET:
            _sell(pf, trades, code, p, f'시간 손절: {held_days}일 보유, 수익률 {ret*100:+.1f}% < +10%')
            continue

    # ── 2) 순위 탈락 교체 (히스테리시스) ─────────────────────
    held = set(pf['holdings'].keys())
    candidates = [r for _, r in ranked.iterrows() if r['종목코드'] not in held]
    for code in list(pf['holdings'].keys()):
        rk = rank_of.get(code, 999)
        sc = score_of.get(code, 0)
        if rk > RANK_CUT and candidates:
            best = candidates[0]
            if best['점수'] - sc >= HYSTERESIS:
                p = prices.get(code)
                if p:
                    _sell(pf, trades, code, p,
                          f'순위 탈락: 현재 {rk}위(점수 {sc}) → {best["종목명"]}(점수 {best["점수"]})로 교체')

    # ── 3) 빈 슬롯 채우기 ───────────────────────────────────
    held = set(pf['holdings'].keys())
    n_needed = N_HOLDINGS - len(held)
    if n_needed > 0:
        sector_count = {}
        for code in held:
            row = df[df['종목코드'] == code]
            sec = row.iloc[0].get('업종', '기타') if not row.empty else '기타'
            sector_count[sec] = sector_count.get(sec, 0) + 1
        total_eq = portfolio_value(pf, prices)
        budget_each = total_eq * TARGET_INVEST_RATIO / N_HOLDINGS
        picks = pick_top(ranked, n_needed, exclude_codes=held, sector_count=sector_count)
        for r in picks:
            budget = min(budget_each, pf['cash'])
            d = r['세부']
            reason = (f"신규 선정: 점수 {r['점수']} "
                      f"(성장 {d['성장']}/30, 밸류 {d['밸류']}/20, 수급 {d['수급']}/20, "
                      f"기술 {d['기술']}/20, 유동성 {d['유동성']}/10)")
            _buy(pf, trades, r['종목코드'], r['종목명'], r['현재가'], budget, reason)

    pf['last_rebalance'] = today.strftime('%Y-%m-%d')
    _save_json(TRADES_FILE, trades)


# ============================================================
# 엔트리포인트
# ============================================================
def cmd_init():
    if os.path.exists(PORTFOLIO_FILE):
        print('이미 포트폴리오가 있습니다. --status 로 확인하세요.')
        return
    df = load_universe()
    pf = {'cash': float(INITIAL_CAPITAL), 'holdings': {},
          'started': now_kst().strftime('%Y-%m-%d'), 'last_rebalance': ''}
    print(f'💰 모의투자 시작 — 초기 자본 {INITIAL_CAPITAL:,}원, 최적 {N_HOLDINGS}종목 선정\n')
    weekly_rebalance(pf, df)
    _save_json(PORTFOLIO_FILE, pf)
    daily_valuation(pf, df)
    _save_json(PORTFOLIO_FILE, pf)


def cmd_auto():
    if not os.path.exists(CSV_FILE):
        print('[WARN] CSV 없음 — 건너뜀'); return
    if not os.path.exists(PORTFOLIO_FILE):
        cmd_init(); return
    df = load_universe()
    pf = _load_json(PORTFOLIO_FILE, None)
    last = pf.get('last_rebalance') or pf.get('started')
    days = (now_kst().date() - datetime.datetime.strptime(last, '%Y-%m-%d').date()).days
    if days >= REBALANCE_DAYS:
        print(f'📅 마지막 리밸런싱 {last} ({days}일 경과) → 주간 평가 실행\n')
        weekly_rebalance(pf, df)
    daily_valuation(pf, df)
    _save_json(PORTFOLIO_FILE, pf)


def cmd_status():
    pf = _load_json(PORTFOLIO_FILE, None)
    if not pf:
        print('포트폴리오 없음 — --init 으로 시작하세요.'); return
    df = load_universe()
    prices = current_prices(df)
    total = portfolio_value(pf, prices)
    print(f"시작일 {pf['started']} | 마지막 리밸런싱 {pf['last_rebalance']}")
    print(f"평가액 {total:,.0f}원 ({(total/INITIAL_CAPITAL-1)*100:+.2f}%) | 현금 {pf['cash']:,.0f}원")
    for code, h in pf['holdings'].items():
        p = prices.get(code, h['avg_price'])
        ret = (p / h['avg_price'] - 1) * 100
        print(f"  {h['종목명']}({code}) {h['shares']:,}주 @ {h['avg_price']:,.0f} "
              f"→ {p:,.0f} ({ret:+.1f}%) 고점 {h['peak_price']:,.0f}")


if __name__ == '__main__':
    if '--init' in sys.argv:
        cmd_init()
    elif '--status' in sys.argv:
        cmd_status()
    else:
        cmd_auto()
