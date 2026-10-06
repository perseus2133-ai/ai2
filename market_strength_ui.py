"""시장을 이기는 종목 탭. 기존 필터 결과만 조회하며 활성 탭에서만 실행한다."""
import datetime as dt
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st

from market_strength import HORIZONS, KST, compare_market, comparison_cutoff, select_winners
from picks_performance import fetch_closes


@st.cache_data(ttl=600, max_entries=4096, show_spinner=False)
def load_quotes(kind, symbol, start, end):
    """성공·실패 응답을 10분 캐시하여 기간 전환 시 외부 요청을 반복하지 않는다."""
    valid = (kind == 'index' and symbol in ('KOSPI', 'KOSDAQ')) or (
        kind == 'item' and re.fullmatch(r'[0-9A-Z]{6}', symbol))
    if not valid:
        return {'prices': {}, 'error': '종목코드 확인 필요'}
    try:
        prices = fetch_closes(kind, symbol, dt.date.fromisoformat(start), dt.date.fromisoformat(end))
        return {'prices': prices, 'error': ''}
    except (requests.RequestException, ValueError, KeyError, TypeError, OverflowError):
        return {'prices': {}, 'error': '시세 조회 실패'}


def collect_comparison(candidates, data_timestamp, progress=None):
    cutoff = comparison_cutoff(data_timestamp)
    start = (cutoff - dt.timedelta(days=120)).isoformat()
    end = cutoff.isoformat()
    index = load_quotes('index', 'KOSPI', start, end)
    if index['error'] or not index['prices']:
        raise ValueError('코스피 지수를 불러오지 못했습니다. 잠시 후 다시 확인해주세요.')
    # 지수 한쪽에 거래일 누락이 있으면 N거래일을 잘못 세므로 게시하지 않는다.
    calendar_check = load_quotes('index', 'KOSDAQ', start, end)
    if calendar_check['error'] or set(index['prices']) != set(calendar_check['prices']):
        raise ValueError('지수 거래일을 검증하지 못했습니다. 잠시 후 다시 확인해주세요.')
    dates = sorted(index['prices'])
    stock_start, stock_end = dates[max(0, len(dates)-31)], dates[-1]
    codes = sorted(set(candidates['종목코드'].astype(str).str.zfill(6)))
    prices, errors = {}, {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(load_quotes, 'item', code, stock_start, stock_end): code for code in codes}
        for i, future in enumerate(as_completed(futures), 1):
            code, result = futures[future], future.result()
            prices[code] = result['prices']
            if result['error']:
                errors[code] = result['error']
            if progress:
                progress(i, len(codes))
    return compare_market(candidates, index['prices'], prices, cutoff, errors)


def render_strength_report(frame, periods, data_timestamp, render_card=None):
    st.subheader('📈 시장을 이기는 종목')
    st.caption('사이드바 필터 통과 종목만 비교합니다. 코스피·코스닥 종목 모두 코스피 대비입니다.')
    controls, option = st.columns([3, 2])
    with controls:
        selected = st.radio('비교 기간', ['5거래일', '15거래일', '30거래일', '세 기간 모두'],
                            horizontal=True, key='market_horizon')
    with option:
        positive_only = st.checkbox('주가도 오른 종목만', value=False, key='market_positive',
                                    help='선택한 각 기간의 종목 수익률도 0%보다 큰 종목만 표시합니다.')
    horizons = HORIZONS if selected == '세 기간 모두' else (int(selected.replace('거래일', '')),)
    winners = select_winners(frame, horizons, positive_only)
    a, *metrics = st.columns(4)
    a.metric('기존 필터 통과', f'{len(frame):,}개')
    for n, column in zip(HORIZONS, metrics):
        count = len(select_winners(frame, (n,), positive_only))
        column.metric(f'{n}거래일 코스피 초과', f'{count:,}개')
    timestamp = pd.Timestamp(data_timestamp)
    timestamp = timestamp.tz_localize(KST) if timestamp.tzinfo is None else timestamp.tz_convert(KST)
    st.caption(f"종가 기준일 {periods[5]['end']} · 재무·유동성 데이터 수집 "
               f"{timestamp.strftime('%Y-%m-%d %H:%M')} KST")
    for n in horizons:
        period = periods[n]
        if period['start']:
            st.caption(f"{n}거래일: {period['start']} → {period['end']} · "
                       f"코스피 {period['market_return']:+.2f}%")

    if winners.empty:
        valid = frame[[f'status_{n}' for n in horizons]].eq('완료').all(axis=1).sum()
        if not valid:
            st.warning('선택한 기간을 계산할 가격 데이터가 없습니다. 아래 조회 상태를 확인해주세요.')
        else:
            st.info('이 기간에 조건을 충족한 종목이 없습니다. 기간 또는 사이드바 필터를 조정해보세요.')
    else:
        sort = st.selectbox('결과 정렬', ['코스피 초과수익 순', '매출+영업이익 성장 순'], key='market_sort')
        if sort == '매출+영업이익 성장 순' and '매출영익_합산성장' in winners:
            winners = winners.sort_values(['매출영익_합산성장', '최소초과수익'], ascending=False,
                                           na_position='last').reset_index(drop=True)
        if len(horizons) == 3 and sort == '코스피 초과수익 순':
            st.caption('세 기간을 모두 이긴 종목 중, 가장 작은 초과수익이 큰 순서입니다.')
        st.markdown(f'**{selected} · {len(winners):,}개 종목**')
        identity = [c for c in ('종목명', '종목코드', '시장', '업종', '평가종가') if c in winners]
        table = winners[identity].copy()
        table.insert(0, '순위', range(1, len(table)+1))
        number_columns = {}
        for n in horizons:
            returns = f'{n}일 수익률 (%)'
            excess = f'{n}일 초과 (%p)'
            table[returns] = winners[f'return_{n}']
            if len(horizons) == 1:
                table['코스피 수익률 (%)'] = winners[f'market_{n}']
            table[excess] = winners[f'excess_{n}']
            number_columns[returns] = st.column_config.NumberColumn(returns, format='%+.2f%%')
            number_columns[excess] = st.column_config.NumberColumn(excess, format='%+.2f',
                help='종목 수익률 − 같은 기간 코스피 수익률. 단위는 퍼센트포인트입니다.')
        if len(horizons) == 1:
            for col, label in [('매출_CAGR', '매출 CAGR (%)'), ('영업이익_CAGR', '영업이익 CAGR (%)'),
                               ('영업이익_26이후_최대', '영업이익 최대 (억)')]:
                if col in winners:
                    table[label] = winners[col]
                    number_columns[label] = st.column_config.NumberColumn(label, format='%.1f')
        table['네이버'] = winners['종목코드'].map(lambda c: f'https://m.stock.naver.com/domestic/stock/{c}/total')
        st.dataframe(table, hide_index=True, width='stretch', height=min(620, 38+len(table)*35),
                     column_config={**number_columns,
                         '코스피 수익률 (%)': st.column_config.NumberColumn('코스피 수익률 (%)', format='%+.2f%%'),
                         '평가종가': st.column_config.NumberColumn('평가종가 (원)', format='localized'),
                         '네이버': st.column_config.LinkColumn('네이버', display_text='종목 보기')})
        if render_card:
            names = dict(zip(winners['종목코드'], winners.get('종목명', winners['종목코드'])))
            if st.session_state.get('market_detail') not in names:
                st.session_state['market_detail'] = None
            code = st.selectbox('종목 상세 보기', list(names), index=None, placeholder='종목을 선택하면 기존 상세 카드가 표시됩니다',
                                format_func=lambda c: f'{names[c]} · {c}', key='market_detail')
            if code:
                row = winners.loc[winners['종목코드'].eq(code)].iloc[0]
                render_card(row, int(winners.index[winners['종목코드'].eq(code)][0])+1)

    missing = frame[[f'status_{n}' for n in horizons]].ne('완료').any(axis=1)
    if missing.any():
        with st.expander(f'가격 데이터 부족·조회 실패 {missing.sum():,}개 확인'):
            columns = [c for c in ('종목명', '종목코드') if c in frame] + [f'status_{n}' for n in horizons]
            st.dataframe(frame.loc[missing, columns].rename(columns={f'status_{n}': f'{n}일 상태' for n in horizons}),
                         hide_index=True, width='stretch')
    with st.expander('어떤 기준으로 시장을 이겼다고 보나요?'):
        st.markdown(
            '수익률은 **(종료일 종가 ÷ 시작일 종가 − 1) × 100**입니다. 코스피 거래일로 '
            '5·15·30일을 세며 종목과 지수의 시작일·종료일을 맞춥니다. 주말·휴일과 수집 당일은 제외합니다. '
            '매일 시장을 이겼다는 의미가 아니라, 선택 기간의 누적 수익률이 앞섰다는 의미입니다.\n\n'
            '**코스피 −5%, 종목 −2%도 +3%p 초과**입니다. 실제 상승 종목만 원하면 '
            '“주가도 오른 종목만”을 켜세요. 동률은 포함하지 않습니다.\n\n'
            '기존 성장률 필터는 매출 또는 영업이익 성장 기준 중 하나 이상 충족하는 방식입니다. '
            '영업이익 규모·흑자·거래대금 등 나머지 선택 필터도 기존 스크리너와 동일하게 적용됩니다.\n\n'
            '종목이나 지수의 가격이 부족하면 다른 날짜 가격으로 채우지 않습니다. 네이버 차트 종가에 '
            '기반한 가격수익률이며 배당·비용은 포함하지 않습니다. 기업행사에 따른 과거 가격 조정 여부는 '
            '별도 검증하지 않았습니다. 코스닥 종목도 코스피를 기준으로 비교하므로 시장 성격 차이가 있습니다.'
        )


def render_market_strength(candidates, data_timestamp, render_card=None):
    if candidates.empty:
        st.info('기존 필터를 통과한 종목이 없습니다. 사이드바 조건을 조정해주세요.')
        return
    progress = st.progress(0., text=f'필터 통과 {len(candidates):,}개 종목의 종가를 확인하고 있습니다…')
    try:
        frame, periods = collect_comparison(candidates, data_timestamp,
            progress=lambda done, total: progress.progress(done/total, text=f'종가 확인 {done:,} / {total:,}'))
    except ValueError as exc:
        st.warning(str(exc))
        return
    finally:
        progress.empty()
    render_strength_report(frame, periods, data_timestamp, render_card)
