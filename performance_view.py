"""기존 누적성과 아래에 붙이는 읽기 전용 성과 비교 화면."""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from picks_performance import history_digest


def render_report(report):
    st.caption('최초 선정 종목별 비교 · 코스피 종목은 KOSPI, 코스닥 종목은 KOSDAQ 대비')
    horizon = st.selectbox('비교 기간', ['latest', '5', '20', '60'],
                           format_func=lambda v: '최신 종가까지' if v == 'latest' else f'선정 후 {v}거래일',
                           key='picks_performance_horizon')
    market = st.radio('비교 시장', ['전체', 'KOSPI', 'KOSDAQ'], horizontal=True,
                      key='picks_performance_market')
    records = []
    for row in report['rows']:
        if market != '전체' and row.get('market') != market:
            continue
        period = row['periods'][horizon]
        records.append({'최초선정일': row['selected'], '종목명': row['name'], '시장': row['market'],
                        '기준거래일': row['base_date'], '평가거래일': period['end_date'],
                        '종목 수익률%': period['stock_return'], '시장 수익률%': period['market_return'],
                        '시장 대비%p': period['excess_pp'], '상태': period['status']})
    if not records:
        st.info('비교할 선정 기록이 없습니다.')
        return
    frame = pd.DataFrame(records)
    valid = frame[frame['상태'] == '완료']
    a, b, c, d = st.columns(4)
    a.metric('평가 가능 / 전체', f'{len(valid)} / {len(frame)}')
    b.metric('평균 종목 수익률', f"{valid['종목 수익률%'].mean():+.1f}%" if len(valid) else '—')
    c.metric('평균 시장 대비', f"{valid['시장 대비%p'].mean():+.1f}%p" if len(valid) else '—')
    d.metric('시장 초과 비율', f"{valid['시장 대비%p'].gt(0).mean():.0%}" if len(valid) else '—')
    missing = frame.loc[frame['상태'] != '완료', '상태'].value_counts()
    if len(missing):
        st.info('평균에서 제외: ' + ' · '.join(f'{label} {count}개' for label, count in missing.items()))
    st.dataframe(frame.sort_values('시장 대비%p', ascending=False, na_position='last'),
                 hide_index=True, width='stretch',
                 column_config={col: st.column_config.NumberColumn(col, format='%+.2f')
                                for col in ('종목 수익률%', '시장 수익률%', '시장 대비%p')})
    st.caption('지수 최신 거래일: ' + ' · '.join(f'{m} {d or "없음"}' for m, d in report['market_asof'].items()))
    with st.expander('계산 기준과 주의사항'):
        st.markdown(
            '- 기존 누적성과는 저장된 **최초 선정가** 기준이며 변경하지 않습니다. '
            '이 비교표는 종목과 지수 모두 **최초 선정일 직전 거래일 종가**로 출발점을 맞춥니다. '
            '따라서 두 표의 수익률은 다를 수 있습니다.\n'
            '- 5·20·60거래일은 기준일 다음부터 지수에 기록된 거래일로 셉니다. '
            '주말·휴일은 세지 않고, 종목 가격 누락 시 다른 날짜 가격으로 대체하지 않습니다.\n'
            '- 시장 대비는 **종목 수익률 − 같은 기간 지수 수익률(%p)**입니다. '
            '평균은 평가 가능한 종목의 단순평균이며 포트폴리오 복리수익률이 아닙니다. '
            '최신 종가까지의 비교는 종목별 기간이 다르고, 고정 기간끼리도 시장 국면은 다릅니다.\n'
            '- 네이버 차트의 사후 조회 종가를 사용합니다. 기업행사·가격 정정에 따라 과거 값이 '
            '바뀔 수 있고, 수정주가 여부를 독립적으로 검증하지 않았습니다. '
            '배당·비용·실제 체결가격은 반영하지 않는 참고용 가격수익률입니다.\n'
            '- 일별 재선정은 중복 표본으로 넣지 않습니다. 가격 누락 종목이 평균에서 빠지므로 '
            '누락 수와 상태를 함께 확인하세요. 2026-09-23 코드 수정 전후 선정 방식의 차이도 있습니다.'
        )


def render_performance(data_dir, history):
    st.subheader('동일 기간 · 시장 대비 성과')
    path = Path(data_dir) / 'daily_picks_performance.json'
    if not path.exists():
        st.info('비교 데이터가 아직 없습니다. 다음 성과 수집 완료 후 표시됩니다.')
        return
    try:
        report = json.loads(path.read_text(encoding='utf-8'))
        if report.get('schema_version') != 1:
            raise ValueError('unsupported schema')
        if report['history_digest'] != history_digest(history):
            st.warning('선정 기록이 갱신되어 성과 재계산이 필요합니다. 이전 비교표는 표시하지 않습니다.')
            return
        meta = json.loads((Path(data_dir) / 'meta.json').read_text(encoding='utf-8'))
        if report.get('selection_data_timestamp') != meta.get('timestamp'):
            st.warning('현재 수집 데이터와 성과 기준 시점이 다릅니다. 이전 비교표는 표시하지 않습니다.')
            return
        render_report(report)
    except (OSError, ValueError, KeyError, TypeError):
        st.warning('성과 데이터를 읽을 수 없습니다. 기존 누적성과는 위에서 확인할 수 있습니다.')
