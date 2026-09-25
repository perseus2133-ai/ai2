"""Streamlit integration, separate from the durable storage engine."""
import html
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from stock_candles import load_candles, prefetch_candles
from watchlist import WatchStore, make_entry, number, opinion, summary


def _eok(value):
    if value is None:
        return '-'
    if abs(value) >= 10_000:
        return f'{value / 10_000:,.2f}조'
    return f'{value:,.0f}억'


def _price(value):
    return f'{value:,.0f}원' if value is not None else '-'


def _pct(value):
    return f'{value:+.1f}%' if value is not None else '-'


def _forecast(first, last):
    return f'{_eok(first)} → {_eok(last)}'


def _summary_html(entries, summaries):
    cards = []
    for entry in entries:
        code = entry['code']
        item = summaries[code]
        signal = item.get('signal') or {'status': 'insufficient'}
        status = signal.get('status')
        if status == 'confirmed':
            signal_label = '🚀 신호 확인'
            signal_detail = f'{html.escape(signal["date"])} · {signal["turnover_multiple"]:.1f}배'
            signal_class = 'confirmed'
        elif status == 'pending':
            signal_label = '⏳ 확인 대기'
            signal_detail = f'{html.escape(signal["date"])} 돌파 · 다음 종가 대기'
            signal_class = 'pending'
        elif status in ('stale', 'insufficient'):
            signal_label = '판정 보류'
            signal_detail = '일봉 부족·지연' if status == 'stale' else '일봉·거래량 부족'
            signal_class = 'unavailable'
        else:
            signal_label = '신호 없음'
            signal_detail = '조건 미충족'
            signal_class = 'none'
        name = html.escape(entry['name'])
        code_html = html.escape(code)
        source = html.escape(item['price_source'])
        price_date = html.escape(item['price_date'])
        low_date = html.escape(item['low_60d_date'])
        selected_date = html.escape(entry['selected_at'][:10])
        selected_short = html.escape(entry['selected_at'][2:10].replace('-', '.'))
        price_label = '현재' if item['price_source'] == '캐시 현재가' else '종가'
        change = item['since_selected_pct']
        low_change = item['since_low_pct']
        change_class = 'up' if change is not None and change >= 0 else 'down'
        low_class = 'up' if low_change is not None and low_change >= 0 else 'down'
        volume = f'{item["volume"]:,.0f}주' if item['volume'] is not None else '-'
        volume_ratio = (f' ({item["volume_multiple"]:.1f}배/20일 평균)'
                        if item['volume_multiple'] is not None else '')
        turnover = _eok(item['turnover_eok'])
        stale = (' <span class="watch-sum-stale">시총·재무는 지정 당시</span>'
                 if item['financial_source'] != '현재 수집 데이터' else '')
        cards.append(
            f'<div class="watch-sum-row {"is-signal" if status == "confirmed" else ""}">'
            f'<div class="watch-sum-signal {signal_class}"><strong>{signal_label}</strong>'
            f'<small>{signal_detail}</small></div>'
            f'<div class="watch-sum-main">'
            f'<div class="watch-sum-line">'
            f'<strong class="watch-sum-name">{name}</strong><span class="watch-sum-code">{code_html}</span>'
            f'<span title="지정일 {selected_date} · 기준종가 {_price(number(entry.get("price")))}">'
            f'지정 {selected_short} <small>기준 {_price(number(entry.get("price")))}</small></span>'
            f'<span title="{source} · {price_date}">{price_label} {_price(item["price"])}</span>'
            f'<span class="{change_class}">지정 후 {_pct(change)}</span>'
            f'<span title="장중 저가 · {low_date}">60일 저점 {_price(item["low_60d"])}</span>'
            f'<span class="{low_class}">저점 대비 {_pct(low_change)}</span>'
            f'</div>'
            f'<div class="watch-sum-line watch-sum-secondary">'
            f'<span>시총 {_eok(item["market_cap"])}</span>'
            f'<span>거래량 {volume}{volume_ratio}</span>'
            f'<span>거래대금≈ {turnover}</span>'
            f'<span>매출 26E→28E {_forecast(item["revenue_2026"], item["revenue_2028"])}</span>'
            f'<span>영익 26E→28E {_forecast(item["op_2026"], item["op_2028"])}</span>'
            f'{stale}</div></div></div>'
        )
    return '<div class="watch-sum-list">' + ''.join(cards) + '</div>'


_SUMMARY_STYLE = '''<style>
.watch-sum-list { display:flex; flex-direction:column; gap:6px; margin-bottom:14px; }
.watch-sum-row { display:grid; grid-template-columns:130px minmax(0,1fr); gap:10px;
                 align-items:center; background:#263647; border:1px solid #56697D;
                 border-radius:9px; padding:9px 12px; color:#EAF2FC; }
.watch-sum-row.is-signal { background:linear-gradient(110deg,#473B22,#263647 42%);
                            border:2px solid #FBBF24; box-shadow:0 0 0 2px rgba(251,191,36,.12); }
.watch-sum-main { min-width:0; }
.watch-sum-signal { min-height:48px; border-radius:7px; padding:5px 7px;
                    display:flex; flex-direction:column; justify-content:center;
                    align-items:center; gap:2px; text-align:center; background:#1A2735;
                    color:#AEC1D3; font-size:.77rem; }
.watch-sum-signal small { font-size:.65rem; line-height:1.2; }
.watch-sum-signal.confirmed { background:#FBBF24; color:#1A1C24; }
.watch-sum-signal.pending { background:#286A83; color:#FFFFFF; }
.watch-sum-signal.unavailable { color:#889CAE; }
.watch-sum-line { display:flex; flex-wrap:wrap; align-items:center; gap:5px 9px;
                  font-size:0.82rem; line-height:1.4; }
.watch-sum-line + .watch-sum-line { margin-top:4px; }
.watch-sum-name { font-size:0.95rem; color:#FFFFFF; }
.watch-sum-code, .watch-sum-line small { color:#AFC0D3; font-size:0.72rem; }
.watch-sum-secondary { color:#C2D0DE; font-size:0.76rem; }
.watch-sum-line .up { color:#34D399; font-weight:700; }
.watch-sum-line .down { color:#F87171; font-weight:700; }
.watch-sum-stale { color:#FBBF24; }
@media (max-width:768px) {
  .watch-sum-row { grid-template-columns:1fr; gap:6px; }
  .watch-sum-signal { min-height:0; flex-direction:row; justify-content:flex-start;
                      align-items:center; gap:8px; }
}
</style>'''


def initialize(base_dir):
    st.session_state['_watch_keys'] = {}
    try:
        path = os.environ.get('AI2_WATCHLIST_DB', str(Path(base_dir) / 'private_data' / 'watchlist.sqlite3'))
        store = WatchStore(path)
        st.session_state['_watch_entries'] = store.entries()
        st.session_state['_watch_store'] = store
    except Exception:
        st.session_state['_watch_store'] = None
        st.session_state['_watch_entries'] = {}
        st.error('관심목록 저장소를 읽을 수 없습니다. 기존 파일은 초기화하지 않았습니다.')


def _add_entry(row, candles, today):
    """Run before Streamlit's normal button rerun so the list is current in that run."""
    code = str(row['종목코드']).zfill(6)
    st.session_state.pop(f'_watch_add_error_{code}', None)
    try:
        store = st.session_state['_watch_store']
        candles = candles or load_candles(code, today)
        entry = make_entry(row, candles, st.session_state.get('_watch_data_as_of', '확인 불가'))
        store.add(entry)
    except (ValueError, OSError) as exc:
        st.session_state[f'_watch_add_error_{code}'] = str(exc)
    except Exception:
        st.session_state[f'_watch_add_error_{code}'] = '관심종목을 저장하지 못했습니다. 저장소 상태를 확인해 주세요.'


def card_button(row, candles, today):
    store = st.session_state.get('_watch_store')
    if store is None:
        return
    code = str(row['종목코드']).zfill(6)
    counts = st.session_state['_watch_keys']
    counts[code] = counts.get(code, 0) + 1
    selected = code in st.session_state['_watch_entries']
    st.button('★ 관심종목 등록됨' if selected else '☆ 관심종목 등록',
              key=f'watch_add_{code}_{counts[code]}', disabled=selected,
              on_click=_add_entry if not selected else None, args=(row, candles, today))
    error = st.session_state.get(f'_watch_add_error_{code}')
    if error:
        st.error(error)


def _backup_controls(store):
    with st.expander('관심목록 백업 및 복원'):
        st.download_button('JSON 백업 다운로드', store.backup(), 'ai2_watchlist.json', 'application/json')
        upload = st.file_uploader('백업 복원 · 기존 종목의 최초 기록은 유지합니다', type=['json'], key='watch_restore_file')
        if st.button('백업 병합 복원', disabled=upload is None):
            try:
                if upload.size > 10_000_000:
                    raise ValueError('백업은 10MB 이하여야 합니다.')
                store.restore(upload.getvalue())
                st.rerun()
            except Exception:
                st.error('복원에 실패했습니다. 백업 형식과 저장소를 확인해 주세요. 기존 기록은 유지됩니다.')


def render_watchlist(all_df, render_card, prepare, data_as_of):
    st.subheader('⭐ 관심종목')
    st.caption('지정일은 등록 시각입니다. 기준 가격은 등록 당시 확인 가능한 최근 완료 거래일 종가이며, 당일 체결가가 아닙니다. 등락률은 배당·수수료·기업행사를 보정하지 않은 참고 값입니다.')
    store = st.session_state.get('_watch_store')
    if store is None:
        return
    entries = st.session_state['_watch_entries']
    if not entries:
        st.info('기존 종목 카드의 ☆ 관심종목 등록 버튼으로 추가해 주세요.')
        _backup_controls(store)
        return
    st.subheader('관심종목 한눈에 보기')
    query = st.text_input('관심종목 검색', key='watch_search').strip().lower()
    entries = [e for e in entries.values() if query in (e['name'] + e['code']).lower()]
    st.caption(f'{len(entries)}개 · 요약 정렬을 바꿔 비교할 수 있습니다. 재등록 시 기준일과 기준 가격이 새로 설정됩니다.')
    if not entries:
        st.info('검색 조건에 맞는 관심종목이 없습니다.')
        return
    with st.spinner('관심종목 요약에 사용할 시세를 불러오는 중입니다…'):
        charts = prefetch_candles([e['code'] for e in entries])
    current = all_df.copy()
    current['__code'] = current['종목코드'].astype(str).str.zfill(6)
    rows_by_code = {row['__code']: row for _, row in current.iterrows()}
    summaries = {e['code']: summary(e, rows_by_code.get(e['code']), charts.get(e['code']), data_as_of)
                 for e in entries}

    st.caption(f'현재가·시총 등 캐시 자료의 수집 시점은 {data_as_of}입니다. 60일 저점은 최근 60일(달력일) 완료 일봉의 장중 저가입니다. 캐시 가격이 없거나 일봉보다 오래되면 최근 완료 종가를 씁니다. 거래대금은 표시 가격×해당 거래량 추정치입니다.')
    st.caption('🚀 신호 확인: 완료 일봉 종가가 직전 20거래일 고점을 돌파하고 그날 추정 거래대금이 이전 20거래일 평균의 2배 이상이며, 다음 거래일에도 그 고점 위에서 마감한 경우입니다. 최근 5거래일 신호만 표시하며 종가가 고점 아래로 내려오면 해제합니다. 매수 신호가 아닌 관찰용입니다.')
    sort_options = {'거래량 증가순': 'volume_multiple', '거래대금순': 'turnover_eok',
                    '지정 후 상승률순': 'since_selected_pct', '60일 저점 반등순': 'since_low_pct'}
    sort_label = st.selectbox('요약 정렬', ['신호 우선', '최근 등록순', *sort_options], key='watch_summary_sort')
    if sort_label == '신호 우선':
        signal_rank = {'confirmed': 2, 'pending': 1}
        entries.sort(key=lambda e: (signal_rank.get(summaries[e['code']]['signal']['status'], 0),
                                    summaries[e['code']]['signal'].get('turnover_multiple', 0)), reverse=True)
    elif sort_label in sort_options:
        field = sort_options[sort_label]
        entries.sort(key=lambda e: summaries[e['code']][field]
                     if summaries[e['code']][field] is not None else float('-inf'), reverse=True)
    st.markdown(_SUMMARY_STYLE, unsafe_allow_html=True)
    st.markdown(_summary_html(entries, summaries), unsafe_allow_html=True)
    st.info('동일 앱 비밀번호를 사용하는 분들이 공유하는 목록입니다. 서버 디스크에 저장되므로 재배포·서버 교체 시 유실될 수 있습니다. 영구 디스크 설정 또는 아래 JSON 백업을 사용해 주세요.')
    _backup_controls(store)

    page = st.number_input('관심목록 페이지', min_value=1, max_value=max(1, (len(entries)+9)//10), value=1, step=1, key='watch_page')
    page_entries = entries[(page-1)*10:page*10]
    for rank, entry in enumerate(page_entries, (page-1)*10+1):
        code = entry['code']; chart = charts.get(code, {})
        hits = current[current['__code'] == code].drop(columns='__code')
        st.markdown(f"### {entry['name']} · {code}")
        st.write(f"지정일 {entry['selected_at'][:10]} · 기준 종가 {entry['price']:,.0f}원 ({entry['price_date']})")
        rows = chart.get('rows', [])
        if rows and rows[-1]['date'] >= entry['price_date']:
            latest = rows[-1]
            change = (latest['close'] / entry['price'] - 1) * 100
            st.write(f"최근 완료 종가 {latest['close']:,.0f}원 ({latest['date']}) · 지정 기준 대비 {change:+.2f}%")
        else:
            st.warning('최신 비교 시세가 없어 등락률을 표시하지 않습니다.')
        if hits.empty:
            st.warning('현재 데이터에서 제외된 종목입니다. 아래 재무·수급 정보는 지정 당시 자료입니다.')
            row = pd.Series(entry['snapshot'])
        else:
            row = prepare(hits).iloc[0]
        st.caption(f"재무·수급 데이터 수집 시점: {data_as_of if not hits.empty else entry['data_as_of']} · 일봉 기준일과 다를 수 있습니다.")
        render_card(row, rank, chart)
        st.markdown('#### AI 의견 · 지표 기반 자동 해설')
        st.write(opinion(row, chart))
        with st.expander('지정 당시 의견 및 실적과 비교'):
            st.caption(f"지정 당시 데이터 수집 시점: {entry['data_as_of']}")
            st.write(entry['initial_opinion'])
            comparisons = []
            for year in (2026, 2027, 2028):
                for field in ('매출액', '영업이익'):
                    key = f'{field}_{year}'
                    before = number(entry['snapshot'].get(key))
                    after = number(row.get(key)) if not hits.empty else None
                    comparisons.append({'항목': f'{year}E {field} (억원)', '지정 당시': before,
                                        '현재': after, '변화': after-before if after is not None and before is not None else None})
            st.dataframe(pd.DataFrame(comparisons), hide_index=True, use_container_width=True)
        confirm = st.checkbox('이 종목의 지정 기록 삭제 확인', key=f'watch_confirm_{code}')
        if st.button('관심목록에서 해제', key=f'watch_remove_{code}', disabled=not confirm):
            try:
                store.remove(code)
                st.rerun()
            except Exception:
                st.error('해제하지 못했습니다. 저장소를 확인해 주세요.')
        st.divider()
