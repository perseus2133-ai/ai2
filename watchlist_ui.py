"""Streamlit integration, separate from the durable storage engine."""
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from stock_candles import load_candles, prefetch_candles
from watchlist import WatchStore, make_entry, number, opinion


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


def render_watchlist(all_df, render_card, prepare, data_as_of):
    st.subheader('⭐ 관심종목')
    st.caption('지정일은 등록 시각입니다. 기준 가격은 등록 당시 확인 가능한 최근 완료 거래일 종가이며, 당일 체결가가 아닙니다. 등락률은 배당·수수료·기업행사를 보정하지 않은 참고 값입니다.')
    st.info('동일 앱 비밀번호를 사용하는 분들이 공유하는 목록입니다. 서버 디스크에 저장되므로 재배포·서버 교체 시 유실될 수 있습니다. 영구 디스크 설정 또는 아래 JSON 백업을 사용해 주세요.')
    store = st.session_state.get('_watch_store')
    if store is None:
        return
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
    entries = st.session_state['_watch_entries']
    if not entries:
        st.info('기존 종목 카드의 ☆ 관심종목 등록 버튼으로 추가해 주세요.')
        return
    query = st.text_input('관심종목 검색', key='watch_search').strip().lower()
    entries = [e for e in entries.values() if query in (e['name'] + e['code']).lower()]
    st.caption(f'{len(entries)}개 · 최신 등록순 · 재등록 시 기준일과 기준 가격이 새로 설정됩니다.')
    page = st.number_input('관심목록 페이지', min_value=1, max_value=max(1, (len(entries)+9)//10), value=1, step=1, key='watch_page')
    entries = entries[(page-1)*10:page*10]
    charts = prefetch_candles([e['code'] for e in entries])
    current = all_df.copy()
    current['__code'] = current['종목코드'].astype(str).str.zfill(6)
    for rank, entry in enumerate(entries, 1):
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
