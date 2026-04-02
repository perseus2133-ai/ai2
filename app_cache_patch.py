#!/usr/bin/env python3
# ============================================================
# app.py 수정 부분 — 기존 캐시 함수 3개를 아래로 교체
# ============================================================
# 교체 위치: 기존 CACHE_DIR, CACHE_FILE, save_cache, load_cache, get_cache_info 블록
# ============================================================

import json  # 상단 import 목록에 추가

# ── 저장 경로 ──────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
CSV_FILE  = os.path.join(DATA_DIR, "consensus_data.csv")
META_FILE = os.path.join(DATA_DIR, "meta.json")

# 기존 CACHE_FILE (pickle) 삭제 — 아래 두 줄 제거
# CACHE_DIR = os.path.dirname(os.path.abspath(__file__))
# CACHE_FILE = os.path.join(CACHE_DIR, "consensus_cache.pkl")


def save_cache(data_df: pd.DataFrame, meta: dict):
    """크롤링 결과를 CSV + JSON 메타로 저장 (git 추적 가능)"""
    os.makedirs(DATA_DIR, exist_ok=True)
    data_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
    meta['timestamp'] = datetime.datetime.now().isoformat()
    with open(META_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def load_cache():
    """CSV + JSON 메타 파일에서 캐시 로드. 없으면 None 반환"""
    if not os.path.exists(CSV_FILE):
        return None
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        meta = {}
        if os.path.exists(META_FILE):
            with open(META_FILE, 'r', encoding='utf-8') as f:
                meta = json.load(f)
        ts_str = meta.get('timestamp', '')
        ts = datetime.datetime.fromisoformat(ts_str) if ts_str else datetime.datetime.now()
        return {'data': df, 'meta': meta, 'timestamp': ts}
    except Exception:
        return None


def get_cache_info():
    """캐시 파일 정보 반환"""
    cache = load_cache()
    if cache is None:
        return None
    return {
        'timestamp':    cache['timestamp'],
        'total_stocks': len(cache['data']),
        'meta':         cache.get('meta', {}),
    }
