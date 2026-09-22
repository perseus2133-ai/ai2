import datetime
import json

import pytest

from daily_picks import generate_daily_picks


def test_up_alignment_awards_three_points_and_reason(tmp_path, valid_frame):
    today = datetime.date(2026, 9, 23)
    up = generate_daily_picks(valid_frame, str(tmp_path / 'snaps'), str(tmp_path / 'up.json'), today=today)
    down_frame = valid_frame.copy()
    down_frame['MA_align'] = 'mixed'
    down = generate_daily_picks(down_frame, str(tmp_path / 'snaps'), str(tmp_path / 'down.json'), today=today)
    down_by_code = {r['code']: r for r in down}
    assert len(up) == len(down) == 6
    for r in up:
        assert r['score'] == pytest.approx(down_by_code[r['code']]['score'] + 3)
        assert any('정배열' in reason for reason in r['reasons'])


def test_no_candidates_persists_empty_today(tmp_path, valid_frame):
    path = tmp_path / 'picks.json'
    path.write_text(json.dumps({'2026-09-22': [{'code': '000001'}]}))
    df = valid_frame.copy()
    df['시가총액'] = 1
    assert generate_daily_picks(df, str(tmp_path), str(path), today=datetime.date(2026, 9, 23)) == []
    history = json.loads(path.read_text())
    assert history['2026-09-22']
    assert history['2026-09-23'] == []


def test_corrupt_history_is_preserved(tmp_path, valid_frame):
    path = tmp_path / 'picks.json'
    path.write_text('{broken')
    with pytest.raises(json.JSONDecodeError):
        generate_daily_picks(valid_frame, str(tmp_path / 'snap'), str(path), today=datetime.date(2026, 9, 23))
    assert path.read_text() == '{broken'
