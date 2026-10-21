"""3a 시간순 데이터셋 테스트. 누수 규칙은 단위 테스트, dbt 결과는 서비스와 .venv-dbt가 있을 때만."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg

from pipelines.modeling.dataset import _recompute, coverage, load_dataset, verify_point_in_time
from tests.test_dbt_integration import build, dbt_settings, ingest  # noqa: F401 (fixture 재사용)
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings  # noqa: F401

T = datetime(2025, 9, 1, 15, 0, tzinfo=timezone.utc)


def test_results_are_usable_only_three_hours_after_kickoff():
    history = [
        (T - timedelta(hours=2, minutes=59), 1, 5, 0, 3),   # 아직 결과가 알려지지 않음 → 제외
        (T - timedelta(hours=3), 2, 2, 1, 3),               # 딱 3시간 전 시작 → 포함
        (T - timedelta(days=7), 3, 0, 2, 0),
        (T + timedelta(days=1), 4, 9, 9, 3),                # 미래 경기 → 제외
    ]
    features = _recompute(history, T, match_id=99)
    assert features["l10_n"] == 2 and features["l10_gf"] == Decimal(1) and features["l10_pts"] == Decimal("1.5")
    assert _recompute(history, T, match_id=2)["l10_n"] == 1   # 대상 경기 자체는 넣지 않음
    assert _recompute([], T, match_id=1) == {"l10_n": 0, "l10_gf": None, "l10_ga": None, "l10_pts": None,
                                              "l38_n": 0, "l38_gf": None, "l38_ga": None}


def test_last_ten_matches_only():
    history = [(T - timedelta(days=d), d, d, 0, 3) for d in range(1, 16)]
    features = _recompute(history, T, match_id=0)
    assert features["l10_n"] == 10 and features["l10_gf"] == Decimal("5.5")  # 1~10일 전 경기만
    assert features["l38_n"] == 15


def test_dataset_values_and_independent_check(dbt_settings):
    s = dbt_settings
    ingest(s, SAMPLE_PATH.read_bytes(), "2025-08-17T00:00:00+00:00")
    build(s, "--select", "+match_features_pit")
    with psycopg.connect(s.postgres_conninfo) as conn:
        rows = {(r["home_team"], r["away_team"]): r for r in load_dataset(conn, finished_only=False)}
        assert load_dataset(conn, finished_only=False, competitions=("de.1",)) == []   # 리그 거르기(5a)
        assert len(load_dataset(conn, finished_only=False, competitions=("en.1",))) == len(rows)
        check = verify_point_in_time(conn)
        seasons = coverage(conn)
    # Delta FC - Alpha FC(예정): Delta의 이전 경기 Gamma 0-0 Delta, Alpha의 이전 경기 Alpha 2-1 Beta
    upcoming = rows[("Delta FC", "Alpha FC")]
    assert (upcoming["home_l10_n"], upcoming["home_l10_gf"], upcoming["home_l10_pts"]) == (1, Decimal("0"), Decimal("1"))
    assert (upcoming["away_l10_n"], upcoming["away_l10_gf"], upcoming["away_l10_ga"]) == (1, Decimal("2"), Decimal("1"))
    assert upcoming["result"] is None and upcoming["mode"] == "HISTORICAL_RECONSTRUCTION"
    # 시즌 첫 경기는 이전 기록이 없다(0경기, 평균 없음).
    first = rows[("Alpha FC", "Beta FC")]
    assert (first["home_l10_n"], first["home_l10_gf"], first["result"]) == (0, None, "HOME")
    # 시작 시각이 없는 Alpha FC - Gamma FC는 "시작 전에 무엇을 알았는지"를 정할 수 없어 데이터셋에서 빠진다.
    assert ("Alpha FC", "Gamma FC") not in rows
    assert check == {"rows_checked": 4, "mismatches": 0, "examples": []}
    assert seasons[0]["matches"] == 4 and seasons[0]["finished"] == 3
