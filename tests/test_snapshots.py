"""경기 전 스냅샷 테스트. decide는 순수 함수, run·build_inputs는 Postgres가 있을 때만(없으면 건너뜀)."""
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from pipelines import snapshots
from pipelines.ingest import openfootball
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

KICKOFF = datetime(2026, 5, 24, 15, 0, tzinfo=timezone.utc)  # 예시 원본의 Delta FC - Alpha FC(16:00 BST)


@pytest.mark.parametrize("now, recorded, expected", [
    (KICKOFF - timedelta(hours=30), set(), {}),                                    # 아직 아무 시점도 안 옴
    (KICKOFF - timedelta(hours=23), set(), {"T-24H": "TAKE"}),                     # 24시간 전 + 1시간(유예 안)
    (KICKOFF - timedelta(hours=22), set(), {"T-24H": "MISS"}),                     # 유예 90분 넘음
    (KICKOFF - timedelta(hours=2, minutes=30), set(), {"T-24H": "MISS", "T-3H": "TAKE"}),
    (KICKOFF - timedelta(hours=2, minutes=30), {"T-24H"}, {"T-3H": "TAKE"}),
    (KICKOFF - timedelta(minutes=30), {"T-24H", "T-3H"}, {"T-1H": "TAKE"}),
    (KICKOFF, {"T-24H", "T-3H"}, {"T-1H": "MISS"}),                                 # 시작 이후에는 찍지 않고 MISS
    (KICKOFF + timedelta(days=1), set(), {"T-24H": "MISS", "T-3H": "MISS", "T-1H": "MISS"}),
])
def test_decide(now, recorded, expected):
    assert snapshots.decide(KICKOFF, now, recorded) == expected


@pytest.fixture
def season(isolated_settings):
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=isolated_settings, received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    return isolated_settings


def rows(s):
    return query(s, """SELECT offset_label, status, as_of IS NOT NULL FROM snapshots.prematch_inputs ORDER BY due_at""")


def test_snapshots_are_taken_once_missed_ones_are_not_backfilled(season):
    s = season
    first = KICKOFF - timedelta(hours=2, minutes=30)
    assert snapshots.run(now=first, settings=s) == {"TAKEN": 1, "MISSED": 1, "upcoming_matches": 1,
                                                    "recently_started_matches": 0, "PREDICTION_WITHHELD": 1,
    assert rows(s) == [("T-24H", "MISSED", False), ("T-3H", "TAKEN", True)]
    # 찍은 스냅샷에만 같은 as_of의 예측이 붙는다. 이 DB에는 백테스트가 없어 활성 모델이 없으므로 확률 없이 보류.
    assert query(s, """SELECT offset_label, as_of = %s, prob_home, display_status, withheld_reasons
                       FROM snapshots.prematch_predictions""", (first,)) == [
        ("T-3H", True, None, "WITHHELD", ["NO_ACTIVE_MODEL", "LOW_TEAM_HISTORY(<5)"])]
    assert snapshots.run(now=first, settings=s)["TAKEN"] == 0  # 멱등: 같은 시각에 다시 불러도 새 기록 없음
    snapshots.run(now=KICKOFF - timedelta(minutes=40), settings=s)
    assert rows(s)[-1] == ("T-1H", "TAKEN", True)
    after = snapshots.run(now=KICKOFF + timedelta(minutes=5), settings=s)  # 시작 후: 세 시점 모두 기록돼 할 일 없음
    assert (after["upcoming_matches"], after["recently_started_matches"], after["TAKEN"], after["MISSED"]) == (0, 1, 0, 0)
    with psycopg.connect(s.postgres_conninfo) as conn, pytest.raises(psycopg.errors.RaiseException):
        conn.execute("UPDATE snapshots.prematch_inputs SET status='TAKEN'")


def test_inputs_use_only_what_was_known_and_are_deterministic(season):
    s = season
    as_of = KICKOFF - timedelta(hours=3)
    with psycopg.connect(s.postgres_conninfo) as conn:
        match_id = conn.execute("""SELECT m.match_id FROM silver.matches m JOIN silver.teams h ON h.team_id=m.home_team_id
                                   WHERE h.canonical_name='Delta FC'""").fetchone()[0]
        inputs = snapshots.build_inputs(conn, match_id, as_of)
        again = snapshots.build_inputs(conn, match_id, as_of)
    assert snapshots.inputs_hash(inputs) == snapshots.inputs_hash(again)
    assert inputs["match"]["home"] == "Delta FC" and "model_status" not in inputs   # 모델 예측은 별도 테이블(3d)
    # Delta FC의 이전 끝난 경기: Gamma 0-0 Delta(원정). Alpha FC: Alpha 2-1 Beta(홈). 시작 이후 경기는 들어가지 않는다.
    assert [(f["opponent"], f["venue"], f["goals_for"], f["goals_against"]) for f in inputs["home_form"]] == [("Gamma FC", "AWAY", 0, 0)]
    assert [(f["opponent"], f["venue"], f["goals_for"]) for f in inputs["away_form"]] == [("Beta FC", "HOME", 2)]
    assert inputs["odds"] == []


def test_matches_missed_while_the_system_was_off_are_recorded(season):
    """경기 당일 내내 꺼져 있다가 다시 켜도, 7일 안에 시작한 경기의 시점은 모두 MISSED로 남는다."""
    s = season
    assert snapshots.run(now=KICKOFF + timedelta(hours=10), settings=s)["MISSED"] == 3
    assert rows(s) == [("T-24H", "MISSED", False), ("T-3H", "MISSED", False), ("T-1H", "MISSED", False)]
    assert snapshots.run(now=KICKOFF + timedelta(hours=11), settings=s)["MISSED"] == 0  # 멱등


def test_matches_older_than_the_lookback_are_left_alone(season):
    s = season
    assert snapshots.run(now=KICKOFF + timedelta(days=8), settings=s)["MISSED"] == 0
    assert rows(s) == []
