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
    (KICKOFF, set(), {}),                                                          # 시작 이후는 건너뜀
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
    assert snapshots.run(now=first, settings=s) == {"TAKEN": 1, "MISSED": 1, "upcoming_matches": 1}
    assert rows(s) == [("T-24H", "MISSED", False), ("T-3H", "TAKEN", True)]
    assert snapshots.run(now=first, settings=s) == {"TAKEN": 0, "MISSED": 0, "upcoming_matches": 1}  # 멱등
    snapshots.run(now=KICKOFF - timedelta(minutes=40), settings=s)
    assert rows(s)[-1] == ("T-1H", "TAKEN", True)
    assert snapshots.run(now=KICKOFF + timedelta(minutes=5), settings=s)["upcoming_matches"] == 0  # 시작 후
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
    assert inputs["match"]["home"] == "Delta FC" and inputs["model_status"] == "UNTRAINED"
    # Delta FC의 이전 끝난 경기: Gamma 0-0 Delta(원정). Alpha FC: Alpha 2-1 Beta(홈). 시작 이후 경기는 들어가지 않는다.
    assert [(f["opponent"], f["venue"], f["goals_for"], f["goals_against"]) for f in inputs["home_form"]] == [("Gamma FC", "AWAY", 0, 0)]
    assert [(f["opponent"], f["venue"], f["goals_for"]) for f in inputs["away_form"]] == [("Beta FC", "HOME", 2)]
    assert inputs["odds"] == []
