"""football-data.co.uk 수집 통합 테스트(Postgres·S3 필요, 없으면 건너뜀). 가상 CSV와 openfootball 예시를 쓴다."""
from datetime import datetime
from pathlib import Path

import psycopg
import pytest

from pipelines.ingest import football_data_co_uk as fd
from pipelines.ingest import openfootball
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

CSV_FIXTURE = Path(__file__).parent / "fixtures" / "E0_2025-26.csv"


@pytest.fixture
def prepared(isolated_settings, tmp_path):
    """openfootball 예시 경기를 넣고, 가상 팀 이름 대조표(Alpha → Alpha FC …)를 만든다."""
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=isolated_settings, received_at=datetime.fromisoformat("2025-08-17T00:00:00+00:00"))
    alias_file = tmp_path / "aliases.csv"
    alias_file.write_text("# 테스트용\nsource_id,alias,canonical_name,evidence\n" + "".join(
        f"football-data-co-uk,{name},{name} FC,test\n" for name in ("Alpha", "Beta", "Gamma", "Delta")), encoding="utf-8")
    return isolated_settings, alias_file, tmp_path


def run(settings, alias_file, path, at):
    return fd.ingest_file(path, settings=settings, alias_file=alias_file, received_at=datetime.fromisoformat(at))


def test_odds_are_linked_versioned_and_idempotent(prepared):
    s, aliases, tmp = prepared
    first = run(s, aliases, CSV_FIXTURE, "2025-12-27T00:00:00+00:00")
    assert (first["fixtures"], first["linked"], first["odds_new"]) == (5, 4, 23 + 20 + 18 + 13)
    assert first["issues"] == {"INVALID_HANDICAP_LINE": 1, "INVALID_PRICE": 1, "UNMAPPED_TEAM": 1}
    again = run(s, aliases, CSV_FIXTURE, "2025-12-28T00:00:00+00:00")
    assert (again["new_content"], again["odds_new"]) == (False, 0)

    # 한 배당만 바뀐 새 파일: 그 배당만 2번 버전이 생긴다.
    changed = tmp / "E0_2025-26.csv"
    changed.write_text(CSV_FIXTURE.read_text(encoding="utf-8").replace(
        "E0,16/08/2025,15:00,Alpha,Beta,2,1,H,1,0,H,1.80,", "E0,16/08/2025,15:00,Alpha,Beta,2,1,H,1,0,H,1.83,"), encoding="utf-8")
    third = run(s, aliases, changed, "2025-12-29T00:00:00+00:00")
    assert (third["new_content"], third["odds_new"]) == (True, 1)
    history = query(s, """SELECT revision_no, price FROM silver.odds_revisions
                          WHERE bookmaker='B365' AND market='1X2' AND selection='HOME' AND timing='PRE_MATCH'
                          AND match_id=(SELECT match_id FROM silver.matches m JOIN silver.teams t ON t.team_id=m.home_team_id
                                        WHERE t.canonical_name='Alpha FC' AND m.away_team_id=(SELECT team_id FROM silver.teams WHERE canonical_name='Beta FC'))
                          ORDER BY revision_no""")
    assert [(row[0], str(row[1])) for row in history] == [(1, "1.8000"), (2, "1.8300")]
    known = "SELECT price FROM silver.odds_known_at(%s) WHERE bookmaker='B365' AND selection='HOME' AND timing='PRE_MATCH' AND market='1X2' ORDER BY match_id LIMIT 1"
    assert str(query(s, known, ("2025-12-28T12:00:00+00:00",))[0][0]) == "1.8000"
    assert str(query(s, known, ("2025-12-29T12:00:00+00:00",))[0][0]) == "1.8300"


def test_pinnacle_caution_and_cross_source_mismatches(prepared):
    s, aliases, tmp = prepared
    mismatched = tmp / "E0_2025-26.csv"
    mismatched.write_text(CSV_FIXTURE.read_text(encoding="utf-8")
                          .replace("E0,16/08/2025,17:30,Gamma,Delta,0,0,", "E0,17/08/2025,17:30,Gamma,Delta,0,0,")
                          .replace("E0,26/12/2025,12:30,Beta,Gamma,3,2,", "E0,26/12/2025,12:30,Beta,Gamma,3,3,"), encoding="utf-8")
    stats = run(s, aliases, mismatched, "2025-12-27T00:00:00+00:00")
    assert stats["issues"]["DATE_MISMATCH"] == 1 and stats["issues"]["SCORE_MISMATCH"] == 1
    assert stats["linked"] == 4  # 어긋나도 연결은 유지하고 어느 쪽이 맞는지 판단하지 않는다
    cautions = query(s, "SELECT DISTINCT bookmaker, caution FROM silver.odds_revisions WHERE caution IS NOT NULL")
    assert cautions == [("PS", fd.PINNACLE_CAUTION)]
    # openfootball 점수는 바뀌지 않는다(다른 소스가 덮어쓰지 않음).
    assert query(s, "SELECT count(*) FROM silver.match_revisions")[0][0] == 5


def test_alias_conflicts_stop_ingestion(prepared):
    s, aliases, tmp = prepared
    run(s, aliases, CSV_FIXTURE, "2025-12-27T00:00:00+00:00")
    conflicting = tmp / "conflict.csv"
    conflicting.write_text(aliases.read_text(encoding="utf-8").replace("Alpha,Alpha FC", "Alpha,Beta FC"), encoding="utf-8")
    with pytest.raises(ValueError, match="별칭 충돌"):
        run(s, conflicting, CSV_FIXTURE, "2025-12-28T00:00:00+00:00")
    assert query(s, "SELECT status FROM ingest.runs WHERE source_id='football-data-co-uk' ORDER BY started_at DESC LIMIT 1") == [("FAILED",)]


def test_odds_history_is_immutable(prepared):
    s, aliases, _ = prepared
    run(s, aliases, CSV_FIXTURE, "2025-12-27T00:00:00+00:00")
    with psycopg.connect(s.postgres_conninfo) as conn, pytest.raises(psycopg.errors.RaiseException):
        conn.execute("UPDATE silver.odds_revisions SET price = 9")
