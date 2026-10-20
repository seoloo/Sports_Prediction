"""dbt Gold 모델 통합 테스트. Postgres·S3 서비스와 .venv-dbt가 있어야 실행되고, 없으면 건너뛴다.

임시 데이터베이스에 예시 원본을 수집한 뒤 dbt build를 실행해
순위 계산, 재실행 시 결과 동일(멱등), 기준 시각(as_of)별 재계산을 확인한다.
"""
import json
import os
from datetime import datetime

import psycopg
import pytest

from pipelines import dbt_runner
from pipelines.ingest import openfootball
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

STANDINGS_CHECKSUM = """SELECT md5(string_agg(concat_ws('|', season, position, team, played, won, drawn, lost,
                                                         goals_for, goals_against, points), ',' ORDER BY season, position))
                        FROM gold.league_standings"""


@pytest.fixture
def dbt_settings(isolated_settings):
    try:
        dbt_runner.find_dbt_executable()
    except FileNotFoundError as exc:
        if os.environ.get("REQUIRE_INTEGRATION") == "1":
            raise
        pytest.skip(str(exc))
    return isolated_settings


def ingest(settings, payload, at):
    openfootball.ingest("2025-26", "en.1", payload=payload, source_url="file:sample.json",
                        settings=settings, received_at=datetime.fromisoformat(at))


def build(settings, *extra):
    assert dbt_runner.run_dbt(["build", "--quiet", *extra], settings) == 0


def test_standings_are_correct_and_rebuild_is_idempotent(dbt_settings):
    s = dbt_settings
    ingest(s, SAMPLE_PATH.read_bytes(), "2025-08-17T00:00:00+00:00")
    build(s)
    standings = query(s, "SELECT position, team, played, won, drawn, lost, goals_for, goals_against, points "
                         "FROM gold.league_standings ORDER BY position")
    # 예시 원본에서 검사를 통과한 끝난 경기: Alpha 2-1 Beta, Gamma 0-0 Delta, Beta 3-2 Gamma
    assert standings == [
        (1, "Alpha FC", 1, 1, 0, 0, 2, 1, 3),
        (2, "Beta FC", 2, 1, 0, 1, 4, 4, 3),
        (3, "Delta FC", 1, 0, 1, 0, 0, 0, 1),
        (4, "Gamma FC", 2, 0, 1, 1, 2, 3, 1),
    ]
    first = query(s, STANDINGS_CHECKSUM)[0][0]
    build(s)
    assert query(s, STANDINGS_CHECKSUM)[0][0] == first


def test_gold_can_be_rebuilt_as_of_an_earlier_time(dbt_settings):
    s = dbt_settings
    original = SAMPLE_PATH.read_bytes()
    ingest(s, original, "2025-08-17T00:00:00+00:00")
    corrected = json.loads(original)
    corrected["matches"][0]["score"]["ft"] = [3, 1]  # Alpha 2-1 → 3-1 정정
    ingest(s, json.dumps(corrected).encode(), "2025-08-19T00:00:00+00:00")

    alpha_goals = "SELECT goals_for FROM gold.league_standings WHERE team='Alpha FC'"
    build(s)
    assert query(s, alpha_goals)[0][0] == 3
    build(s, "--vars", '{as_of: "2025-08-18T00:00:00Z"}')
    assert query(s, alpha_goals)[0][0] == 2
    build(s, "--vars", '{as_of: "2025-08-16T00:00:00Z"}')
    assert query(s, "SELECT count(*) FROM gold.league_standings")[0][0] == 0


def test_failing_quality_test_fails_build(dbt_settings):
    s = dbt_settings
    ingest(s, SAMPLE_PATH.read_bytes(), "2025-08-17T00:00:00+00:00")
    build(s)
    # 품질 테스트가 실제로 실패를 잡는지 확인: 순위표에 승점이 틀린 행을 몰래 넣고 테스트만 다시 실행한다.
    with psycopg.connect(s.postgres_conninfo) as conn:
        conn.execute("UPDATE gold.league_standings SET points = points + 1 WHERE position = 1")
    assert dbt_runner.run_dbt(["test", "--quiet", "--select", "assert_standings_balance"], s) != 0
