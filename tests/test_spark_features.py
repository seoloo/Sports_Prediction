"""4a Spark 특징 계산 테스트. pyspark가 있는 Spark 컨테이너에서 돈다(호스트 .venv에는 pyspark가 없어 건너뜀).

정답은 3a의 Python 독립 재계산(pipelines.modeling.dataset._recompute)이다. dbt·Spark·Python 세 구현이 같은 규칙을
따르는지, 그중 Spark와 Python을 여기서 직접 맞춰 본다(dbt와 Spark는 pipelines/lake/reconcile.py가 맞춘다).
CI는 REQUIRE_SPARK=1로 이 파일을 Spark 컨테이너에서 돌려, 몰래 건너뛰어지지 않게 한다.
"""
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

if os.environ.get("REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pipelines.lake.spark_features import compute_features, spark_session  # noqa: E402
from pipelines.modeling.dataset import _recompute  # noqa: E402

T = datetime(2025, 9, 1, 15, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    session = spark_session("test_match_features")
    yield session
    session.stop()


def frames(spark, matches, results):
    """matches: (match_id, kickoff, home, away), results: (match_id, kickoff, home, away, home_goals, away_goals)."""
    match_rows = [(match_id, "2025-26", kickoff, home, away) for match_id, kickoff, home, away in matches]
    history = []
    for match_id, kickoff, home, away, home_goals, away_goals in results:
        for team, goals_for, goals_against in ((home, home_goals, away_goals), (away, away_goals, home_goals)):
            points = 3 if goals_for > goals_against else 1 if goals_for == goals_against else 0
            history.append((team, match_id, kickoff, goals_for, goals_against, points))
    return (spark.createDataFrame(match_rows, "match_id long, season string, kickoff_utc timestamp, home_team string, away_team string"),
            spark.createDataFrame(history, "team string, match_id long, kickoff_utc timestamp, goals_for int, goals_against int, points int"),
            history)


def test_three_hour_rule_target_exclusion_and_last_ten(spark):
    results = [(100 + d, T - timedelta(days=d), "A", f"X{d}", d % 4, 1) for d in range(1, 15)]    # A의 과거 14경기
    results += [(200, T - timedelta(hours=2, minutes=59), "A", "B", 5, 0),                       # 아직 결과 모름
                (201, T - timedelta(hours=3), "B", "C", 2, 2)]                                    # 딱 3시간 전 → 포함
    matches = [(1, T, "A", "B"), (200, T - timedelta(hours=2, minutes=59), "A", "B"), (2, T, "D", "E")]
    match_frame, history_frame, history = frames(spark, matches, results)
    rows = {row["match_id"]: row.asDict() for row in compute_features(match_frame, history_frame).collect()}

    by_team = {}
    for team, match_id, kickoff, goals_for, goals_against, points in history:
        by_team.setdefault(team, []).append((kickoff, match_id, goals_for, goals_against, points))
    for match_id, kickoff, home, away in matches:
        for side, team in (("home", home), ("away", away)):
            expected = _recompute(by_team.get(team, []), kickoff, match_id)
            for column, value in expected.items():
                actual = rows[match_id][f"{side}_{column}"]
                if value is None or column.endswith("_n"):
                    assert actual == value, (match_id, side, column)
                else:
                    assert Decimal(str(actual)).quantize(Decimal("0.000001")) == value.quantize(Decimal("0.000001"))
    assert rows[1]["home_l10_n"] == 10 and rows[1]["home_l38_n"] == 14   # 15경기 중 결과가 알려진 14경기, 최근 10개만
    assert rows[1]["away_l10_n"] == 1                                     # B: 3시간 전 경기만(2시간 59분 전 경기는 제외)
    assert rows[200]["home_l38_n"] == 14                                  # 대상 경기 자신은 넣지 않는다
    assert rows[2]["home_l10_n"] == 0 and rows[2]["home_l10_gf"] is None and rows[2]["home_rest_days"] is None


def test_rest_days_and_last_feature_kickoff(spark):
    matches = [(1, T, "A", "B")]
    results = [(10, T - timedelta(days=4, hours=12), "A", "C", 1, 0), (11, T - timedelta(days=10), "B", "C", 0, 0)]
    match_frame, history_frame, _ = frames(spark, matches, results)
    (row,) = compute_features(match_frame, history_frame).collect()
    assert row["home_rest_days"] == pytest.approx(4.5) and row["away_rest_days"] == pytest.approx(10.0)
    assert row["home_last_feature_kickoff"].replace(tzinfo=timezone.utc) == T - timedelta(days=4, hours=12)
