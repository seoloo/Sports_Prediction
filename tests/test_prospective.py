"""3d 경기 전 예측 테스트. 실제 시점의 자료만 쓰는지, 검증 전에는 표시하지 않는지, 채점·주간 보고서를 Postgres로 확인한다."""
from datetime import date, datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pipelines import snapshots
from pipelines.ingest import openfootball
from pipelines.modeling import backtest, prospective
from pipelines.modeling.models import PoissonModel
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

KICKOFF = datetime(2026, 5, 24, 15, 0, tzinfo=timezone.utc)   # 예시 원본의 Delta FC - Alpha FC
T3H = KICKOFF - timedelta(hours=2, minutes=30)


def register_model(conn, gate_status="NOT_VALIDATED", ridge=10.0):
    """3b·3c를 흉내 내 검증·시험 실행과 평가 보고서를 만든다."""
    model = PoissonModel(ridge=ridge, half_life_days=180.0)
    for purpose, loss in (("VALIDATION", 0.98), ("TEST", 0.99)):
        run_id, _ = backtest.store_run(conn, model, purpose, ["2025-26"], "d" * 64,
                                       {"overall": {"log_loss": loss}}, [])
    conn.execute("""INSERT INTO modeling.evaluation_reports (run_ids, method_hash, method, results, created_at)
                    VALUES (%s::bigint[], %s, '{}', %s, now())""",
                 ([run_id], gate_status[0] * 64,
                  Jsonb({"models": {"poisson": {"gate": {"status": gate_status, "reasons": [] if gate_status == "VALIDATED" else ["x"]}}}})))
    return run_id


@pytest.fixture
def season(isolated_settings):
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=isolated_settings, received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    return isolated_settings


def test_active_model_is_chosen_by_validation_and_carries_gate(season, monkeypatch):
    with psycopg.connect(season.postgres_conninfo) as conn:
        assert prospective.active_model(conn) is None
        run_id = register_model(conn)
        active = prospective.active_model(conn)
    assert (active["name"], active["run_id"], active["gate_status"]) == ("poisson", run_id, "NOT_VALIDATED")
    assert prospective.build_model(active).version == active["model_version"]
    monkeypatch.setattr("pipelines.modeling.models.CODE_VERSION", "3b.999")   # 평가받지 않은 코드로 바뀐 경우
    assert prospective.build_model(active) is None


def test_snapshot_predictions_use_only_results_received_by_as_of(season, monkeypatch):
    monkeypatch.setattr(prospective, "MIN_TRAINING_MATCHES", 1)
    with psycopg.connect(season.postgres_conninfo) as conn:
        register_model(conn)
        predictor = prospective.Predictor(conn, T3H)
        # 결과는 2025-08-17에 받았으므로 그 뒤 시점에는 알려져 있다. 그 전 시점에는 하나도 없다.
        early = prospective.Predictor(conn, datetime(2025, 8, 16, tzinfo=timezone.utc))
    assert predictor.training and all(match[0] < T3H for match in predictor.training)
    assert all(match.home_new in (True, False) for match in predictor.training)   # 승격 표시(3e)까지 함께 넘긴다
    # 쉰 날 수(3g): 첫 경기는 모르고(None), 그 뒤는 직전 끝난 경기 기준
    assert predictor.training[0].home_rest is None
    assert predictor.rest_days("Delta FC", T3H) is not None
    assert early.training == [] and early.reasons == ["INSUFFICIENT_TRAINING_DATA(<1)"]

    counts = snapshots.run(now=T3H, settings=season)
    assert counts["PREDICTION_WITHHELD"] == 1
    (row,) = query(season, """SELECT as_of, model_name, gate_status, prob_home + prob_draw + prob_away, display_status,
                                     withheld_reasons, last_training_kickoff < as_of FROM snapshots.prematch_predictions""")
    assert row[0] == T3H and row[1:3] == ("poisson", "NOT_VALIDATED") and float(row[3]) == pytest.approx(1)
    assert row[4] == "WITHHELD" and "MODEL_NOT_VALIDATED" in row[5] and row[6] is True


def test_validated_model_with_enough_history_is_ready(season, monkeypatch):
    monkeypatch.setattr(prospective, "MIN_TRAINING_MATCHES", 1)
    monkeypatch.setattr(prospective, "MIN_TEAM_HISTORY", 1)
    with psycopg.connect(season.postgres_conninfo) as conn:
        register_model(conn, gate_status="VALIDATED")
    snapshots.run(now=T3H, settings=season)
    assert query(season, "SELECT display_status, withheld_reasons FROM snapshots.prematch_predictions") == [("READY", [])]


def test_prediction_failure_keeps_the_input_snapshot(season, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("model exploded")
    monkeypatch.setattr(prospective, "Predictor", broken)
    counts = snapshots.run(now=T3H, settings=season)
    assert counts["TAKEN"] == 1 and counts["PREDICTION_FAILED"] == 1
    assert query(season, "SELECT status FROM snapshots.prematch_inputs WHERE status = 'TAKEN'") == [("TAKEN",)]
    assert query(season, "SELECT withheld_reasons FROM snapshots.prematch_predictions") == [(["PREDICTION_FAILED:RuntimeError"],)]


def test_database_rejects_late_or_mismatched_predictions(season):
    snapshots.run(now=T3H, settings=season)
    with psycopg.connect(season.postgres_conninfo) as conn:
        match_id = conn.execute("SELECT match_id FROM snapshots.prematch_inputs WHERE status = 'TAKEN'").fetchone()[0]
        conn.commit()
        base = {"model_name": "poisson", "model_version": "late", "backtest_run_id": None, "evaluation_report_id": None,
                "gate_status": "NOT_VALIDATED", "training_matches": 1, "last_training_kickoff": None,
                "training_hash": None, "home_history": 1, "away_history": 1, "probabilities": (0.5, 0.3, 0.2),
                "display_status": "WITHHELD", "withheld_reasons": ["MODEL_NOT_VALIDATED"]}
        # 스냅샷과 다른 기준 시각(나중에 만든 예측)
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            prospective.insert_prediction(conn, match_id, "T-3H", T3H + timedelta(hours=1), base)
        conn.rollback()
        # 검증되지 않은 모델을 READY로
        with pytest.raises(psycopg.errors.CheckViolation):
            prospective.insert_prediction(conn, match_id, "T-3H", T3H,
                                          {**base, "model_version": "x", "display_status": "READY", "withheld_reasons": []})
        conn.rollback()
        # 기준 시각보다 뒤의 경기를 학습에 썼다고 기록
        with pytest.raises(psycopg.errors.CheckViolation):
            prospective.insert_prediction(conn, match_id, "T-3H", T3H,
                                          {**base, "model_version": "y", "last_training_kickoff": T3H + timedelta(minutes=1)})


def test_weekly_report_scores_finished_matches(season, monkeypatch):
    monkeypatch.setattr(prospective, "MIN_TRAINING_MATCHES", 1)
    with psycopg.connect(season.postgres_conninfo) as conn:
        register_model(conn)
    snapshots.run(now=T3H, settings=season)
    # 경기 끝난 뒤 결과를 받는다(Delta FC 1-0 Alpha FC 정정 원본).
    import json
    document = json.loads(SAMPLE_PATH.read_bytes())
    target = next(m for m in document["matches"] if m["team1"] == "Delta FC" and m["team2"] == "Alpha FC")
    target["score"] = {"ft": [1, 0]}
    openfootball.ingest("2025-26", "en.1", payload=json.dumps(document).encode(), source_url="file:sample.json",
                        settings=season, received_at=KICKOFF + timedelta(hours=3))
    week = KICKOFF.date() - timedelta(days=KICKOFF.weekday())
    before = prospective.run_weekly(now=KICKOFF + timedelta(hours=1), week_start=week, settings=season, store=False)
    after = prospective.run_weekly(now=KICKOFF + timedelta(days=2), week_start=week, settings=season)
    assert before["week"]["T-3H"] == {"matches": 0}                      # 결과를 받기 전에는 채점하지 않는다
    assert after["week"]["T-3H"]["matches"] == 1 and after["week"]["T-3H"]["log_loss"] > 0
    assert after["snapshots"]["T-3H"]["TAKEN"] == 1 and after["withheld_reasons"]["MODEL_NOT_VALIDATED"] == 1
    again = prospective.run_weekly(now=KICKOFF + timedelta(days=3), week_start=week, settings=season)
    assert again["report_id"] == after["report_id"] and again["newly_stored"] is False   # 내용이 같으면 새로 쓰지 않음
    text = prospective.render_weekly_markdown(after)
    assert "평가용 기록" in text and "| 3시간 전 | 1 |" in text


def test_previous_week_start_is_last_monday():
    assert prospective.previous_week_start(datetime(2026, 10, 27, 6, tzinfo=timezone.utc)) == date(2026, 10, 19)
    assert prospective.previous_week_start(datetime(2026, 10, 26, 23, tzinfo=timezone.utc)) == date(2026, 10, 19)


def test_market_probabilities_from_snapshot_inputs():
    odds = [{"source_id": "football-data-co-uk", "bookmaker": "AVG", "market": "1X2", "timing": "PRE_MATCH",
             "selection": selection, "price": price} for selection, price in (("HOME", "2.0"), ("DRAW", "4.0"), ("AWAY", "4.0"))]
    assert prospective.market_probabilities({"odds": odds}) == pytest.approx((0.5, 0.25, 0.25))
    assert prospective.market_probabilities({"odds": odds[:2]}) is None
