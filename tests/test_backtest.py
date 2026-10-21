"""3b 시간순 백테스트 테스트. 학습 시점 규칙은 가상 데이터로, 기록·DB 제약은 Postgres가 있을 때만."""
import math
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from pipelines.db import migrate
from pipelines.ingest import openfootball
from pipelines.modeling import backtest
from pipelines.modeling.models import BaseRateModel, PoissonModel
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

TEAMS = ["A", "B", "C", "D"]


def synthetic_rows(seasons=("2021-22", "2022-23", "2023-24")) -> list[dict]:
    """시즌마다 4팀이 홈·원정으로 두 번씩 만나는 가상 데이터셋(토요일 15:00, 주 1경기)."""
    rows, kickoff, match_id = [], datetime(2021, 8, 14, 15, tzinfo=timezone.utc), 0
    for season in seasons:
        for home in TEAMS:
            for away in TEAMS:
                if home == away:
                    continue
                match_id += 1
                goals = (match_id % 3, (match_id // 3) % 2)
                rows.append({"match_id": match_id, "season": season, "kickoff_utc": kickoff, "home_team": home,
                             "away_team": away, "ft_home": goals[0], "ft_away": goals[1],
                             "result": "HOME" if goals[0] > goals[1] else "DRAW" if goals[0] == goals[1] else "AWAY",
                             "market_close_home": None, "market_close_draw": None, "market_close_away": None,
                             "market_pre_home": None, "market_pre_draw": None, "market_pre_away": None})
                kickoff += timedelta(days=7)
        kickoff += timedelta(days=63)   # 비시즌(9주), 요일은 토요일 유지
    return rows


class RecordingModel(BaseRateModel):
    """fit에 넘어온 학습 경기를 기록해, 미래 정보가 섞이지 않았는지 확인한다."""
    calls = []

    def fit(self, matches, as_of):
        RecordingModel.calls.append((as_of, list(matches)))
        return super().fit(matches, as_of)


def test_training_only_uses_results_known_before_each_block(monkeypatch):
    monkeypatch.setattr(backtest, "MIN_TRAINING_MATCHES", 5)
    RecordingModel.calls = []
    rows = synthetic_rows()
    predictions = backtest.walk_forward(rows, RecordingModel, ["2023-24"])
    assert len(predictions) == 12 and all(p["probabilities"] for p in predictions)
    for as_of, matches in RecordingModel.calls:
        assert all(kickoff + timedelta(hours=3) <= as_of for kickoff, *_ in matches)
        # 알려진 경기는 빠짐없이 들어간다(학습 데이터를 너무 적게 쓰는 실수도 잡는다).
        assert len(matches) == sum(row["kickoff_utc"] + timedelta(hours=3) <= as_of for row in rows)
    for p in predictions:
        assert p["last_training_kickoff"] + timedelta(hours=3) <= p["fit_cutoff"] <= p["kickoff_utc"]


def test_same_week_matches_share_the_cutoff_of_the_first_kickoff(monkeypatch):
    monkeypatch.setattr(backtest, "MIN_TRAINING_MATCHES", 1)
    rows = synthetic_rows()
    saturday = rows[-1]["kickoff_utc"]
    # 같은 주 일요일 경기와, 토요일 경기 2시간 뒤 시작한 경기(토요일 결과가 아직 알려지지 않음)
    rows.append({**rows[-1], "match_id": 900, "kickoff_utc": saturday + timedelta(days=1)})
    rows.append({**rows[-1], "match_id": 901, "kickoff_utc": saturday + timedelta(hours=2)})
    predictions = {p["match_id"]: p for p in backtest.walk_forward(rows, BaseRateModel, ["2023-24"])}
    assert predictions[900]["fit_cutoff"] == predictions[901]["fit_cutoff"] == saturday
    assert predictions[900]["last_training_kickoff"] < saturday


def test_withheld_without_enough_training_data():
    predictions = backtest.walk_forward(synthetic_rows(), BaseRateModel, ["2022-23"])   # 학습 경기 12 < 100
    assert {p["withheld_reason"] for p in predictions} == {"INSUFFICIENT_TRAINING_DATA(<100)"}
    assert all(p["probabilities"] is None for p in predictions)
    summary = backtest.summarize(predictions, {})
    assert summary["overall"] == {"matches": 12, "withheld": 12}


def test_fixed_validation_seasons_do_not_shift_when_older_data_is_added():
    rows = synthetic_rows(("2020-21", "2021-22", "2022-23", "2023-24", "2024-25"))
    assert backtest.season_roles(rows, ("2021-22", "2022-23")) == {
        "warmup": ["2020-21"], "validation": ["2021-22", "2022-23"], "test": ["2023-24", "2024-25"]}
    with pytest.raises(ValueError):   # 학습 전용 시즌이 없으면 안 된다
        backtest.season_roles(synthetic_rows(("2021-22", "2022-23", "2023-24")), ("2021-22", "2022-23"))


def test_fit_temperature_sharpens_underconfident_predictions():
    # 실제로는 70% 맞히는데 50%라고 말하는 예측 → 1보다 큰 보정 계수
    items = [{"probabilities": (0.5, 0.25, 0.25), "result": "HOME" if i % 10 < 7 else "AWAY"} for i in range(200)]
    assert backtest.fit_temperature(items) > 1.2
    calibrated = [{"probabilities": (0.5, 0.25, 0.25), "result": "HOME" if i % 4 < 2 else ("DRAW" if i % 4 == 2 else "AWAY")}
                  for i in range(200)]
    assert backtest.fit_temperature(calibrated) == 1.0


def test_cross_fit_scores_each_season_with_the_other_seasons_temperature():
    # 2021-22는 소극적인 예측(실제 70% 홈 승인데 50%), 2022-23은 이미 맞는 예측
    under = [{"season": "2021-22", "probabilities": (0.5, 0.25, 0.25), "result": "HOME" if i % 10 < 7 else "AWAY"}
             for i in range(200)]
    exact = [{"season": "2022-23", "probabilities": (0.5, 0.25, 0.25),
              "result": "HOME" if i % 4 < 2 else ("DRAW" if i % 4 == 2 else "AWAY")} for i in range(200)]
    calibrated, temperatures = backtest.cross_fit_temperature(under + exact)
    # 2021-22에는 2022-23에서 고른 계수(1.0)를, 2022-23에는 2021-22에서 고른 계수(>1)를 적용한다.
    assert temperatures["2021-22"] == 1.0 and temperatures["2022-23"] > 1.2
    assert calibrated[0]["probabilities"] == (0.5, 0.25, 0.25) and calibrated[-1]["probabilities"][0] > 0.5
    with pytest.raises(ValueError):   # 시즌이 하나면 같은 자료로 고르고 채점하게 되므로 거부
        backtest.cross_fit_temperature(under)


def test_mark_promoted_flags_newcomers_per_season():
    rows = synthetic_rows(("2021-22", "2022-23"))
    rows += [{**rows[-1], "match_id": 999, "home_team": "Z"}]   # 2022-23에 처음 나온 팀
    marked = {row["match_id"]: row for row in backtest.mark_promoted(rows)}
    assert marked[999]["home_new"] is True and marked[999]["away_new"] is False
    assert not any(row["home_new"] or row["away_new"] for row in marked.values() if row["season"] == "2021-22")


def test_season_roles_keep_test_seasons_out_of_selection():
    assert backtest.season_roles(synthetic_rows(("2020-21", "2021-22", "2022-23", "2023-24"))) == {
        "warmup": ["2020-21"], "validation": ["2021-22"], "test": ["2022-23", "2023-24"]}
    with pytest.raises(ValueError):
        backtest.season_roles(synthetic_rows(("2021-22", "2022-23")))


def test_metrics_and_market_comparison_on_same_matches():
    assert backtest.log_loss((0.5, 0.3, 0.2), "DRAW") == pytest.approx(-math.log(0.3))
    assert backtest.brier((0.5, 0.3, 0.2), "HOME") == pytest.approx(0.25 + 0.09 + 0.04)
    rows = {1: {"market_close_home": 0.6, "market_close_draw": 0.25, "market_close_away": 0.15},
            2: {"market_close_home": None, "market_close_draw": None, "market_close_away": None}}
    predictions = [{"match_id": 1, "season": "S", "probabilities": (0.5, 0.3, 0.2), "result": "HOME"},
                   {"match_id": 2, "season": "S", "probabilities": (0.4, 0.3, 0.3), "result": "AWAY"}]
    overall = backtest.summarize(predictions, rows)["overall"]
    assert overall["log_loss"] == pytest.approx((-math.log(0.5) - math.log(0.3)) / 2, abs=1e-4)
    assert overall["vs_market_close"] == {"matches": 1, "model_log_loss": round(-math.log(0.5), 4),
                                          "market_log_loss": round(-math.log(0.6), 4)}
    assert "vs_market_pre" not in overall


def test_dataset_hash_changes_when_a_result_changes():
    rows = synthetic_rows()
    changed = [dict(row) for row in rows]
    changed[0]["ft_home"] += 1
    assert backtest.dataset_hash(rows) == backtest.dataset_hash(list(reversed(rows)))
    assert backtest.dataset_hash(rows) != backtest.dataset_hash(changed)


def test_run_selects_on_validation_and_tests_once(monkeypatch):
    monkeypatch.setattr(backtest, "MIN_TRAINING_MATCHES", 5)
    monkeypatch.setattr(backtest, "VALIDATION_SEASONS", ("2021-22", "2022-23"))
    monkeypatch.setattr(backtest, "RIDGE_GRID", (1.0, 10.0))
    monkeypatch.setattr(backtest, "HALF_LIFE_GRID", (None,))
    report = backtest.run(synthetic_rows(("2020-21", "2021-22", "2022-23", "2023-24")))
    assert report["validation"]["poisson"]["tried"] == 2 and report["validation"]["base_rate"]["tried"] == 1
    assert set(report["test"]["poisson"]["by_season"]) == {"2023-24"}
    assert report["test"]["poisson"]["model_version"] == report["validation"]["poisson"]["chosen"]
    # 보정 계열은 기준 계열에서 고른 설정 + 검증 예측으로 고른 보정 계수
    calibrated = report["validation"]["poisson_calibrated"]
    assert calibrated["chosen"].startswith("poisson_calibrated|") and 0.7 <= calibrated["temperature"] <= 1.6
    assert calibrated["chosen"].endswith("|calibration=cross_fit_by_season")
    assert set(calibrated["fold_temperatures"]) == {"2021-22", "2022-23"}
    assert set(report["test"]) == {"base_rate", "poisson", "dixon_coles", "poisson_promoted", "poisson_calibrated",
                                   "poisson_promoted_calibrated"}


@pytest.fixture
def stored(isolated_settings):
    s = isolated_settings
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=s, received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    with psycopg.connect(s.postgres_conninfo) as conn:
        migrate(conn)
        match_id, kickoff = conn.execute("""SELECT match_id, kickoff_utc FROM silver.match_revisions
                                            WHERE kickoff_utc IS NOT NULL ORDER BY kickoff_utc LIMIT 1""").fetchone()
    prediction = {"match_id": match_id, "season": "2025-26", "kickoff_utc": kickoff, "fit_cutoff": kickoff,
                  "last_training_kickoff": kickoff - timedelta(days=7), "training_matches": 150,
                  "probabilities": (0.5, 0.3, 0.2), "result": "HOME", "withheld_reason": None}
    return s, prediction


def test_store_run_is_idempotent_and_immutable(stored):
    s, prediction = stored
    model = PoissonModel(ridge=10, half_life_days=180)
    with psycopg.connect(s.postgres_conninfo) as conn:
        first = backtest.store_run(conn, model, "TEST", ["2025-26"], "a" * 64, {"overall": {}}, [prediction])
        again = backtest.store_run(conn, model, "TEST", ["2025-26"], "a" * 64, {"overall": {}}, [prediction])
        other_data = backtest.store_run(conn, model, "TEST", ["2025-26"], "b" * 64, {"overall": {}}, [prediction])
    assert first[1] is True and again == (first[0], False) and other_data[1] is True
    assert query(s, "SELECT count(*) FROM modeling.backtest_predictions")[0][0] == 2
    assert query(s, "SELECT model_version, params->>'ridge' FROM modeling.backtest_runs WHERE run_id = %s",
                 (first[0],))[0] == ("poisson|3e.1|ridge=10|half_life_days=180", "10")
    with pytest.raises(psycopg.Error):
        query(s, "UPDATE modeling.backtest_predictions SET prob_home = 0.9")


@pytest.mark.parametrize("change", [
    {"last_training_kickoff_shift": timedelta(hours=-2)},   # 학습 경기 결과가 컷오프 시점에 아직 안 알려짐
    {"fit_cutoff_shift": timedelta(minutes=1)},              # 경기 시작 뒤에 학습
    {"probabilities": (0.5, 0.3, 0.3)},                      # 확률 합 ≠ 1
])
def test_database_rejects_leaky_or_invalid_predictions(stored, change):
    s, prediction = stored
    prediction = dict(prediction)
    if "last_training_kickoff_shift" in change:
        prediction["last_training_kickoff"] = prediction["fit_cutoff"] + change["last_training_kickoff_shift"]
    if "fit_cutoff_shift" in change:
        prediction["fit_cutoff"] = prediction["kickoff_utc"] + change["fit_cutoff_shift"]
    if "probabilities" in change:
        prediction["probabilities"] = change["probabilities"]
    with psycopg.connect(s.postgres_conninfo) as conn, pytest.raises(psycopg.errors.CheckViolation):
        backtest.store_run(conn, BaseRateModel(), "TEST", ["2025-26"], "c" * 64, {"overall": {}}, [prediction])
