"""3c 평가 보고서 테스트. 계산은 가상 예측으로, 저장은 Postgres가 있을 때만."""
import json
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from pipelines.db import migrate
from pipelines.modeling import evaluation
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)

START = datetime(2024, 8, 17, 14, tzinfo=timezone.utc)


def item(match_id, week, probabilities, result, home="A", away="B", season="2024-25", market=(0.5, 0.3, 0.2)):
    kickoff = START + timedelta(days=7 * week, hours=match_id % 5)
    return {"match_id": match_id, "season": season, "kickoff_utc": kickoff, "fit_cutoff": START + timedelta(days=7 * week),
            "probabilities": probabilities, "result": result, "home_team": home, "away_team": away,
            "market_pre": market, "market_close": market}


def test_block_bootstrap_is_wider_than_match_level_when_weeks_move_together():
    # 주마다 모든 경기가 같이 좋거나 같이 나쁘다(같은 학습 결과를 쓰는 주 효과).
    correlated = {week: [(-1.0 if week % 2 else 1.0) + 0.1] * 10 for week in range(40)}
    independent = {index: [value] for index, value in enumerate(v for values in correlated.values() for v in values)}
    by_week, by_match = evaluation.block_bootstrap(correlated), evaluation.block_bootstrap(independent)
    assert by_week["mean"] == by_match["mean"] == pytest.approx(0.1)
    assert by_week["blocks"] == 40 and by_match["blocks"] == 400 and by_week["matches"] == 400
    width = lambda result: result["ci95"][1] - result["ci95"][0]
    assert width(by_week) > 2.5 * width(by_match)   # 경기 단위로 뽑으면 불확실성을 과소평가한다


def test_block_bootstrap_is_reproducible_and_signed():
    differences = {week: [-0.02, -0.01, 0.0] for week in range(20)}
    first, second = evaluation.block_bootstrap(differences), evaluation.block_bootstrap(differences)
    assert first == second and first["mean"] == pytest.approx(-0.01) and first["ci95"][1] <= 0
    assert first["share_below_zero"] == 1.0


def test_wilson_interval_stays_inside_zero_and_one():
    assert evaluation.wilson(0, 10)[0] == 0 and evaluation.wilson(0, 10)[1] == pytest.approx(0.2775, abs=1e-3)
    low, high = evaluation.wilson(23, 111)
    assert low == pytest.approx(0.142, abs=1e-3) and high == pytest.approx(0.292, abs=1e-3)
    assert evaluation.wilson(5, 0) == (0.0, 1.0)


def test_calibration_matches_observed_rates():
    # 홈 승 확률 0.25라고 한 경기 100개 중 25개 홈 승 → 완벽히 보정
    items = [item(i, i // 10, (0.25, 0.35, 0.40), "HOME" if i < 25 else "AWAY") for i in range(100)]
    home = evaluation.calibration(items)["HOME"]
    assert home["ece"] == 0 and home["bins"] == [{"range": [0.2, 0.3], "matches": 100, "predicted": 0.25,
                                                   "observed": 0.25, "observed_ci95": [0.175, 0.343],
                                                   "within_ci": True}]
    draw = evaluation.calibration(items)["DRAW"]
    assert draw["ece"] == pytest.approx(0.35) and draw["bins"][0]["within_ci"] is False


def test_promoted_team_early_matches_are_found():
    seasons = {"2023-24": {"A", "B", "C"}, "2024-25": {"A", "B", "P"}}   # P는 2024-25 승격
    items = [item(i, i, (0.4, 0.3, 0.3), "AWAY", home="A", away="P" if i < 12 else "B") for i in range(14)]
    report = evaluation.segments(items, seasons)
    assert report["promoted_early"]["matches"] == 10                       # P의 첫 10경기만
    assert report["promoted_early"]["promoted_team_win"]["predicted_rate"] == 0.3
    assert report["promoted_early"]["promoted_team_win"]["actual_rate"] == 1.0
    assert report["draws"]["predicted_rate"] == 0.3 and report["draws"]["actual_rate"] == 0.0


def test_gate_requires_calibration_and_beating_the_market():
    calibrated = {outcome: {"ece": 0.01} for outcome in evaluation.OUTCOMES}
    good = {"calibration": calibrated, "vs_market_pre": {"ci95": [-0.02, -0.001]}}
    assert evaluation.gate(good) == {"status": "VALIDATED", "reasons": []}
    worse = {"calibration": {**calibrated, "DRAW": {"ece": 0.05}}, "vs_market_pre": {"ci95": [-0.01, 0.002]}}
    result = evaluation.gate(worse)
    assert result["status"] == "NOT_VALIDATED" and len(result["reasons"]) == 2
    assert evaluation.gate({"calibration": calibrated, "vs_market_pre": None})["status"] == "NOT_VALIDATED"


def synthetic_predictions():
    results = ["HOME", "DRAW", "AWAY", "HOME"]
    sharp = [item(i, i // 4, (0.6, 0.25, 0.15), results[i % 4]) for i in range(80)]
    flat = [dict(entry, probabilities=(0.45, 0.27, 0.28)) for entry in sharp]
    return {"poisson": sharp, "base_rate": flat}


def test_evaluate_and_render_report():
    runs = {"poisson": {"run_id": 2, "model_version": "poisson|3b.1|x", "dataset_hash": "h"},
            "base_rate": {"run_id": 1, "model_version": "base_rate|3b.1|laplace=1", "dataset_hash": "h"}}
    report = evaluation.evaluate(synthetic_predictions(), runs, {"2024-25": {"A", "B"}})
    assert set(report["comparisons"]) == {"poisson_vs_base_rate"}
    assert report["models"]["poisson"]["gate"]["status"] == "NOT_VALIDATED"
    assert report["models"]["poisson"]["sharpness"] == {"matches": 80, "model_mean_max": 0.6, "market_close_mean_max": 0.5}
    text = evaluation.render_markdown(report)
    assert "# EPL 기준 모델 평가 보고서" in text and "| Poisson − 단순 비율 | 80 | 20 |" in text
    assert "NOT_VALIDATED" in text and "run 2 (`poisson|3b.1|x`)" in text


def test_store_report_is_idempotent_and_immutable(isolated_settings):
    s = isolated_settings
    runs = {"poisson": {"run_id": 7, "model_version": "v", "dataset_hash": "h"}}
    report = json.loads(json.dumps({"runs": runs, "models": {}, "comparisons": {}}))
    with psycopg.connect(s.postgres_conninfo) as conn:
        migrate(conn)
        first = evaluation.store_report(conn, report)
        again = evaluation.store_report(conn, report)
    assert first[1] is True and again == (first[0], False)
    assert query(s, "SELECT run_ids, method->>'version' FROM modeling.evaluation_reports")[0] == ([7], "3c.2")
    with pytest.raises(psycopg.Error):
        query(s, "DELETE FROM modeling.evaluation_reports")
