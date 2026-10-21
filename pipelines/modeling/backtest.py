"""기준 모델의 시간순(walk-forward) 백테스트(로드맵 3b).

규칙
- 평가할 경기를 ISO 주(월~일, UTC) 단위 블록으로 나누고, 블록마다 그 블록의 첫 경기 시작 시각(fit_cutoff)에
  결과가 알려진 경기(시작 + 3시간 <= fit_cutoff)로만 다시 학습해 블록 전체를 예측한다.
  같은 주의 뒤쪽 경기는 앞 경기 결과를 쓰지 못하므로 보수적이지만, 미래 정보는 절대 들어가지 않는다.
- 시즌 역할: 첫 시즌은 학습 전용(이전 기록이 없음), 두 번째 시즌은 검증(하이퍼파라미터 선택),
  그 뒤 시즌은 시험(선택한 설정을 고정하고 한 번만 평가). 시험 시즌 성적으로 설정을 고르지 않는다.
- 학습 경기가 MIN_TRAINING_MATCHES보다 적으면 확률을 만들지 않고 WITHHELD로 남긴다.

실행: python -m pipelines.modeling.backtest  (기록하지 않고 보기만: --no-store)
"""
import argparse
import bisect
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from .dataset import AVAILABILITY_LAG, load_dataset
from .models import BaseRateModel, DixonColesModel, PoissonModel

MIN_TRAINING_MATCHES = 100
OUTCOMES = ("HOME", "DRAW", "AWAY")
MODE = "HISTORICAL_RECONSTRUCTION"
AVAILABILITY_RULE = "result known at kickoff + 3h"
# 검증 시즌에서 고를 후보. ridge는 축소 강도, half_life_days는 오래된 경기를 잊는 속도.
RIDGE_GRID = (1.0, 3.0, 10.0, 30.0)
HALF_LIFE_GRID = (None, 365.0, 180.0, 90.0)


def season_roles(rows: list[dict]) -> dict:
    """결과가 있는 시즌을 순서대로 학습 전용·검증·시험으로 나눈다."""
    seasons = sorted({row["season"] for row in rows if row["result"] is not None})
    if len(seasons) < 3:
        raise ValueError(f"시즌이 3개 이상 필요합니다(학습 전용·검증·시험): {seasons}")
    return {"warmup": seasons[:1], "validation": seasons[1:2], "test": seasons[2:]}


def training_tuple(row: dict) -> tuple:
    return row["kickoff_utc"], row["home_team"], row["away_team"], row["ft_home"], row["ft_away"]


def blocks(targets: list[dict]) -> list[tuple[datetime, list[dict]]]:
    """평가 대상 경기를 ISO 주 단위로 묶고, 블록마다 (fit_cutoff = 첫 경기 시작 시각, 경기들)을 돌려준다."""
    grouped = defaultdict(list)
    for row in targets:
        grouped[row["kickoff_utc"].astimezone(timezone.utc).isocalendar()[:2]].append(row)
    return sorted((min(row["kickoff_utc"] for row in group), group) for group in grouped.values())


def walk_forward(rows: list[dict], make_model, eval_seasons: list[str]) -> list[dict]:
    """eval_seasons의 끝난 경기를 블록마다 다시 학습하며 예측한다. make_model()은 새 모델을 만든다."""
    finished = sorted((row for row in rows if row["result"] is not None), key=lambda row: row["kickoff_utc"])
    known_times = [row["kickoff_utc"] + AVAILABILITY_LAG for row in finished]
    targets = [row for row in finished if row["season"] in eval_seasons]
    predictions = []
    for cutoff, group in blocks(targets):
        training = finished[:bisect.bisect_right(known_times, cutoff)]   # 시작 + 3시간 <= cutoff인 경기만
        model = None
        if len(training) >= MIN_TRAINING_MATCHES:
            model = make_model().fit([training_tuple(row) for row in training], cutoff)
        for row in group:
            prediction = {"match_id": row["match_id"], "season": row["season"], "kickoff_utc": row["kickoff_utc"],
                          "fit_cutoff": cutoff, "training_matches": len(training),
                          "last_training_kickoff": training[-1]["kickoff_utc"] if training else None,
                          "result": row["result"], "probabilities": None, "withheld_reason": None}
            if model is None:
                prediction["withheld_reason"] = f"INSUFFICIENT_TRAINING_DATA(<{MIN_TRAINING_MATCHES})"
            else:
                prediction["probabilities"] = model.predict(row["home_team"], row["away_team"])
            predictions.append(prediction)
    return predictions


def log_loss(probabilities, result: str) -> float:
    return -math.log(max(probabilities[OUTCOMES.index(result)], 1e-15))


def brier(probabilities, result: str) -> float:
    """세 결과의 (확률 - 실제)² 합. 0이 완벽, 무작위 1/3씩이면 0.667."""
    return sum((p - (outcome == result)) ** 2 for p, outcome in zip(probabilities, OUTCOMES))


def market_probabilities(row: dict, timing: str):
    values = [row.get(f"market_{timing}_{side}") for side in ("home", "draw", "away")]
    if any(value is None for value in values):
        return None
    total = sum(float(value) for value in values)
    return tuple(float(value) / total for value in values)


def summarize(predictions: list[dict], rows_by_id: dict) -> dict:
    """시즌별·전체 성적. 시장 기준선은 시장 확률이 있는 같은 경기끼리 비교한다."""
    def metrics(items):
        scored = [item for item in items if item["probabilities"] is not None]
        result = {"matches": len(items), "withheld": len(items) - len(scored)}
        if scored:
            result["log_loss"] = round(sum(log_loss(i["probabilities"], i["result"]) for i in scored) / len(scored), 4)
            result["brier"] = round(sum(brier(i["probabilities"], i["result"]) for i in scored) / len(scored), 4)
        for timing in ("pre", "close"):
            pairs = [(item, market_probabilities(rows_by_id[item["match_id"]], timing)) for item in scored]
            pairs = [(item, market) for item, market in pairs if market is not None]
            if pairs:
                result[f"vs_market_{timing}"] = {
                    "matches": len(pairs),
                    "model_log_loss": round(sum(log_loss(i["probabilities"], i["result"]) for i, _ in pairs) / len(pairs), 4),
                    "market_log_loss": round(sum(log_loss(m, i["result"]) for i, m in pairs) / len(pairs), 4)}
        return result

    by_season = defaultdict(list)
    for item in predictions:
        by_season[item["season"]].append(item)
    return {"overall": metrics(predictions), "by_season": {season: metrics(items) for season, items in sorted(by_season.items())}}


def dataset_hash(rows: list[dict]) -> str:
    """학습·평가에 쓰는 열만으로 만든 해시. 결과·일정이 하나라도 바뀌면 달라진다."""
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda row: row["match_id"]):
        digest.update(repr((row["match_id"], row["season"], row["kickoff_utc"].isoformat(), row["home_team"],
                            row["away_team"], row["ft_home"], row["ft_away"], row["result"])).encode())
    digest.update(AVAILABILITY_RULE.encode())
    return digest.hexdigest()


def store_run(conn: psycopg.Connection, model, purpose: str, eval_seasons: list[str], data_hash: str,
              summary: dict, predictions: list[dict]) -> tuple[int, bool]:
    """실행과 예측을 한 트랜잭션으로 저장한다. 같은 실행이 이미 있으면 그대로 두고 (run_id, False)를 돌려준다."""
    with conn.transaction():
        inserted = conn.execute("""
            INSERT INTO modeling.backtest_runs (model_name, model_version, params, purpose, eval_seasons,
                                                dataset_hash, mode, availability_rule, summary, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (model_version, purpose, eval_seasons, dataset_hash) DO NOTHING RETURNING run_id""",
            (model.name, model.version, Jsonb(model.params), purpose, eval_seasons, data_hash, MODE,
             AVAILABILITY_RULE, Jsonb(summary), datetime.now(timezone.utc))).fetchone()
        if inserted is None:
            existing = conn.execute("""SELECT run_id FROM modeling.backtest_runs WHERE model_version = %s
                                       AND purpose = %s AND eval_seasons = %s AND dataset_hash = %s""",
                                    (model.version, purpose, eval_seasons, data_hash)).fetchone()
            return existing[0], False
        run_id = inserted[0]
        with conn.cursor().copy("""COPY modeling.backtest_predictions (run_id, match_id, season, kickoff_utc, fit_cutoff,
                                   last_training_kickoff, training_matches, status, withheld_reason,
                                   prob_home, prob_draw, prob_away, result) FROM STDIN""") as copy:
            for item in predictions:
                probabilities = item["probabilities"] or (None, None, None)
                copy.write_row((run_id, item["match_id"], item["season"], item["kickoff_utc"], item["fit_cutoff"],
                                item["last_training_kickoff"], item["training_matches"],
                                "PREDICTED" if item["probabilities"] else "WITHHELD", item["withheld_reason"],
                                *[None if p is None else round(p, 8) for p in probabilities], item["result"]))
        return run_id, True


def candidates() -> dict:
    """모델 계열별 후보 설정. 단순 비율은 고를 것이 없다."""
    return {
        "base_rate": [BaseRateModel],
        "poisson": [lambda r=r, h=h: PoissonModel(ridge=r, half_life_days=h) for r in RIDGE_GRID for h in HALF_LIFE_GRID],
        "dixon_coles": [lambda r=r, h=h: DixonColesModel(ridge=r, half_life_days=h)
                        for r in RIDGE_GRID for h in HALF_LIFE_GRID],
    }


def run(rows: list[dict], conn: psycopg.Connection | None = None) -> dict:
    """검증 시즌에서 계열별 최선의 설정을 고르고, 그 설정만 시험 시즌에 한 번 평가한다. conn이 있으면 기록한다."""
    roles = season_roles(rows)
    rows_by_id = {row["match_id"]: row for row in rows}
    data_hash = dataset_hash(rows)
    report = {"seasons": roles, "dataset_hash": data_hash, "validation": {}, "test": {}}
    for family, makers in candidates().items():
        tried = []
        for make_model in makers:
            predictions = walk_forward(rows, make_model, roles["validation"])
            summary = summarize(predictions, rows_by_id)
            model = make_model()
            if conn is not None:
                store_run(conn, model, "VALIDATION", roles["validation"], data_hash, summary, predictions)
            tried.append((summary["overall"].get("log_loss", math.inf), model.version, make_model))
        best_loss, best_version, best_maker = min(tried, key=lambda item: item[0])
        report["validation"][family] = {"tried": len(tried), "chosen": best_version, "log_loss": best_loss}
        predictions = walk_forward(rows, best_maker, roles["test"])
        summary = summarize(predictions, rows_by_id)
        if conn is not None:
            run_id, created = store_run(conn, best_maker(), "TEST", roles["test"], data_hash, summary, predictions)
            summary["run_id"], summary["newly_stored"] = run_id, created
        report["test"][family] = {"model_version": best_version, **summary}
    return report


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="기준 모델 시간순 백테스트(3b)")
    parser.add_argument("--no-store", action="store_true", help="결과를 DB에 기록하지 않고 출력만 한다")
    args = parser.parse_args(argv)
    with connect(load_settings()) as conn:
        migrate(conn)
        rows = load_dataset(conn, finished_only=False)
        report = run(rows, None if args.no_store else conn)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
