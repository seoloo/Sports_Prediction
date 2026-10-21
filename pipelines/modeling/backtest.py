"""기준 모델의 시간순(walk-forward) 백테스트(로드맵 3b, 3e에서 개선 모델 추가).

규칙
- 평가할 경기를 ISO 주(월~일, UTC) 단위 블록으로 나누고, 블록마다 그 블록의 첫 경기 시작 시각(fit_cutoff)에
  결과가 알려진 경기(시작 + 3시간 <= fit_cutoff)로만 다시 학습해 블록 전체를 예측한다.
  같은 주의 뒤쪽 경기는 앞 경기 결과를 쓰지 못하므로 보수적이지만, 미래 정보는 절대 들어가지 않는다.
- 시즌 역할: 첫 시즌은 학습 전용(이전 기록이 없음), 두 번째 시즌은 검증(하이퍼파라미터 선택),
  그 뒤 시즌은 시험(선택한 설정을 고정하고 한 번만 평가). 시험 시즌 성적으로 설정을 고르지 않는다.
- 학습 경기가 MIN_TRAINING_MATCHES보다 적으면 확률을 만들지 않고 WITHHELD로 남긴다.
- 3e 개선 계열
  - poisson_promoted: 승격 첫 시즌 효과를 함께 학습(후보 격자는 poisson과 같다).
  - *_calibrated: 기준 계열에서 고른 설정에 확률 보정 계수(temperature) 하나를 더한다.
    3f: 검증 점수는 교차 보정으로 낸다. 검증 시즌마다 "다른 검증 시즌에서 고른 계수"를 적용해 점수를 매기므로,
    계수를 고른 자료로 그 계수를 채점하는 낙관이 없다. 시험·운영에 쓰는 계수는 검증 시즌 전체로 고른다.

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
from .dataset import AVAILABILITY_LAG, attach_stats, load_dataset, load_match_stats
from .models import (BaseRateModel, DixonColesModel, MarketBlendModel, PoissonModel, TrainingMatch, blend,
                     model_from_params, promoted_by_season, sharpen)

MIN_TRAINING_MATCHES = 100
# 하이퍼파라미터를 고르는 시즌(3e: 한 시즌 380경기로는 비슷한 후보 사이 차이가 잡음 수준이라 두 시즌으로 늘림).
# 2021-22는 그동안 학습 전용이었고 시험에 쓰인 적이 없다. 이 뒤의 시즌만 시험이다.
VALIDATION_SEASONS = ("2021-22", "2022-23")
OUTCOMES = ("HOME", "DRAW", "AWAY")
MODE = "HISTORICAL_RECONSTRUCTION"
AVAILABILITY_RULE = "result known at kickoff + 3h"
# 검증 시즌에서 고를 후보. ridge는 축소 강도, half_life_days는 오래된 경기를 잊는 속도.
RIDGE_GRID = (1.0, 3.0, 10.0, 30.0)
HALF_LIFE_GRID = (None, 365.0, 180.0, 90.0)
TEMPERATURE_GRID = tuple(round(0.70 + 0.01 * step, 2) for step in range(91))   # 0.70 ~ 1.60
CALIBRATED_FROM = {"poisson_calibrated": "poisson", "poisson_promoted_calibrated": "poisson_promoted",
                   "poisson_plus_calibrated": "poisson_plus"}
CALIBRATION_METHOD = "cross_fit_by_season"
# 3g: poisson에서 고른 ridge·반감기를 고정하고 경기 기록 옵션만 고른다(256개 전체 격자 대신 20개).
SHOT_WEIGHT_GRID = (0.0, 0.25, 0.5, 0.75, 1.0)
RED_CARD_WEIGHT_GRID = (1.0, 0.5)
REST_EFFECT_GRID = (False, True)
MARKET_WEIGHT_GRID = tuple(round(0.05 * step, 2) for step in range(21))   # 0 ~ 1


def season_roles(rows: list[dict], validation_seasons: tuple[str, ...] | None = None) -> dict:
    """결과가 있는 시즌을 학습 전용·검증·시험으로 나눈다.

    validation_seasons가 모두 자료에 있으면 그 앞은 학습 전용, 뒤는 시험이다. 시즌 자료를 앞쪽에 더해도
    (3e: 2020-21 추가) 시험 시즌이 밀리지 않게 고정하는 용도다. 이미 설정 선택에 쓴 시즌이
    시험으로 바뀌면 시험 결과가 낙관적이 된다. 없으면 첫 시즌 학습 전용, 두 번째 검증, 나머지 시험.
    """
    seasons = sorted({row["season"] for row in rows if row["result"] is not None})
    if validation_seasons and all(season in seasons for season in validation_seasons):
        first, last = seasons.index(validation_seasons[0]), seasons.index(validation_seasons[-1])
    else:
        first = last = 1
    if first < 1 or len(seasons) < last + 2:
        raise ValueError(f"검증 시즌 앞뒤로 시즌이 하나 이상 필요합니다(학습 전용·검증·시험): {seasons}")
    return {"warmup": seasons[:first], "validation": seasons[first:last + 1], "test": seasons[last + 1:]}


def mark_promoted(rows: list[dict]) -> list[dict]:
    """경기마다 홈·원정 팀이 그 시즌 승격 팀인지(home_new, away_new)를 붙인 복사본."""
    promoted = promoted_by_season((row["season"], row["home_team"], row["away_team"]) for row in rows)
    return [{**row, "home_new": row["home_team"] in promoted.get(row["season"], set()),
             "away_new": row["away_team"] in promoted.get(row["season"], set())} for row in rows]


def _float(value):
    return None if value is None else float(value)


def training_tuple(row: dict) -> TrainingMatch:
    return TrainingMatch(row["kickoff_utc"], row["home_team"], row["away_team"], row["ft_home"], row["ft_away"],
                         row.get("home_new", False), row.get("away_new", False), row.get("home_sot"),
                         row.get("away_sot"), row.get("red_cards", 0), _float(row.get("home_rest_days")),
                         _float(row.get("away_rest_days")))


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
                prediction["probabilities"] = model.predict(row["home_team"], row["away_team"],
                                                            row.get("home_new", False), row.get("away_new", False),
                                                            _float(row.get("home_rest_days")),
                                                            _float(row.get("away_rest_days")))
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
                            row["away_team"], row["ft_home"], row["ft_away"], row["result"],
                            row.get("home_sot"), row.get("away_sot"), row.get("red_cards"))).encode())
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
        "poisson_promoted": [lambda r=r, h=h: PoissonModel(name="poisson_promoted", ridge=r, half_life_days=h,
                                                           promoted_effect=True)
                             for r in RIDGE_GRID for h in HALF_LIFE_GRID],
    }


def cross_fit_temperature(predictions: list[dict]) -> tuple[list[dict], dict]:
    """검증 예측을 시즌별로 나눠, 각 시즌에는 나머지 시즌에서 고른 보정 계수를 적용한 예측과 시즌별 계수.

    검증 시즌이 하나뿐이면 나눌 수 없으므로 ValueError(같은 자료로 고르고 채점하는 낙관을 조용히 허용하지 않는다).
    """
    seasons = sorted({item["season"] for item in predictions})
    if len(seasons) < 2:
        raise ValueError(f"교차 보정에는 검증 시즌이 2개 이상 필요합니다: {seasons}")
    calibrated, temperatures = [], {}
    for season in seasons:
        temperatures[season] = fit_temperature([item for item in predictions if item["season"] != season])
        calibrated += [{**item, "probabilities": sharpen(item["probabilities"], temperatures[season])}
                       if item["probabilities"] else item for item in predictions if item["season"] == season]
    return calibrated, temperatures


def fit_market_weight(predictions: list[dict], markets: dict) -> float:
    """검증 예측과 시장 확률을 섞을 때 로그 손실이 가장 낮은 시장 비중. 같으면 0.5에 가까운 쪽."""
    scored = [item for item in predictions if item["probabilities"] is not None and markets.get(item["match_id"])]
    if not scored:
        return 1.0
    losses = {weight: sum(log_loss(blend(item["probabilities"], markets[item["match_id"]], weight), item["result"])
                          for item in scored) for weight in MARKET_WEIGHT_GRID}
    return min(losses, key=lambda weight: (losses[weight], abs(weight - 0.5)))


def blend_predictions(predictions: list[dict], markets: dict, weight_for) -> list[dict]:
    """예측마다 시장 확률과 섞는다. 시장 확률이 없으면 확률 없이 보류(MARKET_ODDS_MISSING)."""
    blended = []
    for item in predictions:
        market = markets.get(item["match_id"])
        if item["probabilities"] is None or market is None:
            blended.append({**item, "probabilities": None,
                            "withheld_reason": item["withheld_reason"] or "MARKET_ODDS_MISSING"})
        else:
            blended.append({**item, "probabilities": blend(item["probabilities"], market, weight_for(item))})
    return blended


def fit_temperature(predictions: list[dict]) -> float:
    """검증 예측의 로그 손실이 가장 낮아지는 보정 계수. 1이면 보정하지 않는 것과 같다."""
    scored = [item for item in predictions if item["probabilities"] is not None]
    if not scored:
        return 1.0
    losses = {temperature: sum(log_loss(sharpen(item["probabilities"], temperature), item["result"]) for item in scored)
              for temperature in TEMPERATURE_GRID}
    return min(losses, key=lambda temperature: (losses[temperature], abs(temperature - 1)))


def run(rows: list[dict], conn: psycopg.Connection | None = None) -> dict:
    """검증 시즌에서 계열별 최선의 설정을 고르고, 그 설정만 시험 시즌에 한 번 평가한다. conn이 있으면 기록한다."""
    roles = season_roles(rows, VALIDATION_SEASONS)
    rows = mark_promoted(rows)
    rows_by_id = {row["match_id"]: row for row in rows}
    data_hash = dataset_hash(rows)
    report = {"seasons": roles, "dataset_hash": data_hash, "validation": {}, "test": {}}
    chosen = {}   # 계열 → (만드는 함수, 검증 예측(점수를 매긴 그대로), 시험 예측)

    def record(purpose, model, seasons, predictions):
        summary = summarize(predictions, rows_by_id)
        if conn is not None:
            run_id, created = store_run(conn, model, purpose, seasons, data_hash, summary, predictions)
            summary["run_id"], summary["newly_stored"] = run_id, created
        return summary

    def test_once(family, make_model, version):
        predictions = walk_forward(rows, make_model, roles["test"])
        report["test"][family] = {"model_version": version, **record("TEST", make_model(), roles["test"], predictions)}
        return predictions

    def select(family, makers):
        tried = []
        for make_model in makers:
            predictions = walk_forward(rows, make_model, roles["validation"])
            summary = record("VALIDATION", make_model(), roles["validation"], predictions)
            tried.append((summary["overall"].get("log_loss", math.inf), make_model().version, make_model, predictions))
        best_loss, best_version, best_maker, best_predictions = min(tried, key=lambda item: item[0])
        report["validation"][family] = {"tried": len(tried), "chosen": best_version, "log_loss": best_loss}
        chosen[family] = (best_maker, best_predictions, test_once(family, best_maker, best_version))

    for family, makers in candidates().items():
        select(family, makers)

    # 3g: 경기 기록 옵션. poisson에서 고른 ridge·반감기를 그대로 쓴다.
    poisson_params = chosen["poisson"][0]().params
    select("poisson_plus", [
        lambda w=w, r=r, e=e, p=poisson_params: PoissonModel(name="poisson_plus", ridge=p["ridge"],
                                                             half_life_days=p["half_life_days"], shot_weight=w,
                                                             red_card_weight=r, rest_effect=e)
        for w in SHOT_WEIGHT_GRID for r in RED_CARD_WEIGHT_GRID for e in REST_EFFECT_GRID])

    for family, base in CALIBRATED_FROM.items():
        base_maker, base_predictions, _ = chosen[base]
        temperature = fit_temperature(base_predictions)   # 시험·운영에 쓰는 계수: 검증 시즌 전체로
        calibrated, fold_temperatures = cross_fit_temperature(base_predictions)   # 검증 점수용: 교차 보정
        params = {**base_maker().params, "temperature": temperature, "calibration": CALIBRATION_METHOD}

        def make_model(family=family, params=params):
            return model_from_params(family, params)

        summary = record("VALIDATION", make_model(), roles["validation"], calibrated)
        version = make_model().version
        report["validation"][family] = {"tried": len(TEMPERATURE_GRID), "chosen": version, "temperature": temperature,
                                        "fold_temperatures": fold_temperatures,
                                        "log_loss": summary["overall"].get("log_loss")}
        chosen[family] = (make_model, calibrated, test_once(family, make_model, version))

    # 3g: 시장 결합. 시장 확률(마감 전 평균 배당)이 없는 계열 중 검증 점수가 가장 좋은 모델을 출발점으로 쓴다.
    markets = {row["match_id"]: market_probabilities(row, "pre") for row in rows}
    base_family = min((family for family in chosen if family != "base_rate"),
                      key=lambda family: report["validation"][family]["log_loss"])
    base_maker, base_validation, base_test = chosen[base_family]
    seasons = sorted({item["season"] for item in base_validation})
    fold_weights = {season: fit_market_weight([i for i in base_validation if i["season"] != season], markets)
                    for season in seasons}
    weight = fit_market_weight(base_validation, markets)
    model = MarketBlendModel(base=base_maker(), market_weight=weight, calibration=CALIBRATION_METHOD)
    validation = blend_predictions(base_validation, markets, lambda item: fold_weights[item["season"]])
    summary = record("VALIDATION", model, roles["validation"], validation)
    report["validation"]["market_blend"] = {"base": base_family, "chosen": model.version, "market_weight": weight,
                                            "fold_weights": fold_weights, "log_loss": summary["overall"].get("log_loss")}
    test = blend_predictions(base_test, markets, lambda item: weight)
    report["test"]["market_blend"] = {"model_version": model.version, **record("TEST", model, roles["test"], test)}
    return report


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="기준 모델 시간순 백테스트(3b)")
    parser.add_argument("--no-store", action="store_true", help="결과를 DB에 기록하지 않고 출력만 한다")
    args = parser.parse_args(argv)
    with connect(load_settings()) as conn:
        migrate(conn)
        rows = attach_stats(load_dataset(conn, finished_only=False), load_match_stats(conn))
        report = run(rows, None if args.no_store else conn)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
