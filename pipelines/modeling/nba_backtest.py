"""NBA 모델의 시간순(walk-forward) 백테스트와 평가 보고서(로드맵 5d-c).

축구(backtest.py, evaluation.py)와 같은 규칙을 2지선다에 맞춰 쓴다.
- ISO 주 단위 블록. 블록의 첫 경기 시작 시각(fit_cutoff)에 결과가 알려진 경기(시작 + 4시간 <= fit_cutoff)로만 학습.
- 시즌 역할: 2018-19~2020-21 학습 전용, 2021-22·2022-23 검증(설정 선택), 2023-24 이후 시험(고른 설정으로 한 번만).
  2019-20 버블·2020-21 관중 제한 시즌은 홈 이점이 달라 검증·시험에서 빼고 학습에만 쓴다.
- 평가: 로그 손실·Brier, 홈 승 확률 구간별 보정(ECE, Wilson), 주 단위 블록 부트스트랩으로 모델 간 차이의 95% 구간.
- 검증 기준(축구와 같음): ECE ≤ 0.03, 그리고 시장 배당보다 확실히 나아야 한다. 과거 NBA 배당이 없어 시장 비교를
  할 수 없으므로 지금은 모두 NOT_VALIDATED다. 앞으로 쌓이는 경기 전 배당(5d-b)으로 비교한다.

실행: python -m pipelines.modeling.nba_backtest [--no-store] [--markdown-dir docs/reports]
"""
import argparse
import bisect
import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from .backtest import MODE, blocks as football_blocks, season_roles
from .evaluation import BOOTSTRAP_SAMPLES, BOOTSTRAP_SEED, CALIBRATION_EDGES, GATE, block_bootstrap, wilson
from .nba_models import BaseRateModel, EloModel, Game, MarginRatingModel

COMPETITION = "nba"
AVAILABILITY_LAG = timedelta(hours=4)
AVAILABILITY_RULE = "result known at tip-off + 4h"
VALIDATION_SEASONS = ("2021-22", "2022-23")
MIN_TRAINING_GAMES = 200
BUBBLE_START = date(2020, 7, 30)   # 2019-20 재개 시즌, 올랜도 한 곳(중립)
RIDGE_GRID = (1.0, 3.0, 10.0, 30.0)
# 첫 실행에서 검증 선택이 격자 끝(반감기 90일, 홈 50, 회귀 0.5)에 걸려 넓혔다(5d-c, 시험 결과는 이때 한 번 봄).
HALF_LIFE_GRID = (None, 365.0, 180.0, 90.0, 60.0, 45.0)
ELO_K_GRID = (10.0, 15.0, 20.0, 25.0, 30.0)
ELO_HOME_GRID = (25.0, 50.0, 75.0, 100.0)
ELO_CARRYOVER_GRID = (0.25, 0.5, 0.75)
EARLY_GAMES = 10
METHOD = {"bootstrap_samples": BOOTSTRAP_SAMPLES, "seed": BOOTSTRAP_SEED, "block": "fit_cutoff (ISO week)",
          "calibration_edges": CALIBRATION_EDGES, "early_games": EARLY_GAMES, "gate": GATE, "version": "5d-c.1"}
COMPARISONS = (("margin_rating", "base_rate"), ("elo", "base_rate"), ("elo", "margin_rating"),
               ("margin_rating_rest", "margin_rating"))
LABELS = {"base_rate": "단순 비율", "margin_rating": "점수 차 등급", "margin_rating_rest": "점수 차 등급+연전",
          "elo": "Elo"}


def load_games(conn: psycopg.Connection, as_of: datetime | None = None) -> tuple[list[dict], dict]:
    """as_of(기본: 지금)에 알려진 끝난 NBA 경기를 시작 시각 순서로. 중립·연전 표시를 붙인다. (행, 제외 건수).

    경기 전 예측(5d-d)은 as_of = 스냅샷 시각으로 불러, 그때 실제로 받아 둔 결과만 쓴다.
    """
    schedule = defaultdict(set)   # 팀 → 경기 날짜(미국 동부). 연전 판단용. 일정은 경기 전에 공개된다
    rows, excluded = [], {"no_tipoff": 0}
    for (game_id, season, postseason, tipoff, local_date, home, away, home_score, away_score,
         status) in conn.execute("""
            SELECT g.game_id, g.season, g.postseason, k.tipoff_utc, k.local_date, h.canonical_name, a.canonical_name,
                   k.home_score, k.away_score, k.status
            FROM silver.games g JOIN silver.game_revisions_known_at(COALESCE(%s, now())) k USING (game_id)
            JOIN silver.teams h ON h.team_id = g.home_team_id JOIN silver.teams a ON a.team_id = g.away_team_id
            WHERE g.competition_code = %s AND k.status <> 'POSTPONED'
            ORDER BY k.tipoff_utc NULLS LAST, g.game_id""", (as_of, COMPETITION)).fetchall():
        schedule[home].add(local_date)
        schedule[away].add(local_date)
        if status != "FINISHED":
            continue
        if tipoff is None:
            excluded["no_tipoff"] += 1
            continue
        rows.append({"game_id": game_id, "season": season, "postseason": postseason, "tipoff_utc": tipoff,
                     "kickoff_utc": tipoff, "local_date": local_date, "home_team": home, "away_team": away,
                     "home_score": home_score, "away_score": away_score,
                     "result": "HOME" if home_score > away_score else "AWAY",
                     "neutral": season == "2019-20" and local_date >= BUBBLE_START})
    for row in rows:
        before = row["local_date"] - timedelta(days=1)
        row["home_b2b"], row["away_b2b"] = before in schedule[row["home_team"]], before in schedule[row["away_team"]]
    return rows, excluded


def as_game(row: dict) -> Game:
    return Game(row["tipoff_utc"], row["season"], row["home_team"], row["away_team"], row["home_score"],
                row["away_score"], row["neutral"], row["home_b2b"], row["away_b2b"])


def walk_forward(rows: list[dict], make_model, eval_seasons: list[str]) -> list[dict]:
    """eval_seasons 경기를 주 단위 블록마다 다시 학습하며 예측한다. incremental 모델은 한 인스턴스에 이어서 넣는다."""
    known_times = [row["tipoff_utc"] + AVAILABILITY_LAG for row in rows]
    games = [as_game(row) for row in rows]
    targets = [row for row in rows if row["season"] in eval_seasons]
    shared = make_model() if getattr(make_model(), "incremental", False) else None
    predictions = []
    for cutoff, group in football_blocks(targets):
        count = bisect.bisect_right(known_times, cutoff)
        model = None
        if count >= MIN_TRAINING_GAMES:
            model = (shared or make_model()).fit(games[:count], cutoff)
        for row in group:
            predictions.append({"game_id": row["game_id"], "season": row["season"], "tipoff_utc": row["tipoff_utc"],
                                "fit_cutoff": cutoff, "training_games": count,
                                "last_training_tipoff": rows[count - 1]["tipoff_utc"] if count else None,
                                "result": row["result"], "postseason": row["postseason"],
                                "prob_home": model.predict(as_game(row)) if model else None,
                                "withheld_reason": None if model else f"INSUFFICIENT_TRAINING_DATA(<{MIN_TRAINING_GAMES})"})
    return predictions


def log_loss(p: float, result: str) -> float:
    return -math.log(max(p if result == "HOME" else 1 - p, 1e-15))


def brier(p: float, result: str) -> float:
    return (p - (result == "HOME")) ** 2


def summarize(predictions: list[dict]) -> dict:
    def metrics(items):
        scored = [item for item in items if item["prob_home"] is not None]
        result = {"games": len(items), "withheld": len(items) - len(scored)}
        if scored:
            result["log_loss"] = round(sum(log_loss(i["prob_home"], i["result"]) for i in scored) / len(scored), 4)
            result["brier"] = round(sum(brier(i["prob_home"], i["result"]) for i in scored) / len(scored), 4)
        return result
    by_season = defaultdict(list)
    for item in predictions:
        by_season[item["season"]].append(item)
    return {"overall": metrics(predictions), "by_season": {season: metrics(items) for season, items in sorted(by_season.items())}}


def dataset_hash(rows: list[dict]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda row: row["game_id"]):
        digest.update(repr((row["game_id"], row["season"], row["tipoff_utc"].isoformat(), row["home_team"], row["away_team"],
                            row["home_score"], row["away_score"], row["neutral"], row["home_b2b"], row["away_b2b"])).encode())
    digest.update(AVAILABILITY_RULE.encode())
    return digest.hexdigest()


def store_run(conn: psycopg.Connection, model, purpose: str, eval_seasons: list[str], data_hash: str,
              summary: dict, predictions: list[dict]) -> tuple[int, bool]:
    """실행과 예측을 한 트랜잭션으로 저장한다. 같은 실행이 있으면 그대로 두고 (run_id, False)."""
    with conn.transaction():
        inserted = conn.execute("""
            INSERT INTO modeling.backtest_runs (model_name, model_version, params, purpose, eval_seasons,
                                                dataset_hash, mode, availability_rule, summary, created_at, competition_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (competition_code, model_version, purpose, eval_seasons, dataset_hash) DO NOTHING RETURNING run_id""",
            (model.name, model.version, Jsonb(model.params), purpose, eval_seasons, data_hash, MODE,
             AVAILABILITY_RULE, Jsonb(summary), datetime.now(timezone.utc), COMPETITION)).fetchone()
        if inserted is None:
            return conn.execute("""SELECT run_id FROM modeling.backtest_runs WHERE competition_code = %s
                                   AND model_version = %s AND purpose = %s AND eval_seasons = %s AND dataset_hash = %s""",
                                (COMPETITION, model.version, purpose, eval_seasons, data_hash)).fetchone()[0], False
        run_id = inserted[0]
        with conn.cursor().copy("""COPY modeling.game_backtest_predictions (run_id, game_id, season, tipoff_utc, fit_cutoff,
                                   last_training_tipoff, training_games, status, withheld_reason, prob_home, result)
                                   FROM STDIN""") as copy:
            for item in predictions:
                copy.write_row((run_id, item["game_id"], item["season"], item["tipoff_utc"], item["fit_cutoff"],
                                item["last_training_tipoff"], item["training_games"],
                                "PREDICTED" if item["prob_home"] is not None else "WITHHELD", item["withheld_reason"],
                                None if item["prob_home"] is None else round(item["prob_home"], 8), item["result"]))
        return run_id, True


def candidates() -> dict:
    return {
        "base_rate": [BaseRateModel],
        "margin_rating": [lambda r=r, h=h: MarginRatingModel(ridge=r, half_life_days=h)
                          for r in RIDGE_GRID for h in HALF_LIFE_GRID],
        "elo": [lambda k=k, h=h, c=c: EloModel(k=k, home=h, carryover=c)
                for k in ELO_K_GRID for h in ELO_HOME_GRID for c in ELO_CARRYOVER_GRID],
    }


def run(rows: list[dict], conn: psycopg.Connection | None = None) -> dict:
    """검증 시즌에서 계열별 최선의 설정을 고르고, 그 설정만 시험 시즌에 한 번 평가한다. conn이 있으면 기록한다."""
    roles = season_roles(rows, VALIDATION_SEASONS)
    data_hash = dataset_hash(rows)
    report = {"competition": COMPETITION, "seasons": roles, "dataset_hash": data_hash, "validation": {}, "test": {}}
    chosen = {}

    def record(purpose, model, seasons, predictions):
        summary = summarize(predictions)
        if conn is not None:
            summary["run_id"], summary["newly_stored"] = store_run(conn, model, purpose, seasons, data_hash, summary, predictions)
        return summary

    def select(family, makers):
        tried = []
        for make_model in makers:
            predictions = walk_forward(rows, make_model, roles["validation"])
            summary = record("VALIDATION", make_model(), roles["validation"], predictions)
            tried.append((summary["overall"].get("log_loss", math.inf), make_model().version, make_model))
        loss, version, maker = min(tried, key=lambda item: item[0])
        report["validation"][family] = {"tried": len(tried), "chosen": version, "log_loss": loss}
        test = walk_forward(rows, maker, roles["test"])
        report["test"][family] = {"model_version": version, **record("TEST", maker(), roles["test"], test)}
        chosen[family] = maker

    for family, makers in candidates().items():
        select(family, makers)
    params = chosen["margin_rating"]().params   # 연전 효과는 고른 ridge·반감기를 고정하고 켜고 끄기만 비교
    select("margin_rating_rest", [lambda p=params: MarginRatingModel(name="margin_rating_rest", ridge=p["ridge"],
                                                                      half_life_days=p["half_life_days"], rest_effect=True)])
    return report


# ---- 평가 ----

def load_test_predictions(conn: psycopg.Connection) -> tuple[dict, dict]:
    """모델 계열별 가장 최근 NBA 시험 실행의 예측(팀·연전 표시 포함)과 실행 정보."""
    runs = {name: {"run_id": run_id, "model_version": version}
            for name, run_id, version in conn.execute("""
                SELECT DISTINCT ON (model_name) model_name, run_id, model_version FROM modeling.backtest_runs
                WHERE purpose = 'TEST' AND competition_code = %s ORDER BY model_name, run_id DESC""", (COMPETITION,)).fetchall()}
    cursor = conn.execute("""
        SELECT r.model_name, p.game_id, p.season, p.tipoff_utc, p.fit_cutoff, p.prob_home::float AS prob_home, p.result,
               g.postseason, h.canonical_name AS home_team, a.canonical_name AS away_team
        FROM modeling.game_backtest_predictions p JOIN modeling.backtest_runs r USING (run_id)
        JOIN silver.games g USING (game_id)
        JOIN silver.teams h ON h.team_id = g.home_team_id JOIN silver.teams a ON a.team_id = g.away_team_id
        WHERE p.run_id = ANY(%s::bigint[]) AND p.status = 'PREDICTED' ORDER BY p.tipoff_utc, p.game_id""",
        ([run["run_id"] for run in runs.values()],))
    columns = [column.name for column in cursor.description]
    predictions = defaultdict(list)
    for values in cursor.fetchall():
        row = dict(zip(columns, values))
        predictions[row.pop("model_name")].append(row)
    return dict(predictions), runs


def compare(items: list[dict], other: dict) -> dict | None:
    """같은 경기에서 (items − other) 로그 손실 차이의 블록 부트스트랩. 음수면 items 쪽이 더 정확."""
    differences = defaultdict(list)
    for item in items:
        if item["game_id"] in other:
            differences[item["fit_cutoff"]].append(log_loss(item["prob_home"], item["result"])
                                                   - log_loss(other[item["game_id"]], item["result"]))
    return block_bootstrap(differences) if differences else None


def calibration(items: list[dict]) -> dict:
    bins, gap, total = [], 0.0, 0
    for low, high in zip(CALIBRATION_EDGES, CALIBRATION_EDGES[1:]):
        members = [i for i in items if low <= i["prob_home"] < high + (1e-9 if high == 1.0 else 0)]
        if not members:
            continue
        predicted = sum(i["prob_home"] for i in members) / len(members)
        hits = sum(i["result"] == "HOME" for i in members)
        lo, hi = wilson(hits, len(members))
        bins.append({"range": [low, high], "games": len(members), "predicted": round(predicted, 3),
                     "observed": round(hits / len(members), 3), "observed_ci95": [round(lo, 3), round(hi, 3)],
                     "within_ci": lo <= predicted <= hi})
        gap += len(members) * abs(hits / len(members) - predicted)
        total += len(members)
    return {"ece": round(gap / total, 4) if total else None, "bins": bins}


def segments(items: list[dict]) -> dict:
    played, groups = defaultdict(int), defaultdict(list)
    for item in sorted(items, key=lambda item: item["tipoff_utc"]):
        key = (item["season"], item["home_team"]), (item["season"], item["away_team"])
        groups["postseason" if item["postseason"] else "regular"].append(item)
        groups["early" if min(played[key[0]], played[key[1]]) < EARLY_GAMES else "rest"].append(item)
        played[key[0]] += 1
        played[key[1]] += 1
    report = {name: {"games": len(members),
                     "log_loss": round(sum(log_loss(i["prob_home"], i["result"]) for i in members) / len(members), 4)}
              for name, members in groups.items()}
    wins = sum(i["result"] == "HOME" for i in items)
    lo, hi = wilson(wins, len(items))
    report["home_win"] = {"predicted_rate": round(sum(i["prob_home"] for i in items) / len(items), 3),
                          "actual_rate": round(wins / len(items), 3), "actual_ci95": [round(lo, 3), round(hi, 3)]}
    return report


def gate(model_report: dict) -> dict:
    reasons = []
    if model_report["calibration"]["ece"] > GATE["max_ece"]:
        reasons.append(f"보정 오차(ECE) {model_report['calibration']['ece']:.3f} > 기준 {GATE['max_ece']}")
    reasons.append("시장 마감 전 배당과 비교할 과거 자료 없음(무료 과거 NBA 배당 없음, 5d-b부터 쌓는 중)")
    return {"status": "NOT_VALIDATED", "reasons": reasons}


def evaluate(predictions: dict, runs: dict) -> dict:
    report = {"competition": COMPETITION, "method": METHOD, "runs": runs, "models": {}, "comparisons": {}}
    probabilities = {name: {i["game_id"]: i["prob_home"] for i in items} for name, items in predictions.items()}
    for name, items in predictions.items():
        model_report = {"games": len(items),
                        "log_loss": round(sum(log_loss(i["prob_home"], i["result"]) for i in items) / len(items), 4),
                        "brier": round(sum(brier(i["prob_home"], i["result"]) for i in items) / len(items), 4),
                        "accuracy": round(sum((i["prob_home"] >= 0.5) == (i["result"] == "HOME") for i in items) / len(items), 4),
                        "calibration": calibration(items), "segments": segments(items)}
        model_report["gate"] = gate(model_report)
        report["models"][name] = model_report
    for left, right in COMPARISONS:
        if left in predictions and right in probabilities:
            report["comparisons"][f"{left}_vs_{right}"] = compare(predictions[left], probabilities[right])
    return report


def store_report(conn: psycopg.Connection, report: dict) -> tuple[int, bool]:
    run_ids = sorted(run["run_id"] for run in report["runs"].values())
    method_hash = hashlib.sha256(json.dumps(METHOD, sort_keys=True, default=str).encode()).hexdigest()
    with conn.transaction():
        inserted = conn.execute("""
            INSERT INTO modeling.evaluation_reports (run_ids, method_hash, method, results, created_at, competition_code)
            VALUES (%s::bigint[], %s, %s, %s, %s, %s) ON CONFLICT (run_ids, method_hash) DO NOTHING RETURNING report_id""",
            (run_ids, method_hash, Jsonb(METHOD), Jsonb(json.loads(json.dumps(report, default=str))),
             datetime.now(timezone.utc), COMPETITION)).fetchone()
        if inserted:
            return inserted[0], True
        return conn.execute("SELECT report_id FROM modeling.evaluation_reports WHERE run_ids = %s::bigint[] AND method_hash = %s",
                            (run_ids, method_hash)).fetchone()[0], False


def interval(result: dict | None) -> str:
    return "—" if result is None else f"{result['mean']:+.4f} [{result['ci95'][0]:+.4f}, {result['ci95'][1]:+.4f}]"


def render_markdown(report: dict, backtest: dict | None = None) -> str:
    lines = ["# NBA 기준 모델 평가 보고서", "",
             "`python -m pipelines.modeling.nba_backtest --markdown-dir docs/reports`가 만든 문서입니다. 직접 고치지 말고 다시 생성하세요.", "",
             f"- 대상: 시험(TEST) 백테스트의 끝난 경기. 과거 재구성 가정({AVAILABILITY_RULE}).",
             f"- 불확실성: 주 단위 블록 부트스트랩 {BOOTSTRAP_SAMPLES}회(seed {BOOTSTRAP_SEED}). 차이는 왼쪽 − 오른쪽 로그 손실, 음수면 왼쪽이 더 정확.",
             "- 실행: " + ", ".join(f"{LABELS.get(n, n)} run {r['run_id']} (`{r['model_version']}`)" for n, r in sorted(report["runs"].items()))]
    if backtest:
        lines.append("- 시즌: 학습 전용 " + ", ".join(backtest["seasons"]["warmup"]) + " · 검증 "
                     + ", ".join(backtest["seasons"]["validation"]) + " · 시험 " + ", ".join(backtest["seasons"]["test"]))
    lines += ["", "## 요약", "", "| 모델 | 경기 | 로그 손실 | Brier | 적중률(50% 기준) | ECE | 검증 상태 |", "|---|---|---|---|---|---|---|"]
    for name, model in report["models"].items():
        lines.append(f"| {LABELS.get(name, name)} | {model['games']} | {model['log_loss']:.4f} | {model['brier']:.4f} | "
                     f"{model['accuracy']:.1%} | {model['calibration']['ece']:.4f} | {model['gate']['status']} |")
    lines += ["", "## 모델끼리 비교", "", "| 비교 | 경기 | 주 | 차이 [95%] | 왼쪽이 나은 비율 |", "|---|---|---|---|---|"]
    for name, result in report["comparisons"].items():
        left, right = name.split("_vs_")
        if result:
            lines.append(f"| {LABELS.get(left, left)} − {LABELS.get(right, right)} | {result['matches']} | {result['blocks']} | "
                         f"{interval(result)} | {result['share_below_zero']:.1%} |")
    best = min((n for n in report["models"] if n != "base_rate"), key=lambda n: report["models"][n]["log_loss"])
    lines += ["", f"## 확률 보정({LABELS.get(best, best)}, 홈 승 확률)", "", "| 예측 구간 | 경기 | 평균 예측 | 실제 [95%] | |", "|---|---|---|---|---|"]
    for item in report["models"][best]["calibration"]["bins"]:
        lines.append(f"| {item['range'][0]:.1f}–{item['range'][1]:.1f} | {item['games']} | {item['predicted']:.3f} | "
                     f"{item['observed']:.3f} [{item['observed_ci95'][0]:.3f}, {item['observed_ci95'][1]:.3f}] | {'✓' if item['within_ci'] else '✗'} |")
    names = {"regular": "정규 시즌", "postseason": "포스트시즌", "early": f"어느 팀이든 시즌 첫 {EARLY_GAMES}경기", "rest": "그 밖의 경기"}
    lines += ["", "## 구간별", "", "| 모델 | 구간 | 경기 | 로그 손실 |", "|---|---|---|---|"]
    for name, model in report["models"].items():
        for key, label in names.items():
            if key in model["segments"]:
                lines.append(f"| {LABELS.get(name, name)} | {label} | {model['segments'][key]['games']} | {model['segments'][key]['log_loss']:.4f} |")
    lines += ["", "| 모델 | 홈 승: 예측 | 실제 [95%] |", "|---|---|---|"]
    for name, model in report["models"].items():
        home = model["segments"]["home_win"]
        lines.append(f"| {LABELS.get(name, name)} | {home['predicted_rate']:.3f} | {home['actual_rate']:.3f} "
                     f"[{home['actual_ci95'][0]:.3f}, {home['actual_ci95'][1]:.3f}] |")
    lines += ["", "## 검증 상태", "", f"기준(축구와 같음): ECE ≤ {GATE['max_ece']}, 그리고 시장 마감 전 배당보다 로그 손실 차이의 "
              "95% 구간 상한이 0 미만. 하나라도 못 넘으면 확률을 보여 주지 않습니다(WITHHELD).", ""]
    for name, model in report["models"].items():
        lines.append(f"- {LABELS.get(name, name)}: **{model['gate']['status']}** — " + "; ".join(model["gate"]["reasons"]))
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="NBA 모델 백테스트·평가(5d-c)")
    parser.add_argument("--no-store", action="store_true", help="DB에 기록하지 않고 출력만")
    parser.add_argument("--markdown-dir", type=Path, help="보고서(evaluation_nba.md)를 저장할 폴더")
    args = parser.parse_args(argv)
    with connect(load_settings()) as conn:
        migrate(conn)
        rows, excluded = load_games(conn)
        backtest = run(rows, None if args.no_store else conn)
        backtest["excluded"] = excluded
        output = {"backtest": backtest}
        if not args.no_store:
            predictions, runs = load_test_predictions(conn)
            report = evaluate(predictions, runs)
            report["report_id"], report["newly_stored"] = store_report(conn, report)
            output["evaluation"] = {name: {key: model[key] for key in ("games", "log_loss", "brier", "accuracy", "gate")}
                                    for name, model in report["models"].items()}
            output["comparisons"] = report["comparisons"]
            if args.markdown_dir:
                args.markdown_dir.mkdir(parents=True, exist_ok=True)
                (args.markdown_dir / "evaluation_nba.md").write_text(render_markdown(report, backtest), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
