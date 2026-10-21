"""기준 모델 평가 보고서(로드맵 3c).

3b가 저장한 시험(TEST) 백테스트 예측을 읽어 다음을 계산한다.
- 차이의 불확실성: 같은 경기끼리 로그 손실 차이를 구하고, 주(학습 블록) 단위로 묶어 다시 뽑는
  블록 부트스트랩으로 95% 구간을 낸다. 같은 주 경기는 같은 학습 결과를 쓰므로 경기 단위로 뽑으면 구간이 좁게 나온다.
- 확률 보정: 예측 확률 구간별로 실제 비율과 비교(Wilson 95% 구간), 기대 보정 오차(ECE).
- 약한 곳: 승격 팀의 시즌 초반 경기, 시즌 초반·이후, 무승부.
- 검증 상태: 아래 기준을 모두 넘어야 VALIDATED. 하나라도 못 넘으면 NOT_VALIDATED와 사유(3d의 표시 여부를 정한다).

결과는 modeling.evaluation_reports에 저장하고(같은 입력·방법이면 다시 쓰지 않음) Markdown 보고서로 낼 수 있다.
실행: python -m pipelines.modeling.evaluation [--markdown docs/reports/epl_baseline_evaluation.md] [--no-store]
"""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from .backtest import OUTCOMES, brier, log_loss, market_probabilities

BOOTSTRAP_SAMPLES = 2000
BOOTSTRAP_SEED = 20261021        # 고정해서 같은 입력이면 같은 구간이 나오게 한다
CALIBRATION_EDGES = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0)
EARLY_MATCHES = 10               # "시즌 초반" = 팀의 그 시즌 첫 10경기
# 검증 기준(제안값). 바꾸면 METHOD가 바뀌어 새 보고서로 저장된다.
GATE = {"max_ece": 0.03, "must_beat": "market_pre"}
METHOD = {"bootstrap_samples": BOOTSTRAP_SAMPLES, "seed": BOOTSTRAP_SEED, "block": "fit_cutoff (ISO week)",
          "calibration_edges": CALIBRATION_EDGES, "early_matches": EARLY_MATCHES, "gate": GATE, "version": "3c.4"}
# 비교할 짝(왼쪽 − 오른쪽). 3e 개선 모델은 같은 설정의 기존 Poisson과 비교해 각 개선의 효과만 본다.
COMPARISONS = (("poisson", "base_rate"), ("dixon_coles", "poisson"), ("poisson_promoted", "poisson"),
               ("poisson_calibrated", "poisson"), ("poisson_promoted_calibrated", "poisson"),
               ("poisson_plus", "poisson"), ("poisson_plus_calibrated", "poisson"))
TIMING_LABELS = {"market_pre": "시장 마감 전 배당", "market_close": "시장 마감 배당"}


def load_test_predictions(conn: psycopg.Connection) -> tuple[dict, dict]:
    """모델 계열별 가장 최근 TEST 실행의 예측과 그 실행 정보. 예측에는 팀·시장 확률을 붙인다."""
    runs = {name: {"run_id": run_id, "model_version": version, "dataset_hash": data_hash}
            for name, run_id, version, data_hash in conn.execute("""
                SELECT DISTINCT ON (model_name) model_name, run_id, model_version, dataset_hash
                FROM modeling.backtest_runs WHERE purpose = 'TEST' ORDER BY model_name, run_id DESC""").fetchall()}
    cursor = conn.execute("""
        SELECT r.model_name, p.match_id, p.season, p.kickoff_utc, p.fit_cutoff, p.prob_home, p.prob_draw, p.prob_away,
               p.result, f.home_team, f.away_team, f.market_pre_home, f.market_pre_draw, f.market_pre_away,
               f.market_close_home, f.market_close_draw, f.market_close_away
        FROM modeling.backtest_predictions p
        JOIN modeling.backtest_runs r USING (run_id)
        JOIN gold.match_features_pit f USING (match_id)
        WHERE p.run_id = ANY(%s) AND p.status = 'PREDICTED'
        ORDER BY p.kickoff_utc, p.match_id""", ([run["run_id"] for run in runs.values()],))
    columns = [column.name for column in cursor.description]
    predictions = defaultdict(list)
    for values in cursor.fetchall():
        row = dict(zip(columns, values))
        row["probabilities"] = tuple(float(row.pop(f"prob_{side}")) for side in ("home", "draw", "away"))
        row["market_pre"] = market_probabilities(row, "pre")
        row["market_close"] = market_probabilities(row, "close")
        predictions[row.pop("model_name")].append(row)
    return dict(predictions), runs


def block_bootstrap(differences: dict, samples: int = BOOTSTRAP_SAMPLES, seed: int = BOOTSTRAP_SEED) -> dict:
    """블록(주)별 차이 목록 {블록: [경기별 차이]}에서 평균 차이와 95% 구간, 차이가 0보다 작을 비율을 구한다.

    음수 = 앞쪽(모델)이 더 좋음. 블록을 복원 추출하고, 뽑힌 블록의 모든 경기를 합쳐 평균을 낸다.
    """
    sums = np.array([sum(values) for values in differences.values()])
    counts = np.array([len(values) for values in differences.values()])
    rng = np.random.default_rng(seed)
    picks = rng.integers(0, len(sums), size=(samples, len(sums)))
    means = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    low, high = np.percentile(means, [2.5, 97.5])
    return {"matches": int(counts.sum()), "blocks": len(sums), "mean": round(float(sums.sum() / counts.sum()), 4),
            "ci95": [round(float(low), 4), round(float(high), 4)],
            "share_below_zero": round(float(np.mean(means < 0)), 3)}


def compare(items: list[dict], left, right) -> dict | None:
    """같은 경기에서 left − right 로그 손실 차이의 블록 부트스트랩. left·right는 경기 → 확률 함수."""
    differences = defaultdict(list)
    for item in items:
        a, b = left(item), right(item)
        if a is not None and b is not None:
            differences[item["fit_cutoff"]].append(log_loss(a, item["result"]) - log_loss(b, item["result"]))
    return block_bootstrap(differences) if differences else None


def wilson(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    """비율의 Wilson 95% 구간. 표본이 작은 구간에서도 0·1 밖으로 나가지 않는다."""
    if total == 0:
        return 0.0, 1.0
    rate = successes / total
    centre = (rate + z * z / (2 * total)) / (1 + z * z / total)
    half = z * math.sqrt(rate * (1 - rate) / total + z * z / (4 * total * total)) / (1 + z * z / total)
    return max(0.0, centre - half), min(1.0, centre + half)   # 부동소수 오차로 0·1을 넘지 않게


def calibration(items: list[dict], probabilities=lambda item: item["probabilities"]) -> dict:
    """결과(홈 승·무·원정 승)마다 예측 확률 구간별 평균 예측과 실제 비율, ECE."""
    report = {}
    for index, outcome in enumerate(OUTCOMES):
        bins, weighted_gap, total = [], 0.0, 0
        for low, high in zip(CALIBRATION_EDGES, CALIBRATION_EDGES[1:]):
            members = [item for item in items if probabilities(item) is not None
                       and low <= probabilities(item)[index] < high + (1e-9 if high == 1.0 else 0)]
            if not members:
                continue
            predicted = sum(probabilities(item)[index] for item in members) / len(members)
            hits = sum(item["result"] == outcome for item in members)
            interval = wilson(hits, len(members))
            bins.append({"range": [low, high], "matches": len(members), "predicted": round(predicted, 3),
                         "observed": round(hits / len(members), 3),
                         "observed_ci95": [round(interval[0], 3), round(interval[1], 3)],
                         "within_ci": interval[0] <= predicted <= interval[1]})
            weighted_gap += len(members) * abs(hits / len(members) - predicted)
            total += len(members)
        report[outcome] = {"ece": round(weighted_gap / total, 4) if total else None, "bins": bins}
    return report


def teams_by_season(conn: psycopg.Connection) -> dict:
    """데이터셋 전체(학습 전용 시즌 포함)의 시즌별 참가 팀. 승격 팀 = 직전 시즌에 없던 팀."""
    teams = defaultdict(set)
    for season, home, away in conn.execute("SELECT season, home_team, away_team FROM gold.match_features_pit").fetchall():
        teams[season] |= {home, away}
    return dict(teams)


def segments(items: list[dict], all_teams_by_season: dict) -> dict:
    """약한 곳 찾기: 승격 팀 시즌 초반, 시즌 초반·이후, 무승부 예측 수준.

    승격 팀은 직전 시즌 데이터셋에 없던 팀이다. 데이터의 첫 시즌은 직전 시즌이 없어 판단하지 않는다.
    """
    seasons = sorted(all_teams_by_season)
    promoted = {season: all_teams_by_season[season] - all_teams_by_season[previous]
                for previous, season in zip(seasons, seasons[1:])}
    played = defaultdict(int)   # (시즌, 팀) → 지금까지 치른 경기 수(시간순)
    groups = defaultdict(list)
    promoted_win = {"predicted": 0.0, "actual": 0, "matches": 0}
    for item in sorted(items, key=lambda item: item["kickoff_utc"]):
        season, home, away = item["season"], item["home_team"], item["away_team"]
        early = played[(season, home)] < EARLY_MATCHES or played[(season, away)] < EARLY_MATCHES
        groups["season_early" if early else "season_rest"].append(item)
        for side, team in ((0, home), (2, away)):
            if team in promoted.get(season, set()) and played[(season, team)] < EARLY_MATCHES:
                groups["promoted_early"].append(item)
                promoted_win["predicted"] += item["probabilities"][side]
                promoted_win["actual"] += item["result"] == OUTCOMES[side]
                promoted_win["matches"] += 1
        played[(season, home)] += 1
        played[(season, away)] += 1
    report = {}
    for name, members in groups.items():
        unique = list({item["match_id"]: item for item in members}.values())
        report[name] = {"matches": len(unique),
                        "model_log_loss": round(sum(log_loss(i["probabilities"], i["result"]) for i in unique) / len(unique), 4),
                        "vs_market_close": compare(unique, lambda i: i["probabilities"], lambda i: i["market_close"])}
    if promoted_win["matches"]:
        report["promoted_early"]["promoted_team_win"] = rate_check(
            promoted_win["predicted"], promoted_win["actual"], promoted_win["matches"])
    draws = sum(item["result"] == "DRAW" for item in items)
    report["draws"] = {**rate_check(sum(i["probabilities"][1] for i in items), draws, len(items)),
                       "market_close_rate": round(sum(i["market_close"][1] for i in items if i["market_close"])
                                                  / max(1, sum(1 for i in items if i["market_close"])), 3)}
    return report


def rate_check(predicted_sum: float, actual: int, matches: int) -> dict:
    """예측 확률의 평균과 실제 비율(Wilson 95% 구간)을 나란히."""
    low, high = wilson(actual, matches)
    return {"matches": matches, "predicted_rate": round(predicted_sum / matches, 3),
            "actual_rate": round(actual / matches, 3), "actual_ci95": [round(low, 3), round(high, 3)]}


def sharpness(items: list[dict]) -> dict:
    """확신 정도: 세 확률 중 가장 큰 값의 평균. 시장 마감과 같은 경기끼리 비교한다.

    모델 값이 시장보다 작으면 확률을 가운데(1/3)로 눌러 내고 있다는 뜻이다(축소가 너무 셀 때 나타남).
    """
    pairs = [(max(item["probabilities"]), max(item["market_close"])) for item in items if item["market_close"]]
    return {"matches": len(pairs), "model_mean_max": round(sum(a for a, _ in pairs) / len(pairs), 4),
            "market_close_mean_max": round(sum(b for _, b in pairs) / len(pairs), 4)} if pairs else {"matches": 0}


def gate(model_report: dict) -> dict:
    """검증 기준. 모두 통과해야 VALIDATED. 사유는 사람이 읽는 문장으로 남긴다."""
    reasons = []
    worst_ece = max(model_report["calibration"][outcome]["ece"] for outcome in OUTCOMES)
    if worst_ece > GATE["max_ece"]:
        reasons.append(f"보정 오차(가장 나쁜 결과의 ECE) {worst_ece:.3f} > 기준 {GATE['max_ece']}")
    versus = model_report["vs_" + GATE["must_beat"]]
    if versus is None or versus["ci95"][1] >= 0:
        reasons.append(f"{TIMING_LABELS[GATE['must_beat']]}보다 확실히 낫지 않음(로그 손실 차이 95% 구간 상한 "
                       f"{'없음' if versus is None else versus['ci95'][1]} ≥ 0)")
    return {"status": "NOT_VALIDATED" if reasons else "VALIDATED", "reasons": reasons}


def evaluate(predictions: dict, runs: dict, season_teams: dict) -> dict:
    """모델 계열별 평가와 계열 간 비교. season_teams는 teams_by_season()의 결과."""
    report = {"method": METHOD, "runs": runs, "models": {}, "comparisons": {}}
    by_match = {name: {item["match_id"]: item for item in items} for name, items in predictions.items()}
    for name, items in predictions.items():
        model_report = {
            "matches": len(items),
            "log_loss": round(sum(log_loss(i["probabilities"], i["result"]) for i in items) / len(items), 4),
            "brier": round(sum(brier(i["probabilities"], i["result"]) for i in items) / len(items), 4),
            "vs_market_pre": compare(items, lambda i: i["probabilities"], lambda i: i["market_pre"]),
            "vs_market_close": compare(items, lambda i: i["probabilities"], lambda i: i["market_close"]),
            "calibration": calibration(items),
            "sharpness": sharpness(items),
            "segments": segments(items, season_teams)}
        model_report["gate"] = gate(model_report)
        report["models"][name] = model_report
    reference = next(iter(predictions.values()))
    report["market_close_calibration"] = calibration(reference, lambda item: item["market_close"])
    for left, right in COMPARISONS:
        if left in by_match and right in by_match:
            report["comparisons"][f"{left}_vs_{right}"] = compare(
                predictions[left], lambda i: i["probabilities"],
                lambda i, other=by_match[right]: other[i["match_id"]]["probabilities"] if i["match_id"] in other else None)
    return report


def store_report(conn: psycopg.Connection, report: dict) -> tuple[int, bool]:
    """같은 실행·방법의 보고서는 한 번만 저장한다. (report_id, 새로 저장했는지)."""
    run_ids = sorted(run["run_id"] for run in report["runs"].values())
    method_hash = hashlib.sha256(json.dumps(METHOD, sort_keys=True, default=str).encode()).hexdigest()
    with conn.transaction():
        inserted = conn.execute("""
            INSERT INTO modeling.evaluation_reports (run_ids, method_hash, method, results, created_at)
            VALUES (%s::bigint[], %s, %s, %s, %s) ON CONFLICT (run_ids, method_hash) DO NOTHING RETURNING report_id""",
            (run_ids, method_hash, Jsonb(METHOD), Jsonb(json.loads(json.dumps(report, default=str))),
             datetime.now(timezone.utc))).fetchone()
        if inserted:
            return inserted[0], True
        return conn.execute("SELECT report_id FROM modeling.evaluation_reports WHERE run_ids = %s::bigint[] AND method_hash = %s",
                            (run_ids, method_hash)).fetchone()[0], False


LABELS = {"base_rate": "단순 비율", "poisson": "Poisson", "dixon_coles": "Dixon-Coles",
          "poisson_promoted": "Poisson+승격 효과", "poisson_calibrated": "Poisson+보정",
          "poisson_promoted_calibrated": "Poisson+승격 효과+보정", "poisson_plus": "Poisson+경기 기록",
          "poisson_plus_calibrated": "Poisson+경기 기록+보정", "market_blend": "시장 결합"}
OUTCOME_LABELS = {"HOME": "홈 승", "DRAW": "무승부", "AWAY": "원정 승"}


def interval(result: dict | None) -> str:
    if result is None:
        return "—"
    return f"{result['mean']:+.4f} [{result['ci95'][0]:+.4f}, {result['ci95'][1]:+.4f}]"


def render_markdown(report: dict) -> str:
    """평가 결과를 사람이 읽는 보고서로. 숫자는 report에서만 가져온다."""
    lines = ["# EPL 기준 모델 평가 보고서", "",
             "`python -m pipelines.modeling.evaluation`이 만든 문서입니다. 직접 고치지 말고 다시 생성하세요.", "",
             "- 대상: 3b 시험(TEST) 백테스트의 끝난 경기. 과거 재구성 가정(결과는 시작 3시간 뒤 공개) 위의 결과입니다.",
             f"- 불확실성: 주 단위 블록 부트스트랩 {BOOTSTRAP_SAMPLES}회(seed {BOOTSTRAP_SEED}). "
             "차이는 왼쪽 − 오른쪽 로그 손실이며, 음수면 왼쪽이 더 정확합니다.",
             "- 실행: " + ", ".join(f"{LABELS.get(name, name)} run {run['run_id']} (`{run['model_version']}`)"
                                   for name, run in sorted(report["runs"].items())), "",
             "## 요약", "",
             "| 모델 | 경기 | 로그 손실 | Brier | 시장 마감 전 대비 [95%] | 시장 마감 대비 [95%] | 검증 상태 |",
             "|---|---|---|---|---|---|---|"]
    for name, model in report["models"].items():
        lines.append(f"| {LABELS.get(name, name)} | {model['matches']} | {model['log_loss']:.4f} | {model['brier']:.4f} | "
                     f"{interval(model['vs_market_pre'])} | {interval(model['vs_market_close'])} | {model['gate']['status']} |")
    lines += ["", "## 모델끼리 비교", "", "| 비교 | 경기 | 주 | 차이 [95%] | 왼쪽이 나은 비율 |", "|---|---|---|---|---|"]
    for name, result in report["comparisons"].items():
        left, right = name.split("_vs_")
        lines.append(f"| {LABELS.get(left, left)} − {LABELS.get(right, right)} | {result['matches']} | {result['blocks']} | "
                     f"{interval(result)} | {result['share_below_zero']:.1%} |")
    lines += ["", "## 확률 보정", "", "예측 확률 구간별 실제 비율입니다. 실제 비율의 95% 구간 밖이면 ✗.", ""]
    calibrated = {**{LABELS.get(name, name): model["calibration"] for name, model in report["models"].items()},
                  "시장 마감(참고)": report["market_close_calibration"]}
    lines += ["| 대상 | " + " | ".join(f"{OUTCOME_LABELS[o]} ECE" for o in OUTCOMES) + " |", "|---|---|---|---|"]
    for label, result in calibrated.items():
        lines.append(f"| {label} | " + " | ".join(f"{result[o]['ece']:.4f}" for o in OUTCOMES) + " |")
    for name in ("market_blend", "poisson_calibrated", "dixon_coles", "poisson"):
        if name not in report["models"]:
            continue
        lines += ["", f"### {LABELS[name]} 구간별", "", "| 결과 | 예측 구간 | 경기 | 평균 예측 | 실제 [95%] | |", "|---|---|---|---|---|---|"]
        for outcome in OUTCOMES:
            for item in report["models"][name]["calibration"][outcome]["bins"]:
                lines.append(f"| {OUTCOME_LABELS[outcome]} | {item['range'][0]:.1f}–{item['range'][1]:.1f} | {item['matches']} | "
                             f"{item['predicted']:.3f} | {item['observed']:.3f} [{item['observed_ci95'][0]:.3f}, "
                             f"{item['observed_ci95'][1]:.3f}] | {'✓' if item['within_ci'] else '✗'} |")
        break
    lines += ["", "## 약한 곳", "", "| 모델 | 구간 | 경기 | 로그 손실 | 시장 마감 대비 [95%] |", "|---|---|---|---|---|"]
    names = {"promoted_early": f"승격 팀 시즌 첫 {EARLY_MATCHES}경기", "season_early": f"어느 팀이든 시즌 첫 {EARLY_MATCHES}경기",
             "season_rest": "그 밖의 경기"}
    for name, model in report["models"].items():
        for key, label in names.items():
            if key in model["segments"]:
                segment = model["segments"][key]
                lines.append(f"| {LABELS.get(name, name)} | {label} | {segment['matches']} | {segment['model_log_loss']:.4f} | "
                             f"{interval(segment['vs_market_close'])} |")
    def rate(check: dict | None) -> tuple[str, str]:
        if not check:
            return "—", "—"
        return (f"{check['predicted_rate']:.3f}",
                f"{check['actual_rate']:.3f} [{check['actual_ci95'][0]:.3f}, {check['actual_ci95'][1]:.3f}] ({check['matches']})")

    lines += ["", "| 모델 | 승격 팀 초반 승리: 예측 | 실제 [95%] (경기) | 무승부: 예측 | 실제 [95%] (경기) | 시장 마감 무승부 |",
              "|---|---|---|---|---|---|"]
    for name, model in report["models"].items():
        promoted = rate(model["segments"].get("promoted_early", {}).get("promoted_team_win"))
        draws = model["segments"]["draws"]
        lines.append(f"| {LABELS.get(name, name)} | {promoted[0]} | {promoted[1]} | {rate(draws)[0]} | {rate(draws)[1]} | "
                     f"{draws['market_close_rate']:.3f} |")
    lines += ["", "확신 정도(세 확률 중 가장 큰 값의 평균, 같은 경기). 모델이 시장보다 작으면 확률을 가운데로 눌러 내는 것입니다.", "",
              "| 모델 | 모델 | 시장 마감 |", "|---|---|---|"]
    for name, model in report["models"].items():
        if model["sharpness"]["matches"]:
            lines.append(f"| {LABELS.get(name, name)} | {model['sharpness']['model_mean_max']:.4f} | "
                         f"{model['sharpness']['market_close_mean_max']:.4f} |")
    lines += ["", "## 검증 상태", "", f"기준(제안값): 결과별 ECE ≤ {GATE['max_ece']}, 그리고 시장 마감 전 배당보다 "
              "로그 손실 차이의 95% 구간 상한이 0 미만. 하나라도 못 넘으면 확률을 보여 주지 않습니다(WITHHELD).", ""]
    for name, model in report["models"].items():
        reasons = "; ".join(model["gate"]["reasons"]) or "모든 기준 통과"
        lines.append(f"- {LABELS.get(name, name)}: **{model['gate']['status']}** — {reasons}")
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="기준 모델 평가 보고서(3c)")
    parser.add_argument("--markdown", type=Path, help="Markdown 보고서를 저장할 경로")
    parser.add_argument("--no-store", action="store_true", help="DB에 기록하지 않는다")
    args = parser.parse_args(argv)
    with connect(load_settings()) as conn:
        migrate(conn)
        predictions, runs = load_test_predictions(conn)
        if not predictions:
            raise SystemExit("시험(TEST) 백테스트가 없습니다. 먼저 python -m pipelines.modeling.backtest를 실행하세요.")
        report = evaluate(predictions, runs, teams_by_season(conn))
        if not args.no_store:
            report["report_id"], report["newly_stored"] = store_report(conn, report)
    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(render_markdown(report), encoding="utf-8")
    summary = {name: {key: model[key] for key in ("matches", "log_loss", "vs_market_pre", "vs_market_close", "gate")}
               for name, model in report["models"].items()}
    print(json.dumps({"report_id": report.get("report_id"), "newly_stored": report.get("newly_stored"),
                      "models": summary, "comparisons": report["comparisons"]}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
