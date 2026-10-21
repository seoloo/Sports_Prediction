"""경기 전 모델 예측 기록, 경기 후 채점, 주간 보고서(로드맵 3d). PROSPECTIVE.

- 예측은 경기 전 스냅샷(2e)을 찍는 순간에만 만든다(snapshots.run이 record_for_snapshot을 부른다).
  놓친 시점을 나중에 채우지 않고, 새 모델을 과거 시점에 소급하지 않는다.
- 학습 자료는 스냅샷 기준 시각(as_of)에 실제로 받아 둔 결과뿐이다(발표·수신 시각 모두 as_of 이전).
  과거 재구성의 "시작 3시간 뒤 공개" 가정을 쓰지 않는다.
- 활성 모델: 3b 시험 실행 중 검증 시즌 로그 손실이 가장 낮은 계열(시험 성적으로 고르지 않는다).
  3c 평가의 검증 기준을 통과하지 못했으면 확률은 평가용으로만 저장하고 표시 상태는 WITHHELD다.
- 채점은 매번 그 시각에 알려진 결과로 다시 계산한다(결과가 정정되면 다음 보고서에 반영).
실행: python -m pipelines.modeling.prospective report [--week 2026-10-19] [--markdown 경로] [--no-store]
"""
import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from ..snapshots import KNOWN_FIXTURES_SQL
from .backtest import OUTCOMES, brier, log_loss
from ..leagues import MODELED
from .models import TrainingMatch, model_from_params, promoted_by_season
MIN_TRAINING_MATCHES = 100
MIN_TEAM_HISTORY = 5          # 한 팀의 학습 경기가 이보다 적으면(승격 팀 시즌 초 등) 사유를 하나 더 붙인다
MODE = "PROSPECTIVE"
FIXTURE_COLUMNS = ("match_id", "competition_code", "season", "home", "away", "kickoff_utc", "status", "ft_home",
                   "ft_away", "fixture_source", "home_team_id", "away_team_id")


def known_fixtures(conn: psycopg.Connection, as_of: datetime) -> list[dict]:
    """as_of 시점에 알려진 경기 정보(스냅샷과 같은 규칙: football-data.org 우선, 없으면 openfootball)."""
    return [dict(zip(FIXTURE_COLUMNS, row)) for row in conn.execute(KNOWN_FIXTURES_SQL, {"as_of": as_of}).fetchall()]


def active_model(conn: psycopg.Connection, competition: str = "en.1") -> dict | None:
    """3b 시험 실행 중 검증 로그 손실이 가장 낮은 모델과, 그 실행을 평가한 가장 최근 3c 보고서의 검증 상태.

    시장 결합(market_blend, 3g)은 뺀다. 경기 전 시점에 쓸 시장 배당(다가올 경기의 평균 배당)이 없기 때문이다.
    """
    candidates = conn.execute("""
        SELECT DISTINCT ON (t.model_name) t.model_name, t.run_id, t.model_version, t.params,
               (v.summary->'overall'->>'log_loss')::float AS validation_log_loss
        FROM modeling.backtest_runs t
        JOIN modeling.backtest_runs v ON v.purpose = 'VALIDATION' AND v.model_version = t.model_version
                                      AND v.dataset_hash = t.dataset_hash
                                      AND v.competition_code = t.competition_code
        WHERE t.purpose = 'TEST' AND t.model_name NOT LIKE 'market_blend%%' AND t.competition_code = %s
        ORDER BY t.model_name, t.run_id DESC""", (competition,)).fetchall()
    if not candidates:
        return None
    name, run_id, version, params, validation_loss = min(candidates, key=lambda row: row[4])
    report = conn.execute("""SELECT report_id, results->'models'->%s->'gate' FROM modeling.evaluation_reports
                             WHERE %s = ANY(run_ids) AND competition_code = %s ORDER BY report_id DESC LIMIT 1""",
                          (name, run_id, competition)).fetchone()
    gate = report[1] if report and report[1] else None
    return {"name": name, "run_id": run_id, "model_version": version, "params": params,
            "validation_log_loss": validation_loss, "report_id": report[0] if report else None,
            "gate_status": gate["status"] if gate else "NO_REPORT",
            "gate_reasons": gate["reasons"] if gate else ["3c 평가 보고서 없음"]}


def build_model(active: dict):
    """활성 모델 정보로 같은 설정의 모델을 만든다. 코드가 바뀌어 버전이 다르면 None(평가받지 않은 코드)."""
    model = model_from_params(active["name"], active["params"])
    return model if model.version == active["model_version"] else None


class Predictor:
    """스냅샷 실행 한 번(같은 as_of)에서 한 번만 학습하고 여러 경기를 예측한다."""

    def __init__(self, conn: psycopg.Connection, as_of: datetime, competition: str = "en.1"):
        self.as_of, self.competition = as_of, competition
        self.active = active_model(conn, competition)
        self.model, self.reasons = None, []
        # 5c: 리그마다 그 리그 경기로만 학습하고, 그 리그에서 평가받은 모델을 쓴다.
        fixtures = [row for row in known_fixtures(conn, as_of) if row["competition_code"] == competition]
        # 시즌별 승격 팀(3e). 시즌 참가 팀은 일정이 나올 때 이미 알려진다.
        self.promoted = promoted_by_season((row["season"], row["home"], row["away"]) for row in fixtures)
        finished = sorted((row for row in fixtures
                           if row["status"] == "FINISHED" and row["kickoff_utc"] is not None
                           and row["kickoff_utc"] < as_of and row["ft_home"] is not None),
                          key=lambda row: (row["kickoff_utc"], row["match_id"]))
        # 경기 기록(3g): as_of에 실제로 받아 둔 것만. 쉰 날 수는 각 팀의 직전 끝난 경기 기준(데이터셋과 같은 정의).
        stats = {match_id: (home_sot, away_sot, reds) for match_id, home_sot, away_sot, reds in conn.execute(
            """SELECT match_id, home_shots_on_target, away_shots_on_target, home_red_cards + away_red_cards
               FROM silver.match_stats_known_at(%s) WHERE source_id = 'football-data-co-uk'""", (as_of,)).fetchall()}
        self.last_played, self.training = {}, []
        for row in finished:
            rests = [self.rest_days(team, row["kickoff_utc"]) for team in (row["home"], row["away"])]
            home_sot, away_sot, reds = stats.get(row["match_id"], (None, None, 0))
            self.training.append(TrainingMatch(
                row["kickoff_utc"], row["home"], row["away"], row["ft_home"], row["ft_away"],
                self.is_promoted(row["home"], row["season"]), self.is_promoted(row["away"], row["season"]),
                home_sot, away_sot, reds, *rests))
            self.last_played[row["home"]] = self.last_played[row["away"]] = row["kickoff_utc"]
        self.training_hash = hashlib.sha256(repr([(r[0].isoformat(), *r[1:]) for r in self.training]).encode()).hexdigest()
        self.history = Counter(team for match in self.training for team in (match[1], match[2]))
        if self.active is None:
            self.reasons.append("NO_ACTIVE_MODEL")
        elif (model := build_model(self.active)) is None:
            self.reasons.append("MODEL_CODE_CHANGED")   # 지금 코드는 3c에서 평가받은 코드가 아니다
        elif len(self.training) < MIN_TRAINING_MATCHES:
            self.reasons.append(f"INSUFFICIENT_TRAINING_DATA(<{MIN_TRAINING_MATCHES})")
        else:
            self.model = model.fit(self.training, as_of)

    def is_promoted(self, team: str, season: str | None) -> bool:
        return team in self.promoted.get(season, set())

    def rest_days(self, team: str, kickoff: datetime | None) -> float | None:
        """팀의 직전 끝난 경기(학습 자료에 이미 넣은 것) 뒤 쉰 날 수. 모르면 None."""
        last = self.last_played.get(team)
        return None if last is None or kickoff is None else (kickoff - last).total_seconds() / 86400

    def predict(self, home: str, away: str, season: str | None = None, kickoff: datetime | None = None,
                competition: str | None = None) -> dict:
        """저장할 예측 한 행. 확률은 모델이 있을 때만, 표시 상태는 검증 기준과 자료 부족 여부로 정한다."""
        if competition is not None and competition not in MODELED:
            reasons = ["LEAGUE_NOT_MODELED"]   # 이 리그로 학습·평가한 모델이 없다. 다른 리그 모델로 대신 예측하지 않는다
            probabilities = None
        else:
            reasons = list(self.reasons)
            probabilities = (self.model.predict(home, away, self.is_promoted(home, season), self.is_promoted(away, season),
                                                self.rest_days(home, kickoff), self.rest_days(away, kickoff))
                             if self.model else None)
        modeled = reasons != ["LEAGUE_NOT_MODELED"]   # 리그 모델이 없으면 다른 사유는 의미가 없다
        if modeled and self.active and self.active["gate_status"] != "VALIDATED":
            reasons.append("MODEL_NOT_VALIDATED")
        if modeled and min(self.history[home], self.history[away]) < MIN_TEAM_HISTORY:
            reasons.append(f"LOW_TEAM_HISTORY(<{MIN_TEAM_HISTORY})")
        active = self.active or {}
        return {"model_name": active.get("name", "unavailable"), "model_version": active.get("model_version", "unavailable"),
                "backtest_run_id": active.get("run_id"), "evaluation_report_id": active.get("report_id"),
                "gate_status": active.get("gate_status", "UNKNOWN"), "training_matches": len(self.training),
                "last_training_kickoff": self.training[-1][0] if self.training else None,
                "training_hash": self.training_hash, "home_history": self.history[home], "away_history": self.history[away],
                "probabilities": probabilities, "display_status": "WITHHELD" if reasons else "READY",
                "withheld_reasons": reasons}


def insert_prediction(conn: psycopg.Connection, match_id: int, offset_label: str, as_of: datetime, row: dict) -> None:
    probabilities = row["probabilities"] or (None, None, None)
    conn.execute("""
        INSERT INTO snapshots.prematch_predictions
            (match_id, offset_label, as_of, model_name, model_version, backtest_run_id, evaluation_report_id, gate_status,
             training_matches, last_training_kickoff, training_hash, home_history, away_history,
             prob_home, prob_draw, prob_away, display_status, withheld_reasons, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (match_id, offset_label, as_of, row["model_name"], row["model_version"], row["backtest_run_id"],
         row["evaluation_report_id"], row["gate_status"], row["training_matches"], row["last_training_kickoff"],
         row["training_hash"], row["home_history"], row["away_history"],
         *[None if p is None else round(p, 8) for p in probabilities], row["display_status"], row["withheld_reasons"], as_of))


def record_for_snapshot(conn: psycopg.Connection, match_id: int, offset_label: str, home: str, away: str,
                        as_of: datetime, cache: dict, season: str | None = None, kickoff: datetime | None = None,
                        competition: str | None = None) -> str:
    """방금 찍은 스냅샷에 모델 예측을 붙인다. 예측이 실패해도 스냅샷은 지키고 실패 사유를 WITHHELD로 남긴다."""
    try:
        with conn.transaction():   # 저장점: 실패하면 이 예측만 취소
            key = (as_of, competition or "en.1")
            if key not in cache:
                cache[key] = Predictor(conn, as_of, competition or "en.1")
            row = cache[key].predict(home, away, season, kickoff, competition)
            insert_prediction(conn, match_id, offset_label, as_of, row)
            return row["display_status"]
    except Exception as exc:   # noqa: BLE001 (예측 실패가 입력 스냅샷까지 잃게 하지 않는다)
        failed = {"model_name": "unavailable", "model_version": "unavailable", "backtest_run_id": None,
                  "evaluation_report_id": None, "gate_status": "UNKNOWN", "training_matches": 0,
                  "last_training_kickoff": None, "training_hash": None, "home_history": 0, "away_history": 0,
                  "probabilities": None, "display_status": "WITHHELD",
                  "withheld_reasons": [f"PREDICTION_FAILED:{type(exc).__name__}"]}
        with conn.transaction():
            insert_prediction(conn, match_id, offset_label, as_of, failed)
        return "FAILED"


def market_probabilities(inputs: dict) -> tuple | None:
    fair, _ = market_fair(inputs.get("odds", []))
    return None if fair is None else tuple(fair[outcome] for outcome in OUTCOMES)


def scored_predictions(conn: psycopg.Connection, as_of: datetime) -> list[dict]:
    """확률이 있는 경기 전 예측과, as_of에 알려진 결과(끝난 경기만)."""
    results = {row["match_id"]: row for row in known_fixtures(conn, as_of)
               if row["status"] == "FINISHED" and row["ft_home"] is not None}
    rows = conn.execute("""
        SELECT p.match_id, p.offset_label, p.as_of, p.model_version, p.display_status, p.prob_home, p.prob_draw,
               p.prob_away, i.kickoff_utc, i.inputs
        FROM snapshots.prematch_predictions p
        JOIN snapshots.prematch_inputs i USING (match_id, offset_label)
        WHERE p.prob_home IS NOT NULL ORDER BY i.kickoff_utc, p.match_id, p.offset_label""").fetchall()
    scored = []
    for match_id, label, snapshot_at, version, display, home, draw, away, kickoff, inputs in rows:
        result = results.get(match_id)
        if result is None:
            continue
        outcome = "HOME" if result["ft_home"] > result["ft_away"] else "DRAW" if result["ft_home"] == result["ft_away"] else "AWAY"
        scored.append({"match_id": match_id, "offset_label": label, "as_of": snapshot_at, "kickoff_utc": kickoff,
                       "model_version": version, "display_status": display, "result": outcome,
                       "probabilities": (float(home), float(draw), float(away)), "market": market_probabilities(inputs)})
    return scored


def metrics(items: list[dict]) -> dict:
    result = {"matches": len(items)}
    if items:
        result["log_loss"] = round(sum(log_loss(i["probabilities"], i["result"]) for i in items) / len(items), 4)
        result["brier"] = round(sum(brier(i["probabilities"], i["result"]) for i in items) / len(items), 4)
        with_market = [i for i in items if i["market"]]
        if with_market:
            result["vs_market_pre"] = {
                "matches": len(with_market),
                "model_log_loss": round(sum(log_loss(i["probabilities"], i["result"]) for i in with_market) / len(with_market), 4),
                "market_log_loss": round(sum(log_loss(i["market"], i["result"]) for i in with_market) / len(with_market), 4)}
    return result


def weekly_summary(conn: psycopg.Connection, week_start: date, as_of: datetime) -> dict:
    """week_start(월요일)부터 7일 동안 시작한 경기의 스냅샷·예측·채점, 그리고 처음부터 누적 성적."""
    start = datetime.combine(week_start, time(0), tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    snapshots = conn.execute("""
        SELECT i.offset_label, i.status, p.display_status, p.withheld_reasons, p.prob_home IS NOT NULL
        FROM snapshots.prematch_inputs i
        LEFT JOIN snapshots.prematch_predictions p USING (match_id, offset_label)
        WHERE i.kickoff_utc >= %s AND i.kickoff_utc < %s AND i.recorded_at <= %s""", (start, end, as_of)).fetchall()
    by_offset = defaultdict(Counter)
    reasons = Counter()
    for label, status, display, withheld, has_probabilities in snapshots:
        by_offset[label][status] += 1
        if status == "TAKEN":
            by_offset[label]["PREDICTION_" + (display or "MISSING")] += 1
            by_offset[label]["with_probabilities"] += int(bool(has_probabilities))
            reasons.update(withheld or [])
    scored = scored_predictions(conn, as_of)
    this_week = [item for item in scored if start <= item["kickoff_utc"] < end]
    active = active_model(conn)
    leagues = {code: active_model(conn, code) for code in MODELED}   # 5c: 리그별 활성 모델
    return {"week_start": week_start.isoformat(), "as_of": as_of.isoformat(), "mode": MODE,
            "active_model": active and {key: active[key] for key in ("name", "model_version", "run_id", "report_id",
                                                                     "gate_status", "gate_reasons")},
            "snapshots": {label: dict(counts) for label, counts in sorted(by_offset.items())},
            "withheld_reasons": dict(reasons),
            "week": {label: metrics([i for i in this_week if i["offset_label"] == label]) for label in ("T-24H", "T-3H", "T-1H")},
            "cumulative": {label: metrics([i for i in scored if i["offset_label"] == label]) for label in ("T-24H", "T-3H", "T-1H")},
            "league_models": {code: model and {"name": model["name"], "gate_status": model["gate_status"]}
                              for code, model in leagues.items()}}


def store_weekly(conn: psycopg.Connection, summary: dict) -> tuple[int, bool]:
    """주간 보고서 저장. 같은 주·같은 내용이면 다시 쓰지 않고, 결과 정정 등으로 내용이 바뀌면 새 버전을 쌓는다."""
    content = {key: value for key, value in summary.items() if key != "as_of"}
    content_hash = hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()
    with conn.transaction():
        inserted = conn.execute("""
            INSERT INTO modeling.prospective_reports (week_start, as_of, content_hash, results, created_at)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (week_start, content_hash) DO NOTHING RETURNING report_id""",
            (summary["week_start"], summary["as_of"], content_hash, Jsonb(summary), datetime.now(timezone.utc))).fetchone()
        if inserted:
            return inserted[0], True
        return conn.execute("SELECT report_id FROM modeling.prospective_reports WHERE week_start = %s AND content_hash = %s",
                            (summary["week_start"], content_hash)).fetchone()[0], False


def previous_week_start(now: datetime) -> date:
    """지난주 월요일(UTC). 화요일 아침에 돌면 주말·월요일 경기까지 끝난 주를 본다."""
    today = now.astimezone(timezone.utc).date()
    return today - timedelta(days=today.weekday() + 7)


def run_weekly(*, now: datetime | None = None, week_start: date | None = None, settings=None, store: bool = True) -> dict:
    """주간 보고서를 만들고(기본: 지난주) 저장한다. Airflow DAG와 명령줄이 함께 쓴다."""
    now = now or datetime.now(timezone.utc)
    with connect(settings or load_settings()) as conn:
        migrate(conn)
        summary = weekly_summary(conn, week_start or previous_week_start(now), now)
        if store:
            summary["report_id"], summary["newly_stored"] = store_weekly(conn, summary)
    return summary


LABEL = {"T-24H": "24시간 전", "T-3H": "3시간 전", "T-1H": "1시간 전"}


def render_weekly_markdown(summary: dict) -> str:
    """주간 보고서를 사람이 읽는 문서로."""
    active = summary["active_model"]
    lines = [f"# 주간 예측 보고서: {summary['week_start']} 주", "",
             f"- 기준 시각: {summary['as_of']} (UTC). 모드: {summary['mode']}(경기 전에 실제로 저장한 예측).",
             "- 활성 모델: " + (f"`{active['model_version']}` (백테스트 run {active['run_id']}, 평가 보고서 "
                               f"{active['report_id']}, 검증 상태 **{active['gate_status']}**)" if active else "없음"), ""]
    if active and active["gate_status"] != "VALIDATED":
    lines += ["## 스냅샷과 예측", "", "| 시점 | 찍음 | 놓침 | 확률 있음 | 표시 가능(READY) | 보류(WITHHELD) |", "|---|---|---|---|---|---|"]
    for label in ("T-24H", "T-3H", "T-1H"):
        counts = summary["snapshots"].get(label, {})
        lines.append(f"| {LABEL[label]} | {counts.get('TAKEN', 0)} | {counts.get('MISSED', 0)} | {counts.get('with_probabilities', 0)} | "
                     f"{counts.get('PREDICTION_READY', 0)} | {counts.get('PREDICTION_WITHHELD', 0)} |")
    if summary["withheld_reasons"]:
        lines += ["", "보류 사유: " + ", ".join(f"`{reason}` {count}" for reason, count in sorted(summary["withheld_reasons"].items()))]
    for title, key in (("이번 주 채점", "week"), ("누적 채점", "cumulative")):
        lines += ["", f"## {title}", "", "| 시점 | 경기 | 로그 손실 | Brier | 시장 마감 전 비교(경기, 모델, 시장) |", "|---|---|---|---|---|"]
        for label in ("T-24H", "T-3H", "T-1H"):
            result = summary[key][label]
            if not result["matches"]:
                lines.append(f"| {LABEL[label]} | 0 | — | — | — |")
                continue
            market = result.get("vs_market_pre")
            versus = f"{market['matches']}, {market['model_log_loss']:.4f}, {market['market_log_loss']:.4f}" if market else "시장 배당 없음"
            lines.append(f"| {LABEL[label]} | {result['matches']} | {result['log_loss']:.4f} | {result['brier']:.4f} | {versus} |")
    lines += ["", "경기 수가 적은 동안의 숫자는 크게 흔들립니다. 3c 기준처럼 300경기 이상 쌓이기 전에는 결론을 내리지 않습니다."]
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="경기 전 예측 주간 보고서(3d)")
    parser.add_argument("command", choices=["report"])
    parser.add_argument("--week", type=date.fromisoformat, help="보고할 주의 월요일(기본: 지난주)")
    parser.add_argument("--markdown", type=Path, help="Markdown 보고서를 저장할 경로")
    parser.add_argument("--no-store", action="store_true", help="DB에 기록하지 않는다")
    args = parser.parse_args(argv)
    summary = run_weekly(week_start=args.week, store=not args.no_store)
    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(render_weekly_markdown(summary), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
