"""NBA 경기 전 모델 예측 기록(로드맵 5d-d). PROSPECTIVE. 축구 prospective.py와 같은 규칙.

- 예측은 경기 전 스냅샷을 찍는 순간에만 만든다(nba_snapshots.run이 record_for_snapshot을 부른다).
- 학습 자료는 스냅샷 기준 시각(as_of)에 실제로 받아 둔 결과뿐이다.
- 활성 모델: NBA 시험 실행 중 검증 로그 손실이 가장 낮은 계열(축구와 같은 active_model 규칙, competition = nba).
  검증 기준을 통과하지 못했으면 확률은 평가용으로만 저장하고 표시 상태는 WITHHELD다.
"""
import hashlib
from datetime import datetime

import psycopg

from .nba_backtest import COMPETITION, MIN_TRAINING_GAMES, as_game, load_games
from .nba_models import Game, model_from_params
from .prospective import active_model

MIN_TEAM_HISTORY = 5


class NbaPredictor:
    """스냅샷 실행 한 번(같은 as_of)에서 한 번만 학습하고 여러 경기를 예측한다."""

    def __init__(self, conn: psycopg.Connection, as_of: datetime):
        self.as_of = as_of
        self.active = active_model(conn, COMPETITION)
        rows, _ = load_games(conn, as_of)
        self.training = [as_game(row) for row in rows if row["tipoff_utc"] < as_of]
        self.training_hash = hashlib.sha256(repr([(g.tipoff.isoformat(), *g[1:]) for g in self.training]).encode()).hexdigest()
        self.history = {}
        for game in self.training:
            for team in (game.home, game.away):
                self.history[team] = self.history.get(team, 0) + 1
        self.model, self.reasons = None, []
        if self.active is None:
            self.reasons.append("NO_ACTIVE_MODEL")
        else:
            model = model_from_params(self.active["name"], self.active["params"])
            if model.version != self.active["model_version"]:
                self.reasons.append("MODEL_CODE_CHANGED")   # 지금 코드는 평가받은 코드가 아니다
            elif len(self.training) < MIN_TRAINING_GAMES:
                self.reasons.append(f"INSUFFICIENT_TRAINING_DATA(<{MIN_TRAINING_GAMES})")
            else:
                self.model = model.fit(self.training, as_of)

    def predict(self, game: Game) -> dict:
        reasons = list(self.reasons)
        probability = self.model.predict(game) if self.model else None
        if self.active and self.active["gate_status"] != "VALIDATED":
            reasons.append("MODEL_NOT_VALIDATED")
        if min(self.history.get(game.home, 0), self.history.get(game.away, 0)) < MIN_TEAM_HISTORY:
            reasons.append(f"LOW_TEAM_HISTORY(<{MIN_TEAM_HISTORY})")
        active = self.active or {}
        return {"model_name": active.get("name", "unavailable"), "model_version": active.get("model_version", "unavailable"),
                "backtest_run_id": active.get("run_id"), "evaluation_report_id": active.get("report_id"),
                "gate_status": active.get("gate_status", "UNKNOWN"), "training_games": len(self.training),
                "last_training_tipoff": self.training[-1].tipoff if self.training else None,
                "training_hash": self.training_hash, "prob_home": probability,
                "display_status": "WITHHELD" if reasons else "READY", "withheld_reasons": reasons}


def insert_prediction(conn: psycopg.Connection, game_id: int, offset_label: str, as_of: datetime, row: dict) -> None:
    conn.execute("""
        INSERT INTO snapshots.game_prematch_predictions
            (game_id, offset_label, as_of, model_name, model_version, backtest_run_id, evaluation_report_id, gate_status,
             training_games, last_training_tipoff, training_hash, prob_home, display_status, withheld_reasons, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (game_id, offset_label, as_of, row["model_name"], row["model_version"], row["backtest_run_id"],
         row["evaluation_report_id"], row["gate_status"], row["training_games"], row["last_training_tipoff"],
         row["training_hash"], None if row["prob_home"] is None else round(row["prob_home"], 8),
         row["display_status"], row["withheld_reasons"], as_of))


def record_for_snapshot(conn: psycopg.Connection, game_id: int, offset_label: str, game: Game, as_of: datetime,
                        cache: dict) -> str:
    """방금 찍은 스냅샷에 예측을 붙인다. 실패해도 스냅샷은 지키고 실패 사유를 WITHHELD로 남긴다."""
    try:
        with conn.transaction():   # 저장점: 실패하면 이 예측만 취소
            if as_of not in cache:
                cache[as_of] = NbaPredictor(conn, as_of)
            row = cache[as_of].predict(game)
            insert_prediction(conn, game_id, offset_label, as_of, row)
            return row["display_status"]
    except Exception as exc:   # noqa: BLE001 (예측 실패가 입력 스냅샷까지 잃게 하지 않는다)
        failed = {"model_name": "unavailable", "model_version": "unavailable", "backtest_run_id": None,
                  "evaluation_report_id": None, "gate_status": "UNKNOWN", "training_games": 0,
                  "last_training_tipoff": None, "training_hash": None, "prob_home": None, "display_status": "WITHHELD",
                  "withheld_reasons": [f"PREDICTION_FAILED:{type(exc).__name__}"]}
        with conn.transaction():
            insert_prediction(conn, game_id, offset_label, as_of, failed)
        return "FAILED"


# ---- 경기 후 채점(주간 보고서의 NBA 절, 5e) ----

OFFSET_LABELS = ("T-24H", "T-3H", "T-1H")


def scored_predictions(conn: psycopg.Connection, as_of: datetime) -> list[dict]:
    """확률이 있는 NBA 경기 전 예측과 as_of에 알려진 결과(끝난 경기만), 스냅샷에 들어 있던 해외 시장 확률."""
    from .market import nba_market_fair
    rows = conn.execute("""
        SELECT p.game_id, p.offset_label, p.model_version, p.prob_home::float, i.tipoff_utc, i.inputs,
               k.home_score, k.away_score
        FROM snapshots.game_prematch_predictions p
        JOIN snapshots.game_prematch_inputs i USING (game_id, offset_label)
        JOIN silver.game_revisions_known_at(%s) k USING (game_id)
        WHERE p.prob_home IS NOT NULL AND k.status = 'FINISHED'
        ORDER BY i.tipoff_utc, p.game_id, p.offset_label""", (as_of,)).fetchall()
    scored = []
    for game_id, label, version, prob_home, tipoff, inputs, home_score, away_score in rows:
        market, _ = nba_market_fair(inputs.get("odds", []))
        scored.append({"game_id": game_id, "offset_label": label, "tipoff_utc": tipoff, "model_version": version,
                       "prob_home": prob_home, "result": "HOME" if home_score > away_score else "AWAY",
                       "market_home": market["HOME"] if market else None})
    return scored


def metrics(items: list[dict]) -> dict:
    from .nba_backtest import brier, log_loss
    result = {"games": len(items)}
    if items:
        result["log_loss"] = round(sum(log_loss(i["prob_home"], i["result"]) for i in items) / len(items), 4)
        result["brier"] = round(sum(brier(i["prob_home"], i["result"]) for i in items) / len(items), 4)
        with_market = [i for i in items if i["market_home"] is not None]
        if with_market:
            result["vs_market_pre"] = {
                "games": len(with_market),
                "model_log_loss": round(sum(log_loss(i["prob_home"], i["result"]) for i in with_market) / len(with_market), 4),
                "market_log_loss": round(sum(log_loss(i["market_home"], i["result"]) for i in with_market) / len(with_market), 4)}
    return result


def weekly(conn: psycopg.Connection, start: datetime, end: datetime, as_of: datetime) -> dict:
    """주간 보고서의 NBA 절: 그 주 스냅샷·예측 상태, 이번 주·누적 채점(시장 비교 포함)."""
    from collections import Counter, defaultdict
    by_offset, reasons = defaultdict(Counter), Counter()
    for label, status, display, withheld, has_probability in conn.execute("""
            SELECT i.offset_label, i.status, p.display_status, p.withheld_reasons, p.prob_home IS NOT NULL
            FROM snapshots.game_prematch_inputs i
            LEFT JOIN snapshots.game_prematch_predictions p USING (game_id, offset_label)
            WHERE i.tipoff_utc >= %s AND i.tipoff_utc < %s AND i.recorded_at <= %s""", (start, end, as_of)).fetchall():
        by_offset[label][status] += 1
        if status == "TAKEN":
            by_offset[label]["PREDICTION_" + (display or "MISSING")] += 1
            by_offset[label]["with_probabilities"] += int(bool(has_probability))
            reasons.update(withheld or [])
    scored = scored_predictions(conn, as_of)
    this_week = [item for item in scored if start <= item["tipoff_utc"] < end]
    active = active_model(conn, COMPETITION)
    return {"active_model": active and {key: active[key] for key in ("name", "model_version", "run_id", "report_id", "gate_status")},
            "snapshots": {label: dict(counts) for label, counts in sorted(by_offset.items())},
            "withheld_reasons": dict(reasons),
            "week": {label: metrics([i for i in this_week if i["offset_label"] == label]) for label in OFFSET_LABELS},
            "cumulative": {label: metrics([i for i in scored if i["offset_label"] == label]) for label in OFFSET_LABELS}}
