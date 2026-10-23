"""NBA 경기 전 자동 스냅샷(로드맵 5d-d). 축구 snapshots.py와 같은 규칙.

Airflow DAG(prematch_snapshots)가 30분마다 run()을 부른다.
- 경기마다 T-24H, T-3H, T-1H. 시점에서 90분 안이면 찍고(TAKEN), 늦었으면 MISSED로만 남긴다(decide 재사용).
- 입력: 경기 정보, 양 팀 최근 5경기, 연전 여부(전날 경기, 일정은 미리 공개), 그 경기의 배당. 모두 as_of에 알려진 버전.
- 같은 as_of로 모델 예측(nba_prospective)을 남긴다. 실패해도 입력 스냅샷은 지킨다.
"""
import json
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from .config import load_settings
from .db import connect, migrate
from .snapshots import FORM_MATCHES, LOOKAHEAD, MISS_LOOKBACK, OFFSETS, decide, inputs_hash

GAMES_SQL = """
    SELECT g.game_id, g.season, g.postseason, h.canonical_name, a.canonical_name, k.tipoff_utc, k.local_date, k.status,
           k.home_score, k.away_score
    FROM silver.games g JOIN silver.game_revisions_known_at(%(as_of)s) k USING (game_id)
    JOIN silver.teams h ON h.team_id = g.home_team_id JOIN silver.teams a ON a.team_id = g.away_team_id
    WHERE g.competition_code = 'nba'
    ORDER BY k.tipoff_utc NULLS LAST, g.game_id"""
COLUMNS = ("game_id", "season", "postseason", "home", "away", "tipoff_utc", "local_date", "status", "home_score", "away_score")


def known_games(conn: psycopg.Connection, as_of: datetime) -> list[dict]:
    return [dict(zip(COLUMNS, row)) for row in conn.execute(GAMES_SQL, {"as_of": as_of}).fetchall()]


def build_inputs(games: list[dict], target: dict, odds: list[dict]) -> dict:
    """as_of에 알려진 경기 목록(games)에서 입력을 모은다. 같은 DB 상태·같은 as_of면 같은 결과(정렬 고정)."""
    def form(team):
        played = [g for g in games if g["status"] == "FINISHED" and g["tipoff_utc"] and g["tipoff_utc"] < target["tipoff_utc"]
                  and team in (g["home"], g["away"])]
        played.sort(key=lambda g: (g["tipoff_utc"], g["game_id"]), reverse=True)
        return [{"game_id": g["game_id"], "tipoff_utc": g["tipoff_utc"].isoformat(),
                 "opponent": g["away"] if g["home"] == team else g["home"], "venue": "HOME" if g["home"] == team else "AWAY",
                 "points_for": g["home_score"] if g["home"] == team else g["away_score"],
                 "points_against": g["away_score"] if g["home"] == team else g["home_score"]} for g in played[:FORM_MATCHES]]

    before = target["local_date"] - timedelta(days=1)
    played_before = lambda team: any(g["local_date"] == before and team in (g["home"], g["away"])
                                     and g["status"] != "POSTPONED" for g in games)
    return {"game": {"game_id": target["game_id"], "season": target["season"], "postseason": target["postseason"],
                     "home": target["home"], "away": target["away"], "tipoff_utc": target["tipoff_utc"].isoformat(),
                     "local_date": target["local_date"].isoformat(), "status": target["status"],
                     "home_b2b": played_before(target["home"]), "away_b2b": played_before(target["away"])},
            "home_form": form(target["home"]), "away_form": form(target["away"]),
            "odds": [{**row, "price": str(row["price"]), "line": str(row.get("line", 0))} for row in odds]}


def attach_prediction(conn, game_id: int, label: str, inputs: dict, as_of: datetime, cache: dict) -> str:
    try:
        from .modeling.nba_models import Game
        from .modeling.nba_prospective import record_for_snapshot
    except ImportError:
        return "UNAVAILABLE"
    g = inputs["game"]
    game = Game(datetime.fromisoformat(g["tipoff_utc"]), g["season"], g["home"], g["away"], 0, 0, False,
                g["home_b2b"], g["away_b2b"])
    return record_for_snapshot(conn, game_id, label, game, as_of, cache)


def run(*, now: datetime | None = None, settings=None) -> dict:
    """해야 할 NBA 스냅샷을 찍거나 MISSED로 기록한다. 같은 시각에 다시 불러도 새로 기록하지 않는다(멱등)."""
    from .modeling.market import nba_odds_at
    now = now or datetime.now(timezone.utc)
    counts = {"TAKEN": 0, "MISSED": 0, "upcoming_games": 0, "recently_started_games": 0}
    cache = {}
    with connect(settings or load_settings()) as conn:
        migrate(conn)
        with conn.transaction():
            games = known_games(conn, now)
            upcoming = [g for g in games if g["tipoff_utc"] and now < g["tipoff_utc"] <= now + LOOKAHEAD and g["status"] == "SCHEDULED"]
            started = [g for g in games if g["tipoff_utc"] and now - MISS_LOOKBACK < g["tipoff_utc"] <= now
                       and g["status"] != "POSTPONED"]
            counts["upcoming_games"], counts["recently_started_games"] = len(upcoming), len(started)
            for target in upcoming + started:
                game_id, tipoff = target["game_id"], target["tipoff_utc"]
                recorded = {label for (label,) in conn.execute(
                    "SELECT offset_label FROM snapshots.game_prematch_inputs WHERE game_id = %s", (game_id,))}
                for label, action in decide(tipoff, now, recorded).items():
                    due = tipoff - OFFSETS[label]
                    if action == "TAKE":
                        inputs = build_inputs(games, target, nba_odds_at(conn, game_id, now))
                        conn.execute("""INSERT INTO snapshots.game_prematch_inputs
                                        (game_id, offset_label, tipoff_utc, due_at, status, as_of, inputs, inputs_hash, recorded_at)
                                        VALUES (%s, %s, %s, %s, 'TAKEN', %s, %s, %s, %s)""",
                                     (game_id, label, tipoff, due, now, Jsonb(inputs), inputs_hash(inputs), now))
                        counts["TAKEN"] += 1
                        outcome = attach_prediction(conn, game_id, label, inputs, now, cache)
                        counts["PREDICTION_" + outcome] = counts.get("PREDICTION_" + outcome, 0) + 1
                    else:
                        conn.execute("""INSERT INTO snapshots.game_prematch_inputs
                                        (game_id, offset_label, tipoff_utc, due_at, status, recorded_at)
                                        VALUES (%s, %s, %s, %s, 'MISSED', %s)""", (game_id, label, tipoff, due, now))
                        counts["MISSED"] += 1
    return counts


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False))
