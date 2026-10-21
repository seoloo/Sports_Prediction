"""경기 시작 전 자동 스냅샷(로드맵 2e).

Airflow DAG(prematch_snapshots)가 30분마다 run()을 부른다.
- 경기마다 T-24H, T-3H, T-1H 세 시점이 있다. 시점이 지났고 아직 기록이 없으면:
  - 시점에서 GRACE(90분) 안이면 지금 알려진 입력으로 스냅샷을 찍는다(TAKEN).
  - 그보다 늦었으면 MISSED로만 남긴다. 늦게 찍은 스냅샷은 "그 시점의 입력"이 아니기 때문이다.
- 최근 7일 안에 이미 시작한 경기도 살펴, 기록이 없는 시점을 모두 MISSED로 남긴다.
  PC가 경기 당일 내내 꺼져 있었던 경우에도 "놓쳤다"는 사실이 기록되게 하기 위해서다(시작 뒤에는 찍지 않는다).
입력(build_inputs): 경기 정보, 양 팀 최근 5경기 결과, 그 경기의 배당. 모두 as_of 시점에 알려진 버전만 쓴다.
스냅샷을 찍을 때 활성 모델의 예측도 같은 as_of로 함께 남긴다(3d, pipelines/modeling/prospective.py).
검증 기준을 통과하지 못한 모델의 확률은 평가용 기록일 뿐이고 표시 상태는 WITHHELD다.
예측이 실패해도 입력 스냅샷은 그대로 저장한다.
"""
import hashlib
import json
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from .config import load_settings
from .db import connect, migrate

OFFSETS = {"T-24H": timedelta(hours=24), "T-3H": timedelta(hours=3), "T-1H": timedelta(hours=1)}
GRACE = timedelta(minutes=90)
LOOKAHEAD = timedelta(hours=25)        # 앞으로 이 시간 안에 시작하는 경기를 살핀다
MISS_LOOKBACK = timedelta(days=7)      # 이미 시작한 경기는 이 기간까지 거슬러 MISSED를 채운다
FORM_MATCHES = 5

# 기준 시각에 알려진 경기 정보. football-data.org 버전이 있으면 우선하고, 없으면 openfootball(dbt stg_matches_known과 같은 규칙).
KNOWN_FIXTURES_SQL = """
    SELECT m.match_id, m.competition_code, m.season, h.canonical_name AS home, a.canonical_name AS away,
           COALESCE(l.kickoff_utc, o.kickoff_utc) AS kickoff_utc,
           CASE WHEN l.match_id IS NULL THEN o.status
                WHEN l.status = 'FINISHED' THEN 'FINISHED'
                WHEN l.status IN ('IN_PLAY', 'PAUSED') THEN 'LIVE'
                WHEN l.status IN ('SCHEDULED', 'TIMED') THEN 'SCHEDULED'
                ELSE 'NOT_PLAYED' END AS status,
           CASE WHEN l.match_id IS NULL THEN o.ft_home ELSE l.ft_home END AS ft_home,
           CASE WHEN l.match_id IS NULL THEN o.ft_away ELSE l.ft_away END AS ft_away,
           CASE WHEN l.match_id IS NULL THEN 'openfootball' ELSE 'football-data-org' END AS fixture_source,
           m.home_team_id, m.away_team_id
    FROM silver.matches m
    LEFT JOIN silver.match_revisions_known_at(%(as_of)s) o ON o.match_id = m.match_id
    LEFT JOIN silver.live_match_revisions_known_at(%(as_of)s) l
           ON l.match_id = m.match_id AND l.source_id = 'football-data-org'
    JOIN silver.teams h ON h.team_id = m.home_team_id
    JOIN silver.teams a ON a.team_id = m.away_team_id
    WHERE o.match_id IS NOT NULL OR l.match_id IS NOT NULL
"""


def decide(kickoff: datetime, now: datetime, already_recorded: set[str]) -> dict[str, str]:
    """경기 하나에 대해 지금 해야 할 일 {시점 이름: "TAKE" 또는 "MISS"}. DB를 쓰지 않는 순수 함수.

    이미 시작한 경기는 남은 시점을 모두 MISS로 돌려준다. 시작 뒤에 찍으면 경기 중·후 정보가 섞이기 때문이다.
    """
    if kickoff <= now:
        return {label: "MISS" for label in OFFSETS if label not in already_recorded}
    actions = {}
    for label, before in OFFSETS.items():
        due = kickoff - before
        if label in already_recorded or due > now:
            continue
        actions[label] = "TAKE" if now - due <= GRACE else "MISS"
    return actions


def _json_ready(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if not isinstance(value, (int, float, str, bool, type(None))) else value


def build_inputs(conn: psycopg.Connection, match_id: int, as_of: datetime) -> dict:
    """as_of 시점에 알려진 입력을 모은다. 같은 DB 상태·같은 as_of면 항상 같은 결과(정렬 고정)."""
    fixtures = conn.execute(KNOWN_FIXTURES_SQL, {"as_of": as_of}).fetchall()
    columns = ["match_id", "competition_code", "season", "home", "away", "kickoff_utc", "status", "ft_home", "ft_away",
               "fixture_source", "home_team_id", "away_team_id"]
    rows = [dict(zip(columns, row)) for row in fixtures]
    target = next(row for row in rows if row["match_id"] == match_id)

    def form(team_id):
        played = [row for row in rows if row["status"] == "FINISHED" and row["kickoff_utc"] is not None
                  and row["kickoff_utc"] < target["kickoff_utc"] and team_id in (row["home_team_id"], row["away_team_id"])]
        played.sort(key=lambda row: (row["kickoff_utc"], row["match_id"]), reverse=True)
        result = []
        for row in played[:FORM_MATCHES]:
            is_home = row["home_team_id"] == team_id
            goals_for, goals_against = (row["ft_home"], row["ft_away"]) if is_home else (row["ft_away"], row["ft_home"])
            result.append({"match_id": row["match_id"], "kickoff_utc": row["kickoff_utc"].isoformat(),
                           "opponent": row["away"] if is_home else row["home"], "venue": "HOME" if is_home else "AWAY",
                           "goals_for": goals_for, "goals_against": goals_against})
        return result

    odds = conn.execute("""SELECT source_id, bookmaker, market, line, selection, timing, price, caution
                           FROM silver.odds_known_at(%s) WHERE match_id=%s
                           ORDER BY source_id, bookmaker, market, line, selection, timing""", (as_of, match_id)).fetchall()
    return {
        "match": {key: _json_ready(target[key]) for key in ("match_id", "competition_code", "season", "home", "away",
                                                             "kickoff_utc", "status", "fixture_source")},
        "home_form": form(target["home_team_id"]),
        "away_form": form(target["away_team_id"]),
        "odds": [dict(zip(("source_id", "bookmaker", "market", "line", "selection", "timing", "price", "caution"),
                          (_json_ready(value) for value in row))) for row in odds],
    }


def inputs_hash(inputs: dict) -> str:
    return hashlib.sha256(json.dumps(inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def attach_prediction(conn: psycopg.Connection, match_id: int, label: str, match: dict, as_of: datetime,
                      predictors: dict) -> str:
    """방금 찍은 스냅샷에 모델 예측을 붙인다. 모델 모듈을 불러올 수 없어도(예: numpy 없는 이미지) 스냅샷은 지킨다."""
    try:
        from .modeling.prospective import record_for_snapshot
    except ImportError:
        return "UNAVAILABLE"
    kickoff = datetime.fromisoformat(match["kickoff_utc"]) if match.get("kickoff_utc") else None
    return record_for_snapshot(conn, match_id, label, match["home"], match["away"], as_of, predictors,
                               season=match["season"], kickoff=kickoff)


    try:
        with conn.transaction():
    except Exception:   # noqa: BLE001 (비교 실패가 입력 스냅샷까지 잃게 하지 않는다)
        return "FAILED"


def run(*, now: datetime | None = None, settings=None) -> dict:
    """해야 할 스냅샷을 찍거나 MISSED로 기록하고 건수를 돌려준다. 같은 시각에 다시 불러도 새로 기록하지 않는다(멱등)."""
    now = now or datetime.now(timezone.utc)
    counts = {"TAKEN": 0, "MISSED": 0, "upcoming_matches": 0, "recently_started_matches": 0}
    predictors = {}   # 같은 실행(같은 as_of)에서는 모델을 한 번만 학습한다
    with connect(settings or load_settings()) as conn:
        migrate(conn)
        with conn.transaction():
            fixtures = [row for row in conn.execute(KNOWN_FIXTURES_SQL, {"as_of": now}).fetchall() if row[5] is not None]
            # 곧 시작할 예정 경기 + 최근 7일 안에 시작한 경기(연기·취소된 경기는 새 일정이 잡히면 다시 살핀다)
            upcoming = [row for row in fixtures if now < row[5] <= now + LOOKAHEAD and row[6] == "SCHEDULED"]
            started = [row for row in fixtures if now - MISS_LOOKBACK < row[5] <= now and row[6] != "NOT_PLAYED"]
            counts["upcoming_matches"], counts["recently_started_matches"] = len(upcoming), len(started)
            for row in upcoming + started:
                match_id, kickoff = row[0], row[5]
                recorded = {label for (label,) in conn.execute(
                    "SELECT offset_label FROM snapshots.prematch_inputs WHERE match_id=%s", (match_id,))}
                for label, action in decide(kickoff, now, recorded).items():
                    due = kickoff - OFFSETS[label]
                    if action == "TAKE":
                        inputs = build_inputs(conn, match_id, now)
                        conn.execute("""INSERT INTO snapshots.prematch_inputs
                                        (match_id, offset_label, kickoff_utc, due_at, status, as_of, inputs, inputs_hash, recorded_at)
                                        VALUES (%s, %s, %s, %s, 'TAKEN', %s, %s, %s, %s)""",
                                     (match_id, label, kickoff, due, now, Jsonb(inputs), inputs_hash(inputs), now))
                        counts["TAKEN"] += 1
                        outcome = attach_prediction(conn, match_id, label, inputs["match"], now, predictors)
                        counts["PREDICTION_" + outcome] = counts.get("PREDICTION_" + outcome, 0) + 1
                        counts["VALUE_" + value] = counts.get("VALUE_" + value, 0) + 1
                    else:
                        conn.execute("""INSERT INTO snapshots.prematch_inputs
                                        (match_id, offset_label, kickoff_utc, due_at, status, recorded_at)
                                        VALUES (%s, %s, %s, %s, 'MISSED', %s)""", (match_id, label, kickoff, due, now))
                        counts["MISSED"] += 1
    return counts


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False))
