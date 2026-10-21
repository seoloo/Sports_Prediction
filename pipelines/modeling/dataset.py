"""시간순 학습·평가 데이터셋 읽기와 검증(로드맵 3a).

데이터셋은 dbt 모델 gold.match_features_pit가 만든다. 이 모듈은
- load_dataset: 모델 학습·평가에서 쓸 행을 시간 순서로 읽는다.
- verify_point_in_time: dbt와 다른 방식(Python)으로 특징을 다시 계산해 같은지 확인한다.
  같은 규칙을 두 번 다르게 구현해 비교하므로, 한쪽에 미래 정보가 섞이면 드러난다.
- coverage: 시즌별로 특징·시장 기준선이 얼마나 채워졌는지 보여 준다.
실행: python -m pipelines.modeling.dataset
"""
import json
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

import psycopg

from ..config import load_settings
from ..db import connect

AVAILABILITY_LAG = timedelta(hours=3)   # dbt 모델과 같은 과거 재구성 가정: 결과는 시작 3시간 뒤에 알려짐
FEATURE_COLUMNS = ("l10_n", "l10_gf", "l10_ga", "l10_pts", "l38_n", "l38_gf", "l38_ga")


def load_dataset(conn: psycopg.Connection, *, finished_only: bool = True,
                 competitions: tuple[str, ...] | None = None) -> list[dict]:
    """데이터셋을 시작 시각 순서로 읽는다. finished_only면 결과가 있는 경기만(학습·평가용).

    competitions를 주면 그 리그만(5a: 모델 학습·평가는 MODELED 리그만, 누수 검사는 전체 리그).
    """
    conditions = (["result IS NOT NULL"] if finished_only else []) + (
        ["competition_code = ANY(%(competitions)s)"] if competitions else [])
    cursor = conn.execute("SELECT * FROM gold.match_features_pit"
                          + (" WHERE " + " AND ".join(conditions) if conditions else "")
                          + " ORDER BY kickoff_utc, match_id", {"competitions": list(competitions or [])})
    columns = [column.name for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def load_match_stats(conn: psycopg.Connection) -> dict:
    """경기 ID → 끝난 경기의 기록(유효 슈팅, 퇴장 수 합, 3g). gold.match_stats에 없는 경기는 빠진다."""
    return {match_id: {"home_sot": home_sot, "away_sot": away_sot, "red_cards": home_red + away_red}
            for match_id, home_sot, away_sot, home_red, away_red in conn.execute(
                """SELECT match_id, home_shots_on_target, away_shots_on_target, home_red_cards, away_red_cards
                   FROM gold.match_stats""").fetchall()}


def attach_stats(rows: list[dict], stats: dict) -> list[dict]:
    """데이터셋 행에 그 경기의 기록을 붙인 복사본. 기록은 결과와 같은 사후 정보라 학습(과거 경기)에만 쓴다."""
    empty = {"home_sot": None, "away_sot": None, "red_cards": 0}
    return [{**row, **stats.get(row["match_id"], empty)} for row in rows]


def _recompute(history: list[tuple], kickoff, match_id) -> dict:
    """한 팀의 과거 경기(시작 시각, 경기 ID, 득점, 실점, 승점) 중 kickoff 시점에 알려진 것으로 특징을 계산한다."""
    known = [row for row in history if row[1] != match_id and row[0] + AVAILABILITY_LAG <= kickoff]
    known.sort(key=lambda row: row[0], reverse=True)
    last10, last38 = known[:10], known[:38]
    mean = lambda rows, index: (sum(Decimal(row[index]) for row in rows) / len(rows)) if rows else None
    return {"l10_n": len(last10), "l10_gf": mean(last10, 2), "l10_ga": mean(last10, 3), "l10_pts": mean(last10, 4),
            "l38_n": len(last38), "l38_gf": mean(last38, 2), "l38_ga": mean(last38, 3)}


def verify_point_in_time(conn: psycopg.Connection) -> dict:
    """모든 경기의 특징을 Python으로 다시 계산해 dbt 결과와 비교한다. mismatches가 0이어야 정상."""
    history = defaultdict(list)
    for team, kickoff, match_id, goals_for, goals_against, points in conn.execute(
            "SELECT team, kickoff_utc, match_id, goals_for, goals_against, points FROM gold.team_match_results "
            "WHERE kickoff_utc IS NOT NULL").fetchall():
        history[team].append((kickoff, match_id, goals_for, goals_against, points))
    mismatches = []
    rows = load_dataset(conn, finished_only=False)
    for row in rows:
        for side in ("home", "away"):
            expected = _recompute(history[row[f"{side}_team"]], row["kickoff_utc"], row["match_id"])
            for column in FEATURE_COLUMNS:
                stored, recomputed = row[f"{side}_{column}"], expected[column]
                if column.endswith("_n"):
                    same = stored == recomputed
                else:
                    same = (stored is None and recomputed is None) or (
                        stored is not None and recomputed is not None and abs(Decimal(stored) - recomputed) < Decimal("0.0001"))
                if not same:
                    mismatches.append({"match_id": row["match_id"], "column": f"{side}_{column}",
                                       "stored": str(stored), "recomputed": str(recomputed)})
    return {"rows_checked": len(rows), "mismatches": len(mismatches), "examples": mismatches[:5]}


def coverage(conn: psycopg.Connection) -> list[dict]:
    """시즌별 행 수, 끝난 경기 수, 양 팀 모두 최근 10경기가 찬 경기 수, 시장 마감 기준선이 있는 경기 수."""
    rows = conn.execute("""
        SELECT season, count(*) AS matches, count(result) AS finished,
               count(*) FILTER (WHERE result IS NOT NULL AND home_l10_n = 10 AND away_l10_n = 10) AS full_history,
               count(*) FILTER (WHERE result IS NOT NULL AND market_close_home IS NOT NULL) AS with_market_close,
               min(least(home_l10_n, away_l10_n)) AS min_history
        FROM gold.match_features_pit GROUP BY season ORDER BY season""").fetchall()
    keys = ("season", "matches", "finished", "full_history", "with_market_close", "min_history")
    return [dict(zip(keys, row)) for row in rows]


if __name__ == "__main__":
    with connect(load_settings()) as connection:
        print(json.dumps({"coverage": coverage(connection), "point_in_time_check": verify_point_in_time(connection)},
                         ensure_ascii=False, indent=2, default=str))
