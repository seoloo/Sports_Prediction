"""Gold 테이블을 Parquet로 lake 버킷에 내보낸다(로드맵 4a).

내보내는 것(한 번의 내보내기 = 스냅샷 하나):
- team_match_results: 끝난 경기를 팀 관점 2행으로(Spark 특징 계산의 입력)
- matches: 특징을 계산할 경기 목록(gold.match_features_pit와 같은 경기)
- dbt_match_features: dbt가 계산한 특징(대조의 정답). 같은 스냅샷 안에 두어, 내보낸 뒤 Gold가 다시 만들어져도
  Spark 결과를 "같은 입력으로 dbt가 낸 값"과 비교할 수 있게 한다.
- nba_games, dbt_nba_standings(6b): 끝난 NBA 경기 원자료와 dbt 순위표(정답).

경로: exports/<스냅샷>/<테이블>/season=<시즌>/part-0.parquet (Hive 방식 파티션: Spark가 시즌 열을 경로에서 읽는다)
스냅샷 ID는 내용의 해시라, 같은 Gold를 다시 내보내면 같은 경로에 같은 파일(이미 있으면 올리지 않음).
exports/latest.json만 가장 최근 스냅샷을 가리키도록 덮어쓴다.
"""
import hashlib
import io
import json
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from ..object_store import ensure_bucket, put_if_absent

PREFIX = "exports"
PARQUET = "application/vnd.apache.parquet"
UTC_TIME = pa.timestamp("us", tz="UTC")
FEATURES = ("l10_n", "l10_gf", "l10_ga", "l10_pts", "l38_n", "l38_gf", "l38_ga", "rest_days", "last_feature_kickoff")
NBA_COUNTS = ("played", "won", "lost", "points_for", "points_against", "point_difference")

# 테이블 이름 → (SQL, 파일 안 열의 형식). 첫 열(season)은 경로로 나뉘어 파일 안에는 넣지 않는다.
TABLES = {
    "team_match_results": (
        """SELECT season, team, match_id, kickoff_utc, goals_for, goals_against, points FROM gold.team_match_results
           WHERE kickoff_utc IS NOT NULL ORDER BY season, kickoff_utc, match_id, team""",
        pa.schema([("team", pa.string()), ("match_id", pa.int64()), ("kickoff_utc", UTC_TIME),
                   ("goals_for", pa.int32()), ("goals_against", pa.int32()), ("points", pa.int32())])),
    "matches": (
        """SELECT season, match_id, kickoff_utc, home_team, away_team FROM gold.match_features_pit
           ORDER BY season, kickoff_utc, match_id""",
        pa.schema([("match_id", pa.int64()), ("kickoff_utc", UTC_TIME), ("home_team", pa.string()),
                   ("away_team", pa.string())])),
    "dbt_match_features": (
        "SELECT season, match_id, " + ", ".join(
            f"{side}_{column}::float8" if column not in ("l10_n", "l38_n", "last_feature_kickoff") else f"{side}_{column}"
            for side in ("home", "away") for column in FEATURES) + " FROM gold.match_features_pit ORDER BY season, match_id",
        pa.schema([("match_id", pa.int64())] + [
            (f"{side}_{column}", pa.int64() if column.endswith("_n") else UTC_TIME if column == "last_feature_kickoff"
             else pa.float64()) for side in ("home", "away") for column in FEATURES])),
    # 6b: NBA. Spark는 팀별로 펼친 Gold(nba_team_game_results)가 아니라 끝난 경기 원자료에서 순위표를 처음부터 만든다.
    "nba_games": (
        """SELECT season, game_id, postseason, home_team, away_team, home_score, away_score FROM staging.stg_games_known
           WHERE status = 'FINISHED' ORDER BY season, game_id""",
        pa.schema([("game_id", pa.int64()), ("postseason", pa.bool_()), ("home_team", pa.string()),
                   ("away_team", pa.string()), ("home_score", pa.int32()), ("away_score", pa.int32())])),
    "dbt_nba_standings": (
        """SELECT season, team, played, won, lost, points_for, points_against, point_difference, win_pct::float8,
                  avg_margin::float8, position FROM gold.nba_standings ORDER BY season, position""",
        pa.schema([("team", pa.string())] + [(name, pa.int64()) for name in NBA_COUNTS]
                  + [("win_pct", pa.float64()), ("avg_margin", pa.float64()), ("position", pa.int64())])),
}


def snapshot_id(tables: dict[str, list[tuple]]) -> str:
    """내보낼 내용 전체의 해시(앞 16자리). 내용이 같으면 ID도 같다."""
    digest = hashlib.sha256()
    for name in sorted(tables):
        digest.update(name.encode())
        for row in tables[name]:
            digest.update(repr(tuple(value.isoformat() if isinstance(value, datetime) else value for value in row)).encode())
    return digest.hexdigest()[:16]


def to_parquet(rows: list[tuple], schema: pa.Schema) -> bytes:
    """행 목록을 Parquet 바이트로. 열 형식을 명시해 시즌마다 형식이 달라지지 않게 한다."""
    columns = list(zip(*rows)) if rows else [[] for _ in schema]
    table = pa.Table.from_arrays([pa.array(list(values), type=field.type) for values, field in zip(columns, schema)],
                                 schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def export(conn: psycopg.Connection, store, bucket: str) -> dict:
    """Gold를 읽어 Parquet로 올리고 매니페스트(파일·행 수·해시)를 돌려준다."""
    tables = {name: conn.execute(sql).fetchall() for name, (sql, _) in TABLES.items()}
    snapshot = snapshot_id(tables)
    ensure_bucket(store, bucket)
    manifest = {"snapshot": snapshot, "exported_at": datetime.now(timezone.utc).isoformat(),
                "source": "postgres gold (dbt)", "tables": {}}
    uploaded = 0
    for name, rows in tables.items():
        by_season = defaultdict(list)
        for season, *values in rows:
            by_season[season].append(tuple(values))
        files = []
        for season, season_rows in sorted(by_season.items()):
            payload = to_parquet(season_rows, TABLES[name][1])
            key = f"{PREFIX}/{snapshot}/{name}/season={season}/part-0.parquet"
            sha256 = hashlib.sha256(payload).hexdigest()
            uploaded += put_if_absent(store, bucket, key, payload, {"sha256": sha256, "rows": str(len(season_rows))},
                                      content_type=PARQUET)
            files.append({"key": key, "season": season, "rows": len(season_rows), "sha256": sha256})
        manifest["tables"][name] = {"rows": len(rows), "files": files}
    put_if_absent(store, bucket, f"{PREFIX}/{snapshot}/_manifest.json",
                  json.dumps(manifest, ensure_ascii=False, indent=2).encode(), {"snapshot": snapshot})
    store.put_object(Bucket=bucket, Key=f"{PREFIX}/latest.json", ContentType="application/json",
                     Body=json.dumps({"snapshot": snapshot}).encode())
    manifest["uploaded_files"] = uploaded
    return manifest
