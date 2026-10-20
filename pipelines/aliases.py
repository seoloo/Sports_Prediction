"""소스별 팀 이름 → 기준 팀(openfootball 이름) 대조표.

대조표(mappings/team_aliases.csv)는 사람이 검토한 파일이다. 수집기는 이름을 추측하지 않고 이 파일만 쓴다.
후보는 각 수집기의 suggest 기능이 "같은 날짜에 열린 경기" 근거로 만든다.
"""
import csv
from datetime import datetime
from pathlib import Path

import psycopg

ALIAS_FILE = Path(__file__).resolve().parent / "mappings" / "team_aliases.csv"


def load_alias_file(path: Path = ALIAS_FILE) -> list[dict]:
    """대조표를 읽는다. 열: source_id, alias, canonical_name, evidence. #으로 시작하는 줄은 설명이다."""
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(line for line in handle if not line.startswith("#")))


def sync_aliases(conn: psycopg.Connection, aliases: list[dict], now: datetime) -> None:
    """대조표의 별칭을 silver.team_aliases에 넣는다. 이미 다른 팀으로 등록된 별칭이 있으면 멈춘다(덮어쓰지 않음).

    기준 팀이 아직 Silver에 없으면 대조표의 기준 이름으로 만든다. 대조표는 사람이 검토한 근거이므로,
    openfootball에 아직 없는 승격 팀 때문에 다른 소스의 경기가 막히지 않게 하기 위해서다.
    """
    for alias in aliases:
        team = conn.execute("SELECT team_id FROM silver.teams WHERE canonical_name=%s", (alias["canonical_name"],)).fetchone()
        if not team:
            team = conn.execute("INSERT INTO silver.teams (canonical_name, created_at) VALUES (%s, %s) RETURNING team_id",
                                (alias["canonical_name"], now)).fetchone()
        existing = conn.execute("SELECT team_id FROM silver.team_aliases WHERE source_id=%s AND alias=%s",
                                (alias["source_id"], alias["alias"])).fetchone()
        if existing and existing[0] != team[0]:
            raise ValueError(f"별칭 충돌: {alias['alias']}은 이미 다른 팀에 연결되어 있습니다.")
        if not existing:
            conn.execute("INSERT INTO silver.team_aliases VALUES (%s, %s, %s, %s)",
                         (alias["source_id"], alias["alias"], team[0], now))
