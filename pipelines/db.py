"""Postgres 연결과 스키마 이전(migration).

pipelines/sql/의 NNN_*.sql 파일을 번호 순서대로 한 번씩만 적용하고, 적용 기록을 public.schema_migrations에 남긴다.
실행: python -m pipelines.db migrate
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from .config import Settings, load_settings

SQL_DIR = Path(__file__).resolve().parent / "sql"

# 여러 프로세스가 동시에 migrate를 실행해도 한 번만 적용되도록 잡는 Postgres advisory lock 번호
MIGRATION_LOCK_ID = 7_302_101


def connect(settings: Settings | None = None) -> psycopg.Connection:
    """Postgres 연결을 연다. 기본은 .env 설정. 호출한 쪽이 with 블록으로 닫는다."""
    return psycopg.connect((settings or load_settings()).postgres_conninfo)


def migration_files() -> list[Path]:
    """적용할 SQL 파일을 이름(번호) 순서로 돌려준다."""
    return sorted(SQL_DIR.glob("[0-9][0-9][0-9]_*.sql"))


def migrate(conn: psycopg.Connection) -> list[str]:
    """아직 적용하지 않은 SQL 파일을 차례로 적용하고, 이번에 적용한 파일 이름 목록을 돌려준다.

    파일 하나는 트랜잭션 하나로 적용되어, 중간에 실패하면 그 파일의 변경은 전부 취소된다.
    """
    applied_now = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
        conn.execute("""CREATE TABLE IF NOT EXISTS public.schema_migrations (
                          version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)""")
        already_applied = {row[0] for row in conn.execute("SELECT version FROM public.schema_migrations")}
        for path in migration_files():
            if path.name in already_applied:
                continue
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO public.schema_migrations VALUES (%s, %s)",
                         (path.name, datetime.now(timezone.utc)))
            applied_now.append(path.name)
    return applied_now


if __name__ == "__main__":
    if sys.argv[1:] != ["migrate"]:
        print("사용법: python -m pipelines.db migrate")
        sys.exit(2)
    with connect() as connection:
        applied = migrate(connection)
    print("적용: " + (", ".join(applied) if applied else "새로 적용할 파일 없음"))
