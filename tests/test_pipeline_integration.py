"""Postgres·S3 컨테이너를 쓰는 통합 테스트.

실행 조건: docker compose로 서비스를 띄우고 .env가 있을 것. 서비스에 접속할 수 없으면 건너뛴다.
운영 DB·버킷을 건드리지 않도록 테스트마다 임시 데이터베이스를 만들고 끝나면 지운다.
버킷은 테스트 전용 하나(bronze-integration-test)를 비워 가며 재사용한다.
버킷을 매번 새로 만들면 SeaweedFS가 버킷마다 볼륨을 새로 잡아 저장 공간을 빠르게 소모하기 때문이다.
"""
import json
import os
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from pipelines.config import load_settings
from pipelines.ingest import openfootball
from pipelines.object_store import client, read_object

SAMPLE_PATH = Path(__file__).parent / "fixtures" / "openfootball_en1_sample.json"
TEST_BUCKET = "bronze-integration-test"


def empty_bucket(store, bucket):
    """테스트 버킷의 객체를 모두 지운다. 버킷이 없으면 아무것도 하지 않는다."""
    if bucket not in [item["Name"] for item in store.list_buckets()["Buckets"]]:
        return
    for item in store.list_objects_v2(Bucket=bucket).get("Contents", []):
        store.delete_object(Bucket=bucket, Key=item["Key"])


@pytest.fixture
def isolated_settings():
    try:
        base = load_settings()
        admin = psycopg.connect(base.postgres_conninfo, autocommit=True)
        store = client(base)
        store.list_buckets()
    except Exception as exc:  # 서비스가 없으면 단위 테스트만 실행한다. CI(REQUIRE_INTEGRATION=1)에서는 실패시킨다.
        if os.environ.get("REQUIRE_INTEGRATION") == "1":
            raise
        pytest.skip(f"Postgres/S3 서비스 없음: {exc}")
    database = f"sp_test_{uuid.uuid4().hex[:10]}"
    admin.execute(f'CREATE DATABASE "{database}"')
    empty_bucket(store, TEST_BUCKET)
    yield replace(base, postgres_db=database, bronze_bucket=TEST_BUCKET)
    admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
    admin.close()
    empty_bucket(store, TEST_BUCKET)


def query(settings, sql, params=()):
    with psycopg.connect(settings.postgres_conninfo) as conn:
        return conn.execute(sql, params).fetchall()


def run(settings, payload, at):
    return openfootball.ingest("2025-26", "en.1", payload=payload, source_url="file:sample.json",
                               settings=settings, received_at=datetime.fromisoformat(at))


def test_ingest_is_idempotent_and_versions_only_changes(isolated_settings):
    s = isolated_settings
    original = SAMPLE_PATH.read_bytes()
    first = run(s, original, "2025-08-17T00:00:00+00:00")
    assert (first["new_content"], first["matches_new"], first["revisions_new"], first["quality_issues"]) == (True, 5, 5, 2)

    again = run(s, original, "2025-08-18T00:00:00+00:00")
    assert (again["new_content"], again["bronze_uploaded"], again["revisions_new"]) == (False, False, 0)
    assert query(s, "SELECT count(*) FROM ingest.raw_files")[0][0] == 1
    assert query(s, "SELECT count(*), sum(is_new_content::int) FROM ingest.fetches")[0] == (2, 1)
    assert query(s, "SELECT count(*) FROM silver.quality_issues")[0][0] == 2

    # 한 경기의 점수만 정정된 새 원본: 그 경기만 2번 버전이 생긴다.
    document = json.loads(original)
    document["matches"][0]["score"]["ft"] = [3, 1]
    corrected = json.dumps(document).encode()
    third = run(s, corrected, "2025-08-19T00:00:00+00:00")
    assert (third["new_content"], third["matches_new"], third["revisions_new"]) == (True, 0, 1)

    # 원본이 예전 내용으로 되돌아가면, 이미 본 파일이어도 최신 버전을 다시 맞춘다.
    fourth = run(s, original, "2025-08-20T00:00:00+00:00")
    assert (fourth["new_content"], fourth["revisions_new"]) == (False, 1)

    revisions = query(s, """SELECT r.revision_no, r.ft_home, r.received_at FROM silver.match_revisions r
                            JOIN silver.matches m USING (match_id) JOIN silver.teams h ON h.team_id=m.home_team_id
                            JOIN silver.teams a ON a.team_id=m.away_team_id
                            WHERE h.canonical_name='Alpha FC' AND a.canonical_name='Beta FC' ORDER BY r.revision_no""")
    assert [(row[0], row[1]) for row in revisions] == [(1, 2), (2, 3), (3, 2)]

    # 기준 시각 함수: 정정 전 시점에는 1번 버전, 정정 후에는 2번 버전이 보인다.
    known = """SELECT ft_home FROM silver.match_revisions_known_at(%s) WHERE match_id=
               (SELECT match_id FROM silver.match_revisions WHERE revision_no=3)"""
    assert query(s, known, ("2025-08-18T12:00:00+00:00",))[0][0] == 2
    assert query(s, known, ("2025-08-19T12:00:00+00:00",))[0][0] == 3

    # Bronze 원본은 내용별로 하나씩, 해시와 일치한다.
    for key, sha in query(s, "SELECT object_key, sha256 FROM ingest.raw_files"):
        import hashlib
        assert hashlib.sha256(read_object(client(s), s.bronze_bucket, key)).hexdigest() == sha.strip()
    assert query(s, "SELECT count(*) FROM ingest.raw_files")[0][0] == 2


def test_history_tables_are_immutable(isolated_settings):
    s = isolated_settings
    run(s, SAMPLE_PATH.read_bytes(), "2025-08-17T00:00:00+00:00")
    for sql in ("UPDATE silver.match_revisions SET ft_home=9", "DELETE FROM silver.match_revisions",
                "UPDATE ingest.raw_files SET byte_size=1", "DELETE FROM silver.team_aliases"):
        with psycopg.connect(s.postgres_conninfo) as conn, pytest.raises(psycopg.errors.RaiseException):
            conn.execute(sql)


def test_failed_run_is_recorded_and_writes_nothing(isolated_settings):
    s = isolated_settings
    with pytest.raises(json.JSONDecodeError):
        run(s, b"{not json", "2025-08-17T00:00:00+00:00")
    assert query(s, "SELECT status FROM ingest.runs") == [("FAILED",)]
    assert query(s, "SELECT count(*) FROM ingest.raw_files")[0][0] == 0
    assert query(s, "SELECT count(*) FROM silver.match_revisions")[0][0] == 0
