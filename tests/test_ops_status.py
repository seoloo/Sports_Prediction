"""7c 운영 상태 보고서 테스트. 판정 규칙은 단위로, DB 조회는 임시 DB(서비스가 없으면 건너뜀)로."""
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from pipelines.object_store import client
from pipelines.ops import status
from tests.test_pipeline_integration import empty_bucket, isolated_settings  # noqa: F401 (fixture 재사용)

NOW = datetime(2026, 10, 24, 6, 0, tzinfo=timezone.utc)
EMPTY_BUCKET = "ops-status-test"


def freshness_item(name, state, hours):
    return {"unique_id": f"source.sports_prediction.ingest.{name}_fetches", "status": state,
            "max_loaded_at": (NOW - timedelta(hours=hours)).isoformat(), "max_loaded_at_time_ago_in_s": hours * 3600,
            "criteria": {"warn_after": {"count": 36, "period": "hour"}, "error_after": {"count": 72, "period": "hour"}}}


def test_overall_is_the_worst_check_and_freshness_follows_dbt():
    assert status.worst(["OK", "WARN", "UNKNOWN"]) == "WARN" and status.worst([]) == "UNKNOWN"
    ok = status.freshness_check([freshness_item("openfootball", "pass", 3)])
    assert ok["status"] == "OK" and ok["rows"][0][:2] == ["openfootball", "OK"] and ok["rows"][0][4] == "36h / 72h"
    mixed = status.freshness_check([freshness_item("openfootball", "pass", 3), freshness_item("balldontlie", "error", 80)])
    assert mixed["status"] == "FAIL" and "balldontlie" in mixed["summary"]
    assert status.freshness_check(None)["status"] == "UNKNOWN"   # 검사를 못 했으면 OK라고 하지 않는다


def test_html_escapes_text_from_sources():
    report = {"generated_at": NOW.isoformat(), "overall": "FAIL", "checks": [
        status.check("수집 실행", "FAIL", "실패", ["사유"], [["<script>alert(1)</script>"]])]}
    page = status.render_html(report)
    assert "<script>alert" not in page and "&lt;script&gt;" in page and "prefers-color-scheme: dark" in page


def test_collect_reports_failures_credits_and_dead_letters(isolated_settings, tmp_path):
    s = replace(isolated_settings, lake_bucket=EMPTY_BUCKET)
    store = client(s)
    empty_bucket(store, EMPTY_BUCKET)
    with psycopg.connect(s.postgres_conninfo) as conn:
        from pipelines.db import migrate
        migrate(conn)
        run = lambda source, state, hours, stats=None, error=None: conn.execute(
            "INSERT INTO ingest.runs VALUES (%s, %s, 'x', %s, %s, %s, %s, %s)",
            (uuid.uuid4(), source, NOW - timedelta(hours=hours), NOW - timedelta(hours=hours), state,
             Jsonb(stats) if stats else None, error))
        run("openfootball", "SUCCEEDED", 30)
        run("openfootball", "FAILED", 2, error="HTTPError 503")            # 마지막 실행 실패 → FAIL
        run("balldontlie", "FAILED", 5, error="timeout")
        run("balldontlie", "SUCCEEDED", 1)                                 # 실패했다가 회복 → WARN
        run("the-odds-api", "SUCCEEDED", 1, {"credits_remaining": "45"})   # 60 미만 → WARN
        reject = lambda offset, reason: conn.execute(
            """INSERT INTO silver.stream_rejections (topic, partition, "offset", event_id, reason, detail, rejected_at)
               VALUES ('fdorg.matches.v1', 0, %s, NULL, %s, '{}', %s)""", (offset, reason, NOW - timedelta(hours=1)))
        reject(1, "STALE_EVENT")
        conn.commit()
        assert status.stream_check(conn, NOW)["status"] == "OK"            # 늦게 온 옛 정보는 정상 동작
        reject(2, "SCHEMA_VIOLATION")
        conn.commit()

    report = status.collect(s, now=NOW, freshness=None, store=store)
    by_name = {check["name"]: check for check in report["checks"]}
    assert report["overall"] == "FAIL"
    runs = {row[0]: row for row in by_name["수집 실행"]["rows"]}
    assert runs["openfootball"][1] == "FAIL" and runs["openfootball"][5] == "HTTPError 503"
    assert runs["balldontlie"][1] == "WARN" and runs["the-odds-api"][1] == "OK"
    assert by_name["API 크레딧"]["status"] == "WARN" and by_name["API 크레딧"]["rows"][0][1] == 45
    stream = by_name["스트림·품질"]
    assert stream["status"] == "WARN" and {(row[1], row[4]) for row in stream["rows"]} == {("STALE_EVENT", ""),
                                                                                           ("SCHEMA_VIOLATION", "예")}
    assert by_name["경기 전 기록"]["status"] == by_name["주간 채점"]["status"] == by_name["레이크"]["status"] == "UNKNOWN"

    result = status.write(report, tmp_path / "status.html")
    assert result["overall"] == "FAIL" and (tmp_path / "status.json").exists()
    assert "HTTPError 503" in (tmp_path / "status.html").read_text(encoding="utf-8")
