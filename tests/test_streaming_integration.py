"""Kafka·Postgres·S3를 쓰는 스트리밍 통합 테스트(서비스가 없으면 건너뜀).

테스트마다 임시 DB, 이름이 겹치지 않는 임시 토픽, 새 소비자 그룹을 써서 실제 데이터와 섞이지 않게 한다.
"""
import io
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipelines.ingest import football_data_org as fdorg
from pipelines.streaming import events, kafka_io, match_consumer
from tests.test_pipeline_integration import isolated_settings, query  # noqa: F401 (fixture 재사용)

API_SAMPLE = (Path(__file__).parent / "fixtures" / "fdorg_matches_sample.json").read_bytes()
T0 = datetime(2025, 8, 16, 16, 0, tzinfo=timezone.utc)


@pytest.fixture
def stream(isolated_settings, tmp_path):
    """임시 토픽과 가상 팀 대조표(football-data.org 팀 ID 1~4 → Alpha FC~Delta FC)."""
    try:
        kafka_io.admin(isolated_settings).list_topics(timeout=5)
    except Exception as exc:
        if os.environ.get("REQUIRE_INTEGRATION") == "1":
            raise
        pytest.skip(f"Kafka 없음: {exc}")
    topic = f"test.matches.{uuid.uuid4().hex[:8]}"
    kafka_io.ensure_topics(isolated_settings, {topic: 1})
    aliases = tmp_path / "aliases.csv"
    aliases.write_text("source_id,alias,canonical_name,evidence\n" + "".join(
        f"football-data-org,{i},{name} FC,test\n" for i, name in enumerate(["Alpha", "Beta", "Gamma", "Delta"], 1)),
        encoding="utf-8")
    yield isolated_settings, topic, aliases
    kafka_io.delete_topics([topic], isolated_settings)


def match_event(*, score, published, match_id="9001", home="1", away="2", status="FINISHED"):
    payload = {"external_match_id": match_id, "utc_kickoff": "2025-08-16T14:00:00Z", "status": status, "matchday": 1,
               "home_team_id": home, "home_team_name": "x", "away_team_id": away, "away_team_name": "y",
               "ft_home": score[0], "ft_away": score[1], "ht_home": None, "ht_away": None, "duration": "REGULAR",
               "last_updated": published.isoformat(), "competition": "en.1", "season": "2025-26", "raw_file_id": None}
    return events.encode(events.build_event(source_id="football-data-org", event_type="match_snapshot", key=match_id,
                                            payload=payload, published_at=published, observed_at=published))


def send(settings, topic, messages):
    producer = kafka_io.producer(settings, client_id="test")
    for key, value in messages:
        kafka_io.publish(producer, topic, key, value)
    kafka_io.flush(producer)


def consume(settings, topic, aliases, group=None):
    return match_consumer.run(topics=[topic], group_id=group or f"test-{uuid.uuid4().hex[:8]}", settings=settings,
                              until_idle=True, idle_polls=3, alias_file=aliases)


def test_duplicates_late_events_and_bad_messages(stream):
    s, topic, aliases = stream
    v1 = match_event(score=(1, 0), published=T0)
    v2 = match_event(score=(2, 0), published=T0 + timedelta(minutes=30))
    stale = match_event(score=(1, 1), published=T0 + timedelta(minutes=10))  # v2보다 이전 정보가 늦게 도착
    send(s, topic, [("9001", v1), ("9001", v1), ("9001", v2), ("9001", stale), ("bad", b"{not json"),
                    ("9009", match_event(score=(0, 0), published=T0, match_id="9009", home="1", away="77"))])
    counts = consume(s, topic, aliases)
    assert counts == {"INSERTED": 2, "DUPLICATE": 1, "REJECTED:STALE_EVENT": 1, "REJECTED:INVALID_JSON": 1,
                      "REJECTED:UNMAPPED_TEAM": 1}
    revisions = query(s, "SELECT revision_no, ft_home, published_at FROM silver.live_match_revisions ORDER BY revision_no")
    assert [(r[0], r[1]) for r in revisions] == [(1, 1), (2, 2)]
    # 기준 시각 함수: 발표 시각뿐 아니라 우리가 받은 시각도 기준 시각 이전이어야 한다.
    # 이 이벤트들은 지금 받았으므로, 2025년 시점에는 아무것도 알려져 있지 않았다(과거로 소급하지 않음).
    known = "SELECT ft_home FROM silver.live_match_revisions_known_at(%s)"
    assert query(s, known, (T0 + timedelta(hours=1),)) == []
    assert query(s, known, (datetime.now(timezone.utc) + timedelta(minutes=1),))[0][0] == 2

    # 같은 토픽을 처음부터 다시 처리해도(새 소비자 그룹) Silver는 그대로다: 멱등 저장.
    again = consume(s, topic, aliases)
    assert again == {"DUPLICATE": 3, "REJECTED:STALE_EVENT": 1, "REJECTED:INVALID_JSON": 1, "REJECTED:UNMAPPED_TEAM": 1}
    assert query(s, "SELECT count(*) FROM silver.live_match_revisions")[0][0] == 2
    assert query(s, "SELECT count(*) FROM silver.stream_rejections")[0][0] == 3


def test_committed_offsets_are_not_reprocessed(stream):
    s, topic, aliases = stream
    send(s, topic, [("9001", match_event(score=(1, 0), published=T0))])
    assert consume(s, topic, aliases, group="same-group") == {"INSERTED": 1}
    send(s, topic, [("9001", match_event(score=(2, 0), published=T0 + timedelta(minutes=1)))])
    assert consume(s, topic, aliases, group="same-group") == {"INSERTED": 1}  # 새 메시지만 읽는다


def test_poll_once_stores_bronze_and_publishes_only_when_content_changes(stream):
    s, topic, aliases = stream
    s = s.__class__(**{**s.__dict__, "football_data_org_api_key": "test-key"})

    class Response(io.BytesIO):
        headers = {"X-Requests-Available-Minute": "9"}

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    opener = lambda request, timeout: Response(API_SAMPLE)
    first = fdorg.poll_once("PL", "2025-26", settings=s, opener=opener, topic=topic, now=T0)
    assert (first["new_content"], first["events_published"]) == (True, 3)
    second = fdorg.poll_once("PL", "2025-26", settings=s, opener=opener, topic=topic, now=T0)
    assert (second["new_content"], second["events_published"]) == (False, 0)
    assert query(s, "SELECT count(*), sum(is_new_content::int) FROM ingest.fetches")[0] == (2, 1)
    assert consume(s, topic, aliases) == {"INSERTED": 3}
    rows = query(s, "SELECT external_match_id, status, ft_home, published_at FROM silver.live_match_revisions ORDER BY 1")
    assert [(r[0], r[1], r[2]) for r in rows] == [("9001", "FINISHED", 2), ("9002", "TIMED", None), ("9003", "SCHEDULED", None)]
    assert rows[0][3] == datetime(2025, 8, 16, 16, 10, tzinfo=timezone.utc)


def test_season_replay_reaches_the_source_state_despite_faults(stream):
    import psycopg as pg
    from dataclasses import replace
    from pipelines.ingest import openfootball
    from pipelines.streaming import replay
    from tests.test_pipeline_integration import SAMPLE_PATH
    s, topic, _ = stream
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json", settings=s,
                        received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    target = replace(s, postgres_db=s.postgres_db + "_replay")
    with pg.connect(s.postgres_conninfo, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{target.postgres_db}"')
    try:
        stats = replay.replay_season("2025-26", source_settings=s, target_settings=target, topic=topic,
                                     seed=1, duplicate_rate=0.5, late_rate=0.5)
        assert stats["mismatches"] == 0 and stats["matches_replayed"] == stats["matches_expected"] == 5
        consumer = stats["consumer"]
        assert consumer.get("DUPLICATE", 0) == stats["injected_duplicates"]
        assert consumer.get("REJECTED:STALE_EVENT", 0) == stats["injected_late"]
        assert consumer["INSERTED"] == stats["events"] - stats["injected_late"]
    finally:
        with pg.connect(s.postgres_conninfo, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{target.postgres_db}" WITH (FORCE)')
