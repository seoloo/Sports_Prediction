"""과거 시즌 재생기(로드맵 2d): 스트리밍 소비자가 중복·순서 뒤바뀜에도 올바른 최종 상태를 만드는지 검증한다.

방법
1. 원본 DB의 openfootball 경기마다 이벤트 2개를 만든다.
   - 일정 발표: published_at = 경기 7일 전, 상태 SCHEDULED
   - 결과: published_at = 경기 시작 3시간 후(과거 재구성 가정, docs/football_data_sources.md), 상태 FINISHED
2. 발표 시각 순서로 보내되, 일부러 장애를 섞는다(고정 시드라 매번 같다).
   - 중복: 일부 이벤트를 한 번 더 보낸다 → 소비자는 DUPLICATE로 건너뛰어야 한다.
   - 늦은 도착: 일부 "일정 발표" 이벤트를 결과 뒤로 미룬다 → 소비자는 STALE_EVENT로 거부해야 한다.
3. 별도 재생용 DB(sports_replay)에 소비자를 돌린 뒤, 경기별 최종 점수가 원본과 모두 같은지 확인한다.
실제 데이터(sports DB)는 읽기만 하고 바꾸지 않는다.

실행: python -m pipelines.streaming.replay --season 2025-26
"""
import argparse
import json
import random
import uuid
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone

import psycopg

from ..aliases import sync_aliases
from ..config import load_settings
from ..db import connect, migrate
from . import events, kafka_io, match_consumer

SOURCE_ID = "openfootball"
TOPIC = "replay.matches.v1"
REPLAY_DATABASE = "sports_replay"
ANNOUNCE_BEFORE = timedelta(days=7)
RESULT_AFTER = timedelta(hours=3)


def season_events(conn: psycopg.Connection, season: str, competition: str = "en.1") -> tuple[list[tuple], list[str]]:
    """원본 DB에서 시즌 경기를 읽어 (발표 시각, 키, 이벤트 바이트) 목록과 팀 이름 목록을 만든다."""
    rows = conn.execute("""SELECT m.match_id, h.canonical_name, a.canonical_name, r.kickoff_utc, r.local_date, r.round,
                                  r.status, r.ht_home, r.ht_away, r.ft_home, r.ft_away
                           FROM silver.matches m
                           JOIN silver.match_revisions_known_at(now()) r USING (match_id)
                           JOIN silver.teams h ON h.team_id = m.home_team_id
                           JOIN silver.teams a ON a.team_id = m.away_team_id
                           WHERE m.competition_code=%s AND m.season=%s ORDER BY m.match_id""",
                        (competition, season)).fetchall()
    produced, teams = [], set()
    for match_id, home, away, kickoff, local_date, round_name, status, ht_home, ht_away, ft_home, ft_away in rows:
        teams.update((home, away))
        kickoff = kickoff or datetime.combine(local_date, time(12, 0), tzinfo=timezone.utc)
        base = {"external_match_id": str(match_id), "utc_kickoff": kickoff.isoformat(), "matchday": None,
                "home_team_id": home, "home_team_name": home, "away_team_id": away, "away_team_name": away,
                "duration": "REGULAR", "competition": competition, "season": season, "raw_file_id": None}
        stages = [(kickoff - ANNOUNCE_BEFORE, {"status": "SCHEDULED", "ft_home": None, "ft_away": None,
                                               "ht_home": None, "ht_away": None})]
        if status == "FINISHED":
            stages.append((kickoff + RESULT_AFTER, {"status": "FINISHED", "ft_home": ft_home, "ft_away": ft_away,
                                                    "ht_home": ht_home, "ht_away": ht_away}))
        for published_at, stage in stages:
            payload = {**base, **stage, "last_updated": published_at.isoformat()}
            event = events.build_event(source_id=SOURCE_ID, event_type="match_snapshot", key=str(match_id),
                                       payload=payload, published_at=published_at, observed_at=published_at)
            produced.append((published_at, str(match_id), events.encode(event), stage["status"]))
    return produced, sorted(teams)


def inject_faults(ordered: list[tuple], *, seed: int, duplicate_rate: float, late_rate: float):
    """발표 시각 순서의 이벤트에 중복과 늦은 도착을 섞는다. (보낼 목록, 섞은 중복 수, 섞은 늦은 도착 수)"""
    rng = random.Random(seed)
    finished_keys = {key for _, key, _, status in ordered if status == "FINISHED"}
    late = {i for i, (_, key, _, status) in enumerate(ordered)
            if status == "SCHEDULED" and key in finished_keys and rng.random() < late_rate}
    sent = [item for i, item in enumerate(ordered) if i not in late]
    duplicates = [item for item in sent if rng.random() < duplicate_rate]
    for item in duplicates:
        sent.insert(rng.randrange(sent.index(item) + 1, len(sent) + 1), item)
    # 늦게 도착하는 일정 발표는 그 경기 결과보다 뒤에 넣는다.
    for i in sorted(late):
        key = ordered[i][1]
        result_position = max(j for j, item in enumerate(sent) if item[1] == key and item[3] == "FINISHED")
        sent.insert(rng.randrange(result_position + 1, len(sent) + 1), ordered[i])
    return sent, len(duplicates), len(late)


def recreate_database(settings, name: str) -> None:
    """재생용 데이터베이스를 지우고 새로 만든다. 재생 결과가 지난 재생에 영향을 받지 않게 하기 위해서다.
    실제 데이터베이스 이름이면 거부한다."""
    if name == settings.postgres_db:
        raise ValueError("실제 데이터베이스는 재생 대상으로 쓸 수 없습니다.")
    with psycopg.connect(settings.postgres_conninfo, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')


def replay_season(season: str, *, source_settings=None, target_settings=None, topic: str = TOPIC, seed: int = 7,
                  duplicate_rate: float = 0.1, late_rate: float = 0.05) -> dict:
    """시즌 재생 → 소비 → 검증을 하고 통계를 돌려준다. mismatches가 0이어야 정상이다."""
    source_settings = source_settings or load_settings()
    if target_settings is None:
        recreate_database(source_settings, REPLAY_DATABASE)
        target_settings = replace(source_settings, postgres_db=REPLAY_DATABASE)
    with connect(source_settings) as conn:
        produced, teams = season_events(conn, season)
    ordered = sorted(produced, key=lambda item: (item[0], item[1]))
    to_send, duplicates, late = inject_faults(ordered, seed=seed, duplicate_rate=duplicate_rate, late_rate=late_rate)

    # 재생은 매번 깨끗한 상태에서 시작한다: 재생 DB를 새로 만들고(위), 재생 토픽도 지우고 다시 만든다.
    client = kafka_io.admin(source_settings)
    if topic in client.list_topics(timeout=10).topics:
        for future in client.delete_topics([topic]).values():
            future.result()
        for _ in range(50):
            if topic not in client.list_topics(timeout=10).topics:
                break
    kafka_io.ensure_topics(source_settings, {topic: kafka_io.TOPICS.get(topic, 3)})
    with connect(target_settings) as conn:
        migrate(conn)
    producer = kafka_io.producer(source_settings, client_id="season-replay")
    for _, key, value, _ in to_send:
        kafka_io.publish(producer, topic, key, value)
    kafka_io.flush(producer)

    with connect(target_settings) as conn, conn.transaction():
        sync_aliases(conn, [{"source_id": SOURCE_ID, "alias": name, "canonical_name": name} for name in teams],
                     datetime.now(timezone.utc))
    outcome = match_consumer.run(topics=[topic], group_id=f"replay-{uuid.uuid4().hex[:8]}", settings=target_settings,
                                 until_idle=True, idle_polls=5)

    with connect(source_settings) as source, connect(target_settings) as target:
        expected = {str(r[0]): (r[1], r[2], r[3]) for r in source.execute(
            """SELECT m.match_id, r.status, r.ft_home, r.ft_away FROM silver.matches m
               JOIN silver.match_revisions_known_at(now()) r USING (match_id)
               WHERE m.competition_code='en.1' AND m.season=%s""", (season,)).fetchall()}
        actual = {r[0]: (r[1], r[2], r[3]) for r in target.execute(
            """SELECT external_match_id, status, ft_home, ft_away FROM silver.live_match_revisions_known_at('infinity')
               WHERE source_id=%s""", (SOURCE_ID,)).fetchall()}
    mismatches = sorted(key for key in expected if actual.get(key) != expected[key])
    return {"season": season, "events": len(ordered), "sent_with_faults": len(to_send),
            "injected_duplicates": duplicates, "injected_late": late, "consumer": outcome,
            "matches_expected": len(expected), "matches_replayed": len(actual), "mismatches": len(mismatches)}


def main(argv=None):
    parser = argparse.ArgumentParser(description="과거 시즌을 Kafka로 재생해 소비자의 중복·늦은 이벤트 처리를 검증합니다.")
    parser.add_argument("--season", required=True)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duplicate-rate", type=float, default=0.1)
    parser.add_argument("--late-rate", type=float, default=0.05)
    args = parser.parse_args(argv)
    stats = replay_season(args.season, seed=args.seed, duplicate_rate=args.duplicate_rate, late_rate=args.late_rate)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0 if stats["mismatches"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
